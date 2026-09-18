//! Real Linux `io_uring` backend. Built on the `io-uring` crate's safe(r)
//! wrapper — see the module-level doc comment in `lib.rs` for why we don't
//! hand-roll the raw SQE/CQE ring here.

use crate::{CellId, GuestFd, IoCompletion, IoError, IoSubsystem};
use io_uring::{opcode, types, IoUring};
use std::collections::HashMap;

/// User-data encoding packed into each SQE so the completion handler can
/// recover which cell/guest-fd/request this CQE belongs to without a
/// separate side-table keyed by a kernel-assigned identifier. Layout:
/// `[ cell:24 bits | guest_fd:20 bits | request_id:20 bits ]` packed into
/// a u64 — deliberately compact so it survives the round trip through the
/// kernel's opaque `u64 user_data` field untouched.
#[inline]
fn pack_user_data(cell: CellId, guest_fd: GuestFd, request_id: u64) -> u64 {
    debug_assert!(cell < (1 << 24));
    debug_assert!(guest_fd < (1 << 20));
    debug_assert!(request_id < (1 << 20));
    (cell << 40) | ((guest_fd as u64) << 20) | (request_id & 0xF_FFFF)
}

#[inline]
fn unpack_user_data(bits: u64) -> (CellId, GuestFd, u64) {
    let cell = bits >> 40;
    let guest_fd = ((bits >> 20) & 0xFFFFF) as GuestFd;
    let request_id = bits & 0xFFFFF;
    (cell, guest_fd, request_id)
}

pub struct UringIoSubsystem {
    ring: IoUring,
    /// Fixed buffers registered with the kernel via
    /// `IORING_REGISTER_BUFFERS`. Indexed identically to how they were
    /// registered, so `submit_*_fixed` can hand the kernel a plain index.
    registered_buffer_lens: HashMap<u16, usize>,
}

impl UringIoSubsystem {
    /// Construct a new ring with `entries` submission-queue slots
    /// (rounded up to a power of two by the kernel). `entries` should be
    /// sized to comfortably exceed the number of in-flight requests across
    /// all cells sharing this worker thread's ring.
    pub fn new(entries: u32) -> Result<Self, IoError> {
        let ring = IoUring::new(entries).map_err(|e| IoError::SetupFailed(e.to_string()))?;
        Ok(Self {
            ring,
            registered_buffer_lens: HashMap::new(),
        })
    }
}

impl IoSubsystem for UringIoSubsystem {
    fn register_fixed_buffer(&mut self, buffer_index: u16, buf: &mut [u8]) -> Result<(), IoError> {
        // SAFETY: `buf` is a caller-owned, `'_`-lifetime-bounded slice that
        // must remain valid and un-moved for as long as it stays
        // registered with the kernel. The supervisor's convention (see
        // ARCHITECTURE.md §5) is that fixed buffers are carved out of a
        // cell's pinned guest-linear-memory region (itself a stable mmap
        // allocation per chronos_mem::MmapGuardedMemory, not moved for the
        // cell's lifetime), so the address stays valid for the
        // registration's lifetime. `register_buffers` takes a slice of
        // `iovec`-equivalent descriptors; we construct exactly one,
        // pointing at `buf`.
        let iovec = libc::iovec {
            iov_base: buf.as_mut_ptr() as *mut libc::c_void,
            iov_len: buf.len(),
        };
        unsafe {
            self.ring
                .submitter()
                .register_buffers(std::slice::from_ref(&iovec))
                .map_err(|e| IoError::BufferRegistrationFailed(e.to_string()))?;
        }
        self.registered_buffer_lens.insert(buffer_index, buf.len());
        Ok(())
    }

    fn submit_read_fixed(
        &mut self,
        cell: CellId,
        guest_fd: GuestFd,
        host_fd: i32,
        buffer_index: u16,
        offset: u64,
        len: u32,
        request_id: u64,
    ) -> Result<(), IoError> {
        let user_data = pack_user_data(cell, guest_fd, request_id);
        // Buffer pointer/len are resolved kernel-side from the registered
        // fixed-buffer table by `buffer_index`; we pass a null pointer +
        // requested len here because `io-uring`'s `ReadFixed` opcode
        // builder only needs the buffer *index*, not the address, once
        // the buffer has been registered via `register_fixed_buffer`
        // above — the kernel already has the address from registration.
        let read_e = opcode::ReadFixed::new(types::Fd(host_fd), std::ptr::null_mut(), len, buffer_index)
            .offset(offset)
            .build()
            .user_data(user_data);

        // SAFETY: `read_e` was built against a `host_fd` that the caller
        // (the supervisor's syscall-interception layer) is responsible
        // for keeping open for at least the lifetime of this in-flight
        // request — the `FdTable` (lib.rs) is the single source of truth
        // tying guest fds to host fds and is never torn down mid-flight
        // for a live request. The SQE itself contains no raw pointers
        // beyond what `io-uring` manages internally.
        unsafe {
            self.ring
                .submission()
                .push(&read_e)
                .map_err(|_| IoError::SubmissionQueueFull)?;
        }
        self.ring
            .submit()
            .map_err(|e| IoError::SetupFailed(e.to_string()))?;
        Ok(())
    }

    fn submit_write_fixed(
        &mut self,
        cell: CellId,
        guest_fd: GuestFd,
        host_fd: i32,
        buffer_index: u16,
        offset: u64,
        len: u32,
        request_id: u64,
    ) -> Result<(), IoError> {
        let user_data = pack_user_data(cell, guest_fd, request_id);
        let write_e = opcode::WriteFixed::new(types::Fd(host_fd), std::ptr::null(), len, buffer_index)
            .offset(offset)
            .build()
            .user_data(user_data);

        // SAFETY: same reasoning as `submit_read_fixed` — `host_fd`
        // outlives the request by FdTable convention, and the fixed
        // buffer referenced by `buffer_index` was pinned and registered
        // by `register_fixed_buffer` before this call.
        unsafe {
            self.ring
                .submission()
                .push(&write_e)
                .map_err(|_| IoError::SubmissionQueueFull)?;
        }
        self.ring
            .submit()
            .map_err(|e| IoError::SetupFailed(e.to_string()))?;
        Ok(())
    }

    fn poll_completions(&mut self) -> Vec<IoCompletion> {
        let mut out = Vec::new();
        // `completion()` gives a draining view over currently-available
        // CQEs; this call does not block (blocking-with-timeout would use
        // `submit_and_wait` before draining).
        let mut cq = self.ring.completion();
        cq.sync();
        for cqe in &mut cq {
            let (cell, guest_fd, request_id) = unpack_user_data(cqe.user_data());
            out.push(IoCompletion {
                cell,
                guest_fd,
                request_id,
                result: cqe.result(),
            });
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_data_packing_round_trips() {
        let (cell, guest_fd, request_id) = (12345u64, 99u32, 555u64);
        let packed = pack_user_data(cell, guest_fd, request_id);
        let (c2, g2, r2) = unpack_user_data(packed);
        assert_eq!((cell, guest_fd, request_id), (c2, g2, r2));
    }
}
