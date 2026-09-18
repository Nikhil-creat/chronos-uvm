//! io_uring-powered asynchronous I/O subsystem (ARCHITECTURE.md §5).
//!
//! One `io_uring` instance is shared by all cells dispatched on a given
//! worker thread. Guest `fd_read`/`fd_write` host-calls are translated into
//! `io_uring` SQEs against *fixed, pre-registered* buffers, so no
//! host-side copy occurs between the guest's linear memory and the
//! kernel's I/O path. Completions are not delivered to the tenant
//! immediately — they are queued per-cell and only handed over at the next
//! quantum boundary (§2 of ARCHITECTURE.md), which is what keeps I/O
//! completion order part of the deterministic, replayable event log
//! rather than a live race between cells.
//!
//! This module is written against the `io-uring` crate's safe(r)
//! `IoUring` wrapper rather than raw `io_uring_enter`/`io_uring_setup`
//! syscalls, to keep the `unsafe` surface area scoped to buffer
//! registration (see `register_fixed_buffers`) and auditable; see
//! ARCHITECTURE.md §10 for the tradeoffs of a hand-rolled ring.

#[cfg(target_os = "linux")]
mod linux_ring;
#[cfg(target_os = "linux")]
pub use linux_ring::UringIoSubsystem;

use std::collections::VecDeque;
use thiserror::Error;

/// Stable identifier for a cell, matching `chronos_mem::CellId`. Duplicated
/// here (rather than depending on chronos-mem) to keep this crate usable
/// standalone against any memory backend.
pub type CellId = u64;

/// Cell-local file descriptor, as seen by guest host-calls. Never a raw OS
/// fd — always resolved through `FdTable`.
pub type GuestFd = u32;

#[derive(Debug, Error)]
pub enum IoError {
    #[error("guest fd {0} is not registered for this cell")]
    UnknownFd(GuestFd),
    #[error("io_uring submission queue full")]
    SubmissionQueueFull,
    #[error("io_uring setup failed: {0}")]
    SetupFailed(String),
    #[error("buffer registration failed: {0}")]
    BufferRegistrationFailed(String),
}

/// A single completed I/O operation, queued for delivery to its owning
/// cell at the next quantum boundary.
#[derive(Debug, Clone)]
pub struct IoCompletion {
    pub cell: CellId,
    pub guest_fd: GuestFd,
    pub request_id: u64,
    /// Bytes transferred, or negative `-errno` on failure — mirroring the
    /// raw io_uring CQE `res` field semantics so no information is lost in
    /// translation.
    pub result: i32,
}

/// Per-cell mapping from guest-visible fd numbers to real host fds
/// registered with the shared io_uring instance. Kept separate per cell so
/// no tenant can ever address another tenant's fd, even by guessing a
/// number.
#[derive(Default)]
pub struct FdTable {
    entries: std::collections::HashMap<GuestFd, i32>,
    next_guest_fd: GuestFd,
}

impl FdTable {
    pub fn new() -> Self {
        Self {
            entries: std::collections::HashMap::new(),
            next_guest_fd: 3, // reserve 0/1/2 as guest stdio-equivalents
        }
    }

    pub fn register(&mut self, host_fd: i32) -> GuestFd {
        let guest_fd = self.next_guest_fd;
        self.next_guest_fd += 1;
        self.entries.insert(guest_fd, host_fd);
        guest_fd
    }

    pub fn resolve(&self, guest_fd: GuestFd) -> Result<i32, IoError> {
        self.entries.get(&guest_fd).copied().ok_or(IoError::UnknownFd(guest_fd))
    }

    pub fn close(&mut self, guest_fd: GuestFd) -> Option<i32> {
        self.entries.remove(&guest_fd)
    }
}

/// Per-cell queue of completions that have arrived from the kernel but not
/// yet been handed to the tenant, enforcing the "only visible at a quantum
/// boundary" discipline from ARCHITECTURE.md §2.
#[derive(Default)]
pub struct PendingCompletions {
    queue: VecDeque<IoCompletion>,
}

impl PendingCompletions {
    pub fn new() -> Self {
        Self { queue: VecDeque::new() }
    }

    pub fn push(&mut self, completion: IoCompletion) {
        self.queue.push_back(completion);
    }

    /// Called exactly once per quantum boundary by the scheduler, draining
    /// everything accumulated since the cell was last dispatched, in
    /// arrival order — this order is what gets appended to the
    /// deterministic event log so replay reproduces it exactly.
    pub fn drain_for_dispatch(&mut self) -> Vec<IoCompletion> {
        self.queue.drain(..).collect()
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
}

/// Platform-independent trait the supervisor programs against; the Linux
/// implementation (`linux_ring::UringIoSubsystem`) backs it with a real
/// io_uring instance, while a non-Linux build (or unit tests) can use a
/// trivial in-memory stub implementing the same trait.
pub trait IoSubsystem {
    /// Register a fixed, pre-pinned buffer for zero-copy transfers. Must
    /// be called before any read/write referencing `buffer_index`.
    fn register_fixed_buffer(&mut self, buffer_index: u16, buf: &mut [u8]) -> Result<(), IoError>;

    /// Submit a zero-copy read using a previously registered fixed buffer.
    /// Non-blocking: returns immediately after the SQE is submitted;
    /// completion arrives later via `poll_completions`.
    fn submit_read_fixed(
        &mut self,
        cell: CellId,
        guest_fd: GuestFd,
        host_fd: i32,
        buffer_index: u16,
        offset: u64,
        len: u32,
        request_id: u64,
    ) -> Result<(), IoError>;

    fn submit_write_fixed(
        &mut self,
        cell: CellId,
        guest_fd: GuestFd,
        host_fd: i32,
        buffer_index: u16,
        offset: u64,
        len: u32,
        request_id: u64,
    ) -> Result<(), IoError>;

    /// Drain the kernel completion queue into caller-owned `IoCompletion`
    /// records. Non-blocking (a blocking variant with a timeout is a
    /// straightforward extension using `io_uring`'s `submit_and_wait`).
    fn poll_completions(&mut self) -> Vec<IoCompletion>;
}

/// Minimal in-process stub, useful for unit tests and non-Linux
/// development builds where a real io_uring instance is unavailable. Not
/// zero-copy — exists purely to exercise the `IoSubsystem` contract.
pub struct StubIoSubsystem {
    completions: Vec<IoCompletion>,
}

impl StubIoSubsystem {
    pub fn new() -> Self {
        Self { completions: Vec::new() }
    }
}

impl Default for StubIoSubsystem {
    fn default() -> Self {
        Self::new()
    }
}

impl IoSubsystem for StubIoSubsystem {
    fn register_fixed_buffer(&mut self, _buffer_index: u16, _buf: &mut [u8]) -> Result<(), IoError> {
        Ok(())
    }

    fn submit_read_fixed(
        &mut self,
        cell: CellId,
        guest_fd: GuestFd,
        _host_fd: i32,
        _buffer_index: u16,
        _offset: u64,
        len: u32,
        request_id: u64,
    ) -> Result<(), IoError> {
        // Stub completes instantly with "full transfer" semantics.
        self.completions.push(IoCompletion {
            cell,
            guest_fd,
            request_id,
            result: len as i32,
        });
        Ok(())
    }

    fn submit_write_fixed(
        &mut self,
        cell: CellId,
        guest_fd: GuestFd,
        _host_fd: i32,
        _buffer_index: u16,
        _offset: u64,
        len: u32,
        request_id: u64,
    ) -> Result<(), IoError> {
        self.completions.push(IoCompletion {
            cell,
            guest_fd,
            request_id,
            result: len as i32,
        });
        Ok(())
    }

    fn poll_completions(&mut self) -> Vec<IoCompletion> {
        std::mem::take(&mut self.completions)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fd_table_isolates_guest_fds() {
        let mut table = FdTable::new();
        let guest_fd_a = table.register(10);
        let guest_fd_b = table.register(11);
        assert_ne!(guest_fd_a, guest_fd_b);
        assert_eq!(table.resolve(guest_fd_a).unwrap(), 10);
        assert_eq!(table.resolve(guest_fd_b).unwrap(), 11);
        assert!(table.resolve(9999).is_err());
    }

    #[test]
    fn pending_completions_drain_in_order() {
        let mut pending = PendingCompletions::new();
        for i in 0..3 {
            pending.push(IoCompletion { cell: 0, guest_fd: 3, request_id: i, result: 4096 });
        }
        let drained = pending.drain_for_dispatch();
        let ids: Vec<u64> = drained.iter().map(|c| c.request_id).collect();
        assert_eq!(ids, vec![0, 1, 2]);
        assert!(pending.is_empty());
    }

    #[test]
    fn stub_subsystem_round_trips_a_completion() {
        let mut io = StubIoSubsystem::new();
        io.submit_read_fixed(0, 3, 5, 0, 0, 4096, 42).unwrap();
        let completions = io.poll_completions();
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].request_id, 42);
        assert_eq!(completions[0].result, 4096);
    }
}
