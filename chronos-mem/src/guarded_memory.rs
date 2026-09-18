//! Software-fault-isolated guest linear memory.
//!
//! Layout per cell:
//!
//! ```text
//! [ ------------- linear memory (quota, power-of-two) ------------- ][ guard page(s), PROT_NONE ]
//! ```
//!
//! Every guest access is bounds-checked against the *declared* memory size
//! (`self.size_bytes`) before any pointer arithmetic occurs. The guard page
//! is a second, independent line of defense: if the bounds check itself has
//! a bug, an out-of-bounds access still lands on unmapped memory and raises
//! SIGSEGV rather than corrupting an adjacent tenant's heap.

use crate::dirty::DirtyPageSet;
use std::ptr::NonNull;
use thiserror::Error;

pub const PAGE_SIZE: usize = 4096;

#[derive(Debug, Error)]
pub enum MemFault {
    #[error("guest access out of bounds: ptr={ptr} len={len} size={size}")]
    OutOfBounds { ptr: u32, len: u32, size: u32 },
    #[error("requested growth would exceed tenant quota: requested={requested} quota={quota}")]
    QuotaExceeded { requested: u32, quota: u32 },
    #[error("mmap failed: {0}")]
    MmapFailed(std::io::Error),
    #[error("mprotect failed: {0}")]
    MprotectFailed(std::io::Error),
    #[error("integer overflow computing memory layout")]
    LayoutOverflow,
}

/// Trait implemented by any backing store for a cell's linear memory,
/// regardless of whether isolation is enforced by software bounds-checks
/// (this crate's `MmapGuardedMemory`) or by hardware nested paging.
pub trait GuardedMemory {
    /// Validate and translate a guest pointer + length into a raw host
    /// pointer. Returns `Err` rather than ever returning a pointer that
    /// falls outside the declared memory region.
    fn translate(&self, guest_ptr: u32, len: u32) -> Result<*mut u8, MemFault>;

    /// Grow the linear memory by `delta_pages` (each `PAGE_SIZE` bytes),
    /// enforcing the tenant's quota. Returns the *previous* page count on
    /// success (matching Wasm `memory.grow` semantics).
    fn grow(&mut self, delta_pages: u32) -> Result<u32, MemFault>;

    /// Current committed size, in bytes.
    fn size_bytes(&self) -> u32;

    /// Read-only view of which pages have been written since the last
    /// checkpoint / replication flush.
    fn dirty_pages(&self) -> &DirtyPageSet;

    /// Mutable access to the dirty set, reserved for the supervisor's
    /// checkpoint/replication flush step (the only code path permitted to
    /// clear dirty tracking — see chronos-core::supervisor).
    fn dirty_pages_mut(&mut self) -> &mut DirtyPageSet;

    /// Mark a page dirty. Called by the store-path trampoline after every
    /// guest write that `translate` approved. Kept separate from
    /// `translate` so read-only accesses never touch the dirty set.
    fn mark_dirty(&mut self, page_idx: u64);

    /// Raw byte slice of a single page, for checkpoint hashing / delta
    /// diffing. `page_idx` must be `< size_bytes() / PAGE_SIZE`.
    fn page_bytes(&self, page_idx: u64) -> &[u8];
}

/// Software-fault-isolated memory region backed by a single `mmap`
/// allocation, sized to the tenant's quota with a trailing `PROT_NONE`
/// guard region.
pub struct MmapGuardedMemory {
    /// Base of the mapping, including the guard region.
    base: NonNull<u8>,
    /// Total mapped length (quota_bytes + guard_bytes), for munmap on drop.
    mapped_len: usize,
    /// Bytes currently committed and visible to the guest (<= quota).
    size_bytes: u32,
    /// Hard ceiling for this tenant, in bytes. Never changes post-construction.
    quota_bytes: u32,
    dirty: DirtyPageSet,
}

// SAFETY: `MmapGuardedMemory` owns its mapping exclusively; it is Send
// because the underlying mmap region has no thread-affinity, and Sync
// is *not* derived (we do not implement it) because concurrent guest
// stores without external synchronization would race — the supervisor
// is responsible for single-threaded-per-cell dispatch (see
// chronos-core::scheduler), which upholds that invariant.
unsafe impl Send for MmapGuardedMemory {}

impl MmapGuardedMemory {
    /// Construct a new guarded region for `quota_bytes` (rounded up to the
    /// next page boundary), with `initial_bytes` committed and immediately
    /// accessible to the guest.
    ///
    /// # Safety-relevant invariants established here
    /// The trailing guard region is mapped with `PROT_NONE` immediately
    /// after `mmap`, before this function returns, so there is no window
    /// during which an out-of-bounds pointer derived from a concurrently
    /// racing thread could observe a readable/writable guard page. (In
    /// practice the supervisor does not hand out guest pointers until
    /// construction completes, but this ordering is kept as defense in
    /// depth.)
    pub fn new(initial_bytes: u32, quota_bytes: u32) -> Result<Self, MemFault> {
        if initial_bytes > quota_bytes {
            return Err(MemFault::QuotaExceeded {
                requested: initial_bytes,
                quota: quota_bytes,
            });
        }

        let quota_pages = (quota_bytes as usize + PAGE_SIZE - 1) / PAGE_SIZE;
        let quota_rounded = quota_pages
            .checked_mul(PAGE_SIZE)
            .ok_or(MemFault::LayoutOverflow)?;
        // One guard page is sufficient for single-instruction overruns;
        // widen this if the host ever allows guest-controlled bulk-copy
        // lengths larger than one page without an explicit length check
        // upstream of translate().
        let guard_bytes = PAGE_SIZE;
        let mapped_len = quota_rounded
            .checked_add(guard_bytes)
            .ok_or(MemFault::LayoutOverflow)?;

        // SAFETY: standard anonymous, private mapping; no fd, no offset.
        // MAP_NORESERVE avoids committing swap for the full quota up
        // front — pages are backed lazily by the kernel on first touch,
        // which is compatible with (and later replaced by) the UFFD lazy
        // paging path described in ARCHITECTURE.md §8.
        let addr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                mapped_len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
                -1,
                0,
            )
        };
        if addr == libc::MAP_FAILED {
            return Err(MemFault::MmapFailed(std::io::Error::last_os_error()));
        }
        // SAFETY: mmap succeeded and did not return null (MAP_FAILED is a
        // distinct sentinel, checked above), so `addr` is a valid non-null
        // pointer to `mapped_len` bytes we exclusively own.
        let base = unsafe { NonNull::new_unchecked(addr as *mut u8) };

        // SAFETY: `guard_ptr` is `base + quota_rounded`, strictly inside
        // the `mapped_len`-byte mapping we just created (mapped_len =
        // quota_rounded + guard_bytes), so this mprotect call targets
        // memory we own and does not go out of bounds of the mapping.
        let guard_ptr = unsafe { addr.add(quota_rounded) };
        let rc = unsafe { libc::mprotect(guard_ptr, guard_bytes, libc::PROT_NONE) };
        if rc != 0 {
            let err = std::io::Error::last_os_error();
            // SAFETY: tearing down the mapping we just created on the
            // error path; base/mapped_len are exactly what mmap returned.
            unsafe {
                libc::munmap(addr, mapped_len);
            }
            return Err(MemFault::MprotectFailed(err));
        }

        let dirty_capacity_pages = quota_rounded / PAGE_SIZE;
        Ok(Self {
            base,
            mapped_len,
            size_bytes: initial_bytes,
            quota_bytes,
            dirty: DirtyPageSet::new(dirty_capacity_pages as u64),
        })
    }
}

impl Drop for MmapGuardedMemory {
    fn drop(&mut self) {
        // SAFETY: `self.base`/`self.mapped_len` are exactly the pointer
        // and length returned by the `mmap` call in `new`, never mutated
        // afterward, and this is the only owner (no Clone/Sync impl), so
        // this is the unique, final unmap of this region.
        unsafe {
            libc::munmap(self.base.as_ptr() as *mut libc::c_void, self.mapped_len);
        }
    }
}

impl GuardedMemory for MmapGuardedMemory {
    fn translate(&self, guest_ptr: u32, len: u32) -> Result<*mut u8, MemFault> {
        // Checked arithmetic: guest_ptr + len must not overflow u32 *and*
        // must not exceed the currently committed size. This is the
        // primary bounds check; the guard page (see `new`) is the
        // secondary, hardware-enforced backstop.
        let end = guest_ptr
            .checked_add(len)
            .ok_or(MemFault::OutOfBounds { ptr: guest_ptr, len, size: self.size_bytes })?;
        if end > self.size_bytes {
            return Err(MemFault::OutOfBounds {
                ptr: guest_ptr,
                len,
                size: self.size_bytes,
            });
        }
        // SAFETY: `end <= self.size_bytes <= quota_rounded <= mapped_len`,
        // so `base + guest_ptr .. base + end` lies strictly within the
        // mapping owned by `self`. `guest_ptr` and `len` are both `u32`,
        // so the pointer offset cannot overflow `isize` on any supported
        // platform (mapped_len is bounded by quota_bytes, itself a u32).
        Ok(unsafe { self.base.as_ptr().add(guest_ptr as usize) })
    }

    fn grow(&mut self, delta_pages: u32) -> Result<u32, MemFault> {
        let delta_bytes = (delta_pages as u64) * (PAGE_SIZE as u64);
        let new_size = self.size_bytes as u64 + delta_bytes;
        if new_size > self.quota_bytes as u64 {
            return Err(MemFault::QuotaExceeded {
                requested: new_size as u32,
                quota: self.quota_bytes,
            });
        }
        let previous_pages = self.size_bytes / PAGE_SIZE as u32;
        self.size_bytes = new_size as u32;
        Ok(previous_pages)
    }

    fn size_bytes(&self) -> u32 {
        self.size_bytes
    }

    fn dirty_pages(&self) -> &DirtyPageSet {
        &self.dirty
    }

    fn dirty_pages_mut(&mut self) -> &mut DirtyPageSet {
        &mut self.dirty
    }

    fn mark_dirty(&mut self, page_idx: u64) {
        self.dirty.mark(page_idx);
    }

    fn page_bytes(&self, page_idx: u64) -> &[u8] {
        let offset = page_idx as usize * PAGE_SIZE;
        debug_assert!(offset + PAGE_SIZE <= self.size_bytes as usize);
        // SAFETY: caller-checked (debug_assert above; release builds rely
        // on all call sites deriving page_idx from a size already
        // validated by `translate`/`grow`) offset within the committed,
        // owned mapping.
        unsafe { std::slice::from_raw_parts(self.base.as_ptr().add(offset), PAGE_SIZE) }
    }
}
