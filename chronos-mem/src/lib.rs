//! chronos-mem: guarded linear memory, dirty-page tracking, delta-diff engine,
//! and Sparse Merkle Tree (SMT) execution-integrity checkpointing.
//!
//! This crate is isolation-mode-agnostic: both the software-fault-isolation
//! path (mmap + guard page) and a future hardware nested-paging path are
//! expected to implement `PageFaultResolver`, while `GuardedMemory` and the
//! SMT/delta code are written once against that trait.

pub mod guarded_memory;
pub mod dirty;
pub mod delta;
pub mod smt;

pub use guarded_memory::{GuardedMemory, MmapGuardedMemory, MemFault};
pub use dirty::DirtyPageSet;
pub use delta::{DeltaEngine, PageDelta};
pub use smt::{SparseMerkleTree, CheckpointChain, PAGE_SIZE};

/// Stable identifier for a tenant execution context ("cell").
pub type CellId = u64;

/// Logical scheduler tick — see chronos-core::clock::LogicalClock.
pub type Tick = u64;
