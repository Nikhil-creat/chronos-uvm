//! A "cell" is one tenant's execution context: guarded linear memory, its
//! own non-determinism event recorder, its own dirty-page-derived
//! checkpoint chain, and its own pending-I/O-completion queue. The
//! supervisor dispatches cells; a cell never touches another cell's state.

use chronos_io::{FdTable, PendingCompletions};
use chronos_mem::{CheckpointChain, MmapGuardedMemory};

pub type CellId = u64;

#[derive(Debug, Clone)]
pub struct CellConfig {
    pub id: CellId,
    pub initial_memory_bytes: u32,
    pub quota_bytes: u32,
    /// Ticks between SMT checkpoints (ARCHITECTURE.md §6). Smaller values
    /// give tighter integrity/HA granularity at higher hashing cost.
    pub checkpoint_interval_ticks: u64,
}

/// One tenant's full runtime state. Deliberately holds no reference to any
/// other `Cell` — cross-cell isolation is a structural property of this
/// type, not an access-control check layered on top.
pub struct Cell {
    pub config: CellConfig,
    pub memory: MmapGuardedMemory,
    pub recorder: crate::event_log::EventRecorder,
    pub checkpoints: CheckpointChain,
    pub fd_table: FdTable,
    pub pending_io: PendingCompletions,
    /// Tick of the last SMT checkpoint taken for this cell, used to decide
    /// when the next one is due.
    pub last_checkpoint_tick: u64,
}

impl Cell {
    pub fn new(config: CellConfig, recorder_mode: crate::event_log::RecorderMode) -> Result<Self, chronos_mem::MemFault> {
        let memory = MmapGuardedMemory::new(config.initial_memory_bytes, config.quota_bytes)?;
        Ok(Self {
            recorder: crate::event_log::EventRecorder::new(recorder_mode),
            checkpoints: CheckpointChain::new(),
            fd_table: FdTable::new(),
            pending_io: PendingCompletions::new(),
            last_checkpoint_tick: 0,
            memory,
            config,
        })
    }

    /// Whether a checkpoint is due at `current_tick`, per this cell's
    /// configured interval.
    pub fn checkpoint_due(&self, current_tick: u64) -> bool {
        current_tick.saturating_sub(self.last_checkpoint_tick) >= self.config.checkpoint_interval_ticks
    }
}
