//! chronos-core: the supervisor. Owns the deterministic scheduler, the
//! non-determinism event recorder, cell lifecycle, and ties together
//! chronos-mem (memory/checkpointing), chronos-io (I/O), and
//! chronos-replicate (HA) into one dispatch loop.

pub mod clock;
pub mod event_log;
pub mod cell;
pub mod scheduler;
pub mod supervisor;

pub use clock::{LogicalClock, Tick};
pub use event_log::{EventRecorder, NonDeterminismSource};
pub use cell::{Cell, CellId, CellConfig};
pub use scheduler::{Scheduler, DispatchOutcome};
pub use supervisor::Supervisor;
