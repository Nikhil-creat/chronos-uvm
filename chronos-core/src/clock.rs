//! Deterministic logical clock (ARCHITECTURE.md §2). Advances only when
//! the scheduler dispatches a quantum — never from wall-clock time — so
//! that `(tick, call_ordinal)` keys into the event log are reproducible
//! across hosts with different clock speeds, core counts, or load.

pub type Tick = u64;

/// A monotonic tick counter, one unit per dispatched execution quantum.
/// Deliberately *not* `Clone`/`Copy` for the mutable handle — there must be
/// exactly one authority advancing it (the scheduler); read-only observers
/// use `snapshot()`.
pub struct LogicalClock {
    tick: Tick,
}

impl LogicalClock {
    pub fn new() -> Self {
        Self { tick: 0 }
    }

    /// Advance by exactly one quantum. Called once per dispatch by the
    /// scheduler, never speculatively.
    pub fn advance(&mut self) -> Tick {
        self.tick += 1;
        self.tick
    }

    pub fn snapshot(&self) -> Tick {
        self.tick
    }
}

impl Default for LogicalClock {
    fn default() -> Self {
        Self::new()
    }
}
