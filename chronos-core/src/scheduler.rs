//! Deterministic scheduler (ARCHITECTURE.md §2). Strict round-robin
//! dispatch across cells, advancing one shared `LogicalClock` tick per
//! dispatched quantum. Deliberately does *not* reorder based on I/O
//! readiness, timer expiry, or any other live signal — those are exactly
//! the non-deterministic inputs §2 requires be captured, not observed by
//! the dispatch decision itself.

use crate::cell::{Cell, CellId};
use crate::clock::LogicalClock;
use std::collections::VecDeque;

/// Result of dispatching one cell for one quantum. The supervisor uses
/// this to decide whether a checkpoint or replication flush is due.
pub struct DispatchOutcome {
    pub cell_id: CellId,
    pub tick: u64,
    pub checkpoint_due: bool,
}

/// Strict round-robin scheduler over a fixed set of cells. Cells are
/// dispatched in a stable order every rotation; this order is itself part
/// of the deterministic contract (ARCHITECTURE.md §2) — changing the
/// registration order between a record run and a replay run would change
/// which cell's non-determinism gets which tick number, breaking replay.
pub struct Scheduler {
    order: VecDeque<CellId>,
    clock: LogicalClock,
}

impl Scheduler {
    pub fn new(cell_ids: impl IntoIterator<Item = CellId>) -> Self {
        Self {
            order: cell_ids.into_iter().collect(),
            clock: LogicalClock::new(),
        }
    }

    pub fn register_cell(&mut self, cell_id: CellId) {
        self.order.push_back(cell_id);
    }

    /// Deregister a cell (e.g. it completed or was evicted). Does not
    /// affect the tick numbering of remaining cells' future dispatches —
    /// tick is a global scheduler-wide counter, not per-cell.
    pub fn deregister_cell(&mut self, cell_id: CellId) {
        self.order.retain(|&id| id != cell_id);
    }

    pub fn current_tick(&self) -> u64 {
        self.clock.snapshot()
    }

    /// Advance to the next cell in rotation and return its id plus the new
    /// global tick, or `None` if there are no cells registered.
    pub fn next_dispatch(&mut self, cells: &std::collections::HashMap<CellId, Cell>) -> Option<DispatchOutcome> {
        if self.order.is_empty() {
            return None;
        }
        // Rotate: pop from front, push to back — this is the entire
        // scheduling policy for the baseline strict-round-robin mode.
        // Weighted-fair or priority variants would replace only this
        // rotation rule, keeping tick advancement and the
        // checkpoint-due check below unchanged.
        let cell_id = self.order.pop_front()?;
        self.order.push_back(cell_id);

        let tick = self.clock.advance();
        let checkpoint_due = cells
            .get(&cell_id)
            .map(|c| c.checkpoint_due(tick))
            .unwrap_or(false);

        Some(DispatchOutcome { cell_id, tick, checkpoint_due })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cell::CellConfig;
    use crate::event_log::RecorderMode;
    use std::collections::HashMap;

    fn make_cell(id: CellId) -> Cell {
        let config = CellConfig {
            id,
            initial_memory_bytes: 4096,
            quota_bytes: 4096 * 16,
            checkpoint_interval_ticks: 4,
        };
        Cell::new(config, RecorderMode::Record).unwrap()
    }

    #[test]
    fn round_robin_visits_every_cell_before_repeating() {
        let mut cells = HashMap::new();
        for id in [1, 2, 3] {
            cells.insert(id, make_cell(id));
        }
        let mut sched = Scheduler::new([1, 2, 3]);
        let mut seen = Vec::new();
        for _ in 0..6 {
            let outcome = sched.next_dispatch(&cells).unwrap();
            seen.push(outcome.cell_id);
        }
        assert_eq!(seen, vec![1, 2, 3, 1, 2, 3]);
    }

    #[test]
    fn tick_advances_globally_not_per_cell() {
        let mut cells = HashMap::new();
        for id in [1, 2] {
            cells.insert(id, make_cell(id));
        }
        let mut sched = Scheduler::new([1, 2]);
        let o1 = sched.next_dispatch(&cells).unwrap();
        let o2 = sched.next_dispatch(&cells).unwrap();
        assert_eq!(o1.tick, 1);
        assert_eq!(o2.tick, 2);
    }

    #[test]
    fn checkpoint_due_respects_configured_interval() {
        let mut cells = HashMap::new();
        cells.insert(1, make_cell(1)); // checkpoint_interval_ticks = 4
        let mut sched = Scheduler::new([1]);
        let outcomes: Vec<bool> = (0..5)
            .map(|_| sched.next_dispatch(&cells).unwrap().checkpoint_due)
            .collect();
        // ticks 1,2,3,4,5 -> due at tick 4 (>= interval since last=0)
        assert_eq!(outcomes, vec![false, false, false, true, true]);
    }
}
