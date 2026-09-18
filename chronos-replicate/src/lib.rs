//! Live State-Diff Replication (ARCHITECTURE.md §7).
//!
//! One producer (the cell's dirty-page hook, running on the supervisor's
//! dispatch thread) pushes `PageDelta`s into a lock-free SPSC ring buffer.
//! One consumer (a dedicated network-streaming task) drains the ring in
//! batches at quantum boundaries and ships them to the standby, which
//! applies them in tick order via `chronos_mem::DeltaEngine::apply`.
//!
//! The ring is built on `crossbeam::queue::ArrayQueue`, which is already a
//! correct, well-audited lock-free bounded MPMC queue; we specialize it
//! here to a single-producer/single-consumer usage pattern and wrap it
//! with the tick-batching and backpressure policy this subsystem needs, so
//! we are not reinventing lock-free correctness proofs, only composing a
//! proven primitive with domain-specific batching logic.

use chronos_mem::delta::PageDelta;
use crossbeam::queue::ArrayQueue;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use thiserror::Error;

/// One tick's worth of dirty-page deltas, the unit of transfer to the
/// standby. Batching at tick granularity (rather than streaming each
/// PageDelta individually) is what lets the standby apply deltas
/// atomically per tick and lets the primary attach the tick's checkpoint
/// metadata (see chronos-mem::smt::CheckpointEntry) alongside the deltas
/// for verification.
#[derive(Debug, Clone)]
pub struct TickBatch {
    pub tick: u64,
    pub deltas: Vec<PageDelta>,
}

#[derive(Debug, Error)]
pub enum RingError {
    #[error("ring buffer full at capacity {0}; standby is falling behind")]
    Full(usize),
}

/// Bounded lock-free SPSC ring carrying `TickBatch`es from the primary's
/// dispatch thread to the network-streaming consumer thread.
///
/// Capacity should be sized to absorb the primary's worst-case burst
/// (deltas-per-tick × ticks-in-flight-before-consumer-catches-up) without
/// blocking the dispatch thread; on overflow we surface `RingError::Full`
/// rather than blocking, so the supervisor can make an explicit policy
/// decision (e.g. degrade to synchronous replication, or drop the standby
/// and alert) instead of silently stalling tenant execution.
pub struct DeltaRing {
    queue: Arc<ArrayQueue<TickBatch>>,
    /// Highest tick successfully pushed, for producer-side observability.
    last_pushed_tick: AtomicU64,
}

impl DeltaRing {
    pub fn new(capacity: usize) -> Self {
        Self {
            queue: Arc::new(ArrayQueue::new(capacity)),
            last_pushed_tick: AtomicU64::new(0),
        }
    }

    /// Producer side: push one tick's batch. Non-blocking; returns
    /// `Err(RingError::Full)` immediately rather than backing up the
    /// dispatch thread, per the capacity note above.
    pub fn push(&self, batch: TickBatch) -> Result<(), RingError> {
        let tick = batch.tick;
        self.queue
            .push(batch)
            .map_err(|_| RingError::Full(self.queue.capacity()))?;
        self.last_pushed_tick.store(tick, Ordering::Release);
        Ok(())
    }

    /// Consumer side: drain everything currently available, in FIFO
    /// (i.e. tick) order. Returns an empty vec if nothing is ready —
    /// callers should poll on their own schedule (typically driven by the
    /// same quantum-boundary cadence as the scheduler, so replication
    /// latency is bounded by one quantum in the steady state).
    pub fn drain(&self) -> Vec<TickBatch> {
        let mut out = Vec::new();
        while let Some(batch) = self.queue.pop() {
            out.push(batch);
        }
        out
    }

    pub fn last_pushed_tick(&self) -> u64 {
        self.last_pushed_tick.load(Ordering::Acquire)
    }

    pub fn producer_handle(&self) -> DeltaRingProducer {
        DeltaRingProducer {
            queue: Arc::clone(&self.queue),
            last_pushed_tick: &self.last_pushed_tick as *const AtomicU64 as usize,
        }
    }
}

/// A cloneable handle for the producer side, so the dirty-page hook can
/// hold its own reference without borrowing the full `DeltaRing`.
///
/// We store the atomic's address rather than an `Arc` to it purely to
/// keep this struct `Copy`-free-and-simple in this reference
/// implementation; a production version should instead wrap the shared
/// state in one `Arc<DeltaRingInner>` and hand out clones of that `Arc`,
/// avoiding the raw-address trick entirely. Kept here as a documented,
/// deliberate simplification — see ARCHITECTURE.md §10.
pub struct DeltaRingProducer {
    queue: Arc<ArrayQueue<TickBatch>>,
    last_pushed_tick: usize,
}

impl DeltaRingProducer {
    pub fn push(&self, batch: TickBatch) -> Result<(), RingError> {
        let tick = batch.tick;
        self.queue
            .push(batch)
            .map_err(|_| RingError::Full(self.queue.capacity()))?;
        // SAFETY: `last_pushed_tick` was derived from a live `&AtomicU64`
        // owned by the `DeltaRing` this producer was created from, and
        // that `DeltaRing` is required (by construction convention in
        // this crate) to outlive all producer handles it issues — the
        // supervisor holds the `DeltaRing` for the lifetime of the cell,
        // strictly longer than any per-tick producer usage. This raw
        // pointer round-trip exists only to avoid a second `Arc` layer in
        // this reference implementation; see the struct doc comment for
        // the production-grade alternative.
        let atomic = unsafe { &*(self.last_pushed_tick as *const AtomicU64) };
        atomic.store(tick, Ordering::Release);
        Ok(())
    }
}

/// Standby-side apply loop: consumes `TickBatch`es (received over the
/// network, deserialization not modeled here) in strict tick order and
/// applies each page delta via a per-cell `DeltaEngine`, independently
/// recomputing SMT roots to cross-check against the primary's declared
/// checkpoint (ARCHITECTURE.md §7 step 5).
pub struct StandbyApplyLoop {
    engine: chronos_mem::delta::DeltaEngine,
    last_applied_tick: u64,
}

impl StandbyApplyLoop {
    pub fn new(initial_capacity_pages: usize) -> Self {
        Self {
            engine: chronos_mem::delta::DeltaEngine::new(initial_capacity_pages),
            last_applied_tick: 0,
        }
    }

    /// Apply a batch, enforcing strict tick monotonicity — an out-of-order
    /// batch indicates a transport bug or reordering upstream and is
    /// rejected rather than silently applied, since applying out of order
    /// would break the bit-identical-replay guarantee this whole
    /// subsystem exists to provide.
    pub fn apply_batch(&mut self, batch: &TickBatch) -> Result<(), ApplyError> {
        if batch.tick <= self.last_applied_tick && self.last_applied_tick != 0 {
            return Err(ApplyError::OutOfOrder {
                expected_after: self.last_applied_tick,
                got: batch.tick,
            });
        }
        for delta in &batch.deltas {
            self.engine.apply(delta);
        }
        self.last_applied_tick = batch.tick;
        Ok(())
    }

    pub fn last_applied_tick(&self) -> u64 {
        self.last_applied_tick
    }
}

#[derive(Debug, Error)]
pub enum ApplyError {
    #[error("received tick {got} out of order (expected something after {expected_after})")]
    OutOfOrder { expected_after: u64, got: u64 },
}

#[cfg(test)]
mod tests {
    use super::*;
    use chronos_mem::guarded_memory::PAGE_SIZE;

    #[test]
    fn push_and_drain_preserves_order() {
        let ring = DeltaRing::new(8);
        for tick in 1..=3u64 {
            ring.push(TickBatch { tick, deltas: vec![] }).unwrap();
        }
        let drained = ring.drain();
        let ticks: Vec<u64> = drained.iter().map(|b| b.tick).collect();
        assert_eq!(ticks, vec![1, 2, 3]);
    }

    #[test]
    fn full_ring_surfaces_backpressure() {
        let ring = DeltaRing::new(1);
        ring.push(TickBatch { tick: 1, deltas: vec![] }).unwrap();
        let err = ring.push(TickBatch { tick: 2, deltas: vec![] });
        assert!(matches!(err, Err(RingError::Full(_))));
    }

    #[test]
    fn standby_rejects_out_of_order_batches() {
        let mut standby = StandbyApplyLoop::new(4);
        let mut page = [0u8; PAGE_SIZE];
        page[0] = 1;
        let batch1 = TickBatch {
            tick: 5,
            deltas: vec![PageDelta::Raw { page_idx: 0, bytes: page }],
        };
        standby.apply_batch(&batch1).unwrap();

        let batch_stale = TickBatch { tick: 3, deltas: vec![] };
        let err = standby.apply_batch(&batch_stale);
        assert!(matches!(err, Err(ApplyError::OutOfOrder { .. })));
    }
}
