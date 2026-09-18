//! Non-determinism capture and replay (ARCHITECTURE.md §2, §3).
//!
//! Every host-call that would otherwise leak real-world non-determinism
//! into a cell (`clock_time_get`, `random_get`, and — indirectly — the
//! order in which io_uring completions become visible, handled instead by
//! `chronos_io::PendingCompletions`) is routed through an
//! `EventRecorder`, keyed by `(tick, call_ordinal)` rather than wall time,
//! so replay is host-independent.

use crate::clock::Tick;
use std::collections::HashMap;

/// Distinguishes the two operating modes of the recorder. A cell is
/// always in exactly one mode for its entire lifetime — switching mid-run
/// is not supported because it would let genuinely-fresh and
/// replayed-from-log values interleave in a way that breaks the
/// bit-identical guarantee.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecorderMode {
    Record,
    Replay,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct EventKey {
    tick: Tick,
    call_ordinal: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RecordedValue {
    Clock(u64),
    Random(Vec<u8>),
}

#[derive(Debug, thiserror::Error)]
pub enum ReplayError {
    #[error("no recorded value for tick={tick} ordinal={ordinal}; log is truncated or inputs diverged")]
    MissingEntry { tick: Tick, ordinal: u64 },
    #[error("recorded value type mismatch at tick={tick} ordinal={ordinal}")]
    TypeMismatch { tick: Tick, ordinal: u64 },
}

/// Per-cell recorder/replayer of non-deterministic host-call results.
pub struct EventRecorder {
    mode: RecorderMode,
    log: HashMap<EventKey, RecordedValue>,
    /// Per-tick counter of how many non-deterministic calls have been made
    /// so far, so repeated calls within the same tick get distinct
    /// ordinals. Reset implicitly whenever `tick` changes between calls
    /// (tracked via `current_tick`/`current_ordinal`).
    current_tick: Tick,
    current_ordinal: u64,
}

impl EventRecorder {
    pub fn new(mode: RecorderMode) -> Self {
        Self {
            mode,
            log: HashMap::new(),
            current_tick: 0,
            current_ordinal: 0,
        }
    }

    /// Construct a recorder pre-seeded with a previously captured log, for
    /// replay. The log format here is an in-memory map for clarity; a
    /// production system would deserialize this from the append-only
    /// on-disk/streamed log described in ARCHITECTURE.md §2.
    pub fn from_replay_log(entries: Vec<(Tick, u64, ReplayEntry)>) -> Self {
        let mut log = HashMap::new();
        for (tick, ordinal, entry) in entries {
            let key = EventKey { tick, call_ordinal: ordinal };
            let value = match entry {
                ReplayEntry::Clock(v) => RecordedValue::Clock(v),
                ReplayEntry::Random(v) => RecordedValue::Random(v),
            };
            log.insert(key, value);
        }
        Self {
            mode: RecorderMode::Replay,
            log,
            current_tick: 0,
            current_ordinal: 0,
        }
    }

    fn next_ordinal(&mut self, tick: Tick) -> u64 {
        if tick != self.current_tick {
            self.current_tick = tick;
            self.current_ordinal = 0;
        }
        let ordinal = self.current_ordinal;
        self.current_ordinal += 1;
        ordinal
    }

    /// Export the recorded log so it can be persisted/streamed to a
    /// standby or an auditor. Only meaningful in `Record` mode.
    pub fn export_log(&self) -> Vec<(Tick, u64, ReplayEntry)> {
        self.log
            .iter()
            .map(|(k, v)| {
                let entry = match v {
                    RecordedValue::Clock(c) => ReplayEntry::Clock(*c),
                    RecordedValue::Random(r) => ReplayEntry::Random(r.clone()),
                };
                (k.tick, k.call_ordinal, entry)
            })
            .collect()
    }
}

/// Plain-data variant of `RecordedValue` for log import/export across
/// crate boundaries without exposing the private enum.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplayEntry {
    Clock(u64),
    Random(Vec<u8>),
}

/// Host-call surface the supervisor's trampolines call into. Implemented
/// by `EventRecorder` for both record and replay modes — callers do not
/// need to branch on mode themselves.
pub trait NonDeterminismSource {
    fn wall_clock_now(
        &mut self,
        tick: Tick,
        real_now_fn: &mut dyn FnMut() -> u64,
    ) -> Result<u64, ReplayError>;

    fn random_bytes(
        &mut self,
        tick: Tick,
        buf: &mut [u8],
        real_random_fn: &mut dyn FnMut(&mut [u8]),
    ) -> Result<(), ReplayError>;
}

impl NonDeterminismSource for EventRecorder {
    fn wall_clock_now(
        &mut self,
        tick: Tick,
        real_now_fn: &mut dyn FnMut() -> u64,
    ) -> Result<u64, ReplayError> {
        let ordinal = self.next_ordinal(tick);
        let key = EventKey { tick, call_ordinal: ordinal };
        match self.mode {
            RecorderMode::Record => {
                let value = real_now_fn();
                self.log.insert(key, RecordedValue::Clock(value));
                Ok(value)
            }
            RecorderMode::Replay => match self.log.get(&key) {
                Some(RecordedValue::Clock(v)) => Ok(*v),
                Some(_) => Err(ReplayError::TypeMismatch { tick, ordinal }),
                None => Err(ReplayError::MissingEntry { tick, ordinal }),
            },
        }
    }

    fn random_bytes(
        &mut self,
        tick: Tick,
        buf: &mut [u8],
        real_random_fn: &mut dyn FnMut(&mut [u8]),
    ) -> Result<(), ReplayError> {
        let ordinal = self.next_ordinal(tick);
        let key = EventKey { tick, call_ordinal: ordinal };
        match self.mode {
            RecorderMode::Record => {
                real_random_fn(buf);
                self.log.insert(key, RecordedValue::Random(buf.to_vec()));
                Ok(())
            }
            RecorderMode::Replay => match self.log.get(&key) {
                Some(RecordedValue::Random(v)) => {
                    if v.len() != buf.len() {
                        return Err(ReplayError::TypeMismatch { tick, ordinal });
                    }
                    buf.copy_from_slice(v);
                    Ok(())
                }
                Some(_) => Err(ReplayError::TypeMismatch { tick, ordinal }),
                None => Err(ReplayError::MissingEntry { tick, ordinal }),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_then_replay_reproduces_clock_values() {
        let mut recorder = EventRecorder::new(RecorderMode::Record);
        let mut fake_time = 1000u64;
        let mut real_now = || {
            fake_time += 7;
            fake_time
        };
        let v1 = recorder.wall_clock_now(1, &mut real_now).unwrap();
        let v2 = recorder.wall_clock_now(1, &mut real_now).unwrap();
        assert_ne!(v1, v2);

        let log = recorder.export_log();
        let mut replay = EventRecorder::from_replay_log(log);
        // A "different host" real_now closure that would diverge if used —
        // replay must ignore it entirely.
        let mut divergent_now = || 999_999u64;
        let r1 = replay.wall_clock_now(1, &mut divergent_now).unwrap();
        let r2 = replay.wall_clock_now(1, &mut divergent_now).unwrap();
        assert_eq!((v1, v2), (r1, r2));
    }

    #[test]
    fn replay_missing_entry_is_a_hard_error_not_silent_fallback() {
        let mut replay = EventRecorder::from_replay_log(vec![]);
        let mut real_now = || 42u64;
        let err = replay.wall_clock_now(0, &mut real_now);
        assert!(matches!(err, Err(ReplayError::MissingEntry { .. })));
    }

    #[test]
    fn ordinals_reset_across_ticks() {
        let mut recorder = EventRecorder::new(RecorderMode::Record);
        let mut real_now = || 1u64;
        recorder.wall_clock_now(1, &mut real_now).unwrap();
        recorder.wall_clock_now(1, &mut real_now).unwrap();
        recorder.wall_clock_now(2, &mut real_now).unwrap();
        // tick 2's first call should reuse ordinal 0, i.e. not collide
        // with tick 1's entries (different tick in the key).
        assert_eq!(recorder.log.len(), 3);
    }
}
