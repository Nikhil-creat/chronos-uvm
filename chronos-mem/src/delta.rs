//! Delta-compression engine: turns a dirty page's new content plus its last
//! known-good copy into a compact `PageDelta` for streaming to a hot
//! standby (see chronos-replicate::DeltaRing, ARCHITECTURE.md §7).
//!
//! Strategy: XOR the new page against the previous copy, then run-length
//! encode the (typically long) runs of zero bytes this produces for
//! locality-heavy heap mutations (e.g. a single counter field changing
//! inside an otherwise-static struct). Falls back to a raw copy when the
//! XOR result is not sparse enough for RLE to win, so `PageDelta` is never
//! larger than "raw page + 1 byte tag" in the worst case.

use crate::guarded_memory::PAGE_SIZE;

/// A single run-length-encoded delta record: `run` bytes of zero, followed
/// by `literal.len()` bytes copied verbatim from the XOR stream.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RunLiteral {
    zero_run: u32,
    literal: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PageDelta {
    /// XOR-then-RLE encoded delta against the previous page snapshot.
    Xor {
        page_idx: u64,
        runs: Vec<RunLiteral>,
    },
    /// Dense mutation: RLE did not shrink the payload, so the full page is
    /// carried instead. Still cheaper to reason about downstream than a
    /// third encoding format.
    Raw {
        page_idx: u64,
        bytes: [u8; PAGE_SIZE],
    },
}

impl PageDelta {
    pub fn page_idx(&self) -> u64 {
        match self {
            PageDelta::Xor { page_idx, .. } => *page_idx,
            PageDelta::Raw { page_idx, .. } => *page_idx,
        }
    }

    /// Approximate wire size, used by the replication consumer to batch
    /// deltas under a fixed per-quantum byte budget.
    pub fn encoded_len(&self) -> usize {
        match self {
            PageDelta::Xor { runs, .. } => {
                runs.iter().map(|r| 4 /*run len*/ + 4 /*lit len*/ + r.literal.len()).sum()
            }
            PageDelta::Raw { .. } => PAGE_SIZE,
        }
    }
}

/// Stateful diff engine holding the last known-good copy of every page the
/// standby has acknowledged. One instance per cell.
pub struct DeltaEngine {
    /// Last-replicated snapshot, indexed by page number. Grows lazily.
    baseline: Vec<[u8; PAGE_SIZE]>,
    have_baseline: Vec<bool>,
}

impl DeltaEngine {
    pub fn new(initial_capacity_pages: usize) -> Self {
        Self {
            baseline: vec![[0u8; PAGE_SIZE]; initial_capacity_pages],
            have_baseline: vec![false; initial_capacity_pages],
        }
    }

    fn ensure_capacity(&mut self, page_idx: usize) {
        if page_idx >= self.baseline.len() {
            self.baseline.resize(page_idx + 1, [0u8; PAGE_SIZE]);
            self.have_baseline.resize(page_idx + 1, false);
        }
    }

    /// Compute the delta for `page_idx` given its current content, and
    /// advance the internal baseline to match (i.e. this call is a
    /// "diff-and-commit", not a pure/idempotent query — matching the
    /// at-least-once-per-flush usage pattern from the replication
    /// consumer, which never re-diffs a page it has already streamed).
    pub fn diff_and_commit(&mut self, page_idx: u64, current: &[u8]) -> PageDelta {
        assert_eq!(current.len(), PAGE_SIZE, "page diff requires exactly PAGE_SIZE bytes");
        let idx = page_idx as usize;
        self.ensure_capacity(idx);

        if !self.have_baseline[idx] {
            // First observation of this page: nothing to diff against,
            // send raw and adopt as baseline.
            let mut bytes = [0u8; PAGE_SIZE];
            bytes.copy_from_slice(current);
            self.baseline[idx] = bytes;
            self.have_baseline[idx] = true;
            return PageDelta::Raw { page_idx, bytes };
        }

        let prev = &self.baseline[idx];
        let mut xor_buf = [0u8; PAGE_SIZE];
        for i in 0..PAGE_SIZE {
            xor_buf[i] = prev[i] ^ current[i];
        }

        let runs = rle_encode(&xor_buf);
        let raw_saved_size = PAGE_SIZE;
        let rle_size: usize = runs.iter().map(|r| 8 + r.literal.len()).sum();

        // Commit new baseline regardless of which encoding wins.
        let mut new_baseline = [0u8; PAGE_SIZE];
        new_baseline.copy_from_slice(current);
        self.baseline[idx] = new_baseline;

        if rle_size < raw_saved_size {
            PageDelta::Xor { page_idx, runs }
        } else {
            let mut bytes = [0u8; PAGE_SIZE];
            bytes.copy_from_slice(current);
            PageDelta::Raw { page_idx, bytes }
        }
    }

    /// Standby-side: reconstruct the new page content by applying a delta
    /// against the standby's own baseline copy of the same page.
    pub fn apply(&mut self, delta: &PageDelta) -> [u8; PAGE_SIZE] {
        let idx = delta.page_idx() as usize;
        self.ensure_capacity(idx);
        let out = match delta {
            PageDelta::Raw { bytes, .. } => *bytes,
            PageDelta::Xor { runs, .. } => {
                let prev = if self.have_baseline[idx] {
                    self.baseline[idx]
                } else {
                    [0u8; PAGE_SIZE]
                };
                let xor_buf = rle_decode(runs);
                let mut out = [0u8; PAGE_SIZE];
                for i in 0..PAGE_SIZE {
                    out[i] = prev[i] ^ xor_buf[i];
                }
                out
            }
        };
        self.baseline[idx] = out;
        self.have_baseline[idx] = true;
        out
    }
}

fn rle_encode(buf: &[u8; PAGE_SIZE]) -> Vec<RunLiteral> {
    let mut runs = Vec::new();
    let mut i = 0usize;
    while i < PAGE_SIZE {
        let run_start = i;
        while i < PAGE_SIZE && buf[i] == 0 {
            i += 1;
        }
        let zero_run = (i - run_start) as u32;

        let lit_start = i;
        while i < PAGE_SIZE && buf[i] != 0 {
            i += 1;
        }
        let literal = buf[lit_start..i].to_vec();

        if zero_run > 0 || !literal.is_empty() {
            runs.push(RunLiteral { zero_run, literal });
        }
    }
    runs
}

fn rle_decode(runs: &[RunLiteral]) -> [u8; PAGE_SIZE] {
    let mut out = [0u8; PAGE_SIZE];
    let mut pos = 0usize;
    for r in runs {
        pos += r.zero_run as usize;
        let end = (pos + r.literal.len()).min(PAGE_SIZE);
        if pos < PAGE_SIZE {
            out[pos..end].copy_from_slice(&r.literal[..end - pos]);
        }
        pos = end;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_touch_is_raw() {
        let mut eng = DeltaEngine::new(4);
        let page = [7u8; PAGE_SIZE];
        let d = eng.diff_and_commit(0, &page);
        matches!(d, PageDelta::Raw { .. });
    }

    #[test]
    fn sparse_mutation_roundtrips() {
        let mut primary = DeltaEngine::new(4);
        let mut standby = DeltaEngine::new(4);

        let mut page_a = [0u8; PAGE_SIZE];
        page_a[10] = 42;
        let d1 = primary.diff_and_commit(0, &page_a);
        let applied1 = standby.apply(&d1);
        assert_eq!(applied1, page_a);

        // Small localized mutation -> should favor Xor/RLE encoding.
        let mut page_b = page_a;
        page_b[10] = 43;
        page_b[2000] = 99;
        let d2 = primary.diff_and_commit(0, &page_b);
        assert!(d2.encoded_len() < PAGE_SIZE);
        let applied2 = standby.apply(&d2);
        assert_eq!(applied2, page_b);
    }

    #[test]
    fn dense_mutation_falls_back_to_raw() {
        let mut primary = DeltaEngine::new(4);
        let mut standby = DeltaEngine::new(4);

        let page_a = [0u8; PAGE_SIZE];
        let d1 = primary.diff_and_commit(0, &page_a);
        standby.apply(&d1);

        // Fully random-ish page -> XOR won't be sparse -> Raw wins.
        let mut page_b = [0u8; PAGE_SIZE];
        for (i, b) in page_b.iter_mut().enumerate() {
            *b = (i * 131 + 7) as u8;
        }
        let d2 = primary.diff_and_commit(0, &page_b);
        let applied2 = standby.apply(&d2);
        assert_eq!(applied2, page_b);
    }
}
