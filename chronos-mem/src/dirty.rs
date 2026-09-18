//! Dirty-page tracking, shared by the SMT checkpoint pipeline (only rehash
//! dirty subtrees) and the replication pipeline (only stream dirty pages).

/// A capacity-bounded, page-granularity dirty bitmap. Backed by a `Vec<u64>`
/// bit-set rather than a `HashSet<u64>` because occupancy is typically dense
/// enough (a serverless workload touching a meaningful fraction of its own
/// heap) that a bitmap is both smaller and branch-predictor-friendly to scan.
pub struct DirtyPageSet {
    bits: Vec<u64>,
    capacity_pages: u64,
    /// Cached count for O(1) `len()`; kept in sync by `mark`/`clear_all`.
    count: u64,
}

impl DirtyPageSet {
    pub fn new(capacity_pages: u64) -> Self {
        let words = (capacity_pages as usize + 63) / 64;
        Self {
            bits: vec![0u64; words.max(1)],
            capacity_pages,
            count: 0,
        }
    }

    #[inline]
    pub fn mark(&mut self, page_idx: u64) {
        debug_assert!(page_idx < self.capacity_pages, "page index out of tracked range");
        let word = (page_idx / 64) as usize;
        let bit = page_idx % 64;
        let mask = 1u64 << bit;
        if self.bits[word] & mask == 0 {
            self.bits[word] |= mask;
            self.count += 1;
        }
    }

    #[inline]
    pub fn is_dirty(&self, page_idx: u64) -> bool {
        let word = (page_idx / 64) as usize;
        let bit = page_idx % 64;
        self.bits.get(word).map_or(false, |w| w & (1u64 << bit) != 0)
    }

    /// Iterate dirty page indices in ascending order without allocating.
    pub fn iter_dirty(&self) -> impl Iterator<Item = u64> + '_ {
        self.bits.iter().enumerate().flat_map(|(word_idx, &word)| {
            (0..64).filter_map(move |bit| {
                if word & (1u64 << bit) != 0 {
                    Some((word_idx as u64) * 64 + bit as u64)
                } else {
                    None
                }
            })
        })
    }

    pub fn len(&self) -> u64 {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Reset after a checkpoint/replication flush has consumed all dirty
    /// pages. Called exactly once per flush boundary by the caller that
    /// owns the flush (checkpoint pipeline or replication consumer) —
    /// never called speculatively, since clearing early would silently
    /// drop mutations from the integrity/HA guarantees.
    pub fn clear_all(&mut self) {
        for w in self.bits.iter_mut() {
            *w = 0;
        }
        self.count = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mark_and_iterate() {
        let mut d = DirtyPageSet::new(200);
        d.mark(0);
        d.mark(63);
        d.mark(64);
        d.mark(199);
        assert_eq!(d.len(), 4);
        let collected: Vec<u64> = d.iter_dirty().collect();
        assert_eq!(collected, vec![0, 63, 64, 199]);
    }

    #[test]
    fn double_mark_is_idempotent() {
        let mut d = DirtyPageSet::new(10);
        d.mark(3);
        d.mark(3);
        assert_eq!(d.len(), 1);
    }

    #[test]
    fn clear_resets_state() {
        let mut d = DirtyPageSet::new(10);
        d.mark(1);
        d.mark(5);
        d.clear_all();
        assert!(d.is_empty());
        assert!(!d.is_dirty(1));
    }
}
