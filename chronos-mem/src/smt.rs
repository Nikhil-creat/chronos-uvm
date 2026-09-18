//! Sparse Merkle Tree (SMT) over a cell's heap pages, used to produce a
//! cryptographic execution-integrity checkpoint every N ticks
//! (ARCHITECTURE.md §6). Only dirty subtrees are rehashed per checkpoint;
//! clean subtrees reuse cached hashes from the previous checkpoint.

use crate::dirty::DirtyPageSet;
use crate::guarded_memory::GuardedMemory;

pub const PAGE_SIZE: usize = crate::guarded_memory::PAGE_SIZE;

/// Depth of the tree = number of bits used to index a leaf. 32 bits of page
/// index covers up to 2^32 pages = 16 TiB of guest heap at 4 KiB pages,
/// comfortably beyond any realistic serverless tenant quota.
const TREE_DEPTH: u32 = 32;

/// The hash of an empty subtree at a given depth, precomputed bottom-up.
/// `default_hashes[0]` is the hash of an all-zero leaf; `default_hashes[d]`
/// is `H(default_hashes[d-1] || default_hashes[d-1])`.
fn default_hashes() -> [blake3::Hash; (TREE_DEPTH + 1) as usize] {
    let mut hashes = [blake3::hash(&[]); (TREE_DEPTH + 1) as usize];
    hashes[0] = blake3::hash(&[0u8; PAGE_SIZE]);
    for d in 1..=(TREE_DEPTH as usize) {
        let prev = hashes[d - 1];
        let mut hasher = blake3::Hasher::new();
        hasher.update(prev.as_bytes());
        hasher.update(prev.as_bytes());
        hashes[d] = hasher.finalize();
    }
    hashes
}

/// A Sparse Merkle Tree keyed by page index. Internal nodes that equal the
/// default (all-zero-subtree) hash are never materialized in `nodes` —
/// only the "frontier" of non-default nodes is stored, which keeps memory
/// proportional to touched pages rather than 2^32.
pub struct SparseMerkleTree {
    /// Cached leaf hashes for pages that have been hashed at least once.
    /// `None` entries are logically "default" (all-zero page).
    leaves: std::collections::HashMap<u64, blake3::Hash>,
    defaults: [blake3::Hash; (TREE_DEPTH + 1) as usize],
}

impl SparseMerkleTree {
    pub fn new() -> Self {
        Self {
            leaves: std::collections::HashMap::new(),
            defaults: default_hashes(),
        }
    }

    /// Update the cached leaf hash for a single page. Called for every
    /// dirty page at checkpoint time; O(1) amortized.
    pub fn update_leaf(&mut self, page_idx: u64, page_bytes: &[u8]) {
        let h = blake3::hash(page_bytes);
        self.leaves.insert(page_idx, h);
    }

    fn leaf_hash(&self, page_idx: u64) -> blake3::Hash {
        *self.leaves.get(&page_idx).unwrap_or(&self.defaults[0])
    }

    /// Recompute the full tree root by walking only the sparse set of
    /// touched leaves up to depth `TREE_DEPTH`. Cost is
    /// `O(touched_leaves * TREE_DEPTH)`, not `O(2^TREE_DEPTH)`, because at
    /// each level we only need the sibling hashes of nodes on the path
    /// from a touched leaf, and untouched siblings collapse to
    /// `defaults[level]`.
    ///
    /// This is a correctness-first reference implementation: for very
    /// large touched-set checkpoints a production system would maintain
    /// an incremental frontier structure instead of recomputing bottom-up
    /// on every checkpoint; see ARCHITECTURE.md §10.
    pub fn root(&self) -> blake3::Hash {
        if self.leaves.is_empty() {
            return self.defaults[TREE_DEPTH as usize];
        }

        // Level 0: map of index -> hash, seeded from touched leaves only.
        let mut level: std::collections::HashMap<u64, blake3::Hash> =
            self.leaves.iter().map(|(&idx, &h)| (idx, h)).collect();

        for depth in 0..TREE_DEPTH {
            let mut next: std::collections::HashMap<u64, blake3::Hash> =
                std::collections::HashMap::new();
            let default_child = self.defaults[depth as usize];

            for (&idx, _) in level.iter() {
                let parent = idx >> 1;
                if next.contains_key(&parent) {
                    continue;
                }
                let left_idx = parent << 1;
                let right_idx = left_idx + 1;
                let left = *level.get(&left_idx).unwrap_or(&default_child);
                let right = *level.get(&right_idx).unwrap_or(&default_child);
                let mut hasher = blake3::Hasher::new();
                hasher.update(left.as_bytes());
                hasher.update(right.as_bytes());
                next.insert(parent, hasher.finalize());
            }
            level = next;
        }

        level
            .into_values()
            .next()
            .unwrap_or(self.defaults[TREE_DEPTH as usize])
    }
}

impl Default for SparseMerkleTree {
    fn default() -> Self {
        Self::new()
    }
}

/// A single entry in the execution-integrity checkpoint chain: this
/// checkpoint's SMT root, the register/context digest, the tick at which
/// it was taken, and a chained hash linking it to the previous entry so
/// the whole sequence forms a tamper-evident log (ARCHITECTURE.md §6).
#[derive(Debug, Clone)]
pub struct CheckpointEntry {
    pub tick: u64,
    pub smt_root: blake3::Hash,
    pub register_digest: blake3::Hash,
    pub chain_hash: blake3::Hash,
}

/// Owns the SMT plus the append-only chain of checkpoint entries for one
/// cell.
pub struct CheckpointChain {
    tree: SparseMerkleTree,
    entries: Vec<CheckpointEntry>,
}

impl CheckpointChain {
    pub fn new() -> Self {
        Self {
            tree: SparseMerkleTree::new(),
            entries: Vec::new(),
        }
    }

    /// Take a checkpoint at `tick`: rehash only the pages present in
    /// `dirty`, fold in a caller-supplied register/context digest
    /// (typically `blake3::hash` over a serialized snapshot of the Wasm
    /// operand stack + program counter + tick), and append a new,
    /// chain-linked entry.
    ///
    /// `mem` is any `GuardedMemory` implementor — this function is
    /// isolation-mode-agnostic (ARCHITECTURE.md §4).
    pub fn checkpoint<M: GuardedMemory>(
        &mut self,
        tick: u64,
        mem: &M,
        dirty: &DirtyPageSet,
        register_digest: blake3::Hash,
    ) -> CheckpointEntry {
        for page_idx in dirty.iter_dirty() {
            self.tree.update_leaf(page_idx, mem.page_bytes(page_idx));
        }

        let smt_root = self.tree.root();

        let prev_chain_hash = self
            .entries
            .last()
            .map(|e| e.chain_hash)
            .unwrap_or_else(|| blake3::hash(b"chronos-uvm-genesis"));

        let mut hasher = blake3::Hasher::new();
        hasher.update(prev_chain_hash.as_bytes());
        hasher.update(smt_root.as_bytes());
        hasher.update(register_digest.as_bytes());
        hasher.update(&tick.to_le_bytes());
        let chain_hash = hasher.finalize();

        let entry = CheckpointEntry {
            tick,
            smt_root,
            register_digest,
            chain_hash,
        };
        self.entries.push(entry.clone());
        entry
    }

    /// Independently verify a claimed chain by replaying the same folding
    /// function over a caller-supplied sequence of (tick, smt_root,
    /// register_digest) triples — this is what an auditor holding only
    /// the recorded non-determinism event log and a re-executed trace
    /// would run to confirm the primary's declared checkpoints.
    pub fn verify_chain(
        claims: &[(u64, blake3::Hash, blake3::Hash)],
    ) -> Result<blake3::Hash, ChainVerifyError> {
        let mut chain_hash = blake3::hash(b"chronos-uvm-genesis");
        for &(tick, smt_root, register_digest) in claims {
            let mut hasher = blake3::Hasher::new();
            hasher.update(chain_hash.as_bytes());
            hasher.update(smt_root.as_bytes());
            hasher.update(register_digest.as_bytes());
            hasher.update(&tick.to_le_bytes());
            chain_hash = hasher.finalize();
        }
        Ok(chain_hash)
    }

    pub fn latest(&self) -> Option<&CheckpointEntry> {
        self.entries.last()
    }
}

impl Default for CheckpointChain {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ChainVerifyError {
    #[error("empty claim sequence")]
    Empty,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_tree_root_is_stable() {
        let t1 = SparseMerkleTree::new();
        let t2 = SparseMerkleTree::new();
        assert_eq!(t1.root(), t2.root());
    }

    #[test]
    fn touching_a_leaf_changes_the_root() {
        let mut t = SparseMerkleTree::new();
        let empty_root = t.root();
        t.update_leaf(1234, &[9u8; PAGE_SIZE]);
        let touched_root = t.root();
        assert_ne!(empty_root, touched_root);
    }

    #[test]
    fn same_touched_pages_same_root_across_instances() {
        let mut t1 = SparseMerkleTree::new();
        let mut t2 = SparseMerkleTree::new();
        for (idx, byte) in [(0u64, 1u8), (5, 2), (1_000_000, 3)] {
            t1.update_leaf(idx, &[byte; PAGE_SIZE]);
            t2.update_leaf(idx, &[byte; PAGE_SIZE]);
        }
        assert_eq!(t1.root(), t2.root());
    }

    #[test]
    fn checkpoint_chain_is_tamper_evident() {
        let reg_digest = blake3::hash(b"pc=0,sp=0");
        let mut tree = SparseMerkleTree::new();
        tree.update_leaf(0, &[1u8; PAGE_SIZE]);
        let root1 = tree.root();

        let claims_honest = vec![(1u64, root1, reg_digest)];
        let honest_hash = CheckpointChain::verify_chain(&claims_honest).unwrap();

        let tampered_digest = blake3::hash(b"pc=0,sp=1"); // attacker claims different register state
        let claims_tampered = vec![(1u64, root1, tampered_digest)];
        let tampered_hash = CheckpointChain::verify_chain(&claims_tampered).unwrap();

        assert_ne!(honest_hash, tampered_hash);
    }
}
