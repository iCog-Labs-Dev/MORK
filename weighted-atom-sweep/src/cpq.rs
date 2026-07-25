use crate::sweep::AtomPosition;
use crate::traversal::{TraversalError, TraversalEngine};
use pathmap::zipper::{ReadZipperTracked, ReadZipperUntracked, ZipperIteration, ZipperForking, ZipperAbsolutePath};
use std::sync::{Arc, Mutex};
use std::collections::BinaryHeap;
use std::cmp::Ordering;

/// A chunk with a path and an aggregate-weight score.
#[derive(Clone, Debug)]
struct AtomChunk {
    path: AtomPosition,
    score: u64,
}

impl Eq for AtomChunk {}
impl PartialEq for AtomChunk {
    fn eq(&self, other: &Self) -> bool {
        self.score == other.score && self.path == other.path
    }
}

impl Ord for AtomChunk {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score
            .cmp(&other.score)
            .then_with(|| other.path.cmp(&self.path))
    }
}
impl PartialOrd for AtomChunk {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// A chunked priority queue traversal engine.
///
/// Collects subtree chunks at a fixed depth, scores them by stored `agg_w`
/// (O(1) per chunk), and serves them from a max-heap. This biases sampling
/// toward heavier subtrees while maintaining spatial locality.
pub struct ChunkedPQTraverse {
    heap: Arc<Mutex<BinaryHeap<AtomChunk>>>,
    depth: usize,
}

impl ChunkedPQTraverse {
    /// Create a new traversal with the given chunk depth.
    pub fn new(depth: usize) -> Self {
        Self {
            heap: Arc::new(Mutex::new(BinaryHeap::new())),
            depth,
        }
    }

    /// Refresh the heap by clearing it and re-collecting chunks at `target_depth`.
    pub fn refresh(&self, z: &ReadZipperTracked<u64>) {
        let mut h = self.heap.lock().unwrap();
        h.clear();
        drop(h);

        let read_root = z.fork_read_zipper();
        self.collect_atoms_at_depth(read_root, self.depth, &self.heap);
    }

    /// Walk all k-length paths at the current position and push each chunk
    /// (with its agg_w score) onto the shared heap.
    fn collect_atoms_at_depth(
        &self,
        mut z: ReadZipperUntracked<u64>,
        target_depth: usize,
        heap: &Arc<Mutex<BinaryHeap<AtomChunk>>>,
    ) {
        if z.descend_first_k_path(target_depth) {
            loop {
                let score = z.agg_w();
                heap.lock().unwrap().push(AtomChunk {
                    path: z.origin_path().to_vec(),
                    score,
                });

                if !z.to_next_k_path(target_depth) {
                    break;
                }
            }
        }
    }
}

impl TraversalEngine for ChunkedPQTraverse {
    fn name(&self) -> &str {
        "pq"
    }

    fn next_atom(&self, z: ReadZipperTracked<u64>) -> Result<AtomPosition, TraversalError> {
        {
            let h = self.heap.lock().unwrap();
            if h.is_empty() {
                drop(h);
                let read_root = z.fork_read_zipper();
                self.collect_atoms_at_depth(read_root, self.depth, &self.heap);
            }
        }

        let mut h = self.heap.lock().unwrap();
        match h.pop() {
            Some(chunk) => Ok(chunk.path),
            None => {
                // Return error on empty trie to avoid returning root path and causing lock contention.
                Err(TraversalError {
                    message: "Trie is empty or has no atoms at specified depth".to_string(),
                })
            }
        }
    }
}
