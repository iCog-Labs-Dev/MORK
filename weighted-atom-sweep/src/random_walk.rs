use crate::sweep::AtomPosition;
use crate::traversal::{TraversalError, TraversalEngine};
use pathmap::zipper::{ReadZipperTracked, ZipperMoving, Zipper, ZipperValues, ZipperAbsolutePath};

/// A weighted random walk traversal engine.
///
/// Samples atoms proportional to their weight using a random walk that
/// descends the trie weighted by stored `agg_w` at each step (O(1) per read).
#[derive(Clone, Default)]
pub struct RandomWalk;

impl TraversalEngine for RandomWalk {
    fn name(&self) -> &str {
        "random_walk"
    }

    fn next_atom(&self, mut z: ReadZipperTracked<u64>) -> Result<AtomPosition, TraversalError> {
        let total_w: u64 = z.agg_w();

        if total_w == 0 {
            return Ok(z.origin_path().to_vec());
        }

        let mut random_num: u64 = rand::random_range(0..total_w);

        loop {
            if let Some(val) = z.val() {
                let node_weight: u64 = *val;
                if random_num < node_weight {
                    return Ok(z.origin_path().to_vec());
                }
                random_num -= node_weight;
            }

            let mut found_child = false;
            for b in z.child_mask().iter() {
                z.descend_to_byte(b);
                let child_agg_w: u64 = z.agg_w();

                if random_num < child_agg_w {
                    found_child = true;
                    break;
                }
                random_num -= child_agg_w;
                z.ascend_byte();
            }

            if !found_child {
                break;
            }
        }

        Ok(z.origin_path().to_vec())
    }
}
