use crate::sweep::AtomPosition;
use pathmap::zipper::ReadZipperTracked;
use pathmap::morphisms::Catamorphism;
use std::error::Error;
use core::convert::Infallible;

/// Error returned when traversal fails.
#[derive(Debug)]
pub struct TraversalError {
    pub message: String,
}

impl std::fmt::Display for TraversalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "traversal error: {}", self.message)
    }
}
impl Error for TraversalError {}

/// Trait for traversal engines that sample atoms from a PathMap trie.
///
/// Each engine implements a strategy for selecting the next atom to process.
/// Implementations must be `Send + Sync + 'static` so they can be moved
/// into background sweep threads.
pub trait TraversalEngine: Send + Sync + 'static {
    /// Returns the name of this traversal engine, used for tracing and identification.
    fn name(&self) -> &str;
    /// Sample the next atom from the trie and return its path.
    fn next_atom(&self, z: ReadZipperTracked<u64>) -> Result<AtomPosition, TraversalError>;
}

/// Full catamorphism over the subtrie — O(subtree size).
///
/// Aggregates all values in the subtrie by summing them from leaves to root.
/// Use as a test oracle to validate that stored `agg_w` values are correct.
/// In production, prefer `zipper.agg_w()` which reads the O(1) stored field.
///
/// # Oracle contract
///
/// For any trie mutated exclusively via `set_val_w` / `remove_val_w`,
/// `node_agg_w(z)` MUST equal `z.agg_w()` at every position. The
/// `agg_w_parity` test enforces this invariant.
pub fn node_agg_w<Z: Catamorphism<u64>>(path: Z) -> Result<u64, TraversalError> {
    node_agg_w_fallible(path)
        .map_err(|_| TraversalError {
            message: "aggregation failed".to_string(),
        })
}

/// Infallible variant of `node_agg_w`. Returns `Ok(u64)` always.
/// Useful in contexts where the error type is constrained (e.g. iterator adapters).
pub fn node_agg_w_fallible<Z: Catamorphism<u64>>(path: Z) -> Result<u64, Infallible> {
    path.into_cata_jumping_side_effect_fallible(
        |_mask, children: &mut [u64], _size, maybe_v: Option<&u64>, _path| {
            let from_children = children.iter().copied().sum::<u64>();
            let here: u64 = maybe_v.copied().unwrap_or(0);
            Ok(here + from_children)
        },
    )
}
