use crate::cpq::ChunkedPQTraverse;
use crate::random_walk::RandomWalk;
use crate::traversal::TraversalEngine;

/// Build a traversal engine by key name.
///
/// Supported keys:
/// - `"random_walk"` — weighted random walk
/// - `"cpq"` — chunked priority queue traversal
pub fn build_strategy(key: &str) -> Option<Box<dyn TraversalEngine>> {
    match key {
        "random_walk" => Some(Box::new(RandomWalk)),
        "cpq" => Some(Box::new(ChunkedPQTraverse::new(4))),
        _ => None,
    }
}
