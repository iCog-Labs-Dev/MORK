use crate::cpq::ChunkedPQTraverse;
use crate::ecan_af_topk::EcanAfTopK;
use crate::random_walk::RandomWalk;
use crate::traversal::TraversalEngine;

/// Build a traversal engine by key name.
///
/// Supported keys:
/// - `"random_walk"` — weighted random walk
/// - `"cpq"` — chunked priority queue traversal
/// - `"ecan_af_topk"` — exact deterministic semantic-STI attentional focus
pub fn build_strategy(key: &str) -> Option<Box<dyn TraversalEngine>> {
    match key {
        "random_walk" => Some(Box::new(RandomWalk)),
        "cpq" => Some(Box::new(ChunkedPQTraverse::new(4))),
        "ecan_af_topk" => Some(Box::new(EcanAfTopK::new())),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registers_ecan_af_topk_without_changing_existing_keys() {
        assert_eq!(
            build_strategy("ecan_af_topk").unwrap().name(),
            "ecan_af_topk"
        );
        assert_eq!(build_strategy("random_walk").unwrap().name(), "random_walk");
        assert_eq!(build_strategy("cpq").unwrap().name(), "pq");
        assert!(build_strategy("unknown").is_none());
    }
}
