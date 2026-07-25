use crate::cpq::ChunkedPQTraverse;
use crate::operation::{Operation, TransformOp};
use crate::random_walk::RandomWalk;
use crate::sexpr_operation::{SExprOperation, TemplateEffect};
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

/// Build an operation by type name with arguments.
///
/// Supported types:
/// - `"decay"` — built-in importance-decay transform (no args); reads the sampled
///   atom's weight and writes back 90% via `set_val_w`, propagating agg_w.
/// - `"sexpr"` — an mm2 exec operation; `args[0]` = pattern, `args[1]` = template
pub fn build_operation(op_type: &str, args: &[&[u8]]) -> Option<Box<dyn TransformOp>> {
    match op_type {
        "decay" => Some(Box::new(Operation::decay())),
        "sexpr" => {
            if args.len() >= 2 {
                Some(Box::new(SExprOperation::exec(
                    "mork_rule",
                    args[0],
                    &[(args[1], TemplateEffect::Add)],
                )))
            } else {
                None
            }
        }
        "log-atom" => Some(Box::new(Operation::log_atom())),
        _ => None,
    }
}
