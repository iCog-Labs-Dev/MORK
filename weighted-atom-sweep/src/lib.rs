//! # Weighted Atom Sweep
//!
//! A Rust library for traversing and sampling atomic data structures with comprehensive tracing support.

pub mod cpq;
mod ecan_af_topk;
mod ecan_sti;
pub mod random_walk;
mod sweep;
pub mod traversal;
pub mod traversal_factory;

pub use ecan_af_topk::EcanAfTopK;
pub use sweep::{
    AtomCandidate, AtomPosition, ProcessId, SweepController, SweepProcess, WeightedAtomSweep,
    WeightedAtomSweepSettings, SweepMetrics,
};
pub use traversal::{TraversalEngine, TraversalError};
pub use traversal_factory::build_strategy;
