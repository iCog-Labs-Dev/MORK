//! # Weighted Atom Sweep
//!
//! A Rust library for traversing and sampling atomic data structures with comprehensive tracing support.

pub mod cpq;
pub mod random_walk;
mod sweep;
pub mod traversal;
pub mod traversal_factory;

pub use sweep::{
    AtomCandidate, AtomPosition, ProcessId, SweepController, SweepProcess, WeightedAtomSweep,
    WeightedAtomSweepSettings,
};
pub use traversal::{TraversalEngine, TraversalError};
pub use traversal_factory::build_strategy;
