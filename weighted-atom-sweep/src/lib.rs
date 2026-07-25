//! # Weighted Atom Sweep
//!
//! A Rust library for traversing and transforming atomic data structures with comprehensive tracing support.
//!
//! ## Architecture
//!
//! The core abstraction is [`WeightedAtomSweep`], which orchestrates multiple
//! [`SweepProcess`] instances. Each process pairs a [`TraversalEngine`] (which
//! samples atoms from a PathMap trie) with a set of operations (implementing
//! [`TransformOp`]) that modify the subtrie at each sampled position.
//!
//! Operations receive a [`WriteZipperTracked`](pathmap::zipper::WriteZipperTracked)
//! focused at the sampled atom's position. The write zipper is scoped — operations
//! can navigate and modify everything at and below the focus, but cannot ascend
//! above it.
//!
//! ## Operation Types
//!
//! Two concrete operation types are provided:
//!
//! - [`Operation`] — a stateless function pointer for simple transforms
//! - [`SExprOperation`] — an mm2 exec operation that pattern-matches against
//!   the subtrie and instantiates templates with variable bindings
//!
//! Both implement the [`TransformOp`] trait and can be mixed freely within a
//! single [`SweepProcess`].
//!
//! ## mm2 Exec Operations
//!
//! The [`SExprOperation`] type enables MORK-style exec operations within the
//! sweep framework. An exec operation carries a **pattern** and a list of
//! **(template, effect)** pairs:
//!
//! 1. The pattern is walked against the trie structure using mm2 tag encoding.
//!    Concrete elements (symbols, arities) must match exactly. Variables
//!    (`NewVar` as wildcards, `VarRef` for co-referential bindings) match
//!    flexibly.
//! 2. For each match, mork-expr's `extract_data` extracts variable bindings.
//! 3. Each template is instantiated via `substitute` with those bindings.
//! 4. The [`TemplateEffect`] determines the action:
//!    - **Add**: Insert the instantiated template as a trie path
//!    - **Remove**: Delete the instantiated template's trie path
//!
//! # Tracing Instrumentation
//!
//! This crate includes comprehensive tracing support via the `tracing` crate. The instrumentation
//! is structured hierarchically to provide visibility at multiple levels:
//!
//! ## Quick Start
//!
//! To enable and view tracing output in your application, add `tracing-subscriber` as a dev dependency
//! and initialize a subscriber:
//!
//! ```ignore
//! use tracing_subscriber::fmt;
//!
//! fn main() {
//!     // Initialize the tracing subscriber with default settings
//!     fmt::init();
//!
//!     // Your code here - all tracing output will be captured
//! }
//! ```
//!
//! Or for more control over filtering:
//!
//! ```ignore
//! use tracing_subscriber::EnvFilter;
//!
//! fn main() {
//!     // Set RUST_LOG=debug (or trace) before running your application
//!     tracing_subscriber::fmt()
//!         .with_env_filter(EnvFilter::from_default_env())
//!         .init();
//! }
//! ```
//!
//! ## Creating Custom Operations
//!
//! Operations implement the [`TransformOp`] trait:
//!
//! ```ignore
//! use tracing::{instrument, debug};
//! use pathmap::zipper::WriteZipperTracked;
//! use weighted_atom_sweep::TransformOp;
//!
//! struct MyCustomOp;
//!
//! impl TransformOp for MyCustomOp {
//!     fn name(&self) -> &str { "my_custom_operation" }
//!     fn apply(&self, wz: &mut WriteZipperTracked<u64>, _atom_path: &[u8]) {
//!         debug!("starting custom transformation");
//!         // Navigate and modify the subtrie via wz
//!         debug!("transformation completed");
//!     }
//! }
//! ```
//!
//! Or use the simple function pointer form:
//!
//! ```ignore
//! use weighted_atom_sweep::Operation;
//! use pathmap::zipper::WriteZipperTracked;
//!
//! fn my_transform(wz: &mut WriteZipperTracked<u64>, _atom_path: &[u8]) {
//!     // ... transformation logic ...
//! }
//!
//! let op = Operation::new("my_operation", my_transform);
//! ```

pub mod new_eng_op;
pub mod random_walk;
pub mod cpq;
mod map;
mod operation;
pub mod sexpr_operation;
mod sweep;
pub mod traversal;

pub use new_eng_op::{build_operation, build_strategy};
pub use operation::{Operation, OperationObserver, TransformOp};
pub use sexpr_operation::{SExprOperation, TemplateEffect};
pub use sweep::WeightedAtomSweep;
pub use sweep::*;
pub use traversal::{TraversalEngine, TraversalError};
