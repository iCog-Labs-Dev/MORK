#![feature(gen_blocks)]
#![feature(coroutine_trait)]
#![feature(coroutines)]
#![feature(stmt_expr_attributes)]
#![feature(more_float_constants)]

pub mod space;
/// Shared CLI output helpers used by the run command and related status logs.
pub mod output;
mod sources;
mod sinks;
mod pure;

pub use sinks::WriteResourceRequest;
pub use sources::ResourceRequest;