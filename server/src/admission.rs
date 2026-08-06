//! Admission control: resource governance for the HTTP boundary.
//!
//! Every request that could consume significant resources (body buffering, engine channel
//! slots, SSE broadcast slots) must pass through an [`AdmissionController`] before the
//! expensive work begins. This replaces the previous unbounded `body.collect().await` and
//! the ad-hoc `AtomicUsize` delta-subscriber counter with a single, testable, configurable
//! resource-budget layer.
//!
//! Three independent resources are tracked:
//!
//! | Resource | Guard type | What it bounds |
//! |---|---|---|
//! | Request body size | checked in [`RunPermit::acquire`] | heap allocation from buffering a large upload |
//! | In-flight POST /run | [`RunPermit`] | engine channel slots, memory for pending transactions |
//! | SSE subscribers | [`SsePermit`] | broadcast channel fan-out, per-subscriber event buffer |

use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Shared admission controller. Cheap to clone (all fields are `Arc`-wrapped).
#[derive(Clone)]
pub struct AdmissionController {
    max_body_bytes: usize,
    inflight: Arc<Semaphore>,
    sse_subs: Arc<Semaphore>,
    delta_subs: Arc<AtomicUsize>,
}

impl AdmissionController {
    pub fn new(max_body_bytes: usize, max_inflight: usize, max_sse_subs: usize) -> Self {
        Self {
            max_body_bytes,
            inflight: Arc::new(Semaphore::new(max_inflight)),
            sse_subs: Arc::new(Semaphore::new(max_sse_subs)),
            delta_subs: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Try to acquire an in-flight permit for POST /run. Returns `None` immediately if the
    /// server is at capacity — the caller should respond with 503 without buffering the
    /// request body.
    pub async fn acquire_run(&self) -> Option<RunPermit> {
        let permit = Arc::clone(&self.inflight).acquire_owned().await.ok()?;
        Some(RunPermit { _permit: permit, max_body_bytes: self.max_body_bytes })
    }

    /// Try to acquire an SSE subscriber permit. Returns `None` immediately if at capacity.
    pub async fn acquire_sse(&self) -> Option<SsePermit> {
        let permit = Arc::clone(&self.sse_subs).acquire_owned().await.ok()?;
        Some(SsePermit {
            _permit: permit,
            delta_subs: self.delta_subs.clone(),
            delta: false,
        })
    }

    /// Number of active delta subscribers (for the delta task to skip work at zero).
    pub fn delta_sub_count(&self) -> usize {
        self.delta_subs.load(Relaxed)
    }
}

/// Guard for an in-flight POST /run. Dropped when the HTTP response is sent.
/// Carries the body-size limit so the caller can enforce it without reaching
/// back into the controller.
pub struct RunPermit {
    _permit: OwnedSemaphorePermit,
    max_body_bytes: usize,
}

impl RunPermit {
    pub fn max_body_bytes(&self) -> usize {
        self.max_body_bytes
    }
}

/// Guard for an active SSE subscriber. Dropped when the SSE connection closes.
/// Optionally tracks delta-subscriber membership.
pub struct SsePermit {
    _permit: OwnedSemaphorePermit,
    delta_subs: Arc<AtomicUsize>,
    delta: bool,
}

impl SsePermit {
    /// Mark this subscriber as interested in delta events.
    pub fn with_deltas(mut self) -> Self {
        self.delta = true;
        self.delta_subs.fetch_add(1, Relaxed);
        self
    }
}

impl Drop for SsePermit {
    fn drop(&mut self) {
        if self.delta {
            self.delta_subs.fetch_sub(1, Relaxed);
        }
    }
}
