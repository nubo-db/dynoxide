//! `Clock` capability used by the trait surface to abstract time access.
//!
//! Clock injection is scoped to the call sites that the
//! [`StorageBackend`](super::StorageBackend) trait surfaces, namely the
//! stream and TTL paths in [`crate::streams`] and [`crate::ttl`]. The other
//! wall-clock call sites that compile on every target (the idempotency cache
//! and the action-handler `created_at` stamps) read time through `web_time`,
//! which sources the browser clock on wasm and `std::time` everywhere else.
//! Snapshot epoch helpers stay on `std::time` because they sit inside
//! native-only code the trait does not expose.

use std::sync::Arc;
use web_time::{SystemTime, UNIX_EPOCH};

/// Provides wall-clock time to the trait's stream and TTL paths.
///
/// Implementations must be `Send + Sync` so a single shared `Arc<dyn Clock>`
/// can sit inside [`crate::storage::Storage`].
pub trait Clock: Send + Sync {
    /// Whole seconds since the Unix epoch.
    fn now_unix_secs(&self) -> u64;

    /// Fractional seconds since the Unix epoch, retaining sub-second precision
    /// for callers that need it (e.g., `cached_at` in import flows that route
    /// through stream events).
    fn now_unix_secs_f64(&self) -> f64;
}

/// Production clock backed by [`web_time::SystemTime`] (`std::time` on native).
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl SystemClock {
    /// Return a shareable [`Arc<dyn Clock>`] handle to a `SystemClock`.
    pub fn arc() -> Arc<dyn Clock> {
        Arc::new(Self)
    }
}

impl Clock for SystemClock {
    fn now_unix_secs(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    fn now_unix_secs_f64(&self) -> f64 {
        let d = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        d.as_secs_f64()
    }
}

/// Clock whose value is set explicitly. Intended for tests; carrying it in
/// release builds costs only the size of the type itself, and it lets
/// integration tests outside `src/` reach `ManualClock` without juggling
/// feature flags.
///
/// Use [`ManualClock::new`] to start at a specific epoch, then [`ManualClock::set`]
/// or [`ManualClock::tick`] to advance time deterministically inside tests.
#[derive(Debug, Default, Clone)]
pub struct ManualClock {
    inner: Arc<std::sync::Mutex<f64>>,
}

impl ManualClock {
    /// Construct a `ManualClock` pinned at `secs` epoch seconds.
    pub fn new(secs: u64) -> Self {
        Self {
            inner: Arc::new(std::sync::Mutex::new(secs as f64)),
        }
    }

    /// Set the current time to exactly `secs` epoch seconds.
    pub fn set(&self, secs: u64) {
        if let Ok(mut guard) = self.inner.lock() {
            *guard = secs as f64;
        }
    }

    /// Advance the clock by `delta`.
    pub fn tick(&self, delta: std::time::Duration) {
        if let Ok(mut guard) = self.inner.lock() {
            *guard += delta.as_secs_f64();
        }
    }

    /// Return a shareable `Arc<dyn Clock>` handle to this `ManualClock`.
    /// The handle stays in sync with the original via the shared inner state.
    pub fn arc(&self) -> Arc<dyn Clock> {
        Arc::new(self.clone())
    }
}

impl Clock for ManualClock {
    fn now_unix_secs(&self) -> u64 {
        self.inner.lock().map(|v| *v as u64).unwrap_or_default()
    }

    fn now_unix_secs_f64(&self) -> f64 {
        self.inner.lock().map(|v| *v).unwrap_or_default()
    }
}
