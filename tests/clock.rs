//! Coverage for the `Clock` capability and the streams/TTL surface that
//! consumes it.

use dynoxide::Database;
use dynoxide::storage_backend::{Clock, ManualClock, SystemClock};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[test]
fn system_clock_returns_value_close_to_wall_clock() {
    let clock = SystemClock;
    let from_clock = clock.now_unix_secs();
    let from_std = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let delta = from_clock.abs_diff(from_std);
    assert!(
        delta <= 1,
        "SystemClock drifted from wall clock by {delta}s"
    );
}

#[test]
fn manual_clock_returns_pinned_value() {
    let clock = ManualClock::new(1_700_000_000);
    assert_eq!(clock.now_unix_secs(), 1_700_000_000);
    assert!((clock.now_unix_secs_f64() - 1_700_000_000.0).abs() < f64::EPSILON);
}

#[test]
fn manual_clock_advances_via_tick() {
    let clock = ManualClock::new(1_700_000_000);
    clock.tick(Duration::from_secs(5));
    assert_eq!(clock.now_unix_secs(), 1_700_000_005);
}

#[test]
fn manual_clock_set_overrides_value() {
    let clock = ManualClock::new(0);
    clock.set(42);
    assert_eq!(clock.now_unix_secs(), 42);
}

#[test]
fn manual_clock_handles_subsecond_ticks() {
    let clock = ManualClock::new(1_700_000_000);
    clock.tick(Duration::from_millis(500));
    let v = clock.now_unix_secs_f64();
    assert!(
        (v - 1_700_000_000.5).abs() < 0.001,
        "expected ~1_700_000_000.5, got {v}"
    );
}

#[test]
fn arc_clock_handles_share_state() {
    let clock = ManualClock::new(100);
    let handle: Arc<dyn Clock> = clock.arc();
    clock.set(200);
    assert_eq!(handle.now_unix_secs(), 200);
}

#[test]
fn database_default_uses_system_clock() {
    // Smoke test: Database::memory() boots with the SystemClock default.
    // No assertion on the exact time — just that nothing panics on the path.
    let _db = Database::memory().expect("Database::memory should succeed");
}
