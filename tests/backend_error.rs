//! Coverage for `BackendError` and the `rusqlite::Error -> BackendError` mapping.

use dynoxide::BackendError;
use dynoxide::storage_backend::error::from_rusqlite;
use rusqlite::ErrorCode;
use rusqlite::ffi;

fn sqlite_failure(extended_code: i32, msg: Option<&str>) -> rusqlite::Error {
    rusqlite::Error::SqliteFailure(ffi::Error::new(extended_code), msg.map(str::to_owned))
}

#[test]
fn display_is_non_empty_for_every_variant() {
    let variants = [
        BackendError::NotADatabase,
        BackendError::Locked,
        BackendError::Constraint("violated".into()),
        BackendError::Io("disk full".into()),
        BackendError::Other("anything".into()),
    ];
    for v in variants {
        let s = v.to_string();
        assert!(!s.is_empty(), "Display for {v:?} produced empty string");
    }
}

#[test]
fn other_carries_a_non_empty_message() {
    let msg = "disk full";
    let err = BackendError::Other(msg.into());
    assert!(err.to_string().contains(msg));
}

#[test]
fn maps_not_a_database_code() {
    let err = sqlite_failure(ffi::SQLITE_NOTADB, None);
    assert_eq!(err.sqlite_error_code(), Some(ErrorCode::NotADatabase));
    let mapped = from_rusqlite(err);
    assert!(matches!(mapped, BackendError::NotADatabase));
}

#[test]
fn maps_database_busy_to_locked() {
    let err = sqlite_failure(ffi::SQLITE_BUSY, None);
    assert_eq!(err.sqlite_error_code(), Some(ErrorCode::DatabaseBusy));
    let mapped = from_rusqlite(err);
    assert!(matches!(mapped, BackendError::Locked));
}

#[test]
fn maps_database_locked_to_locked() {
    let err = sqlite_failure(ffi::SQLITE_LOCKED, None);
    assert_eq!(err.sqlite_error_code(), Some(ErrorCode::DatabaseLocked));
    let mapped = from_rusqlite(err);
    assert!(matches!(mapped, BackendError::Locked));
}

#[test]
fn maps_constraint_violation_with_message() {
    let err = sqlite_failure(ffi::SQLITE_CONSTRAINT, Some("UNIQUE constraint failed"));
    assert_eq!(
        err.sqlite_error_code(),
        Some(ErrorCode::ConstraintViolation)
    );
    let mapped = from_rusqlite(err);
    match mapped {
        BackendError::Constraint(msg) => assert_eq!(msg, "UNIQUE constraint failed"),
        other => panic!("expected Constraint, got {other:?}"),
    }
}

#[test]
fn maps_constraint_violation_without_message() {
    let err = sqlite_failure(ffi::SQLITE_CONSTRAINT, None);
    let mapped = from_rusqlite(err);
    match mapped {
        BackendError::Constraint(msg) => assert!(msg.is_empty()),
        other => panic!("expected Constraint, got {other:?}"),
    }
}

#[test]
fn maps_io_failure_with_message() {
    let err = sqlite_failure(ffi::SQLITE_IOERR, Some("disk full"));
    assert_eq!(err.sqlite_error_code(), Some(ErrorCode::SystemIoFailure));
    let mapped = from_rusqlite(err);
    match mapped {
        BackendError::Io(msg) => assert_eq!(msg, "disk full"),
        other => panic!("expected Io, got {other:?}"),
    }
}

#[test]
fn unmapped_rusqlite_variant_falls_through_to_other() {
    let err = rusqlite::Error::QueryReturnedNoRows;
    let original = err.to_string();
    let mapped = from_rusqlite(err);
    match mapped {
        BackendError::Other(msg) => {
            assert!(!msg.is_empty(), "Other carried empty message");
            assert_eq!(msg, original);
        }
        other => panic!("expected Other, got {other:?}"),
    }
}

#[test]
fn unmapped_sqlite_failure_code_falls_through_to_other() {
    let err = sqlite_failure(ffi::SQLITE_FULL, Some("database or disk is full"));
    let mapped = from_rusqlite(err);
    match mapped {
        BackendError::Other(msg) => assert!(!msg.is_empty()),
        other => panic!("expected Other for SQLITE_FULL, got {other:?}"),
    }
}
