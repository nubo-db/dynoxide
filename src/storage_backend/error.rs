//! Backend-neutral error type returned by the [`StorageBackend`] trait.
//!
//! `BackendError` is the trait surface's error type. The native rusqlite-backed
//! `Storage` impl converts `rusqlite::Error` into `BackendError` via
//! [`from_rusqlite`], keeping the trait surface free of rusqlite types.
//!
//! `DynoxideError` is unchanged. Action handlers that call rusqlite directly
//! through `Storage::conn()` continue to surface `DynoxideError::SqliteError`.
//!
//! [`StorageBackend`]: super::StorageBackend
//! [`from_rusqlite`]: from_rusqlite

/// Backend-neutral error variants surfaced by the [`StorageBackend`] trait.
///
/// [`StorageBackend`]: super::StorageBackend
#[derive(Debug, thiserror::Error)]
pub enum BackendError {
    /// The opened file is not a valid SQLite database, or is encrypted with the
    /// wrong key.
    #[error("backend: not a valid database")]
    NotADatabase,

    /// The database or a table within it is locked or busy.
    #[error("backend: database is locked or busy")]
    Locked,

    /// A backend-level constraint (uniqueness, check, foreign key) was violated.
    #[error("backend: constraint violation: {0}")]
    Constraint(String),

    /// An I/O error from the backend.
    #[error("backend: I/O error: {0}")]
    Io(String),

    /// Any other backend failure. Carries the original error's `Display` output.
    #[error("backend: {0}")]
    Other(String),
}

/// Convert a [`rusqlite::Error`] to a [`BackendError`].
///
/// The mapping covers the SQLite error codes the native rusqlite impl expects
/// to surface across the trait. Anything not explicitly mapped falls through
/// to [`BackendError::Other`] carrying the original error's `Display` output,
/// so no rusqlite variant produces an empty backend error.
///
/// This is a named helper rather than a `From` impl on purpose: the
/// `?`-conversion would otherwise silently turn rusqlite errors into
/// `BackendError` in code that should keep them rusqlite-typed (action handlers
/// using `Storage::conn()` directly).
pub fn from_rusqlite(err: rusqlite::Error) -> BackendError {
    use rusqlite::Error::SqliteFailure;
    use rusqlite::ErrorCode;

    match &err {
        SqliteFailure(ffi_err, msg) => match ffi_err.code {
            ErrorCode::NotADatabase => BackendError::NotADatabase,
            ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked => BackendError::Locked,
            ErrorCode::ConstraintViolation => {
                BackendError::Constraint(msg.clone().unwrap_or_default())
            }
            ErrorCode::SystemIoFailure => BackendError::Io(msg.clone().unwrap_or_default()),
            _ => BackendError::Other(err.to_string()),
        },
        _ => BackendError::Other(err.to_string()),
    }
}
