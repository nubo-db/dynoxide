//! Snapshot management for the MCP server.
//!
//! Provides create, restore, list, and delete operations for database snapshots.
//! Auto-snapshot before destructive operations (e.g. `delete_table`).
//!
//! Two storage backends:
//! - **File-backed databases**: snapshots stored as `.db` files in
//!   `<db_parent>/.dynoxide/snapshots/`.
//! - **In-memory databases**: snapshots held as in-memory SQLite connections.
//!   Same lifecycle as the database — die with the process, no filesystem
//!   side-effects.

use crate::Database;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{LazyLock, Mutex};

/// Maximum number of auto-snapshots to keep.
const MAX_AUTO_SNAPSHOTS: usize = 10;

/// Maximum total snapshots (auto + manual) before oldest are evicted.
/// Eviction prefers auto-snapshots first, then oldest manual snapshots.
const MAX_TOTAL_SNAPSHOTS: usize = 20;

// ---------------------------------------------------------------------------
// In-memory snapshot store
// ---------------------------------------------------------------------------

/// A snapshot stored as an in-memory SQLite connection.
struct InMemorySnapshot {
    conn: rusqlite::Connection,
    size_bytes: u64,
    created_epoch: u64,
    is_auto: bool,
}

static IN_MEMORY_SNAPSHOTS: LazyLock<Mutex<HashMap<String, InMemorySnapshot>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Check whether a database is in-memory (no file path).
fn is_in_memory(db: &Database) -> bool {
    db.db_path().ok().flatten().is_none()
}

/// Current epoch seconds.
fn epoch_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Validate a snapshot name to prevent path traversal attacks.
///
/// Rejects names containing `/`, `\`, `..`, or null bytes.
fn validate_snapshot_name(name: &str) -> crate::errors::Result<()> {
    if name.is_empty() {
        return Err(crate::errors::DynoxideError::ValidationException(
            "Snapshot name must not be empty".to_string(),
        ));
    }
    if name.contains('/') || name.contains('\\') || name.contains("..") || name.contains('\0') {
        return Err(crate::errors::DynoxideError::ValidationException(
            "Snapshot name must not contain '/', '\\', '..', or null bytes".to_string(),
        ));
    }
    Ok(())
}

/// Resolve a validated snapshot name to its full filesystem path.
///
/// Validates the name, resolves the snapshot directory, and appends `.db`
/// if needed. Single place for name-to-path resolution (#096).
fn resolve_snapshot_path(db: &Database, name: &str) -> crate::errors::Result<PathBuf> {
    validate_snapshot_name(name)?;
    let dir = snapshot_dir(db).map_err(|e| {
        crate::errors::DynoxideError::InternalServerError(format!(
            "Failed to resolve snapshot directory: {e}"
        ))
    })?;
    let filename = if name.ends_with(".db") {
        name.to_string()
    } else {
        format!("{name}.db")
    };
    Ok(dir.join(&filename))
}

/// Strip the `.db` extension from a snapshot name if present.
fn normalize_name(name: &str) -> &str {
    name.strip_suffix(".db").unwrap_or(name)
}

/// Resolve the snapshot directory for a file-backed database.
///
/// Returns `<db_parent>/.dynoxide/snapshots/`.
/// Errors for in-memory databases (they use the in-memory store).
pub fn snapshot_dir(db: &Database) -> std::io::Result<PathBuf> {
    match db.db_path().ok().flatten() {
        Some(p) => {
            let path = PathBuf::from(&p);
            let base = path.parent().unwrap_or(Path::new(".")).to_path_buf();
            Ok(base.join(".dynoxide").join("snapshots"))
        }
        None => Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "In-memory databases use in-memory snapshots, not filesystem snapshots",
        )),
    }
}

/// Compact RFC 3339 timestamp: `20260308T143022Z`
fn compact_timestamp() -> String {
    let now = epoch_secs();
    let days = now / 86400;
    let time_of_day = now % 86400;
    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;

    // Civil date from days since epoch (algorithm from Howard Hinnant)
    let z = days as i64 + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };

    format!("{y:04}{m:02}{d:02}T{hours:02}{minutes:02}{seconds:02}Z")
}

// ---------------------------------------------------------------------------
// Drop guard for partial snapshot file cleanup (#092)
// ---------------------------------------------------------------------------

/// RAII guard that deletes a snapshot file unless explicitly committed.
///
/// Ensures partial files are cleaned up even on panic during VACUUM INTO.
struct SnapshotFileGuard {
    path: PathBuf,
    committed: bool,
}

impl SnapshotFileGuard {
    fn new(path: PathBuf) -> Self {
        Self {
            path,
            committed: false,
        }
    }

    fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for SnapshotFileGuard {
    fn drop(&mut self) {
        if !self.committed {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

// ---------------------------------------------------------------------------
// Disk space pre-check (#092)
// ---------------------------------------------------------------------------

/// Best-effort check for available disk space before creating a snapshot.
///
/// On Unix, uses `statvfs` to verify sufficient space. On other platforms,
/// this is a no-op. The check is inherently racy but useful as a fast-fail.
fn check_disk_space(dir: &Path, db_size: u64) -> crate::errors::Result<()> {
    #[cfg(unix)]
    {
        let dir_str = dir.to_str().unwrap_or(".");
        if let Ok(c_path) = std::ffi::CString::new(dir_str) {
            unsafe {
                let mut stat: libc::statvfs = std::mem::zeroed();
                if libc::statvfs(c_path.as_ptr(), &mut stat) == 0 {
                    #[allow(clippy::unnecessary_cast)] // u32 on macOS, u64 on Linux
                    let available = stat.f_bavail as u64 * stat.f_frsize as u64;
                    let required = db_size + (db_size / 10).max(1024 * 1024);
                    if available < required {
                        return Err(crate::errors::DynoxideError::InternalServerError(format!(
                            "Insufficient disk space for snapshot: {available} bytes available, \
                                 ~{required} bytes required (database is {db_size} bytes)"
                        )));
                    }
                }
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (dir, db_size);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Create
// ---------------------------------------------------------------------------

/// Create a snapshot of the database.
///
/// For file-backed databases, creates a `.db` file via VACUUM INTO.
/// For in-memory databases, creates a backup connection held in process memory.
///
/// Enforces a global snapshot limit — when the limit is reached, auto-snapshots
/// are evicted first, then the oldest manual snapshots.
pub fn create_snapshot(db: &Database, name: Option<&str>) -> crate::errors::Result<SnapshotInfo> {
    let snapshot_name = match name {
        Some(n) => {
            validate_snapshot_name(n)?;
            n.to_string()
        }
        None => format!("snapshot-{}", compact_timestamp()),
    };

    if is_in_memory(db) {
        create_snapshot_in_memory(db, &snapshot_name, false)
    } else {
        create_snapshot_on_disk(db, &snapshot_name, false)
    }
}

/// Create a file-backed snapshot with disk space check and Drop guard cleanup.
fn create_snapshot_on_disk(
    db: &Database,
    name: &str,
    is_auto: bool,
) -> crate::errors::Result<SnapshotInfo> {
    let dir = snapshot_dir(db).map_err(|e| {
        crate::errors::DynoxideError::InternalServerError(format!(
            "Failed to resolve snapshot directory: {e}"
        ))
    })?;
    std::fs::create_dir_all(&dir).map_err(|e| {
        crate::errors::DynoxideError::InternalServerError(format!(
            "Failed to create snapshot directory: {e}"
        ))
    })?;

    // Pre-check available disk space (#092)
    let db_size = db.db_size_bytes().unwrap_or(0);
    check_disk_space(&dir, db_size)?;

    // Enforce global snapshot limit before creating
    prune_file_snapshots(&dir);

    let filename = format!("{name}.db");
    let path = dir.join(&filename);

    // Drop guard ensures partial files are cleaned up on failure or panic
    let guard = SnapshotFileGuard::new(path.clone());
    db.vacuum_into(path.to_str().unwrap_or(&filename))?;
    guard.commit();

    let size_bytes = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);

    Ok(SnapshotInfo {
        name: name.to_string(),
        size_bytes,
        created_epoch: Some(epoch_secs()),
        is_auto,
    })
}

/// Create an in-memory snapshot using SQLite's backup API.
fn create_snapshot_in_memory(
    db: &Database,
    name: &str,
    is_auto: bool,
) -> crate::errors::Result<SnapshotInfo> {
    let conn = db.backup_to_memory()?;
    let size_bytes = crate::storage::Storage::connection_size_bytes(&conn)?;
    let created_epoch = epoch_secs();

    let mut store = IN_MEMORY_SNAPSHOTS.lock().unwrap();
    prune_in_memory_store(&mut store);

    store.insert(
        name.to_string(),
        InMemorySnapshot {
            conn,
            size_bytes,
            created_epoch,
            is_auto,
        },
    );

    Ok(SnapshotInfo {
        name: name.to_string(),
        size_bytes,
        created_epoch: Some(created_epoch),
        is_auto,
    })
}

// ---------------------------------------------------------------------------
// Auto-snapshot
// ---------------------------------------------------------------------------

/// Create an auto-snapshot before a destructive operation.
pub fn auto_snapshot(db: &Database, table_name: &str) -> crate::errors::Result<SnapshotInfo> {
    let safe_table_name: String = table_name
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    let name = format!("pre-delete-{}-{}", safe_table_name, compact_timestamp());

    if is_in_memory(db) {
        create_snapshot_in_memory(db, &name, true)
    } else {
        create_snapshot_on_disk(db, &name, true)
    }
}

// ---------------------------------------------------------------------------
// Restore
// ---------------------------------------------------------------------------

/// Restore a snapshot into the active database.
///
/// Accepts a snapshot **name** (not a full path).
pub fn restore_snapshot(db: &Database, name: &str) -> crate::errors::Result<()> {
    validate_snapshot_name(name)?;
    let clean_name = normalize_name(name);

    if is_in_memory(db) {
        let store = IN_MEMORY_SNAPSHOTS.lock().unwrap();
        let snap = store.get(clean_name).ok_or_else(|| {
            crate::errors::DynoxideError::ResourceNotFoundException(format!(
                "Snapshot not found: {clean_name}"
            ))
        })?;
        db.restore_from_connection(&snap.conn)
    } else {
        let path = resolve_snapshot_path(db, name)?;
        if !path.exists() {
            return Err(crate::errors::DynoxideError::ResourceNotFoundException(
                format!("Snapshot not found: {name}"),
            ));
        }
        db.restore_from(path.to_str().unwrap_or(name))
    }
}

// ---------------------------------------------------------------------------
// List
// ---------------------------------------------------------------------------

/// List available snapshots with metadata.
///
/// Returns snapshots sorted by name descending (newest first), limited to
/// `limit` entries. Pass `None` for the default limit.
pub fn list_snapshots(
    db: &Database,
    limit: Option<usize>,
) -> crate::errors::Result<Vec<SnapshotInfo>> {
    let max = limit.unwrap_or(MAX_TOTAL_SNAPSHOTS);

    if is_in_memory(db) {
        let store = IN_MEMORY_SNAPSHOTS.lock().unwrap();
        let mut snapshots: Vec<SnapshotInfo> = store
            .iter()
            .map(|(name, snap)| SnapshotInfo {
                name: name.clone(),
                size_bytes: snap.size_bytes,
                created_epoch: Some(snap.created_epoch),
                is_auto: snap.is_auto,
            })
            .collect();
        snapshots.sort_by(|a, b| b.name.cmp(&a.name));
        snapshots.truncate(max);
        return Ok(snapshots);
    }

    let dir = match snapshot_dir(db) {
        Ok(d) => d,
        Err(_) => return Ok(vec![]),
    };
    if !dir.exists() {
        return Ok(vec![]);
    }

    let mut snapshots = Vec::new();
    let entries = std::fs::read_dir(&dir).map_err(|e| {
        crate::errors::DynoxideError::InternalServerError(format!(
            "Failed to read snapshot directory: {e}"
        ))
    })?;

    for entry in entries {
        let entry = match entry {
            Ok(e) => e,
            Err(_) => continue,
        };
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("db") {
            continue;
        }
        let metadata = match entry.metadata() {
            Ok(m) => m,
            Err(_) => continue,
        };
        let name = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("")
            .to_string();
        let size_bytes = metadata.len();
        let created = metadata
            .created()
            .or_else(|_| metadata.modified())
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_secs());
        let is_auto = name.starts_with("pre-delete-");

        snapshots.push(SnapshotInfo {
            name,
            size_bytes,
            created_epoch: created,
            is_auto,
        });
    }

    snapshots.sort_by(|a, b| b.name.cmp(&a.name));
    snapshots.truncate(max);
    Ok(snapshots)
}

// ---------------------------------------------------------------------------
// Delete
// ---------------------------------------------------------------------------

/// Delete a snapshot by name.
pub fn delete_snapshot(db: &Database, name: &str) -> crate::errors::Result<()> {
    validate_snapshot_name(name)?;
    let clean_name = normalize_name(name);

    if is_in_memory(db) {
        let mut store = IN_MEMORY_SNAPSHOTS.lock().unwrap();
        if store.remove(clean_name).is_none() {
            return Err(crate::errors::DynoxideError::ResourceNotFoundException(
                format!("Snapshot not found: {clean_name}"),
            ));
        }
        return Ok(());
    }

    let path = resolve_snapshot_path(db, name)?;
    if !path.exists() {
        return Err(crate::errors::DynoxideError::ResourceNotFoundException(
            format!("Snapshot not found: {name}"),
        ));
    }

    std::fs::remove_file(&path).map_err(|e| {
        crate::errors::DynoxideError::InternalServerError(format!("Failed to delete snapshot: {e}"))
    })
}

// ---------------------------------------------------------------------------
// Metadata
// ---------------------------------------------------------------------------

/// Metadata about a snapshot.
///
/// Does not expose filesystem paths — agents work with snapshot names only.
#[derive(serde::Serialize, Clone, Debug)]
pub struct SnapshotInfo {
    pub name: String,
    pub size_bytes: u64,
    pub created_epoch: Option<u64>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub is_auto: bool,
}

// ---------------------------------------------------------------------------
// Pruning
// ---------------------------------------------------------------------------

/// Prune file-based snapshots to stay within limits.
///
/// Eviction order: auto-snapshots first (oldest), then manual snapshots (oldest).
/// This makes explicitly-named manual snapshots stickier.
fn prune_file_snapshots(dir: &Path) {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };

    let mut auto_snapshots: Vec<PathBuf> = Vec::new();
    let mut manual_snapshots: Vec<PathBuf> = Vec::new();

    for entry in entries.filter_map(|e| e.ok()) {
        let path = entry.path();
        let is_db = path
            .extension()
            .and_then(|e| e.to_str())
            .is_some_and(|e| e == "db");
        if !is_db {
            continue;
        }
        let is_auto = path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.starts_with("pre-delete-"));

        if is_auto {
            auto_snapshots.push(path);
        } else {
            manual_snapshots.push(path);
        }
    }

    // Sort oldest first (names include timestamps)
    auto_snapshots.sort();
    manual_snapshots.sort();

    // 1. Prune auto-snapshots beyond their sub-limit
    if auto_snapshots.len() > MAX_AUTO_SNAPSHOTS {
        let to_remove = auto_snapshots.len() - MAX_AUTO_SNAPSHOTS;
        for path in auto_snapshots.iter().take(to_remove) {
            let _ = std::fs::remove_file(path);
        }
        auto_snapshots.drain(..to_remove);
    }

    // 2. Prune total beyond global limit — evict auto first, then manual
    let total = auto_snapshots.len() + manual_snapshots.len();
    if total >= MAX_TOTAL_SNAPSHOTS {
        let mut to_remove = total - MAX_TOTAL_SNAPSHOTS + 1; // +1 to make room

        // Evict oldest auto-snapshots first
        let auto_evict = to_remove.min(auto_snapshots.len());
        for path in auto_snapshots.iter().take(auto_evict) {
            let _ = std::fs::remove_file(path);
        }
        to_remove -= auto_evict;

        // Then evict oldest manual snapshots
        for path in manual_snapshots.iter().take(to_remove) {
            let _ = std::fs::remove_file(path);
        }
    }
}

/// Prune the in-memory snapshot store to stay within limits.
///
/// Same eviction policy: auto-snapshots first, then oldest manual.
fn prune_in_memory_store(store: &mut HashMap<String, InMemorySnapshot>) {
    // 1. Prune auto-snapshots beyond sub-limit
    let mut auto_names: Vec<String> = store
        .iter()
        .filter(|(_, s)| s.is_auto)
        .map(|(n, _)| n.clone())
        .collect();
    auto_names.sort();
    if auto_names.len() > MAX_AUTO_SNAPSHOTS {
        let to_remove = auto_names.len() - MAX_AUTO_SNAPSHOTS;
        for name in auto_names.iter().take(to_remove) {
            store.remove(name);
        }
    }

    // 2. Prune total beyond global limit — auto first, then manual
    if store.len() >= MAX_TOTAL_SNAPSHOTS {
        let mut auto_entries: Vec<(String, u64)> = store
            .iter()
            .filter(|(_, s)| s.is_auto)
            .map(|(n, s)| (n.clone(), s.created_epoch))
            .collect();
        auto_entries.sort_by_key(|(_, epoch)| *epoch);

        let mut manual_entries: Vec<(String, u64)> = store
            .iter()
            .filter(|(_, s)| !s.is_auto)
            .map(|(n, s)| (n.clone(), s.created_epoch))
            .collect();
        manual_entries.sort_by_key(|(_, epoch)| *epoch);

        let mut to_remove = store.len() - MAX_TOTAL_SNAPSHOTS + 1;

        let auto_evict = to_remove.min(auto_entries.len());
        for (name, _) in auto_entries.iter().take(auto_evict) {
            store.remove(name);
        }
        to_remove -= auto_evict;

        for (name, _) in manual_entries.iter().take(to_remove) {
            store.remove(name);
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_snapshot_name_rejects_traversal() {
        assert!(validate_snapshot_name("../evil").is_err());
        assert!(validate_snapshot_name("foo/bar").is_err());
        assert!(validate_snapshot_name("foo\\bar").is_err());
        assert!(validate_snapshot_name("foo\0bar").is_err());
        assert!(validate_snapshot_name("").is_err());
        assert!(validate_snapshot_name("valid-name_123").is_ok());
    }

    #[test]
    fn resolve_snapshot_path_appends_db() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db = Database::new(db_path.to_str().unwrap()).unwrap();

        let path = resolve_snapshot_path(&db, "my-snap").unwrap();
        assert!(path.to_str().unwrap().ends_with("my-snap.db"));
        assert!(path.to_str().unwrap().contains(".dynoxide/snapshots"));
    }

    #[test]
    fn resolve_snapshot_path_does_not_double_db() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db = Database::new(db_path.to_str().unwrap()).unwrap();

        let path = resolve_snapshot_path(&db, "my-snap.db").unwrap();
        assert!(path.to_str().unwrap().ends_with("my-snap.db"));
        assert!(!path.to_str().unwrap().ends_with("my-snap.db.db"));
    }

    #[test]
    fn snapshot_dir_errors_for_in_memory() {
        let db = Database::memory().unwrap();
        assert!(snapshot_dir(&db).is_err());
    }

    #[test]
    fn in_memory_snapshot_roundtrip() {
        // Use a unique snapshot name to avoid races with other in-memory tests
        // sharing the global IN_MEMORY_SNAPSHOTS store.
        let snap_name = "roundtrip-test-unique";
        let db = Database::memory().unwrap();
        db.create_table(crate::actions::create_table::CreateTableRequest {
            table_name: "Test".to_string(),
            key_schema: vec![crate::types::KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: crate::types::KeyType::HASH,
            }],
            attribute_definitions: vec![crate::types::AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: crate::types::ScalarAttributeType::S,
            }],
            ..Default::default()
        })
        .unwrap();

        db.put_item(crate::actions::put_item::PutItemRequest {
            table_name: "Test".to_string(),
            item: crate::item! { "pk" => "item1" },
            ..Default::default()
        })
        .unwrap();

        // Snapshot
        let info = create_snapshot(&db, Some(snap_name)).unwrap();
        assert_eq!(info.name, snap_name);
        assert!(info.size_bytes > 0);

        // Add more data
        db.put_item(crate::actions::put_item::PutItemRequest {
            table_name: "Test".to_string(),
            item: crate::item! { "pk" => "item2" },
            ..Default::default()
        })
        .unwrap();
        assert_eq!(db.table_stats().unwrap()[0].item_count, 2);

        // Restore
        restore_snapshot(&db, snap_name).unwrap();
        assert_eq!(db.table_stats().unwrap()[0].item_count, 1);

        // List
        let snaps = list_snapshots(&db, None).unwrap();
        assert!(snaps.iter().any(|s| s.name == snap_name));

        // Delete only this test's snapshot (not clear() which races with other tests)
        delete_snapshot(&db, snap_name).unwrap();
        let snaps = list_snapshots(&db, None).unwrap();
        assert!(!snaps.iter().any(|s| s.name == snap_name));
    }

    #[test]
    fn in_memory_auto_snapshot_is_auto() {
        let db = Database::memory().unwrap();
        db.create_table(crate::actions::create_table::CreateTableRequest {
            table_name: "AutoTest".to_string(),
            key_schema: vec![crate::types::KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: crate::types::KeyType::HASH,
            }],
            attribute_definitions: vec![crate::types::AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: crate::types::ScalarAttributeType::S,
            }],
            ..Default::default()
        })
        .unwrap();

        let info = auto_snapshot(&db, "AutoTest").unwrap();
        assert!(info.name.starts_with("pre-delete-AutoTest-"));
        assert!(info.is_auto);

        // Clean up only this test's snapshot
        let _ = delete_snapshot(&db, &info.name);
    }

    #[test]
    fn file_snapshot_creates_and_restores() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db = Database::new(db_path.to_str().unwrap()).unwrap();

        db.create_table(crate::actions::create_table::CreateTableRequest {
            table_name: "Items".to_string(),
            key_schema: vec![crate::types::KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: crate::types::KeyType::HASH,
            }],
            attribute_definitions: vec![crate::types::AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: crate::types::ScalarAttributeType::S,
            }],
            ..Default::default()
        })
        .unwrap();

        db.put_item(crate::actions::put_item::PutItemRequest {
            table_name: "Items".to_string(),
            item: crate::item! { "pk" => "a" },
            ..Default::default()
        })
        .unwrap();

        let info = create_snapshot(&db, Some("file-snap")).unwrap();
        assert_eq!(info.name, "file-snap");

        // Verify file exists
        let snap_path = snapshot_dir(&db).unwrap().join("file-snap.db");
        assert!(snap_path.exists());

        // Add more data then restore
        db.put_item(crate::actions::put_item::PutItemRequest {
            table_name: "Items".to_string(),
            item: crate::item! { "pk" => "b" },
            ..Default::default()
        })
        .unwrap();
        assert_eq!(db.table_stats().unwrap()[0].item_count, 2);

        restore_snapshot(&db, "file-snap").unwrap();
        assert_eq!(db.table_stats().unwrap()[0].item_count, 1);
    }

    #[test]
    fn list_snapshots_respects_limit() {
        // Use file-backed DB to avoid global in-memory store contention
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("limit-test.db");
        let db = Database::new(db_path.to_str().unwrap()).unwrap();

        db.create_table(crate::actions::create_table::CreateTableRequest {
            table_name: "LimitTest".to_string(),
            key_schema: vec![crate::types::KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: crate::types::KeyType::HASH,
            }],
            attribute_definitions: vec![crate::types::AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: crate::types::ScalarAttributeType::S,
            }],
            ..Default::default()
        })
        .unwrap();

        for i in 0..5 {
            create_snapshot(&db, Some(&format!("limit-snap-{i:03}"))).unwrap();
        }

        let all = list_snapshots(&db, None).unwrap();
        assert_eq!(all.len(), 5);

        let limited = list_snapshots(&db, Some(3)).unwrap();
        assert_eq!(limited.len(), 3);
    }

    #[test]
    fn snapshot_info_does_not_contain_path() {
        let info = SnapshotInfo {
            name: "test".to_string(),
            size_bytes: 100,
            created_epoch: Some(1000),
            is_auto: false,
        };
        let json = serde_json::to_string(&info).unwrap();
        assert!(!json.contains("path"));
        // is_auto=false should be omitted
        assert!(!json.contains("is_auto"));
    }

    #[test]
    fn snapshot_info_shows_is_auto_when_true() {
        let info = SnapshotInfo {
            name: "pre-delete-test".to_string(),
            size_bytes: 100,
            created_epoch: Some(1000),
            is_auto: true,
        };
        let json = serde_json::to_string(&info).unwrap();
        assert!(json.contains("is_auto"));
    }

    #[test]
    fn normalize_name_strips_db_extension() {
        assert_eq!(normalize_name("foo.db"), "foo");
        assert_eq!(normalize_name("foo"), "foo");
        assert_eq!(normalize_name("foo.db.db"), "foo.db");
    }

    #[test]
    fn eviction_prefers_auto_over_manual() {
        let mut store: HashMap<String, InMemorySnapshot> = HashMap::new();

        // Fill with auto-snapshots at the limit
        for i in 0..MAX_TOTAL_SNAPSHOTS {
            let conn = rusqlite::Connection::open_in_memory().unwrap();
            store.insert(
                format!("pre-delete-table-{i:03}"),
                InMemorySnapshot {
                    conn,
                    size_bytes: 100,
                    created_epoch: i as u64,
                    is_auto: true,
                },
            );
        }

        // Add one manual snapshot
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        store.insert(
            "my-important-snap".to_string(),
            InMemorySnapshot {
                conn,
                size_bytes: 100,
                created_epoch: 999,
                is_auto: false,
            },
        );

        prune_in_memory_store(&mut store);

        // Manual snapshot should survive; auto-snapshots should have been evicted
        assert!(
            store.contains_key("my-important-snap"),
            "Manual snapshot should survive eviction"
        );
        assert!(
            store.len() <= MAX_TOTAL_SNAPSHOTS,
            "Store should be within limit"
        );
    }
}
