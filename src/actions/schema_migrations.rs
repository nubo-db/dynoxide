//! The migrations a database needs when it is opened, run the same way on
//! both backends.

use std::future::Future;

use super::helpers::with_write_transaction;
use crate::errors::{DynoxideError, Result};
use crate::storage_backend::StorageBackend;
use crate::storage_backend::schema::{self, OpenMigrations};

/// Bring a database an earlier build wrote up to date, as `plan` says: rebuild
/// its multi-attribute GSIs, recompute the stored sizes of its vector tables,
/// then fill in the hash prefixes its oldest items lack. A database a newer
/// build wrote is refused.
///
/// Each step is one write transaction that records the step's version as it
/// commits, so an open that fails part way leaves the database as the last
/// finished step left it, and the next open carries on from there.
pub(crate) async fn run_open_migrations<S: StorageBackend>(
    storage: &S,
    plan: OpenMigrations,
) -> Result<()> {
    // Both opens refuse such a file before they write to it; this keeps a
    // caller that skipped that check from running on it.
    if let Some(version) = plan.newer_than_this_build {
        return Err(DynoxideError::InternalServerError(
            schema::newer_than_this_build(version),
        ));
    }
    if plan.rebuild_multi_attribute_gsis {
        migrate_step(storage, schema::MULTI_ATTRIBUTE_KEYS, || async {
            super::gsi::rebuild_multi_attribute_gsis(storage)
                .await
                .map(|_| ())
        })
        .await?;
    }
    if plan.recompute_vector_sizes {
        migrate_step(storage, schema::VECTOR_BILLED_SIZES, || async {
            super::vector_index::recompute_vector_table_sizes(storage)
                .await
                .map(|_| ())
        })
        .await?;
    }
    if plan.fill_hash_prefixes {
        if plan.read_only {
            // Recording the version is a write, so a read-only open with
            // nothing to fill leaves the file as it is; a later writable open
            // records it. One with rows to fill is refused, as the earlier
            // steps refuse a read-only file.
            if fill_hash_prefixes(storage, false).await? > 0 {
                return Err(DynoxideError::InternalServerError(
                    schema::read_only_needs_hash_prefixes(),
                ));
            }
        } else {
            migrate_step(storage, schema::HASH_PREFIXES, || async {
                fill_hash_prefixes(storage, true).await.map(|_| ())
            })
            .await?;
        }
    }
    Ok(())
}

/// Give every item without a stored hash prefix the prefix a write of its key
/// stores now, or, when `write` is false, only look for one that needs it.
/// Returns how many items were given a prefix; without `write`, 1 when any
/// item needs one and 0 when none does.
///
/// Items written before schema version 6 were left with an empty
/// `hash_prefix`. A parallel scan picks each segment's items by that prefix,
/// and an empty one falls below every segment, so a parallel scan over the
/// whole table missed them. Only the rows with an empty prefix are read, and
/// only their keys, so a table with none costs one query. Only data tables
/// carry the column; index tables are left alone. A row whose stored `pk` does
/// not read back as a key keeps its empty prefix rather than failing the open,
/// and no other column of any row is touched.
async fn fill_hash_prefixes<S: StorageBackend>(storage: &S, write: bool) -> Result<usize> {
    const BATCH_SIZE: usize = 1000;
    let mut filled = 0;
    for table_name in storage.list_table_names().await? {
        // A listed table without its data table, as an interrupted CreateTable
        // or DeleteTable can leave, has no items to fill in.
        if !storage.data_table_exists(&table_name).await? {
            continue;
        }
        // Each call writes the prefixes worked out from the last page, then
        // reads the next page of keys past it. The page is ordered by key, so
        // a row filled behind the cursor can't move it, and a row whose key
        // doesn't read back is passed over rather than read again.
        let mut fill: Vec<(String, String, String)> = Vec::new();
        let mut after: Option<(String, String)> = None;
        loop {
            let keys = storage
                .fill_hash_prefixes(
                    &table_name,
                    &fill,
                    after.as_ref().map(|(pk, sk)| (pk.as_str(), sk.as_str())),
                    BATCH_SIZE,
                )
                .await?;
            fill = keys
                .iter()
                .filter_map(|(pk, sk)| {
                    let key = crate::types::key_value_from_key_string(pk)?;
                    let prefix = crate::storage::compute_hash_prefix(&key);
                    Some((pk.clone(), sk.clone(), prefix))
                })
                .collect();
            if !write && !fill.is_empty() {
                return Ok(1);
            }
            filled += fill.len();
            match keys.last() {
                Some(last) if keys.len() == BATCH_SIZE => after = Some(last.clone()),
                _ => break,
            }
        }
        if !fill.is_empty() {
            storage
                .fill_hash_prefixes(&table_name, &fill, None, 0)
                .await?;
        }
    }
    Ok(filled)
}

/// Run `step` and record `version`, in one write transaction.
///
/// The recorded version is read again once the transaction holds the write
/// lock (`BEGIN IMMEDIATE` on both backends), so a second process opening the
/// same file can't run the step alongside this one: it waits for the lock, or
/// fails to get it, and then finds the step done. A version a newer build
/// recorded in the meantime is refused, not taken as the step being done. A
/// failure part way through rolls back the step and leaves the old version
/// recorded, so the next open runs it again from the start.
async fn migrate_step<S, F, Fut>(storage: &S, version: u32, step: F) -> Result<()>
where
    S: StorageBackend,
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<()>>,
{
    with_write_transaction(storage, async {
        let recorded = storage.recorded_schema_version().await?;
        schema::refuse_newer_mid_migration(recorded).map_err(DynoxideError::InternalServerError)?;
        if recorded.is_some_and(|recorded| recorded >= version) {
            return Ok(());
        }
        step().await?;
        storage.record_schema_version(version).await?;
        Ok(())
    })
    .await
}

#[cfg(test)]
mod tests {
    use crate::Database;
    use crate::storage_backend::schema;
    use crate::storage_backend::sql_builders::SCHEMA_VERSION;
    use crate::types::Item;
    use rusqlite::Connection;
    use rusqlite::types::Value;
    use serde_json::json;

    /// The data tables the fixture creates: string, number and binary
    /// partition keys, a table with a sort key and a GSI, and a table holding
    /// one number partition key written in several forms.
    const TABLES: [&str; 5] = ["Str", "Num", "Bin", "Sorted", "NumForms"];

    /// The segment counts each table is scanned with.
    const TOTALS: [u32; 4] = [1, 3, 4, 16];

    /// The recorded versions an aged fixture is opened from: none, as the
    /// browser build wrote from Dynoxide 0.10 to 0.13, then one from each
    /// stretch of native and shared steps.
    const STARTING_VERSIONS: [Option<u32>; 5] = [None, Some(5), Some(9), Some(10), Some(11)];

    /// Every item of every parallel scan of every table, by table, then total,
    /// then segment, in the order the scan returned them.
    type Segments = Vec<Vec<Vec<Item>>>;

    /// Every row of every SQLite table but `_config`, by table name, in key
    /// order: what the file holds, column for column.
    type Rows = Vec<(String, Vec<Vec<Value>>)>;

    fn request<T: serde::de::DeserializeOwned>(value: serde_json::Value) -> T {
        serde_json::from_value(value).unwrap()
    }

    /// Create the fixture tables at `path` with this build, so every row holds
    /// the prefix a fresh write of its key stores.
    fn write_fixture(path: &str) {
        let db = Database::new(path).unwrap();
        for (table, kind) in [("Str", "S"), ("Num", "N"), ("Bin", "B")] {
            db.create_table(request(json!({
                "TableName": table,
                "BillingMode": "PAY_PER_REQUEST",
                "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": kind}],
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}]
            })))
            .unwrap();
        }
        db.create_table(request(json!({
            "TableName": "Sorted",
            "BillingMode": "PAY_PER_REQUEST",
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "N"},
                {"AttributeName": "g", "AttributeType": "S"}
            ],
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"}
            ],
            "GlobalSecondaryIndexes": [{
                "IndexName": "byG",
                "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"}
            }]
        })))
        .unwrap();
        db.create_table(request(json!({
            "TableName": "NumForms",
            "BillingMode": "PAY_PER_REQUEST",
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "N"},
                {"AttributeName": "sk", "AttributeType": "S"}
            ],
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"}
            ]
        })))
        .unwrap();

        let mut items = Vec::new();
        for i in 0..24 {
            items.push((
                "Str",
                json!({"pk": {"S": format!("item-{i:04}")}, "n": {"N": i.to_string()}}),
            ));
            let number = format!("{}.{}", i * 37 - 400, i % 7);
            items.push(("Num", json!({"pk": {"N": number}, "s": {"S": "x"}})));
            let bytes: Vec<u8> = (0..=i as u8).map(|b| b.wrapping_mul(31)).collect();
            items.push((
                "Bin",
                json!({"pk": {"B": base64_encode(&bytes)}, "l": {"L": [{"BOOL": true}]}}),
            ));
        }
        // Characters that mean something to SQL, to LIKE, or to the stored
        // key string, which starts with the key's type and a colon.
        for key in [
            "it's",
            "say \"hi\"",
            "100%",
            "a_b",
            "a:b",
            "S:x",
            "N:1",
            "B:AA==",
            "nul\u{0}inside",
            "back\\slash",
            "é ü 😀",
        ] {
            items.push(("Str", json!({"pk": {"S": key}})));
        }
        for p in 0..6 {
            for s in 0..4 {
                items.push((
                    "Sorted",
                    json!({
                        "pk": {"S": format!("p{p}")},
                        "sk": {"N": (s * 10 - 15).to_string()},
                        "g": {"S": format!("g{}", (p + s) % 3)},
                        "m": {"M": {"a": {"SS": ["x", "y"]}}}
                    }),
                ));
            }
        }
        // One number written in several forms is one partition key: each item
        // stores the key as its own form gave it.
        for form in [
            "1",
            "1.0",
            "1e0",
            "10E-1",
            "0.1e1",
            "1.00",
            "-0.50",
            "-5e-1",
            "123.4560",
            "1.23456E2",
        ] {
            items.push(("NumForms", json!({"pk": {"N": form}, "sk": {"S": form}})));
        }
        for (table, item) in items {
            db.put_item(request(json!({"TableName": table, "Item": item})))
                .unwrap();
        }
    }

    fn base64_encode(bytes: &[u8]) -> String {
        use base64::Engine;
        base64::engine::general_purpose::STANDARD.encode(bytes)
    }

    /// Every parallel scan of every fixture table, through `db`.
    fn segments(db: &Database) -> Segments {
        TABLES
            .iter()
            .map(|table| {
                TOTALS
                    .iter()
                    .flat_map(|&total| (0..total).map(move |segment| (total, segment)))
                    .map(|(total, segment)| {
                        let mut items = Vec::new();
                        let mut start: Option<Item> = None;
                        loop {
                            let mut scan = json!({
                                "TableName": table,
                                "Segment": segment,
                                "TotalSegments": total
                            });
                            if let Some(start) = &start {
                                scan["ExclusiveStartKey"] = serde_json::to_value(start).unwrap();
                            }
                            let response = db.scan(request(scan)).unwrap();
                            items.extend(response.items.unwrap_or_default());
                            match response.last_evaluated_key {
                                Some(key) => start = Some(key),
                                None => break,
                            }
                        }
                        items
                    })
                    .collect()
            })
            .collect()
    }

    fn rows(path: &str) -> Rows {
        let conn = Connection::open(path).unwrap();
        let mut names: Vec<String> = conn
            .prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name <> '_config'")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        names.sort();
        names
            .into_iter()
            .map(|name| {
                let mut stmt = conn.prepare(&format!("SELECT * FROM \"{name}\"")).unwrap();
                let width = stmt.column_count();
                let mut table: Vec<Vec<Value>> = stmt
                    .query_map([], |r| (0..width).map(|i| r.get::<_, Value>(i)).collect())
                    .unwrap()
                    .collect::<rusqlite::Result<_>>()
                    .unwrap();
                table.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
                (name, table)
            })
            .collect()
    }

    /// Put the file back the way a build before schema version 6 left its
    /// items, with no hash prefix, and record `version`.
    fn age(path: &str, version: Option<u32>) {
        let conn = Connection::open(path).unwrap();
        for table in TABLES {
            conn.execute(&format!("UPDATE \"{table}\" SET hash_prefix = ''"), [])
                .unwrap();
        }
        match version {
            Some(version) => set_version(&conn, version),
            None => {
                conn.execute("DELETE FROM _config WHERE key = 'schema_version'", [])
                    .unwrap();
            }
        }
    }

    fn set_version(conn: &Connection, version: u32) {
        conn.execute(
            "UPDATE _config SET value = ?1 WHERE key = 'schema_version'",
            [version.to_string()],
        )
        .unwrap();
    }

    /// How many items the fixture tables hold, and how many of them have an
    /// empty hash prefix.
    fn items_and_empty_prefixes(path: &str) -> (usize, usize) {
        let conn = Connection::open(path).unwrap();
        let count =
            |sql: String| conn.query_row(&sql, [], |r| r.get::<_, i64>(0)).unwrap() as usize;
        TABLES.iter().fold((0, 0), |(items, empty), table| {
            (
                items + count(format!("SELECT COUNT(*) FROM \"{table}\"")),
                empty
                    + count(format!(
                        "SELECT COUNT(*) FROM \"{table}\" WHERE hash_prefix = ''"
                    )),
            )
        })
    }

    fn empty_prefixes(path: &str) -> usize {
        items_and_empty_prefixes(path).1
    }

    /// The stored hash prefix of the `Str` item whose `pk` column is `pk`.
    fn prefix_of(path: &str, pk: &str) -> String {
        Connection::open(path)
            .unwrap()
            .query_row("SELECT hash_prefix FROM \"Str\" WHERE pk = ?1", [pk], |r| {
                r.get(0)
            })
            .unwrap()
    }

    fn recorded(path: &str) -> Option<String> {
        Connection::open(path)
            .unwrap()
            .query_row(
                "SELECT value FROM _config WHERE key = 'schema_version'",
                [],
                |r| r.get(0),
            )
            .ok()
    }

    fn current() -> Option<String> {
        Some(SCHEMA_VERSION.to_string())
    }

    fn fresh_fixture() -> (tempfile::NamedTempFile, String, Segments, Rows) {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        write_fixture(&path);
        let fresh = segments(&Database::new(&path).unwrap());
        let rows = rows(&path);
        (tmp, path, fresh, rows)
    }

    #[cfg(unix)]
    fn set_mode(path: &str, mode: u32) {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
    }

    /// Make the file writable again and remove the `-wal` and `-shm` files a
    /// read-only connection leaves beside it.
    #[cfg(unix)]
    fn tidy_read_only(path: &str) {
        set_mode(path, 0o644);
        for suffix in ["-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{path}{suffix}"));
        }
    }

    #[test]
    fn old_items_scan_in_the_segments_a_fresh_write_gets() {
        for version in STARTING_VERSIONS {
            let (_tmp, path, fresh, _) = fresh_fixture();
            age(&path, version);
            let (items, empty) = items_and_empty_prefixes(&path);
            assert_eq!(empty, items, "the fixture is aged");

            let db = Database::new(&path).unwrap();
            let opened = segments(&db);
            for (t, table) in TABLES.iter().enumerate() {
                let count = db.scan(request(json!({"TableName": table}))).unwrap().count;
                assert!(count > 0, "{table}");
                let mut at = 0;
                for &total in &TOTALS {
                    let all: usize = opened[t][at..at + total as usize]
                        .iter()
                        .map(Vec::len)
                        .sum();
                    assert_eq!(
                        all, count,
                        "{table}: {total} segments return every item, from {version:?}"
                    );
                    at += total as usize;
                }
                assert_eq!(
                    opened[t], fresh[t],
                    "{table}: each segment, in order, from {version:?}"
                );
            }
            assert_eq!(empty_prefixes(&path), 0);
            assert_eq!(recorded(&path), current());
        }
    }

    #[test]
    fn filling_prefixes_loses_no_row_and_changes_no_other_column() {
        for version in STARTING_VERSIONS {
            let (_tmp, path, _, fresh) = fresh_fixture();
            age(&path, version);
            assert_ne!(rows(&path), fresh, "the fixture is aged");

            drop(Database::new(&path).unwrap());
            assert_eq!(rows(&path), fresh, "from {version:?}");
        }
    }

    #[test]
    fn a_stored_prefix_is_kept_and_a_second_run_changes_nothing() {
        let (_tmp, path, _, fresh) = fresh_fixture();
        age(&path, Some(11));
        Connection::open(&path)
            .unwrap()
            .execute(
                "UPDATE \"Str\" SET hash_prefix = 'abcdef' WHERE pk = 'S:item-0003'",
                [],
            )
            .unwrap();

        drop(Database::new(&path).unwrap());
        let once = rows(&path);
        assert_eq!(prefix_of(&path, "S:item-0003"), "abcdef");
        let changed: Vec<&Vec<Value>> = once
            .iter()
            .zip(&fresh)
            .flat_map(|((_, now), (_, before))| now.iter().filter(|row| !before.contains(row)))
            .collect();
        assert_eq!(once.len(), fresh.len());
        assert_eq!(changed.len(), 1, "only the row given a prefix differs");
        assert_eq!(empty_prefixes(&path), 0);

        set_version(&Connection::open(&path).unwrap(), 11);
        drop(Database::new(&path).unwrap());
        assert_eq!(rows(&path), once);
        assert_eq!(recorded(&path), current());
    }

    #[test]
    fn a_key_that_does_not_read_back_keeps_its_empty_prefix() {
        let (_tmp, path, _, _) = fresh_fixture();
        age(&path, Some(11));
        Connection::open(&path)
            .unwrap()
            .execute(
                "UPDATE \"Str\" SET pk = 'X:unreadable' WHERE pk = 'S:item-0003'",
                [],
            )
            .unwrap();

        drop(Database::new(&path).unwrap());
        assert_eq!(prefix_of(&path, "X:unreadable"), "");
        assert_eq!(empty_prefixes(&path), 1);
        assert_eq!(recorded(&path), current());
    }

    #[test]
    fn a_table_larger_than_one_batch_is_filled_throughout() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        {
            let db = Database::new(&path).unwrap();
            db.create_table(request(json!({
                "TableName": "Str",
                "BillingMode": "PAY_PER_REQUEST",
                "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}]
            })))
            .unwrap();
            for i in 0..2500 {
                db.put_item(request(json!({
                    "TableName": "Str",
                    "Item": {"pk": {"S": format!("k{i:05}")}}
                })))
                .unwrap();
            }
        }
        let fresh = rows(&path);
        let conn = Connection::open(&path).unwrap();
        conn.execute("UPDATE \"Str\" SET hash_prefix = ''", [])
            .unwrap();
        // Rows whose key doesn't read back, spread through the table and
        // across the batch boundaries, keep their empty prefix and must not
        // stop the pages after them being filled.
        conn.execute(
            "INSERT INTO \"Str\" (pk, sk, item_json, item_size)
                SELECT 'S:' || substr(pk, 3) || '!', sk, item_json, item_size
                FROM \"Str\" WHERE pk LIKE 'S:k__0__'",
            [],
        )
        .unwrap();
        conn.execute("UPDATE \"Str\" SET pk = 'X:' || pk WHERE pk LIKE '%!'", [])
            .unwrap();
        set_version(&conn, 11);
        drop(conn);

        drop(Database::new(&path).unwrap());
        let conn = Connection::open(&path).unwrap();
        conn.execute("DELETE FROM \"Str\" WHERE pk LIKE 'X:%'", [])
            .unwrap();
        drop(conn);
        assert_eq!(rows(&path), fresh);
    }

    #[test]
    fn a_current_file_with_nothing_to_fill_is_not_rewritten() {
        let (_tmp, path, _, fresh) = fresh_fixture();
        let conn = Connection::open(&path).unwrap();
        set_version(&conn, 11);
        // Count every row update, including one that writes a row's values
        // back unchanged.
        conn.execute_batch("CREATE TABLE probe (n INTEGER)")
            .unwrap();
        for table in TABLES {
            conn.execute_batch(&format!(
                "CREATE TRIGGER \"probe {table}\" AFTER UPDATE ON \"{table}\"
                    BEGIN INSERT INTO probe VALUES (1); END"
            ))
            .unwrap();
        }
        drop(conn);

        drop(Database::new(&path).unwrap());
        let conn = Connection::open(&path).unwrap();
        let updates: i64 = conn
            .query_row("SELECT COUNT(*) FROM probe", [], |r| r.get(0))
            .unwrap();
        assert_eq!(updates, 0);
        conn.execute_batch("DROP TABLE probe").unwrap();
        drop(conn);
        assert_eq!(rows(&path), fresh);
        assert_eq!(recorded(&path), current(), "the version is still recorded");
    }

    #[cfg(unix)]
    #[test]
    fn a_read_only_file_with_nothing_to_fill_opens_as_it_is() {
        let (_tmp, path, fresh, _) = fresh_fixture();
        set_version(&Connection::open(&path).unwrap(), 11);
        set_mode(&path, 0o444);
        let before = std::fs::read(&path).unwrap();

        {
            let db = Database::new(&path).unwrap();
            assert_eq!(segments(&db), fresh);
        }
        assert_eq!(std::fs::read(&path).unwrap(), before);
        assert_eq!(recorded(&path), Some("11".to_string()));

        // The first writable open records the version. It opens a copy: this
        // process's SQLite may still hold the read-only file handle for the
        // original, which a later process would not.
        tidy_read_only(&path);
        let copy = tempfile::NamedTempFile::new().unwrap();
        let copy = copy.path().to_str().unwrap().to_string();
        std::fs::copy(&path, &copy).unwrap();
        drop(Database::new(&copy).unwrap());
        assert_eq!(recorded(&copy), current());
    }

    #[cfg(unix)]
    #[test]
    fn a_read_only_file_with_items_to_fill_is_refused_as_it_is() {
        let (_tmp, path, _, _) = fresh_fixture();
        age(&path, Some(11));
        set_mode(&path, 0o444);
        let before = std::fs::read(&path).unwrap();

        let message = match Database::new(&path) {
            Ok(_) => panic!("a read-only file with items to fill opened"),
            Err(e) => e.to_string(),
        };
        assert!(
            message.contains(&schema::read_only_needs_hash_prefixes()),
            "{message}"
        );
        assert!(message.contains("Open it writable once"), "{message}");
        assert_eq!(std::fs::read(&path).unwrap(), before);
        assert_eq!(recorded(&path), Some("11".to_string()));
        let (items, empty) = items_and_empty_prefixes(&path);
        assert_eq!(empty, items);
        tidy_read_only(&path);
    }

    /// The steps before this one record their version even when they have
    /// nothing to change, so a read-only file below version 11 is refused, as
    /// it was before the hash prefix fill.
    #[cfg(unix)]
    #[test]
    fn a_read_only_file_before_vector_billed_sizes_is_refused_as_before() {
        let (_tmp, path, _, _) = fresh_fixture();
        set_version(&Connection::open(&path).unwrap(), 10);
        set_mode(&path, 0o444);
        let before = std::fs::read(&path).unwrap();

        let message = match Database::new(&path) {
            Ok(_) => panic!("a read-only file at version 10 opened"),
            Err(e) => e.to_string(),
        };
        assert!(
            message.contains("attempt to write a readonly database"),
            "{message}"
        );
        assert_eq!(std::fs::read(&path).unwrap(), before);
        assert_eq!(recorded(&path), Some("10".to_string()));
        tidy_read_only(&path);
    }
}
