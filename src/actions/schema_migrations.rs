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
        migrate_step(storage, schema::HASH_PREFIXES, || async {
            fill_hash_prefixes(storage).await
        })
        .await?;
    }
    Ok(())
}

/// Give every item without a stored hash prefix the prefix a write of its key
/// stores now.
///
/// Items written before schema version 6 were left with an empty
/// `hash_prefix`. A parallel scan picks each segment's items by that prefix,
/// and an empty one falls below every segment, so a parallel scan over the
/// whole table missed them. Only data tables carry the column; index tables
/// are left alone. A row whose stored `pk` does not read back as a key keeps
/// its empty prefix rather than failing the open, and no other column of any
/// row is touched.
async fn fill_hash_prefixes<S: StorageBackend>(storage: &S) -> Result<()> {
    const BATCH_SIZE: usize = 1000;
    for table_name in storage.list_table_names().await? {
        // A listed table without its data table, as an interrupted CreateTable
        // or DeleteTable can leave, has no items to fill in.
        if !storage.data_table_exists(&table_name).await? {
            continue;
        }
        let mut last: Option<(String, String)> = None;
        loop {
            let items = storage
                .scan_items(
                    &table_name,
                    &crate::storage::ScanParams {
                        limit: Some(BATCH_SIZE),
                        exclusive_start_pk: last.as_ref().map(|(pk, _)| pk.as_str()),
                        exclusive_start_sk: last.as_ref().map(|(_, sk)| sk.as_str()),
                        ..Default::default()
                    },
                )
                .await?;
            let prefixes: Vec<(String, String, String)> = items
                .iter()
                .filter_map(|(pk, sk, _)| {
                    let key = crate::types::key_value_from_key_string(pk)?;
                    let prefix = crate::storage::compute_hash_prefix(&key);
                    Some((pk.clone(), sk.clone(), prefix))
                })
                .collect();
            storage.fill_hash_prefixes(&table_name, &prefixes).await?;
            match items.last() {
                Some((pk, sk, _)) if items.len() == BATCH_SIZE => {
                    last = Some((pk.clone(), sk.clone()));
                }
                _ => break,
            }
        }
    }
    Ok(())
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
    use crate::storage_backend::sql_builders::SCHEMA_VERSION;
    use crate::types::Item;
    use rusqlite::Connection;
    use rusqlite::types::Value;
    use serde_json::json;

    /// The data tables the fixture creates: string, number and binary
    /// partition keys, and a table with a sort key and a GSI.
    const TABLES: [&str; 4] = ["Str", "Num", "Bin", "Sorted"];

    /// The segment counts each table is scanned with.
    const TOTALS: [u32; 4] = [1, 3, 4, 16];

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
    fn age(path: &str, version: u32) {
        let conn = Connection::open(path).unwrap();
        for table in TABLES {
            conn.execute(&format!("UPDATE \"{table}\" SET hash_prefix = ''"), [])
                .unwrap();
        }
        set_version(&conn, version);
    }

    fn set_version(conn: &Connection, version: u32) {
        conn.execute(
            "UPDATE _config SET value = ?1 WHERE key = 'schema_version'",
            [version.to_string()],
        )
        .unwrap();
    }

    fn empty_prefixes(path: &str) -> usize {
        let conn = Connection::open(path).unwrap();
        TABLES
            .iter()
            .map(|table| {
                conn.query_row(
                    &format!("SELECT COUNT(*) FROM \"{table}\" WHERE hash_prefix = ''"),
                    [],
                    |r| r.get::<_, i64>(0),
                )
                .unwrap() as usize
            })
            .sum()
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

    fn recorded(path: &str) -> String {
        Connection::open(path)
            .unwrap()
            .query_row(
                "SELECT value FROM _config WHERE key = 'schema_version'",
                [],
                |r| r.get(0),
            )
            .unwrap()
    }

    fn fresh_fixture() -> (tempfile::NamedTempFile, String, Segments, Rows) {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        write_fixture(&path);
        let fresh = segments(&Database::new(&path).unwrap());
        let rows = rows(&path);
        (tmp, path, fresh, rows)
    }

    #[test]
    fn old_items_scan_in_the_segments_a_fresh_write_gets() {
        for version in [9, 11] {
            let (_tmp, path, fresh, _) = fresh_fixture();
            age(&path, version);
            assert_eq!(empty_prefixes(&path), 24 * 4, "the fixture is aged");

            let db = Database::new(&path).unwrap();
            let opened = segments(&db);
            for (t, table) in TABLES.iter().enumerate() {
                let count = db.scan(request(json!({"TableName": table}))).unwrap().count;
                assert_eq!(count, 24, "{table}");
                let mut at = 0;
                for &total in &TOTALS {
                    let all: usize = opened[t][at..at + total as usize]
                        .iter()
                        .map(Vec::len)
                        .sum();
                    assert_eq!(
                        all, count,
                        "{table}: {total} segments return every item, from version {version}"
                    );
                    at += total as usize;
                }
                assert_eq!(
                    opened[t], fresh[t],
                    "{table}: each segment, in order, from version {version}"
                );
            }
            assert_eq!(empty_prefixes(&path), 0);
            assert_eq!(recorded(&path), SCHEMA_VERSION.to_string());
        }
    }

    #[test]
    fn filling_prefixes_loses_no_row_and_changes_no_other_column() {
        let (_tmp, path, _, fresh) = fresh_fixture();
        age(&path, 11);
        let aged = rows(&path);
        assert_ne!(aged, fresh, "the fixture is aged");

        drop(Database::new(&path).unwrap());
        assert_eq!(rows(&path), fresh);
    }

    #[test]
    fn a_stored_prefix_is_kept_and_a_second_run_changes_nothing() {
        let (_tmp, path, _, fresh) = fresh_fixture();
        age(&path, 11);
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
        assert_eq!(recorded(&path), SCHEMA_VERSION.to_string());
    }

    #[test]
    fn a_key_that_does_not_read_back_keeps_its_empty_prefix() {
        let (_tmp, path, _, _) = fresh_fixture();
        age(&path, 11);
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
        assert_eq!(recorded(&path), SCHEMA_VERSION.to_string());
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
        set_version(&conn, 11);
        drop(conn);

        drop(Database::new(&path).unwrap());
        assert_eq!(rows(&path), fresh);
    }
}
