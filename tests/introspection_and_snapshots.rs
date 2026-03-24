//! Integration tests for Database introspection and snapshot methods.

use dynoxide::actions::create_table::CreateTableRequest;
use dynoxide::actions::put_item::PutItemRequest;
use dynoxide::types::{AttributeDefinition, KeySchemaElement, KeyType, ScalarAttributeType};
use dynoxide::{Database, item};

fn create_test_db() -> Database {
    Database::memory().unwrap()
}

fn create_table(db: &Database, name: &str) {
    db.create_table(CreateTableRequest {
        table_name: name.to_string(),
        key_schema: vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::HASH,
        }],
        attribute_definitions: vec![AttributeDefinition {
            attribute_name: "pk".to_string(),
            attribute_type: ScalarAttributeType::S,
        }],
        ..Default::default()
    })
    .unwrap();
}

fn put_item(db: &Database, table: &str, pk: &str) {
    db.put_item(PutItemRequest {
        table_name: table.to_string(),
        item: item! { "pk" => pk },
        ..Default::default()
    })
    .unwrap();
}

// ---------------------------------------------------------------------------
// Introspection: db_path
// ---------------------------------------------------------------------------

#[test]
fn db_path_returns_none_for_in_memory() {
    let db = create_test_db();
    assert!(db.db_path().unwrap().is_none());
}

#[test]
fn db_path_returns_path_for_file_backed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.db");
    let db = Database::new(path.to_str().unwrap()).unwrap();
    let result = db.db_path().unwrap();
    assert!(result.is_some());
    assert!(result.unwrap().contains("test.db"));
}

// ---------------------------------------------------------------------------
// Introspection: db_size_bytes
// ---------------------------------------------------------------------------

#[test]
fn db_size_bytes_returns_nonzero() {
    let db = create_test_db();
    let size = db.db_size_bytes().unwrap();
    // Even an empty DB has metadata tables, so size > 0
    assert!(size > 0);
}

#[test]
fn db_size_bytes_grows_with_data() {
    let db = create_test_db();
    let size_before = db.db_size_bytes().unwrap();

    create_table(&db, "Items");
    for i in 0..100 {
        put_item(&db, "Items", &format!("item#{i}"));
    }

    let size_after = db.db_size_bytes().unwrap();
    assert!(size_after > size_before);
}

// ---------------------------------------------------------------------------
// Introspection: table_count
// ---------------------------------------------------------------------------

#[test]
fn table_count_empty() {
    let db = create_test_db();
    assert_eq!(db.table_count().unwrap(), 0);
}

#[test]
fn table_count_after_creates() {
    let db = create_test_db();
    create_table(&db, "Alpha");
    create_table(&db, "Beta");
    create_table(&db, "Gamma");
    assert_eq!(db.table_count().unwrap(), 3);
}

// ---------------------------------------------------------------------------
// Introspection: table_stats
// ---------------------------------------------------------------------------

#[test]
fn table_stats_empty_db() {
    let db = create_test_db();
    let stats = db.table_stats().unwrap();
    assert!(stats.is_empty());
}

#[test]
fn table_stats_with_data() {
    let db = create_test_db();
    create_table(&db, "Users");
    create_table(&db, "Orders");

    put_item(&db, "Users", "user#1");
    put_item(&db, "Users", "user#2");
    put_item(&db, "Orders", "order#1");

    let stats = db.table_stats().unwrap();
    assert_eq!(stats.len(), 2);

    // Stats are sorted by table name (list_table_names sorts)
    let orders = stats.iter().find(|s| s.table_name == "Orders").unwrap();
    let users = stats.iter().find(|s| s.table_name == "Users").unwrap();

    assert_eq!(orders.item_count, 1);
    assert_eq!(users.item_count, 2);
    assert!(users.size_bytes > 0);
    assert!(orders.size_bytes > 0);
}

// ---------------------------------------------------------------------------
// Snapshots: vacuum_into
// ---------------------------------------------------------------------------

#[test]
fn vacuum_into_creates_valid_snapshot_from_memory() {
    let db = create_test_db();
    create_table(&db, "Items");
    put_item(&db, "Items", "item#1");
    put_item(&db, "Items", "item#2");

    let dir = tempfile::tempdir().unwrap();
    let snapshot_path = dir.path().join("snapshot.db");
    db.vacuum_into(snapshot_path.to_str().unwrap()).unwrap();

    // Verify the snapshot is a valid Dynoxide database
    let restored = Database::new(snapshot_path.to_str().unwrap()).unwrap();
    assert_eq!(restored.table_count().unwrap(), 1);
    let stats = restored.table_stats().unwrap();
    assert_eq!(stats[0].item_count, 2);
}

#[test]
fn vacuum_into_creates_valid_snapshot_from_file() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("source.db");
    let db = Database::new(db_path.to_str().unwrap()).unwrap();
    create_table(&db, "Items");
    put_item(&db, "Items", "item#1");

    let snapshot_path = dir.path().join("snapshot.db");
    db.vacuum_into(snapshot_path.to_str().unwrap()).unwrap();

    let restored = Database::new(snapshot_path.to_str().unwrap()).unwrap();
    assert_eq!(restored.table_count().unwrap(), 1);
}

// ---------------------------------------------------------------------------
// Snapshots: restore_from
// ---------------------------------------------------------------------------

#[test]
fn restore_from_replaces_in_memory_db() {
    // Create original DB with data
    let db = create_test_db();
    create_table(&db, "Original");
    put_item(&db, "Original", "item#1");

    // Snapshot it
    let dir = tempfile::tempdir().unwrap();
    let snapshot_path = dir.path().join("snapshot.db");
    db.vacuum_into(snapshot_path.to_str().unwrap()).unwrap();

    // Modify the DB (add more data, new table)
    create_table(&db, "NewTable");
    put_item(&db, "Original", "item#2");
    put_item(&db, "Original", "item#3");
    assert_eq!(db.table_count().unwrap(), 2);

    // Restore from snapshot — should revert to original state
    db.restore_from(snapshot_path.to_str().unwrap()).unwrap();

    assert_eq!(db.table_count().unwrap(), 1);
    let stats = db.table_stats().unwrap();
    assert_eq!(stats[0].table_name, "Original");
    assert_eq!(stats[0].item_count, 1);
}

#[test]
fn restore_from_replaces_file_backed_db() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("live.db");
    let db = Database::new(db_path.to_str().unwrap()).unwrap();
    create_table(&db, "Original");
    put_item(&db, "Original", "item#1");

    // Snapshot
    let snapshot_path = dir.path().join("snapshot.db");
    db.vacuum_into(snapshot_path.to_str().unwrap()).unwrap();

    // Modify
    create_table(&db, "Extra");
    assert_eq!(db.table_count().unwrap(), 2);

    // Restore
    db.restore_from(snapshot_path.to_str().unwrap()).unwrap();
    assert_eq!(db.table_count().unwrap(), 1);
}

#[test]
fn restore_from_invalidates_metadata_cache() {
    let db = create_test_db();
    create_table(&db, "Table1");

    // Snapshot with 1 table
    let dir = tempfile::tempdir().unwrap();
    let snapshot_path = dir.path().join("snapshot.db");
    db.vacuum_into(snapshot_path.to_str().unwrap()).unwrap();

    // Create 2 more tables (which get cached in the metadata cache)
    create_table(&db, "Table2");
    create_table(&db, "Table3");

    // Accessing Table2 and Table3 warms the cache
    put_item(&db, "Table2", "item#1");
    put_item(&db, "Table3", "item#1");

    // Restore — Table2 and Table3 no longer exist
    db.restore_from(snapshot_path.to_str().unwrap()).unwrap();

    // Operations on Table2 should fail (table doesn't exist)
    let result = db.put_item(PutItemRequest {
        table_name: "Table2".to_string(),
        item: item! { "pk" => "item#2" },
        ..Default::default()
    });
    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// Snapshot round-trip: vacuum_into → restore_from
// ---------------------------------------------------------------------------

#[test]
fn snapshot_roundtrip_preserves_data() {
    let db = create_test_db();
    create_table(&db, "Users");
    create_table(&db, "Orders");

    for i in 0..10 {
        put_item(&db, "Users", &format!("user#{i}"));
    }
    for i in 0..5 {
        put_item(&db, "Orders", &format!("order#{i}"));
    }

    // Snapshot
    let dir = tempfile::tempdir().unwrap();
    let snapshot_path = dir.path().join("roundtrip.db");
    db.vacuum_into(snapshot_path.to_str().unwrap()).unwrap();

    // Create a fresh DB and restore into it
    let db2 = create_test_db();
    db2.restore_from(snapshot_path.to_str().unwrap()).unwrap();

    // Verify data
    assert_eq!(db2.table_count().unwrap(), 2);
    let stats = db2.table_stats().unwrap();
    let users = stats.iter().find(|s| s.table_name == "Users").unwrap();
    let orders = stats.iter().find(|s| s.table_name == "Orders").unwrap();
    assert_eq!(users.item_count, 10);
    assert_eq!(orders.item_count, 5);
}
