//! Table names SQLite can confuse: names that differ only in case, Dynoxide's
//! own tables, and names starting `sqlite_`. Dynoxide keeps each table in an
//! SQLite table of the same name, and SQLite compares table names ignoring
//! case, so CreateTable refuses these. A refusal must leave nothing behind,
//! and DeleteTable must never drop a SQLite table that isn't the table's own.

use dynoxide::Database;
use serde_json::{Value, json};

fn create(db: &Database, name: &str) -> Result<(), String> {
    let req = json!({
        "TableName": name,
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "BillingMode": "PAY_PER_REQUEST"
    });
    db.create_table(serde_json::from_value(req).unwrap())
        .map(|_| ())
        .map_err(|e| format!("{e:?}"))
}

fn create_with_gsi(db: &Database, name: &str) {
    let req = json!({
        "TableName": name,
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "g", "AttributeType": "S"}
        ],
        "GlobalSecondaryIndexes": [{
            "IndexName": "byG",
            "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
            "Projection": {"ProjectionType": "ALL"}
        }],
        "BillingMode": "PAY_PER_REQUEST"
    });
    db.create_table(serde_json::from_value(req).unwrap())
        .unwrap();
}

fn put(db: &Database, table: &str, item: Value) {
    let req = json!({"TableName": table, "Item": item});
    db.put_item(serde_json::from_value(req).unwrap()).unwrap();
}

fn get_v(db: &Database, table: &str) -> Result<Option<String>, String> {
    let req = json!({"TableName": table, "Key": {"pk": {"S": "k"}}});
    db.get_item(serde_json::from_value(req).unwrap())
        .map(|res| {
            let item = serde_json::to_value(&res).unwrap();
            item["Item"]["v"]["S"].as_str().map(str::to_string)
        })
        .map_err(|e| format!("{e:?}"))
}

fn query_g(db: &Database, table: &str) -> usize {
    let req = json!({
        "TableName": table,
        "IndexName": "byG",
        "KeyConditionExpression": "g = :g",
        "ExpressionAttributeValues": {":g": {"S": "x"}}
    });
    db.query(serde_json::from_value(req).unwrap())
        .unwrap()
        .items
        .map_or(0, |items| items.len())
}

fn tables(db: &Database) -> Vec<String> {
    db.list_tables(serde_json::from_value(json!({})).unwrap())
        .unwrap()
        .table_names
}

fn delete(db: &Database, name: &str) -> Result<(), String> {
    let req = json!({"TableName": name});
    db.delete_table(serde_json::from_value(req).unwrap())
        .map(|_| ())
        .map_err(|e| format!("{e:?}"))
}

/// Register `ghost` as a copy of `real`'s `_tables` row, with no SQLite table
/// of its own: what CreateTable left behind before it ran in one transaction,
/// when its CREATE TABLE clashed with `real`'s table.
fn leave_half_created(path: &std::path::Path, real: &str, ghost: &str) {
    let conn = rusqlite::Connection::open(path).unwrap();
    conn.execute_batch("CREATE TEMP TABLE copy AS SELECT * FROM _tables WHERE 0")
        .unwrap();
    conn.execute(
        "INSERT INTO copy SELECT * FROM _tables WHERE table_name = ?1",
        [real],
    )
    .unwrap();
    conn.execute("UPDATE copy SET table_name = ?1", [ghost])
        .unwrap();
    conn.execute_batch("INSERT INTO _tables SELECT * FROM copy")
        .unwrap();
}

#[test]
fn a_name_that_differs_only_in_case_is_refused_and_leaves_nothing_behind() {
    let db = Database::memory().unwrap();
    create(&db, "Users").unwrap();
    put(&db, "Users", json!({"pk": {"S": "k"}, "v": {"S": "real"}}));

    let error = create(&db, "users").unwrap_err();
    assert!(error.contains("ResourceInUseException"), "{error}");

    assert_eq!(tables(&db), ["Users"]);
    let described =
        db.describe_table(serde_json::from_value(json!({"TableName": "users"})).unwrap());
    assert!(format!("{:?}", described.unwrap_err()).contains("ResourceNotFoundException"));
    assert_eq!(get_v(&db, "Users").unwrap().as_deref(), Some("real"));
}

#[test]
fn dynoxides_own_table_names_are_refused_and_leave_nothing_behind() {
    for name in [
        "_TABLES",
        "_Config",
        "_STREAM_RECORDS",
        "_tables",
        "sqlite_x",
    ] {
        let db = Database::memory().unwrap();
        create(&db, "Keep").unwrap();
        put(&db, "Keep", json!({"pk": {"S": "k"}, "v": {"S": "kept"}}));

        let error = create(&db, name).unwrap_err();
        assert!(error.contains("ResourceInUseException"), "{name}: {error}");

        assert_eq!(tables(&db), ["Keep"], "{name}");
        create(&db, "Other").unwrap();
        assert_eq!(
            get_v(&db, "Keep").unwrap().as_deref(),
            Some("kept"),
            "{name}"
        );
    }
}

#[test]
fn a_create_that_fails_at_an_index_leaves_no_table() {
    let db = Database::memory().unwrap();
    create_with_gsi(&db, "Users");

    // `Users::gsi::byG` exists; creating `users` with a GSI of the same name
    // clashes at the data table already, and must leave no index table either.
    let req = json!({
        "TableName": "users",
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "g", "AttributeType": "S"}
        ],
        "GlobalSecondaryIndexes": [{
            "IndexName": "byG",
            "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
            "Projection": {"ProjectionType": "ALL"}
        }],
        "StreamSpecification": {"StreamEnabled": true, "StreamViewType": "NEW_IMAGE"},
        "Tags": [{"Key": "k", "Value": "v"}],
        "BillingMode": "PAY_PER_REQUEST"
    });
    assert!(
        db.create_table(serde_json::from_value(req).unwrap())
            .is_err()
    );
    assert_eq!(tables(&db), ["Users"]);

    put(
        &db,
        "Users",
        json!({"pk": {"S": "k"}, "g": {"S": "x"}, "v": {"S": "real"}}),
    );
    assert_eq!(query_g(&db, "Users"), 1);
}

#[test]
fn a_refused_create_leaves_a_database_file_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("names.db");
    {
        let db = Database::new(path.to_str().unwrap()).unwrap();
        create(&db, "Users").unwrap();
        put(&db, "Users", json!({"pk": {"S": "k"}, "v": {"S": "real"}}));
        assert!(create(&db, "users").is_err());
    }
    let db = Database::new(path.to_str().unwrap()).unwrap();
    assert_eq!(tables(&db), ["Users"]);
    assert_eq!(get_v(&db, "Users").unwrap().as_deref(), Some("real"));
}

#[test]
fn deleting_a_half_created_table_leaves_the_table_it_shadowed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ghost.db");
    {
        let db = Database::new(path.to_str().unwrap()).unwrap();
        create(&db, "Users").unwrap();
        put(&db, "Users", json!({"pk": {"S": "k"}, "v": {"S": "real"}}));
    }
    leave_half_created(&path, "Users", "users");

    let db = Database::new(path.to_str().unwrap()).unwrap();
    assert_eq!(tables(&db), ["Users", "users"]);
    delete(&db, "users").unwrap();

    assert_eq!(tables(&db), ["Users"]);
    assert_eq!(get_v(&db, "Users").unwrap().as_deref(), Some("real"));
}

#[test]
fn deleting_a_half_created_table_leaves_its_shadowed_index() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ghost-gsi.db");
    {
        let db = Database::new(path.to_str().unwrap()).unwrap();
        create_with_gsi(&db, "Users");
        put(
            &db,
            "Users",
            json!({"pk": {"S": "k"}, "g": {"S": "x"}, "v": {"S": "real"}}),
        );
    }
    leave_half_created(&path, "Users", "users");

    let db = Database::new(path.to_str().unwrap()).unwrap();
    delete(&db, "users").unwrap();

    assert_eq!(tables(&db), ["Users"]);
    assert_eq!(query_g(&db, "Users"), 1);
    assert_eq!(get_v(&db, "Users").unwrap().as_deref(), Some("real"));
}

#[test]
fn deleting_a_half_created_table_named_like_dynoxides_own_keeps_the_database_working() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ghost-meta.db");
    {
        let db = Database::new(path.to_str().unwrap()).unwrap();
        create(&db, "Keep").unwrap();
        put(&db, "Keep", json!({"pk": {"S": "k"}, "v": {"S": "kept"}}));
    }
    leave_half_created(&path, "Keep", "_TABLES");
    leave_half_created(&path, "Keep", "_tables");

    let db = Database::new(path.to_str().unwrap()).unwrap();
    delete(&db, "_TABLES").unwrap();
    delete(&db, "_tables").unwrap();

    assert_eq!(tables(&db), ["Keep"]);
    assert_eq!(get_v(&db, "Keep").unwrap().as_deref(), Some("kept"));
    create(&db, "Other").unwrap();
    drop(db);
    let db = Database::new(path.to_str().unwrap()).unwrap();
    assert_eq!(tables(&db), ["Keep", "Other"]);
}

#[test]
fn an_ordinary_delete_still_drops_everything_it_created() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ordinary.db");
    let db = Database::new(path.to_str().unwrap()).unwrap();
    create_with_gsi(&db, "Gone");
    put(&db, "Gone", json!({"pk": {"S": "k"}, "g": {"S": "x"}}));
    delete(&db, "Gone").unwrap();
    drop(db);

    let conn = rusqlite::Connection::open(&path).unwrap();
    let left: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name LIKE 'Gone%'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(left, 0);
}

#[test]
fn a_create_that_fails_after_listing_the_table_leaves_nothing_behind() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("leftover-index.db");
    drop(Database::new(path.to_str().unwrap()).unwrap());
    // An index table an interrupted delete left behind: `New` itself is free,
    // so CreateTable lists it and makes its data table, then fails at the GSI.
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute_batch("CREATE TABLE \"New::gsi::byG\" (x TEXT)")
        .unwrap();

    let db = Database::new(path.to_str().unwrap()).unwrap();
    let req = json!({
        "TableName": "New",
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "g", "AttributeType": "S"}
        ],
        "GlobalSecondaryIndexes": [{
            "IndexName": "byG",
            "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
            "Projection": {"ProjectionType": "ALL"}
        }],
        "BillingMode": "PAY_PER_REQUEST"
    });
    assert!(
        db.create_table(serde_json::from_value(req).unwrap())
            .is_err()
    );
    assert!(tables(&db).is_empty());
    drop(db);

    let conn = rusqlite::Connection::open(&path).unwrap();
    let data_table: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'New'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(data_table, 0);
}
