//! One-shot helper to create test fixture databases.
//!
//! Run with: cargo test --features encryption --test create_fixtures -- --ignored
//!
//! This creates:
//! - tests/fixtures/option-a-unencrypted.db: Created by SQLCipher-linked build without a key
//! - tests/fixtures/option-a-encrypted.db: Created by SQLCipher-linked build with a key
//!
//! These fixtures are committed to the repo and used to verify cross-build
//! database compatibility after switching from Option A (SQLCipher always linked)
//! to Option B (SQLCipher behind encryption feature flag).

use dynoxide::Database;
use std::path::Path;

const FIXTURE_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures");
const TEST_KEY: &str = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";

fn create_table_and_insert(db: &Database) {
    use dynoxide::actions::create_table::*;
    use dynoxide::types::{AttributeDefinition, KeySchemaElement, KeyType, ScalarAttributeType};

    db.create_table(CreateTableRequest {
        table_name: "Users".to_string(),
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

    let item = dynoxide::item! {
        "pk" => "user#1",
        "name" => "Alice",
        "age" => 30,
    };
    db.put_item(dynoxide::actions::put_item::PutItemRequest {
        table_name: "Users".to_string(),
        item,
        ..Default::default()
    })
    .unwrap();
}

fn checkpoint_and_cleanup(db_path: &Path) {
    // Open a raw rusqlite connection to force WAL checkpoint
    let conn = rusqlite::Connection::open(db_path).unwrap();
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();
    drop(conn);

    // Clean up WAL/SHM files
    let _ = std::fs::remove_file(db_path.with_extension("db-wal"));
    let _ = std::fs::remove_file(db_path.with_extension("db-shm"));
}

#[test]
#[ignore] // Only run manually to (re)create fixtures
fn create_unencrypted_fixture() {
    let path = Path::new(FIXTURE_DIR).join("option-a-unencrypted.db");
    // Remove old fixture if present
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("db-wal"));
    let _ = std::fs::remove_file(path.with_extension("db-shm"));

    let db = Database::new(path.to_str().unwrap()).unwrap();
    create_table_and_insert(&db);
    drop(db);

    checkpoint_and_cleanup(&path);

    assert!(path.exists(), "Fixture file should exist");
    println!("Created: {}", path.display());
}

#[test]
#[ignore] // Only run manually to (re)create fixtures
#[cfg(feature = "encryption")]
fn create_encrypted_fixture() {
    let path = Path::new(FIXTURE_DIR).join("option-a-encrypted.db");
    // Remove old fixture if present
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("db-wal"));
    let _ = std::fs::remove_file(path.with_extension("db-shm"));

    let db = Database::new_encrypted(path.to_str().unwrap(), TEST_KEY).unwrap();
    create_table_and_insert(&db);
    drop(db);

    // For encrypted DBs, we need to open with the key to checkpoint
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.pragma_update(None, "key", format!("x'{TEST_KEY}'"))
        .unwrap();
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();
    drop(conn);

    // Clean up WAL/SHM files
    let _ = std::fs::remove_file(path.with_extension("db-wal"));
    let _ = std::fs::remove_file(path.with_extension("db-shm"));

    assert!(path.exists(), "Fixture file should exist");
    println!("Created: {}", path.display());
}
