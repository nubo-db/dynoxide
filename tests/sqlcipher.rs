#![cfg(feature = "encryption")]

use dynoxide::types::{AttributeDefinition, KeySchemaElement, KeyType, ScalarAttributeType};
use dynoxide::{Database, DynoxideError};
use std::collections::HashMap;

const TEST_KEY: &str = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
const WRONG_KEY: &str = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";

fn create_test_table(db: &Database) {
    use dynoxide::actions::create_table::*;
    db.create_table(CreateTableRequest {
        table_name: "Users".to_string(),
        key_schema: vec![
            KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: KeyType::HASH,
            },
            KeySchemaElement {
                attribute_name: "sk".to_string(),
                key_type: KeyType::RANGE,
            },
        ],
        attribute_definitions: vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: ScalarAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".to_string(),
                attribute_type: ScalarAttributeType::S,
            },
        ],
        ..Default::default()
    })
    .unwrap();
}

fn put_test_item(db: &Database, pk: &str, sk: &str) {
    let item = dynoxide::item! {
        "pk" => pk,
        "sk" => sk,
        "name" => "Alice",
    };
    db.put_item(dynoxide::actions::put_item::PutItemRequest {
        table_name: "Users".to_string(),
        item,
        ..Default::default()
    })
    .unwrap();
}

fn get_test_item(
    db: &Database,
    pk: &str,
    sk: &str,
) -> Option<HashMap<String, dynoxide::AttributeValue>> {
    let key = dynoxide::item! { "pk" => pk, "sk" => sk };
    db.get_item(dynoxide::actions::get_item::GetItemRequest {
        table_name: "Users".to_string(),
        key,
        ..Default::default()
    })
    .unwrap()
    .item
}

#[test]
fn create_encrypted_database() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.db");
    let db = Database::new_encrypted(path.to_str().unwrap(), TEST_KEY).unwrap();
    create_test_table(&db);
    put_test_item(&db, "user#1", "PROFILE");
    let item = get_test_item(&db, "user#1", "PROFILE");
    assert!(item.is_some());
}

#[test]
fn reopen_encrypted_database_with_correct_key() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.db");

    // Create and populate
    {
        let db = Database::new_encrypted(path.to_str().unwrap(), TEST_KEY).unwrap();
        create_test_table(&db);
        put_test_item(&db, "user#1", "PROFILE");
    }

    // Reopen with correct key
    let db = Database::new_encrypted(path.to_str().unwrap(), TEST_KEY).unwrap();
    let item = get_test_item(&db, "user#1", "PROFILE");
    assert!(item.is_some());
    assert_eq!(
        item.unwrap().get("name").unwrap(),
        &dynoxide::AttributeValue::S("Alice".to_string())
    );
}

#[test]
fn wrong_key_fails() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.db");

    // Create with correct key
    {
        let db = Database::new_encrypted(path.to_str().unwrap(), TEST_KEY).unwrap();
        create_test_table(&db);
    }

    // Reopen with wrong key
    let result = Database::new_encrypted(path.to_str().unwrap(), WRONG_KEY);
    assert!(result.is_err());
}

#[test]
fn invalid_key_format_too_short() {
    let result = Database::new_encrypted("/tmp/dynoxide_test_invalid.db", "abcd");
    assert!(result.is_err());
    if let Err(DynoxideError::ValidationException(msg)) = result {
        assert!(msg.contains("64-character hex string"));
    } else {
        panic!("Expected ValidationException");
    }
}

#[test]
fn invalid_key_format_not_hex() {
    let result = Database::new_encrypted(
        "/tmp/dynoxide_test_invalid.db",
        "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
    );
    assert!(result.is_err());
    if let Err(DynoxideError::ValidationException(msg)) = result {
        assert!(msg.contains("64-character hex string"));
    } else {
        panic!("Expected ValidationException");
    }
}

#[test]
fn open_encrypted_db_without_key_gives_clear_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.db");

    // Create with encryption
    {
        let db = Database::new_encrypted(path.to_str().unwrap(), TEST_KEY).unwrap();
        create_test_table(&db);
    }

    // Open without encryption key
    let result = Database::new(path.to_str().unwrap());
    match result {
        Ok(_) => panic!("Expected error when opening encrypted DB without key"),
        Err(err) => {
            let msg = err.to_string();
            assert!(
                msg.contains("encrypted") || msg.contains("not a valid"),
                "Error should mention encryption, got: {msg}"
            );
        }
    }
}

#[test]
fn encrypted_database_file_is_not_readable_as_plaintext() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.db");

    // Create encrypted database with data
    {
        let db = Database::new_encrypted(path.to_str().unwrap(), TEST_KEY).unwrap();
        create_test_table(&db);
        put_test_item(&db, "user#1", "PROFILE");
    }

    // Read the raw file bytes — should not contain plaintext
    let bytes = std::fs::read(&path).unwrap();
    let content = String::from_utf8_lossy(&bytes);
    assert!(
        !content.contains("Alice"),
        "Encrypted database should not contain plaintext data"
    );
    assert!(
        !content.contains("SQLite format"),
        "Encrypted database should not have standard SQLite header"
    );
}
