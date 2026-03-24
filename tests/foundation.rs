//! Phase 1 integration tests: types, storage, errors working together.

use dynoxide::errors::DynoxideError;
use dynoxide::storage::Storage;
use dynoxide::types::{self, AttributeValue, Item};
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Storage lifecycle
// ---------------------------------------------------------------------------

#[test]
fn test_storage_lifecycle() {
    let storage = Storage::memory().unwrap();
    let tables = storage.list_table_names().unwrap();
    assert!(tables.is_empty());
}

// ---------------------------------------------------------------------------
// AttributeValue serialization round-trips through storage
// ---------------------------------------------------------------------------

#[test]
fn test_item_roundtrip_through_storage() {
    let storage = Storage::memory().unwrap();
    storage.create_data_table("Items").unwrap();

    // Build a rich DynamoDB item
    let mut item: Item = HashMap::new();
    item.insert("pk".into(), AttributeValue::S("user#123".into()));
    item.insert("sk".into(), AttributeValue::S("profile".into()));
    item.insert("name".into(), AttributeValue::S("Alice".into()));
    item.insert("age".into(), AttributeValue::N("30".into()));
    item.insert("active".into(), AttributeValue::BOOL(true));
    item.insert(
        "tags".into(),
        AttributeValue::SS(vec!["admin".into(), "user".into()]),
    );
    item.insert(
        "nested".into(),
        AttributeValue::M({
            let mut m = HashMap::new();
            m.insert("key".into(), AttributeValue::S("value".into()));
            m
        }),
    );

    let json = serde_json::to_string(&item).unwrap();
    let item_size = types::item_size(&item);

    let pk = item["pk"].to_key_string().unwrap();
    let sk = item["sk"].to_key_string().unwrap();

    // Store and retrieve
    storage
        .put_item("Items", &pk, &sk, &json, item_size)
        .unwrap();
    let retrieved = storage.get_item("Items", &pk, &sk).unwrap().unwrap();
    let retrieved_item: Item = serde_json::from_str(&retrieved).unwrap();

    assert_eq!(retrieved_item["name"], AttributeValue::S("Alice".into()));
    assert_eq!(retrieved_item["age"], AttributeValue::N("30".into()));
    assert_eq!(retrieved_item["active"], AttributeValue::BOOL(true));
}

// ---------------------------------------------------------------------------
// Number sort key ordering in SQLite
// ---------------------------------------------------------------------------

#[test]
fn test_number_sort_keys_order_correctly_in_sqlite() {
    let storage = Storage::memory().unwrap();
    storage.create_data_table("Numbers").unwrap();

    // Insert items with numeric sort keys in random order
    let numbers = vec!["100", "-5", "0", "42", "-100", "0.5", "-0.5", "1000"];

    for num in &numbers {
        let sk = AttributeValue::N(num.to_string()).to_key_string().unwrap();
        let json = format!(r#"{{"val":{{"N":"{num}"}}}}"#);
        storage.put_item("Numbers", "pk1", &sk, &json, 10).unwrap();
    }

    // Query and verify they come back in correct numeric order
    let results = storage
        .query_items(
            "Numbers",
            "pk1",
            &dynoxide::storage::QueryParams {
                forward: true,
                ..Default::default()
            },
        )
        .unwrap();

    let mut prev_num: Option<f64> = None;
    for (_, sk, _) in &results {
        let json_str = storage.get_item("Numbers", "pk1", sk).unwrap().unwrap();
        let item: HashMap<String, AttributeValue> = serde_json::from_str(&json_str).unwrap();
        if let AttributeValue::N(n) = &item["val"] {
            let current: f64 = n.parse().unwrap();
            if let Some(prev) = prev_num {
                assert!(
                    current >= prev,
                    "Sort order broken: {prev} should be <= {current}"
                );
            }
            prev_num = Some(current);
        }
    }

    assert_eq!(results.len(), 8);
}

// ---------------------------------------------------------------------------
// Error types
// ---------------------------------------------------------------------------

#[test]
fn test_error_response_matches_dynamodb_format() {
    let err = DynoxideError::ResourceNotFoundException(
        "Requested resource not found: Table: MyTable not found".into(),
    );

    let resp = err.to_response();
    let json: serde_json::Value = serde_json::to_value(&resp).unwrap();

    // DynamoDB error format requires __type and message
    assert!(json.get("__type").is_some());
    assert!(json.get("message").is_some());

    let type_str = json["__type"].as_str().unwrap();
    assert!(type_str.starts_with("com.amazonaws.dynamodb.v20120810#"));
    assert!(type_str.ends_with("ResourceNotFoundException"));
}

// ---------------------------------------------------------------------------
// Item size calculation
// ---------------------------------------------------------------------------

#[test]
fn test_item_size_matches_dynamodb_rules() {
    let mut item: Item = HashMap::new();
    // "pk" = 2 bytes name + 5 bytes string value = 7
    item.insert("pk".into(), AttributeValue::S("hello".into()));
    // "n" = 1 byte name + 2 bytes number (42 = 2 digits -> (2/2)+1 = 2) = 3
    item.insert("n".into(), AttributeValue::N("42".into()));
    // "b" = 1 byte name + 1 byte bool = 2
    item.insert("b".into(), AttributeValue::BOOL(true));

    let size = types::item_size(&item);
    assert_eq!(size, 7 + 3 + 2); // 12 bytes
}

#[test]
fn test_max_item_size_constant() {
    assert_eq!(types::MAX_ITEM_SIZE, 400 * 1024);
}

// ---------------------------------------------------------------------------
// Thread safety via Database
// ---------------------------------------------------------------------------

#[test]
fn test_database_thread_safety() {
    use dynoxide::Database;

    let db = Database::memory().unwrap();

    // Verify it can be cloned and sent across threads
    let db2 = db.clone();
    let handle = std::thread::spawn(move || {
        // db2 is usable in another thread
        drop(db2);
    });
    handle.join().unwrap();
}

// ---------------------------------------------------------------------------
// Full table lifecycle: create metadata + data table, insert, query, delete
// ---------------------------------------------------------------------------

#[test]
fn test_full_table_lifecycle() {
    let storage = Storage::memory().unwrap();

    // Create
    storage
        .insert_table_metadata(&dynoxide::storage::CreateTableMetadata {
            table_name: "Users",
            key_schema: r#"[{"AttributeName":"pk","KeyType":"HASH"},{"AttributeName":"sk","KeyType":"RANGE"}]"#,
            attribute_definitions: r#"[{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"sk","AttributeType":"S"}]"#,
            created_at: 1000,
            ..Default::default()
        })
        .unwrap();
    storage.create_data_table("Users").unwrap();

    // Verify metadata
    let meta = storage.get_table_metadata("Users").unwrap().unwrap();
    assert_eq!(meta.table_name, "Users");
    assert_eq!(meta.table_status, "ACTIVE");

    // Insert items
    storage
        .put_item(
            "Users",
            "user#1",
            "profile",
            r#"{"name":{"S":"Alice"}}"#,
            20,
        )
        .unwrap();
    storage
        .put_item(
            "Users",
            "user#1",
            "settings",
            r#"{"theme":{"S":"dark"}}"#,
            22,
        )
        .unwrap();
    storage
        .put_item("Users", "user#2", "profile", r#"{"name":{"S":"Bob"}}"#, 18)
        .unwrap();

    // Query by partition key
    let results = storage
        .query_items(
            "Users",
            "user#1",
            &dynoxide::storage::QueryParams {
                forward: true,
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(results.len(), 2);

    // Scan all
    let all = storage.scan_items("Users", &Default::default()).unwrap();
    assert_eq!(all.len(), 3);

    // Delete item
    let deleted = storage.delete_item("Users", "user#1", "profile").unwrap();
    assert!(deleted.is_some());

    // Count
    assert_eq!(storage.count_items("Users").unwrap(), 2);

    // Drop table
    storage.drop_data_table("Users").unwrap();
    storage.delete_table_metadata("Users").unwrap();
    assert!(!storage.table_exists("Users").unwrap());
}
