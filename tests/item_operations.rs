//! Phase 3a integration tests: Item operations (PutItem, GetItem, DeleteItem).

use dynoxide::Database;
use dynoxide::actions::create_table::CreateTableRequest;
use dynoxide::actions::delete_item::DeleteItemRequest;
use dynoxide::actions::get_item::GetItemRequest;
use dynoxide::actions::put_item::PutItemRequest;
use dynoxide::types::*;
use std::collections::HashMap;

fn make_db() -> Database {
    Database::memory().unwrap()
}

fn create_hash_only_table(db: &Database, name: &str) {
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

fn create_composite_table(db: &Database, name: &str) {
    db.create_table(CreateTableRequest {
        table_name: name.to_string(),
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

fn make_item(pairs: &[(&str, AttributeValue)]) -> HashMap<String, AttributeValue> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

fn key_map(pairs: &[(&str, AttributeValue)]) -> HashMap<String, AttributeValue> {
    make_item(pairs)
}

// ---------------------------------------------------------------------------
// PutItem
// ---------------------------------------------------------------------------

#[test]
fn test_put_item_hash_only() {
    let db = make_db();
    create_hash_only_table(&db, "Items");

    let resp = db
        .put_item(PutItemRequest {
            table_name: "Items".to_string(),
            item: make_item(&[
                ("pk", AttributeValue::S("user#1".into())),
                ("name", AttributeValue::S("Alice".into())),
            ]),
            return_values: None,
            condition_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            ..Default::default()
        })
        .unwrap();

    assert!(resp.attributes.is_none());
}

#[test]
fn test_put_item_composite_key() {
    let db = make_db();
    create_composite_table(&db, "Items");

    db.put_item(PutItemRequest {
        table_name: "Items".to_string(),
        item: make_item(&[
            ("pk", AttributeValue::S("user#1".into())),
            ("sk", AttributeValue::S("profile".into())),
            ("name", AttributeValue::S("Alice".into())),
        ]),
        return_values: None,
        condition_expression: None,
        expression_attribute_names: None,
        expression_attribute_values: None,
        ..Default::default()
    })
    .unwrap();

    // Get it back
    let resp = db
        .get_item(GetItemRequest {
            table_name: "Items".to_string(),
            key: key_map(&[
                ("pk", AttributeValue::S("user#1".into())),
                ("sk", AttributeValue::S("profile".into())),
            ]),
            consistent_read: None,
            projection_expression: None,
            ..Default::default()
        })
        .unwrap();

    let item = resp.item.unwrap();
    assert_eq!(item["name"], AttributeValue::S("Alice".into()));
}

#[test]
fn test_put_item_all_attribute_types() {
    let db = make_db();
    create_hash_only_table(&db, "AllTypes");

    let mut nested = HashMap::new();
    nested.insert("inner".to_string(), AttributeValue::S("value".into()));

    let item = make_item(&[
        ("pk", AttributeValue::S("test".into())),
        ("str_val", AttributeValue::S("hello".into())),
        ("num_val", AttributeValue::N("42.5".into())),
        ("bin_val", AttributeValue::B(vec![1, 2, 3])),
        ("bool_val", AttributeValue::BOOL(true)),
        ("null_val", AttributeValue::NULL(true)),
        ("str_set", AttributeValue::SS(vec!["a".into(), "b".into()])),
        ("num_set", AttributeValue::NS(vec!["1".into(), "2".into()])),
        ("bin_set", AttributeValue::BS(vec![vec![10], vec![20]])),
        (
            "list_val",
            AttributeValue::L(vec![
                AttributeValue::S("item".into()),
                AttributeValue::N("99".into()),
            ]),
        ),
        ("map_val", AttributeValue::M(nested)),
    ]);

    db.put_item(PutItemRequest {
        table_name: "AllTypes".to_string(),
        item,
        return_values: None,
        condition_expression: None,
        expression_attribute_names: None,
        expression_attribute_values: None,
        ..Default::default()
    })
    .unwrap();

    let resp = db
        .get_item(GetItemRequest {
            table_name: "AllTypes".to_string(),
            key: key_map(&[("pk", AttributeValue::S("test".into()))]),
            consistent_read: None,
            projection_expression: None,
            ..Default::default()
        })
        .unwrap();

    let item = resp.item.unwrap();
    assert_eq!(item["str_val"], AttributeValue::S("hello".into()));
    assert_eq!(item["num_val"], AttributeValue::N("42.5".into()));
    assert_eq!(item["bin_val"], AttributeValue::B(vec![1, 2, 3]));
    assert_eq!(item["bool_val"], AttributeValue::BOOL(true));
    assert_eq!(item["null_val"], AttributeValue::NULL(true));
    assert!(matches!(&item["list_val"], AttributeValue::L(l) if l.len() == 2));
    assert!(matches!(&item["map_val"], AttributeValue::M(m) if m.contains_key("inner")));
}

#[test]
fn test_put_item_return_values_all_old() {
    let db = make_db();
    create_hash_only_table(&db, "Items");

    // First put — no old item
    let resp = db
        .put_item(PutItemRequest {
            table_name: "Items".to_string(),
            item: make_item(&[
                ("pk", AttributeValue::S("key1".into())),
                ("val", AttributeValue::S("first".into())),
            ]),
            return_values: Some("ALL_OLD".into()),
            condition_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            ..Default::default()
        })
        .unwrap();
    assert!(resp.attributes.is_none());

    // Second put — should get old item back
    let resp = db
        .put_item(PutItemRequest {
            table_name: "Items".to_string(),
            item: make_item(&[
                ("pk", AttributeValue::S("key1".into())),
                ("val", AttributeValue::S("second".into())),
            ]),
            return_values: Some("ALL_OLD".into()),
            condition_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            ..Default::default()
        })
        .unwrap();

    let old = resp.attributes.unwrap();
    assert_eq!(old["val"], AttributeValue::S("first".into()));
}

#[test]
fn test_put_item_missing_partition_key() {
    let db = make_db();
    create_hash_only_table(&db, "Items");

    let err = db
        .put_item(PutItemRequest {
            table_name: "Items".to_string(),
            item: make_item(&[("name", AttributeValue::S("Alice".into()))]),
            return_values: None,
            condition_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            ..Default::default()
        })
        .unwrap_err();
    assert!(err.to_string().contains("Missing the key pk"));
}

#[test]
fn test_put_item_empty_string_key() {
    let db = make_db();
    create_hash_only_table(&db, "Items");

    let err = db
        .put_item(PutItemRequest {
            table_name: "Items".to_string(),
            item: make_item(&[("pk", AttributeValue::S("".into()))]),
            return_values: None,
            condition_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            ..Default::default()
        })
        .unwrap_err();
    assert!(err.to_string().contains("empty string"));
}

#[test]
fn test_put_item_nonexistent_table() {
    let db = make_db();
    let err = db
        .put_item(PutItemRequest {
            table_name: "Ghost".to_string(),
            item: make_item(&[("pk", AttributeValue::S("x".into()))]),
            return_values: None,
            condition_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            ..Default::default()
        })
        .unwrap_err();
    assert!(err.to_string().contains("not found"));
}

// ---------------------------------------------------------------------------
// GetItem
// ---------------------------------------------------------------------------

#[test]
fn test_get_item_existing() {
    let db = make_db();
    create_hash_only_table(&db, "Items");

    db.put_item(PutItemRequest {
        table_name: "Items".to_string(),
        item: make_item(&[
            ("pk", AttributeValue::S("user#1".into())),
            ("name", AttributeValue::S("Alice".into())),
        ]),
        return_values: None,
        condition_expression: None,
        expression_attribute_names: None,
        expression_attribute_values: None,
        ..Default::default()
    })
    .unwrap();

    let resp = db
        .get_item(GetItemRequest {
            table_name: "Items".to_string(),
            key: key_map(&[("pk", AttributeValue::S("user#1".into()))]),
            consistent_read: None,
            projection_expression: None,
            ..Default::default()
        })
        .unwrap();

    assert!(resp.item.is_some());
    assert_eq!(
        resp.item.unwrap()["name"],
        AttributeValue::S("Alice".into())
    );
}

#[test]
fn test_get_item_nonexisting() {
    let db = make_db();
    create_hash_only_table(&db, "Items");

    let resp = db
        .get_item(GetItemRequest {
            table_name: "Items".to_string(),
            key: key_map(&[("pk", AttributeValue::S("ghost".into()))]),
            consistent_read: None,
            projection_expression: None,
            ..Default::default()
        })
        .unwrap();

    assert!(resp.item.is_none());
}

#[test]
fn test_get_item_wrong_key_count() {
    let db = make_db();
    create_hash_only_table(&db, "Items");

    let err = db
        .get_item(GetItemRequest {
            table_name: "Items".to_string(),
            key: key_map(&[
                ("pk", AttributeValue::S("x".into())),
                ("extra", AttributeValue::S("y".into())),
            ]),
            consistent_read: None,
            projection_expression: None,
            ..Default::default()
        })
        .unwrap_err();
    assert!(err.to_string().contains("does not match the schema"));
}

// ---------------------------------------------------------------------------
// DeleteItem
// ---------------------------------------------------------------------------

#[test]
fn test_delete_item_existing() {
    let db = make_db();
    create_hash_only_table(&db, "Items");

    db.put_item(PutItemRequest {
        table_name: "Items".to_string(),
        item: make_item(&[
            ("pk", AttributeValue::S("del-me".into())),
            ("val", AttributeValue::S("bye".into())),
        ]),
        return_values: None,
        condition_expression: None,
        expression_attribute_names: None,
        expression_attribute_values: None,
        ..Default::default()
    })
    .unwrap();

    let resp = db
        .delete_item(DeleteItemRequest {
            table_name: "Items".to_string(),
            key: key_map(&[("pk", AttributeValue::S("del-me".into()))]),
            return_values: Some("ALL_OLD".into()),
            condition_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            ..Default::default()
        })
        .unwrap();

    let old = resp.attributes.unwrap();
    assert_eq!(old["val"], AttributeValue::S("bye".into()));

    // Verify it's gone
    let get_resp = db
        .get_item(GetItemRequest {
            table_name: "Items".to_string(),
            key: key_map(&[("pk", AttributeValue::S("del-me".into()))]),
            consistent_read: None,
            projection_expression: None,
            ..Default::default()
        })
        .unwrap();
    assert!(get_resp.item.is_none());
}

#[test]
fn test_delete_item_nonexisting() {
    let db = make_db();
    create_hash_only_table(&db, "Items");

    let resp = db
        .delete_item(DeleteItemRequest {
            table_name: "Items".to_string(),
            key: key_map(&[("pk", AttributeValue::S("ghost".into()))]),
            return_values: Some("ALL_OLD".into()),
            condition_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            ..Default::default()
        })
        .unwrap();

    // No old item to return
    assert!(resp.attributes.is_none());
}

// ---------------------------------------------------------------------------
// JSON round-trip
// ---------------------------------------------------------------------------

#[test]
fn test_put_item_from_json() {
    let db = make_db();
    create_hash_only_table(&db, "Items");

    let json = r#"{
        "TableName": "Items",
        "Item": {
            "pk": {"S": "from-json"},
            "data": {"N": "42"}
        }
    }"#;

    let request: PutItemRequest = serde_json::from_str(json).unwrap();
    db.put_item(request).unwrap();

    let resp = db
        .get_item(GetItemRequest {
            table_name: "Items".to_string(),
            key: key_map(&[("pk", AttributeValue::S("from-json".into()))]),
            consistent_read: None,
            projection_expression: None,
            ..Default::default()
        })
        .unwrap();

    let item = resp.item.unwrap();
    assert_eq!(item["data"], AttributeValue::N("42".into()));
}
