//! Tests for Fix #2: Empty String / Empty Set Rejection
//!
//! DynamoDB rejects empty sets (SS: [], NS: [], BS: []) at any nesting level.
//! Empty strings in non-key attributes have been permitted since DynamoDB's 2020
//! update. These tests verify Dynoxide matches this behaviour.

use dynoxide::Database;
use dynoxide::actions::create_table::CreateTableRequest;
use dynoxide::actions::put_item::PutItemRequest;
use dynoxide::actions::update_item::UpdateItemRequest;
use dynoxide::types::*;
use std::collections::HashMap;

fn make_db() -> Database {
    let db = Database::memory().unwrap();
    db.create_table(CreateTableRequest {
        table_name: "Tbl".to_string(),
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
    db
}

fn put_item(db: &Database, item: HashMap<String, AttributeValue>) -> Result<(), String> {
    db.put_item(PutItemRequest {
        table_name: "Tbl".to_string(),
        item,
        ..Default::default()
    })
    .map(|_| ())
    .map_err(|e| format!("{e}"))
}

#[test]
fn test_accept_empty_string() {
    // Empty strings in non-key attributes are permitted since DynamoDB's 2020 update.
    let db = make_db();
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("key1".to_string()));
    item.insert("name".to_string(), AttributeValue::S("".to_string()));
    put_item(&db, item).unwrap();
}

#[test]
fn test_reject_empty_ss() {
    let db = make_db();
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("key1".to_string()));
    item.insert("tags".to_string(), AttributeValue::SS(vec![]));
    let err = put_item(&db, item).unwrap_err();
    assert!(
        err.contains("string set") && err.contains("may not be empty"),
        "unexpected error: {err}"
    );
}

#[test]
fn test_reject_empty_ns() {
    let db = make_db();
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("key1".to_string()));
    item.insert("nums".to_string(), AttributeValue::NS(vec![]));
    let err = put_item(&db, item).unwrap_err();
    assert!(
        err.contains("number set") && err.contains("may not be empty"),
        "unexpected error: {err}"
    );
}

#[test]
fn test_reject_empty_bs() {
    let db = make_db();
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("key1".to_string()));
    item.insert("bins".to_string(), AttributeValue::BS(vec![]));
    let err = put_item(&db, item).unwrap_err();
    assert!(
        err.contains("Binary sets") && err.contains("not be empty"),
        "unexpected error: {err}"
    );
}

#[test]
fn test_accept_empty_string_nested_in_map() {
    // Empty strings in non-key attributes are permitted since DynamoDB's 2020 update.
    let db = make_db();
    let mut nested = HashMap::new();
    nested.insert("inner".to_string(), AttributeValue::S("".to_string()));
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("key1".to_string()));
    item.insert("data".to_string(), AttributeValue::M(nested));
    put_item(&db, item).unwrap();
}

#[test]
fn test_reject_empty_set_nested_in_list() {
    let db = make_db();
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("key1".to_string()));
    item.insert(
        "data".to_string(),
        AttributeValue::L(vec![AttributeValue::SS(vec![])]),
    );
    let err = put_item(&db, item).unwrap_err();
    assert!(
        err.contains("string set") && err.contains("may not be empty"),
        "unexpected error: {err}"
    );
}

#[test]
fn test_update_item_accept_empty_string() {
    // Empty strings in non-key attributes are permitted since DynamoDB's 2020 update.
    let db = make_db();
    // First put a valid item
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("key1".to_string()));
    item.insert("name".to_string(), AttributeValue::S("hello".to_string()));
    put_item(&db, item).unwrap();

    // Update to set an empty string — should succeed
    let mut key = HashMap::new();
    key.insert("pk".to_string(), AttributeValue::S("key1".to_string()));
    let mut expr_values = HashMap::new();
    expr_values.insert(":val".to_string(), AttributeValue::S("".to_string()));

    db.update_item(UpdateItemRequest {
        table_name: "Tbl".to_string(),
        key,
        update_expression: Some("SET #n = :val".to_string()),
        expression_attribute_names: Some({
            let mut m = HashMap::new();
            m.insert("#n".to_string(), "name".to_string());
            m
        }),
        expression_attribute_values: Some(expr_values),
        ..Default::default()
    })
    .unwrap();
}

#[test]
fn test_valid_non_empty_values_succeed() {
    let db = make_db();
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("key1".to_string()));
    item.insert("name".to_string(), AttributeValue::S("hello".to_string()));
    item.insert(
        "tags".to_string(),
        AttributeValue::SS(vec!["a".to_string()]),
    );
    item.insert(
        "nums".to_string(),
        AttributeValue::NS(vec!["42".to_string()]),
    );
    item.insert("bins".to_string(), AttributeValue::BS(vec![vec![1, 2, 3]]));
    put_item(&db, item).unwrap();
}

#[test]
fn test_batch_write_accept_empty_string() {
    // Empty strings in non-key attributes are permitted since DynamoDB's 2020 update.
    let db = make_db();

    let req: serde_json::Value = serde_json::json!({
        "RequestItems": {
            "Tbl": [{
                "PutRequest": {
                    "Item": {
                        "pk": {"S": "key1"},
                        "name": {"S": ""}
                    }
                }
            }]
        }
    });
    let batch_req: dynoxide::actions::batch_write_item::BatchWriteItemRequest =
        serde_json::from_value(req).unwrap();
    db.batch_write_item(batch_req).unwrap();
}
