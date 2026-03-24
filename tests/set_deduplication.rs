//! Tests for set deduplication and duplicate rejection.
//!
//! DynamoDB rejects duplicate values in SS/NS/BS sets during PutItem validation.
//! The ADD operation in UpdateItem merges sets without creating duplicates.

use dynoxide::Database;
use dynoxide::actions::create_table::CreateTableRequest;
use dynoxide::actions::get_item::GetItemRequest;
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

fn get_item(db: &Database, pk: &str) -> HashMap<String, AttributeValue> {
    let resp = db
        .get_item(GetItemRequest {
            table_name: "Tbl".to_string(),
            key: {
                let mut k = HashMap::new();
                k.insert("pk".to_string(), AttributeValue::S(pk.to_string()));
                k
            },
            ..Default::default()
        })
        .unwrap();
    resp.item.unwrap()
}

#[test]
fn test_ss_duplicate_rejected() {
    let db = make_db();
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("k1".to_string()));
    item.insert(
        "tags".to_string(),
        AttributeValue::SS(vec!["a".to_string(), "b".to_string(), "a".to_string()]),
    );
    let result = db.put_item(PutItemRequest {
        table_name: "Tbl".to_string(),
        item,
        ..Default::default()
    });
    assert!(result.is_err(), "expected duplicate SS to be rejected");
}

#[test]
fn test_ns_numeric_duplicate_rejected() {
    let db = make_db();
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("k1".to_string()));
    item.insert(
        "nums".to_string(),
        AttributeValue::NS(vec!["1".to_string(), "1.0".to_string()]),
    );
    let result = db.put_item(PutItemRequest {
        table_name: "Tbl".to_string(),
        item,
        ..Default::default()
    });
    assert!(result.is_err(), "expected duplicate NS to be rejected");
}

#[test]
fn test_bs_duplicate_rejected() {
    let db = make_db();
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("k1".to_string()));
    item.insert(
        "bins".to_string(),
        AttributeValue::BS(vec![vec![1, 2], vec![3, 4], vec![1, 2]]),
    );
    let result = db.put_item(PutItemRequest {
        table_name: "Tbl".to_string(),
        item,
        ..Default::default()
    });
    assert!(result.is_err(), "expected duplicate BS to be rejected");
}

#[test]
fn test_add_to_ss_no_duplicates() {
    let db = make_db();
    // Put initial item with unique SS
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("k1".to_string()));
    item.insert(
        "tags".to_string(),
        AttributeValue::SS(vec!["a".to_string(), "b".to_string()]),
    );
    db.put_item(PutItemRequest {
        table_name: "Tbl".to_string(),
        item,
        ..Default::default()
    })
    .unwrap();

    // ADD with overlapping values — should merge without creating duplicates
    let mut key = HashMap::new();
    key.insert("pk".to_string(), AttributeValue::S("k1".to_string()));
    let mut expr_values = HashMap::new();
    expr_values.insert(
        ":vals".to_string(),
        AttributeValue::SS(vec!["b".to_string(), "c".to_string()]),
    );
    db.update_item(UpdateItemRequest {
        table_name: "Tbl".to_string(),
        key,
        update_expression: Some("ADD tags :vals".to_string()),
        expression_attribute_values: Some(expr_values),
        ..Default::default()
    })
    .unwrap();

    let stored = get_item(&db, "k1");
    match &stored["tags"] {
        AttributeValue::SS(set) => {
            assert_eq!(set.len(), 3, "expected 3 unique values, got {:?}", set);
        }
        other => panic!("expected SS, got {:?}", other),
    }
}

#[test]
fn test_nested_set_in_map_duplicate_rejected() {
    let db = make_db();
    let mut nested = HashMap::new();
    nested.insert(
        "inner_tags".to_string(),
        AttributeValue::SS(vec!["x".to_string(), "y".to_string(), "x".to_string()]),
    );
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("k1".to_string()));
    item.insert("data".to_string(), AttributeValue::M(nested));
    let result = db.put_item(PutItemRequest {
        table_name: "Tbl".to_string(),
        item,
        ..Default::default()
    });
    assert!(
        result.is_err(),
        "expected nested duplicate SS to be rejected"
    );
}
