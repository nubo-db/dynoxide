//! Tests for unused ExpressionAttributeNames/Values rejection.
//!
//! DynamoDB rejects requests containing ExpressionAttributeNames or ExpressionAttributeValues
//! entries that are NOT referenced by any expression in the request.

use dynoxide::Database;
use dynoxide::actions::create_table::CreateTableRequest;
use dynoxide::actions::get_item::GetItemRequest;
use dynoxide::actions::put_item::PutItemRequest;
use dynoxide::actions::query::QueryRequest;
use dynoxide::actions::update_item::UpdateItemRequest;
use dynoxide::types::*;
use std::collections::HashMap;

fn make_db() -> Database {
    Database::memory().unwrap()
}

fn create_table(db: &Database, name: &str) {
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

fn put_item(db: &Database, table: &str, pk: &str, sk: &str) {
    db.put_item(PutItemRequest {
        table_name: table.to_string(),
        item: HashMap::from([
            ("pk".to_string(), AttributeValue::S(pk.to_string())),
            ("sk".to_string(), AttributeValue::S(sk.to_string())),
            (
                "status".to_string(),
                AttributeValue::S("active".to_string()),
            ),
        ]),
        ..Default::default()
    })
    .unwrap();
}

#[test]
fn test_put_item_with_unused_name_rejected() {
    let db = make_db();
    create_table(&db, "TestTable");

    let result = db.put_item(PutItemRequest {
        table_name: "TestTable".to_string(),
        item: HashMap::from([
            ("pk".to_string(), AttributeValue::S("k1".to_string())),
            ("sk".to_string(), AttributeValue::S("s1".to_string())),
        ]),
        condition_expression: Some("attribute_not_exists(pk)".to_string()),
        expression_attribute_names: Some(HashMap::from([(
            "#unused".to_string(),
            "something".to_string(),
        )])),
        ..Default::default()
    });

    let err = result.unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("ExpressionAttributeNames unused in expressions"),
        "Expected unused names error, got: {msg}"
    );
    assert!(msg.contains("#unused"), "Expected #unused in error: {msg}");
}

#[test]
fn test_put_item_with_unused_value_rejected() {
    let db = make_db();
    create_table(&db, "TestTable");

    let result = db.put_item(PutItemRequest {
        table_name: "TestTable".to_string(),
        item: HashMap::from([
            ("pk".to_string(), AttributeValue::S("k1".to_string())),
            ("sk".to_string(), AttributeValue::S("s1".to_string())),
        ]),
        condition_expression: Some("attribute_not_exists(pk)".to_string()),
        expression_attribute_values: Some(HashMap::from([(
            ":unused".to_string(),
            AttributeValue::S("val".to_string()),
        )])),
        ..Default::default()
    });

    let err = result.unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("ExpressionAttributeValues unused in expressions"),
        "Expected unused values error, got: {msg}"
    );
    assert!(msg.contains(":unused"), "Expected :unused in error: {msg}");
}

#[test]
fn test_put_item_unused_name_message_is_bare() {
    // The unused-attribute rejection carries no
    // "1 validation error detected: " envelope on PutItem.
    let db = make_db();
    create_table(&db, "TestTable");

    let err = db
        .put_item(PutItemRequest {
            table_name: "TestTable".to_string(),
            item: HashMap::from([
                ("pk".to_string(), AttributeValue::S("k1".to_string())),
                ("sk".to_string(), AttributeValue::S("s1".to_string())),
            ]),
            condition_expression: Some("attribute_not_exists(pk)".to_string()),
            expression_attribute_names: Some(HashMap::from([(
                "#unused".to_string(),
                "something".to_string(),
            )])),
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "Value provided in ExpressionAttributeNames unused in expressions: keys: {#unused}"
    );
}

#[test]
fn test_update_item_unused_name_message_is_bare() {
    // The unused-attribute rejection carries no
    // "1 validation error detected: " envelope on UpdateItem either,
    // matching PutItem.
    let db = make_db();
    create_table(&db, "TestTable");

    let err = db
        .update_item(UpdateItemRequest {
            table_name: "TestTable".to_string(),
            key: HashMap::from([
                ("pk".to_string(), AttributeValue::S("k1".to_string())),
                ("sk".to_string(), AttributeValue::S("s1".to_string())),
            ]),
            update_expression: Some("SET flag = :v".to_string()),
            expression_attribute_values: Some(HashMap::from([(
                ":v".to_string(),
                AttributeValue::S("on".to_string()),
            )])),
            expression_attribute_names: Some(HashMap::from([(
                "#unused".to_string(),
                "something".to_string(),
            )])),
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "Value provided in ExpressionAttributeNames unused in expressions: keys: {#unused}"
    );
}

#[test]
fn test_query_name_used_across_expressions_is_valid() {
    // #name1 used in KeyConditionExpression, #name2 used in FilterExpression
    // Both count as used - the request should succeed.
    let db = make_db();
    create_table(&db, "TestTable");
    put_item(&db, "TestTable", "user#1", "2024-01-01");

    let result = db.query({
        QueryRequest {
            table_name: "TestTable".to_string(),
            key_condition_expression: Some("#pk = :pk".to_string()),
            filter_expression: Some("#s = :status".to_string()),
            expression_attribute_names: Some(HashMap::from([
                ("#pk".to_string(), "pk".to_string()),
                ("#s".to_string(), "status".to_string()),
            ])),
            expression_attribute_values: Some(HashMap::from([
                (":pk".to_string(), AttributeValue::S("user#1".to_string())),
                (
                    ":status".to_string(),
                    AttributeValue::S("active".to_string()),
                ),
            ])),
            ..Default::default()
        }
    });

    assert!(result.is_ok(), "Expected success, got: {:?}", result.err());
}

#[test]
fn test_query_name_used_only_in_projection_is_valid() {
    let db = make_db();
    create_table(&db, "TestTable");
    put_item(&db, "TestTable", "user#1", "2024-01-01");

    let result = db.query({
        QueryRequest {
            table_name: "TestTable".to_string(),
            key_condition_expression: Some("pk = :pk".to_string()),
            projection_expression: Some("#s".to_string()),
            expression_attribute_names: Some(HashMap::from([(
                "#s".to_string(),
                "status".to_string(),
            )])),
            expression_attribute_values: Some(HashMap::from([(
                ":pk".to_string(),
                AttributeValue::S("user#1".to_string()),
            )])),
            ..Default::default()
        }
    });

    assert!(result.is_ok(), "Expected success, got: {:?}", result.err());
}

#[test]
fn test_all_names_values_used_is_valid() {
    let db = make_db();
    create_table(&db, "TestTable");
    put_item(&db, "TestTable", "k1", "s1");

    let result = db.put_item(PutItemRequest {
        table_name: "TestTable".to_string(),
        item: HashMap::from([
            ("pk".to_string(), AttributeValue::S("k1".to_string())),
            ("sk".to_string(), AttributeValue::S("s1".to_string())),
        ]),
        condition_expression: Some("#p = :val".to_string()),
        expression_attribute_names: Some(HashMap::from([("#p".to_string(), "pk".to_string())])),
        expression_attribute_values: Some(HashMap::from([(
            ":val".to_string(),
            AttributeValue::S("k1".to_string()),
        )])),
        ..Default::default()
    });

    assert!(result.is_ok(), "Expected success, got: {:?}", result.err());
}

#[test]
fn test_multiple_unused_entries_listed_in_error() {
    let db = make_db();
    create_table(&db, "TestTable");

    let result = db.put_item(PutItemRequest {
        table_name: "TestTable".to_string(),
        item: HashMap::from([
            ("pk".to_string(), AttributeValue::S("k1".to_string())),
            ("sk".to_string(), AttributeValue::S("s1".to_string())),
        ]),
        condition_expression: Some("attribute_not_exists(pk)".to_string()),
        expression_attribute_names: Some(HashMap::from([
            ("#a".to_string(), "alpha".to_string()),
            ("#b".to_string(), "beta".to_string()),
        ])),
        ..Default::default()
    });

    let err = result.unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("#a") && msg.contains("#b"),
        "Expected both #a and #b in error: {msg}"
    );
}

#[test]
fn test_get_item_with_unused_name_in_projection_rejected() {
    let db = make_db();
    create_table(&db, "TestTable");
    put_item(&db, "TestTable", "k1", "s1");

    let result = db.get_item(GetItemRequest {
        table_name: "TestTable".to_string(),
        key: HashMap::from([
            ("pk".to_string(), AttributeValue::S("k1".to_string())),
            ("sk".to_string(), AttributeValue::S("s1".to_string())),
        ]),
        projection_expression: Some("#used".to_string()),
        expression_attribute_names: Some(HashMap::from([
            ("#used".to_string(), "status".to_string()),
            ("#unused".to_string(), "other".to_string()),
        ])),
        ..Default::default()
    });

    let err = result.unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("ExpressionAttributeNames unused in expressions"),
        "Expected unused names error, got: {msg}"
    );
    assert!(msg.contains("#unused"), "Expected #unused in error: {msg}");
}
