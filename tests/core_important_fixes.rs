//! Tests for the 11 core IMPORTANT correctness fixes from the DynamoDB compatibility audit.

use dynoxide::Database;
use dynoxide::actions::batch_write_item::{
    BatchWriteItemRequest, DeleteRequest, PutRequest, WriteRequest,
};
use dynoxide::actions::delete_item::DeleteItemRequest;
use dynoxide::actions::put_item::PutItemRequest;
use dynoxide::actions::query::QueryRequest;
use dynoxide::actions::update_item::UpdateItemRequest;
use dynoxide::errors::DynoxideError;
use dynoxide::types::AttributeValue;
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn create_pk_sk_table(db: &Database, name: &str) {
    let req: serde_json::Value = serde_json::json!({
        "TableName": name,
        "KeySchema": [
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"}
        ],
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"}
        ]
    });
    db.create_table(serde_json::from_value(req).unwrap())
        .unwrap();
}

fn item(pairs: &[(&str, AttributeValue)]) -> HashMap<String, AttributeValue> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

fn put(db: &Database, table: &str, pairs: &[(&str, AttributeValue)]) {
    db.put_item(PutItemRequest {
        table_name: table.to_string(),
        item: item(pairs),
        ..Default::default()
    })
    .unwrap();
}

// ---------------------------------------------------------------------------
// I1: PutItem ReturnValues validation
// ---------------------------------------------------------------------------

#[test]
fn test_put_item_invalid_return_values() {
    let db = Database::memory().unwrap();
    create_pk_sk_table(&db, "TestTbl");

    let result = db.put_item(PutItemRequest {
        table_name: "TestTbl".to_string(),
        item: item(&[
            ("pk", AttributeValue::S("a".into())),
            ("sk", AttributeValue::S("b".into())),
        ]),
        return_values: Some("ALL_NEW".to_string()),
        ..Default::default()
    });

    match result {
        Err(DynoxideError::ValidationException(msg)) => {
            assert!(
                msg.contains("ReturnValues"),
                "Error message should mention ReturnValues: {msg}"
            );
            assert!(
                msg.contains("ALL_OLD") && msg.contains("NONE"),
                "Error message should list valid values: {msg}"
            );
        }
        other => panic!("Expected ValidationException, got: {other:?}"),
    }
}

#[test]
fn test_put_item_valid_return_values_all_old() {
    let db = Database::memory().unwrap();
    create_pk_sk_table(&db, "TestTbl");

    // First put
    put(
        &db,
        "TestTbl",
        &[
            ("pk", AttributeValue::S("a".into())),
            ("sk", AttributeValue::S("b".into())),
            ("val", AttributeValue::S("old".into())),
        ],
    );

    // Second put with ALL_OLD should succeed
    let result = db.put_item(PutItemRequest {
        table_name: "TestTbl".to_string(),
        item: item(&[
            ("pk", AttributeValue::S("a".into())),
            ("sk", AttributeValue::S("b".into())),
            ("val", AttributeValue::S("new".into())),
        ]),
        return_values: Some("ALL_OLD".to_string()),
        ..Default::default()
    });
    assert!(result.is_ok());
    let resp = result.unwrap();
    assert!(resp.attributes.is_some());
}

// ---------------------------------------------------------------------------
// I2: DeleteItem ReturnValues validation
// ---------------------------------------------------------------------------

#[test]
fn test_delete_item_invalid_return_values() {
    let db = Database::memory().unwrap();
    create_pk_sk_table(&db, "TestTbl");

    let result = db.delete_item(DeleteItemRequest {
        table_name: "TestTbl".to_string(),
        key: item(&[
            ("pk", AttributeValue::S("a".into())),
            ("sk", AttributeValue::S("b".into())),
        ]),
        return_values: Some("ALL_NEW".to_string()),
        ..Default::default()
    });

    match result {
        Err(DynoxideError::ValidationException(msg)) => {
            assert!(msg.contains("returnValues"));
            assert!(msg.contains("ALL_OLD, NONE"));
        }
        other => panic!("Expected ValidationException, got: {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// I3: UpdateItem REMOVE/ADD/DELETE on key attributes
// ---------------------------------------------------------------------------

#[test]
fn test_update_item_remove_key_attribute_rejected() {
    let db = Database::memory().unwrap();
    create_pk_sk_table(&db, "TestTbl");

    put(
        &db,
        "TestTbl",
        &[
            ("pk", AttributeValue::S("a".into())),
            ("sk", AttributeValue::S("b".into())),
            ("val", AttributeValue::S("x".into())),
        ],
    );

    let result = db.update_item(UpdateItemRequest {
        table_name: "TestTbl".to_string(),
        key: item(&[
            ("pk", AttributeValue::S("a".into())),
            ("sk", AttributeValue::S("b".into())),
        ]),
        update_expression: Some("REMOVE sk".to_string()),
        ..Default::default()
    });

    match result {
        Err(DynoxideError::ValidationException(msg)) => {
            assert!(
                msg.contains("Cannot update attribute sk"),
                "Expected key attr error: {msg}"
            );
        }
        other => panic!("Expected ValidationException, got: {other:?}"),
    }
}

#[test]
fn test_update_item_add_key_attribute_rejected() {
    let db = Database::memory().unwrap();
    create_pk_sk_table(&db, "TestTbl");

    put(
        &db,
        "TestTbl",
        &[
            ("pk", AttributeValue::S("a".into())),
            ("sk", AttributeValue::S("b".into())),
        ],
    );

    let result = db.update_item(UpdateItemRequest {
        table_name: "TestTbl".to_string(),
        key: item(&[
            ("pk", AttributeValue::S("a".into())),
            ("sk", AttributeValue::S("b".into())),
        ]),
        update_expression: Some("ADD pk :val".to_string()),
        expression_attribute_values: Some(item(&[(":val", AttributeValue::N("1".into()))])),
        ..Default::default()
    });

    match result {
        Err(DynoxideError::ValidationException(msg)) => {
            assert!(msg.contains("Cannot update attribute pk"));
        }
        other => panic!("Expected ValidationException, got: {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// I4: PutItem with ConditionExpression is atomic
// ---------------------------------------------------------------------------

#[test]
fn test_put_item_condition_failure_leaves_no_partial_state() {
    let db = Database::memory().unwrap();
    create_pk_sk_table(&db, "TestTbl");

    // Put an initial item
    put(
        &db,
        "TestTbl",
        &[
            ("pk", AttributeValue::S("a".into())),
            ("sk", AttributeValue::S("b".into())),
            ("val", AttributeValue::S("original".into())),
        ],
    );

    // Try to put with a condition that fails (attribute_not_exists(pk) on existing item)
    let result = db.put_item(PutItemRequest {
        table_name: "TestTbl".to_string(),
        item: item(&[
            ("pk", AttributeValue::S("a".into())),
            ("sk", AttributeValue::S("b".into())),
            ("val", AttributeValue::S("replaced".into())),
        ]),
        condition_expression: Some("attribute_not_exists(pk)".to_string()),
        ..Default::default()
    });

    assert!(result.is_err());

    // Verify original item is still there, unchanged
    let mut qr = QueryRequest::default();
    qr.table_name = "TestTbl".to_string();
    qr.key_condition_expression = Some("pk = :pk AND sk = :sk".to_string());
    qr.expression_attribute_values = Some(item(&[
        (":pk", AttributeValue::S("a".into())),
        (":sk", AttributeValue::S("b".into())),
    ]));
    let query_resp = db.query(qr).unwrap();

    let items = query_resp.items.unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(
        items[0].get("val"),
        Some(&AttributeValue::S("original".into()))
    );
}

// ---------------------------------------------------------------------------
// I5: size() on N type returns no match
// ---------------------------------------------------------------------------

#[test]
fn test_size_on_number_type_returns_no_match() {
    let db = Database::memory().unwrap();
    create_pk_sk_table(&db, "TestTbl");

    put(
        &db,
        "TestTbl",
        &[
            ("pk", AttributeValue::S("a".into())),
            ("sk", AttributeValue::S("b".into())),
            ("num", AttributeValue::N("42".into())),
        ],
    );

    // Query with filter: size(num) > 0 — should NOT match since N doesn't support size()
    let mut qr = QueryRequest::default();
    qr.table_name = "TestTbl".to_string();
    qr.key_condition_expression = Some("pk = :pk".to_string());
    qr.filter_expression = Some("size(num) > :zero".to_string());
    qr.expression_attribute_values = Some(item(&[
        (":pk", AttributeValue::S("a".into())),
        (":zero", AttributeValue::N("0".into())),
    ]));
    let resp = db.query(qr).unwrap();

    // The item should not pass the filter since size() on N returns no match
    assert_eq!(resp.count, 0);
    assert_eq!(resp.scanned_count, 1);
}

#[test]
fn test_size_on_string_type_still_works() {
    let db = Database::memory().unwrap();
    create_pk_sk_table(&db, "TestTbl");

    put(
        &db,
        "TestTbl",
        &[
            ("pk", AttributeValue::S("a".into())),
            ("sk", AttributeValue::S("b".into())),
            ("name", AttributeValue::S("Alice".into())),
        ],
    );

    // size(#n) = 5, so size(#n) > :three should pass (name is a reserved word)
    let mut qr = QueryRequest::default();
    qr.table_name = "TestTbl".to_string();
    qr.key_condition_expression = Some("pk = :pk".to_string());
    qr.filter_expression = Some("size(#n) > :three".to_string());
    qr.expression_attribute_names = Some(HashMap::from([("#n".to_string(), "name".to_string())]));
    qr.expression_attribute_values = Some(item(&[
        (":pk", AttributeValue::S("a".into())),
        (":three", AttributeValue::N("3".into())),
    ]));
    let resp = db.query(qr).unwrap();

    assert_eq!(resp.count, 1);
}

// ---------------------------------------------------------------------------
// I7+I8: GSI query/scan LastEvaluatedKey includes table primary key
// ---------------------------------------------------------------------------

fn create_table_with_gsi(db: &Database) {
    let req: serde_json::Value = serde_json::json!({
        "TableName": "Orders",
        "KeySchema": [
            {"AttributeName": "UserId", "KeyType": "HASH"},
            {"AttributeName": "OrderId", "KeyType": "RANGE"}
        ],
        "AttributeDefinitions": [
            {"AttributeName": "UserId", "AttributeType": "S"},
            {"AttributeName": "OrderId", "AttributeType": "S"},
            {"AttributeName": "Status", "AttributeType": "S"}
        ],
        "GlobalSecondaryIndexes": [
            {
                "IndexName": "StatusIndex",
                "KeySchema": [
                    {"AttributeName": "Status", "KeyType": "HASH"},
                    {"AttributeName": "OrderId", "KeyType": "RANGE"}
                ],
                "Projection": {"ProjectionType": "ALL"}
            }
        ]
    });
    db.create_table(serde_json::from_value(req).unwrap())
        .unwrap();
}

#[test]
fn test_gsi_query_lek_includes_table_primary_key() {
    let db = Database::memory().unwrap();
    create_table_with_gsi(&db);

    // Insert 3 items — query GSI with Limit=1 to trigger pagination
    for i in 0..3 {
        let req: serde_json::Value = serde_json::json!({
            "TableName": "Orders",
            "Item": {
                "UserId": {"S": format!("user{i}")},
                "OrderId": {"S": format!("order{i}")},
                "Status": {"S": "ACTIVE"}
            }
        });
        db.put_item(serde_json::from_value(req).unwrap()).unwrap();
    }

    // Query GSI with Limit=1
    let req: serde_json::Value = serde_json::json!({
        "TableName": "Orders",
        "IndexName": "StatusIndex",
        "KeyConditionExpression": "#st = :s",
        "ExpressionAttributeNames": {"#st": "Status"},
        "ExpressionAttributeValues": {
            ":s": {"S": "ACTIVE"}
        },
        "Limit": 1
    });
    let resp = db.query(serde_json::from_value(req).unwrap()).unwrap();

    let lek = resp.last_evaluated_key.unwrap();

    // LEK should contain GSI keys (Status, OrderId) AND table primary key (UserId)
    assert!(
        lek.contains_key("Status"),
        "LEK should contain GSI partition key 'Status'"
    );
    assert!(
        lek.contains_key("OrderId"),
        "LEK should contain GSI sort key 'OrderId'"
    );
    assert!(
        lek.contains_key("UserId"),
        "LEK should contain table partition key 'UserId'"
    );
}

#[test]
fn test_gsi_scan_lek_includes_table_primary_key() {
    let db = Database::memory().unwrap();
    create_table_with_gsi(&db);

    for i in 0..3 {
        let req: serde_json::Value = serde_json::json!({
            "TableName": "Orders",
            "Item": {
                "UserId": {"S": format!("user{i}")},
                "OrderId": {"S": format!("order{i}")},
                "Status": {"S": "ACTIVE"}
            }
        });
        db.put_item(serde_json::from_value(req).unwrap()).unwrap();
    }

    // Scan GSI with Limit=1
    let req: serde_json::Value = serde_json::json!({
        "TableName": "Orders",
        "IndexName": "StatusIndex",
        "Limit": 1
    });
    let resp = db.scan(serde_json::from_value(req).unwrap()).unwrap();

    let lek = resp.last_evaluated_key.unwrap();

    // LEK should contain GSI keys AND table primary key
    assert!(
        lek.contains_key("Status"),
        "LEK should contain GSI partition key 'Status'"
    );
    assert!(
        lek.contains_key("UserId"),
        "LEK should contain table partition key 'UserId'"
    );
}

// ---------------------------------------------------------------------------
// I9: BatchWriteItem duplicate key detection
// ---------------------------------------------------------------------------

#[test]
fn test_batch_write_item_duplicate_keys_rejected() {
    let db = Database::memory().unwrap();
    create_pk_sk_table(&db, "TestTbl");

    let mut request_items = HashMap::new();
    request_items.insert(
        "TestTbl".to_string(),
        vec![
            WriteRequest {
                put_request: Some(PutRequest {
                    item: item(&[
                        ("pk", AttributeValue::S("a".into())),
                        ("sk", AttributeValue::S("b".into())),
                        ("val", AttributeValue::S("first".into())),
                    ]),
                }),
                delete_request: None,
            },
            WriteRequest {
                put_request: Some(PutRequest {
                    item: item(&[
                        ("pk", AttributeValue::S("a".into())),
                        ("sk", AttributeValue::S("b".into())),
                        ("val", AttributeValue::S("duplicate".into())),
                    ]),
                }),
                delete_request: None,
            },
        ],
    );

    let result = db.batch_write_item(BatchWriteItemRequest {
        request_items,
        ..Default::default()
    });

    match result {
        Err(DynoxideError::ValidationException(msg)) => {
            assert!(
                msg.contains("duplicates"),
                "Error message should mention duplicates: {msg}"
            );
        }
        other => panic!("Expected ValidationException, got: {other:?}"),
    }
}

#[test]
fn test_batch_write_item_put_and_delete_same_key_rejected() {
    let db = Database::memory().unwrap();
    create_pk_sk_table(&db, "TestTbl");

    let mut request_items = HashMap::new();
    request_items.insert(
        "TestTbl".to_string(),
        vec![
            WriteRequest {
                put_request: Some(PutRequest {
                    item: item(&[
                        ("pk", AttributeValue::S("a".into())),
                        ("sk", AttributeValue::S("b".into())),
                    ]),
                }),
                delete_request: None,
            },
            WriteRequest {
                put_request: None,
                delete_request: Some(DeleteRequest {
                    key: item(&[
                        ("pk", AttributeValue::S("a".into())),
                        ("sk", AttributeValue::S("b".into())),
                    ]),
                }),
            },
        ],
    );

    let result = db.batch_write_item(BatchWriteItemRequest {
        request_items,
        ..Default::default()
    });

    assert!(
        matches!(result, Err(DynoxideError::ValidationException(_))),
        "Put+Delete same key should be rejected"
    );
}

// ---------------------------------------------------------------------------
// UpdateItem ReturnValues validation
// ---------------------------------------------------------------------------

#[test]
fn test_update_item_invalid_return_values() {
    let db = Database::memory().unwrap();
    create_pk_sk_table(&db, "TestTbl");

    let result = db.update_item(UpdateItemRequest {
        table_name: "TestTbl".to_string(),
        key: item(&[
            ("pk", AttributeValue::S("a".into())),
            ("sk", AttributeValue::S("b".into())),
        ]),
        update_expression: Some("SET val = :v".to_string()),
        expression_attribute_values: Some(item(&[(":v", AttributeValue::S("x".into()))])),
        return_values: Some("INVALID_VALUE".to_string()),
        ..Default::default()
    });

    match result {
        Err(DynoxideError::ValidationException(msg)) => {
            assert!(msg.contains("returnValues"));
        }
        other => panic!("Expected ValidationException, got: {other:?}"),
    }
}
