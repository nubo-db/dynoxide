//! Transaction integration tests.

use dynoxide::Database;
use dynoxide::types::AttributeValue;
use serde_json::json;

fn create_test_table(db: &Database, name: &str) {
    let req: serde_json::Value = json!({
        "TableName": name,
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}]
    });
    let create_req = serde_json::from_value(req).unwrap();
    db.create_table(create_req).unwrap();
}

fn put_item(db: &Database, table: &str, pk: &str, val: &str) {
    let req: serde_json::Value = json!({
        "TableName": table,
        "Item": {"pk": {"S": pk}, "val": {"S": val}}
    });
    let put_req = serde_json::from_value(req).unwrap();
    db.put_item(put_req).unwrap();
}

fn get_val(db: &Database, table: &str, pk: &str) -> Option<String> {
    let req: serde_json::Value = json!({
        "TableName": table,
        "Key": {"pk": {"S": pk}}
    });
    let get_req = serde_json::from_value(req).unwrap();
    let resp = db.get_item(get_req).unwrap();
    resp.item.and_then(|item| match item.get("val") {
        Some(AttributeValue::S(s)) => Some(s.clone()),
        _ => None,
    })
}

#[test]
fn test_transact_write_multiple_puts() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");
    create_test_table(&db, "Table2");

    let req: serde_json::Value = json!({
        "TransactItems": [
            {"Put": {"TableName": "Table1", "Item": {"pk": {"S": "a"}, "val": {"S": "1"}}}},
            {"Put": {"TableName": "Table2", "Item": {"pk": {"S": "b"}, "val": {"S": "2"}}}}
        ]
    });
    let transact_req = serde_json::from_value(req).unwrap();
    db.transact_write_items(transact_req).unwrap();

    assert_eq!(get_val(&db, "Table1", "a"), Some("1".to_string()));
    assert_eq!(get_val(&db, "Table2", "b"), Some("2".to_string()));
}

#[test]
fn test_transact_write_condition_fails_rolls_back() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");
    put_item(&db, "Table1", "existing", "old");

    // Transaction: put a new item + condition check on existing (that will fail)
    let req: serde_json::Value = json!({
        "TransactItems": [
            {"Put": {"TableName": "Table1", "Item": {"pk": {"S": "new_item"}, "val": {"S": "new"}}}},
            {"ConditionCheck": {
                "TableName": "Table1",
                "Key": {"pk": {"S": "existing"}},
                "ConditionExpression": "attribute_not_exists(pk)"
            }}
        ]
    });
    let transact_req = serde_json::from_value(req).unwrap();
    let err = db.transact_write_items(transact_req).unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("Transaction cancelled"));

    // The put should have been rolled back
    assert_eq!(get_val(&db, "Table1", "new_item"), None);
    // existing item unchanged
    assert_eq!(get_val(&db, "Table1", "existing"), Some("old".to_string()));
}

#[test]
fn test_transact_write_mix_put_update_delete() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");
    put_item(&db, "Table1", "to_update", "before");
    put_item(&db, "Table1", "to_delete", "doomed");

    let req: serde_json::Value = json!({
        "TransactItems": [
            {"Put": {"TableName": "Table1", "Item": {"pk": {"S": "new"}, "val": {"S": "created"}}}},
            {"Update": {
                "TableName": "Table1",
                "Key": {"pk": {"S": "to_update"}},
                "UpdateExpression": "SET val = :v",
                "ExpressionAttributeValues": {":v": {"S": "after"}}
            }},
            {"Delete": {"TableName": "Table1", "Key": {"pk": {"S": "to_delete"}}}}
        ]
    });
    let transact_req = serde_json::from_value(req).unwrap();
    db.transact_write_items(transact_req).unwrap();

    assert_eq!(get_val(&db, "Table1", "new"), Some("created".to_string()));
    assert_eq!(
        get_val(&db, "Table1", "to_update"),
        Some("after".to_string())
    );
    assert_eq!(get_val(&db, "Table1", "to_delete"), None);
}

#[test]
fn test_transact_write_duplicate_target_rejected() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");

    let req: serde_json::Value = json!({
        "TransactItems": [
            {"Put": {"TableName": "Table1", "Item": {"pk": {"S": "same"}}}},
            {"Delete": {"TableName": "Table1", "Key": {"pk": {"S": "same"}}}}
        ]
    });
    let transact_req = serde_json::from_value(req).unwrap();
    let err = db.transact_write_items(transact_req).unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("multiple operations on one item"));
}

#[test]
fn test_transact_write_exceeds_100_actions() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");

    let items: Vec<serde_json::Value> = (0..101)
        .map(|i| json!({"Put": {"TableName": "Table1", "Item": {"pk": {"S": format!("k{i}")}}}}))
        .collect();

    let req: serde_json::Value = json!({"TransactItems": items});
    let transact_req = serde_json::from_value(req).unwrap();
    let err = db.transact_write_items(transact_req).unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("100"));
}

#[test]
fn test_transact_write_cancellation_reasons() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");
    put_item(&db, "Table1", "exists", "val");

    // First action succeeds condition, second fails
    let req: serde_json::Value = json!({
        "TransactItems": [
            {"Put": {
                "TableName": "Table1",
                "Item": {"pk": {"S": "new"}},
                "ConditionExpression": "attribute_not_exists(pk)"
            }},
            {"ConditionCheck": {
                "TableName": "Table1",
                "Key": {"pk": {"S": "exists"}},
                "ConditionExpression": "attribute_not_exists(pk)"
            }}
        ]
    });
    let transact_req = serde_json::from_value(req).unwrap();
    let err = db.transact_write_items(transact_req).unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("ConditionalCheckFailed"));
    assert!(msg.contains("None"));
}

#[test]
fn test_transact_get_multiple_tables() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");
    create_test_table(&db, "Table2");
    put_item(&db, "Table1", "a", "val1");
    put_item(&db, "Table2", "b", "val2");

    let req: serde_json::Value = json!({
        "TransactItems": [
            {"Get": {"TableName": "Table1", "Key": {"pk": {"S": "a"}}}},
            {"Get": {"TableName": "Table2", "Key": {"pk": {"S": "b"}}}}
        ]
    });
    let transact_req = serde_json::from_value(req).unwrap();
    let resp = db.transact_get_items(transact_req).unwrap();

    assert_eq!(resp.responses.len(), 2);
    let item1 = resp.responses[0].item.as_ref().unwrap();
    assert!(matches!(item1.get("val"), Some(AttributeValue::S(s)) if s == "val1"));
    let item2 = resp.responses[1].item.as_ref().unwrap();
    assert!(matches!(item2.get("val"), Some(AttributeValue::S(s)) if s == "val2"));
}

#[test]
fn test_transact_get_item_not_found() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");
    put_item(&db, "Table1", "exists", "val");

    let req: serde_json::Value = json!({
        "TransactItems": [
            {"Get": {"TableName": "Table1", "Key": {"pk": {"S": "exists"}}}},
            {"Get": {"TableName": "Table1", "Key": {"pk": {"S": "missing"}}}}
        ]
    });
    let transact_req = serde_json::from_value(req).unwrap();
    let resp = db.transact_get_items(transact_req).unwrap();

    assert_eq!(resp.responses.len(), 2);
    assert!(resp.responses[0].item.is_some());
    assert!(resp.responses[1].item.is_none());
}

#[test]
fn test_transact_get_with_projection() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");

    let req: serde_json::Value = json!({
        "TableName": "Table1",
        "Item": {"pk": {"S": "a"}, "val": {"S": "v"}, "extra": {"S": "e"}}
    });
    let put_req = serde_json::from_value(req).unwrap();
    db.put_item(put_req).unwrap();

    let req: serde_json::Value = json!({
        "TransactItems": [
            {"Get": {
                "TableName": "Table1",
                "Key": {"pk": {"S": "a"}},
                "ProjectionExpression": "val"
            }}
        ]
    });
    let transact_req = serde_json::from_value(req).unwrap();
    let resp = db.transact_get_items(transact_req).unwrap();

    let item = resp.responses[0].item.as_ref().unwrap();
    assert!(item.contains_key("pk")); // key always included
    assert!(item.contains_key("val"));
    assert!(!item.contains_key("extra"));
}

#[test]
fn test_transact_write_with_condition_expression() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");

    // Put with condition that pk doesn't exist → succeeds
    let req: serde_json::Value = json!({
        "TransactItems": [
            {"Put": {
                "TableName": "Table1",
                "Item": {"pk": {"S": "new"}, "val": {"S": "created"}},
                "ConditionExpression": "attribute_not_exists(pk)"
            }}
        ]
    });
    let transact_req = serde_json::from_value(req).unwrap();
    db.transact_write_items(transact_req).unwrap();

    assert_eq!(get_val(&db, "Table1", "new"), Some("created".to_string()));
}

#[test]
fn test_transact_write_exceeds_4mb_aggregate_size() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");

    // Individual items must be under 400KB, but aggregate must exceed 4MB.
    // Use 15 items each ~300KB ≈ 4.5MB total.
    let big_val = "x".repeat(300 * 1024); // 300KB string
    let items: Vec<serde_json::Value> = (0..15)
        .map(|i| {
            json!({"Put": {
                "TableName": "Table1",
                "Item": {
                    "pk": {"S": format!("k{i}")},
                    "data": {"S": big_val}
                }
            }})
        })
        .collect();

    let req: serde_json::Value = json!({"TransactItems": items});
    let transact_req = serde_json::from_value(req).unwrap();
    let err = db.transact_write_items(transact_req).unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("4MB"), "Expected 4MB limit error, got: {msg}");
}

#[test]
fn test_transact_write_just_under_4mb_succeeds() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");

    // 12 items each ~300KB ≈ 3.6MB, under 4MB limit and each under 400KB.
    let val = "x".repeat(300 * 1024);
    let items: Vec<serde_json::Value> = (0..12)
        .map(|i| {
            json!({"Put": {
                "TableName": "Table1",
                "Item": {
                    "pk": {"S": format!("k{i}")},
                    "data": {"S": val}
                }
            }})
        })
        .collect();

    let req: serde_json::Value = json!({"TransactItems": items});
    let transact_req = serde_json::from_value(req).unwrap();
    db.transact_write_items(transact_req).unwrap();

    // Verify an item was written
    let get_req: serde_json::Value = json!({
        "TableName": "Table1",
        "Key": {"pk": {"S": "k0"}}
    });
    let get_req = serde_json::from_value(get_req).unwrap();
    let resp = db.get_item(get_req).unwrap();
    assert!(resp.item.is_some());
}

#[test]
fn test_transact_write_with_client_request_token() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");

    let req: serde_json::Value = json!({
        "TransactItems": [
            {"Put": {"TableName": "Table1", "Item": {"pk": {"S": "a"}, "val": {"S": "1"}}}}
        ],
        "ClientRequestToken": "token-123"
    });
    let transact_req = serde_json::from_value(req).unwrap();
    db.transact_write_items(transact_req).unwrap();

    assert_eq!(get_val(&db, "Table1", "a"), Some("1".to_string()));
}

#[test]
fn test_transact_write_idempotent_with_same_token() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");

    // First call: insert item
    let req: serde_json::Value = json!({
        "TransactItems": [
            {"Put": {"TableName": "Table1", "Item": {"pk": {"S": "a"}, "val": {"S": "1"}}}}
        ],
        "ClientRequestToken": "idempotent-token"
    });
    let transact_req = serde_json::from_value(req).unwrap();
    db.transact_write_items(transact_req).unwrap();

    // Delete the item so we can tell if the second call re-executes
    let del_req: serde_json::Value = json!({
        "TableName": "Table1",
        "Key": {"pk": {"S": "a"}}
    });
    let del_req = serde_json::from_value(del_req).unwrap();
    db.delete_item(del_req).unwrap();
    assert_eq!(get_val(&db, "Table1", "a"), None);

    // Second call with same token: should return cached response, NOT re-execute
    let req2: serde_json::Value = json!({
        "TransactItems": [
            {"Put": {"TableName": "Table1", "Item": {"pk": {"S": "a"}, "val": {"S": "1"}}}}
        ],
        "ClientRequestToken": "idempotent-token"
    });
    let transact_req2 = serde_json::from_value(req2).unwrap();
    db.transact_write_items(transact_req2).unwrap();

    // Item should still be absent because the second call was served from cache
    assert_eq!(get_val(&db, "Table1", "a"), None);
}

#[test]
fn test_transact_write_different_token_executes_normally() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");

    let req1: serde_json::Value = json!({
        "TransactItems": [
            {"Put": {"TableName": "Table1", "Item": {"pk": {"S": "a"}, "val": {"S": "1"}}}}
        ],
        "ClientRequestToken": "token-A"
    });
    let transact_req1 = serde_json::from_value(req1).unwrap();
    db.transact_write_items(transact_req1).unwrap();

    // Different token: should execute normally
    let req2: serde_json::Value = json!({
        "TransactItems": [
            {"Put": {"TableName": "Table1", "Item": {"pk": {"S": "b"}, "val": {"S": "2"}}}}
        ],
        "ClientRequestToken": "token-B"
    });
    let transact_req2 = serde_json::from_value(req2).unwrap();
    db.transact_write_items(transact_req2).unwrap();

    assert_eq!(get_val(&db, "Table1", "a"), Some("1".to_string()));
    assert_eq!(get_val(&db, "Table1", "b"), Some("2".to_string()));
}

#[test]
fn test_transact_write_idempotent_parameter_mismatch() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");

    // First call with token
    let req1: serde_json::Value = json!({
        "TransactItems": [
            {"Put": {"TableName": "Table1", "Item": {"pk": {"S": "a"}, "val": {"S": "1"}}}}
        ],
        "ClientRequestToken": "reused-token"
    });
    let transact_req1 = serde_json::from_value(req1).unwrap();
    db.transact_write_items(transact_req1).unwrap();

    // Second call with SAME token but DIFFERENT content
    let req2: serde_json::Value = json!({
        "TransactItems": [
            {"Put": {"TableName": "Table1", "Item": {"pk": {"S": "b"}, "val": {"S": "2"}}}}
        ],
        "ClientRequestToken": "reused-token"
    });
    let transact_req2 = serde_json::from_value(req2).unwrap();
    let err = db.transact_write_items(transact_req2).unwrap_err();

    assert!(
        matches!(
            err,
            dynoxide::errors::DynoxideError::IdempotentParameterMismatchException(_)
        ),
        "expected IdempotentParameterMismatchException, got: {:?}",
        err
    );

    // Original item should still exist, mismatch item should not
    assert_eq!(get_val(&db, "Table1", "a"), Some("1".to_string()));
    assert_eq!(get_val(&db, "Table1", "b"), None);
}

#[test]
fn test_transact_write_return_values_on_condition_check_failure() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");
    put_item(&db, "Table1", "exists", "old_value");

    // TransactPut with condition that fails + ALL_OLD → should return old item
    let req: serde_json::Value = json!({
        "TransactItems": [
            {"Put": {
                "TableName": "Table1",
                "Item": {"pk": {"S": "exists"}, "val": {"S": "new_value"}},
                "ConditionExpression": "attribute_not_exists(pk)",
                "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
            }}
        ]
    });
    let transact_req = serde_json::from_value(req).unwrap();
    let err = db.transact_write_items(transact_req).unwrap_err();
    let json: serde_json::Value = serde_json::from_str(&err.to_json()).unwrap();
    let reasons = json["CancellationReasons"].as_array().unwrap();
    assert_eq!(reasons[0]["Code"], "ConditionalCheckFailed");
    assert!(reasons[0]["Item"].is_object());
    assert_eq!(reasons[0]["Item"]["val"]["S"], "old_value");
}

#[test]
fn test_transact_write_no_return_values_on_condition_check_failure() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");
    put_item(&db, "Table1", "exists", "old_value");

    // TransactPut with condition that fails, NO ReturnValuesOnConditionCheckFailure
    let req: serde_json::Value = json!({
        "TransactItems": [
            {"Put": {
                "TableName": "Table1",
                "Item": {"pk": {"S": "exists"}, "val": {"S": "new_value"}},
                "ConditionExpression": "attribute_not_exists(pk)"
            }}
        ]
    });
    let transact_req = serde_json::from_value(req).unwrap();
    let err = db.transact_write_items(transact_req).unwrap_err();
    let json: serde_json::Value = serde_json::from_str(&err.to_json()).unwrap();
    let reasons = json["CancellationReasons"].as_array().unwrap();
    assert_eq!(reasons[0]["Code"], "ConditionalCheckFailed");
    // Item should NOT be present in cancellation reasons
    assert!(reasons[0].get("Item").is_none());
}

#[test]
fn test_transact_update_return_values_on_condition_check_failure() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");
    put_item(&db, "Table1", "exists", "old_value");

    // TransactUpdate with condition that fails + ALL_OLD
    let req: serde_json::Value = json!({
        "TransactItems": [
            {"Update": {
                "TableName": "Table1",
                "Key": {"pk": {"S": "exists"}},
                "UpdateExpression": "SET val = :v",
                "ExpressionAttributeValues": {":v": {"S": "new_value"}},
                "ConditionExpression": "attribute_not_exists(pk)",
                "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
            }}
        ]
    });
    let transact_req = serde_json::from_value(req).unwrap();
    let err = db.transact_write_items(transact_req).unwrap_err();
    let json: serde_json::Value = serde_json::from_str(&err.to_json()).unwrap();
    let reasons = json["CancellationReasons"].as_array().unwrap();
    assert_eq!(reasons[0]["Code"], "ConditionalCheckFailed");
    assert!(reasons[0]["Item"].is_object());
    assert_eq!(reasons[0]["Item"]["val"]["S"], "old_value");
}

#[test]
fn test_transact_delete_return_values_on_condition_check_failure() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");
    put_item(&db, "Table1", "exists", "old_value");

    // TransactDelete with condition that fails + ALL_OLD
    let req: serde_json::Value = json!({
        "TransactItems": [
            {"Delete": {
                "TableName": "Table1",
                "Key": {"pk": {"S": "exists"}},
                "ConditionExpression": "attribute_not_exists(pk)",
                "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
            }}
        ]
    });
    let transact_req = serde_json::from_value(req).unwrap();
    let err = db.transact_write_items(transact_req).unwrap_err();
    let json: serde_json::Value = serde_json::from_str(&err.to_json()).unwrap();
    let reasons = json["CancellationReasons"].as_array().unwrap();
    assert_eq!(reasons[0]["Code"], "ConditionalCheckFailed");
    assert!(reasons[0]["Item"].is_object());
    assert_eq!(reasons[0]["Item"]["val"]["S"], "old_value");
}

#[test]
fn test_transact_condition_check_return_values_on_condition_check_failure() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");
    put_item(&db, "Table1", "exists", "old_value");

    // ConditionCheck with condition that fails + ALL_OLD
    let req: serde_json::Value = json!({
        "TransactItems": [
            {"ConditionCheck": {
                "TableName": "Table1",
                "Key": {"pk": {"S": "exists"}},
                "ConditionExpression": "attribute_not_exists(pk)",
                "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
            }}
        ]
    });
    let transact_req = serde_json::from_value(req).unwrap();
    let err = db.transact_write_items(transact_req).unwrap_err();
    let json: serde_json::Value = serde_json::from_str(&err.to_json()).unwrap();
    let reasons = json["CancellationReasons"].as_array().unwrap();
    assert_eq!(reasons[0]["Code"], "ConditionalCheckFailed");
    assert!(reasons[0]["Item"].is_object());
    assert_eq!(reasons[0]["Item"]["val"]["S"], "old_value");
}

#[test]
fn test_transact_get_exceeds_100() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");

    let items: Vec<serde_json::Value> = (0..101)
        .map(|i| json!({"Get": {"TableName": "Table1", "Key": {"pk": {"S": format!("k{i}")}}}}))
        .collect();

    let req: serde_json::Value = json!({"TransactItems": items});
    let transact_req = serde_json::from_value(req).unwrap();
    let err = db.transact_get_items(transact_req).unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("100"));
}

#[test]
fn test_transact_write_empty_items_rejected() {
    let db = Database::memory().unwrap();

    let req: serde_json::Value = json!({"TransactItems": []});
    let transact_req = serde_json::from_value(req).unwrap();
    let err = db.transact_write_items(transact_req).unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("Member must have length greater than or equal to 1"),
        "Expected empty array error, got: {msg}"
    );
}

#[test]
fn test_transact_write_cancellation_reasons_in_error_json() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "CancelReasons");
    put_item(&db, "CancelReasons", "existing", "original");

    // Try to put with condition that will fail (item already exists)
    let req: serde_json::Value = json!({
        "TransactItems": [{
            "Put": {
                "TableName": "CancelReasons",
                "Item": {"pk": {"S": "existing"}, "val": {"S": "new"}},
                "ConditionExpression": "attribute_not_exists(pk)",
                "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
            }
        }]
    });
    let transact_req = serde_json::from_value(req).unwrap();
    let err = db.transact_write_items(transact_req).unwrap_err();

    // Verify CancellationReasons is a top-level JSON field
    let json_str = err.to_json();
    let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();

    let reasons = json["CancellationReasons"]
        .as_array()
        .expect("CancellationReasons should be an array");
    assert_eq!(reasons.len(), 1);
    assert_eq!(reasons[0]["Code"], "ConditionalCheckFailed");

    // ReturnValuesOnConditionCheckFailure: ALL_OLD should populate Item
    let item = reasons[0]["Item"]
        .as_object()
        .expect("Item should be present");
    assert_eq!(item["pk"]["S"], "existing");
    assert_eq!(item["val"]["S"], "original");
}

#[test]
fn test_transact_get_rejects_duplicate_keys() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "DupGet");
    put_item(&db, "DupGet", "k1", "v1");

    let req: serde_json::Value = json!({
        "TransactItems": [
            {"Get": {"TableName": "DupGet", "Key": {"pk": {"S": "k1"}}}},
            {"Get": {"TableName": "DupGet", "Key": {"pk": {"S": "k1"}}}}
        ]
    });
    let transact_req = serde_json::from_value(req).unwrap();
    let err = db.transact_get_items(transact_req).unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("multiple operations on one item"),
        "Expected duplicate key error, got: {msg}"
    );
}

#[test]
fn test_transact_get_empty_items_rejected() {
    let db = Database::memory().unwrap();

    let req: serde_json::Value = json!({"TransactItems": []});
    let transact_req = serde_json::from_value(req).unwrap();
    let err = db.transact_get_items(transact_req).unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("Member must have length greater than or equal to 1"),
        "Expected empty array error, got: {msg}"
    );
}

// ---------------------------------------------------------------------------
// Bug: TransactWriteItems Update ignores ConditionExpression on non-existent items
// ---------------------------------------------------------------------------

fn create_pk_sk_table(db: &Database, name: &str) {
    let req: serde_json::Value = json!({
        "TableName": name,
        "KeySchema": [
            {"AttributeName": "PK", "KeyType": "HASH"},
            {"AttributeName": "SK", "KeyType": "RANGE"}
        ],
        "AttributeDefinitions": [
            {"AttributeName": "PK", "AttributeType": "S"},
            {"AttributeName": "SK", "AttributeType": "S"}
        ]
    });
    let create_req = serde_json::from_value(req).unwrap();
    db.create_table(create_req).unwrap();
}

/// TransactWriteItems Update with `attribute_exists(PK)` should fail when the
/// item does not exist.
///
/// Root cause: `execute_update` populated key attributes on the item BEFORE
/// evaluating the condition expression, so `attribute_exists(PK)` always
/// returned true.
#[test]
fn test_transact_update_condition_rejects_nonexistent_item() {
    let db = Database::memory().unwrap();
    create_pk_sk_table(&db, "cond-test");

    let req: serde_json::Value = json!({
        "TransactItems": [{
            "Update": {
                "TableName": "cond-test",
                "Key": {"PK": {"S": "does-not-exist"}, "SK": {"S": "nope"}},
                "UpdateExpression": "ADD TagCount :inc",
                "ExpressionAttributeValues": {":inc": {"N": "1"}},
                "ConditionExpression": "attribute_exists(PK)"
            }
        }]
    });
    let transact_req = serde_json::from_value(req).unwrap();
    let err = db.transact_write_items(transact_req).unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("ConditionalCheckFailed"),
        "Expected ConditionalCheckFailed for non-existent item, got: {msg}"
    );

    // Verify no ghost item was created.
    let get_req: serde_json::Value = json!({
        "TableName": "cond-test",
        "Key": {"PK": {"S": "does-not-exist"}, "SK": {"S": "nope"}}
    });
    let get_req = serde_json::from_value(get_req).unwrap();
    let resp = db.get_item(get_req).unwrap();
    assert!(resp.item.is_none(), "item should not exist after failed condition");
}

/// Standalone UpdateItem with `attribute_exists(PK)` should also fail when
/// the item does not exist (same bug, different code path).
#[test]
fn test_update_item_condition_rejects_nonexistent_item() {
    let db = Database::memory().unwrap();
    create_pk_sk_table(&db, "cond-test-single");

    let req: serde_json::Value = json!({
        "TableName": "cond-test-single",
        "Key": {"PK": {"S": "ghost"}, "SK": {"S": "nope"}},
        "UpdateExpression": "ADD TagCount :inc",
        "ExpressionAttributeValues": {":inc": {"N": "1"}},
        "ConditionExpression": "attribute_exists(PK)"
    });
    let update_req = serde_json::from_value(req).unwrap();
    let err = db.update_item(update_req).unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("conditional request failed"),
        "Expected conditional check failure for non-existent item, got: {msg}"
    );

    // Verify no ghost item was created.
    let get_req: serde_json::Value = json!({
        "TableName": "cond-test-single",
        "Key": {"PK": {"S": "ghost"}, "SK": {"S": "nope"}}
    });
    let get_req = serde_json::from_value(get_req).unwrap();
    let resp = db.get_item(get_req).unwrap();
    assert!(resp.item.is_none(), "item should not exist after failed condition");
}
