//! Transaction integration tests.

use dynoxide::Database;
use dynoxide::DynoxideError;
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
fn test_transact_get_projection_matching_nothing_omits_item() {
    // AWS omits `Item` entirely when a ProjectionExpression matches no attribute
    // on an otherwise-present item, rather than returning an empty `{}` (or a
    // key-only object). Mirrors the conformance assertion
    // tests/tier2/transactions/transactGet.test.ts —
    // "omits Item when the projection matches no attribute on a present item".
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");

    let req: serde_json::Value = json!({
        "TableName": "Table1",
        "Item": {"pk": {"S": "a"}, "real": {"S": "here"}}
    });
    let put_req = serde_json::from_value(req).unwrap();
    db.put_item(put_req).unwrap();

    let req: serde_json::Value = json!({
        "TransactItems": [
            {"Get": {
                "TableName": "Table1",
                "Key": {"pk": {"S": "a"}},
                "ProjectionExpression": "#x",
                "ExpressionAttributeNames": {"#x": "doesNotExist"}
            }}
        ]
    });
    let transact_req = serde_json::from_value(req).unwrap();
    let resp = db.transact_get_items(transact_req).unwrap();

    assert!(
        resp.responses[0].item.is_none(),
        "projection matching nothing on a present item must omit Item, got: {:?}",
        resp.responses[0].item
    );
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
    assert!(
        resp.item.is_none(),
        "item should not exist after failed condition"
    );
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
    assert!(
        resp.item.is_none(),
        "item should not exist after failed condition"
    );
}

// ---------------------------------------------------------------------------
// ConsumedCapacity (#37)
// ---------------------------------------------------------------------------

/// #37: a transactional write charges 2 WCU per item. Two sub-1KB Puts cost
/// 1 WCU each non-transactionally, doubled to 2 each, so 4 in total.
#[test]
fn test_transact_write_charges_two_wcu_per_item() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Txn");

    let req: serde_json::Value = json!({
        "ReturnConsumedCapacity": "TOTAL",
        "TransactItems": [
            {"Put": {"TableName": "Txn", "Item": {"pk": {"S": "a"}, "v": {"N": "1"}}}},
            {"Put": {"TableName": "Txn", "Item": {"pk": {"S": "b"}, "v": {"N": "1"}}}}
        ]
    });
    let resp = db
        .transact_write_items(serde_json::from_value(req).unwrap())
        .unwrap();

    let total: f64 = resp
        .consumed_capacity
        .unwrap()
        .iter()
        .map(|c| c.capacity_units)
        .sum();
    assert_eq!(total, 4.0);
}

/// #37: a transactional read charges 2 RCU per requested item, including an
/// item that turned out to be missing. One present + one missing = 4.
#[test]
fn test_transact_get_charges_two_rcu_per_item_including_missing() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Txn");

    let put: serde_json::Value =
        json!({"TableName": "Txn", "Item": {"pk": {"S": "present"}, "v": {"N": "1"}}});
    db.put_item(serde_json::from_value(put).unwrap()).unwrap();

    let req: serde_json::Value = json!({
        "ReturnConsumedCapacity": "TOTAL",
        "TransactItems": [
            {"Get": {"TableName": "Txn", "Key": {"pk": {"S": "present"}}}},
            {"Get": {"TableName": "Txn", "Key": {"pk": {"S": "missing"}}}}
        ]
    });
    let resp = db
        .transact_get_items(serde_json::from_value(req).unwrap())
        .unwrap();

    let total: f64 = resp
        .consumed_capacity
        .unwrap()
        .iter()
        .map(|c| c.capacity_units)
        .sum();
    assert_eq!(total, 4.0);
}

/// #37: under INDEXES the TransactGet breakdown carries the table's read
/// capacity units, not just the top-level CapacityUnits.
#[test]
fn test_transact_get_indexes_reports_table_read_capacity() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Txn");

    let put: serde_json::Value =
        json!({"TableName": "Txn", "Item": {"pk": {"S": "present"}, "v": {"N": "1"}}});
    db.put_item(serde_json::from_value(put).unwrap()).unwrap();

    let req: serde_json::Value = json!({
        "ReturnConsumedCapacity": "INDEXES",
        "TransactItems": [
            {"Get": {"TableName": "Txn", "Key": {"pk": {"S": "present"}}}}
        ]
    });
    let resp = db
        .transact_get_items(serde_json::from_value(req).unwrap())
        .unwrap();

    let caps = resp.consumed_capacity.unwrap();
    let entry = &caps[0];
    let table = entry
        .table
        .as_ref()
        .expect("INDEXES breakdown must include the Table detail");
    assert!(
        table.read_capacity_units.unwrap_or(0.0) > 0.0,
        "Table.ReadCapacityUnits should be populated under INDEXES: {table:?}"
    );
}

/// #37: with ReturnConsumedCapacity omitted, the response carries no
/// ConsumedCapacity block (the contract boundary AWS enforces).
#[test]
fn test_transact_write_no_consumed_capacity_when_not_requested() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Txn");

    let req: serde_json::Value = json!({
        "TransactItems": [
            {"Put": {"TableName": "Txn", "Item": {"pk": {"S": "a"}, "v": {"N": "1"}}}}
        ]
    });
    let resp = db
        .transact_write_items(serde_json::from_value(req).unwrap())
        .unwrap();
    assert!(resp.consumed_capacity.is_none());
}

/// #37: TransactGet with ReturnConsumedCapacity omitted carries no block.
#[test]
fn test_transact_get_no_consumed_capacity_when_not_requested() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Txn");

    let put: serde_json::Value =
        json!({"TableName": "Txn", "Item": {"pk": {"S": "present"}, "v": {"N": "1"}}});
    db.put_item(serde_json::from_value(put).unwrap()).unwrap();

    let req: serde_json::Value = json!({
        "TransactItems": [
            {"Get": {"TableName": "Txn", "Key": {"pk": {"S": "present"}}}}
        ]
    });
    let resp = db
        .transact_get_items(serde_json::from_value(req).unwrap())
        .unwrap();
    assert!(resp.consumed_capacity.is_none());
}

/// #37: under INDEXES the TransactWrite breakdown carries the table's write
/// capacity units, mirroring the read path.
#[test]
fn test_transact_write_indexes_reports_table_write_capacity() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Txn");

    let req: serde_json::Value = json!({
        "ReturnConsumedCapacity": "INDEXES",
        "TransactItems": [
            {"Put": {"TableName": "Txn", "Item": {"pk": {"S": "a"}, "v": {"N": "1"}}}}
        ]
    });
    let resp = db
        .transact_write_items(serde_json::from_value(req).unwrap())
        .unwrap();

    let caps = resp.consumed_capacity.unwrap();
    let table = caps[0]
        .table
        .as_ref()
        .expect("INDEXES breakdown must include the Table detail");
    assert!(
        table.write_capacity_units.unwrap_or(0.0) > 0.0,
        "Table.WriteCapacityUnits should be populated under INDEXES: {table:?}"
    );
}

// ---- empty-string key values surface top-level; type-mismatch / non-scalar stay cancellation reasons ----

#[test]
fn test_transact_put_empty_string_table_key_is_top_level_validation() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");
    let req: serde_json::Value = json!({
        "TransactItems": [
            {"Put": {"TableName": "Table1", "Item": {"pk": {"S": ""}}}}
        ]
    });
    let err = db
        .transact_write_items(serde_json::from_value(req).unwrap())
        .unwrap_err();
    assert_eq!(
        err.error_type(),
        "com.amazon.coral.validate#ValidationException",
        "empty-string table key must surface as a top-level ValidationException, got {err:?}"
    );
    assert!(
        !matches!(err, DynoxideError::TransactionCanceledException(..)),
        "must not be wrapped as a transaction cancellation: {err:?}"
    );
    assert_eq!(
        err.to_string(),
        "One or more parameter values are not valid. The AttributeValue for a key attribute cannot contain an empty string value. Key: pk"
    );
}

#[test]
fn test_transact_put_wrong_type_table_key_is_cancellation_reason() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");
    let req: serde_json::Value = json!({
        "TransactItems": [
            {"Put": {"TableName": "Table1", "Item": {"pk": {"N": "5"}}}}
        ]
    });
    let err = db
        .transact_write_items(serde_json::from_value(req).unwrap())
        .unwrap_err();
    match err {
        DynoxideError::TransactionCanceledException(_, reasons) => {
            assert_eq!(reasons[0].code, "ValidationError");
            assert_eq!(
                reasons[0].message.as_deref().unwrap_or_default(),
                "One or more parameter values were invalid: Type mismatch for key pk expected: S actual: N"
            );
        }
        other => panic!("wrong-type table key must stay a cancellation reason, got {other:?}"),
    }
}

#[test]
fn test_transact_put_non_scalar_table_key_is_cancellation_reason() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");
    let req: serde_json::Value = json!({
        "TransactItems": [
            {"Put": {"TableName": "Table1", "Item": {"pk": {"L": [{"S": "x"}]}}}}
        ]
    });
    let err = db
        .transact_write_items(serde_json::from_value(req).unwrap())
        .unwrap_err();
    match err {
        DynoxideError::TransactionCanceledException(_, reasons) => {
            assert_eq!(reasons[0].code, "ValidationError");
            assert_eq!(
                reasons[0].message.as_deref().unwrap_or_default(),
                "One or more parameter values were invalid: Type mismatch for key pk expected: S actual: L"
            );
        }
        other => panic!(
            "non-scalar table key must become a cancellation reason, not a top-level InternalServerError, got {other:?}"
        ),
    }
}

#[test]
fn test_transact_empty_string_key_short_circuit_no_partial_write() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");
    // First item is a valid put; the second carries an empty-string key.
    let req: serde_json::Value = json!({
        "TransactItems": [
            {"Put": {"TableName": "Table1", "Item": {"pk": {"S": "a"}, "val": {"S": "1"}}}},
            {"Put": {"TableName": "Table1", "Item": {"pk": {"S": ""}}}}
        ]
    });
    let err = db
        .transact_write_items(serde_json::from_value(req).unwrap())
        .unwrap_err();
    assert_eq!(
        err.error_type(),
        "com.amazon.coral.validate#ValidationException",
        "an empty-string key anywhere in the transaction must surface top-level, got {err:?}"
    );
    assert!(
        !matches!(err, DynoxideError::TransactionCanceledException(..)),
        "must not be wrapped as a transaction cancellation: {err:?}"
    );
    // The earlier valid put must have rolled back.
    assert_eq!(get_val(&db, "Table1", "a"), None);
}

// ---- malformed lookup Key on a transact Update / Delete / ConditionCheck ----
//
// #98: the lookup-key path (an Update / Delete / ConditionCheck `Key`) splits
// the same way the Put item-key path does. Captured against real AWS across four
// regions, byte-identical: an empty-string key value surfaces as a top-level
// ValidationException, while a wrong-type or non-scalar key value stays a
// cancellation reason whose message is the generic schema-mismatch string (not
// the PutItem "Type mismatch for key" wording). ConditionCheck is caught at the
// key stage, before its condition runs.

const LOOKUP_EMPTY_KEY_MSG: &str = "One or more parameter values are not valid. The AttributeValue for a key attribute cannot contain an empty string value. Key: pk";
const LOOKUP_SCHEMA_MISMATCH_MSG: &str = "The provided key element does not match the schema";

fn upd_lookup(key: serde_json::Value) -> serde_json::Value {
    json!({"Update": {"TableName": "Table1", "Key": {"pk": key}, "UpdateExpression": "SET attr1 = :v", "ExpressionAttributeValues": {":v": {"S": "x"}}}})
}
fn del_lookup(key: serde_json::Value) -> serde_json::Value {
    json!({"Delete": {"TableName": "Table1", "Key": {"pk": key}}})
}
fn cc_lookup(key: serde_json::Value) -> serde_json::Value {
    json!({"ConditionCheck": {"TableName": "Table1", "Key": {"pk": key}, "ConditionExpression": "attribute_not_exists(pk)"}})
}

fn assert_lookup_key_top_level(transact_item: serde_json::Value) {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");
    let req = json!({"TransactItems": [transact_item]});
    let err = db
        .transact_write_items(serde_json::from_value(req).unwrap())
        .unwrap_err();
    assert_eq!(
        err.error_type(),
        "com.amazon.coral.validate#ValidationException",
        "empty-string lookup key must surface as a top-level ValidationException, got {err:?}"
    );
    assert!(
        !matches!(err, DynoxideError::TransactionCanceledException(..)),
        "must not be wrapped as a transaction cancellation: {err:?}"
    );
    assert_eq!(err.to_string(), LOOKUP_EMPTY_KEY_MSG);
}

fn assert_lookup_key_cancelled(transact_item: serde_json::Value) {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");
    let req = json!({"TransactItems": [transact_item]});
    let err = db
        .transact_write_items(serde_json::from_value(req).unwrap())
        .unwrap_err();
    match err {
        DynoxideError::TransactionCanceledException(_, reasons) => {
            assert_eq!(reasons[0].code, "ValidationError");
            assert_eq!(
                reasons[0].message.as_deref().unwrap_or_default(),
                LOOKUP_SCHEMA_MISMATCH_MSG
            );
        }
        other => panic!("malformed lookup key must stay a cancellation reason, got {other:?}"),
    }
}

#[test]
fn test_transact_update_empty_string_lookup_key_is_top_level_validation() {
    assert_lookup_key_top_level(upd_lookup(json!({"S": ""})));
}

#[test]
fn test_transact_delete_empty_string_lookup_key_is_top_level_validation() {
    assert_lookup_key_top_level(del_lookup(json!({"S": ""})));
}

#[test]
fn test_transact_condition_check_empty_string_lookup_key_is_top_level_validation() {
    assert_lookup_key_top_level(cc_lookup(json!({"S": ""})));
}

#[test]
fn test_transact_update_wrong_type_lookup_key_is_cancellation_reason() {
    assert_lookup_key_cancelled(upd_lookup(json!({"N": "5"})));
}

#[test]
fn test_transact_update_non_scalar_lookup_key_is_cancellation_reason() {
    assert_lookup_key_cancelled(upd_lookup(json!({"L": [{"S": "x"}]})));
}

#[test]
fn test_transact_delete_wrong_type_lookup_key_is_cancellation_reason() {
    assert_lookup_key_cancelled(del_lookup(json!({"N": "5"})));
}

#[test]
fn test_transact_delete_non_scalar_lookup_key_is_cancellation_reason() {
    assert_lookup_key_cancelled(del_lookup(json!({"L": [{"S": "x"}]})));
}

#[test]
fn test_transact_condition_check_wrong_type_lookup_key_is_cancellation_reason() {
    assert_lookup_key_cancelled(cc_lookup(json!({"N": "5"})));
}

#[test]
fn test_transact_condition_check_non_scalar_lookup_key_is_cancellation_reason() {
    assert_lookup_key_cancelled(cc_lookup(json!({"L": [{"S": "x"}]})));
}

#[test]
fn test_transact_empty_string_lookup_key_rolls_back_earlier_action() {
    // A valid Put followed by an Update with an empty-string lookup key: the
    // empty key surfaces top-level and the earlier valid Put must roll back.
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");
    let req: serde_json::Value = json!({
        "TransactItems": [
            {"Put": {"TableName": "Table1", "Item": {"pk": {"S": "a"}, "val": {"S": "1"}}}},
            {"Update": {"TableName": "Table1", "Key": {"pk": {"S": ""}}, "UpdateExpression": "SET attr1 = :v", "ExpressionAttributeValues": {":v": {"S": "x"}}}}
        ]
    });
    let err = db
        .transact_write_items(serde_json::from_value(req).unwrap())
        .unwrap_err();
    assert_eq!(
        err.error_type(),
        "com.amazon.coral.validate#ValidationException",
        "an empty-string lookup key anywhere in the transaction must surface top-level, got {err:?}"
    );
    assert!(
        !matches!(err, DynoxideError::TransactionCanceledException(..)),
        "must not be wrapped as a transaction cancellation: {err:?}"
    );
    assert_eq!(get_val(&db, "Table1", "a"), None);
}

#[test]
fn test_transact_get_empty_string_key_stays_cancellation_reason() {
    // TransactGetItems surfaces per-action key validation through the
    // cancellation channel (captured AWS behaviour), so an empty-string Key
    // must NOT be hoisted top-level the way the transact-write path is.
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");
    let req: serde_json::Value = json!({
        "TransactItems": [
            {"Get": {"TableName": "Table1", "Key": {"pk": {"S": ""}}}}
        ]
    });
    let err = db
        .transact_get_items(serde_json::from_value(req).unwrap())
        .unwrap_err();
    match err {
        DynoxideError::TransactionCanceledException(_, reasons) => {
            assert_eq!(reasons[0].code, "ValidationError");
            // Pin the message too: the empty-string class carries the corrected
            // "are not valid" wording even through the cancellation channel.
            assert_eq!(
                reasons[0].message.as_deref().unwrap_or_default(),
                LOOKUP_EMPTY_KEY_MSG
            );
        }
        other => panic!(
            "an empty-string key in TransactGetItems must stay a cancellation reason, got {other:?}"
        ),
    }
}

#[test]
fn test_transact_get_mixed_empty_and_wrong_type_keys_both_validation_reasons() {
    // Two transact-get actions: one empty-string Key, one wrong-typed Key. Both
    // surface as per-action ValidationError cancellation reasons, in order, and
    // the second action is still validated (the loop does not short-circuit).
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");
    let req: serde_json::Value = json!({
        "TransactItems": [
            {"Get": {"TableName": "Table1", "Key": {"pk": {"S": ""}}}},
            {"Get": {"TableName": "Table1", "Key": {"pk": {"N": "5"}}}}
        ]
    });
    let err = db
        .transact_get_items(serde_json::from_value(req).unwrap())
        .unwrap_err();
    match err {
        DynoxideError::TransactionCanceledException(_, reasons) => {
            assert_eq!(reasons.len(), 2);
            assert_eq!(reasons[0].code, "ValidationError");
            assert_eq!(reasons[1].code, "ValidationError");
            assert_eq!(
                reasons[0].message.as_deref().unwrap_or_default(),
                LOOKUP_EMPTY_KEY_MSG
            );
            assert_eq!(
                reasons[1].message.as_deref().unwrap_or_default(),
                LOOKUP_SCHEMA_MISMATCH_MSG
            );
        }
        other => panic!("mixed malformed transact-get keys must cancel, got {other:?}"),
    }
}

// ---- empty-string SORT key on the lookup path; count-vs-value precedence ----

#[test]
fn test_transact_update_empty_string_sort_key_is_top_level_validation() {
    // An empty-string value in the SORT key slot of a composite table reaches the
    // same validator and surfaces top-level, naming the sort key.
    let db = Database::memory().unwrap();
    create_pk_sk_table(&db, "Composite");
    let req: serde_json::Value = json!({
        "TransactItems": [
            {"Update": {"TableName": "Composite", "Key": {"PK": {"S": "x"}, "SK": {"S": ""}}, "UpdateExpression": "SET attr1 = :v", "ExpressionAttributeValues": {":v": {"S": "y"}}}}
        ]
    });
    let err = db
        .transact_write_items(serde_json::from_value(req).unwrap())
        .unwrap_err();
    assert_eq!(
        err.error_type(),
        "com.amazon.coral.validate#ValidationException",
        "empty-string sort key must surface top-level, got {err:?}"
    );
    assert!(!matches!(
        err,
        DynoxideError::TransactionCanceledException(..)
    ));
    assert_eq!(
        err.to_string(),
        "One or more parameter values are not valid. The AttributeValue for a key attribute cannot contain an empty string value. Key: SK"
    );
}

#[test]
fn test_transact_update_missing_sort_key_is_schema_mismatch_cancellation() {
    // Omitting the sort key entirely fails the key-count/shape check before the
    // per-value empty-string check is ever reached, so it stays a cancellation
    // reason with the schema-mismatch message (count-vs-value precedence).
    let db = Database::memory().unwrap();
    create_pk_sk_table(&db, "Composite");
    let req: serde_json::Value = json!({
        "TransactItems": [
            {"Update": {"TableName": "Composite", "Key": {"PK": {"S": ""}}, "UpdateExpression": "SET attr1 = :v", "ExpressionAttributeValues": {":v": {"S": "y"}}}}
        ]
    });
    let err = db
        .transact_write_items(serde_json::from_value(req).unwrap())
        .unwrap_err();
    match err {
        DynoxideError::TransactionCanceledException(_, reasons) => {
            assert_eq!(reasons[0].code, "ValidationError");
            assert_eq!(
                reasons[0].message.as_deref().unwrap_or_default(),
                LOOKUP_SCHEMA_MISMATCH_MSG
            );
        }
        other => panic!("a missing sort key must stay a cancellation reason, got {other:?}"),
    }
}
