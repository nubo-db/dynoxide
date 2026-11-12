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
    // tests/tier2/transactions/transactGet.test.ts -
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

/// A transactional write reports top-level WriteCapacityUnits (and no
/// ReadCapacityUnits) under INDEXES, matching real DynamoDB. One sub-1KB Put
/// costs 2 transactional write units.
#[test]
fn test_transact_write_reports_top_level_write_capacity() {
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

    let entry = &resp.consumed_capacity.unwrap()[0];
    assert_eq!(entry.capacity_units, 2.0);
    assert_eq!(entry.write_capacity_units, Some(2.0));
    assert_eq!(entry.read_capacity_units, None);
}

/// A standalone ConditionCheck action costs 2 write capacity units, billed as
/// write, on its own table line under INDEXES. The check sits on a different
/// table from the write so the per-table breakdown isolates its cost.
#[test]
fn test_transact_condition_check_costs_two_write_units() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "WriteTbl");
    create_test_table(&db, "CheckTbl");
    put_item(&db, "CheckTbl", "present", "x");

    let req: serde_json::Value = json!({
        "ReturnConsumedCapacity": "INDEXES",
        "TransactItems": [
            {"Put": {"TableName": "WriteTbl", "Item": {"pk": {"S": "a"}, "v": {"N": "1"}}}},
            {"ConditionCheck": {
                "TableName": "CheckTbl",
                "Key": {"pk": {"S": "present"}},
                "ConditionExpression": "attribute_exists(pk)"
            }}
        ]
    });
    let resp = db
        .transact_write_items(serde_json::from_value(req).unwrap())
        .unwrap();

    let caps = resp.consumed_capacity.unwrap();
    let check = caps
        .iter()
        .find(|c| c.table_name == "CheckTbl")
        .expect("per-table entry for the check table");
    assert_eq!(check.capacity_units, 2.0);
    assert_eq!(check.write_capacity_units, Some(2.0));
    assert_eq!(check.read_capacity_units, None);
}

/// A same-token idempotent replay re-reads the stored result: the first call
/// reports transactional WRITE capacity, and the in-window replay reports the
/// same magnitude as READ capacity (with the opposite axis omitted), matching
/// real DynamoDB.
#[test]
fn test_transact_write_replay_reports_read_capacity() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Txn");

    let body = json!({
        "ClientRequestToken": "replay-token-1",
        "ReturnConsumedCapacity": "INDEXES",
        "TransactItems": [
            {"Put": {"TableName": "Txn", "Item": {"pk": {"S": "a"}, "v": {"N": "1"}}}}
        ]
    });

    let first = db
        .transact_write_items(serde_json::from_value(body.clone()).unwrap())
        .unwrap();
    let first_entry = &first.consumed_capacity.unwrap()[0];
    assert_eq!(first_entry.write_capacity_units, Some(2.0));
    assert_eq!(first_entry.read_capacity_units, None);

    let replay = db
        .transact_write_items(serde_json::from_value(body).unwrap())
        .unwrap();
    let replay_entry = &replay.consumed_capacity.unwrap()[0];
    assert_eq!(replay_entry.capacity_units, 2.0);
    assert_eq!(replay_entry.read_capacity_units, Some(2.0));
    assert_eq!(replay_entry.write_capacity_units, None);
}

/// A same-token replay of a >1KB item recomputes the read cost at 4KB read
/// granularity rather than relabelling the first call's 1KB-granular write
/// magnitude. For a ~1.5KB Put the first call reports write `4` (2 WCU at 1KB,
/// doubled) but the replay reports read `2` (1 RCU at 4KB, doubled), where the
/// two magnitudes diverge. This is the case the sub-1KB test cannot catch,
/// since below 1KB both round to the same number.
#[test]
fn test_transact_write_replay_recomputes_read_above_1kb() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Txn");

    // ~1.5KB item: write rounds to 2 units (1KB granularity), read to 1 (4KB).
    let blob = "x".repeat(1500);
    let body = json!({
        "ClientRequestToken": "replay-token-1kb",
        "ReturnConsumedCapacity": "INDEXES",
        "TransactItems": [
            {"Put": {"TableName": "Txn", "Item": {"pk": {"S": "big"}, "blob": {"S": blob}}}}
        ]
    });

    let first = db
        .transact_write_items(serde_json::from_value(body.clone()).unwrap())
        .unwrap();
    let first_entry = &first.consumed_capacity.unwrap()[0];
    assert_eq!(first_entry.capacity_units, 4.0);
    assert_eq!(first_entry.write_capacity_units, Some(4.0));
    assert_eq!(first_entry.read_capacity_units, None);

    let replay = db
        .transact_write_items(serde_json::from_value(body).unwrap())
        .unwrap();
    let replay_entry = &replay.consumed_capacity.unwrap()[0];
    assert_eq!(replay_entry.capacity_units, 2.0);
    assert_eq!(replay_entry.read_capacity_units, Some(2.0));
    assert_eq!(replay_entry.write_capacity_units, None);
}

/// A same-token replay honours the replay request's own ReturnConsumedCapacity
/// mode, not the first call's. First call INDEXES, replay TOTAL: the replay
/// reports read capacity in TOTAL shape (no Table breakdown).
#[test]
fn test_transact_write_replay_honours_replay_mode() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Txn");

    let items = json!([
        {"Put": {"TableName": "Txn", "Item": {"pk": {"S": "a"}, "v": {"N": "1"}}}}
    ]);
    let first = json!({
        "ClientRequestToken": "replay-mode-token",
        "ReturnConsumedCapacity": "INDEXES",
        "TransactItems": items.clone()
    });
    db.transact_write_items(serde_json::from_value(first).unwrap())
        .unwrap();

    let replay = json!({
        "ClientRequestToken": "replay-mode-token",
        "ReturnConsumedCapacity": "TOTAL",
        "TransactItems": items
    });
    let resp = db
        .transact_write_items(serde_json::from_value(replay).unwrap())
        .unwrap();
    let entry = &resp.consumed_capacity.unwrap()[0];
    assert_eq!(entry.capacity_units, 2.0);
    assert_eq!(entry.read_capacity_units, Some(2.0));
    assert_eq!(entry.write_capacity_units, None);
    assert!(
        entry.table.is_none(),
        "TOTAL mode carries no Table breakdown"
    );
}

/// A same-token replay with no ReturnConsumedCapacity reports no capacity at
/// all, even though the first call requested INDEXES (the replay honours its
/// own mode).
#[test]
fn test_transact_write_replay_without_capacity_mode_returns_none() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Txn");

    let items = json!([
        {"Put": {"TableName": "Txn", "Item": {"pk": {"S": "a"}, "v": {"N": "1"}}}}
    ]);
    let first = json!({
        "ClientRequestToken": "replay-none-token",
        "ReturnConsumedCapacity": "INDEXES",
        "TransactItems": items.clone()
    });
    db.transact_write_items(serde_json::from_value(first).unwrap())
        .unwrap();

    let replay = json!({
        "ClientRequestToken": "replay-none-token",
        "TransactItems": items
    });
    let resp = db
        .transact_write_items(serde_json::from_value(replay).unwrap())
        .unwrap();
    assert!(resp.consumed_capacity.is_none());
}

/// Under TOTAL mode a transactional write reports top-level WriteCapacityUnits
/// (and no ReadCapacityUnits, no Table breakdown).
#[test]
fn test_transact_write_total_mode_reports_top_level_write_capacity() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Txn");

    let req = json!({
        "ReturnConsumedCapacity": "TOTAL",
        "TransactItems": [
            {"Put": {"TableName": "Txn", "Item": {"pk": {"S": "a"}, "v": {"N": "1"}}}}
        ]
    });
    let resp = db
        .transact_write_items(serde_json::from_value(req).unwrap())
        .unwrap();
    let entry = &resp.consumed_capacity.unwrap()[0];
    assert_eq!(entry.capacity_units, 2.0);
    assert_eq!(entry.write_capacity_units, Some(2.0));
    assert_eq!(entry.read_capacity_units, None);
    assert!(
        entry.table.is_none(),
        "TOTAL mode carries no Table breakdown"
    );
}

/// Under TOTAL mode a transactional read reports top-level ReadCapacityUnits
/// (and no WriteCapacityUnits).
#[test]
fn test_transact_get_total_mode_reports_top_level_read_capacity() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Txn");
    put_item(&db, "Txn", "present", "x");

    let req = json!({
        "ReturnConsumedCapacity": "TOTAL",
        "TransactItems": [
            {"Get": {"TableName": "Txn", "Key": {"pk": {"S": "present"}}}}
        ]
    });
    let resp = db
        .transact_get_items(serde_json::from_value(req).unwrap())
        .unwrap();
    let entry = &resp.consumed_capacity.unwrap()[0];
    assert_eq!(entry.capacity_units, 2.0);
    assert_eq!(entry.read_capacity_units, Some(2.0));
    assert_eq!(entry.write_capacity_units, None);
}

/// Concurrency: same-token calls racing must not both execute the transaction.
/// Each Put carries attribute_not_exists(pk), so a second execution would cancel
/// where the first succeeded. Idempotency must serialise them: one executes, the
/// rest replay, so every call returns Ok.
#[test]
fn test_transact_write_concurrent_same_token_executes_once() {
    use std::sync::{Arc, Barrier};

    let db = Database::memory().unwrap();
    create_test_table(&db, "Txn");

    const N: usize = 16;
    let barrier = Arc::new(Barrier::new(N));
    let handles: Vec<_> = (0..N)
        .map(|_| {
            let db = db.clone();
            let barrier = Arc::clone(&barrier);
            // Parse on the spawning thread, before the barrier: a panic here
            // fails the test rather than hanging the other threads on the barrier.
            let parsed = serde_json::from_value(json!({
                "ClientRequestToken": "race-token",
                "TransactItems": [
                    {"Put": {
                        "TableName": "Txn",
                        "Item": {"pk": {"S": "once"}},
                        "ConditionExpression": "attribute_not_exists(pk)"
                    }}
                ]
            }))
            .unwrap();
            std::thread::spawn(move || {
                barrier.wait();
                db.transact_write_items(parsed).is_ok()
            })
        })
        .collect();

    let oks = handles
        .into_iter()
        .map(|h| h.join().unwrap())
        .filter(|&ok| ok)
        .count();
    assert_eq!(
        oks, N,
        "all {N} same-token calls should succeed (one executes, the rest replay); got {oks}"
    );

    // The transaction applied exactly once (not zero times): the item exists.
    let got = db
        .get_item(
            serde_json::from_value(json!({"TableName": "Txn", "Key": {"pk": {"S": "once"}}}))
                .unwrap(),
        )
        .unwrap();
    assert!(
        got.item.is_some(),
        "the transaction's Put must have applied"
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
fn test_transact_put_duplicate_ss_is_refused_up_front() {
    // A transaction checks every action's sets before it runs any: a repeated
    // member is a top-level refusal in the current layer's words, not a
    // cancellation reason (captured eu-west-2, us-east-1, eu-west-1 and
    // us-west-2, 2026-11-11).
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");
    let req: serde_json::Value = json!({
        "TransactItems": [
            {"Put": {"TableName": "Table1", "Item": {"pk": {"S": "k1"}, "tags": {"SS": ["a", "a"]}}}}
        ]
    });
    let err = db
        .transact_write_items(serde_json::from_value(req).unwrap())
        .unwrap_err();
    match err {
        DynoxideError::ValidationException(message) => {
            assert_eq!(
                message,
                r#"One or more parameter values were invalid: Input collection ["a", "a"] contains duplicates."#
            );
        }
        other => panic!("duplicate SS must be refused up front, got {other:?}"),
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

// ---- empty-binary key value: top-level everywhere, mirroring empty-string ----
//
// Real AWS (four-region capture, 2026-06-24) returns a top-level ValidationException
// for an empty-binary key value on every path, hoisting inside a transaction exactly
// like the empty-string case. Needs a binary-keyed table to reach the binary key arm.

const EMPTY_BINARY_KEY_MSG: &str = "One or more parameter values are not valid. The AttributeValue for a key attribute cannot contain an empty binary value. Key: pk";

fn create_binary_key_table(db: &Database, name: &str) {
    let req: serde_json::Value = json!({
        "TableName": name,
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "B"}]
    });
    db.create_table(serde_json::from_value(req).unwrap())
        .unwrap();
}

fn assert_binary_key_top_level(transact_item: serde_json::Value) {
    let db = Database::memory().unwrap();
    create_binary_key_table(&db, "BinTable");
    let req = json!({"TransactItems": [transact_item]});
    let err = db
        .transact_write_items(serde_json::from_value(req).unwrap())
        .unwrap_err();
    assert_eq!(
        err.error_type(),
        "com.amazon.coral.validate#ValidationException",
        "empty-binary key must surface as a top-level ValidationException, got {err:?}"
    );
    assert!(
        !matches!(err, DynoxideError::TransactionCanceledException(..)),
        "must not be wrapped as a transaction cancellation: {err:?}"
    );
    assert_eq!(err.to_string(), EMPTY_BINARY_KEY_MSG);
}

#[test]
fn test_transact_update_empty_binary_lookup_key_is_top_level_validation() {
    assert_binary_key_top_level(
        json!({"Update": {"TableName": "BinTable", "Key": {"pk": {"B": ""}}, "UpdateExpression": "SET attr1 = :v", "ExpressionAttributeValues": {":v": {"S": "x"}}}}),
    );
}

#[test]
fn test_transact_delete_empty_binary_lookup_key_is_top_level_validation() {
    assert_binary_key_top_level(
        json!({"Delete": {"TableName": "BinTable", "Key": {"pk": {"B": ""}}}}),
    );
}

#[test]
fn test_transact_condition_check_empty_binary_lookup_key_is_top_level_validation() {
    assert_binary_key_top_level(
        json!({"ConditionCheck": {"TableName": "BinTable", "Key": {"pk": {"B": ""}}, "ConditionExpression": "attribute_not_exists(pk)"}}),
    );
}

#[test]
fn test_transact_put_empty_binary_item_key_is_top_level_validation() {
    // The item-key path must hoist too: an empty-binary item key in a transact Put
    // is top-level, not a cancellation reason.
    assert_binary_key_top_level(
        json!({"Put": {"TableName": "BinTable", "Item": {"pk": {"B": ""}}}}),
    );
}

#[test]
fn test_transact_get_empty_binary_key_stays_cancellation_reason() {
    // As with empty-string, a transact READ surfaces per-action key validation
    // through the cancellation channel, not top-level.
    let db = Database::memory().unwrap();
    create_binary_key_table(&db, "BinTable");
    let req: serde_json::Value = json!({
        "TransactItems": [
            {"Get": {"TableName": "BinTable", "Key": {"pk": {"B": ""}}}}
        ]
    });
    let err = db
        .transact_get_items(serde_json::from_value(req).unwrap())
        .unwrap_err();
    match err {
        DynoxideError::TransactionCanceledException(_, reasons) => {
            assert_eq!(reasons[0].code, "ValidationError");
            assert_eq!(
                reasons[0].message.as_deref().unwrap_or_default(),
                EMPTY_BINARY_KEY_MSG
            );
        }
        other => panic!(
            "an empty-binary key in TransactGetItems must stay a cancellation reason, got {other:?}"
        ),
    }
}

#[test]
fn test_transact_update_empty_binary_sort_key_is_top_level_validation() {
    // An empty-binary value in the SORT key slot of a binary composite table
    // reaches the same validator and surfaces top-level, naming the sort key.
    let db = Database::memory().unwrap();
    let create: serde_json::Value = json!({
        "TableName": "BinComposite",
        "KeySchema": [
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"}
        ],
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "B"},
            {"AttributeName": "sk", "AttributeType": "B"}
        ]
    });
    db.create_table(serde_json::from_value(create).unwrap())
        .unwrap();
    let req: serde_json::Value = json!({
        "TransactItems": [
            {"Update": {"TableName": "BinComposite", "Key": {"pk": {"B": "AQ=="}, "sk": {"B": ""}}, "UpdateExpression": "SET attr1 = :v", "ExpressionAttributeValues": {":v": {"S": "x"}}}}
        ]
    });
    let err = db
        .transact_write_items(serde_json::from_value(req).unwrap())
        .unwrap_err();
    assert_eq!(
        err.error_type(),
        "com.amazon.coral.validate#ValidationException",
        "empty-binary sort key must surface top-level, got {err:?}"
    );
    assert!(!matches!(
        err,
        DynoxideError::TransactionCanceledException(..)
    ));
    assert_eq!(
        err.to_string(),
        "One or more parameter values are not valid. The AttributeValue for a key attribute cannot contain an empty binary value. Key: sk"
    );
}

// ---- expression size and Put item nesting are checked before any action runs ----
//
// An expression over 4096 bytes on any action, or a Put item nested past 32
// levels, is a top-level ValidationException rather than a cancellation reason.
// DynamoDB checks the actions in request order, each for its expressions and its
// Put item, before it looks up a table, checks for duplicate targets or reads a
// key. Captured in us-east-1, us-east-2, us-west-2, sa-east-1, eu-west-1,
// eu-central-1, ap-northeast-1 and ap-southeast-2, 2026-10-08.

const NESTING_MSG: &str = "Nesting Levels have exceeded supported limits: \
                           Attributes in the item have nested levels beyond supported limit";
const ITEM_SIZE_MSG: &str = "Item size has exceeded the maximum allowed size";

fn condition_size_msg(len: usize) -> String {
    format!(
        "Invalid ConditionExpression: Expression size has exceeded the maximum allowed size; \
         expression size: {len}"
    )
}

/// `attribute_not_exists(c0000) AND attribute_not_exists(c0001) ...`, padded to
/// exactly `len` bytes. True against any item that has none of the names.
fn condition_of_len(len: usize) -> String {
    let mut expr = String::new();
    for i in 0.. {
        let clause = format!("attribute_not_exists(c{i:04})");
        let next = if expr.is_empty() {
            clause
        } else {
            format!("{expr} AND {clause}")
        };
        if next.len() > len {
            break;
        }
        expr = next;
    }
    let short = len - expr.len();
    expr.insert_str(expr.len() - 1, &"z".repeat(short));
    assert_eq!(expr.len(), len);
    expr
}

/// `SET u0000 = :v, u0001 = :v ...`, padded to exactly `len` bytes.
fn update_of_len(len: usize) -> String {
    let mut expr = String::from("SET ");
    for i in 0.. {
        let clause = format!("u{i:04} = :v");
        let next = if i == 0 {
            format!("{expr}{clause}")
        } else {
            format!("{expr}, {clause}")
        };
        if next.len() > len {
            break;
        }
        expr = next;
    }
    let short = len - expr.len();
    expr.insert_str(expr.len() - " = :v".len(), &"z".repeat(short));
    assert_eq!(expr.len(), len);
    expr
}

/// A value wrapped in `depth` single-key maps; 32 wraps is one level too deep.
fn deep_value(depth: usize) -> serde_json::Value {
    let mut v = json!({"S": "leaf"});
    for _ in 0..depth {
        v = json!({"M": {"n": v}});
    }
    v
}

fn transact(db: &Database, items: serde_json::Value) -> Result<(), DynoxideError> {
    let req = json!({ "TransactItems": items });
    db.transact_write_items(serde_json::from_value(req).unwrap())
        .map(|_| ())
}

fn assert_top_level_validation(err: DynoxideError, expected: &str) {
    assert!(
        !matches!(err, DynoxideError::TransactionCanceledException(..)),
        "expected a top-level ValidationException, got a cancellation: {err:?}"
    );
    assert_eq!(
        err.error_type(),
        "com.amazon.coral.validate#ValidationException",
        "{err:?}"
    );
    assert_eq!(err.to_string(), expected);
}

fn cancellation_codes(err: DynoxideError) -> Vec<String> {
    match err {
        DynoxideError::TransactionCanceledException(_, reasons) => {
            reasons.into_iter().map(|r| r.code).collect()
        }
        other => panic!("expected a cancellation, got {other:?}"),
    }
}

#[test]
fn test_transact_oversized_condition_check_is_top_level_validation() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");
    put_item(&db, "Table1", "a", "v");
    let err = transact(
        &db,
        json!([{"ConditionCheck": {
            "TableName": "Table1",
            "Key": {"pk": {"S": "a"}},
            "ConditionExpression": condition_of_len(4495)
        }}]),
    )
    .unwrap_err();
    assert_top_level_validation(err, &condition_size_msg(4495));
}

#[test]
fn test_transact_oversized_expression_on_each_action_is_top_level_validation() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");
    put_item(&db, "Table1", "a", "v");
    let over = condition_of_len(4097);
    let cases = [
        (
            json!({"Put": {"TableName": "Table1", "Item": {"pk": {"S": "p"}},
                "ConditionExpression": over}}),
            condition_size_msg(4097),
        ),
        (
            // The UpdateExpression form carries no size figure, in every region.
            json!({"Update": {"TableName": "Table1", "Key": {"pk": {"S": "u"}},
                "UpdateExpression": update_of_len(4097),
                "ExpressionAttributeValues": {":v": {"S": "x"}}}}),
            "Invalid UpdateExpression: Expression size has exceeded the maximum allowed size;"
                .to_string(),
        ),
        (
            json!({"Update": {"TableName": "Table1", "Key": {"pk": {"S": "u"}},
                "UpdateExpression": "SET x = :v", "ConditionExpression": over,
                "ExpressionAttributeValues": {":v": {"S": "x"}}}}),
            condition_size_msg(4097),
        ),
        (
            json!({"Delete": {"TableName": "Table1", "Key": {"pk": {"S": "a"}},
                "ConditionExpression": over}}),
            condition_size_msg(4097),
        ),
    ];
    for (action, expected) in cases {
        let err = transact(&db, json!([action])).unwrap_err();
        assert_top_level_validation(err, &expected);
    }
    assert_eq!(get_val(&db, "Table1", "a"), Some("v".to_string()));
}

#[test]
fn test_transact_oversized_second_action_writes_nothing() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");
    let err = transact(
        &db,
        json!([
            {"Put": {"TableName": "Table1", "Item": {"pk": {"S": "first"}, "val": {"S": "1"}}}},
            {"Put": {"TableName": "Table1", "Item": {"pk": {"S": "second"}},
                "ConditionExpression": condition_of_len(4097)}}
        ]),
    )
    .unwrap_err();
    assert_top_level_validation(err, &condition_size_msg(4097));
    assert_eq!(get_val(&db, "Table1", "first"), None);
}

#[test]
fn test_transact_expressions_at_size_limit_run() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");
    put_item(&db, "Table1", "a", "v");
    put_item(&db, "Table1", "d", "v");
    let at_limit = condition_of_len(4096);
    transact(
        &db,
        json!([
            {"ConditionCheck": {"TableName": "Table1", "Key": {"pk": {"S": "a"}},
                "ConditionExpression": at_limit}},
            {"Update": {"TableName": "Table1", "Key": {"pk": {"S": "b"}},
                "UpdateExpression": update_of_len(4096),
                "ExpressionAttributeValues": {":v": {"S": "x"}}}},
            {"Put": {"TableName": "Table1", "Item": {"pk": {"S": "c"}, "val": {"S": "put"}},
                "ConditionExpression": at_limit}},
            {"Delete": {"TableName": "Table1", "Key": {"pk": {"S": "d"}},
                "ConditionExpression": at_limit}}
        ]),
    )
    .unwrap();
    // The conditions ran and held, so the Put and the Delete both applied.
    assert_eq!(get_val(&db, "Table1", "c"), Some("put".to_string()));
    assert_eq!(get_val(&db, "Table1", "d"), None);
}

#[test]
fn test_transact_expression_size_is_measured_per_expression() {
    // 3000 bytes of UpdateExpression and 3000 of ConditionExpression on one action
    // is accepted: the cap is per expression, not per action.
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");
    transact(
        &db,
        json!([{"Update": {"TableName": "Table1", "Key": {"pk": {"S": "a"}},
            "UpdateExpression": update_of_len(3000),
            "ConditionExpression": condition_of_len(3000),
            "ExpressionAttributeValues": {":v": {"S": "x"}}}}]),
    )
    .unwrap();
}

#[test]
fn test_transact_oversized_expression_is_checked_before_tables_and_keys() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");
    put_item(&db, "Table1", "a", "v");
    let oversized_check = |pk: &str, table: &str| {
        json!({"ConditionCheck": {"TableName": table, "Key": {"pk": {"S": pk}},
            "ConditionExpression": condition_of_len(4097)}})
    };
    let cases = [
        // A missing table, on the oversized action or an earlier one.
        json!([oversized_check("a", "Missing")]),
        json!([{"Put": {"TableName": "Missing", "Item": {"pk": {"S": "x"}}}},
            oversized_check("a", "Table1")]),
        // An empty key value, either side of the oversized action.
        json!([{"Put": {"TableName": "Table1", "Item": {"pk": {"S": ""}}}},
            oversized_check("a", "Table1")]),
        json!([oversized_check("a", "Table1"),
            {"Put": {"TableName": "Table1", "Item": {"pk": {"S": ""}}}}]),
        // Two actions on one item.
        json!([{"Put": {"TableName": "Table1", "Item": {"pk": {"S": "a2"}}}},
            oversized_check("a2", "Table1")]),
    ];
    for items in cases {
        let err = transact(&db, items).unwrap_err();
        assert_top_level_validation(err, &condition_size_msg(4097));
    }
}

#[test]
fn test_transact_put_nested_32_levels_is_top_level_validation() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");
    let err = transact(
        &db,
        json!([{"Put": {"TableName": "Table1",
            "Item": {"pk": {"S": "deep"}, "data": deep_value(32)}}}]),
    )
    .unwrap_err();
    assert_top_level_validation(err, NESTING_MSG);
    assert_eq!(get_val(&db, "Table1", "deep"), None);

    // One level shallower is stored.
    transact(
        &db,
        json!([{"Put": {"TableName": "Table1",
            "Item": {"pk": {"S": "ok"}, "data": deep_value(31)}}}]),
    )
    .unwrap();
}

#[test]
fn test_transact_put_nesting_is_checked_before_tables_and_keys() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");
    let deep_put = |pk: &str| {
        json!({"Put": {"TableName": "Table1",
            "Item": {"pk": {"S": pk}, "data": deep_value(32)}}})
    };
    let cases = [
        json!([deep_put("d1"), {"Put": {"TableName": "Missing", "Item": {"pk": {"S": "x"}}}}]),
        json!([{"Put": {"TableName": "Missing", "Item": {"pk": {"S": "x"}}}}, deep_put("d2")]),
        json!([deep_put("d3"), {"Put": {"TableName": "Table1", "Item": {"pk": {"S": "d3"}}}}]),
        json!([{"Put": {"TableName": "Table1", "Item": {"pk": {"N": "1"}}}}, deep_put("d4")]),
        json!([{"Put": {"TableName": "Table1", "Item": {"pk": {"S": ""}}}}, deep_put("d5")]),
        // The too-deep attribute can be the key itself.
        json!([{"Put": {"TableName": "Table1", "Item": {"pk": deep_value(32)}}}]),
    ];
    for items in cases {
        let err = transact(&db, items).unwrap_err();
        assert_top_level_validation(err, NESTING_MSG);
    }
}

#[test]
fn test_transact_up_front_checks_take_actions_in_request_order() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");
    put_item(&db, "Table1", "a", "v");
    let oversized = json!({"ConditionCheck": {"TableName": "Table1",
        "Key": {"pk": {"S": "a"}}, "ConditionExpression": condition_of_len(4097)}});
    let deep = json!({"Put": {"TableName": "Table1",
        "Item": {"pk": {"S": "deep"}, "data": deep_value(32)}}});
    let big = json!({"Put": {"TableName": "Table1",
        "Item": {"pk": {"S": "big"}, "pad": {"S": "x".repeat(400 * 1024)}}}});
    let cases = [
        (json!([oversized, deep]), condition_size_msg(4097)),
        (json!([deep, oversized]), NESTING_MSG.to_string()),
        (json!([big, deep]), ITEM_SIZE_MSG.to_string()),
        (json!([deep, big]), NESTING_MSG.to_string()),
        (json!([oversized, big]), condition_size_msg(4097)),
    ];
    for (items, expected) in cases {
        let err = transact(&db, items).unwrap_err();
        assert_top_level_validation(err, &expected);
    }
}

// Within one action, us-east-1 reports a Put's condition size ahead of its item
// depth and eu-west-2 the reverse (2026-10-09). These pin the order the
// single-item operations use: PutItem checks the item's depth before it parses
// the condition, and UpdateItem parses the UpdateExpression first.

#[test]
fn test_transact_put_checks_item_depth_before_condition_size() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");
    let err = transact(
        &db,
        json!([{"Put": {"TableName": "Table1",
            "Item": {"pk": {"S": "both"}, "data": deep_value(32)},
            "ConditionExpression": condition_of_len(4097)}}]),
    )
    .unwrap_err();
    assert_top_level_validation(err, NESTING_MSG);
}

#[test]
fn test_transact_update_checks_update_expression_size_first() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");
    let err = transact(
        &db,
        json!([{"Update": {"TableName": "Table1", "Key": {"pk": {"S": "u"}},
            "UpdateExpression": update_of_len(4097),
            "ConditionExpression": condition_of_len(4097),
            "ExpressionAttributeValues": {":v": {"S": "x"}}}}]),
    )
    .unwrap_err();
    assert_top_level_validation(
        err,
        "Invalid UpdateExpression: Expression size has exceeded the maximum allowed size;",
    );
}

#[test]
fn test_transact_item_size_is_checked_before_tables_and_keys() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");
    let big = |pk: &str| {
        json!({"Put": {"TableName": "Table1",
            "Item": {"pk": {"S": pk}, "pad": {"S": "x".repeat(400 * 1024)}}}})
    };
    let cases = [
        json!([{"Put": {"TableName": "Missing", "Item": {"pk": {"S": "x"}}}}, big("b1")]),
        json!([{"Put": {"TableName": "Table1", "Item": {"pk": {"S": "b2"}}}}, big("b2")]),
    ];
    for items in cases {
        let err = transact(&db, items).unwrap_err();
        assert_top_level_validation(err, ITEM_SIZE_MSG);
    }
}

#[test]
fn test_transact_deep_condition_value_is_evaluated_not_rejected() {
    // A 32-level ExpressionAttributeValues entry that is only compared is not
    // depth-checked inside a transaction, on any action: the condition runs.
    // From 33 levels it is refused up front (see below).
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");
    put_item(&db, "Table1", "a", "v");
    let names = json!({"#d": "data"});
    let values = json!({":deep": deep_value(32)});
    let actions = [
        json!({"ConditionCheck": {"TableName": "Table1", "Key": {"pk": {"S": "a"}},
            "ConditionExpression": "#d = :deep",
            "ExpressionAttributeNames": names, "ExpressionAttributeValues": values}}),
        json!({"Put": {"TableName": "Table1", "Item": {"pk": {"S": "p"}},
            "ConditionExpression": "#d = :deep",
            "ExpressionAttributeNames": names, "ExpressionAttributeValues": values}}),
        json!({"Delete": {"TableName": "Table1", "Key": {"pk": {"S": "a"}},
            "ConditionExpression": "#d = :deep",
            "ExpressionAttributeNames": names, "ExpressionAttributeValues": values}}),
        json!({"Update": {"TableName": "Table1", "Key": {"pk": {"S": "a"}},
            "UpdateExpression": "SET t = :t", "ConditionExpression": "#d = :deep",
            "ExpressionAttributeNames": names,
            "ExpressionAttributeValues": {":t": {"S": "y"}, ":deep": deep_value(32)}}}),
    ];
    for action in actions {
        let err = transact(&db, json!([action])).unwrap_err();
        assert_eq!(cancellation_codes(err), vec!["ConditionalCheckFailed"]);
    }

    // And when the condition holds, the transaction goes through.
    transact(
        &db,
        json!([{"ConditionCheck": {"TableName": "Table1", "Key": {"pk": {"S": "a"}},
            "ConditionExpression": "attribute_not_exists(zz) OR #d = :deep",
            "ExpressionAttributeNames": names, "ExpressionAttributeValues": values}}]),
    )
    .unwrap();
}

#[test]
fn test_transact_update_writing_too_deep_value_still_cancels() {
    // The value reaches the item, so the stored-item cap applies to the finished
    // item and the action cancels with a ValidationError reason.
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");
    put_item(&db, "Table1", "a", "v");
    let err = transact(
        &db,
        json!([{"Update": {"TableName": "Table1", "Key": {"pk": {"S": "a"}},
            "UpdateExpression": "SET dd = :deep",
            "ExpressionAttributeValues": {":deep": deep_value(32)}}}]),
    )
    .unwrap_err();
    let DynoxideError::TransactionCanceledException(_, reasons) = err else {
        panic!("expected a cancellation, got {err:?}");
    };
    assert_eq!(reasons.len(), 1);
    assert_eq!(reasons[0].code, "ValidationError");
    // The reason stops after "limits", without the clause about the item's
    // attributes that the stored-item message carries. Captured us-east-1,
    // 2026-10-09.
    assert_eq!(
        reasons[0].message.as_deref(),
        Some("Nesting Levels have exceeded supported limits")
    );
}

fn deep_value_msg(key: &str) -> String {
    format!("ExpressionAttributeValues contains invalid value: {NESTING_MSG} for key {key}")
}

#[test]
fn test_transact_condition_value_nested_33_levels_is_top_level_validation() {
    // One level past the stored-item limit, an ExpressionAttributeValues entry is
    // refused up front on every action, compared, written or unused. Before, a
    // Put whose condition compared such a value went on to write its item.
    // Captured us-east-1, 2026-10-09.
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");
    put_item(&db, "Table1", "a", "v");
    let names = json!({"#d": "data"});
    let values = json!({":deep": deep_value(33)});
    let actions = [
        json!({"ConditionCheck": {"TableName": "Table1", "Key": {"pk": {"S": "a"}},
            "ConditionExpression": "#d = :deep",
            "ExpressionAttributeNames": names, "ExpressionAttributeValues": values}}),
        json!({"Put": {"TableName": "Table1", "Item": {"pk": {"S": "p"}},
            "ConditionExpression": "attribute_not_exists(pk) OR #d = :deep",
            "ExpressionAttributeNames": names, "ExpressionAttributeValues": values}}),
        json!({"Delete": {"TableName": "Table1", "Key": {"pk": {"S": "a"}},
            "ConditionExpression": "#d = :deep",
            "ExpressionAttributeNames": names, "ExpressionAttributeValues": values}}),
        json!({"Update": {"TableName": "Table1", "Key": {"pk": {"S": "a"}},
            "UpdateExpression": "SET dd = :deep", "ExpressionAttributeValues": values}}),
        json!({"ConditionCheck": {"TableName": "Table1", "Key": {"pk": {"S": "a"}},
            "ConditionExpression": "attribute_exists(pk)", "ExpressionAttributeValues": values}}),
    ];
    for action in actions {
        let err = transact(&db, json!([action])).unwrap_err();
        assert_top_level_validation(err, &deep_value_msg(":deep"));
    }
    assert_eq!(get_val(&db, "Table1", "p"), None, "the Put must not write");
    assert_eq!(get_val(&db, "Table1", "a"), Some("v".to_string()));
}

#[test]
fn test_transact_deep_condition_value_is_checked_first() {
    // Within an action the value check comes ahead of the Put item's depth,
    // both expression sizes and anything about tables or keys; across actions,
    // the first failing action wins. Captured us-east-1, 2026-10-09.
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");
    put_item(&db, "Table1", "a", "v");
    let values = json!({":deep": deep_value(33)});
    let cc_deep = json!({"ConditionCheck": {"TableName": "Table1",
        "Key": {"pk": {"S": "a"}}, "ConditionExpression": "attribute_exists(pk)",
        "ExpressionAttributeValues": values}});
    let deep_put = json!({"Put": {"TableName": "Table1",
        "Item": {"pk": {"S": "deep"}, "data": deep_value(32)}}});
    let cases = [
        // A Put with a 32-level item and a 33-level value in its condition.
        (
            json!([{"Put": {"TableName": "Table1",
                "Item": {"pk": {"S": "x1"}, "data": deep_value(32)},
                "ConditionExpression": "attribute_not_exists(pk) OR data = :deep",
                "ExpressionAttributeValues": values}}]),
            deep_value_msg(":deep"),
        ),
        // A Put with an oversized condition and a 33-level value.
        (
            json!([{"Put": {"TableName": "Table1", "Item": {"pk": {"S": "x2"}},
                "ConditionExpression": condition_of_len(4097),
                "ExpressionAttributeValues": values}}]),
            deep_value_msg(":deep"),
        ),
        // An Update writing a 33-level value with an oversized UpdateExpression.
        (
            json!([{"Update": {"TableName": "Table1", "Key": {"pk": {"S": "a"}},
                "UpdateExpression": update_of_len(4097),
                "ExpressionAttributeValues": {":v": deep_value(33)}}}]),
            deep_value_msg(":v"),
        ),
        // An Update writing a 33-level value with an oversized condition.
        (
            json!([{"Update": {"TableName": "Table1", "Key": {"pk": {"S": "a"}},
                "UpdateExpression": "SET dd = :deep",
                "ConditionExpression": condition_of_len(4097),
                "ExpressionAttributeValues": values}}]),
            deep_value_msg(":deep"),
        ),
        (json!([cc_deep, deep_put]), deep_value_msg(":deep")),
        (json!([deep_put, cc_deep]), NESTING_MSG.to_string()),
        // Ahead of a missing table, a duplicate target and an empty key value
        // on an earlier action.
        (
            json!([{"Put": {"TableName": "Missing", "Item": {"pk": {"S": "m"}}}}, cc_deep]),
            deep_value_msg(":deep"),
        ),
        (
            json!([{"Put": {"TableName": "Table1", "Item": {"pk": {"S": "a"}}}}, cc_deep]),
            deep_value_msg(":deep"),
        ),
        (
            json!([{"Put": {"TableName": "Table1", "Item": {"pk": {"S": ""}}}}, cc_deep]),
            deep_value_msg(":deep"),
        ),
    ];
    for (items, expected) in cases {
        let err = transact(&db, items).unwrap_err();
        assert_top_level_validation(err, &expected);
    }
}

#[test]
fn test_transact_condition_syntax_is_checked_up_front() {
    // A ConditionExpression that doesn't parse is a top-level error, reported in
    // request order ahead of a later action's checks and of any table lookup.
    // Captured us-east-1 and eu-west-2, 2026-10-09.
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");
    put_item(&db, "Table1", "a", "v");
    let bad = json!({"ConditionCheck": {"TableName": "Table1",
        "Key": {"pk": {"S": "a"}}, "ConditionExpression": "attribute_not_exists("}});
    let deep_put = json!({"Put": {"TableName": "Table1",
        "Item": {"pk": {"S": "deep"}, "data": deep_value(32)}}});
    let missing = json!({"Put": {"TableName": "Missing", "Item": {"pk": {"S": "m"}}}});

    for items in [json!([bad, deep_put]), json!([missing, bad])] {
        let err = transact(&db, items).unwrap_err();
        assert!(
            !matches!(err, DynoxideError::TransactionCanceledException(..)),
            "expected a top-level ValidationException, got a cancellation: {err:?}"
        );
        assert_eq!(
            err.error_type(),
            "com.amazon.coral.validate#ValidationException",
            "{err:?}"
        );
        assert_eq!(
            err.to_string(),
            r#"Invalid ConditionExpression: Syntax error; token: "<EOF>", near: "(""#
        );
    }

    let err = transact(&db, json!([deep_put, bad])).unwrap_err();
    assert_top_level_validation(err, NESTING_MSG);
}

#[test]
fn test_transact_empty_condition_expression_is_top_level_validation() {
    // Captured us-east-1 and eu-west-2, 2026-10-09.
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");
    put_item(&db, "Table1", "a", "v");
    let err = transact(
        &db,
        json!([{"ConditionCheck": {"TableName": "Table1", "Key": {"pk": {"S": "a"}},
            "ConditionExpression": ""}}]),
    )
    .unwrap_err();
    assert_top_level_validation(
        err,
        "Invalid ConditionExpression: The expression can not be empty;",
    );
}

fn unused_values_msg(keys: &str) -> String {
    format!("Value provided in ExpressionAttributeValues unused in expressions: keys: {{{keys}}}")
}

fn unused_check(pk: &str) -> serde_json::Value {
    json!({"ConditionCheck": {"TableName": "Table1", "Key": {"pk": {"S": pk}},
        "ConditionExpression": "attribute_exists(pk)",
        "ExpressionAttributeValues": {":v": {"S": "x"}}}})
}

#[test]
fn test_transact_unused_expression_value_is_top_level_validation() {
    // An ExpressionAttributeValues entry no expression uses is refused before
    // the transaction runs, on every action type, so an earlier valid Put writes
    // nothing either way but the error is a ValidationException rather than a
    // cancellation. Captured us-east-1 and eu-west-2, 2026-10-09.
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");
    put_item(&db, "Table1", "a", "v");
    let cases = [
        (json!([unused_check("a")]), unused_values_msg(":v")),
        (
            json!([{"Put": {"TableName": "Table1", "Item": {"pk": {"S": "u2"}, "val": {"S": "w"}}}},
                unused_check("a")]),
            unused_values_msg(":v"),
        ),
        (
            json!([{"Update": {"TableName": "Table1", "Key": {"pk": {"S": "u20"}},
                "UpdateExpression": "SET a = :a",
                "ExpressionAttributeValues": {":a": {"S": "x"}, ":b": {"S": "y"}}}}]),
            unused_values_msg(":b"),
        ),
        (
            json!([{"Put": {"TableName": "Table1", "Item": {"pk": {"S": "u22"}},
                "ConditionExpression": "attribute_not_exists(pk)",
                "ExpressionAttributeValues": {":v": {"S": "x"}}}}]),
            unused_values_msg(":v"),
        ),
        (
            json!([{"Delete": {"TableName": "Table1", "Key": {"pk": {"S": "u23"}},
                "ConditionExpression": "attribute_not_exists(pk)",
                "ExpressionAttributeValues": {":v": {"S": "x"}}}}]),
            unused_values_msg(":v"),
        ),
    ];
    for (items, expected) in cases {
        let err = transact(&db, items).unwrap_err();
        assert_top_level_validation(err, &expected);
    }
    assert_eq!(get_val(&db, "Table1", "u2"), None, "the Put must not write");
}

#[test]
fn test_transact_unused_value_is_reported_ahead_of_tables_keys_and_duplicates() {
    // The unused check is one of the request-only checks, so it wins over a
    // missing table, a duplicate target, an empty key value and a key that
    // doesn't match the schema, wherever they sit in the request. Captured
    // us-east-1 and eu-west-2, 2026-10-09.
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");
    put_item(&db, "Table1", "a", "v");
    put_item(&db, "Table1", "b", "v");
    let missing = json!({"Put": {"TableName": "Missing", "Item": {"pk": {"S": "x"}}}});
    let empty_key = json!({"Put": {"TableName": "Table1", "Item": {"pk": {"S": ""}}}});
    let wrong_key = json!({"ConditionCheck": {"TableName": "Table1",
        "Key": {"zz": {"S": "b"}}, "ConditionExpression": "attribute_exists(pk)"}});
    let cases = [
        json!([missing, unused_check("a")]),
        json!([unused_check("a"), missing]),
        json!([{"Put": {"TableName": "Table1", "Item": {"pk": {"S": "u6"}}}}, unused_check("u6")]),
        json!([unused_check("u7"), {"Put": {"TableName": "Table1", "Item": {"pk": {"S": "u7"}}}}]),
        json!([empty_key, unused_check("a")]),
        json!([unused_check("a"), empty_key]),
        json!([wrong_key, unused_check("b")]),
        json!([unused_check("a"), wrong_key]),
    ];
    for items in cases {
        let err = transact(&db, items.clone()).unwrap_err();
        assert_top_level_validation(err, &unused_values_msg(":v"));
    }
}

#[test]
fn test_transact_unused_value_takes_its_place_in_request_order() {
    // Across actions the first failing action wins; within one action the
    // expression size and syntax come first. Captured us-east-1 and eu-west-2,
    // 2026-10-09 (the size message's figure is the us-east-1 form).
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");
    put_item(&db, "Table1", "a", "v");
    put_item(&db, "Table1", "b", "v");
    let deep_put = json!({"Put": {"TableName": "Table1",
        "Item": {"pk": {"S": "deep"}, "data": deep_value(32)}}});
    let oversized = json!({"ConditionCheck": {"TableName": "Table1",
        "Key": {"pk": {"S": "b"}}, "ConditionExpression": condition_of_len(4097)}});
    let size_msg = "Invalid ConditionExpression: Expression size has exceeded the maximum \
                    allowed size; expression size: 4097";
    let syntax_msg = r#"Invalid ConditionExpression: Syntax error; token: "<EOF>", near: "(""#;
    let cases = [
        (
            json!([unused_check("a"), deep_put]),
            unused_values_msg(":v"),
        ),
        (
            json!([deep_put, unused_check("a")]),
            NESTING_MSG.to_string(),
        ),
        (
            json!([unused_check("a"), oversized]),
            unused_values_msg(":v"),
        ),
        (json!([oversized, unused_check("a")]), size_msg.to_string()),
        (
            json!([{"ConditionCheck": {"TableName": "Table1", "Key": {"pk": {"S": "a"}},
                "ConditionExpression": condition_of_len(4097),
                "ExpressionAttributeValues": {":v": {"S": "x"}}}}]),
            size_msg.to_string(),
        ),
        (
            json!([{"ConditionCheck": {"TableName": "Table1", "Key": {"pk": {"S": "a"}},
                "ConditionExpression": "attribute_not_exists(",
                "ExpressionAttributeValues": {":v": {"S": "x"}}}}]),
            syntax_msg.to_string(),
        ),
    ];
    for (items, expected) in cases {
        let err = transact(&db, items).unwrap_err();
        assert_top_level_validation(err, &expected);
    }
}

#[test]
fn test_transact_unused_names_come_before_values_and_keys_are_sorted() {
    // Captured us-east-1 and eu-west-2, 2026-10-09. DynamoDB lists several
    // unused values in an order that follows the request's own key order (the
    // two regions disagree on which way round), which a parsed request no
    // longer carries, so Dynoxide sorts them.
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");
    put_item(&db, "Table1", "a", "v");
    let err = transact(
        &db,
        json!([{"ConditionCheck": {"TableName": "Table1", "Key": {"pk": {"S": "a"}},
            "ConditionExpression": "attribute_exists(pk)",
            "ExpressionAttributeNames": {"#n": "x"},
            "ExpressionAttributeValues": {":v": {"S": "x"}}}}]),
    )
    .unwrap_err();
    assert_top_level_validation(
        err,
        "Value provided in ExpressionAttributeNames unused in expressions: keys: {#n}",
    );
    let err = transact(
        &db,
        json!([{"Put": {"TableName": "Table1", "Item": {"pk": {"S": "u26"}, "val": {"S": "w"}}}},
            {"ConditionCheck": {"TableName": "Table1", "Key": {"pk": {"S": "a"}},
            "ConditionExpression": "attribute_exists(pk)",
            "ExpressionAttributeNames": {"#n": "x"}}}]),
    )
    .unwrap_err();
    assert_top_level_validation(
        err,
        "Value provided in ExpressionAttributeNames unused in expressions: keys: {#n}",
    );
    assert_eq!(
        get_val(&db, "Table1", "u26"),
        None,
        "the Put must not write"
    );
    let err = transact(
        &db,
        json!([{"ConditionCheck": {"TableName": "Table1", "Key": {"pk": {"S": "a"}},
            "ConditionExpression": "attribute_exists(pk)",
            "ExpressionAttributeValues": {":w": {"S": "y"}, ":v": {"S": "x"}}}}]),
    )
    .unwrap_err();
    assert_top_level_validation(err, &unused_values_msg(":v, :w"));
}

#[test]
fn test_transact_undefined_value_is_top_level_validation() {
    // A condition naming a value the request doesn't define is refused up front
    // too, ahead of a missing table, and takes its place in request order
    // against an unused value. Captured us-east-1 and eu-west-2, 2026-10-09.
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");
    put_item(&db, "Table1", "a", "v");
    put_item(&db, "Table1", "b", "v");
    let undefined = json!({"ConditionCheck": {"TableName": "Table1",
        "Key": {"pk": {"S": "b"}}, "ConditionExpression": "a = :nope"}});
    let undefined_msg = "Invalid ConditionExpression: An expression attribute value used in \
                         expression is not defined; attribute value: :nope";
    let cases = [
        (
            json!([{"Put": {"TableName": "Missing", "Item": {"pk": {"S": "x"}}}}, undefined]),
            undefined_msg.to_string(),
        ),
        (
            json!([{"Put": {"TableName": "Table1", "Item": {"pk": {"S": "u30"}, "val": {"S": "w"}}}}, undefined]),
            undefined_msg.to_string(),
        ),
        (
            json!([unused_check("a"), undefined]),
            unused_values_msg(":v"),
        ),
        (
            json!([undefined, unused_check("a")]),
            undefined_msg.to_string(),
        ),
    ];
    for (items, expected) in cases {
        let err = transact(&db, items).unwrap_err();
        assert_top_level_validation(err, &expected);
    }
    assert_eq!(
        get_val(&db, "Table1", "u30"),
        None,
        "the Put must not write"
    );
}

#[test]
fn test_transact_update_expression_syntax_is_top_level_validation() {
    // An UpdateExpression that doesn't parse is refused up front, ahead of a
    // missing table and of an earlier valid Put. Captured us-east-1 and
    // eu-west-2, 2026-10-09.
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");
    let bad = json!({"Update": {"TableName": "Table1", "Key": {"pk": {"S": "u33"}},
        "UpdateExpression": "SET a ="}});
    let msg = r#"Invalid UpdateExpression: Syntax error; token: "<EOF>", near: "=""#;
    for items in [
        json!([bad]),
        json!([{"Put": {"TableName": "Missing", "Item": {"pk": {"S": "x"}}}}, bad]),
        json!([{"Put": {"TableName": "Table1", "Item": {"pk": {"S": "u35"}, "val": {"S": "w"}}}}, bad]),
    ] {
        let err = transact(&db, items).unwrap_err();
        assert_top_level_validation(err, msg);
    }
    assert_eq!(
        get_val(&db, "Table1", "u35"),
        None,
        "the Put must not write"
    );
}

#[test]
fn test_transact_put_with_values_and_no_condition_is_accepted() {
    // us-east-1 accepts a transactional Put carrying ExpressionAttributeValues
    // and no ConditionExpression, and writes the item; eu-west-2 refuses it with
    // "ExpressionAttributeValues can only be specified when using expressions".
    // Dynoxide follows us-east-1. Captured 2026-10-09.
    let db = Database::memory().unwrap();
    create_test_table(&db, "Table1");
    transact(
        &db,
        json!([{"Put": {"TableName": "Table1", "Item": {"pk": {"S": "u24"}, "val": {"S": "w"}},
            "ExpressionAttributeValues": {":v": {"S": "x"}}}}]),
    )
    .unwrap();
    assert_eq!(get_val(&db, "Table1", "u24"), Some("w".to_string()));
}
