use dynoxide::Database;
use dynoxide::actions::create_table::CreateTableRequest;
use dynoxide::actions::put_item::PutItemRequest;
use dynoxide::types::{
    AttributeDefinition, AttributeValue, KeySchemaElement, KeyType, ScalarAttributeType,
};
use std::collections::HashMap;

fn setup() -> Database {
    let db = Database::memory().unwrap();
    db.create_table(CreateTableRequest {
        table_name: "TestTable".to_string(),
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

fn make_item(pk: &str) -> HashMap<String, AttributeValue> {
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S(pk.to_string()));
    item.insert(
        "data".to_string(),
        AttributeValue::S("some value".to_string()),
    );
    item
}

#[test]
fn test_put_item_consumed_capacity_total() {
    let db = setup();
    let resp = db
        .put_item(PutItemRequest {
            table_name: "TestTable".to_string(),
            item: make_item("u1"),
            condition_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            return_values: None,
            return_consumed_capacity: Some("TOTAL".to_string()),
            ..Default::default()
        })
        .unwrap();

    let cc = resp.consumed_capacity.unwrap();
    assert_eq!(cc.table_name, "TestTable");
    assert!(cc.capacity_units >= 1.0);
    assert!(cc.table.is_none()); // TOTAL mode doesn't include per-resource
}

#[test]
fn test_put_item_consumed_capacity_indexes() {
    let db = setup();
    let resp = db
        .put_item(PutItemRequest {
            table_name: "TestTable".to_string(),
            item: make_item("u1"),
            condition_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            return_values: None,
            return_consumed_capacity: Some("INDEXES".to_string()),
            ..Default::default()
        })
        .unwrap();

    let cc = resp.consumed_capacity.unwrap();
    assert_eq!(cc.table_name, "TestTable");
    assert!(cc.capacity_units >= 1.0);
    assert!(cc.table.is_some()); // INDEXES mode includes per-resource
}

#[test]
fn test_put_item_consumed_capacity_none() {
    let db = setup();
    let resp = db
        .put_item(PutItemRequest {
            table_name: "TestTable".to_string(),
            item: make_item("u1"),
            condition_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            return_values: None,
            return_consumed_capacity: Some("NONE".to_string()),
            ..Default::default()
        })
        .unwrap();

    assert!(resp.consumed_capacity.is_none());
}

#[test]
fn test_get_item_consumed_capacity() {
    let db = setup();
    db.put_item(PutItemRequest {
        table_name: "TestTable".to_string(),
        item: make_item("u1"),
        condition_expression: None,
        expression_attribute_names: None,
        expression_attribute_values: None,
        ..Default::default()
    })
    .unwrap();

    let resp = db
        .get_item(dynoxide::actions::get_item::GetItemRequest {
            table_name: "TestTable".to_string(),
            key: {
                let mut k = HashMap::new();
                k.insert("pk".to_string(), AttributeValue::S("u1".to_string()));
                k
            },
            consistent_read: None,
            projection_expression: None,
            expression_attribute_names: None,
            return_consumed_capacity: Some("TOTAL".to_string()),
            ..Default::default()
        })
        .unwrap();

    let cc = resp.consumed_capacity.unwrap();
    assert_eq!(cc.table_name, "TestTable");
    // Eventually consistent read (default) halves the RCU
    assert!(cc.capacity_units > 0.0);
}

#[test]
fn test_query_consumed_capacity() {
    let db = setup();
    db.put_item(PutItemRequest {
        table_name: "TestTable".to_string(),
        item: make_item("u1"),
        condition_expression: None,
        expression_attribute_names: None,
        expression_attribute_values: None,
        ..Default::default()
    })
    .unwrap();

    let qr = dynoxide::actions::query::QueryRequest {
        table_name: "TestTable".to_string(),
        key_condition_expression: Some("pk = :pk".to_string()),
        expression_attribute_values: Some({
            let mut m = HashMap::new();
            m.insert(":pk".to_string(), AttributeValue::S("u1".to_string()));
            m
        }),
        scan_index_forward: true,
        return_consumed_capacity: Some("TOTAL".to_string()),
        ..Default::default()
    };
    let resp = db.query(qr).unwrap();

    let cc = resp.consumed_capacity.unwrap();
    assert_eq!(cc.table_name, "TestTable");
    // Eventually consistent query (default) halves the RCU
    assert!(cc.capacity_units > 0.0);
}

#[test]
fn test_delete_item_consumed_capacity() {
    let db = setup();
    db.put_item(PutItemRequest {
        table_name: "TestTable".to_string(),
        item: make_item("u1"),
        condition_expression: None,
        expression_attribute_names: None,
        expression_attribute_values: None,
        ..Default::default()
    })
    .unwrap();

    let resp = db
        .delete_item(dynoxide::actions::delete_item::DeleteItemRequest {
            table_name: "TestTable".to_string(),
            key: {
                let mut k = HashMap::new();
                k.insert("pk".to_string(), AttributeValue::S("u1".to_string()));
                k
            },
            return_values: None,
            condition_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            return_consumed_capacity: Some("TOTAL".to_string()),
            ..Default::default()
        })
        .unwrap();

    let cc = resp.consumed_capacity.unwrap();
    assert_eq!(cc.table_name, "TestTable");
    assert!(cc.capacity_units >= 1.0);
}

#[test]
fn test_update_item_consumed_capacity() {
    let db = setup();
    db.put_item(PutItemRequest {
        table_name: "TestTable".to_string(),
        item: make_item("u1"),
        condition_expression: None,
        expression_attribute_names: None,
        expression_attribute_values: None,
        ..Default::default()
    })
    .unwrap();

    let resp = db
        .update_item(dynoxide::actions::update_item::UpdateItemRequest {
            table_name: "TestTable".to_string(),
            key: {
                let mut k = HashMap::new();
                k.insert("pk".to_string(), AttributeValue::S("u1".to_string()));
                k
            },
            update_expression: Some("SET #d = :val".to_string()),
            condition_expression: None,
            expression_attribute_names: Some({
                let mut m = HashMap::new();
                m.insert("#d".to_string(), "data".to_string());
                m
            }),
            expression_attribute_values: Some({
                let mut m = HashMap::new();
                m.insert(":val".to_string(), AttributeValue::S("updated".to_string()));
                m
            }),
            return_values: None,
            return_consumed_capacity: Some("TOTAL".to_string()),
            ..Default::default()
        })
        .unwrap();

    let cc = resp.consumed_capacity.unwrap();
    assert_eq!(cc.table_name, "TestTable");
    assert!(cc.capacity_units >= 1.0);
}

#[test]
fn test_batch_get_item_consumed_capacity() {
    let db = setup();
    db.put_item(PutItemRequest {
        table_name: "TestTable".to_string(),
        item: make_item("u1"),
        condition_expression: None,
        expression_attribute_names: None,
        expression_attribute_values: None,
        ..Default::default()
    })
    .unwrap();

    let resp = db
        .batch_get_item(dynoxide::actions::batch_get_item::BatchGetItemRequest {
            request_items: {
                let mut m = HashMap::new();
                m.insert(
                    "TestTable".to_string(),
                    dynoxide::actions::batch_get_item::KeysAndAttributes {
                        keys: vec![{
                            let mut k = HashMap::new();
                            k.insert("pk".to_string(), AttributeValue::S("u1".to_string()));
                            k
                        }],
                        projection_expression: None,
                        expression_attribute_names: None,
                        consistent_read: None,
                        attributes_to_get: None,
                    },
                );
                m
            },
            return_consumed_capacity: Some("TOTAL".to_string()),
        })
        .unwrap();

    let caps = resp.consumed_capacity.unwrap();
    assert_eq!(caps.len(), 1);
    assert_eq!(caps[0].table_name, "TestTable");
    // Eventually consistent read (default) uses 0.5 RCU per 4KB
    assert!(caps[0].capacity_units >= 0.5);
}

// ---------------------------------------------------------------------------
// ReturnConsumedCapacity outside the enum, on the batch and transaction
// operations
// ---------------------------------------------------------------------------

/// The message DynamoDB returns for a `ReturnConsumedCapacity` outside the enum,
/// on every operation that takes one (captured eu-west-2, 2026-10-08, for
/// BatchGetItem, BatchWriteItem, TransactGetItems and TransactWriteItems).
const BOGUS_MODE_MESSAGE: &str = "1 validation error detected: Value 'BOGUS' at \
    'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum \
    value set: [INDEXES, TOTAL, NONE]";

fn request<T: serde::de::DeserializeOwned>(value: serde_json::Value) -> T {
    serde_json::from_value(value).unwrap()
}

fn validation_message(err: dynoxide::DynoxideError) -> String {
    match err {
        dynoxide::DynoxideError::ValidationException(msg) => msg,
        other => panic!("expected a ValidationException, got {other:?}"),
    }
}

fn stored(db: &Database, pk: &str) -> bool {
    db.get_item(request(serde_json::json!({
        "TableName": "TestTable",
        "Key": {"pk": {"S": pk}},
        "ConsistentRead": true
    })))
    .unwrap()
    .item
    .is_some()
}

fn batch_get(mode: &str) -> dynoxide::actions::batch_get_item::BatchGetItemRequest {
    request(serde_json::json!({
        "RequestItems": {"TestTable": {"Keys": [{"pk": {"S": "u1"}}]}},
        "ReturnConsumedCapacity": mode
    }))
}

fn batch_write(mode: &str, pk: &str) -> dynoxide::actions::batch_write_item::BatchWriteItemRequest {
    request(serde_json::json!({
        "RequestItems": {"TestTable": [
            {"PutRequest": {"Item": {"pk": {"S": pk}, "data": {"S": "x"}}}}
        ]},
        "ReturnConsumedCapacity": mode
    }))
}

fn transact_get(mode: &str) -> dynoxide::actions::transact_get_items::TransactGetItemsRequest {
    request(serde_json::json!({
        "TransactItems": [{"Get": {"TableName": "TestTable", "Key": {"pk": {"S": "u1"}}}}],
        "ReturnConsumedCapacity": mode
    }))
}

fn transact_write(
    mode: &str,
    pk: &str,
    token: Option<&str>,
) -> dynoxide::actions::transact_write_items::TransactWriteItemsRequest {
    let mut value = serde_json::json!({
        "TransactItems": [{"Put": {"TableName": "TestTable",
            "Item": {"pk": {"S": pk}, "data": {"S": "x"}}}}],
        "ReturnConsumedCapacity": mode
    });
    if let Some(token) = token {
        value["ClientRequestToken"] = serde_json::json!(token);
    }
    request(value)
}

/// BatchGetItem, BatchWriteItem, TransactGetItems and TransactWriteItems refuse
/// a `ReturnConsumedCapacity` outside the enum with the message the single-item
/// operations give, and the two writes write nothing.
#[test]
fn batch_and_transaction_operations_reject_an_unknown_return_consumed_capacity() {
    let db = setup();
    db.put_item(PutItemRequest {
        table_name: "TestTable".to_string(),
        item: make_item("u1"),
        ..Default::default()
    })
    .unwrap();

    let err = db.batch_get_item(batch_get("BOGUS")).unwrap_err();
    assert_eq!(validation_message(err), BOGUS_MODE_MESSAGE, "BatchGetItem");

    let err = db.batch_write_item(batch_write("BOGUS", "w1")).unwrap_err();
    assert_eq!(
        validation_message(err),
        BOGUS_MODE_MESSAGE,
        "BatchWriteItem"
    );
    assert!(
        !stored(&db, "w1"),
        "BatchWriteItem wrote despite refusing the request"
    );

    let err = db.transact_get_items(transact_get("BOGUS")).unwrap_err();
    assert_eq!(
        validation_message(err),
        BOGUS_MODE_MESSAGE,
        "TransactGetItems"
    );

    let err = db
        .transact_write_items(transact_write("BOGUS", "w2", None))
        .unwrap_err();
    assert_eq!(
        validation_message(err),
        BOGUS_MODE_MESSAGE,
        "TransactWriteItems"
    );
    assert!(
        !stored(&db, "w2"),
        "TransactWriteItems wrote despite refusing the request"
    );

    // The three modes DynamoDB accepts still go through on all four.
    for (i, mode) in ["INDEXES", "TOTAL", "NONE"].into_iter().enumerate() {
        db.batch_get_item(batch_get(mode))
            .unwrap_or_else(|e| panic!("BatchGetItem {mode}: {e:?}"));
        db.batch_write_item(batch_write(mode, &format!("b{i}")))
            .unwrap_or_else(|e| panic!("BatchWriteItem {mode}: {e:?}"));
        db.transact_get_items(transact_get(mode))
            .unwrap_or_else(|e| panic!("TransactGetItems {mode}: {e:?}"));
        db.transact_write_items(transact_write(mode, &format!("t{i}"), None))
            .unwrap_or_else(|e| panic!("TransactWriteItems {mode}: {e:?}"));
    }
}

/// A replayed `ClientRequestToken` is checked too.
///
/// A same-token, same-items call replays the stored result without running the
/// writes, and the token is keyed on the items alone, so a replay can carry a
/// capacity mode the first call never did. The check has to sit ahead of the
/// idempotency cache, as it does for ExecuteTransaction.
#[test]
fn a_replayed_transact_write_still_rejects_an_unknown_return_consumed_capacity() {
    let db = setup();
    db.transact_write_items(transact_write("TOTAL", "r1", Some("rcc-replay-token")))
        .expect("a valid mode runs and caches its result");

    let err = db
        .transact_write_items(transact_write("BOGUS", "r1", Some("rcc-replay-token")))
        .unwrap_err();
    assert_eq!(validation_message(err), BOGUS_MODE_MESSAGE);
}

/// TransactWriteItems checks `TransactItems` before `ReturnConsumedCapacity`.
///
/// An empty `TransactItems` with an unknown mode reports the `TransactItems`
/// constraint alone, in the older form with no list echoed (captured eu-west-2
/// and us-east-1, 2026-11-11). The same message comes back with and without a
/// `ClientRequestToken`; both go through the check ahead of the idempotency
/// cache, so this pins the order of that check rather than comparing two
/// separate paths.
#[test]
fn transact_write_checks_transact_items_before_return_consumed_capacity() {
    let db = setup();
    let empty = |token: Option<&str>| {
        let mut value = serde_json::json!({"TransactItems": [], "ReturnConsumedCapacity": "BOGUS"});
        if let Some(token) = token {
            value["ClientRequestToken"] = serde_json::json!(token);
        }
        request::<dynoxide::actions::transact_write_items::TransactWriteItemsRequest>(value)
    };
    let without_token = validation_message(db.transact_write_items(empty(None)).unwrap_err());
    let with_token = validation_message(
        db.transact_write_items(empty(Some("rcc-order-token")))
            .unwrap_err(),
    );
    assert_eq!(
        without_token,
        "1 validation error detected: Value at 'TransactItems' failed to satisfy constraint: \
         Member must have length greater than or equal to 1",
        "expected the TransactItems constraint alone"
    );
    assert_eq!(with_token, without_token);
}

/// The action itself refuses an unknown mode, not only `Database`.
///
/// `actions::transact_write_items::execute` is public and skips the check
/// `Database::transact_write_items` makes ahead of its idempotency cache, so
/// the check inside the action is the only guard on this path.
#[tokio::test(flavor = "current_thread")]
async fn the_transact_write_action_rejects_an_unknown_return_consumed_capacity() {
    use dynoxide::storage::Storage;

    let storage = Storage::memory().unwrap();
    dynoxide::actions::create_table::execute(
        &storage,
        request(serde_json::json!({
            "TableName": "TestTable",
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "BillingMode": "PAY_PER_REQUEST"
        })),
    )
    .await
    .unwrap();

    let err = dynoxide::actions::transact_write_items::execute(
        &storage,
        transact_write("BOGUS", "direct", None),
    )
    .await
    .unwrap_err();
    assert_eq!(validation_message(err), BOGUS_MODE_MESSAGE);

    let got = dynoxide::actions::get_item::execute(
        &storage,
        request(serde_json::json!({
            "TableName": "TestTable",
            "Key": {"pk": {"S": "direct"}},
            "ConsistentRead": true
        })),
    )
    .await
    .unwrap();
    assert!(
        got.item.is_none(),
        "the action wrote despite refusing the request"
    );
}

#[test]
fn batch_write_lists_return_consumed_capacity_after_table_names() {
    // BatchWriteItem lists a bad capacity mode after a table name that fails
    // its pattern, while a table's request list that is too long comes first
    // and alone. Captured eu-west-2, 2026-11-13; on 2026-10-09 both regions
    // reported the mode ahead of the name, and us-east-1 still listed the
    // list's length with the mode.
    let db = setup();
    let bad_name = |mode: &str| {
        request::<dynoxide::actions::batch_write_item::BatchWriteItemRequest>(serde_json::json!({
            "RequestItems": {"bad!name": [
                {"PutRequest": {"Item": {"pk": {"S": "x"}}}}
            ]},
            "ReturnConsumedCapacity": mode
        }))
    };
    let with_mode = validation_message(db.batch_write_item(bad_name("BOGUS")).unwrap_err());
    assert!(
        with_mode.starts_with("2 validation errors detected: Value '{\"bad!name\":")
            && with_mode.ends_with(&BOGUS_MODE_MESSAGE["1 validation error detected: ".len()..]),
        "expected the table name and then the mode, got {with_mode}"
    );
    let without_mode = validation_message(db.batch_write_item(bad_name("NONE")).unwrap_err());
    assert!(
        without_mode.contains("bad!name"),
        "expected the table name error without a bad mode, got {without_mode}"
    );

    let too_many: Vec<serde_json::Value> = (0..26)
        .map(|i| serde_json::json!({"PutRequest": {"Item": {"pk": {"S": format!("w{i}")}}}}))
        .collect();
    let err = db
        .batch_write_item(request(serde_json::json!({
            "RequestItems": {"TestTable": too_many},
            "ReturnConsumedCapacity": "BOGUS"
        })))
        .unwrap_err();
    let message = validation_message(err);
    assert!(
        message
            == "1 validation error detected: Value at 'RequestItems.TestTable.member' failed to \
                satisfy constraint: Member must have length less than or equal to 25",
        "expected the request count alone, got {message}"
    );
}

#[test]
fn transact_write_checks_client_request_token_before_return_consumed_capacity() {
    // An overlong ClientRequestToken is reported ahead of the capacity mode,
    // alone, in the older form with no value echoed (captured eu-west-2 and
    // us-east-1, 2026-11-11).
    let db = setup();
    let token = "x".repeat(37);
    let err = db
        .transact_write_items(transact_write("BOGUS", "tok", Some(&token)))
        .unwrap_err();
    assert_eq!(
        validation_message(err),
        "1 validation error detected: Value at 'ClientRequestToken' failed to satisfy \
         constraint: Member must have length less than or equal to 36"
    );
    assert!(!stored(&db, "tok"));
}
