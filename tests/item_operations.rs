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
fn test_put_item_return_values_all_new_enveloped() {
    // PutItem only supports NONE and ALL_OLD; the action-level rejection is
    // wrapped in the request-validation envelope (eu-west-2).
    let db = make_db();
    create_hash_only_table(&db, "Items");

    let err = db
        .put_item(PutItemRequest {
            table_name: "Items".to_string(),
            item: make_item(&[("pk", AttributeValue::S("k1".into()))]),
            return_values: Some("ALL_NEW".into()),
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "1 validation error detected: ReturnValues can only be ALL_OLD or NONE"
    );
}

#[test]
fn test_put_item_body_null_false_enveloped() {
    // The in-process API constructs AttributeValue directly, bypassing the
    // request deserialiser that rejects {"NULL": false} on the HTTP path; the
    // action-level check gives the same enveloped rejection (eu-west-2).
    let db = make_db();
    create_hash_only_table(&db, "Items");

    let err = db
        .put_item(PutItemRequest {
            table_name: "Items".to_string(),
            item: make_item(&[
                ("pk", AttributeValue::S("k1".into())),
                ("flag", AttributeValue::NULL(false)),
            ]),
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "1 validation error detected: One or more parameter values were invalid: \
         Null attribute value types must have the value of true"
    );
}

#[test]
fn test_put_item_nested_null_false_enveloped() {
    // The rejection recurses through lists and maps, like the deserialiser.
    let db = make_db();
    create_hash_only_table(&db, "Items");

    let nested = AttributeValue::M(
        [("flag".to_string(), AttributeValue::NULL(false))]
            .into_iter()
            .collect(),
    );
    let err = db
        .put_item(PutItemRequest {
            table_name: "Items".to_string(),
            item: make_item(&[
                ("pk", AttributeValue::S("k1".into())),
                ("data", AttributeValue::L(vec![nested])),
            ]),
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "1 validation error detected: One or more parameter values were invalid: \
         Null attribute value types must have the value of true"
    );
}

#[test]
fn test_put_item_eav_null_false_enveloped() {
    // {NULL: false} in ExpressionAttributeValues gets the bare inner message
    // inside the envelope, not the per-key "contains invalid value" wrapper.
    let db = make_db();
    create_hash_only_table(&db, "Items");

    let err = db
        .put_item(PutItemRequest {
            table_name: "Items".to_string(),
            item: make_item(&[("pk", AttributeValue::S("k1".into()))]),
            condition_expression: Some("flag = :n".into()),
            expression_attribute_values: Some(make_item(&[(":n", AttributeValue::NULL(false))])),
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "1 validation error detected: One or more parameter values were invalid: \
         Null attribute value types must have the value of true"
    );
}

#[test]
fn test_update_item_return_values_invalid_enveloped_exactly_once() {
    // The in-process backstop for an invalid ReturnValues enum produces the
    // same enveloped message as the request deserialiser, with exactly one
    // envelope.
    let db = make_db();
    create_hash_only_table(&db, "Items");

    let err = db
        .update_item(dynoxide::actions::update_item::UpdateItemRequest {
            table_name: "Items".to_string(),
            key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
            return_values: Some("BOGUS".into()),
            ..Default::default()
        })
        .unwrap_err()
        .to_string();
    assert_eq!(
        err,
        "1 validation error detected: Value 'BOGUS' at 'returnValues' failed to satisfy \
         constraint: Member must satisfy enum value set: \
         [ALL_NEW, UPDATED_OLD, ALL_OLD, NONE, UPDATED_NEW]"
    );
    assert_eq!(
        err.matches("validation error detected").count(),
        1,
        "expected exactly one envelope, got: {err}"
    );
}

#[test]
fn test_put_item_key_type_mismatch_stays_bare() {
    // Type mismatch for a key attribute is captured bare (eu-west-2); it must
    // not gain the request-validation envelope.
    let db = make_db();
    create_hash_only_table(&db, "Items");

    let err = db
        .put_item(PutItemRequest {
            table_name: "Items".to_string(),
            item: make_item(&[("pk", AttributeValue::N("5".into()))]),
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "One or more parameter values were invalid: Type mismatch for key pk expected: S actual: N"
    );
}

#[test]
fn test_put_item_empty_string_key_stays_bare() {
    let db = make_db();
    create_hash_only_table(&db, "Items");

    let err = db
        .put_item(PutItemRequest {
            table_name: "Items".to_string(),
            item: make_item(&[("pk", AttributeValue::S("".into()))]),
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "One or more parameter values are not valid. The AttributeValue for a key attribute \
         cannot contain an empty string value. Key: pk"
    );
}

#[test]
fn test_put_item_oversized_item_stays_bare() {
    // Item-size-exceeded is captured bare (eu-west-2).
    let db = make_db();
    create_hash_only_table(&db, "Items");

    let err = db
        .put_item(PutItemRequest {
            table_name: "Items".to_string(),
            item: make_item(&[
                ("pk", AttributeValue::S("k1".into())),
                ("blob", AttributeValue::S("x".repeat(400 * 1024))),
            ]),
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "Item size has exceeded the maximum allowed size"
    );
}

#[test]
fn test_put_item_invalid_table_name_enveloped_exactly_once() {
    // The table-name constraint error already carries its own envelope; the
    // operation boundary must not add a second one.
    let db = make_db();

    let err = db
        .put_item(PutItemRequest {
            table_name: "bad name".to_string(),
            item: make_item(&[("pk", AttributeValue::S("k1".into()))]),
            ..Default::default()
        })
        .unwrap_err()
        .to_string();
    assert_eq!(
        err.matches("validation error detected").count(),
        1,
        "expected exactly one envelope, got: {err}"
    );
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

#[test]
fn test_put_item_rejects_null_false() {
    // AWS rejects {"NULL": false}; only {"NULL": true} is valid. The request
    // deserialiser surfaces it as a validation error before any write.
    let json = r#"{
        "TableName": "Items",
        "Item": {
            "pk": {"S": "null-false"},
            "flag": {"NULL": false}
        }
    }"#;

    let err = serde_json::from_str::<PutItemRequest>(json).unwrap_err();
    assert!(
        err.to_string().contains("must have the value of true"),
        "unexpected error: {err}"
    );
}

#[test]
fn test_delete_item_null_false_in_eav_is_validation_exception() {
    // A rejected {"NULL": false} supplied as an ExpressionAttributeValue on the
    // DeleteItem raw-EAV path must surface as a ValidationException with the
    // per-key "contains invalid value" wrapper, not a SerializationException
    // leaking the internal validation prefix.
    let db = make_db();
    create_hash_only_table(&db, "Items");

    let req: DeleteItemRequest = serde_json::from_value(serde_json::json!({
        "TableName": "Items",
        "Key": {"pk": {"S": "k1"}},
        "ConditionExpression": "flag = :n",
        "ExpressionAttributeValues": {":n": {"NULL": false}}
    }))
    .unwrap();

    let err = db.delete_item(req).unwrap_err();
    assert_eq!(
        err.error_type(),
        "com.amazon.coral.validate#ValidationException"
    );
    // Real DynamoDB returns the bare validation message, not the per-key
    // "contains invalid value" wrapper used for empty/malformed values.
    assert!(
        err.to_string().contains(
            "One or more parameter values were invalid: \
             Null attribute value types must have the value of true"
        ),
        "unexpected error: {err}"
    );
    assert!(
        !err.to_string()
            .contains("ExpressionAttributeValues contains invalid value"),
        "unexpected per-key wrapper: {err}"
    );
    assert!(
        !err.to_string().contains("VALIDATION:"),
        "internal prefix leaked to client: {err}"
    );
}

#[test]
fn test_update_item_attribute_updates_null_false_enveloped() {
    // The in-process API constructs AttributeValueUpdate directly, bypassing
    // the request deserialiser that rejects {"NULL": false} on the HTTP path;
    // the action-level check gives the same enveloped rejection as the Key and
    // ExpressionAttributeValues positions, and the item is left untouched.
    let db = make_db();
    create_hash_only_table(&db, "Items");
    db.put_item(PutItemRequest {
        table_name: "Items".to_string(),
        item: make_item(&[
            ("pk", AttributeValue::S("k1".into())),
            ("name", AttributeValue::S("before".into())),
        ]),
        ..Default::default()
    })
    .unwrap();

    let err = db
        .update_item(dynoxide::actions::update_item::UpdateItemRequest {
            table_name: "Items".to_string(),
            key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
            attribute_updates: Some(HashMap::from([(
                "flag".to_string(),
                dynoxide::actions::update_item::AttributeValueUpdate {
                    action: "PUT".to_string(),
                    value: Some(AttributeValue::NULL(false)),
                },
            )])),
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "1 validation error detected: One or more parameter values were invalid: \
         Null attribute value types must have the value of true"
    );

    let item = db
        .get_item(GetItemRequest {
            table_name: "Items".to_string(),
            key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
            ..Default::default()
        })
        .unwrap()
        .item
        .unwrap();
    assert_eq!(item["name"], AttributeValue::S("before".into()));
    assert!(!item.contains_key("flag"), "item was modified: {item:?}");
}

// ---------------------------------------------------------------------------
// Empty-string key value on the single-action key paths
// ---------------------------------------------------------------------------
//
// An empty-string key value returns a top-level ValidationException carrying the
// "...are not valid..." wording, matching the real-AWS GetItem/DeleteItem/
// UpdateItem baseline. The error surfaces top-level (HTTP 400) on all three.

const SINGLE_ACTION_EMPTY_KEY_MSG: &str = "One or more parameter values are not valid. The AttributeValue for a key attribute cannot contain an empty string value. Key: pk";

#[test]
fn test_get_item_empty_string_key_is_top_level_validation() {
    let db = make_db();
    create_hash_only_table(&db, "Items");
    let err = db
        .get_item(GetItemRequest {
            table_name: "Items".to_string(),
            key: key_map(&[("pk", AttributeValue::S(String::new()))]),
            consistent_read: None,
            projection_expression: None,
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(
        err.error_type(),
        "com.amazon.coral.validate#ValidationException"
    );
    assert_eq!(err.status_code(), 400);
    assert_eq!(err.to_string(), SINGLE_ACTION_EMPTY_KEY_MSG);
}

#[test]
fn test_delete_item_empty_string_key_is_top_level_validation() {
    let db = make_db();
    create_hash_only_table(&db, "Items");
    let err = db
        .delete_item(DeleteItemRequest {
            table_name: "Items".to_string(),
            key: key_map(&[("pk", AttributeValue::S(String::new()))]),
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(
        err.error_type(),
        "com.amazon.coral.validate#ValidationException"
    );
    assert_eq!(err.status_code(), 400);
    assert_eq!(err.to_string(), SINGLE_ACTION_EMPTY_KEY_MSG);
}

#[test]
fn test_update_item_empty_string_key_is_top_level_validation() {
    let db = make_db();
    create_hash_only_table(&db, "Items");
    let req: dynoxide::actions::update_item::UpdateItemRequest =
        serde_json::from_value(serde_json::json!({
            "TableName": "Items",
            "Key": {"pk": {"S": ""}},
            "UpdateExpression": "SET attr1 = :v",
            "ExpressionAttributeValues": {":v": {"S": "x"}}
        }))
        .unwrap();
    let err = db.update_item(req).unwrap_err();
    assert_eq!(
        err.error_type(),
        "com.amazon.coral.validate#ValidationException"
    );
    assert_eq!(err.status_code(), 400);
    assert_eq!(err.to_string(), SINGLE_ACTION_EMPTY_KEY_MSG);
}

// ---------------------------------------------------------------------------
// Empty-binary key value on the single-action key paths
// ---------------------------------------------------------------------------
//
// Mirrors the empty-string cases on a binary-keyed table: a top-level
// ValidationException with the "...empty binary value..." message (real AWS,
// 2026-06-24 capture).

const SINGLE_ACTION_EMPTY_BINARY_MSG: &str = "One or more parameter values are not valid. The AttributeValue for a key attribute cannot contain an empty binary value. Key: pk";

fn create_binary_key_table(db: &Database, name: &str) {
    let req: CreateTableRequest = serde_json::from_value(serde_json::json!({
        "TableName": name,
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "B"}]
    }))
    .unwrap();
    db.create_table(req).unwrap();
}

#[test]
fn test_get_item_empty_binary_key_is_top_level_validation() {
    let db = make_db();
    create_binary_key_table(&db, "BinItems");
    let err = db
        .get_item(GetItemRequest {
            table_name: "BinItems".to_string(),
            key: key_map(&[("pk", AttributeValue::B(vec![]))]),
            consistent_read: None,
            projection_expression: None,
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(
        err.error_type(),
        "com.amazon.coral.validate#ValidationException"
    );
    assert_eq!(err.status_code(), 400);
    assert_eq!(err.to_string(), SINGLE_ACTION_EMPTY_BINARY_MSG);
}

#[test]
fn test_delete_item_empty_binary_key_is_top_level_validation() {
    let db = make_db();
    create_binary_key_table(&db, "BinItems");
    let err = db
        .delete_item(DeleteItemRequest {
            table_name: "BinItems".to_string(),
            key: key_map(&[("pk", AttributeValue::B(vec![]))]),
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(
        err.error_type(),
        "com.amazon.coral.validate#ValidationException"
    );
    assert_eq!(err.status_code(), 400);
    assert_eq!(err.to_string(), SINGLE_ACTION_EMPTY_BINARY_MSG);
}

#[test]
fn test_update_item_empty_binary_key_is_top_level_validation() {
    let db = make_db();
    create_binary_key_table(&db, "BinItems");
    let req: dynoxide::actions::update_item::UpdateItemRequest =
        serde_json::from_value(serde_json::json!({
            "TableName": "BinItems",
            "Key": {"pk": {"B": ""}},
            "UpdateExpression": "SET attr1 = :v",
            "ExpressionAttributeValues": {":v": {"S": "x"}}
        }))
        .unwrap();
    let err = db.update_item(req).unwrap_err();
    assert_eq!(
        err.error_type(),
        "com.amazon.coral.validate#ValidationException"
    );
    assert_eq!(err.status_code(), 400);
    assert_eq!(err.to_string(), SINGLE_ACTION_EMPTY_BINARY_MSG);
}

#[test]
fn test_put_item_empty_binary_key_is_top_level_validation() {
    let db = make_db();
    create_binary_key_table(&db, "BinItems");
    let err = db
        .put_item(PutItemRequest {
            table_name: "BinItems".to_string(),
            item: make_item(&[("pk", AttributeValue::B(vec![]))]),
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(
        err.error_type(),
        "com.amazon.coral.validate#ValidationException"
    );
    assert_eq!(err.status_code(), 400);
    assert_eq!(err.to_string(), SINGLE_ACTION_EMPTY_BINARY_MSG);
}
