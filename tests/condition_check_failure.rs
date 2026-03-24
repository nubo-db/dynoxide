use dynoxide::Database;
use dynoxide::actions::create_table::CreateTableRequest;
use dynoxide::actions::delete_item::DeleteItemRequest;
use dynoxide::actions::put_item::PutItemRequest;
use dynoxide::actions::update_item::UpdateItemRequest;
use dynoxide::errors::DynoxideError;
use dynoxide::types::{
    AttributeDefinition, AttributeValue, KeySchemaElement, KeyType, ProvisionedThroughput,
    ScalarAttributeType,
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
        provisioned_throughput: Some(ProvisionedThroughput {
            read_capacity_units: Some(5),
            write_capacity_units: Some(5),
        }),
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

fn seed_item(db: &Database, pk: &str) {
    db.put_item(PutItemRequest {
        table_name: "TestTable".to_string(),
        item: make_item(pk),
        condition_expression: None,
        expression_attribute_names: None,
        expression_attribute_values: None,
        ..Default::default()
    })
    .unwrap();
}

fn pk_key(pk: &str) -> HashMap<String, AttributeValue> {
    let mut k = HashMap::new();
    k.insert("pk".to_string(), AttributeValue::S(pk.to_string()));
    k
}

// ---------------------------------------------------------------------------
// PutItem
// ---------------------------------------------------------------------------

#[test]
fn test_put_item_condition_failure_all_old_returns_item() {
    let db = setup();
    seed_item(&db, "u1");

    let err = db
        .put_item(PutItemRequest {
            table_name: "TestTable".to_string(),
            item: make_item("u1"),
            condition_expression: Some("attribute_not_exists(pk)".to_string()),
            expression_attribute_names: None,
            expression_attribute_values: None,
            return_values_on_condition_check_failure: Some("ALL_OLD".to_string()),
            ..Default::default()
        })
        .unwrap_err();

    if let DynoxideError::ConditionalCheckFailedException(msg, item) = &err {
        assert_eq!(msg, "The conditional request failed");
        let item = item.as_ref().expect("Item should be present with ALL_OLD");
        assert_eq!(item["pk"], AttributeValue::S("u1".to_string()));
        assert_eq!(item["data"], AttributeValue::S("some value".to_string()));
    } else {
        panic!("Expected ConditionalCheckFailedException, got: {:?}", err);
    }
}

#[test]
fn test_put_item_condition_failure_none_no_item() {
    let db = setup();
    seed_item(&db, "u1");

    let err = db
        .put_item(PutItemRequest {
            table_name: "TestTable".to_string(),
            item: make_item("u1"),
            condition_expression: Some("attribute_not_exists(pk)".to_string()),
            expression_attribute_names: None,
            expression_attribute_values: None,
            return_values_on_condition_check_failure: Some("NONE".to_string()),
            ..Default::default()
        })
        .unwrap_err();

    if let DynoxideError::ConditionalCheckFailedException(_, item) = &err {
        assert!(item.is_none(), "Item should not be present with NONE");
    } else {
        panic!("Expected ConditionalCheckFailedException, got: {:?}", err);
    }
}

#[test]
fn test_put_item_condition_failure_default_no_item() {
    let db = setup();
    seed_item(&db, "u1");

    let err = db
        .put_item(PutItemRequest {
            table_name: "TestTable".to_string(),
            item: make_item("u1"),
            condition_expression: Some("attribute_not_exists(pk)".to_string()),
            expression_attribute_names: None,
            expression_attribute_values: None,
            ..Default::default()
        })
        .unwrap_err();

    if let DynoxideError::ConditionalCheckFailedException(_, item) = &err {
        assert!(
            item.is_none(),
            "Item should not be present when field is absent"
        );
    } else {
        panic!("Expected ConditionalCheckFailedException, got: {:?}", err);
    }
}

// ---------------------------------------------------------------------------
// DeleteItem
// ---------------------------------------------------------------------------

#[test]
fn test_delete_item_condition_failure_all_old_returns_item() {
    let db = setup();
    seed_item(&db, "u1");

    let err = db
        .delete_item(DeleteItemRequest {
            table_name: "TestTable".to_string(),
            key: pk_key("u1"),
            return_values: None,
            condition_expression: Some("attribute_not_exists(pk)".to_string()),
            expression_attribute_names: None,
            expression_attribute_values: None,
            return_values_on_condition_check_failure: Some("ALL_OLD".to_string()),
            ..Default::default()
        })
        .unwrap_err();

    if let DynoxideError::ConditionalCheckFailedException(_, item) = &err {
        let item = item.as_ref().expect("Item should be present with ALL_OLD");
        assert_eq!(item["pk"], AttributeValue::S("u1".to_string()));
    } else {
        panic!("Expected ConditionalCheckFailedException, got: {:?}", err);
    }
}

// ---------------------------------------------------------------------------
// UpdateItem
// ---------------------------------------------------------------------------

#[test]
fn test_update_item_condition_failure_all_old_returns_item() {
    let db = setup();
    seed_item(&db, "u1");

    let err = db
        .update_item(UpdateItemRequest {
            table_name: "TestTable".to_string(),
            key: pk_key("u1"),
            update_expression: Some("SET #d = :val".to_string()),
            condition_expression: Some("attribute_not_exists(pk)".to_string()),
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
            return_values_on_condition_check_failure: Some("ALL_OLD".to_string()),
            ..Default::default()
        })
        .unwrap_err();

    if let DynoxideError::ConditionalCheckFailedException(_, item) = &err {
        let item = item.as_ref().expect("Item should be present with ALL_OLD");
        assert_eq!(item["pk"], AttributeValue::S("u1".to_string()));
        assert_eq!(item["data"], AttributeValue::S("some value".to_string()));
    } else {
        panic!("Expected ConditionalCheckFailedException, got: {:?}", err);
    }
}

// ---------------------------------------------------------------------------
// Error response JSON serialization
// ---------------------------------------------------------------------------

#[test]
fn test_error_response_includes_item_in_json() {
    let db = setup();
    seed_item(&db, "u1");

    let err = db
        .put_item(PutItemRequest {
            table_name: "TestTable".to_string(),
            item: make_item("u1"),
            condition_expression: Some("attribute_not_exists(pk)".to_string()),
            expression_attribute_names: None,
            expression_attribute_values: None,
            return_values_on_condition_check_failure: Some("ALL_OLD".to_string()),
            ..Default::default()
        })
        .unwrap_err();

    let resp = err.to_response();
    let json: serde_json::Value = serde_json::to_value(&resp).unwrap();

    assert!(json.get("__type").is_some());
    assert!(json.get("message").is_some());
    assert!(json.get("Item").is_some());
    let item = json["Item"].as_object().unwrap();
    assert!(item.contains_key("pk"));
}

#[test]
fn test_error_response_no_item_when_not_condition_failure() {
    let err = DynoxideError::ValidationException("bad input".to_string());
    let resp = err.to_response();
    let json: serde_json::Value = serde_json::to_value(&resp).unwrap();

    assert!(json.get("Item").is_none());
}
