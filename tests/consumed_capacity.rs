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

    let mut qr = dynoxide::actions::query::QueryRequest::default();
    qr.table_name = "TestTable".to_string();
    qr.key_condition_expression = Some("pk = :pk".to_string());
    qr.expression_attribute_values = Some({
        let mut m = HashMap::new();
        m.insert(":pk".to_string(), AttributeValue::S("u1".to_string()));
        m
    });
    qr.scan_index_forward = true;
    qr.return_consumed_capacity = Some("TOTAL".to_string());
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
