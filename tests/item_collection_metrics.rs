//! Tests for ItemCollectionMetrics (Fix #5) and per-GSI ConsumedCapacity (Fix #9).

use dynoxide::Database;
use dynoxide::actions::create_table::CreateTableRequest;
use dynoxide::actions::put_item::PutItemRequest;
use dynoxide::types::{
    AttributeDefinition, AttributeValue, GlobalSecondaryIndex, KeySchemaElement, KeyType,
    Projection, ProjectionType, ScalarAttributeType,
};
use std::collections::HashMap;

/// Create a table with an LSI (required for ItemCollectionMetrics).
fn setup_table_with_lsi() -> Database {
    let db = Database::memory().unwrap();
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
        "LocalSecondaryIndexes": [
            {
                "IndexName": "StatusIndex",
                "KeySchema": [
                    {"AttributeName": "UserId", "KeyType": "HASH"},
                    {"AttributeName": "Status", "KeyType": "RANGE"}
                ],
                "Projection": {"ProjectionType": "ALL"}
            }
        ]
    });
    db.create_table(serde_json::from_value(req).unwrap())
        .unwrap();
    db
}

/// Create a table without any LSIs.
fn setup_table_without_lsi() -> Database {
    let db = Database::memory().unwrap();
    db.create_table(CreateTableRequest {
        table_name: "Simple".to_string(),
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

fn make_order(user_id: &str, order_id: &str, status: &str) -> HashMap<String, AttributeValue> {
    let mut item = HashMap::new();
    item.insert("UserId".to_string(), AttributeValue::S(user_id.to_string()));
    item.insert(
        "OrderId".to_string(),
        AttributeValue::S(order_id.to_string()),
    );
    item.insert("Status".to_string(), AttributeValue::S(status.to_string()));
    item.insert(
        "Data".to_string(),
        AttributeValue::S("some payload".to_string()),
    );
    item
}

// =========================================================================
// Fix #5: ItemCollectionMetrics tests
// =========================================================================

#[test]
fn test_put_item_with_size_on_table_with_lsi_returns_metrics() {
    let db = setup_table_with_lsi();
    let resp = db
        .put_item(PutItemRequest {
            table_name: "Orders".to_string(),
            item: make_order("user1", "order1", "PENDING"),
            return_item_collection_metrics: Some("SIZE".to_string()),
            ..Default::default()
        })
        .unwrap();

    let icm = resp
        .item_collection_metrics
        .expect("should return ItemCollectionMetrics");
    // The key should contain the partition key attribute
    assert!(icm.item_collection_key.contains_key("UserId"));
    assert_eq!(
        icm.item_collection_key.get("UserId").unwrap(),
        &AttributeValue::S("user1".to_string())
    );
    // Size estimate should be a two-element array
    assert_eq!(icm.size_estimate_range_gb.len(), 2);
    assert!(icm.size_estimate_range_gb[0] >= 0.0);
}

#[test]
fn test_put_item_with_size_on_table_without_lsi_returns_no_metrics() {
    let db = setup_table_without_lsi();
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("key1".to_string()));
    item.insert("data".to_string(), AttributeValue::S("value".to_string()));

    let resp = db
        .put_item(PutItemRequest {
            table_name: "Simple".to_string(),
            item,
            return_item_collection_metrics: Some("SIZE".to_string()),
            ..Default::default()
        })
        .unwrap();

    assert!(
        resp.item_collection_metrics.is_none(),
        "should NOT return ItemCollectionMetrics for table without LSI"
    );
}

#[test]
fn test_delete_item_with_size_on_table_with_lsi_returns_metrics() {
    let db = setup_table_with_lsi();

    // Put an item first
    db.put_item(PutItemRequest {
        table_name: "Orders".to_string(),
        item: make_order("user1", "order1", "PENDING"),
        ..Default::default()
    })
    .unwrap();

    // Delete with SIZE
    let mut key = HashMap::new();
    key.insert("UserId".to_string(), AttributeValue::S("user1".to_string()));
    key.insert(
        "OrderId".to_string(),
        AttributeValue::S("order1".to_string()),
    );

    let resp = db
        .delete_item(dynoxide::actions::delete_item::DeleteItemRequest {
            table_name: "Orders".to_string(),
            key,
            return_item_collection_metrics: Some("SIZE".to_string()),
            ..Default::default()
        })
        .unwrap();

    let icm = resp
        .item_collection_metrics
        .expect("should return ItemCollectionMetrics");
    assert!(icm.item_collection_key.contains_key("UserId"));
}

#[test]
fn test_update_item_with_size_on_table_with_lsi_returns_metrics() {
    let db = setup_table_with_lsi();

    let mut key = HashMap::new();
    key.insert("UserId".to_string(), AttributeValue::S("user1".to_string()));
    key.insert(
        "OrderId".to_string(),
        AttributeValue::S("order1".to_string()),
    );

    let resp = db
        .update_item(dynoxide::actions::update_item::UpdateItemRequest {
            table_name: "Orders".to_string(),
            key,
            update_expression: Some("SET #s = :s".to_string()),
            expression_attribute_names: Some({
                let mut m = HashMap::new();
                m.insert("#s".to_string(), "Status".to_string());
                m
            }),
            expression_attribute_values: Some({
                let mut m = HashMap::new();
                m.insert(":s".to_string(), AttributeValue::S("SHIPPED".to_string()));
                m
            }),
            return_item_collection_metrics: Some("SIZE".to_string()),
            ..Default::default()
        })
        .unwrap();

    let icm = resp
        .item_collection_metrics
        .expect("should return ItemCollectionMetrics");
    assert!(icm.item_collection_key.contains_key("UserId"));
}

#[test]
fn test_multiple_items_in_partition_reflect_total_size() {
    let db = setup_table_with_lsi();

    // Put multiple items in the same partition
    for i in 0..5 {
        db.put_item(PutItemRequest {
            table_name: "Orders".to_string(),
            item: make_order("user1", &format!("order{i}"), "PENDING"),
            ..Default::default()
        })
        .unwrap();
    }

    // Put another item and check the size reflects all items in the partition
    let resp = db
        .put_item(PutItemRequest {
            table_name: "Orders".to_string(),
            item: make_order("user1", "order5", "DONE"),
            return_item_collection_metrics: Some("SIZE".to_string()),
            ..Default::default()
        })
        .unwrap();

    let icm = resp
        .item_collection_metrics
        .expect("should return ItemCollectionMetrics");
    // Size should be larger than zero since we have 6 items
    assert!(icm.size_estimate_range_gb[0] > 0.0);
}

// =========================================================================
// Fix #9: Per-GSI ConsumedCapacity breakdown tests
// =========================================================================

/// Create a table with a GSI for consumed capacity tests.
fn setup_table_with_gsi() -> Database {
    let db = Database::memory().unwrap();
    db.create_table(CreateTableRequest {
        table_name: "Products".to_string(),
        key_schema: vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::HASH,
        }],
        attribute_definitions: vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: ScalarAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "category".to_string(),
                attribute_type: ScalarAttributeType::S,
            },
        ],
        global_secondary_indexes: Some(vec![GlobalSecondaryIndex {
            index_name: "CategoryIndex".to_string(),
            key_schema: vec![KeySchemaElement {
                attribute_name: "category".to_string(),
                key_type: KeyType::HASH,
            }],
            projection: Projection {
                projection_type: Some(ProjectionType::ALL),
                non_key_attributes: None,
            },
            provisioned_throughput: None,
        }]),
        ..Default::default()
    })
    .unwrap();
    db
}

#[test]
fn test_put_item_with_gsi_indexes_mode_returns_per_gsi_capacity() {
    let db = setup_table_with_gsi();

    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("prod1".to_string()));
    item.insert(
        "category".to_string(),
        AttributeValue::S("electronics".to_string()),
    );

    let resp = db
        .put_item(PutItemRequest {
            table_name: "Products".to_string(),
            item,
            return_consumed_capacity: Some("INDEXES".to_string()),
            ..Default::default()
        })
        .unwrap();

    let cc = resp.consumed_capacity.unwrap();
    assert_eq!(cc.table_name, "Products");
    // INDEXES mode should include Table detail
    assert!(cc.table.is_some());
    // Should include per-GSI breakdown
    let gsi_map = cc
        .global_secondary_indexes
        .expect("should have GSI breakdown");
    assert!(gsi_map.contains_key("CategoryIndex"));
    assert!(gsi_map["CategoryIndex"].capacity_units >= 1.0);
    // Total should be table + GSI
    assert!(cc.capacity_units > cc.table.as_ref().unwrap().capacity_units);
}

#[test]
fn test_put_item_with_gsi_total_mode_no_gsi_breakdown() {
    let db = setup_table_with_gsi();

    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("prod1".to_string()));
    item.insert(
        "category".to_string(),
        AttributeValue::S("electronics".to_string()),
    );

    let resp = db
        .put_item(PutItemRequest {
            table_name: "Products".to_string(),
            item,
            return_consumed_capacity: Some("TOTAL".to_string()),
            ..Default::default()
        })
        .unwrap();

    let cc = resp.consumed_capacity.unwrap();
    assert_eq!(cc.table_name, "Products");
    // TOTAL mode should NOT include per-resource breakdown
    assert!(cc.table.is_none());
    assert!(cc.global_secondary_indexes.is_none());
    // But total should still include GSI capacity
    assert!(cc.capacity_units >= 1.0);
}

#[test]
fn test_query_gsi_with_indexes_mode_attributes_capacity_to_gsi() {
    let db = setup_table_with_gsi();

    // Put some items
    for i in 0..3 {
        let mut item = HashMap::new();
        item.insert("pk".to_string(), AttributeValue::S(format!("prod{i}")));
        item.insert(
            "category".to_string(),
            AttributeValue::S("electronics".to_string()),
        );
        db.put_item(PutItemRequest {
            table_name: "Products".to_string(),
            item,
            ..Default::default()
        })
        .unwrap();
    }

    // Query the GSI with INDEXES
    let resp = db
        .query({
            let mut req = dynoxide::actions::query::QueryRequest::default();
            req.table_name = "Products".to_string();
            req.key_condition_expression = Some("category = :cat".to_string());
            req.expression_attribute_values = Some({
                let mut m = HashMap::new();
                m.insert(
                    ":cat".to_string(),
                    AttributeValue::S("electronics".to_string()),
                );
                m
            });
            req.scan_index_forward = true;
            req.index_name = Some("CategoryIndex".to_string());
            req.return_consumed_capacity = Some("INDEXES".to_string());
            req
        })
        .unwrap();

    let cc = resp.consumed_capacity.unwrap();
    assert_eq!(cc.table_name, "Products");
    // Capacity should be attributed to the GSI
    let gsi_map = cc
        .global_secondary_indexes
        .expect("should have GSI breakdown");
    assert!(gsi_map.contains_key("CategoryIndex"));
    // Eventually consistent read (default) halves the RCU
    assert!(gsi_map["CategoryIndex"].capacity_units > 0.0);
    // Table capacity should be 0 for a pure GSI read
    assert!(cc.table.is_some());
    assert_eq!(cc.table.as_ref().unwrap().capacity_units, 0.0);
}
