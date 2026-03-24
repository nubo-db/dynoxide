use dynoxide::Database;
use dynoxide::actions::create_table::{CreateTableRequest, StreamSpecification};
use dynoxide::actions::describe_time_to_live::DescribeTimeToLiveRequest;
use dynoxide::actions::get_records::GetRecordsRequest;
use dynoxide::actions::get_shard_iterator::GetShardIteratorRequest;
use dynoxide::actions::list_streams::ListStreamsRequest;
use dynoxide::actions::put_item::PutItemRequest;
use dynoxide::actions::update_time_to_live::{TimeToLiveSpecification, UpdateTimeToLiveRequest};
use dynoxide::types::{
    AttributeDefinition, AttributeValue, KeySchemaElement, KeyType, ScalarAttributeType,
};
use std::collections::HashMap;

fn create_test_table(db: &Database, table_name: &str) {
    let request = CreateTableRequest {
        table_name: table_name.to_string(),
        key_schema: vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::HASH,
        }],
        attribute_definitions: vec![AttributeDefinition {
            attribute_name: "pk".to_string(),
            attribute_type: ScalarAttributeType::S,
        }],
        ..Default::default()
    };
    db.create_table(request).unwrap();
}

fn create_test_table_with_streams(db: &Database, table_name: &str) {
    let request = CreateTableRequest {
        table_name: table_name.to_string(),
        key_schema: vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::HASH,
        }],
        attribute_definitions: vec![AttributeDefinition {
            attribute_name: "pk".to_string(),
            attribute_type: ScalarAttributeType::S,
        }],
        global_secondary_indexes: None,
        provisioned_throughput: None,
        billing_mode: None,
        stream_specification: Some(StreamSpecification {
            stream_enabled: true,
            stream_view_type: Some("NEW_AND_OLD_IMAGES".to_string()),
        }),
        ..Default::default()
    };
    db.create_table(request).unwrap();
}

fn put_item_with_ttl(db: &Database, table_name: &str, pk: &str, ttl_value: i64) {
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S(pk.to_string()));
    item.insert("ttl".to_string(), AttributeValue::N(ttl_value.to_string()));

    db.put_item(PutItemRequest {
        table_name: table_name.to_string(),
        item,
        condition_expression: None,
        expression_attribute_names: None,
        expression_attribute_values: None,
        return_values: None,
        ..Default::default()
    })
    .unwrap();
}

fn enable_ttl(db: &Database, table_name: &str, attribute_name: &str) {
    db.update_time_to_live(UpdateTimeToLiveRequest {
        table_name: table_name.to_string(),
        time_to_live_specification: TimeToLiveSpecification {
            attribute_name: attribute_name.to_string(),
            enabled: true,
        },
    })
    .unwrap();
}

fn disable_ttl(db: &Database, table_name: &str, attribute_name: &str) {
    db.update_time_to_live(UpdateTimeToLiveRequest {
        table_name: table_name.to_string(),
        time_to_live_specification: TimeToLiveSpecification {
            attribute_name: attribute_name.to_string(),
            enabled: false,
        },
    })
    .unwrap();
}

fn item_count(db: &Database, table_name: &str) -> usize {
    let mut req = dynoxide::actions::scan::ScanRequest::default();
    req.table_name = table_name.to_string();
    let resp = db.scan(req).unwrap();
    resp.count as usize
}

// -----------------------------------------------------------------------
// UpdateTimeToLive / DescribeTimeToLive
// -----------------------------------------------------------------------

#[test]
fn test_enable_ttl_on_table() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "TestTable");

    let resp = db
        .update_time_to_live(UpdateTimeToLiveRequest {
            table_name: "TestTable".to_string(),
            time_to_live_specification: TimeToLiveSpecification {
                attribute_name: "ttl".to_string(),
                enabled: true,
            },
        })
        .unwrap();

    assert!(resp.time_to_live_specification.enabled);
    assert_eq!(resp.time_to_live_specification.attribute_name, "ttl");
}

#[test]
fn test_describe_time_to_live_enabled() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "TestTable");
    enable_ttl(&db, "TestTable", "expiry");

    let resp = db
        .describe_time_to_live(DescribeTimeToLiveRequest {
            table_name: "TestTable".to_string(),
        })
        .unwrap();

    assert_eq!(resp.time_to_live_description.time_to_live_status, "ENABLED");
    assert_eq!(
        resp.time_to_live_description.attribute_name.as_deref(),
        Some("expiry")
    );
}

#[test]
fn test_describe_time_to_live_disabled() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "TestTable");

    let resp = db
        .describe_time_to_live(DescribeTimeToLiveRequest {
            table_name: "TestTable".to_string(),
        })
        .unwrap();

    assert_eq!(
        resp.time_to_live_description.time_to_live_status,
        "DISABLED"
    );
    assert!(resp.time_to_live_description.attribute_name.is_none());
}

#[test]
fn test_update_ttl_nonexistent_table() {
    let db = Database::memory().unwrap();

    let result = db.update_time_to_live(UpdateTimeToLiveRequest {
        table_name: "NonExistent".to_string(),
        time_to_live_specification: TimeToLiveSpecification {
            attribute_name: "ttl".to_string(),
            enabled: true,
        },
    });

    assert!(result.is_err());
}

// -----------------------------------------------------------------------
// TTL Sweep — expired items deleted
// -----------------------------------------------------------------------

#[test]
fn test_expired_item_deleted_after_sweep() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "TestTable");
    enable_ttl(&db, "TestTable", "ttl");

    // Put item with TTL in the past (epoch 1000)
    put_item_with_ttl(&db, "TestTable", "item1", 1000);

    assert_eq!(item_count(&db, "TestTable"), 1);

    let deleted = db.sweep_ttl().unwrap();
    assert_eq!(deleted, 1);
    assert_eq!(item_count(&db, "TestTable"), 0);
}

#[test]
fn test_future_ttl_item_not_deleted() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "TestTable");
    enable_ttl(&db, "TestTable", "ttl");

    // Put item with TTL far in the future
    put_item_with_ttl(&db, "TestTable", "item1", 9999999999);

    let deleted = db.sweep_ttl().unwrap();
    assert_eq!(deleted, 0);
    assert_eq!(item_count(&db, "TestTable"), 1);
}

#[test]
fn test_item_without_ttl_attribute_not_deleted() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "TestTable");
    enable_ttl(&db, "TestTable", "ttl");

    // Put item WITHOUT the ttl attribute
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("item1".to_string()));
    item.insert("data".to_string(), AttributeValue::S("hello".to_string()));
    db.put_item(PutItemRequest {
        table_name: "TestTable".to_string(),
        item,
        condition_expression: None,
        expression_attribute_names: None,
        expression_attribute_values: None,
        return_values: None,
        ..Default::default()
    })
    .unwrap();

    let deleted = db.sweep_ttl().unwrap();
    assert_eq!(deleted, 0);
    assert_eq!(item_count(&db, "TestTable"), 1);
}

#[test]
fn test_non_numeric_ttl_not_deleted() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "TestTable");
    enable_ttl(&db, "TestTable", "ttl");

    // Put item with string TTL attribute (wrong type)
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("item1".to_string()));
    item.insert(
        "ttl".to_string(),
        AttributeValue::S("not-a-number".to_string()),
    );
    db.put_item(PutItemRequest {
        table_name: "TestTable".to_string(),
        item,
        condition_expression: None,
        expression_attribute_names: None,
        expression_attribute_values: None,
        return_values: None,
        ..Default::default()
    })
    .unwrap();

    let deleted = db.sweep_ttl().unwrap();
    assert_eq!(deleted, 0);
    assert_eq!(item_count(&db, "TestTable"), 1);
}

// -----------------------------------------------------------------------
// TTL Sweep — stream record generation
// -----------------------------------------------------------------------

#[test]
fn test_ttl_deletion_generates_stream_remove_record() {
    let db = Database::memory().unwrap();
    create_test_table_with_streams(&db, "TestTable");
    enable_ttl(&db, "TestTable", "ttl");

    // Put item (generates INSERT stream record)
    put_item_with_ttl(&db, "TestTable", "item1", 1000);

    // Sweep (should generate REMOVE stream record with user identity)
    let deleted = db.sweep_ttl().unwrap();
    assert_eq!(deleted, 1);

    // Get stream records
    let streams_resp = db
        .list_streams(ListStreamsRequest {
            table_name: Some("TestTable".to_string()),
            exclusive_start_stream_arn: None,
            limit: None,
        })
        .unwrap();
    assert_eq!(streams_resp.streams.len(), 1);
    let stream_arn = &streams_resp.streams[0].stream_arn;

    let iter_resp = db
        .get_shard_iterator(GetShardIteratorRequest {
            stream_arn: stream_arn.clone(),
            shard_id: format!("shardId-00000001-TestTable"),
            shard_iterator_type: "TRIM_HORIZON".to_string(),
            sequence_number: None,
        })
        .unwrap();

    let records_resp = db
        .get_records(GetRecordsRequest {
            shard_iterator: iter_resp.shard_iterator.unwrap(),
            limit: None,
        })
        .unwrap();

    // Should have 2 records: INSERT from put, REMOVE from TTL sweep
    assert_eq!(records_resp.records.len(), 2);

    let insert_record = &records_resp.records[0];
    assert_eq!(insert_record.event_name, "INSERT");
    assert!(insert_record.user_identity.is_none());

    let remove_record = &records_resp.records[1];
    assert_eq!(remove_record.event_name, "REMOVE");
    assert!(remove_record.user_identity.is_some());
    let identity = remove_record.user_identity.as_ref().unwrap();
    assert_eq!(identity.identity_type, "Service");
    assert_eq!(identity.principal_id, "dynamodb.amazonaws.com");
}

#[test]
fn test_ttl_deletion_no_stream_when_disabled() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "TestTable"); // No streams
    enable_ttl(&db, "TestTable", "ttl");

    put_item_with_ttl(&db, "TestTable", "item1", 1000);

    let deleted = db.sweep_ttl().unwrap();
    assert_eq!(deleted, 1);

    // Should be no streams for this table
    let streams_resp = db
        .list_streams(ListStreamsRequest {
            table_name: Some("TestTable".to_string()),
            exclusive_start_stream_arn: None,
            limit: None,
        })
        .unwrap();
    assert!(streams_resp.streams.is_empty());
}

// -----------------------------------------------------------------------
// TTL disable stops expiry
// -----------------------------------------------------------------------

#[test]
fn test_disable_ttl_stops_expiry() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "TestTable");
    enable_ttl(&db, "TestTable", "ttl");

    put_item_with_ttl(&db, "TestTable", "item1", 1000);

    // Disable TTL before sweep
    disable_ttl(&db, "TestTable", "ttl");

    let deleted = db.sweep_ttl().unwrap();
    assert_eq!(deleted, 0);
    assert_eq!(item_count(&db, "TestTable"), 1);
}

// -----------------------------------------------------------------------
// TTL with GSI maintenance
// -----------------------------------------------------------------------

#[test]
fn test_ttl_deletion_removes_from_gsi() {
    let db = Database::memory().unwrap();

    // Create table with GSI
    let request = CreateTableRequest {
        table_name: "TestTable".to_string(),
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
                attribute_name: "gsi_pk".to_string(),
                attribute_type: ScalarAttributeType::S,
            },
        ],
        global_secondary_indexes: Some(vec![dynoxide::types::GlobalSecondaryIndex {
            index_name: "ByGsiPk".to_string(),
            key_schema: vec![KeySchemaElement {
                attribute_name: "gsi_pk".to_string(),
                key_type: KeyType::HASH,
            }],
            projection: dynoxide::types::Projection {
                projection_type: Some(dynoxide::types::ProjectionType::ALL),
                non_key_attributes: None,
            },
            provisioned_throughput: None,
        }]),
        provisioned_throughput: None,
        billing_mode: None,
        stream_specification: None,
        ..Default::default()
    };
    db.create_table(request).unwrap();
    enable_ttl(&db, "TestTable", "ttl");

    // Put item with GSI attribute and expired TTL
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("item1".to_string()));
    item.insert(
        "gsi_pk".to_string(),
        AttributeValue::S("gsi_value".to_string()),
    );
    item.insert("ttl".to_string(), AttributeValue::N("1000".to_string()));

    db.put_item(PutItemRequest {
        table_name: "TestTable".to_string(),
        item,
        condition_expression: None,
        expression_attribute_names: None,
        expression_attribute_values: None,
        return_values: None,
        ..Default::default()
    })
    .unwrap();

    // Verify item exists in GSI
    let mut gsi_req = dynoxide::actions::scan::ScanRequest::default();
    gsi_req.table_name = "TestTable".to_string();
    gsi_req.index_name = Some("ByGsiPk".to_string());
    let gsi_scan = db.scan(gsi_req).unwrap();
    assert_eq!(gsi_scan.count, 1);

    // Sweep
    let deleted = db.sweep_ttl().unwrap();
    assert_eq!(deleted, 1);

    // Verify item removed from base table
    assert_eq!(item_count(&db, "TestTable"), 0);

    // Verify item removed from GSI
    let mut gsi_req2 = dynoxide::actions::scan::ScanRequest::default();
    gsi_req2.table_name = "TestTable".to_string();
    gsi_req2.index_name = Some("ByGsiPk".to_string());
    let gsi_scan = db.scan(gsi_req2).unwrap();
    assert_eq!(gsi_scan.count, 0);
}

// -----------------------------------------------------------------------
// Multiple items, mixed expiry
// -----------------------------------------------------------------------

#[test]
fn test_mixed_expiry_only_expired_items_deleted() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "TestTable");
    enable_ttl(&db, "TestTable", "ttl");

    // Expired
    put_item_with_ttl(&db, "TestTable", "expired1", 100);
    put_item_with_ttl(&db, "TestTable", "expired2", 200);
    // Not expired
    put_item_with_ttl(&db, "TestTable", "alive1", 9999999999);
    put_item_with_ttl(&db, "TestTable", "alive2", 9999999998);

    assert_eq!(item_count(&db, "TestTable"), 4);

    let deleted = db.sweep_ttl().unwrap();
    assert_eq!(deleted, 2);
    assert_eq!(item_count(&db, "TestTable"), 2);
}

#[test]
fn test_no_items_expired_returns_zero() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "TestTable");
    enable_ttl(&db, "TestTable", "ttl");

    // All items have future TTL
    put_item_with_ttl(&db, "TestTable", "item1", 9999999999);
    put_item_with_ttl(&db, "TestTable", "item2", 9999999998);

    let deleted = db.sweep_ttl().unwrap();
    assert_eq!(deleted, 0);
}

#[test]
fn test_sweep_with_no_ttl_enabled_tables() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "TestTable");

    put_item_with_ttl(&db, "TestTable", "item1", 1000);

    // No TTL enabled, sweep should do nothing
    let deleted = db.sweep_ttl().unwrap();
    assert_eq!(deleted, 0);
    assert_eq!(item_count(&db, "TestTable"), 1);
}

#[test]
fn test_update_ttl_empty_attribute_name_rejected() {
    let db = Database::memory().unwrap();

    // Should fail even without creating a table (validation fires before table lookup)
    let result = db.update_time_to_live(UpdateTimeToLiveRequest {
        table_name: "TestTable".to_string(),
        time_to_live_specification: TimeToLiveSpecification {
            attribute_name: "".to_string(),
            enabled: true,
        },
    });

    let err = result.unwrap_err();
    let err_str = err.to_string();
    assert!(
        err_str.contains("timeToLiveSpecification.attributeName"),
        "Expected attributeName validation error, got: {}",
        err_str
    );
}
