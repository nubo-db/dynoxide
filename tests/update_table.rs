use dynoxide::Database;
use dynoxide::actions::create_table::CreateTableRequest;
use dynoxide::actions::update_table::UpdateTableRequest;
use dynoxide::types::{AttributeDefinition, KeySchemaElement, KeyType, ScalarAttributeType};
use serde_json::json;

fn make_db() -> Database {
    Database::memory().unwrap()
}

fn create_simple_table(db: &Database, name: &str) {
    let req = CreateTableRequest {
        table_name: name.to_string(),
        key_schema: vec![
            KeySchemaElement {
                attribute_name: "PK".to_string(),
                key_type: KeyType::HASH,
            },
            KeySchemaElement {
                attribute_name: "SK".to_string(),
                key_type: KeyType::RANGE,
            },
        ],
        attribute_definitions: vec![
            AttributeDefinition {
                attribute_name: "PK".to_string(),
                attribute_type: ScalarAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "SK".to_string(),
                attribute_type: ScalarAttributeType::S,
            },
        ],
        ..Default::default()
    };
    db.create_table(req).unwrap();
}

fn put_item(
    db: &Database,
    table: &str,
    pk: &str,
    sk: &str,
    gsi1pk: Option<&str>,
    gsi1sk: Option<&str>,
) {
    let mut item = json!({
        "PK": {"S": pk},
        "SK": {"S": sk},
    });
    if let Some(gpk) = gsi1pk {
        item["GSI1PK"] = json!({"S": gpk});
    }
    if let Some(gsk) = gsi1sk {
        item["GSI1SK"] = json!({"S": gsk});
    }
    let req = serde_json::from_value(json!({
        "TableName": table,
        "Item": item,
    }))
    .unwrap();
    db.put_item(req).unwrap();
}

#[test]
fn test_update_table_create_gsi() {
    let db = make_db();
    create_simple_table(&db, "TestTable");

    let req: UpdateTableRequest = serde_json::from_value(json!({
        "TableName": "TestTable",
        "AttributeDefinitions": [
            {"AttributeName": "PK", "AttributeType": "S"},
            {"AttributeName": "SK", "AttributeType": "S"},
            {"AttributeName": "GSI1PK", "AttributeType": "S"},
            {"AttributeName": "GSI1SK", "AttributeType": "S"},
        ],
        "GlobalSecondaryIndexUpdates": [{
            "Create": {
                "IndexName": "GSI1",
                "KeySchema": [
                    {"AttributeName": "GSI1PK", "KeyType": "HASH"},
                    {"AttributeName": "GSI1SK", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            }
        }]
    }))
    .unwrap();

    let resp = db.update_table(req).unwrap();
    assert_eq!(resp.table_description.table_name, "TestTable");

    let gsis = resp.table_description.global_secondary_indexes.unwrap();
    assert_eq!(gsis.len(), 1);
    assert_eq!(gsis[0].index_name, "GSI1");
    assert_eq!(gsis[0].index_status, "ACTIVE");
}

#[test]
fn test_update_table_create_gsi_backfills_existing_items() {
    let db = make_db();
    create_simple_table(&db, "TestTable");

    // Put items BEFORE creating the GSI
    put_item(
        &db,
        "TestTable",
        "user#1",
        "profile",
        Some("org#A"),
        Some("user#1"),
    );
    put_item(
        &db,
        "TestTable",
        "user#2",
        "profile",
        Some("org#A"),
        Some("user#2"),
    );
    put_item(
        &db,
        "TestTable",
        "user#3",
        "profile",
        Some("org#B"),
        Some("user#3"),
    );
    // This item lacks GSI keys — should NOT appear in GSI
    put_item(&db, "TestTable", "user#4", "settings", None, None);

    // Now create the GSI
    let req: UpdateTableRequest = serde_json::from_value(json!({
        "TableName": "TestTable",
        "AttributeDefinitions": [
            {"AttributeName": "PK", "AttributeType": "S"},
            {"AttributeName": "SK", "AttributeType": "S"},
            {"AttributeName": "GSI1PK", "AttributeType": "S"},
            {"AttributeName": "GSI1SK", "AttributeType": "S"},
        ],
        "GlobalSecondaryIndexUpdates": [{
            "Create": {
                "IndexName": "GSI1",
                "KeySchema": [
                    {"AttributeName": "GSI1PK", "KeyType": "HASH"},
                    {"AttributeName": "GSI1SK", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            }
        }]
    }))
    .unwrap();
    db.update_table(req).unwrap();

    // Query the GSI — should find backfilled items
    let query_req = serde_json::from_value(json!({
        "TableName": "TestTable",
        "IndexName": "GSI1",
        "KeyConditionExpression": "GSI1PK = :pk",
        "ExpressionAttributeValues": {":pk": {"S": "org#A"}},
    }))
    .unwrap();
    let resp = db.query(query_req).unwrap();
    assert_eq!(resp.count, 2);

    // Query org#B
    let query_req = serde_json::from_value(json!({
        "TableName": "TestTable",
        "IndexName": "GSI1",
        "KeyConditionExpression": "GSI1PK = :pk",
        "ExpressionAttributeValues": {":pk": {"S": "org#B"}},
    }))
    .unwrap();
    let resp = db.query(query_req).unwrap();
    assert_eq!(resp.count, 1);
}

#[test]
fn test_update_table_delete_gsi() {
    let db = make_db();

    // Create table with a GSI
    let req: CreateTableRequest = serde_json::from_value(json!({
        "TableName": "TestTable",
        "KeySchema": [
            {"AttributeName": "PK", "KeyType": "HASH"},
            {"AttributeName": "SK", "KeyType": "RANGE"},
        ],
        "AttributeDefinitions": [
            {"AttributeName": "PK", "AttributeType": "S"},
            {"AttributeName": "SK", "AttributeType": "S"},
            {"AttributeName": "GSI1PK", "AttributeType": "S"},
            {"AttributeName": "GSI1SK", "AttributeType": "S"},
        ],
        "GlobalSecondaryIndexes": [{
            "IndexName": "GSI1",
            "KeySchema": [
                {"AttributeName": "GSI1PK", "KeyType": "HASH"},
                {"AttributeName": "GSI1SK", "KeyType": "RANGE"},
            ],
            "Projection": {"ProjectionType": "ALL"},
        }]
    }))
    .unwrap();
    db.create_table(req).unwrap();

    // Delete the GSI via UpdateTable
    let req: UpdateTableRequest = serde_json::from_value(json!({
        "TableName": "TestTable",
        "GlobalSecondaryIndexUpdates": [{
            "Delete": {"IndexName": "GSI1"}
        }]
    }))
    .unwrap();

    let resp = db.update_table(req).unwrap();
    assert!(resp.table_description.global_secondary_indexes.is_none());

    // Verify GSI query now fails
    let query_req = serde_json::from_value(json!({
        "TableName": "TestTable",
        "IndexName": "GSI1",
        "KeyConditionExpression": "GSI1PK = :pk",
        "ExpressionAttributeValues": {":pk": {"S": "test"}},
    }))
    .unwrap();
    let err = db.query(query_req).unwrap_err();
    assert!(
        err.to_string()
            .contains("does not have the specified index")
    );
}

#[test]
fn test_update_table_create_duplicate_gsi_fails() {
    let db = make_db();

    let req: CreateTableRequest = serde_json::from_value(json!({
        "TableName": "TestTable",
        "KeySchema": [{"AttributeName": "PK", "KeyType": "HASH"}],
        "AttributeDefinitions": [
            {"AttributeName": "PK", "AttributeType": "S"},
            {"AttributeName": "GSI1PK", "AttributeType": "S"},
        ],
        "GlobalSecondaryIndexes": [{
            "IndexName": "GSI1",
            "KeySchema": [{"AttributeName": "GSI1PK", "KeyType": "HASH"}],
            "Projection": {"ProjectionType": "ALL"},
        }]
    }))
    .unwrap();
    db.create_table(req).unwrap();

    // Try to create the same GSI again
    let req: UpdateTableRequest = serde_json::from_value(json!({
        "TableName": "TestTable",
        "AttributeDefinitions": [
            {"AttributeName": "PK", "AttributeType": "S"},
            {"AttributeName": "GSI1PK", "AttributeType": "S"},
        ],
        "GlobalSecondaryIndexUpdates": [{
            "Create": {
                "IndexName": "GSI1",
                "KeySchema": [{"AttributeName": "GSI1PK", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
            }
        }]
    }))
    .unwrap();

    let err = db.update_table(req).unwrap_err();
    assert!(err.to_string().contains("Index already exists"));
}

#[test]
fn test_update_table_delete_nonexistent_gsi_fails() {
    let db = make_db();
    create_simple_table(&db, "TestTable");

    let req: UpdateTableRequest = serde_json::from_value(json!({
        "TableName": "TestTable",
        "GlobalSecondaryIndexUpdates": [{
            "Delete": {"IndexName": "NonexistentGSI"}
        }]
    }))
    .unwrap();

    let err = db.update_table(req).unwrap_err();
    assert!(
        matches!(
            &err,
            dynoxide::errors::DynoxideError::ResourceNotFoundException(_)
        ),
        "Expected ResourceNotFoundException, got: {:?}",
        err
    );
    assert!(
        err.to_string().contains("Requested resource not found"),
        "Unexpected message: {}",
        err
    );
}

#[test]
fn test_update_table_nonexistent_table_fails() {
    let db = make_db();

    let req: UpdateTableRequest = serde_json::from_value(json!({
        "TableName": "DoesNotExist",
        "GlobalSecondaryIndexUpdates": [{
            "Create": {
                "IndexName": "GSI1",
                "KeySchema": [{"AttributeName": "GSI1PK", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
            }
        }]
    }))
    .unwrap();

    let err = db.update_table(req).unwrap_err();
    assert!(err.to_string().contains("not found"));
}

#[test]
fn test_update_table_create_multiple_gsis_sequentially() {
    let db = make_db();
    create_simple_table(&db, "TestTable");

    // Add GSI1
    let req: UpdateTableRequest = serde_json::from_value(json!({
        "TableName": "TestTable",
        "AttributeDefinitions": [
            {"AttributeName": "PK", "AttributeType": "S"},
            {"AttributeName": "SK", "AttributeType": "S"},
            {"AttributeName": "GSI1PK", "AttributeType": "S"},
        ],
        "GlobalSecondaryIndexUpdates": [{
            "Create": {
                "IndexName": "GSI1",
                "KeySchema": [{"AttributeName": "GSI1PK", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
            }
        }]
    }))
    .unwrap();
    db.update_table(req).unwrap();

    // Add GSI2 in a separate call
    let req: UpdateTableRequest = serde_json::from_value(json!({
        "TableName": "TestTable",
        "AttributeDefinitions": [
            {"AttributeName": "PK", "AttributeType": "S"},
            {"AttributeName": "SK", "AttributeType": "S"},
            {"AttributeName": "GSI1PK", "AttributeType": "S"},
            {"AttributeName": "GSI2PK", "AttributeType": "S"},
        ],
        "GlobalSecondaryIndexUpdates": [{
            "Create": {
                "IndexName": "GSI2",
                "KeySchema": [{"AttributeName": "GSI2PK", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
            }
        }]
    }))
    .unwrap();
    let resp = db.update_table(req).unwrap();

    let gsis = resp.table_description.global_secondary_indexes.unwrap();
    assert_eq!(gsis.len(), 2);
    let names: Vec<&str> = gsis.iter().map(|g| g.index_name.as_str()).collect();
    assert!(names.contains(&"GSI1"));
    assert!(names.contains(&"GSI2"));
}

#[test]
fn test_update_table_response_includes_table_description() {
    let db = make_db();
    create_simple_table(&db, "TestTable");

    let req: UpdateTableRequest = serde_json::from_value(json!({
        "TableName": "TestTable",
        "AttributeDefinitions": [
            {"AttributeName": "PK", "AttributeType": "S"},
            {"AttributeName": "SK", "AttributeType": "S"},
            {"AttributeName": "GSI1PK", "AttributeType": "S"},
        ],
        "GlobalSecondaryIndexUpdates": [{
            "Create": {
                "IndexName": "GSI1",
                "KeySchema": [{"AttributeName": "GSI1PK", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
            }
        }]
    }))
    .unwrap();

    let resp = db.update_table(req).unwrap();
    let desc = &resp.table_description;

    assert_eq!(desc.table_name, "TestTable");
    assert_eq!(desc.table_status, "ACTIVE");
    assert!(!desc.table_arn.is_empty());
    assert!(desc.key_schema.len() == 2);
    assert!(desc.creation_date_time.is_some());
}

#[test]
fn test_update_table_gsi_projection_types() {
    let db = make_db();
    create_simple_table(&db, "TestTable");

    // Add item before creating GSIs
    put_item(
        &db,
        "TestTable",
        "user#1",
        "profile",
        Some("org#A"),
        Some("user#1"),
    );
    // Also add a non-key attribute
    let req = serde_json::from_value(json!({
        "TableName": "TestTable",
        "Item": {
            "PK": {"S": "user#2"},
            "SK": {"S": "profile"},
            "GSI1PK": {"S": "org#A"},
            "GSI1SK": {"S": "user#2"},
            "email": {"S": "user2@example.com"},
            "name": {"S": "User Two"},
        }
    }))
    .unwrap();
    db.put_item(req).unwrap();

    // Create GSI with KEYS_ONLY projection
    let req: UpdateTableRequest = serde_json::from_value(json!({
        "TableName": "TestTable",
        "AttributeDefinitions": [
            {"AttributeName": "PK", "AttributeType": "S"},
            {"AttributeName": "SK", "AttributeType": "S"},
            {"AttributeName": "GSI1PK", "AttributeType": "S"},
            {"AttributeName": "GSI1SK", "AttributeType": "S"},
        ],
        "GlobalSecondaryIndexUpdates": [{
            "Create": {
                "IndexName": "GSI1",
                "KeySchema": [
                    {"AttributeName": "GSI1PK", "KeyType": "HASH"},
                    {"AttributeName": "GSI1SK", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "KEYS_ONLY"},
            }
        }]
    }))
    .unwrap();
    db.update_table(req).unwrap();

    // Query GSI — should only return key attributes
    let query_req = serde_json::from_value(json!({
        "TableName": "TestTable",
        "IndexName": "GSI1",
        "KeyConditionExpression": "GSI1PK = :pk",
        "ExpressionAttributeValues": {":pk": {"S": "org#A"}},
    }))
    .unwrap();
    let resp = db.query(query_req).unwrap();
    assert_eq!(resp.count, 2);

    // Verify the backfilled items only have key attributes
    let items = resp.items.unwrap();
    for item in &items {
        assert!(item.contains_key("PK"));
        assert!(item.contains_key("SK"));
        assert!(item.contains_key("GSI1PK"));
        assert!(item.contains_key("GSI1SK"));
        // Non-key attributes should NOT be present
        assert!(!item.contains_key("email"));
        assert!(!item.contains_key("name"));
    }
}

#[test]
fn test_update_table_cache_invalidation() {
    let db = make_db();
    create_simple_table(&db, "TestTable");

    // DescribeTable to populate any internal cache
    let desc_req = serde_json::from_value(serde_json::json!({"TableName": "TestTable"})).unwrap();
    let resp = db.describe_table(desc_req).unwrap();
    assert!(resp.table.global_secondary_indexes.is_none());

    // Add a GSI via UpdateTable
    let req: UpdateTableRequest = serde_json::from_value(serde_json::json!({
        "TableName": "TestTable",
        "AttributeDefinitions": [
            {"AttributeName": "PK", "AttributeType": "S"},
            {"AttributeName": "SK", "AttributeType": "S"},
            {"AttributeName": "GSI1PK", "AttributeType": "S"},
        ],
        "GlobalSecondaryIndexUpdates": [{"Create": {
            "IndexName": "GSI1",
            "KeySchema": [{"AttributeName": "GSI1PK", "KeyType": "HASH"}],
            "Projection": {"ProjectionType": "ALL"},
        }}]
    }))
    .unwrap();
    db.update_table(req).unwrap();

    // DescribeTable should reflect the new GSI (not stale cached data)
    let desc_req = serde_json::from_value(serde_json::json!({"TableName": "TestTable"})).unwrap();
    let resp = db.describe_table(desc_req).unwrap();
    let gsis = resp.table.global_secondary_indexes.unwrap();
    assert_eq!(gsis.len(), 1);
    assert_eq!(gsis[0].index_name, "GSI1");
}

fn create_pay_per_request_table(db: &Database, name: &str) {
    let req: CreateTableRequest = serde_json::from_value(json!({
        "TableName": name,
        "KeySchema": [{"AttributeName": "a", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "a", "AttributeType": "N"}],
        "BillingMode": "PAY_PER_REQUEST"
    }))
    .unwrap();
    db.create_table(req).unwrap();
}

fn create_provisioned_table(db: &Database, name: &str, rcu: i64, wcu: i64) {
    let req: CreateTableRequest = serde_json::from_value(json!({
        "TableName": name,
        "KeySchema": [{"AttributeName": "a", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "a", "AttributeType": "S"}],
        "ProvisionedThroughput": {
            "ReadCapacityUnits": rcu,
            "WriteCapacityUnits": wcu
        }
    }))
    .unwrap();
    db.create_table(req).unwrap();
}

#[test]
fn test_limit_exceeded_too_many_gsi_updates() {
    let db = make_db();
    create_provisioned_table(&db, "TestTable", 10, 5);

    let req: UpdateTableRequest = serde_json::from_value(json!({
        "TableName": "TestTable",
        "GlobalSecondaryIndexUpdates": [
            {"Delete": {"IndexName": "abc"}},
            {"Delete": {"IndexName": "abd"}},
            {"Delete": {"IndexName": "abe"}},
            {"Delete": {"IndexName": "abf"}},
            {"Delete": {"IndexName": "abg"}},
            {"Delete": {"IndexName": "abh"}}
        ]
    }))
    .unwrap();

    let err = db.update_table(req).unwrap_err();
    assert!(
        err.to_string().contains("Subscriber limit exceeded"),
        "Expected LimitExceededException, got: {}",
        err
    );
    assert_eq!(
        err.error_type(),
        "com.amazonaws.dynamodb.v20120810#LimitExceededException"
    );
}

#[test]
fn test_provisioned_without_provisioned_throughput() {
    let db = make_db();
    create_pay_per_request_table(&db, "TestTable");

    let req: UpdateTableRequest = serde_json::from_value(json!({
        "TableName": "TestTable",
        "BillingMode": "PROVISIONED"
    }))
    .unwrap();

    let err = db.update_table(req).unwrap_err();
    assert!(
        err.to_string()
            .contains("ProvisionedThroughput must be specified when BillingMode is PROVISIONED"),
        "Expected validation about missing PT, got: {}",
        err
    );
}

#[test]
fn test_provisioned_throughput_update_when_pay_per_request() {
    let db = make_db();
    create_pay_per_request_table(&db, "TestTable");

    let req: UpdateTableRequest = serde_json::from_value(json!({
        "TableName": "TestTable",
        "ProvisionedThroughput": {"ReadCapacityUnits": 1, "WriteCapacityUnits": 1}
    }))
    .unwrap();

    let err = db.update_table(req).unwrap_err();
    assert!(
        err.to_string().contains(
            "Neither ReadCapacityUnits nor WriteCapacityUnits can be specified when BillingMode is PAY_PER_REQUEST"
        ),
        "Expected PAY_PER_REQUEST validation, got: {}",
        err
    );
}

#[test]
fn test_high_index_capacity_when_index_does_not_exist() {
    let db = make_db();
    create_provisioned_table(&db, "TestTable", 10, 5);

    let req: UpdateTableRequest = serde_json::from_value(json!({
        "TableName": "TestTable",
        "GlobalSecondaryIndexUpdates": [{
            "Update": {
                "IndexName": "abc",
                "ProvisionedThroughput": {
                    "ReadCapacityUnits": 1000000000001_i64,
                    "WriteCapacityUnits": 1000000000001_i64
                }
            }
        }]
    }))
    .unwrap();

    let err = db.update_table(req).unwrap_err();
    assert!(
        err.to_string().contains("Action Blocked: IndexUpdate"),
        "Expected Action Blocked error, got: {}",
        err
    );
}

#[test]
fn test_same_read_write_validation() {
    let db = make_db();
    create_provisioned_table(&db, "TestTable", 10, 5);

    let req: UpdateTableRequest = serde_json::from_value(json!({
        "TableName": "TestTable",
        "ProvisionedThroughput": {"ReadCapacityUnits": 10, "WriteCapacityUnits": 5}
    }))
    .unwrap();

    let err = db.update_table(req).unwrap_err();
    assert!(
        err.to_string()
            .contains("The provisioned throughput for the table will not change"),
        "Expected same-values validation, got: {}",
        err
    );
}

#[test]
fn test_triple_rates_and_reduce() {
    let db = make_db();
    create_provisioned_table(&db, "TestTable", 10, 5);

    // Triple the rates
    let req: UpdateTableRequest = serde_json::from_value(json!({
        "TableName": "TestTable",
        "ProvisionedThroughput": {"ReadCapacityUnits": 30, "WriteCapacityUnits": 15}
    }))
    .unwrap();

    let resp = db.update_table(req).unwrap();
    let desc = &resp.table_description;
    assert_eq!(desc.table_status, "UPDATING");

    // Immediate response shows old values
    let pt = desc.provisioned_throughput.as_ref().unwrap();
    assert_eq!(pt.read_capacity_units, 10);
    assert_eq!(pt.write_capacity_units, 5);
    assert!(pt.last_increase_date_time.is_some());

    // DescribeTable should show the new values (we apply instantly)
    let desc_req = serde_json::from_value(serde_json::json!({"TableName": "TestTable"})).unwrap();
    let desc_resp = db.describe_table(desc_req).unwrap();
    let dt = &desc_resp.table;
    let pt2 = dt.provisioned_throughput.as_ref().unwrap();
    assert_eq!(pt2.read_capacity_units, 30);
    assert_eq!(pt2.write_capacity_units, 15);

    // Now reduce back
    let req: UpdateTableRequest = serde_json::from_value(json!({
        "TableName": "TestTable",
        "ProvisionedThroughput": {"ReadCapacityUnits": 10, "WriteCapacityUnits": 5}
    }))
    .unwrap();

    let resp = db.update_table(req).unwrap();
    let desc = &resp.table_description;
    assert_eq!(desc.table_status, "UPDATING");
    let pt3 = desc.provisioned_throughput.as_ref().unwrap();
    // Shows old values (30/15) while UPDATING
    assert_eq!(pt3.read_capacity_units, 30);
    assert_eq!(pt3.write_capacity_units, 15);
    assert!(pt3.last_decrease_date_time.is_some());

    // After "settling", DescribeTable shows new values
    let desc_req = serde_json::from_value(serde_json::json!({"TableName": "TestTable"})).unwrap();
    let desc_resp = db.describe_table(desc_req).unwrap();
    let pt4 = desc_resp.table.provisioned_throughput.as_ref().unwrap();
    assert_eq!(pt4.read_capacity_units, 10);
    assert_eq!(pt4.write_capacity_units, 5);
    assert_eq!(pt4.number_of_decreases_today, 1);
}

#[test]
fn test_switch_provisioned_to_pay_per_request() {
    let db = make_db();
    create_provisioned_table(&db, "TestTable", 5, 5);

    let req: UpdateTableRequest = serde_json::from_value(json!({
        "TableName": "TestTable",
        "BillingMode": "PAY_PER_REQUEST"
    }))
    .unwrap();

    db.update_table(req).unwrap();

    // DescribeTable should reflect PAY_PER_REQUEST
    let desc_req = serde_json::from_value(json!({"TableName": "TestTable"})).unwrap();
    let desc_resp = db.describe_table(desc_req).unwrap();
    let bms = desc_resp
        .table
        .billing_mode_summary
        .as_ref()
        .expect("BillingModeSummary should be present");
    assert_eq!(bms.billing_mode, "PAY_PER_REQUEST");
}

#[test]
fn test_switch_pay_per_request_to_provisioned() {
    let db = make_db();
    create_pay_per_request_table(&db, "TestTable");

    let req: UpdateTableRequest = serde_json::from_value(json!({
        "TableName": "TestTable",
        "BillingMode": "PROVISIONED",
        "ProvisionedThroughput": {
            "ReadCapacityUnits": 5,
            "WriteCapacityUnits": 5
        }
    }))
    .unwrap();

    db.update_table(req).unwrap();

    // DescribeTable should reflect PROVISIONED with throughput values
    let desc_req = serde_json::from_value(json!({"TableName": "TestTable"})).unwrap();
    let desc_resp = db.describe_table(desc_req).unwrap();
    // BillingModeSummary should be absent for PROVISIONED tables
    assert!(
        desc_resp.table.billing_mode_summary.is_none(),
        "BillingModeSummary should be None for PROVISIONED tables"
    );
    let pt = desc_resp
        .table
        .provisioned_throughput
        .as_ref()
        .expect("ProvisionedThroughput should be present");
    assert_eq!(pt.read_capacity_units, 5);
    assert_eq!(pt.write_capacity_units, 5);
}

#[test]
fn test_reject_pay_per_request_with_provisioned_throughput() {
    let db = make_db();
    create_provisioned_table(&db, "TestTable", 5, 5);

    let req: UpdateTableRequest = serde_json::from_value(json!({
        "TableName": "TestTable",
        "BillingMode": "PAY_PER_REQUEST",
        "ProvisionedThroughput": {
            "ReadCapacityUnits": 5,
            "WriteCapacityUnits": 5
        }
    }))
    .unwrap();

    let err = db.update_table(req).unwrap_err();
    assert!(
        err.to_string().contains(
            "Neither ReadCapacityUnits nor WriteCapacityUnits can be specified when BillingMode is PAY_PER_REQUEST"
        ),
        "Expected PAY_PER_REQUEST + PT validation, got: {}",
        err
    );
}

#[test]
fn test_reject_invalid_billing_mode() {
    let db = make_db();
    create_provisioned_table(&db, "TestTable", 5, 5);

    let req: UpdateTableRequest = serde_json::from_value(json!({
        "TableName": "TestTable",
        "BillingMode": "INVALID_MODE"
    }))
    .unwrap();

    let err = db.update_table(req).unwrap_err();
    assert!(
        err.to_string()
            .contains("failed to satisfy constraint: Member must satisfy enum value set"),
        "Expected enum validation, got: {}",
        err
    );
}

#[test]
fn test_provisioned_to_provisioned_same_throughput_rejected() {
    let db = make_db();
    create_provisioned_table(&db, "TestTable", 5, 5);

    // Explicitly setting BillingMode: PROVISIONED with same throughput should fail
    let req: UpdateTableRequest = serde_json::from_value(json!({
        "TableName": "TestTable",
        "BillingMode": "PROVISIONED",
        "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
    }))
    .unwrap();

    let err = db.update_table(req).unwrap_err();
    assert!(
        err.to_string()
            .contains("The provisioned throughput for the table will not change"),
        "Expected same-values validation, got: {}",
        err
    );
}

#[test]
fn test_provisioned_to_provisioned_different_throughput_accepted() {
    let db = make_db();
    create_provisioned_table(&db, "TestTable", 5, 5);

    // Explicitly setting BillingMode: PROVISIONED with different throughput should succeed
    let req: UpdateTableRequest = serde_json::from_value(json!({
        "TableName": "TestTable",
        "BillingMode": "PROVISIONED",
        "ProvisionedThroughput": {"ReadCapacityUnits": 10, "WriteCapacityUnits": 5}
    }))
    .unwrap();

    db.update_table(req).unwrap();
}
