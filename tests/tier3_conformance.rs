//! Tests for Tier 3 DynamoDB conformance: validation ordering, error messages,
//! and reserved word detection.

use dynoxide::Database;
use dynoxide::actions::create_table::CreateTableRequest;
use dynoxide::actions::query::QueryRequest;
use dynoxide::actions::scan::ScanRequest;
use dynoxide::errors::DynoxideError;
use dynoxide::types::AttributeValue;
use std::collections::HashMap;

fn make_db() -> Database {
    Database::memory().unwrap()
}

fn create_table_with_gsi(db: &Database, name: &str) {
    let req: CreateTableRequest = serde_json::from_value(serde_json::json!({
        "TableName": name,
        "KeySchema": [
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"}
        ],
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "gsi_pk", "AttributeType": "S"}
        ],
        "BillingMode": "PAY_PER_REQUEST",
        "GlobalSecondaryIndexes": [{
            "IndexName": "gsi-index",
            "KeySchema": [
                {"AttributeName": "gsi_pk", "KeyType": "HASH"}
            ],
            "Projection": {"ProjectionType": "ALL"}
        }]
    }))
    .unwrap();
    db.create_table(req).unwrap();
}

fn create_table_with_lsi(db: &Database, name: &str) {
    let req: CreateTableRequest = serde_json::from_value(serde_json::json!({
        "TableName": name,
        "KeySchema": [
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"}
        ],
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "lsi_sk", "AttributeType": "S"}
        ],
        "BillingMode": "PAY_PER_REQUEST",
        "LocalSecondaryIndexes": [{
            "IndexName": "lsi-index",
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "lsi_sk", "KeyType": "RANGE"}
            ],
            "Projection": {"ProjectionType": "ALL"}
        }]
    }))
    .unwrap();
    db.create_table(req).unwrap();
}

// ---- Group A: CreateTable validation ordering ----

#[test]
fn create_table_empty_table_name_returns_constraint_format() {
    let result = serde_json::from_value::<CreateTableRequest>(serde_json::json!({
        "TableName": "",
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}]
    }));
    let err = result.unwrap_err().to_string();
    // Should contain multi-field constraint format, not the old flat message
    assert!(
        err.contains("2 validation errors detected"),
        "Expected multi-field constraint format, got: {err}"
    );
    assert!(
        err.contains("Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+"),
        "Expected pattern constraint, got: {err}"
    );
    assert!(
        err.contains("Member must have length greater than or equal to 3"),
        "Expected length constraint, got: {err}"
    );
}

#[test]
fn create_table_invalid_pattern_returns_constraint_format() {
    let result = serde_json::from_value::<CreateTableRequest>(serde_json::json!({
        "TableName": "a!b",
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}]
    }));
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("1 validation error detected"),
        "Expected single constraint error, got: {err}"
    );
    assert!(
        err.contains("Value 'a!b' at 'tableName' failed to satisfy constraint"),
        "Expected tableName constraint, got: {err}"
    );
}

#[test]
fn create_table_short_name_returns_constraint_format() {
    // Table name "ab" is length 2 — should get constraint format, not old flat message
    let result = serde_json::from_value::<CreateTableRequest>(serde_json::json!({
        "TableName": "ab",
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}]
    }));
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("1 validation error detected"),
        "Expected constraint format for short name, got: {err}"
    );
    assert!(
        err.contains("Member must have length greater than or equal to 3"),
        "Expected length constraint, got: {err}"
    );
}

#[test]
fn create_table_missing_name_still_returns_required_error() {
    let result = serde_json::from_value::<CreateTableRequest>(serde_json::json!({
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}]
    }));
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("The parameter 'TableName' is required but was not present in the request"),
        "Expected 'required' error for missing TableName, got: {err}"
    );
}

// ---- Group C: Query error messages ----

#[test]
fn query_rejects_invalid_select_value() {
    let result = serde_json::from_value::<QueryRequest>(serde_json::json!({
        "TableName": "TestTable",
        "KeyConditionExpression": "pk = :pk",
        "ExpressionAttributeValues": {":pk": {"S": "a"}},
        "Select": "INVALID_VALUE"
    }));
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("at 'select' failed to satisfy constraint"),
        "Expected select enum validation error, got: {err}"
    );
    assert!(
        err.contains("SPECIFIC_ATTRIBUTES, COUNT, ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES"),
        "Expected enum set in AWS API-model order, got: {err}"
    );
}

#[test]
fn scan_rejects_invalid_select_value() {
    let result = serde_json::from_value::<ScanRequest>(serde_json::json!({
        "TableName": "TestTable",
        "Select": "BAD"
    }));
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("at 'select' failed to satisfy constraint"),
        "Expected select enum validation error, got: {err}"
    );
}

#[test]
fn query_rejects_limit_zero() {
    let result = serde_json::from_value::<QueryRequest>(serde_json::json!({
        "TableName": "TestTable",
        "KeyConditionExpression": "pk = :pk",
        "ExpressionAttributeValues": {":pk": {"S": "a"}},
        "Limit": 0
    }));
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("at 'Limit' failed to satisfy constraint"),
        "Expected limit validation error, got: {err}"
    );
    assert!(
        err.contains("greater than or equal to 1"),
        "Expected limit >= 1, got: {err}"
    );
}

#[test]
fn scan_rejects_limit_zero() {
    let result = serde_json::from_value::<ScanRequest>(serde_json::json!({
        "TableName": "TestTable",
        "Limit": 0
    }));
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("at 'Limit' failed to satisfy constraint"),
        "Expected limit validation error, got: {err}"
    );
}

#[test]
fn query_rejects_consistent_read_on_gsi() {
    let db = make_db();
    create_table_with_gsi(&db, "GsiTable");

    let request = QueryRequest {
        table_name: "GsiTable".to_string(),
        index_name: Some("gsi-index".to_string()),
        key_condition_expression: Some("gsi_pk = :pk".to_string()),
        expression_attribute_values: Some(HashMap::from([(
            ":pk".to_string(),
            AttributeValue::S("test".to_string()),
        )])),
        consistent_read: Some(true),
        ..Default::default()
    };

    let result = db.query(request);
    let err = result.unwrap_err();
    match err {
        DynoxideError::ValidationException(msg) => {
            assert!(
                msg.contains("Consistent reads are not supported on global secondary indexes"),
                "Expected GSI consistent read error, got: {msg}"
            );
        }
        other => panic!("Expected ValidationException, got: {other:?}"),
    }
}

#[test]
fn query_allows_consistent_read_on_lsi() {
    let db = make_db();
    create_table_with_lsi(&db, "LsiTable");

    let request = QueryRequest {
        table_name: "LsiTable".to_string(),
        index_name: Some("lsi-index".to_string()),
        key_condition_expression: Some("pk = :pk".to_string()),
        expression_attribute_values: Some(HashMap::from([(
            ":pk".to_string(),
            AttributeValue::S("test".to_string()),
        )])),
        consistent_read: Some(true),
        ..Default::default()
    };

    // Should succeed — ConsistentRead is allowed on LSIs
    let result = db.query(request);
    assert!(
        result.is_ok(),
        "ConsistentRead on LSI should be allowed: {result:?}"
    );
}

#[test]
fn query_rejects_undefined_expression_attribute_name_in_filter() {
    let db = make_db();
    create_table_with_gsi(&db, "NameRefTable");

    let request = QueryRequest {
        table_name: "NameRefTable".to_string(),
        key_condition_expression: Some("pk = :pk".to_string()),
        filter_expression: Some("#nonexistent = :val".to_string()),
        expression_attribute_values: Some(HashMap::from([
            (":pk".to_string(), AttributeValue::S("test".to_string())),
            (":val".to_string(), AttributeValue::S("x".to_string())),
        ])),
        // No ExpressionAttributeNames provided — #nonexistent is undefined
        ..Default::default()
    };

    let result = db.query(request);
    let err = result.unwrap_err();
    match err {
        DynoxideError::ValidationException(msg) => {
            assert!(
                msg.contains("Invalid FilterExpression"),
                "Expected 'Invalid FilterExpression' prefix, got: {msg}"
            );
        }
        other => panic!("Expected ValidationException, got: {other:?}"),
    }
}

#[test]
fn scan_rejects_consistent_read_on_gsi() {
    let db = make_db();
    create_table_with_gsi(&db, "GsiScanTable");

    let request = ScanRequest {
        table_name: "GsiScanTable".to_string(),
        index_name: Some("gsi-index".to_string()),
        consistent_read: Some(true),
        ..Default::default()
    };

    let result = db.scan(request);
    let err = result.unwrap_err();
    match err {
        DynoxideError::ValidationException(msg) => {
            assert!(
                msg.contains("Consistent reads are not supported on global secondary indexes"),
                "Expected GSI consistent read error, got: {msg}"
            );
        }
        other => panic!("Expected ValidationException, got: {other:?}"),
    }
}

// ---- Group B: Reserved word validation in expressions ----

#[test]
fn reserved_word_in_condition_expression_rejected() {
    let db = make_db();
    create_table_with_gsi(&db, "ReservedCondTbl");

    let request = dynoxide::actions::put_item::PutItemRequest {
        table_name: "ReservedCondTbl".to_string(),
        item: HashMap::from([
            ("pk".to_string(), AttributeValue::S("a".to_string())),
            ("sk".to_string(), AttributeValue::S("b".to_string())),
            (
                "status".to_string(),
                AttributeValue::S("active".to_string()),
            ),
        ]),
        condition_expression: Some("status = :val".to_string()),
        expression_attribute_values: Some(HashMap::from([(
            ":val".to_string(),
            AttributeValue::S("active".to_string()),
        )])),
        ..Default::default()
    };

    let err = db.put_item(request).unwrap_err();
    match err {
        DynoxideError::ValidationException(msg) => {
            assert!(msg.contains("reserved keyword"), "got: {msg}");
            assert!(msg.contains("status"), "got: {msg}");
        }
        other => panic!("Expected ValidationException, got: {other:?}"),
    }
}

#[test]
fn reserved_word_in_update_expression_rejected() {
    let db = make_db();
    create_table_with_gsi(&db, "ReservedUpdTbl");

    let request = dynoxide::actions::update_item::UpdateItemRequest {
        table_name: "ReservedUpdTbl".to_string(),
        key: HashMap::from([
            ("pk".to_string(), AttributeValue::S("a".to_string())),
            ("sk".to_string(), AttributeValue::S("b".to_string())),
        ]),
        update_expression: Some("SET name = :val".to_string()),
        expression_attribute_values: Some(HashMap::from([(
            ":val".to_string(),
            AttributeValue::S("Alice".to_string()),
        )])),
        ..Default::default()
    };

    let err = db.update_item(request).unwrap_err();
    match err {
        DynoxideError::ValidationException(msg) => {
            assert!(msg.contains("reserved keyword"), "got: {msg}");
            assert!(msg.contains("name"), "got: {msg}");
        }
        other => panic!("Expected ValidationException, got: {other:?}"),
    }
}

#[test]
fn reserved_word_in_filter_expression_rejected() {
    let db = make_db();
    create_table_with_gsi(&db, "ReservedFiltTbl");

    let request = QueryRequest {
        table_name: "ReservedFiltTbl".to_string(),
        key_condition_expression: Some("pk = :pk".to_string()),
        filter_expression: Some("status = :val".to_string()),
        expression_attribute_values: Some(HashMap::from([
            (":pk".to_string(), AttributeValue::S("a".to_string())),
            (":val".to_string(), AttributeValue::S("active".to_string())),
        ])),
        ..Default::default()
    };

    let err = db.query(request).unwrap_err();
    match err {
        DynoxideError::ValidationException(msg) => {
            assert!(msg.contains("reserved keyword"), "got: {msg}");
        }
        other => panic!("Expected ValidationException, got: {other:?}"),
    }
}

#[test]
fn reserved_word_in_projection_expression_rejected() {
    let db = make_db();
    create_table_with_gsi(&db, "ReservedProjTbl");

    let request = dynoxide::actions::get_item::GetItemRequest {
        table_name: "ReservedProjTbl".to_string(),
        key: HashMap::from([
            ("pk".to_string(), AttributeValue::S("a".to_string())),
            ("sk".to_string(), AttributeValue::S("b".to_string())),
        ]),
        projection_expression: Some("status".to_string()),
        ..Default::default()
    };

    let err = db.get_item(request).unwrap_err();
    match err {
        DynoxideError::ValidationException(msg) => {
            assert!(msg.contains("reserved keyword"), "got: {msg}");
        }
        other => panic!("Expected ValidationException, got: {other:?}"),
    }
}

#[test]
fn aliased_reserved_word_is_allowed() {
    let db = make_db();
    create_table_with_gsi(&db, "AliasedTbl");

    let request = dynoxide::actions::put_item::PutItemRequest {
        table_name: "AliasedTbl".to_string(),
        item: HashMap::from([
            ("pk".to_string(), AttributeValue::S("a".to_string())),
            ("sk".to_string(), AttributeValue::S("b".to_string())),
            (
                "status".to_string(),
                AttributeValue::S("active".to_string()),
            ),
        ]),
        condition_expression: Some("attribute_not_exists(#s)".to_string()),
        expression_attribute_names: Some(HashMap::from([("#s".to_string(), "status".to_string())])),
        ..Default::default()
    };

    db.put_item(request).unwrap();
}

#[test]
fn reserved_word_case_insensitive() {
    let db = make_db();
    create_table_with_gsi(&db, "CaseTbl");

    let request = dynoxide::actions::put_item::PutItemRequest {
        table_name: "CaseTbl".to_string(),
        item: HashMap::from([
            ("pk".to_string(), AttributeValue::S("a".to_string())),
            ("sk".to_string(), AttributeValue::S("b".to_string())),
        ]),
        condition_expression: Some("Status = :val".to_string()),
        expression_attribute_values: Some(HashMap::from([(
            ":val".to_string(),
            AttributeValue::S("active".to_string()),
        )])),
        ..Default::default()
    };

    assert!(
        db.put_item(request).is_err(),
        "Mixed-case reserved word should be rejected"
    );
}
