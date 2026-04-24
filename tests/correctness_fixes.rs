//! Tests verifying the 10 CRITICAL correctness fixes from the DynamoDB compatibility audit.

use dynoxide::Database;
use dynoxide::actions::create_table::CreateTableRequest;
use dynoxide::actions::execute_statement::ExecuteStatementRequest;
use dynoxide::actions::execute_transaction::{ExecuteTransactionRequest, ParameterizedStatement};
use dynoxide::actions::put_item::PutItemRequest;
use dynoxide::actions::query::QueryRequest;
use dynoxide::actions::scan::ScanRequest;
use dynoxide::actions::update_item::UpdateItemRequest;
use dynoxide::types::{
    AttributeDefinition, AttributeValue, KeySchemaElement, KeyType, ScalarAttributeType,
};
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn create_pk_sk_table(db: &Database, name: &str) {
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

fn create_pk_only_table(db: &Database, name: &str) {
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

fn put(db: &Database, table: &str, item: HashMap<String, AttributeValue>) {
    db.put_item(PutItemRequest {
        table_name: table.to_string(),
        item,
        ..Default::default()
    })
    .unwrap();
}

fn item(pairs: &[(&str, AttributeValue)]) -> HashMap<String, AttributeValue> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

fn exec(
    db: &Database,
    stmt: &str,
) -> dynoxide::actions::execute_statement::ExecuteStatementResponse {
    db.execute_statement(ExecuteStatementRequest {
        statement: stmt.to_string(),
        parameters: None,
        ..Default::default()
    })
    .unwrap()
}

fn exec_with_params(
    db: &Database,
    stmt: &str,
    params: Vec<AttributeValue>,
) -> dynoxide::actions::execute_statement::ExecuteStatementResponse {
    db.execute_statement(ExecuteStatementRequest {
        statement: stmt.to_string(),
        parameters: Some(params),
        ..Default::default()
    })
    .unwrap()
}

fn exec_err(db: &Database, stmt: &str) -> String {
    db.execute_statement(ExecuteStatementRequest {
        statement: stmt.to_string(),
        parameters: None,
        ..Default::default()
    })
    .unwrap_err()
    .to_string()
}

// ===========================================================================
// C1-C3: BigDecimal number precision
// ===========================================================================

#[test]
fn test_c1_number_comparison_38_digit_precision() {
    // Two 38-digit numbers that differ only in the last digit.
    // f64 would lose precision and consider them equal.
    let db = Database::memory().unwrap();
    create_pk_only_table(&db, "Tbl");

    let big_a = "12345678901234567890123456789012345678";
    let big_b = "12345678901234567890123456789012345679";

    put(
        &db,
        "Tbl",
        item(&[
            ("pk", AttributeValue::S("a".into())),
            ("val", AttributeValue::N(big_a.to_string())),
        ]),
    );

    // Query with FilterExpression checking val < big_b  -- should match
    let resp = db
        .query({
            let mut req = QueryRequest::default();
            req.table_name = "Tbl".to_string();
            req.key_condition_expression = Some("pk = :pk".to_string());
            req.filter_expression = Some("val < :limit".to_string());
            req.expression_attribute_values = Some(HashMap::from([
                (":pk".to_string(), AttributeValue::S("a".into())),
                (":limit".to_string(), AttributeValue::N(big_b.to_string())),
            ]));
            req
        })
        .unwrap();
    assert_eq!(
        resp.count, 1,
        "38-digit number comparison should work correctly"
    );

    // Now check val < big_a -- should NOT match (equal, not less)
    let resp2 = db
        .query({
            let mut req = QueryRequest::default();
            req.table_name = "Tbl".to_string();
            req.key_condition_expression = Some("pk = :pk".to_string());
            req.filter_expression = Some("val < :limit".to_string());
            req.expression_attribute_values = Some(HashMap::from([
                (":pk".to_string(), AttributeValue::S("a".into())),
                (":limit".to_string(), AttributeValue::N(big_a.to_string())),
            ]));
            req
        })
        .unwrap();
    assert_eq!(
        resp2.count, 0,
        "Equal 38-digit numbers should not satisfy <"
    );
}

#[test]
fn test_c2_number_arithmetic_preserves_precision() {
    let db = Database::memory().unwrap();
    create_pk_only_table(&db, "Tbl");

    let big = "99999999999999999999999999999999999999";
    put(
        &db,
        "Tbl",
        item(&[
            ("pk", AttributeValue::S("a".into())),
            ("counter", AttributeValue::N(big.to_string())),
        ]),
    );

    // ADD 1 to a 38-digit number
    db.update_item(UpdateItemRequest {
        table_name: "Tbl".to_string(),
        key: HashMap::from([("pk".to_string(), AttributeValue::S("a".into()))]),
        update_expression: Some("ADD #c :inc".to_string()),
        expression_attribute_names: Some(HashMap::from([(
            "#c".to_string(),
            "counter".to_string(),
        )])),
        expression_attribute_values: Some(HashMap::from([(
            ":inc".to_string(),
            AttributeValue::N("1".to_string()),
        )])),
        ..Default::default()
    })
    .unwrap();

    // Verify: should be 100000000000000000000000000000000000000 (39 digits)
    let resp = db
        .query({
            let mut req = QueryRequest::default();
            req.table_name = "Tbl".to_string();
            req.key_condition_expression = Some("pk = :pk".to_string());
            req.expression_attribute_values = Some(HashMap::from([(
                ":pk".to_string(),
                AttributeValue::S("a".into()),
            )]));
            req
        })
        .unwrap();
    let items = resp.items.unwrap();
    let counter = items[0].get("counter").unwrap();
    match counter {
        AttributeValue::N(n) => {
            assert_eq!(n, "100000000000000000000000000000000000000");
        }
        _ => panic!("Expected N"),
    }
}

#[test]
fn test_c2_set_plus_minus_high_precision() {
    let db = Database::memory().unwrap();
    create_pk_only_table(&db, "Tbl");

    put(
        &db,
        "Tbl",
        item(&[
            ("pk", AttributeValue::S("a".into())),
            (
                "val",
                AttributeValue::N("1.00000000000000000001".to_string()),
            ),
        ]),
    );

    // SET val = val + :inc where inc is also high precision
    db.update_item(UpdateItemRequest {
        table_name: "Tbl".to_string(),
        key: HashMap::from([("pk".to_string(), AttributeValue::S("a".into()))]),
        update_expression: Some("SET val = val + :inc".to_string()),
        expression_attribute_values: Some(HashMap::from([(
            ":inc".to_string(),
            AttributeValue::N("2.00000000000000000002".to_string()),
        )])),
        ..Default::default()
    })
    .unwrap();

    let resp = db
        .query({
            let mut req = QueryRequest::default();
            req.table_name = "Tbl".to_string();
            req.key_condition_expression = Some("pk = :pk".to_string());
            req.expression_attribute_values = Some(HashMap::from([(
                ":pk".to_string(),
                AttributeValue::S("a".into()),
            )]));
            req
        })
        .unwrap();
    let items = resp.items.unwrap();
    let val = items[0].get("val").unwrap();
    match val {
        AttributeValue::N(n) => {
            assert_eq!(n, "3.00000000000000000003");
        }
        _ => panic!("Expected N"),
    }
}

// ===========================================================================
// C4: begins_with LIKE escaping
// ===========================================================================

#[test]
fn test_c4_begins_with_percent_in_sort_key() {
    let db = Database::memory().unwrap();
    create_pk_sk_table(&db, "Tbl");

    put(
        &db,
        "Tbl",
        item(&[
            ("pk", AttributeValue::S("p".into())),
            ("sk", AttributeValue::S("100%_off".into())),
        ]),
    );
    put(
        &db,
        "Tbl",
        item(&[
            ("pk", AttributeValue::S("p".into())),
            ("sk", AttributeValue::S("100abc".into())),
        ]),
    );

    // begins_with(sk, "100%") should match only the first item
    let resp = db
        .query({
            let mut req = QueryRequest::default();
            req.table_name = "Tbl".to_string();
            req.key_condition_expression =
                Some("pk = :pk AND begins_with(sk, :prefix)".to_string());
            req.expression_attribute_values = Some(HashMap::from([
                (":pk".to_string(), AttributeValue::S("p".into())),
                (":prefix".to_string(), AttributeValue::S("100%".into())),
            ]));
            req
        })
        .unwrap();
    assert_eq!(
        resp.count, 1,
        "begins_with with % should not match unrelated items"
    );
    let items = resp.items.unwrap();
    match items[0].get("sk").unwrap() {
        AttributeValue::S(s) => assert_eq!(s, "100%_off"),
        _ => panic!("Expected S"),
    }
}

#[test]
fn test_c4_begins_with_underscore_in_sort_key() {
    let db = Database::memory().unwrap();
    create_pk_sk_table(&db, "Tbl");

    put(
        &db,
        "Tbl",
        item(&[
            ("pk", AttributeValue::S("p".into())),
            ("sk", AttributeValue::S("a_b".into())),
        ]),
    );
    put(
        &db,
        "Tbl",
        item(&[
            ("pk", AttributeValue::S("p".into())),
            ("sk", AttributeValue::S("aXb".into())),
        ]),
    );

    // begins_with(sk, "a_") should match only "a_b", not "aXb"
    let resp = db
        .query({
            let mut req = QueryRequest::default();
            req.table_name = "Tbl".to_string();
            req.key_condition_expression =
                Some("pk = :pk AND begins_with(sk, :prefix)".to_string());
            req.expression_attribute_values = Some(HashMap::from([
                (":pk".to_string(), AttributeValue::S("p".into())),
                (":prefix".to_string(), AttributeValue::S("a_".into())),
            ]));
            req
        })
        .unwrap();
    assert_eq!(
        resp.count, 1,
        "begins_with with _ should not act as single-char wildcard"
    );
}

// ===========================================================================
// C5: COUNT returns filtered count (not scanned count)
// ===========================================================================

#[test]
fn test_c5_query_count_with_filter_expression() {
    let db = Database::memory().unwrap();
    create_pk_sk_table(&db, "Tbl");

    // Insert 5 items, only 2 pass the filter
    for i in 0..5 {
        put(
            &db,
            "Tbl",
            item(&[
                ("pk", AttributeValue::S("p".into())),
                ("sk", AttributeValue::S(format!("sk{i}"))),
                ("score", AttributeValue::N(format!("{}", i * 10))),
            ]),
        );
    }

    // SELECT COUNT with filter: score >= 30 (items 3,4 = 2 items)
    let resp = db
        .query({
            let mut req = QueryRequest::default();
            req.table_name = "Tbl".to_string();
            req.key_condition_expression = Some("pk = :pk".to_string());
            req.filter_expression = Some("score >= :min".to_string());
            req.expression_attribute_values = Some(HashMap::from([
                (":pk".to_string(), AttributeValue::S("p".into())),
                (":min".to_string(), AttributeValue::N("30".to_string())),
            ]));
            req.select = Some("COUNT".to_string());
            req
        })
        .unwrap();

    assert_eq!(resp.scanned_count, 5, "ScannedCount should be 5");
    assert_eq!(
        resp.count, 2,
        "Count should be 2 (filtered), not 5 (scanned)"
    );
}

#[test]
fn test_c5_scan_count_with_filter_expression() {
    let db = Database::memory().unwrap();
    create_pk_only_table(&db, "Tbl");

    for i in 0..5 {
        put(
            &db,
            "Tbl",
            item(&[
                ("pk", AttributeValue::S(format!("pk{i}"))),
                ("score", AttributeValue::N(format!("{}", i * 10))),
            ]),
        );
    }

    // Scan with COUNT + filter: score >= 30 (items 3,4 = 2 items)
    let resp = db
        .scan({
            let mut req = ScanRequest::default();
            req.table_name = "Tbl".to_string();
            req.filter_expression = Some("score >= :min".to_string());
            req.expression_attribute_values = Some(HashMap::from([(
                ":min".to_string(),
                AttributeValue::N("30".to_string()),
            )]));
            req.select = Some("COUNT".to_string());
            req
        })
        .unwrap();

    assert_eq!(resp.scanned_count, 5, "ScannedCount should be 5");
    assert_eq!(
        resp.count, 2,
        "Count should be 2 (filtered), not 5 (scanned)"
    );
}

// ===========================================================================
// C6: PartiQL INSERT duplicate rejection
// ===========================================================================

#[test]
fn test_c6_partiql_insert_duplicate_fails() {
    let db = Database::memory().unwrap();
    create_pk_only_table(&db, "Tbl");

    exec(
        &db,
        "INSERT INTO \"Tbl\" VALUE {'pk': 'k1', 'data': 'first'}",
    );

    // Second insert with same key should fail
    let err = exec_err(
        &db,
        "INSERT INTO \"Tbl\" VALUE {'pk': 'k1', 'data': 'second'}",
    );
    assert!(
        err.contains("Duplicate primary key"),
        "Expected duplicate key error, got: {err}"
    );

    // Verify original item is unchanged
    let resp = exec(&db, "SELECT * FROM \"Tbl\" WHERE pk = 'k1'");
    let items = resp.items.unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(
        items[0].get("data"),
        Some(&AttributeValue::S("first".to_string()))
    );
}

// ===========================================================================
// C7: PartiQL INSERT VALUE with ? parameter placeholders
// ===========================================================================

#[test]
fn test_c7_partiql_insert_with_parameters() {
    let db = Database::memory().unwrap();
    create_pk_only_table(&db, "Tbl");

    exec_with_params(
        &db,
        "INSERT INTO \"Tbl\" VALUE {'pk': ?, 'data': ?}",
        vec![
            AttributeValue::S("param_key".to_string()),
            AttributeValue::S("param_data".to_string()),
        ],
    );

    let resp = exec(&db, "SELECT * FROM \"Tbl\" WHERE pk = 'param_key'");
    let items = resp.items.unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(
        items[0].get("data"),
        Some(&AttributeValue::S("param_data".to_string()))
    );
}

#[test]
fn test_c7_partiql_insert_with_mixed_literal_and_param() {
    let db = Database::memory().unwrap();
    create_pk_only_table(&db, "Tbl");

    exec_with_params(
        &db,
        "INSERT INTO \"Tbl\" VALUE {'pk': 'literal_key', 'score': ?}",
        vec![AttributeValue::N("42".to_string())],
    );

    let resp = exec(&db, "SELECT * FROM \"Tbl\" WHERE pk = 'literal_key'");
    let items = resp.items.unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(
        items[0].get("score"),
        Some(&AttributeValue::N("42".to_string()))
    );
}

// ===========================================================================
// C8: Negative number tokenisation in PartiQL
// ===========================================================================

#[test]
fn test_c8_partiql_negative_number_in_where() {
    let db = Database::memory().unwrap();
    create_pk_only_table(&db, "Tbl");

    put(
        &db,
        "Tbl",
        item(&[
            ("pk", AttributeValue::S("a".into())),
            ("score", AttributeValue::N("-5".to_string())),
        ]),
    );
    put(
        &db,
        "Tbl",
        item(&[
            ("pk", AttributeValue::S("b".into())),
            ("score", AttributeValue::N("-15".to_string())),
        ]),
    );

    // WHERE score > -10 should match "a" (score=-5) but not "b" (score=-15)
    let resp = exec(&db, "SELECT * FROM \"Tbl\" WHERE score > -10");
    let items = resp.items.unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(
        items[0].get("pk"),
        Some(&AttributeValue::S("a".to_string()))
    );
}

#[test]
fn test_c8_partiql_negative_number_in_insert() {
    let db = Database::memory().unwrap();
    create_pk_only_table(&db, "Tbl");

    exec(&db, "INSERT INTO \"Tbl\" VALUE {'pk': 'neg', 'temp': -42}");

    let resp = exec(&db, "SELECT * FROM \"Tbl\" WHERE pk = 'neg'");
    let items = resp.items.unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(
        items[0].get("temp"),
        Some(&AttributeValue::N("-42".to_string()))
    );
}

// ===========================================================================
// C9: Escaped single quotes in PartiQL strings
// ===========================================================================

#[test]
fn test_c9_partiql_escaped_single_quote_in_where() {
    let db = Database::memory().unwrap();
    create_pk_only_table(&db, "Tbl");

    put(
        &db,
        "Tbl",
        item(&[
            ("pk", AttributeValue::S("k1".into())),
            ("name", AttributeValue::S("it's".to_string())),
        ]),
    );
    put(
        &db,
        "Tbl",
        item(&[
            ("pk", AttributeValue::S("k2".into())),
            ("name", AttributeValue::S("its".to_string())),
        ]),
    );

    // WHERE name = 'it''s' should match only k1
    let resp = exec(&db, "SELECT * FROM \"Tbl\" WHERE name = 'it''s'");
    let items = resp.items.unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(
        items[0].get("pk"),
        Some(&AttributeValue::S("k1".to_string()))
    );
}

#[test]
fn test_c9_partiql_escaped_quote_in_insert() {
    let db = Database::memory().unwrap();
    create_pk_only_table(&db, "Tbl");

    exec(
        &db,
        "INSERT INTO \"Tbl\" VALUE {'pk': 'q1', 'msg': 'he said ''hello'''}",
    );

    let resp = exec(&db, "SELECT * FROM \"Tbl\" WHERE pk = 'q1'");
    let items = resp.items.unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(
        items[0].get("msg"),
        Some(&AttributeValue::S("he said 'hello'".to_string()))
    );
}

// ===========================================================================
// C10: ExecuteTransaction INSERT duplicate fails and rolls back
// ===========================================================================

#[test]
fn test_c10_execute_transaction_insert_duplicate_rolls_back() {
    let db = Database::memory().unwrap();
    create_pk_only_table(&db, "Tbl");

    // Insert an initial item
    exec(&db, "INSERT INTO \"Tbl\" VALUE {'pk': 'existing'}");

    // Transaction: insert two items, second is a duplicate of existing
    let result = db.execute_transaction(ExecuteTransactionRequest {
        transact_statements: vec![
            ParameterizedStatement {
                statement: "INSERT INTO \"Tbl\" VALUE {'pk': 'new_item'}".to_string(),
                parameters: None,
            },
            ParameterizedStatement {
                statement: "INSERT INTO \"Tbl\" VALUE {'pk': 'existing'}".to_string(),
                parameters: None,
            },
        ],
        ..Default::default()
    });

    assert!(
        result.is_err(),
        "Transaction with duplicate INSERT should fail"
    );
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("Transaction cancelled"),
        "Expected transaction cancellation, got: {err}"
    );

    // Verify rollback: 'new_item' should NOT exist
    let resp = exec(&db, "SELECT * FROM \"Tbl\" WHERE pk = 'new_item'");
    let items = resp.items.unwrap();
    assert_eq!(
        items.len(),
        0,
        "Transaction should have rolled back — 'new_item' should not exist"
    );
}

// ===========================================================================
// C3: PartiQL executor number comparison precision
// ===========================================================================

#[test]
fn test_c3_partiql_number_comparison_precision() {
    let db = Database::memory().unwrap();
    create_pk_only_table(&db, "Tbl");

    let big = "12345678901234567890123456789012345678";
    put(
        &db,
        "Tbl",
        item(&[
            ("pk", AttributeValue::S("a".into())),
            ("val", AttributeValue::N(big.to_string())),
        ]),
    );

    // PartiQL comparison with a number differing in last digit
    let resp = exec(
        &db,
        "SELECT * FROM \"Tbl\" WHERE val = 12345678901234567890123456789012345678",
    );
    let items = resp.items.unwrap();
    assert_eq!(items.len(), 1, "PartiQL exact comparison should match");

    let resp2 = exec(
        &db,
        "SELECT * FROM \"Tbl\" WHERE val = 12345678901234567890123456789012345679",
    );
    let items2 = resp2.items.unwrap();
    assert_eq!(
        items2.len(),
        0,
        "PartiQL comparison with different last digit should not match"
    );
}

/// PutItem with `<>` condition on a missing attribute should succeed.
/// A missing attribute is not equal to any value, so `status <> "working"`
/// is true when the item has no `status` attribute.
#[test]
fn test_ne_on_missing_attribute_returns_true() {
    let db = Database::memory().unwrap();
    let req: serde_json::Value = serde_json::json!({
        "TableName": "ne-missing",
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}]
    });
    db.create_table(serde_json::from_value(req).unwrap())
        .unwrap();

    // PutItem with condition: status <> "working" on a non-existent item.
    let req: serde_json::Value = serde_json::json!({
        "TableName": "ne-missing",
        "Item": {"pk": {"S": "item1"}, "status": {"S": "idle"}},
        "ConditionExpression": "#s <> :v",
        "ExpressionAttributeNames": {"#s": "status"},
        "ExpressionAttributeValues": {":v": {"S": "working"}}
    });
    db.put_item(serde_json::from_value(req).unwrap()).unwrap();

    let req: serde_json::Value = serde_json::json!({
        "TableName": "ne-missing",
        "Key": {"pk": {"S": "item1"}}
    });
    let resp = db.get_item(serde_json::from_value(req).unwrap()).unwrap();
    assert!(resp.item.is_some(), "item should have been created");

    // OR with <> and < on missing attributes — OR should short-circuit
    let req: serde_json::Value = serde_json::json!({
        "TableName": "ne-missing",
        "Item": {"pk": {"S": "item2"}, "status": {"S": "new"}},
        "ConditionExpression": "#s <> :v OR #u < :t",
        "ExpressionAttributeNames": {"#s": "status", "#u": "updatedAt"},
        "ExpressionAttributeValues": {
            ":v": {"S": "working"},
            ":t": {"S": "2099-01-01T00:00:00Z"}
        }
    });
    db.put_item(serde_json::from_value(req).unwrap()).unwrap();

    let req: serde_json::Value = serde_json::json!({
        "TableName": "ne-missing",
        "Key": {"pk": {"S": "item2"}}
    });
    let resp = db.get_item(serde_json::from_value(req).unwrap()).unwrap();
    assert!(
        resp.item.is_some(),
        "item2 should have been created via OR short-circuit"
    );
}
