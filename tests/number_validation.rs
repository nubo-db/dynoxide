//! Tests for Fix #3: Number Precision Validation
//!
//! DynamoDB numbers: up to 38 significant digits, magnitude ±9.99...E+125,
//! positive values at least 1E-130.

use dynoxide::Database;
use dynoxide::actions::create_table::CreateTableRequest;
use dynoxide::actions::put_item::PutItemRequest;
use dynoxide::types::*;
use std::collections::HashMap;

fn make_db() -> Database {
    let db = Database::memory().unwrap();
    db.create_table(CreateTableRequest {
        table_name: "Tbl".to_string(),
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

fn put_number(db: &Database, num: &str) -> Result<(), String> {
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("k1".to_string()));
    item.insert("val".to_string(), AttributeValue::N(num.to_string()));
    db.put_item(PutItemRequest {
        table_name: "Tbl".to_string(),
        item,
        ..Default::default()
    })
    .map(|_| ())
    .map_err(|e| format!("{e}"))
}

#[test]
fn test_38_digit_number_accepted() {
    let db = make_db();
    // 38 significant digits
    let num = "12345678901234567890123456789012345678";
    put_number(&db, num).unwrap();
}

#[test]
fn test_39_digit_number_rejected() {
    let db = make_db();
    let num = "123456789012345678901234567890123456789";
    let err = put_number(&db, num).unwrap_err();
    assert!(
        err.contains("38 significant digits"),
        "unexpected error: {err}"
    );
}

#[test]
fn test_number_overflow_rejected() {
    let db = make_db();
    let err = put_number(&db, "1E126").unwrap_err();
    assert!(
        err.contains("magnitude larger than supported range"),
        "unexpected error: {err}"
    );
}

#[test]
fn test_max_magnitude_accepted() {
    let db = make_db();
    // 9.9999999999999999999999999999999999999E+125 is valid
    put_number(&db, "9.9999999999999999999999999999999999999E125").unwrap();
}

#[test]
fn test_underflow_rejected() {
    let db = make_db();
    let err = put_number(&db, "1E-131").unwrap_err();
    assert!(
        err.contains("magnitude smaller than supported range"),
        "unexpected error: {err}"
    );
}

#[test]
fn test_min_positive_accepted() {
    let db = make_db();
    put_number(&db, "1E-130").unwrap();
}

#[test]
fn test_zero_accepted() {
    let db = make_db();
    put_number(&db, "0").unwrap();
}

#[test]
fn test_negative_within_range_accepted() {
    let db = make_db();
    put_number(&db, "-42.5").unwrap();
}

#[test]
fn test_scientific_notation_within_range() {
    let db = make_db();
    put_number(&db, "1.23E10").unwrap();
    put_number(&db, "-5.5E-50").unwrap();
}

#[test]
fn test_leading_zeros_not_significant() {
    let db = make_db();
    // "00042" has 2 significant digits, not 5
    put_number(&db, "00042").unwrap();
}

#[test]
fn test_number_set_validates_each_element() {
    let db = make_db();
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("k1".to_string()));
    item.insert(
        "nums".to_string(),
        AttributeValue::NS(vec![
            "42".to_string(),
            "123456789012345678901234567890123456789".to_string(), // 39 digits
        ]),
    );
    let err = db
        .put_item(PutItemRequest {
            table_name: "Tbl".to_string(),
            item,
            ..Default::default()
        })
        .unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("38 significant digits"),
        "unexpected error: {msg}"
    );
}
