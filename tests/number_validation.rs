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
fn test_leading_plus_sign_accepted_and_normalised() {
    // Issue #109: real DynamoDB accepts a leading '+' on the mantissa and
    // stores it normalised with the '+' dropped. dynoxide rejected it. The
    // input -> stored pairs here were all captured against real DynamoDB.
    let db = make_db();
    for (input, stored) in [
        ("+5", "5"),
        ("+1.5", "1.5"),
        ("+0", "0"),
        ("+1e2", "100"),
        ("+.5", "0.5"),
        ("5.", "5"),
        ("+0.0", "0"),
    ] {
        let mut item = HashMap::new();
        item.insert("pk".to_string(), AttributeValue::S("k1".to_string()));
        item.insert("val".to_string(), AttributeValue::N(input.to_string()));
        db.put_item(PutItemRequest {
            table_name: "Tbl".to_string(),
            item,
            ..Default::default()
        })
        .unwrap_or_else(|e| panic!("put of {input} should succeed: {e}"));

        let mut key = HashMap::new();
        key.insert("pk".to_string(), AttributeValue::S("k1".to_string()));
        let resp = db
            .get_item(dynoxide::actions::get_item::GetItemRequest {
                table_name: "Tbl".to_string(),
                key,
                consistent_read: Some(true),
                ..Default::default()
            })
            .unwrap();
        let got = resp.item.expect("item present");
        assert_eq!(
            got.get("val"),
            Some(&AttributeValue::N(stored.to_string())),
            "{input} should read back as {stored}"
        );
    }
}

#[test]
fn test_malformed_numbers_rejected_end_to_end() {
    // These are all rejected by real DynamoDB; dynoxide must reject them too.
    let db = make_db();
    for input in [
        "+e2", "1+2", "+1+2", "1.2.3", "++5", "1e", " 5", "5 ", "NaN",
    ] {
        let err = put_number(&db, input).expect_err(&format!("{input:?} should be rejected"));
        assert!(
            err.contains("cannot be converted to a numeric value"),
            "unexpected error for {input:?}: {err}"
        );
    }
}

#[test]
fn test_malformed_number_message_stays_bare() {
    // Number-format errors on PutItem are not a captured enveloped family and
    // keep the bare message, byte for byte.
    let db = make_db();
    let err = put_number(&db, "1.2.3").unwrap_err();
    assert_eq!(
        err,
        "The parameter cannot be converted to a numeric value: 1.2.3"
    );
}

#[test]
fn test_number_set_leading_plus_normalised() {
    // NS elements with a leading '+' are accepted and stored normalised.
    let db = make_db();
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("k1".to_string()));
    item.insert(
        "nums".to_string(),
        AttributeValue::NS(vec!["+5".to_string(), "+10".to_string()]),
    );
    db.put_item(PutItemRequest {
        table_name: "Tbl".to_string(),
        item,
        ..Default::default()
    })
    .unwrap();

    let mut key = HashMap::new();
    key.insert("pk".to_string(), AttributeValue::S("k1".to_string()));
    let resp = db
        .get_item(dynoxide::actions::get_item::GetItemRequest {
            table_name: "Tbl".to_string(),
            key,
            consistent_read: Some(true),
            ..Default::default()
        })
        .unwrap();
    let got = resp.item.expect("item present");
    match got.get("nums") {
        Some(AttributeValue::NS(ns)) => {
            let mut ns = ns.clone();
            ns.sort();
            assert_eq!(ns, vec!["10".to_string(), "5".to_string()]);
        }
        other => panic!("expected NS, got {other:?}"),
    }
}

#[test]
fn test_leading_plus_numeric_sort_key_round_trips() {
    // A '+'-prefixed Number used as a sort key normalises to the same key as
    // its bare form, so it can be written with '+5' and read back with '5'
    // (and vice versa). Exercises normalize_number_for_sort, not just the
    // attribute-value normalisation.
    let db = Database::memory().unwrap();
    db.create_table(CreateTableRequest {
        table_name: "Tbl2".to_string(),
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
                attribute_type: ScalarAttributeType::N,
            },
        ],
        ..Default::default()
    })
    .unwrap();

    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("p".to_string()));
    item.insert("sk".to_string(), AttributeValue::N("+5".to_string()));
    db.put_item(PutItemRequest {
        table_name: "Tbl2".to_string(),
        item,
        ..Default::default()
    })
    .unwrap();

    for lookup in ["5", "+5"] {
        let mut key = HashMap::new();
        key.insert("pk".to_string(), AttributeValue::S("p".to_string()));
        key.insert("sk".to_string(), AttributeValue::N(lookup.to_string()));
        let resp = db
            .get_item(dynoxide::actions::get_item::GetItemRequest {
                table_name: "Tbl2".to_string(),
                key,
                consistent_read: Some(true),
                ..Default::default()
            })
            .unwrap();
        let got = resp
            .item
            .unwrap_or_else(|| panic!("item not found via sort key {lookup:?}"));
        assert_eq!(got.get("sk"), Some(&AttributeValue::N("5".to_string())));
    }
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
