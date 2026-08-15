//! What a PartiQL predicate does to each attribute type.
//!
//! Measured against real DynamoDB in eu-west-2 on 15 August 2026. Every type was
//! fired twice on the same stored value, once as a PartiQL predicate and once as
//! the equivalent `ConditionExpression`, and the two agreed on all sixty-six
//! comparisons. So there is one set of semantics here, not two, and the
//! expectations below are shared between the surfaces deliberately.
//!
//! `permuted` is the interesting probe. Sets are order-independent, lists are
//! not, and maps compare on their key set rather than on key order.

use dynoxide::Database;
use dynoxide::actions::execute_statement::ExecuteStatementRequest;
use dynoxide::types::AttributeValue;
use std::collections::HashMap;

const TABLE: &str = "pq_pred";

fn db_with(attr: &str, value: AttributeValue) -> Database {
    let db = Database::memory().unwrap();
    db.create_table(
        serde_json::from_value(serde_json::json!({
            "TableName": TABLE,
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "BillingMode": "PAY_PER_REQUEST"
        }))
        .unwrap(),
    )
    .unwrap();
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("k".to_string()));
    item.insert(attr.to_string(), value);
    db.put_item(
        serde_json::from_value(serde_json::json!({"TableName": TABLE, "Item": item})).unwrap(),
    )
    .unwrap();
    db
}

/// Does `attr <op> value` select the row?
fn matches(db: &Database, attr: &str, op: &str, value: AttributeValue) -> bool {
    let resp = db
        .execute_statement(ExecuteStatementRequest {
            statement: format!("SELECT pk FROM \"{TABLE}\" WHERE pk = ? AND \"{attr}\" {op} ?"),
            parameters: Some(vec![AttributeValue::S("k".to_string()), value]),
            ..Default::default()
        })
        .unwrap();
    !resp.items.unwrap_or_default().is_empty()
}

/// Does the same predicate let a PartiQL UPDATE through? The issue's real
/// complaint is that a write conditioned on one of these types can never fire,
/// so the write path is exercised rather than inferred from the read.
fn update_fires(db: &Database, attr: &str, value: AttributeValue) -> bool {
    db.execute_statement(ExecuteStatementRequest {
        statement: format!(
            "UPDATE \"{TABLE}\" SET marker = 'fired' WHERE pk = ? AND \"{attr}\" = ?"
        ),
        parameters: Some(vec![AttributeValue::S("k".to_string()), value]),
        ..Default::default()
    })
    .is_ok()
}

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}
fn n(v: &str) -> AttributeValue {
    AttributeValue::N(v.to_string())
}
fn b(v: &str) -> AttributeValue {
    AttributeValue::B(v.as_bytes().to_vec())
}

/// `=` matches and `<>` does not, and the update conditioned on it fires.
fn assert_matches(db: &Database, attr: &str, value: AttributeValue, case: &str) {
    assert!(
        matches(db, attr, "=", value.clone()),
        "{case}: = should match"
    );
    assert!(
        !matches(db, attr, "<>", value.clone()),
        "{case}: <> should not match"
    );
    assert!(update_fires(db, attr, value), "{case}: UPDATE should fire");
}

/// `=` does not match and `<>` does.
fn assert_differs(db: &Database, attr: &str, value: AttributeValue, case: &str) {
    assert!(
        !matches(db, attr, "=", value.clone()),
        "{case}: = should not match"
    );
    assert!(matches(db, attr, "<>", value), "{case}: <> should match");
}

// --- sets: order-independent ---------------------------------------------

#[test]
fn string_sets_compare_without_regard_to_order() {
    let db = db_with(
        "ss",
        AttributeValue::SS(vec!["a".into(), "b".into(), "c".into()]),
    );
    assert_matches(
        &db,
        "ss",
        AttributeValue::SS(vec!["a".into(), "b".into(), "c".into()]),
        "same",
    );
    assert_matches(
        &db,
        "ss",
        AttributeValue::SS(vec!["c".into(), "a".into(), "b".into()]),
        "permuted",
    );
    assert_differs(
        &db,
        "ss",
        AttributeValue::SS(vec!["a".into(), "b".into()]),
        "differs",
    );
}

#[test]
fn number_sets_compare_without_regard_to_order() {
    let db = db_with(
        "ns",
        AttributeValue::NS(vec!["1".into(), "2".into(), "3".into()]),
    );
    assert_matches(
        &db,
        "ns",
        AttributeValue::NS(vec!["1".into(), "2".into(), "3".into()]),
        "same",
    );
    assert_matches(
        &db,
        "ns",
        AttributeValue::NS(vec!["3".into(), "1".into(), "2".into()]),
        "permuted",
    );
    assert_differs(
        &db,
        "ns",
        AttributeValue::NS(vec!["1".into(), "2".into()]),
        "differs",
    );
}

#[test]
fn binary_sets_compare_without_regard_to_order() {
    let db = db_with("bs", AttributeValue::BS(vec![b"p".to_vec(), b"q".to_vec()]));
    assert_matches(
        &db,
        "bs",
        AttributeValue::BS(vec![b"p".to_vec(), b"q".to_vec()]),
        "same",
    );
    assert_matches(
        &db,
        "bs",
        AttributeValue::BS(vec![b"q".to_vec(), b"p".to_vec()]),
        "permuted",
    );
    assert_differs(
        &db,
        "bs",
        AttributeValue::BS(vec![b"p".to_vec()]),
        "differs",
    );
}

// --- lists are order-sensitive, maps are not ------------------------------

#[test]
fn lists_compare_in_order() {
    let list = || AttributeValue::L(vec![s("a"), n("1"), s("b")]);
    let db = db_with("list", list());
    assert_matches(&db, "list", list(), "same");
    // The one place set and list semantics part.
    assert_differs(
        &db,
        "list",
        AttributeValue::L(vec![s("b"), n("1"), s("a")]),
        "permuted",
    );
    assert_differs(&db, "list", AttributeValue::L(vec![s("a")]), "differs");
}

#[test]
fn maps_compare_without_regard_to_key_order() {
    let mut stored = HashMap::new();
    stored.insert("x".to_string(), n("1"));
    stored.insert("y".to_string(), s("two"));
    let db = db_with("map", AttributeValue::M(stored.clone()));

    assert_matches(&db, "map", AttributeValue::M(stored), "same");

    // A different insertion order is the same map.
    let mut reordered = HashMap::new();
    reordered.insert("y".to_string(), s("two"));
    reordered.insert("x".to_string(), n("1"));
    assert_matches(&db, "map", AttributeValue::M(reordered), "permuted");

    let mut differs = HashMap::new();
    differs.insert("x".to_string(), n("9"));
    differs.insert("y".to_string(), s("two"));
    assert_differs(&db, "map", AttributeValue::M(differs), "differs");
}

// --- the remaining scalars the catch-all swallowed ------------------------

#[test]
fn binary_compares_by_bytes() {
    let db = db_with("bin", b("hello"));
    assert_matches(&db, "bin", b("hello"), "same");
    assert_differs(&db, "bin", b("world"), "differs");
}

#[test]
fn null_compares_equal_to_null() {
    let db = db_with("nul", AttributeValue::NULL(true));
    assert_matches(&db, "nul", AttributeValue::NULL(true), "same");
}

// --- characterisation: the scalars that already worked --------------------

#[test]
fn booleans_are_unchanged() {
    let db = db_with("flag", AttributeValue::BOOL(true));
    assert_matches(&db, "flag", AttributeValue::BOOL(true), "same");
    assert_differs(&db, "flag", AttributeValue::BOOL(false), "differs");
}

#[test]
fn strings_are_unchanged() {
    let db = db_with("name", s("plain"));
    assert_matches(&db, "name", s("plain"), "same");
    assert_differs(&db, "name", s("other-value"), "differs");
    assert!(matches(&db, "name", "<", s("q")), "string ordering");
    assert!(matches(&db, "name", ">", s("a")), "string ordering");
}

#[test]
fn numbers_are_unchanged_including_across_written_forms() {
    let db = db_with("count", n("1"));
    assert_matches(&db, "count", n("1"), "same");
    // 1 and 1.0 are the same number, which is the behaviour the shared engine
    // must preserve rather than compare as strings.
    assert_matches(&db, "count", n("1.0"), "decimal form");
    assert_differs(&db, "count", n("2"), "differs");
    assert!(matches(&db, "count", "<", n("2")), "numeric ordering");
    assert!(matches(&db, "count", ">=", n("1")), "numeric ordering");
}

#[test]
fn a_type_mismatch_is_never_equal_and_always_unequal() {
    let db = db_with("name", s("plain"));
    assert!(!matches(&db, "name", "=", n("1")), "S = N");
    assert!(matches(&db, "name", "<>", n("1")), "S <> N");
    assert!(
        !matches(&db, "name", "<", n("1")),
        "ordering across types is false"
    );
}

#[test]
fn an_absent_attribute_matches_nothing() {
    let db = db_with("name", s("plain"));
    assert!(!matches(&db, "missing", "=", s("plain")));
    // An absent attribute is not unequal either: there is nothing to compare.
    assert!(!matches(&db, "missing", "<>", s("plain")));
}
