//! Document nesting is capped at 32 levels, counting the top-level attribute as
//! level 1. Real DynamoDB rejects deeper values with a ValidationException, on both
//! the stored-item path (PutItem) and the ExpressionAttributeValue path, before the
//! expression is evaluated. Boundary and message captured against real DynamoDB
//! (eu-west-2, 2026-06): accepts 31 map-wraps, rejects 32.

use dynoxide::Database;
use dynoxide::actions::create_table::CreateTableRequest;
use dynoxide::actions::put_item::PutItemRequest;
use dynoxide::actions::update_item::UpdateItemRequest;
use dynoxide::types::*;
use std::collections::HashMap;

const NEST_MSG: &str = "Nesting Levels have exceeded supported limits";

fn make_db() -> Database {
    Database::memory().unwrap()
}

fn create_table(db: &Database) {
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
}

/// Wrap a scalar leaf in `depth` single-key maps. depth=1 -> M{ n: S("leaf") }.
fn deep_map(depth: usize) -> AttributeValue {
    let mut v = AttributeValue::S("leaf".into());
    for _ in 0..depth {
        v = AttributeValue::M(HashMap::from([("n".to_string(), v)]));
    }
    v
}

fn put_with_data(db: &Database, pk: &str, data: AttributeValue) -> dynoxide::errors::Result<()> {
    db.put_item(PutItemRequest {
        table_name: "Tbl".to_string(),
        item: HashMap::from([
            ("pk".to_string(), AttributeValue::S(pk.into())),
            ("data".to_string(), data),
        ]),
        ..Default::default()
    })
    .map(|_| ())
}

fn update_with_deep_condition(db: &Database, depth: usize) -> dynoxide::errors::Result<()> {
    db.update_item(UpdateItemRequest {
        table_name: "Tbl".to_string(),
        key: HashMap::from([("pk".to_string(), AttributeValue::S("k1".into()))]),
        update_expression: Some("SET touched = :t".into()),
        condition_expression: Some("#d = :deep".into()),
        expression_attribute_names: Some(HashMap::from([("#d".to_string(), "data".to_string())])),
        expression_attribute_values: Some(HashMap::from([
            (":t".to_string(), AttributeValue::S("y".into())),
            (":deep".to_string(), deep_map(depth)),
        ])),
        ..Default::default()
    })
    .map(|_| ())
}

// --- Stored item (PutItem) ---

#[test]
fn stored_item_accepts_31_levels() {
    let db = make_db();
    create_table(&db);
    put_with_data(&db, "ok", deep_map(31)).expect("31 levels should store");
}

#[test]
fn stored_item_rejects_32_levels() {
    let db = make_db();
    create_table(&db);
    let err = put_with_data(&db, "bad", deep_map(32)).unwrap_err();
    assert!(
        err.to_string().contains(NEST_MSG),
        "expected nesting ValidationException, got: {err}"
    );
}

#[test]
fn stored_item_nesting_message_stays_bare() {
    // The nesting-depth rejection on PutItem is not a captured enveloped family
    // and keeps the bare message, byte for byte.
    let db = make_db();
    create_table(&db);
    let err = put_with_data(&db, "bad", deep_map(32)).unwrap_err();
    assert_eq!(
        err.to_string(),
        "Nesting Levels have exceeded supported limits: \
         Attributes in the item have nested levels beyond supported limit"
    );
}

// --- ExpressionAttributeValue (UpdateItem ConditionExpression) ---

#[test]
fn condition_eav_accepts_31_levels_and_evaluates() {
    let db = make_db();
    create_table(&db);
    // Seed an item with no `data` attribute, so the condition is false once evaluated.
    db.put_item(PutItemRequest {
        table_name: "Tbl".to_string(),
        item: HashMap::from([("pk".to_string(), AttributeValue::S("k1".into()))]),
        ..Default::default()
    })
    .unwrap();

    let err = update_with_deep_condition(&db, 31).unwrap_err();
    let msg = err.to_string();
    // A 31-level value is accepted and the condition is evaluated, so this is a
    // conditional failure, NOT a nesting ValidationException.
    assert!(
        msg.contains("conditional request failed"),
        "expected conditional failure, got: {msg}"
    );
    assert!(
        !msg.contains(NEST_MSG),
        "31 levels must not trip the nesting cap"
    );
}

#[test]
fn condition_eav_rejects_32_levels() {
    let db = make_db();
    create_table(&db);
    db.put_item(PutItemRequest {
        table_name: "Tbl".to_string(),
        item: HashMap::from([("pk".to_string(), AttributeValue::S("k1".into()))]),
        ..Default::default()
    })
    .unwrap();

    let err = update_with_deep_condition(&db, 32).unwrap_err();
    assert!(
        err.to_string().contains(NEST_MSG),
        "expected nesting ValidationException, got: {err}"
    );
}
