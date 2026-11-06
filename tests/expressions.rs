//! Phase 4 integration tests: Expression engine + UpdateItem.

use dynoxide::Database;
use dynoxide::actions::create_table::CreateTableRequest;
use dynoxide::actions::delete_item::DeleteItemRequest;
use dynoxide::actions::get_item::GetItemRequest;
use dynoxide::actions::put_item::PutItemRequest;
use dynoxide::actions::update_item::UpdateItemRequest;
use dynoxide::types::*;
use std::collections::HashMap;

fn make_db() -> Database {
    Database::memory().unwrap()
}

fn create_table(db: &Database, name: &str) {
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

fn make_item(pairs: &[(&str, AttributeValue)]) -> HashMap<String, AttributeValue> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

fn key_map(pairs: &[(&str, AttributeValue)]) -> HashMap<String, AttributeValue> {
    make_item(pairs)
}

fn put(db: &Database, table: &str, item: &[(&str, AttributeValue)]) {
    db.put_item(PutItemRequest {
        table_name: table.to_string(),
        item: make_item(item),
        return_values: None,
        condition_expression: None,
        expression_attribute_names: None,
        expression_attribute_values: None,
        ..Default::default()
    })
    .unwrap();
}

// ---------------------------------------------------------------------------
// ConditionExpression on PutItem
// ---------------------------------------------------------------------------

#[test]
fn test_put_condition_attribute_not_exists() {
    let db = make_db();
    create_table(&db, "Tbl");

    // First put: condition should pass (item doesn't exist)
    db.put_item(PutItemRequest {
        table_name: "Tbl".to_string(),
        item: make_item(&[
            ("pk", AttributeValue::S("k1".into())),
            ("val", AttributeValue::S("first".into())),
        ]),
        return_values: None,
        condition_expression: Some("attribute_not_exists(pk)".into()),
        expression_attribute_names: None,
        expression_attribute_values: None,
        ..Default::default()
    })
    .unwrap();

    // Second put: condition should fail (item exists)
    let err = db
        .put_item(PutItemRequest {
            table_name: "Tbl".to_string(),
            item: make_item(&[
                ("pk", AttributeValue::S("k1".into())),
                ("val", AttributeValue::S("second".into())),
            ]),
            return_values: None,
            condition_expression: Some("attribute_not_exists(pk)".into()),
            expression_attribute_names: None,
            expression_attribute_values: None,
            ..Default::default()
        })
        .unwrap_err();

    assert!(err.to_string().contains("conditional request failed"));
}

#[test]
fn test_put_condition_with_comparison() {
    let db = make_db();
    create_table(&db, "Tbl");

    put(
        &db,
        "Tbl",
        &[
            ("pk", AttributeValue::S("k1".into())),
            ("version", AttributeValue::N("1".into())),
        ],
    );

    // Should succeed: version = 1
    db.put_item(PutItemRequest {
        table_name: "Tbl".to_string(),
        item: make_item(&[
            ("pk", AttributeValue::S("k1".into())),
            ("version", AttributeValue::N("2".into())),
        ]),
        return_values: None,
        condition_expression: Some("version = :v".into()),
        expression_attribute_names: None,
        expression_attribute_values: Some(make_item(&[(":v", AttributeValue::N("1".into()))])),
        ..Default::default()
    })
    .unwrap();

    // Should fail: version is now 2, not 1
    let err = db
        .put_item(PutItemRequest {
            table_name: "Tbl".to_string(),
            item: make_item(&[
                ("pk", AttributeValue::S("k1".into())),
                ("version", AttributeValue::N("3".into())),
            ]),
            return_values: None,
            condition_expression: Some("version = :v".into()),
            expression_attribute_names: None,
            expression_attribute_values: Some(make_item(&[(":v", AttributeValue::N("1".into()))])),
            ..Default::default()
        })
        .unwrap_err();

    assert!(err.to_string().contains("conditional request failed"));
}

// #103: a ConditionExpression that compares a Map (M) attribute for equality
// must succeed when the stored map equals the supplied value. Before the fix,
// compare_values had no arm for M, so map equality always failed.
#[test]
fn test_condition_map_equality() {
    let db = make_db();
    create_table(&db, "Tbl");

    let status = || {
        AttributeValue::M(HashMap::from([(
            "Union_Case".to_string(),
            AttributeValue::S("Passive".into()),
        )]))
    };

    put(
        &db,
        "Tbl",
        &[("pk", AttributeValue::S("k1".into())), ("Status", status())],
    );

    // Should succeed: Status equals the supplied map.
    db.update_item(UpdateItemRequest {
        table_name: "Tbl".to_string(),
        key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
        update_expression: Some("SET Touched = :t".into()),
        condition_expression: Some("#S = :p".into()),
        expression_attribute_names: Some(HashMap::from([("#S".to_string(), "Status".to_string())])),
        expression_attribute_values: Some(make_item(&[
            (":t", AttributeValue::S("yes".into())),
            (":p", status()),
        ])),
        return_values: None,
        ..Default::default()
    })
    .unwrap();

    // Should fail: the stored map no longer matches a different map.
    let err = db
        .update_item(UpdateItemRequest {
            table_name: "Tbl".to_string(),
            key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
            update_expression: Some("SET Touched = :t".into()),
            condition_expression: Some("#S = :p".into()),
            expression_attribute_names: Some(HashMap::from([(
                "#S".to_string(),
                "Status".to_string(),
            )])),
            expression_attribute_values: Some(make_item(&[
                (":t", AttributeValue::S("again".into())),
                (
                    ":p",
                    AttributeValue::M(HashMap::from([(
                        "Union_Case".to_string(),
                        AttributeValue::S("Active".into()),
                    )])),
                ),
            ])),
            return_values: None,
            ..Default::default()
        })
        .unwrap_err();
    assert!(err.to_string().contains("conditional request failed"));
}

// #103: the List (L) counterpart to test_condition_map_equality. Element-wise,
// order-sensitive deep equality must gate a real UpdateItem end-to-end.
#[test]
fn test_condition_list_equality() {
    let db = make_db();
    create_table(&db, "Tbl");

    let roles = || {
        AttributeValue::L(vec![
            AttributeValue::S("admin".into()),
            AttributeValue::S("viewer".into()),
        ])
    };

    put(
        &db,
        "Tbl",
        &[("pk", AttributeValue::S("k1".into())), ("Roles", roles())],
    );

    // Should succeed: Roles equals the supplied list.
    db.update_item(UpdateItemRequest {
        table_name: "Tbl".to_string(),
        key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
        update_expression: Some("SET Touched = :t".into()),
        condition_expression: Some("#R = :p".into()),
        expression_attribute_names: Some(HashMap::from([("#R".to_string(), "Roles".to_string())])),
        expression_attribute_values: Some(make_item(&[
            (":t", AttributeValue::S("yes".into())),
            (":p", roles()),
        ])),
        return_values: None,
        ..Default::default()
    })
    .unwrap();

    // Should fail: same elements, reversed order is not equal for a List.
    let err = db
        .update_item(UpdateItemRequest {
            table_name: "Tbl".to_string(),
            key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
            update_expression: Some("SET Touched = :t".into()),
            condition_expression: Some("#R = :p".into()),
            expression_attribute_names: Some(HashMap::from([(
                "#R".to_string(),
                "Roles".to_string(),
            )])),
            expression_attribute_values: Some(make_item(&[
                (":t", AttributeValue::S("again".into())),
                (
                    ":p",
                    AttributeValue::L(vec![
                        AttributeValue::S("viewer".into()),
                        AttributeValue::S("admin".into()),
                    ]),
                ),
            ])),
            return_values: None,
            ..Default::default()
        })
        .unwrap_err();
    assert!(err.to_string().contains("conditional request failed"));
}

// ---------------------------------------------------------------------------
// ConditionExpression on DeleteItem
// ---------------------------------------------------------------------------

#[test]
fn test_delete_condition_succeeds() {
    let db = make_db();
    create_table(&db, "Tbl");

    put(
        &db,
        "Tbl",
        &[
            ("pk", AttributeValue::S("k1".into())),
            ("status", AttributeValue::S("archived".into())),
        ],
    );

    db.delete_item(DeleteItemRequest {
        table_name: "Tbl".to_string(),
        key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
        return_values: None,
        condition_expression: Some("#st = :s".into()),
        expression_attribute_names: Some(HashMap::from([(
            "#st".to_string(),
            "status".to_string(),
        )])),
        expression_attribute_values: Some(make_item(&[(
            ":s",
            AttributeValue::S("archived".into()),
        )])),
        ..Default::default()
    })
    .unwrap();

    // Verify deleted
    let resp = db
        .get_item(GetItemRequest {
            table_name: "Tbl".to_string(),
            key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
            consistent_read: None,
            projection_expression: None,
            ..Default::default()
        })
        .unwrap();
    assert!(resp.item.is_none());
}

#[test]
fn test_delete_condition_fails() {
    let db = make_db();
    create_table(&db, "Tbl");

    put(
        &db,
        "Tbl",
        &[
            ("pk", AttributeValue::S("k1".into())),
            ("status", AttributeValue::S("active".into())),
        ],
    );

    let err = db
        .delete_item(DeleteItemRequest {
            table_name: "Tbl".to_string(),
            key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
            return_values: None,
            condition_expression: Some("#st = :s".into()),
            expression_attribute_names: Some(HashMap::from([(
                "#st".to_string(),
                "status".to_string(),
            )])),
            expression_attribute_values: Some(make_item(&[(
                ":s",
                AttributeValue::S("archived".into()),
            )])),
            ..Default::default()
        })
        .unwrap_err();

    assert!(err.to_string().contains("conditional request failed"));
}

// ---------------------------------------------------------------------------
// ProjectionExpression on GetItem
// ---------------------------------------------------------------------------

#[test]
fn test_get_with_projection() {
    let db = make_db();
    create_table(&db, "Tbl");

    put(
        &db,
        "Tbl",
        &[
            ("pk", AttributeValue::S("k1".into())),
            ("name", AttributeValue::S("Alice".into())),
            ("age", AttributeValue::N("30".into())),
            ("email", AttributeValue::S("alice@example.com".into())),
        ],
    );

    let resp = db
        .get_item(GetItemRequest {
            table_name: "Tbl".to_string(),
            key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
            consistent_read: None,
            projection_expression: Some("#nm, age".into()),
            expression_attribute_names: Some(HashMap::from([(
                "#nm".to_string(),
                "name".to_string(),
            )])),
            ..Default::default()
        })
        .unwrap();

    let item = resp.item.unwrap();
    // GetItem does NOT automatically include key attributes in ProjectionExpression
    assert!(!item.contains_key("pk"));
    assert!(item.contains_key("name"));
    assert!(item.contains_key("age"));
    assert!(!item.contains_key("email")); // Not projected
}

#[test]
fn test_get_with_projection_attribute_names() {
    let db = make_db();
    create_table(&db, "Tbl");

    put(
        &db,
        "Tbl",
        &[
            ("pk", AttributeValue::S("k1".into())),
            ("name", AttributeValue::S("Alice".into())),
            ("status", AttributeValue::S("active".into())),
        ],
    );

    let resp = db
        .get_item(GetItemRequest {
            table_name: "Tbl".to_string(),
            key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
            consistent_read: None,
            projection_expression: Some("#n".into()),
            expression_attribute_names: Some(HashMap::from([(
                "#n".to_string(),
                "name".to_string(),
            )])),
            ..Default::default()
        })
        .unwrap();

    let item = resp.item.unwrap();
    assert!(item.contains_key("name"));
    assert!(!item.contains_key("status"));
}

// ---------------------------------------------------------------------------
// UpdateItem: SET
// ---------------------------------------------------------------------------

#[test]
fn test_update_set_basic() {
    let db = make_db();
    create_table(&db, "Tbl");

    put(
        &db,
        "Tbl",
        &[
            ("pk", AttributeValue::S("k1".into())),
            ("name", AttributeValue::S("Alice".into())),
        ],
    );

    db.update_item(UpdateItemRequest {
        table_name: "Tbl".to_string(),
        key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
        update_expression: Some("SET #nm = :n".into()),
        condition_expression: None,
        expression_attribute_names: Some(HashMap::from([("#nm".to_string(), "name".to_string())])),
        expression_attribute_values: Some(make_item(&[(":n", AttributeValue::S("Bob".into()))])),
        return_values: None,
        ..Default::default()
    })
    .unwrap();

    let resp = db
        .get_item(GetItemRequest {
            table_name: "Tbl".to_string(),
            key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
            consistent_read: None,
            projection_expression: None,
            ..Default::default()
        })
        .unwrap();

    assert_eq!(resp.item.unwrap()["name"], AttributeValue::S("Bob".into()));
}

#[test]
fn test_update_set_arithmetic() {
    let db = make_db();
    create_table(&db, "Tbl");

    put(
        &db,
        "Tbl",
        &[
            ("pk", AttributeValue::S("k1".into())),
            ("price", AttributeValue::N("100".into())),
        ],
    );

    db.update_item(UpdateItemRequest {
        table_name: "Tbl".to_string(),
        key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
        update_expression: Some("SET price = price - :discount".into()),
        condition_expression: None,
        expression_attribute_names: None,
        expression_attribute_values: Some(make_item(&[(
            ":discount",
            AttributeValue::N("25".into()),
        )])),
        return_values: None,
        ..Default::default()
    })
    .unwrap();

    let resp = db
        .get_item(GetItemRequest {
            table_name: "Tbl".to_string(),
            key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
            consistent_read: None,
            projection_expression: None,
            ..Default::default()
        })
        .unwrap();

    assert_eq!(resp.item.unwrap()["price"], AttributeValue::N("75".into()));
}

#[test]
fn test_update_set_if_not_exists() {
    let db = make_db();
    create_table(&db, "Tbl");

    put(&db, "Tbl", &[("pk", AttributeValue::S("k1".into()))]);

    db.update_item(UpdateItemRequest {
        table_name: "Tbl".to_string(),
        key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
        update_expression: Some("SET #vw = if_not_exists(#vw, :zero)".into()),
        condition_expression: None,
        expression_attribute_names: Some(HashMap::from([("#vw".to_string(), "views".to_string())])),
        expression_attribute_values: Some(make_item(&[(":zero", AttributeValue::N("0".into()))])),
        return_values: None,
        ..Default::default()
    })
    .unwrap();

    let resp = db
        .get_item(GetItemRequest {
            table_name: "Tbl".to_string(),
            key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
            consistent_read: None,
            projection_expression: None,
            ..Default::default()
        })
        .unwrap();

    assert_eq!(resp.item.unwrap()["views"], AttributeValue::N("0".into()));
}

/// DynamoDB validates expression attribute usage syntactically, not semantically.
/// When if_not_exists(existing, :def) short-circuits because `existing` already has a value,
/// :def is still considered "used" because it appears in the expression text.
#[test]
fn test_if_not_exists_value_ref_not_falsely_reported_unused() {
    let db = make_db();
    create_table(&db, "Tbl");

    // Put item WITH the attribute that if_not_exists will check
    put(
        &db,
        "Tbl",
        &[
            ("pk", AttributeValue::S("k1".into())),
            ("existing", AttributeValue::S("already_here".into())),
        ],
    );

    // SET existing = if_not_exists(existing, :def), newone = if_not_exists(newone, :def2)
    // :def is referenced but if_not_exists short-circuits - DynamoDB still considers it "used"
    db.update_item(UpdateItemRequest {
        table_name: "Tbl".to_string(),
        key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
        update_expression: Some(
            "SET existing = if_not_exists(existing, :def), newone = if_not_exists(newone, :def2)"
                .into(),
        ),
        expression_attribute_values: Some(make_item(&[
            (":def", AttributeValue::S("default1".into())),
            (":def2", AttributeValue::S("default2".into())),
        ])),
        ..Default::default()
    })
    .expect("should not reject :def as unused - it appears in the expression text");

    let resp = db
        .get_item(GetItemRequest {
            table_name: "Tbl".to_string(),
            key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
            ..Default::default()
        })
        .unwrap();

    let item = resp.item.unwrap();
    // existing was already there: if_not_exists returns the existing value
    assert_eq!(item["existing"], AttributeValue::S("already_here".into()));
    // newone didn't exist: if_not_exists uses the default
    assert_eq!(item["newone"], AttributeValue::S("default2".into()));
}

#[test]
fn test_update_set_list_append() {
    let db = make_db();
    create_table(&db, "Tbl");

    put(
        &db,
        "Tbl",
        &[
            ("pk", AttributeValue::S("k1".into())),
            (
                "items",
                AttributeValue::L(vec![AttributeValue::S("a".into())]),
            ),
        ],
    );

    db.update_item(UpdateItemRequest {
        table_name: "Tbl".to_string(),
        key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
        update_expression: Some("SET #it = list_append(#it, :new)".into()),
        condition_expression: None,
        expression_attribute_names: Some(HashMap::from([("#it".to_string(), "items".to_string())])),
        expression_attribute_values: Some(make_item(&[(
            ":new",
            AttributeValue::L(vec![AttributeValue::S("b".into())]),
        )])),
        return_values: None,
        ..Default::default()
    })
    .unwrap();

    let resp = db
        .get_item(GetItemRequest {
            table_name: "Tbl".to_string(),
            key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
            consistent_read: None,
            projection_expression: None,
            ..Default::default()
        })
        .unwrap();

    let item = resp.item.unwrap();
    if let AttributeValue::L(list) = &item["items"] {
        assert_eq!(list.len(), 2);
        assert_eq!(list[1], AttributeValue::S("b".into()));
    } else {
        panic!("Expected list");
    }
}

// ---------------------------------------------------------------------------
// UpdateItem: REMOVE
// ---------------------------------------------------------------------------

#[test]
fn test_update_remove() {
    let db = make_db();
    create_table(&db, "Tbl");

    put(
        &db,
        "Tbl",
        &[
            ("pk", AttributeValue::S("k1".into())),
            ("name", AttributeValue::S("Alice".into())),
            ("temp", AttributeValue::S("delete-me".into())),
        ],
    );

    db.update_item(UpdateItemRequest {
        table_name: "Tbl".to_string(),
        key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
        update_expression: Some("REMOVE #tmp".into()),
        condition_expression: None,
        expression_attribute_names: Some(HashMap::from([("#tmp".to_string(), "temp".to_string())])),
        expression_attribute_values: None,
        return_values: None,
        ..Default::default()
    })
    .unwrap();

    let resp = db
        .get_item(GetItemRequest {
            table_name: "Tbl".to_string(),
            key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
            consistent_read: None,
            projection_expression: None,
            ..Default::default()
        })
        .unwrap();

    let item = resp.item.unwrap();
    assert!(!item.contains_key("temp"));
    assert!(item.contains_key("name"));
}

// ---------------------------------------------------------------------------
// UpdateItem: ADD
// ---------------------------------------------------------------------------

#[test]
fn test_update_add_number() {
    let db = make_db();
    create_table(&db, "Tbl");

    put(
        &db,
        "Tbl",
        &[
            ("pk", AttributeValue::S("k1".into())),
            ("counter", AttributeValue::N("10".into())),
        ],
    );

    db.update_item(UpdateItemRequest {
        table_name: "Tbl".to_string(),
        key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
        update_expression: Some("ADD #ctr :inc".into()),
        condition_expression: None,
        expression_attribute_names: Some(HashMap::from([(
            "#ctr".to_string(),
            "counter".to_string(),
        )])),
        expression_attribute_values: Some(make_item(&[(":inc", AttributeValue::N("5".into()))])),
        return_values: None,
        ..Default::default()
    })
    .unwrap();

    let resp = db
        .get_item(GetItemRequest {
            table_name: "Tbl".to_string(),
            key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
            consistent_read: None,
            projection_expression: None,
            ..Default::default()
        })
        .unwrap();

    assert_eq!(
        resp.item.unwrap()["counter"],
        AttributeValue::N("15".into())
    );
}

#[test]
fn test_update_add_string_set() {
    let db = make_db();
    create_table(&db, "Tbl");

    put(
        &db,
        "Tbl",
        &[
            ("pk", AttributeValue::S("k1".into())),
            (
                "tags",
                AttributeValue::SS(vec!["rust".into(), "dynamo".into()]),
            ),
        ],
    );

    db.update_item(UpdateItemRequest {
        table_name: "Tbl".to_string(),
        key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
        update_expression: Some("ADD tags :new".into()),
        condition_expression: None,
        expression_attribute_names: None,
        expression_attribute_values: Some(make_item(&[(
            ":new",
            AttributeValue::SS(vec!["sqlite".into()]),
        )])),
        return_values: None,
        ..Default::default()
    })
    .unwrap();

    let resp = db
        .get_item(GetItemRequest {
            table_name: "Tbl".to_string(),
            key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
            consistent_read: None,
            projection_expression: None,
            ..Default::default()
        })
        .unwrap();

    if let AttributeValue::SS(set) = &resp.item.unwrap()["tags"] {
        assert_eq!(set.len(), 3);
        assert!(set.contains(&"sqlite".to_string()));
    } else {
        panic!("Expected SS");
    }
}

// ---------------------------------------------------------------------------
// UpdateItem: DELETE (set elements)
// ---------------------------------------------------------------------------

#[test]
fn test_update_delete_set_elements() {
    let db = make_db();
    create_table(&db, "Tbl");

    put(
        &db,
        "Tbl",
        &[
            ("pk", AttributeValue::S("k1".into())),
            (
                "colors",
                AttributeValue::SS(vec!["red".into(), "blue".into(), "green".into()]),
            ),
        ],
    );

    db.update_item(UpdateItemRequest {
        table_name: "Tbl".to_string(),
        key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
        update_expression: Some("DELETE colors :remove".into()),
        condition_expression: None,
        expression_attribute_names: None,
        expression_attribute_values: Some(make_item(&[(
            ":remove",
            AttributeValue::SS(vec!["blue".into(), "green".into()]),
        )])),
        return_values: None,
        ..Default::default()
    })
    .unwrap();

    let resp = db
        .get_item(GetItemRequest {
            table_name: "Tbl".to_string(),
            key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
            consistent_read: None,
            projection_expression: None,
            ..Default::default()
        })
        .unwrap();

    if let AttributeValue::SS(set) = &resp.item.unwrap()["colors"] {
        assert_eq!(set, &vec!["red".to_string()]);
    } else {
        panic!("Expected SS");
    }
}

// ---------------------------------------------------------------------------
// UpdateItem: combined SET + REMOVE
// ---------------------------------------------------------------------------

#[test]
fn test_update_combined_set_remove() {
    let db = make_db();
    create_table(&db, "Tbl");

    put(
        &db,
        "Tbl",
        &[
            ("pk", AttributeValue::S("k1".into())),
            ("name", AttributeValue::S("Alice".into())),
            ("temp", AttributeValue::S("delete-me".into())),
        ],
    );

    db.update_item(UpdateItemRequest {
        table_name: "Tbl".to_string(),
        key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
        update_expression: Some("SET #nm = :n REMOVE #tmp".into()),
        condition_expression: None,
        expression_attribute_names: Some(HashMap::from([
            ("#nm".to_string(), "name".to_string()),
            ("#tmp".to_string(), "temp".to_string()),
        ])),
        expression_attribute_values: Some(make_item(&[(":n", AttributeValue::S("Bob".into()))])),
        return_values: None,
        ..Default::default()
    })
    .unwrap();

    let resp = db
        .get_item(GetItemRequest {
            table_name: "Tbl".to_string(),
            key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
            consistent_read: None,
            projection_expression: None,
            ..Default::default()
        })
        .unwrap();

    let item = resp.item.unwrap();
    assert_eq!(item["name"], AttributeValue::S("Bob".into()));
    assert!(!item.contains_key("temp"));
}

// ---------------------------------------------------------------------------
// UpdateItem: upsert (item doesn't exist)
// ---------------------------------------------------------------------------

#[test]
fn test_update_upsert() {
    let db = make_db();
    create_table(&db, "Tbl");

    db.update_item(UpdateItemRequest {
        table_name: "Tbl".to_string(),
        key: key_map(&[("pk", AttributeValue::S("new-key".into()))]),
        update_expression: Some("SET #nm = :n".into()),
        condition_expression: None,
        expression_attribute_names: Some(HashMap::from([("#nm".to_string(), "name".to_string())])),
        expression_attribute_values: Some(make_item(&[(
            ":n",
            AttributeValue::S("Created".into()),
        )])),
        return_values: None,
        ..Default::default()
    })
    .unwrap();

    let resp = db
        .get_item(GetItemRequest {
            table_name: "Tbl".to_string(),
            key: key_map(&[("pk", AttributeValue::S("new-key".into()))]),
            consistent_read: None,
            projection_expression: None,
            ..Default::default()
        })
        .unwrap();

    let item = resp.item.unwrap();
    assert_eq!(item["name"], AttributeValue::S("Created".into()));
    assert_eq!(item["pk"], AttributeValue::S("new-key".into()));
}

// ---------------------------------------------------------------------------
// UpdateItem: ReturnValues
// ---------------------------------------------------------------------------

#[test]
fn test_update_return_all_old() {
    let db = make_db();
    create_table(&db, "Tbl");

    put(
        &db,
        "Tbl",
        &[
            ("pk", AttributeValue::S("k1".into())),
            ("name", AttributeValue::S("Alice".into())),
        ],
    );

    let resp = db
        .update_item(UpdateItemRequest {
            table_name: "Tbl".to_string(),
            key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
            update_expression: Some("SET #nm = :n".into()),
            condition_expression: None,
            expression_attribute_names: Some(HashMap::from([(
                "#nm".to_string(),
                "name".to_string(),
            )])),
            expression_attribute_values: Some(make_item(&[(
                ":n",
                AttributeValue::S("Bob".into()),
            )])),
            return_values: Some("ALL_OLD".into()),
            ..Default::default()
        })
        .unwrap();

    let old = resp.attributes.unwrap();
    assert_eq!(old["name"], AttributeValue::S("Alice".into()));
}

#[test]
fn test_update_return_all_new() {
    let db = make_db();
    create_table(&db, "Tbl");

    put(
        &db,
        "Tbl",
        &[
            ("pk", AttributeValue::S("k1".into())),
            ("name", AttributeValue::S("Alice".into())),
        ],
    );

    let resp = db
        .update_item(UpdateItemRequest {
            table_name: "Tbl".to_string(),
            key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
            update_expression: Some("SET #nm = :n".into()),
            condition_expression: None,
            expression_attribute_names: Some(HashMap::from([(
                "#nm".to_string(),
                "name".to_string(),
            )])),
            expression_attribute_values: Some(make_item(&[(
                ":n",
                AttributeValue::S("Bob".into()),
            )])),
            return_values: Some("ALL_NEW".into()),
            ..Default::default()
        })
        .unwrap();

    let new = resp.attributes.unwrap();
    assert_eq!(new["name"], AttributeValue::S("Bob".into()));
}

// ---------------------------------------------------------------------------
// UpdateItem: SET evaluation semantics (#35) and ReturnValues granularity (#36)
// ---------------------------------------------------------------------------

/// #35(a): the whole UpdateExpression is evaluated against the pre-update image,
/// so `SET a = :v, b = a` gives `b` the OLD value of `a`, not the value just
/// assigned in the same call.
#[test]
fn test_update_set_reads_pre_update_snapshot() {
    let db = make_db();
    create_table(&db, "Tbl");

    put(
        &db,
        "Tbl",
        &[
            ("pk", AttributeValue::S("k1".into())),
            ("a", AttributeValue::S("OLD".into())),
        ],
    );

    let resp = db
        .update_item(UpdateItemRequest {
            table_name: "Tbl".to_string(),
            key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
            update_expression: Some("SET a = :v, b = a".into()),
            expression_attribute_values: Some(make_item(&[(
                ":v",
                AttributeValue::S("NEW".into()),
            )])),
            return_values: Some("ALL_NEW".into()),
            ..Default::default()
        })
        .unwrap();

    let attrs = resp.attributes.unwrap();
    assert_eq!(attrs["a"], AttributeValue::S("NEW".into()));
    assert_eq!(attrs["b"], AttributeValue::S("OLD".into()));
}

/// #35(b): parenthesised arithmetic `SET c = (c - :v)` parses and applies on the
/// BigDecimal path, rather than being rejected as a syntax error.
#[test]
fn test_update_parenthesised_arithmetic() {
    let db = make_db();
    create_table(&db, "Tbl");

    put(
        &db,
        "Tbl",
        &[
            ("pk", AttributeValue::S("k1".into())),
            ("c", AttributeValue::N("10".into())),
        ],
    );

    let resp = db
        .update_item(UpdateItemRequest {
            table_name: "Tbl".to_string(),
            key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
            update_expression: Some("SET c = (c - :v)".into()),
            expression_attribute_values: Some(make_item(&[(":v", AttributeValue::N("3".into()))])),
            return_values: Some("ALL_NEW".into()),
            ..Default::default()
        })
        .unwrap();

    let attrs = resp.attributes.unwrap();
    assert_eq!(attrs["c"], AttributeValue::N("7".into()));
}

/// #36(a): `UPDATED_NEW` for a nested `SET parent.child = :v` returns only the
/// changed fragment `{parent: {M: {child}}}`, not the whole parent map.
#[test]
fn test_update_updated_new_nested_fragment() {
    let db = make_db();
    create_table(&db, "Tbl");

    let parent = AttributeValue::M(make_item(&[
        ("keep", AttributeValue::S("k".into())),
        ("child", AttributeValue::S("old".into())),
    ]));
    put(
        &db,
        "Tbl",
        &[("pk", AttributeValue::S("k1".into())), ("parent", parent)],
    );

    let resp = db
        .update_item(UpdateItemRequest {
            table_name: "Tbl".to_string(),
            key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
            update_expression: Some("SET parent.child = :v".into()),
            expression_attribute_values: Some(make_item(&[(
                ":v",
                AttributeValue::S("new".into()),
            )])),
            return_values: Some("UPDATED_NEW".into()),
            ..Default::default()
        })
        .unwrap();

    let attrs = resp.attributes.unwrap();
    match &attrs["parent"] {
        AttributeValue::M(map) => {
            assert_eq!(map.get("child"), Some(&AttributeValue::S("new".into())));
            assert!(
                !map.contains_key("keep"),
                "UPDATED_NEW must return only the changed fragment, not sibling keys: {map:?}"
            );
        }
        other => panic!("expected parent to be a map, got {other:?}"),
    }
}

/// #36(b): a REMOVE-only update with `UPDATED_NEW` omits `Attributes` entirely,
/// because nothing was set to a new value.
#[test]
fn test_update_updated_new_remove_omits_attributes() {
    let db = make_db();
    create_table(&db, "Tbl");

    put(
        &db,
        "Tbl",
        &[
            ("pk", AttributeValue::S("k1".into())),
            ("x", AttributeValue::S("keep".into())),
            ("y", AttributeValue::S("drop".into())),
        ],
    );

    let resp = db
        .update_item(UpdateItemRequest {
            table_name: "Tbl".to_string(),
            key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
            update_expression: Some("REMOVE y".into()),
            return_values: Some("UPDATED_NEW".into()),
            ..Default::default()
        })
        .unwrap();

    assert!(
        resp.attributes.is_none(),
        "REMOVE-only UPDATED_NEW must omit Attributes, got {:?}",
        resp.attributes
    );
}

/// #36 regression guard: a SET targeting a list index must not mislocate the
/// element or drop its siblings under UPDATED_NEW. `insert_at_path` rebuilds a
/// list from index 0, so indexed targets fall back to the whole list attribute.
#[test]
fn test_update_updated_new_list_index_keeps_siblings() {
    let db = make_db();
    create_table(&db, "Tbl");

    let list = AttributeValue::L(vec![
        AttributeValue::S("a".into()),
        AttributeValue::S("b".into()),
        AttributeValue::S("c".into()),
    ]);
    put(
        &db,
        "Tbl",
        &[("pk", AttributeValue::S("k1".into())), ("list", list)],
    );

    let resp = db
        .update_item(UpdateItemRequest {
            table_name: "Tbl".to_string(),
            key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
            update_expression: Some("SET #l[2] = :v".into()),
            expression_attribute_names: Some(HashMap::from([(
                "#l".to_string(),
                "list".to_string(),
            )])),
            expression_attribute_values: Some(make_item(&[(":v", AttributeValue::S("z".into()))])),
            return_values: Some("UPDATED_NEW".into()),
            ..Default::default()
        })
        .unwrap();

    let attrs = resp.attributes.unwrap();
    match &attrs["list"] {
        AttributeValue::L(items) => {
            // The full updated list comes back, element at index 2 replaced, the
            // preceding elements preserved in place. Never a length-1 list.
            assert_eq!(
                items,
                &vec![
                    AttributeValue::S("a".into()),
                    AttributeValue::S("b".into()),
                    AttributeValue::S("z".into()),
                ],
                "indexed SET must not collapse the list to its changed element"
            );
        }
        other => panic!("expected list to be a list, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// UpdateItem: cannot modify key attributes
// ---------------------------------------------------------------------------

#[test]
fn test_update_cannot_modify_key() {
    let db = make_db();
    create_table(&db, "Tbl");

    put(&db, "Tbl", &[("pk", AttributeValue::S("k1".into()))]);

    let err = db
        .update_item(UpdateItemRequest {
            table_name: "Tbl".to_string(),
            key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
            update_expression: Some("SET pk = :v".into()),
            condition_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: Some(make_item(&[(":v", AttributeValue::S("k2".into()))])),
            return_values: None,
            ..Default::default()
        })
        .unwrap_err();

    // Cannot-update-key is captured bare (eu-west-2): no request-validation
    // envelope.
    assert_eq!(
        err.to_string(),
        "One or more parameter values were invalid: Cannot update attribute pk. \
         This attribute is part of the key"
    );
}

// ---------------------------------------------------------------------------
// UpdateItem: with ConditionExpression
// ---------------------------------------------------------------------------

#[test]
fn test_update_with_condition() {
    let db = make_db();
    create_table(&db, "Tbl");

    put(
        &db,
        "Tbl",
        &[
            ("pk", AttributeValue::S("k1".into())),
            ("version", AttributeValue::N("1".into())),
        ],
    );

    // Should succeed: version = 1
    db.update_item(UpdateItemRequest {
        table_name: "Tbl".to_string(),
        key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
        update_expression: Some("SET version = :new".into()),
        condition_expression: Some("version = :old".into()),
        expression_attribute_names: None,
        expression_attribute_values: Some(make_item(&[
            (":old", AttributeValue::N("1".into())),
            (":new", AttributeValue::N("2".into())),
        ])),
        return_values: None,
        ..Default::default()
    })
    .unwrap();

    // Should fail: version is now 2
    let err = db
        .update_item(UpdateItemRequest {
            table_name: "Tbl".to_string(),
            key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
            update_expression: Some("SET version = :new".into()),
            condition_expression: Some("version = :old".into()),
            expression_attribute_names: None,
            expression_attribute_values: Some(make_item(&[
                (":old", AttributeValue::N("1".into())),
                (":new", AttributeValue::N("3".into())),
            ])),
            return_values: None,
            ..Default::default()
        })
        .unwrap_err();

    assert!(err.to_string().contains("conditional request failed"));
}

// ---------------------------------------------------------------------------
// Expression with ExpressionAttributeNames
// ---------------------------------------------------------------------------

#[test]
fn test_update_with_attribute_names() {
    let db = make_db();
    create_table(&db, "Tbl");

    put(
        &db,
        "Tbl",
        &[
            ("pk", AttributeValue::S("k1".into())),
            ("status", AttributeValue::S("active".into())),
        ],
    );

    db.update_item(UpdateItemRequest {
        table_name: "Tbl".to_string(),
        key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
        update_expression: Some("SET #s = :v".into()),
        condition_expression: None,
        expression_attribute_names: Some(HashMap::from([("#s".to_string(), "status".to_string())])),
        expression_attribute_values: Some(make_item(&[(
            ":v",
            AttributeValue::S("inactive".into()),
        )])),
        return_values: None,
        ..Default::default()
    })
    .unwrap();

    let resp = db
        .get_item(GetItemRequest {
            table_name: "Tbl".to_string(),
            key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
            consistent_read: None,
            projection_expression: None,
            ..Default::default()
        })
        .unwrap();

    assert_eq!(
        resp.item.unwrap()["status"],
        AttributeValue::S("inactive".into())
    );
}

// ---------------------------------------------------------------------------
// SET list index past the end: appends (eu-west-2, 9 October 2026)
// ---------------------------------------------------------------------------

/// Read back `myList` (or any list attribute) from the single test item.
fn read_list(db: &Database, attr: &str) -> Vec<AttributeValue> {
    let item = db
        .get_item(GetItemRequest {
            table_name: "Tbl".into(),
            key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
            ..Default::default()
        })
        .unwrap()
        .item
        .unwrap();
    match &item[attr] {
        AttributeValue::L(l) => l.clone(),
        other => panic!("expected list, got {:?}", other),
    }
}

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.into())
}

#[test]
fn a_set_past_the_end_of_a_list_appends() {
    // Captured against eu-west-2: SET through an index past the end of a list
    // adds the value at the end, whatever the index, rather than padding the
    // gap. `l[5]` on a one-element list gives a two-element list.
    let db = make_db();
    create_table(&db, "Tbl");
    put(
        &db,
        "Tbl",
        &[
            ("pk", s("k1")),
            ("myList", AttributeValue::L(vec![s("a"), s("b"), s("c")])),
        ],
    );

    db.update_item(UpdateItemRequest {
        table_name: "Tbl".into(),
        key: key_map(&[("pk", s("k1"))]),
        update_expression: Some("SET myList[5] = :val".into()),
        expression_attribute_values: Some(make_item(&[(":val", s("x"))])),
        ..Default::default()
    })
    .unwrap();

    assert_eq!(
        read_list(&db, "myList"),
        vec![s("a"), s("b"), s("c"), s("x")]
    );
}

#[test]
fn several_sets_past_the_end_append_in_index_order() {
    // Captured against eu-west-2: two writes past the end of the same list
    // append in the order of their indexes, not of their clauses, and an index
    // inside the list is still replaced in place.
    let db = make_db();
    create_table(&db, "Tbl");
    let cases: &[(&str, Vec<AttributeValue>)] = &[
        ("SET l[5] = :a, l[7] = :b", vec![s("e"), s("a"), s("b")]),
        ("SET l[7] = :a, l[5] = :b", vec![s("e"), s("b"), s("a")]),
        ("SET l[3] = :a, l[0] = :b", vec![s("b"), s("a")]),
    ];
    for (expression, expected) in cases {
        put(
            &db,
            "Tbl",
            &[("pk", s("k1")), ("l", AttributeValue::L(vec![s("e")]))],
        );
        db.update_item(UpdateItemRequest {
            table_name: "Tbl".into(),
            key: key_map(&[("pk", s("k1"))]),
            update_expression: Some(expression.to_string()),
            expression_attribute_values: Some(make_item(&[(":a", s("a")), (":b", s("b"))])),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(&read_list(&db, "l"), expected, "{expression}");
    }
}

#[test]
fn a_set_past_the_end_of_a_nested_list_appends() {
    // Captured against eu-west-2: the same holds for a list inside a map or
    // inside another list.
    let db = make_db();
    create_table(&db, "Tbl");
    put(
        &db,
        "Tbl",
        &[
            ("pk", s("k1")),
            (
                "m",
                AttributeValue::M(
                    [("l".to_string(), AttributeValue::L(vec![s("e")]))]
                        .into_iter()
                        .collect(),
                ),
            ),
            (
                "l",
                AttributeValue::L(vec![AttributeValue::L(vec![s("e")])]),
            ),
        ],
    );
    db.update_item(UpdateItemRequest {
        table_name: "Tbl".into(),
        key: key_map(&[("pk", s("k1"))]),
        update_expression: Some("SET m.l[3] = :v, l[0][3] = :v".into()),
        expression_attribute_values: Some(make_item(&[(":v", s("v"))])),
        ..Default::default()
    })
    .unwrap();

    let item = db
        .get_item(GetItemRequest {
            table_name: "Tbl".into(),
            key: key_map(&[("pk", s("k1"))]),
            ..Default::default()
        })
        .unwrap()
        .item
        .unwrap();
    let appended = AttributeValue::L(vec![s("e"), s("v")]);
    match &item["m"] {
        AttributeValue::M(m) => assert_eq!(m["l"], appended),
        other => panic!("expected a map, got {other:?}"),
    }
    assert_eq!(item["l"], AttributeValue::L(vec![appended]));
}

#[test]
fn a_transacted_set_past_the_end_of_a_list_appends() {
    // Captured against eu-west-2: a TransactWriteItems Update appends too.
    let db = make_db();
    create_table(&db, "Tbl");
    put(
        &db,
        "Tbl",
        &[("pk", s("k1")), ("l", AttributeValue::L(vec![s("e")]))],
    );
    db.transact_write_items(
        dynoxide::actions::transact_write_items::TransactWriteItemsRequest {
            transact_items: vec![dynoxide::actions::transact_write_items::TransactWriteItem {
                update: Some(dynoxide::actions::transact_write_items::TransactUpdate {
                    table_name: "Tbl".into(),
                    key: key_map(&[("pk", s("k1"))]),
                    update_expression: "SET l[5] = :v".into(),
                    expression_attribute_values: Some(make_item(&[(":v", s("v"))])),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(read_list(&db, "l"), vec![s("e"), s("v")]);
}

#[test]
fn test_set_list_index_0_on_empty_list() {
    let db = make_db();
    create_table(&db, "Tbl");

    put(
        &db,
        "Tbl",
        &[
            ("pk", AttributeValue::S("k1".into())),
            ("myList", AttributeValue::L(vec![])),
        ],
    );

    db.update_item(UpdateItemRequest {
        table_name: "Tbl".into(),
        key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
        update_expression: Some("SET myList[0] = :val".into()),
        expression_attribute_values: Some(make_item(&[(
            ":val",
            AttributeValue::S("first".into()),
        )])),
        ..Default::default()
    })
    .unwrap();

    let resp = db
        .get_item(GetItemRequest {
            table_name: "Tbl".into(),
            key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
            ..Default::default()
        })
        .unwrap();

    let item = resp.item.unwrap();
    let list = match &item["myList"] {
        AttributeValue::L(l) => l,
        other => panic!("expected list, got {:?}", other),
    };

    assert_eq!(list.len(), 1);
    assert_eq!(list[0], AttributeValue::S("first".into()));
}

#[test]
fn a_nested_set_through_an_index_past_the_end_is_refused() {
    // Captured against eu-west-2: a path that steps through an element past the
    // end of a list cannot be written, and the item is left as it was.
    let db = make_db();
    create_table(&db, "Tbl");
    let zero = AttributeValue::M([("name".to_string(), s("zero"))].into_iter().collect());
    put(
        &db,
        "Tbl",
        &[
            ("pk", s("k1")),
            ("myList", AttributeValue::L(vec![zero.clone()])),
        ],
    );

    let err = db
        .update_item(UpdateItemRequest {
            table_name: "Tbl".into(),
            key: key_map(&[("pk", s("k1"))]),
            update_expression: Some("SET myList[2].#n = :val".into()),
            expression_attribute_names: Some(
                [("#n".to_string(), "name".to_string())]
                    .into_iter()
                    .collect(),
            ),
            expression_attribute_values: Some(make_item(&[(":val", s("two"))])),
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "The document path provided in the update expression is invalid for update"
    );
    assert_eq!(read_list(&db, "myList"), vec![zero]);
}

#[test]
fn a_set_through_a_stored_null_is_refused() {
    // Captured against eu-west-2: a NULL is not a map or a list, so a path that
    // steps through one is refused rather than turning it into either.
    let db = make_db();
    create_table(&db, "Tbl");
    let null = AttributeValue::NULL(true);
    let cases: &[(&str, Vec<(&str, AttributeValue)>)] = &[
        (
            "SET l[0].x = :v",
            vec![("l", AttributeValue::L(vec![null.clone()]))],
        ),
        (
            "SET l[0][0] = :v",
            vec![("l", AttributeValue::L(vec![null.clone()]))],
        ),
        (
            "SET m.n.x = :v",
            vec![(
                "m",
                AttributeValue::M([("n".to_string(), null.clone())].into_iter().collect()),
            )],
        ),
        ("SET n.x = :v", vec![("n", null.clone())]),
    ];
    for (expression, seed) in cases {
        let mut attrs = vec![("pk", s("k1"))];
        attrs.extend(seed.iter().cloned());
        put(&db, "Tbl", &attrs);
        let err = db
            .update_item(UpdateItemRequest {
                table_name: "Tbl".into(),
                key: key_map(&[("pk", s("k1"))]),
                update_expression: Some(expression.to_string()),
                expression_attribute_values: Some(make_item(&[(":v", s("v"))])),
                ..Default::default()
            })
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "The document path provided in the update expression is invalid for update",
            "{expression}"
        );
    }
}

#[test]
fn test_remove_list_index_out_of_bounds_is_noop() {
    let db = make_db();
    create_table(&db, "Tbl");

    put(
        &db,
        "Tbl",
        &[
            ("pk", AttributeValue::S("k1".into())),
            (
                "myList",
                AttributeValue::L(vec![
                    AttributeValue::S("a".into()),
                    AttributeValue::S("b".into()),
                ]),
            ),
        ],
    );

    // REMOVE myList[999]: should be a no-op
    db.update_item(UpdateItemRequest {
        table_name: "Tbl".into(),
        key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
        update_expression: Some("REMOVE myList[999]".into()),
        ..Default::default()
    })
    .unwrap();

    let resp = db
        .get_item(GetItemRequest {
            table_name: "Tbl".into(),
            key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
            ..Default::default()
        })
        .unwrap();

    let item = resp.item.unwrap();
    let list = match &item["myList"] {
        AttributeValue::L(l) => l,
        other => panic!("expected list, got {:?}", other),
    };

    assert_eq!(list.len(), 2);
    assert_eq!(list[0], AttributeValue::S("a".into()));
    assert_eq!(list[1], AttributeValue::S("b".into()));
}

// ---------------------------------------------------------------------------
// ProjectionExpression with list index
// ---------------------------------------------------------------------------

#[test]
fn test_projection_with_list_index() {
    let db = make_db();
    create_table(&db, "Tbl");

    put(
        &db,
        "Tbl",
        &[
            ("pk", AttributeValue::S("k1".into())),
            (
                "myList",
                AttributeValue::L(vec![
                    AttributeValue::S("a".into()),
                    AttributeValue::S("b".into()),
                    AttributeValue::S("c".into()),
                ]),
            ),
        ],
    );

    let resp = db
        .get_item(GetItemRequest {
            table_name: "Tbl".into(),
            key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
            projection_expression: Some("myList[0]".into()),
            ..Default::default()
        })
        .unwrap();

    let item = resp.item.unwrap();
    // myList should be a List containing just the projected element
    let list = match &item["myList"] {
        AttributeValue::L(l) => l,
        other => panic!("expected list, got {:?}", other),
    };
    assert_eq!(list.len(), 1);
    assert_eq!(list[0], AttributeValue::S("a".into()));
}

#[test]
fn test_projection_with_nested_path_and_list_index() {
    let db = make_db();
    create_table(&db, "Tbl");

    let nested_list = AttributeValue::L(vec![
        AttributeValue::M(
            [("label".to_string(), AttributeValue::S("first".into()))]
                .into_iter()
                .collect(),
        ),
        AttributeValue::M(
            [("label".to_string(), AttributeValue::S("second".into()))]
                .into_iter()
                .collect(),
        ),
    ]);

    put(
        &db,
        "Tbl",
        &[
            ("pk", AttributeValue::S("k1".into())),
            (
                "payload",
                AttributeValue::M([("entries".to_string(), nested_list)].into_iter().collect()),
            ),
        ],
    );

    let resp = db
        .get_item(GetItemRequest {
            table_name: "Tbl".into(),
            key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
            projection_expression: Some("payload.entries[0].label".into()),
            ..Default::default()
        })
        .unwrap();

    let item = resp.item.unwrap();
    // payload.entries[0].label should produce: payload -> M { entries -> L [ M { label -> "first" } ] }
    let payload = match &item["payload"] {
        AttributeValue::M(m) => m,
        other => panic!("expected map for payload, got {:?}", other),
    };
    let entries = match &payload["entries"] {
        AttributeValue::L(l) => l,
        other => panic!("expected list for entries, got {:?}", other),
    };
    assert_eq!(entries.len(), 1);
    let first = match &entries[0] {
        AttributeValue::M(m) => m,
        other => panic!("expected map at entries[0], got {:?}", other),
    };
    assert_eq!(first["label"], AttributeValue::S("first".into()));
}

// ---------------------------------------------------------------------------
// #126: projected sub-attributes of one list index merge into one element
// ---------------------------------------------------------------------------

#[test]
fn test_projection_same_list_index_merges() {
    // Two projected paths sharing a list index return a single merged element,
    // matching real DynamoDB (pinned in the parity suite).
    let db = make_db();
    create_table(&db, "Tbl");

    let elem0 = AttributeValue::M(
        [
            ("a".to_string(), AttributeValue::S("a0".into())),
            ("b".to_string(), AttributeValue::S("b0".into())),
            ("c".to_string(), AttributeValue::S("c0".into())),
        ]
        .into_iter()
        .collect(),
    );

    put(
        &db,
        "Tbl",
        &[
            ("pk", AttributeValue::S("k1".into())),
            ("l", AttributeValue::L(vec![elem0])),
        ],
    );

    let resp = db
        .get_item(GetItemRequest {
            table_name: "Tbl".into(),
            key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
            projection_expression: Some("#l[0].#a, #l[0].#b".into()),
            expression_attribute_names: Some(HashMap::from([
                ("#l".to_string(), "l".to_string()),
                ("#a".to_string(), "a".to_string()),
                ("#b".to_string(), "b".to_string()),
            ])),
            ..Default::default()
        })
        .unwrap();

    let item = resp.item.unwrap();
    let list = match &item["l"] {
        AttributeValue::L(l) => l,
        other => panic!("expected list, got {:?}", other),
    };
    assert_eq!(list.len(), 1, "same index must merge into one element");
    let m = match &list[0] {
        AttributeValue::M(m) => m,
        other => panic!("expected map, got {:?}", other),
    };
    assert_eq!(m["a"], AttributeValue::S("a0".into()));
    assert_eq!(m["b"], AttributeValue::S("b0".into()));
    assert!(!m.contains_key("c"));
}

#[test]
fn test_projection_same_list_index_mixed_compacts() {
    // Merge on the shared index, keep a distinct index separate, and compact to
    // ascending order: `l[0].a, l[0].b, l[2].c` -> [ {a,b}, {c} ].
    let db = make_db();
    create_table(&db, "Tbl");

    let elem = |n: &str| {
        AttributeValue::M(
            [
                ("a".to_string(), AttributeValue::S(format!("a{n}"))),
                ("b".to_string(), AttributeValue::S(format!("b{n}"))),
                ("c".to_string(), AttributeValue::S(format!("c{n}"))),
            ]
            .into_iter()
            .collect(),
        )
    };

    put(
        &db,
        "Tbl",
        &[
            ("pk", AttributeValue::S("k1".into())),
            (
                "l",
                AttributeValue::L(vec![elem("0"), elem("1"), elem("2")]),
            ),
        ],
    );

    let resp = db
        .get_item(GetItemRequest {
            table_name: "Tbl".into(),
            key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
            projection_expression: Some("#l[0].#a, #l[0].#b, #l[2].#c".into()),
            expression_attribute_names: Some(HashMap::from([
                ("#l".to_string(), "l".to_string()),
                ("#a".to_string(), "a".to_string()),
                ("#b".to_string(), "b".to_string()),
                ("#c".to_string(), "c".to_string()),
            ])),
            ..Default::default()
        })
        .unwrap();

    let item = resp.item.unwrap();
    let list = match &item["l"] {
        AttributeValue::L(l) => l,
        other => panic!("expected list, got {:?}", other),
    };
    assert_eq!(list.len(), 2);
    let e0 = match &list[0] {
        AttributeValue::M(m) => m,
        other => panic!("expected map, got {:?}", other),
    };
    assert_eq!(e0["a"], AttributeValue::S("a0".into()));
    assert_eq!(e0["b"], AttributeValue::S("b0".into()));
    let e1 = match &list[1] {
        AttributeValue::M(m) => m,
        other => panic!("expected map, got {:?}", other),
    };
    assert_eq!(e1["c"], AttributeValue::S("c2".into()));
    assert!(!e1.contains_key("a"));
}

// ---------------------------------------------------------------------------
// SET intermediate map path: auto-create missing intermediate maps
// ---------------------------------------------------------------------------

#[test]
fn test_set_rejects_missing_intermediate_path() {
    let db = make_db();
    create_table(&db, "Tbl");

    put(&db, "Tbl", &[("pk", AttributeValue::S("k1".into()))]);

    // SET a.b.c = :val where "a" doesn't exist - should fail
    let err = db
        .update_item(UpdateItemRequest {
            table_name: "Tbl".into(),
            key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
            update_expression: Some("SET a.b.c = :val".into()),
            expression_attribute_values: Some(make_item(&[(
                ":val",
                AttributeValue::S("deep".into()),
            )])),
            ..Default::default()
        })
        .unwrap_err();

    assert!(
        err.to_string()
            .contains("document path provided in the update expression is invalid"),
        "expected invalid path error, got: {}",
        err
    );
    // Document-path-invalid is an apply-time error, captured bare (eu-west-2).
    assert!(
        !err.to_string().contains("validation error detected"),
        "apply-time error must stay bare, got: {err}"
    );
}

#[test]
fn test_set_adds_key_to_existing_map() {
    let db = make_db();
    create_table(&db, "Tbl");

    // Item with an existing map
    put(
        &db,
        "Tbl",
        &[
            ("pk", AttributeValue::S("k1".into())),
            (
                "mymap",
                AttributeValue::M(
                    [("existing".to_string(), AttributeValue::S("val".into()))]
                        .into_iter()
                        .collect(),
                ),
            ),
        ],
    );

    // SET mymap.newKey = :val - should succeed (adding key to existing map)
    db.update_item(UpdateItemRequest {
        table_name: "Tbl".into(),
        key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
        update_expression: Some("SET mymap.newKey = :val".into()),
        expression_attribute_values: Some(make_item(&[(
            ":val",
            AttributeValue::S("added".into()),
        )])),
        ..Default::default()
    })
    .unwrap();

    let resp = db
        .get_item(GetItemRequest {
            table_name: "Tbl".into(),
            key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
            ..Default::default()
        })
        .unwrap();

    let item = resp.item.unwrap();
    let mymap = match &item["mymap"] {
        AttributeValue::M(m) => m,
        other => panic!("expected map, got {:?}", other),
    };
    assert_eq!(mymap["existing"], AttributeValue::S("val".into()));
    assert_eq!(mymap["newKey"], AttributeValue::S("added".into()));
}

#[test]
fn test_set_rejects_deep_missing_intermediate() {
    let db = make_db();
    create_table(&db, "Tbl");

    // Item with a map, but parentMap.absent doesn't exist
    put(
        &db,
        "Tbl",
        &[
            ("pk", AttributeValue::S("k1".into())),
            (
                "parentMap",
                AttributeValue::M(
                    [("present".to_string(), AttributeValue::S("val".into()))]
                        .into_iter()
                        .collect(),
                ),
            ),
        ],
    );

    // SET parentMap.absent.deep = :val - should fail because parentMap.absent doesn't exist
    let err = db
        .update_item(UpdateItemRequest {
            table_name: "Tbl".into(),
            key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
            update_expression: Some("SET parentMap.absent.deep = :val".into()),
            expression_attribute_values: Some(make_item(&[(
                ":val",
                AttributeValue::S("nope".into()),
            )])),
            ..Default::default()
        })
        .unwrap_err();

    assert!(
        err.to_string()
            .contains("document path provided in the update expression is invalid"),
        "expected invalid path error, got: {}",
        err
    );
}

// ---------------------------------------------------------------------------
// Expression size limit (4096 bytes) - rejected on each expression surface.
// The message ends at `size;` with no figure, on every surface captured in
// us-east-1 and eu-west-2 on 2026-10-09, except a Scan FilterExpression and a
// transaction's ConditionExpression, where us-east-1 still appends
// `expression size: <n>` and eu-west-2 doesn't.
// ---------------------------------------------------------------------------

// Each expression surface prefixes the size error with `Invalid <Type>Expression:`,
// matching real DynamoDB (eu-west-2).
const UPDATE_OVERSIZE_MSG: &str =
    "Invalid UpdateExpression: Expression size has exceeded the maximum allowed size";
const CONDITION_OVERSIZE_MSG: &str =
    "Invalid ConditionExpression: Expression size has exceeded the maximum allowed size";
const PROJECTION_OVERSIZE_MSG: &str =
    "Invalid ProjectionExpression: Expression size has exceeded the maximum allowed size";

#[test]
fn test_update_expression_over_size_limit_rejected() {
    let db = make_db();
    create_table(&db, "Tbl");
    put(&db, "Tbl", &[("pk", AttributeValue::S("k1".into()))]);

    // 4097-byte UpdateExpression: one byte past the 4096 limit.
    let expr = format!("SET {} = :v", "a".repeat(4088));
    assert_eq!(expr.len(), 4097);

    let err = db
        .update_item(UpdateItemRequest {
            table_name: "Tbl".to_string(),
            key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
            update_expression: Some(expr),
            expression_attribute_values: Some(make_item(&[(":v", AttributeValue::S("x".into()))])),
            ..Default::default()
        })
        .unwrap_err();

    // UpdateItem wraps UpdateExpression parse-time errors in the
    // request-validation envelope.
    assert_eq!(
        err.to_string(),
        format!("1 validation error detected: {UPDATE_OVERSIZE_MSG};")
    );
}

#[test]
fn test_condition_expression_over_size_limit_rejected() {
    let db = make_db();
    create_table(&db, "Tbl");

    // 4097-byte ConditionExpression.
    let expr = format!("{} = :v", "a".repeat(4092));
    assert_eq!(expr.len(), 4097);

    let err = db
        .put_item(PutItemRequest {
            table_name: "Tbl".to_string(),
            item: make_item(&[("pk", AttributeValue::S("k1".into()))]),
            condition_expression: Some(expr),
            expression_attribute_values: Some(make_item(&[(":v", AttributeValue::S("x".into()))])),
            ..Default::default()
        })
        .unwrap_err();

    // PutItem wraps ConditionExpression parse-time errors in the
    // request-validation envelope.
    assert_eq!(
        err.to_string(),
        format!("1 validation error detected: {CONDITION_OVERSIZE_MSG};")
    );
}

#[test]
fn test_projection_expression_over_size_limit_rejected() {
    let db = make_db();
    create_table(&db, "Tbl");
    put(&db, "Tbl", &[("pk", AttributeValue::S("k1".into()))]);

    // 4097-byte ProjectionExpression (a single oversized attribute path).
    let expr = "a".repeat(4097);

    let err = db
        .get_item(GetItemRequest {
            table_name: "Tbl".to_string(),
            key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
            consistent_read: None,
            projection_expression: Some(expr),
            ..Default::default()
        })
        .unwrap_err();

    assert_eq!(err.to_string(), format!("{PROJECTION_OVERSIZE_MSG};"));
}

#[test]
fn test_expression_size_message_on_the_other_surfaces() {
    let db = make_db();
    create_table(&db, "Tbl");
    put(&db, "Tbl", &[("pk", AttributeValue::S("k1".into()))]);
    let over = format!("{} = :v", "a".repeat(4092));
    assert_eq!(over.len(), 4097);
    let values = Some(make_item(&[
        (":v", AttributeValue::S("x".into())),
        (":p", AttributeValue::S("k1".into())),
    ]));

    // DeleteItem: enveloped, no figure.
    let err = db
        .delete_item(DeleteItemRequest {
            table_name: "Tbl".to_string(),
            key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
            condition_expression: Some(over.clone()),
            expression_attribute_values: Some(make_item(&[(":v", AttributeValue::S("x".into()))])),
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        format!("1 validation error detected: {CONDITION_OVERSIZE_MSG};")
    );

    // UpdateItem's ConditionExpression: enveloped, no figure.
    let err = db
        .update_item(UpdateItemRequest {
            table_name: "Tbl".to_string(),
            key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
            update_expression: Some("SET b = :v".into()),
            condition_expression: Some(over.clone()),
            expression_attribute_values: Some(make_item(&[(":v", AttributeValue::S("x".into()))])),
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        format!("1 validation error detected: {CONDITION_OVERSIZE_MSG};")
    );

    // Query: KeyConditionExpression and FilterExpression, bare, no figure.
    let err = db
        .query(
            serde_json::from_value(serde_json::json!({
                "TableName": "Tbl",
                "KeyConditionExpression": format!("pk = :p AND {}", "a".repeat(4085)),
                "ExpressionAttributeValues": {":p": {"S": "k1"}},
            }))
            .unwrap(),
        )
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "Invalid KeyConditionExpression: Expression size has exceeded the maximum allowed size;"
    );
    let err = db
        .query(
            serde_json::from_value(serde_json::json!({
                "TableName": "Tbl",
                "KeyConditionExpression": "pk = :p",
                "FilterExpression": over,
                "ExpressionAttributeValues": values,
            }))
            .unwrap(),
        )
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "Invalid FilterExpression: Expression size has exceeded the maximum allowed size;"
    );

    // Scan's FilterExpression keeps the figure: us-east-1 sends it, eu-west-2
    // doesn't, and Dynoxide follows us-east-1.
    let err = db
        .scan(
            serde_json::from_value(serde_json::json!({
                "TableName": "Tbl",
                "FilterExpression": format!("{} = :v", "a".repeat(4092)),
                "ExpressionAttributeValues": {":v": {"S": "x"}},
            }))
            .unwrap(),
        )
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "Invalid FilterExpression: Expression size has exceeded the maximum allowed size; \
         expression size: 4097"
    );

    // BatchGetItem and TransactGetItems projections: bare, no figure.
    let projection = "a".repeat(4097);
    let err = db
        .batch_get_item(
            serde_json::from_value(serde_json::json!({
                "RequestItems": {"Tbl": {"Keys": [{"pk": {"S": "k1"}}],
                    "ProjectionExpression": projection}}
            }))
            .unwrap(),
        )
        .unwrap_err();
    assert_eq!(err.to_string(), format!("{PROJECTION_OVERSIZE_MSG};"));
    let err = db
        .transact_get_items(
            serde_json::from_value(serde_json::json!({
                "TransactItems": [{"Get": {"TableName": "Tbl", "Key": {"pk": {"S": "k1"}},
                    "ProjectionExpression": projection}}]
            }))
            .unwrap(),
        )
        .unwrap_err();
    assert_eq!(err.to_string(), format!("{PROJECTION_OVERSIZE_MSG};"));
}

#[test]
fn test_expression_at_size_limit_not_rejected_for_size() {
    // A 4096-byte expression is within the limit: it may fail to parse for
    // other reasons, but never with the size-limit message.
    let db = make_db();
    create_table(&db, "Tbl");
    put(&db, "Tbl", &[("pk", AttributeValue::S("k1".into()))]);

    let expr = "a".repeat(4096);

    let result = db.get_item(GetItemRequest {
        table_name: "Tbl".to_string(),
        key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
        consistent_read: None,
        projection_expression: Some(expr),
        ..Default::default()
    });

    // A 4096-byte projection of a single (absent) attribute is within the limit
    // and parses cleanly, so the call succeeds and never trips the size guard.
    assert!(
        result.is_ok(),
        "4096-byte expression should be within the limit, got: {:?}",
        result.err()
    );
}

// ---------------------------------------------------------------------------
// PutItem request-validation envelope.
// PutItem wraps its action-time request-validation families in the
// `1 validation error detected: ` envelope; families the capture shows bare
// stay bare. Confirmed against real DynamoDB (eu-west-2).
// ---------------------------------------------------------------------------

#[test]
fn test_put_condition_syntax_error_enveloped() {
    let db = make_db();
    create_table(&db, "Tbl");

    let err = db
        .put_item(PutItemRequest {
            table_name: "Tbl".to_string(),
            item: make_item(&[("pk", AttributeValue::S("k1".into()))]),
            condition_expression: Some("pk = !!".into()),
            expression_attribute_values: Some(make_item(&[(":v", AttributeValue::S("x".into()))])),
            ..Default::default()
        })
        .unwrap_err();

    assert_eq!(
        err.to_string(),
        r#"1 validation error detected: Invalid ConditionExpression: Syntax error; token: "!", near: "= !!""#
    );
}

#[test]
fn test_put_condition_redundant_parentheses_enveloped() {
    let db = make_db();
    create_table(&db, "Tbl");

    let err = db
        .put_item(PutItemRequest {
            table_name: "Tbl".to_string(),
            item: make_item(&[("pk", AttributeValue::S("k1".into()))]),
            condition_expression: Some("((pk = :v))".into()),
            expression_attribute_values: Some(make_item(&[(":v", AttributeValue::S("x".into()))])),
            ..Default::default()
        })
        .unwrap_err();

    assert_eq!(
        err.to_string(),
        "1 validation error detected: Invalid ConditionExpression: \
         The expression has redundant parentheses;"
    );
}

#[test]
fn test_put_condition_contains_distinct_operand_enveloped() {
    let db = make_db();
    create_table(&db, "Tbl");

    let err = db
        .put_item(PutItemRequest {
            table_name: "Tbl".to_string(),
            item: make_item(&[("pk", AttributeValue::S("k1".into()))]),
            condition_expression: Some("contains(pk, pk)".into()),
            ..Default::default()
        })
        .unwrap_err();

    assert_eq!(
        err.to_string(),
        "1 validation error detected: Invalid ConditionExpression: \
         The first operand must be distinct from the remaining operands for this operator or \
         function; operator: contains, first operand: [pk]"
    );
}

#[test]
fn test_put_eav_without_expression_enveloped() {
    let db = make_db();
    create_table(&db, "Tbl");

    let err = db
        .put_item(PutItemRequest {
            table_name: "Tbl".to_string(),
            item: make_item(&[("pk", AttributeValue::S("k1".into()))]),
            expression_attribute_values: Some(make_item(&[(":v", AttributeValue::S("x".into()))])),
            ..Default::default()
        })
        .unwrap_err();

    // No expression is named after the message (eu-west-2 and us-east-1,
    // 2026-10-09).
    assert_eq!(
        err.to_string(),
        "1 validation error detected: ExpressionAttributeValues can only be specified when \
         using expressions"
    );
}

#[test]
fn test_put_mixing_expected_and_condition_expression_enveloped() {
    let db = make_db();
    create_table(&db, "Tbl");

    let err = db
        .put_item(PutItemRequest {
            table_name: "Tbl".to_string(),
            item: make_item(&[("pk", AttributeValue::S("k1".into()))]),
            condition_expression: Some("attribute_not_exists(pk)".into()),
            expected: Some(serde_json::json!({"pk": {"Exists": false}})),
            ..Default::default()
        })
        .unwrap_err();

    assert_eq!(
        err.to_string(),
        "1 validation error detected: Can not use both expression and non-expression \
         parameters in the same request: Non-expression parameters: {Expected} \
         Expression parameters: {ConditionExpression}"
    );
}

#[test]
fn test_put_ean_without_expression_enveloped() {
    // ExpressionAttributeNames without an expression comes back in the
    // envelope (eu-west-2 and us-east-1, 2026-10-09).
    let db = make_db();
    create_table(&db, "Tbl");

    let err = db
        .put_item(PutItemRequest {
            table_name: "Tbl".to_string(),
            item: make_item(&[("pk", AttributeValue::S("k1".into()))]),
            expression_attribute_names: Some(
                [("#a".to_string(), "a".to_string())].into_iter().collect(),
            ),
            ..Default::default()
        })
        .unwrap_err();

    assert_eq!(
        err.to_string(),
        "1 validation error detected: ExpressionAttributeNames can only be specified when \
         using expressions"
    );
}

// ---------------------------------------------------------------------------
// UpdateItem request-validation envelope.
// UpdateItem wraps its action-time request-validation families in the
// `1 validation error detected: ` envelope; families the capture shows bare
// stay bare. Confirmed against real DynamoDB (eu-west-2).
// ---------------------------------------------------------------------------

#[test]
fn test_update_expression_syntax_error_enveloped() {
    let db = make_db();
    create_table(&db, "Tbl");

    let err = db
        .update_item(UpdateItemRequest {
            table_name: "Tbl".to_string(),
            key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
            update_expression: Some("SET".into()),
            ..Default::default()
        })
        .unwrap_err();

    assert_eq!(
        err.to_string(),
        r#"1 validation error detected: Invalid UpdateExpression: Syntax error; token: "<EOF>", near: "SET""#
    );
}

#[test]
fn test_update_condition_syntax_error_enveloped() {
    let db = make_db();
    create_table(&db, "Tbl");

    let err = db
        .update_item(UpdateItemRequest {
            table_name: "Tbl".to_string(),
            key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
            condition_expression: Some("pk = !!".into()),
            ..Default::default()
        })
        .unwrap_err();

    assert_eq!(
        err.to_string(),
        r#"1 validation error detected: Invalid ConditionExpression: Syntax error; token: "!", near: "= !!""#
    );
}

#[test]
fn test_update_eav_null_false_enveloped() {
    // {NULL: false} in ExpressionAttributeValues is rejected with the bare
    // inner message inside the envelope, not the per-key "contains invalid
    // value" wrapper (eu-west-2).
    let db = make_db();
    create_table(&db, "Tbl");

    let err = db
        .update_item(UpdateItemRequest {
            table_name: "Tbl".to_string(),
            key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
            update_expression: Some("SET flag = :n".into()),
            expression_attribute_values: Some(make_item(&[(":n", AttributeValue::NULL(false))])),
            ..Default::default()
        })
        .unwrap_err();

    assert_eq!(
        err.to_string(),
        "1 validation error detected: One or more parameter values were invalid: \
         Null attribute value types must have the value of true"
    );
}

#[test]
fn test_update_key_null_false_enveloped() {
    let db = make_db();
    create_table(&db, "Tbl");

    let err = db
        .update_item(UpdateItemRequest {
            table_name: "Tbl".to_string(),
            key: key_map(&[("pk", AttributeValue::NULL(false))]),
            update_expression: Some("SET val = :v".into()),
            expression_attribute_values: Some(make_item(&[(":v", AttributeValue::S("x".into()))])),
            ..Default::default()
        })
        .unwrap_err();

    assert_eq!(
        err.to_string(),
        "1 validation error detected: One or more parameter values were invalid: \
         Null attribute value types must have the value of true"
    );
}

#[test]
fn test_update_eav_empty_set_enveloped() {
    // Empty sets in ExpressionAttributeValues carry the per-key wrapper inside
    // the envelope, matching PutItem's EAV behaviour.
    let db = make_db();
    create_table(&db, "Tbl");

    let err = db
        .update_item(UpdateItemRequest {
            table_name: "Tbl".to_string(),
            key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
            update_expression: Some("SET tags = :v".into()),
            expression_attribute_values: Some(make_item(&[(":v", AttributeValue::SS(vec![]))])),
            ..Default::default()
        })
        .unwrap_err();

    assert_eq!(
        err.to_string(),
        "1 validation error detected: ExpressionAttributeValues contains invalid value: \
         One or more parameter values were invalid: An string set  may not be empty for key :v"
    );
}

#[test]
fn test_update_eav_without_expression_enveloped() {
    let db = make_db();
    create_table(&db, "Tbl");

    let err = db
        .update_item(UpdateItemRequest {
            table_name: "Tbl".to_string(),
            key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
            expression_attribute_values: Some(make_item(&[(":v", AttributeValue::S("x".into()))])),
            ..Default::default()
        })
        .unwrap_err();

    // No expression is named after the message (eu-west-2 and us-east-1,
    // 2026-10-09).
    assert_eq!(
        err.to_string(),
        "1 validation error detected: ExpressionAttributeValues can only be specified when \
         using expressions"
    );
}

#[test]
fn test_update_key_empty_set_stays_bare() {
    // Empty sets in the Key map are not a captured enveloped family and keep
    // the bare message.
    let db = make_db();
    create_table(&db, "Tbl");

    let err = db
        .update_item(UpdateItemRequest {
            table_name: "Tbl".to_string(),
            key: key_map(&[("pk", AttributeValue::SS(vec![]))]),
            update_expression: Some("SET val = :v".into()),
            expression_attribute_values: Some(make_item(&[(":v", AttributeValue::S("x".into()))])),
            ..Default::default()
        })
        .unwrap_err();

    assert_eq!(
        err.to_string(),
        "One or more parameter values were invalid: An string set  may not be empty"
    );
}

#[test]
fn test_update_refers_to_nonexistent_attribute_stays_bare() {
    // Apply-time expression errors are captured bare.
    let db = make_db();
    create_table(&db, "Tbl");
    put(&db, "Tbl", &[("pk", AttributeValue::S("k1".into()))]);

    let err = db
        .update_item(UpdateItemRequest {
            table_name: "Tbl".to_string(),
            key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
            update_expression: Some("SET a = b".into()),
            ..Default::default()
        })
        .unwrap_err();

    assert_eq!(
        err.to_string(),
        "The provided expression refers to an attribute that does not exist in the item"
    );
}

#[test]
fn test_update_oversized_item_stays_bare() {
    // Item-size-exceeded on the post-update item is captured bare.
    let db = make_db();
    create_table(&db, "Tbl");
    put(&db, "Tbl", &[("pk", AttributeValue::S("k1".into()))]);

    let err = db
        .update_item(UpdateItemRequest {
            table_name: "Tbl".to_string(),
            key: key_map(&[("pk", AttributeValue::S("k1".into()))]),
            update_expression: Some("SET bigval = :v".into()),
            expression_attribute_values: Some(make_item(&[(
                ":v",
                AttributeValue::S("x".repeat(400 * 1024)),
            )])),
            ..Default::default()
        })
        .unwrap_err();

    assert_eq!(
        err.to_string(),
        "Item size to update has exceeded the maximum allowed size"
    );
}

// ---------------------------------------------------------------------------
// UpdateItem: ReturnValues when the update creates the item. Captured
// against eu-west-2 and us-east-1 on 5 November 2026: the old image of an
// item that didn't exist is empty, so ALL_OLD and UPDATED_OLD return no
// Attributes, whatever the update expression.
// ---------------------------------------------------------------------------

fn upsert_returning(
    db: &Database,
    update_expression: Option<&str>,
    with_value: bool,
    return_values: &str,
) -> Option<HashMap<String, AttributeValue>> {
    db.update_item(UpdateItemRequest {
        table_name: "Tbl".to_string(),
        key: key_map(&[("pk", AttributeValue::S("missing".into()))]),
        update_expression: update_expression.map(str::to_string),
        expression_attribute_values: with_value
            .then(|| make_item(&[(":v", AttributeValue::S("new".into()))])),
        return_values: Some(return_values.to_string()),
        ..Default::default()
    })
    .unwrap()
    .attributes
}

#[test]
fn all_old_on_an_upsert_with_set_returns_no_attributes() {
    let db = make_db();
    create_table(&db, "Tbl");
    assert_eq!(
        upsert_returning(&db, Some("SET a = :v"), true, "ALL_OLD"),
        None
    );
}

#[test]
fn all_old_on_an_upsert_with_remove_returns_no_attributes() {
    let db = make_db();
    create_table(&db, "Tbl");
    assert_eq!(
        upsert_returning(&db, Some("REMOVE a"), false, "ALL_OLD"),
        None
    );
}

#[test]
fn all_old_on_an_upsert_with_no_update_expression_returns_no_attributes() {
    let db = make_db();
    create_table(&db, "Tbl");
    assert_eq!(upsert_returning(&db, None, false, "ALL_OLD"), None);
}

#[test]
fn updated_old_on_an_upsert_returns_no_attributes() {
    let db = make_db();
    create_table(&db, "Tbl");
    assert_eq!(
        upsert_returning(&db, Some("SET a = :v"), true, "UPDATED_OLD"),
        None
    );
}

#[test]
fn all_new_and_updated_new_on_an_upsert_return_the_new_image() {
    let db = make_db();
    create_table(&db, "Tbl");
    assert_eq!(
        upsert_returning(&db, Some("SET a = :v"), true, "ALL_NEW"),
        Some(make_item(&[
            ("pk", AttributeValue::S("missing".into())),
            ("a", AttributeValue::S("new".into())),
        ]))
    );

    let db = make_db();
    create_table(&db, "Tbl");
    assert_eq!(
        upsert_returning(&db, Some("SET a = :v"), true, "UPDATED_NEW"),
        Some(make_item(&[("a", AttributeValue::S("new".into()))]))
    );
}

#[test]
fn all_old_on_an_existing_item_still_returns_the_old_item() {
    let db = make_db();
    create_table(&db, "Tbl");
    put(
        &db,
        "Tbl",
        &[
            ("pk", AttributeValue::S("missing".into())),
            ("a", AttributeValue::S("old".into())),
        ],
    );
    assert_eq!(
        upsert_returning(&db, Some("SET a = :v"), true, "ALL_OLD"),
        Some(make_item(&[
            ("pk", AttributeValue::S("missing".into())),
            ("a", AttributeValue::S("old".into())),
        ]))
    );
}
