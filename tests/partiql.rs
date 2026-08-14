use dynoxide::Database;
use dynoxide::actions::batch_execute_statement::{
    BatchExecuteStatementRequest, BatchStatementRequest,
};
use dynoxide::actions::create_table::CreateTableRequest;
use dynoxide::actions::execute_statement::ExecuteStatementRequest;
use dynoxide::actions::execute_transaction::{ExecuteTransactionRequest, ParameterizedStatement};
use dynoxide::actions::put_item::PutItemRequest;
use dynoxide::errors::DynoxideError;
use dynoxide::types::{
    AttributeDefinition, AttributeValue, KeySchemaElement, KeyType, ScalarAttributeType,
};
use std::collections::HashMap;

/// `BatchStatementRequest` is `#[non_exhaustive]`, so a test outside the crate
/// cannot use struct-literal syntax for it. This is the supported shape.
fn batch_stmt(statement: String) -> BatchStatementRequest {
    let mut req = BatchStatementRequest::default();
    req.statement = statement;
    req
}

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

fn put_test_item(db: &Database, table_name: &str, pk: &str, name: &str) {
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S(pk.to_string()));
    item.insert("name".to_string(), AttributeValue::S(name.to_string()));

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

/// Seed an item with `pk` plus arbitrary string attributes, so UPDATE RETURNING
/// tests can distinguish a changed attribute from an untouched one.
fn put_item_with_attrs(db: &Database, table_name: &str, pk: &str, attrs: &[(&str, &str)]) {
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S(pk.to_string()));
    for (k, v) in attrs {
        item.insert(k.to_string(), AttributeValue::S(v.to_string()));
    }
    db.put_item(PutItemRequest {
        table_name: table_name.to_string(),
        item,
        ..Default::default()
    })
    .unwrap();
}

/// Seed an item with `pk` plus one nested map attribute, so nested-path UPDATE
/// RETURNING tests can check that MODIFIED projects only the changed leaf.
fn put_nested_item(
    db: &Database,
    table_name: &str,
    pk: &str,
    map_attr: &str,
    entries: &[(&str, &str)],
) {
    let mut inner = HashMap::new();
    for (k, v) in entries {
        inner.insert(k.to_string(), AttributeValue::S(v.to_string()));
    }
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S(pk.to_string()));
    item.insert(map_attr.to_string(), AttributeValue::M(inner));
    db.put_item(PutItemRequest {
        table_name: table_name.to_string(),
        item,
        ..Default::default()
    })
    .unwrap();
}

/// Seed an item with `pk` plus one list attribute of strings, for list-index
/// UPDATE tests.
fn put_list_item(db: &Database, table_name: &str, pk: &str, list_attr: &str, elems: &[&str]) {
    let list = elems
        .iter()
        .map(|e| AttributeValue::S(e.to_string()))
        .collect();
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S(pk.to_string()));
    item.insert(list_attr.to_string(), AttributeValue::L(list));
    db.put_item(PutItemRequest {
        table_name: table_name.to_string(),
        item,
        ..Default::default()
    })
    .unwrap();
}

/// Read `pk` back and assert its `attr` is a string list equal to `expected`.
fn assert_string_list(db: &Database, pk: &str, attr: &str, expected: &[&str]) {
    let sel = exec(db, &format!("SELECT * FROM \"Users\" WHERE pk = '{pk}'"));
    let items = sel.items.unwrap();
    let want: Vec<AttributeValue> = expected
        .iter()
        .map(|e| AttributeValue::S(e.to_string()))
        .collect();
    match items[0].get(attr) {
        Some(AttributeValue::L(list)) => assert_eq!(list, &want),
        other => panic!("expected {attr} list, got {other:?}"),
    }
}

fn exec(
    db: &Database,
    statement: &str,
) -> dynoxide::actions::execute_statement::ExecuteStatementResponse {
    db.execute_statement(ExecuteStatementRequest {
        statement: statement.to_string(),
        parameters: None,
        ..Default::default()
    })
    .unwrap()
}

fn exec_with_params(
    db: &Database,
    statement: &str,
    params: Vec<AttributeValue>,
) -> dynoxide::actions::execute_statement::ExecuteStatementResponse {
    db.execute_statement(ExecuteStatementRequest {
        statement: statement.to_string(),
        parameters: Some(params),
        ..Default::default()
    })
    .unwrap()
}

// -----------------------------------------------------------------------
// SELECT
// -----------------------------------------------------------------------

#[test]
fn test_select_star_from_table() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");
    put_test_item(&db, "Users", "u1", "Alice");
    put_test_item(&db, "Users", "u2", "Bob");

    let resp = exec(&db, "SELECT * FROM \"Users\"");
    let items = resp.items.unwrap();
    assert_eq!(items.len(), 2);
}

#[test]
fn test_select_with_where_pk() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");
    put_test_item(&db, "Users", "u1", "Alice");
    put_test_item(&db, "Users", "u2", "Bob");

    let resp = exec(&db, "SELECT * FROM \"Users\" WHERE pk = 'u1'");
    let items = resp.items.unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(
        items[0].get("name"),
        Some(&AttributeValue::S("Alice".to_string()))
    );
}

#[test]
fn test_select_with_projection() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");
    put_test_item(&db, "Users", "u1", "Alice");

    let resp = exec(&db, "SELECT name FROM \"Users\" WHERE pk = 'u1'");
    let items = resp.items.unwrap();
    assert_eq!(items.len(), 1);
    assert!(items[0].contains_key("name"));
    assert!(!items[0].contains_key("pk")); // pk not projected
}

#[test]
fn test_select_empty_result() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");

    let resp = exec(&db, "SELECT * FROM \"Users\" WHERE pk = 'nonexistent'");
    let items = resp.items.unwrap();
    assert!(items.is_empty());
}

// -----------------------------------------------------------------------
// INSERT
// -----------------------------------------------------------------------

#[test]
fn test_insert_single_item() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");

    let resp = exec(
        &db,
        "INSERT INTO \"Users\" VALUE {'pk': 'u1', 'name': 'Charlie'}",
    );
    assert!(resp.items.is_none()); // write ops return no items

    // Verify the item exists
    let sel = exec(&db, "SELECT * FROM \"Users\" WHERE pk = 'u1'");
    let items = sel.items.unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(
        items[0].get("name"),
        Some(&AttributeValue::S("Charlie".to_string()))
    );
}

#[test]
fn test_insert_with_number() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");

    exec(&db, "INSERT INTO \"Users\" VALUE {'pk': 'u1', 'age': 30}");

    let sel = exec(&db, "SELECT * FROM \"Users\" WHERE pk = 'u1'");
    let items = sel.items.unwrap();
    assert_eq!(
        items[0].get("age"),
        Some(&AttributeValue::N("30".to_string()))
    );
}

// -----------------------------------------------------------------------
// UPDATE
// -----------------------------------------------------------------------

#[test]
fn test_update_existing_item() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");
    put_test_item(&db, "Users", "u1", "Alice");

    exec(&db, "UPDATE \"Users\" SET name = 'Alicia' WHERE pk = 'u1'");

    let sel = exec(&db, "SELECT * FROM \"Users\" WHERE pk = 'u1'");
    let items = sel.items.unwrap();
    assert_eq!(
        items[0].get("name"),
        Some(&AttributeValue::S("Alicia".to_string()))
    );
}

#[test]
fn test_update_adds_new_attribute() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");
    put_test_item(&db, "Users", "u1", "Alice");

    exec(
        &db,
        "UPDATE \"Users\" SET email = 'alice@example.com' WHERE pk = 'u1'",
    );

    let sel = exec(&db, "SELECT * FROM \"Users\" WHERE pk = 'u1'");
    let items = sel.items.unwrap();
    assert_eq!(
        items[0].get("email"),
        Some(&AttributeValue::S("alice@example.com".to_string()))
    );
    // Original name should still be there
    assert_eq!(
        items[0].get("name"),
        Some(&AttributeValue::S("Alice".to_string()))
    );
}

#[test]
fn test_update_on_missing_key_is_not_an_upsert() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");
    // The key is deliberately not seeded.

    // PartiQL UPDATE requires the item to exist: a missing key fails
    // ConditionalCheckFailedException rather than creating the item. This holds
    // with or without a RETURNING clause.
    for statement in [
        "UPDATE \"Users\" SET data = 'new' WHERE pk = 'ghost'",
        "UPDATE \"Users\" SET data = 'new' WHERE pk = 'ghost' RETURNING ALL OLD *",
    ] {
        let err = db
            .execute_statement(ExecuteStatementRequest {
                statement: statement.to_string(),
                parameters: None,
                ..Default::default()
            })
            .unwrap_err();
        match err {
            DynoxideError::ConditionalCheckFailedException(msg, _) => {
                assert_eq!(msg, "The conditional request failed", "for `{statement}`")
            }
            other => panic!("expected ConditionalCheckFailedException, got {other:?}"),
        }
    }

    // The rejected updates did not create the item.
    let sel = exec(&db, "SELECT * FROM \"Users\" WHERE pk = 'ghost'");
    assert!(sel.items.unwrap().is_empty());
}

// -----------------------------------------------------------------------
// DELETE
// -----------------------------------------------------------------------

#[test]
fn test_delete_existing_item() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");
    put_test_item(&db, "Users", "u1", "Alice");

    exec(&db, "DELETE FROM \"Users\" WHERE pk = 'u1'");

    let sel = exec(&db, "SELECT * FROM \"Users\" WHERE pk = 'u1'");
    let items = sel.items.unwrap();
    assert!(items.is_empty());
}

#[test]
fn test_delete_nonexistent_item() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");

    // Should not error, just silently succeed
    exec(&db, "DELETE FROM \"Users\" WHERE pk = 'nonexistent'");
}

#[test]
fn test_delete_returning_all_old_returns_deleted_item() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");
    put_test_item(&db, "Users", "u1", "Alice");

    let resp = exec(
        &db,
        "DELETE FROM \"Users\" WHERE pk = 'u1' RETURNING ALL OLD *",
    );
    let items = resp
        .items
        .expect("RETURNING ALL OLD * should return the deleted item");
    assert_eq!(items.len(), 1);
    assert_eq!(
        items[0].get("pk"),
        Some(&AttributeValue::S("u1".to_string()))
    );
    assert_eq!(
        items[0].get("name"),
        Some(&AttributeValue::S("Alice".to_string()))
    );

    // The item is actually gone
    let sel = exec(&db, "SELECT * FROM \"Users\" WHERE pk = 'u1'");
    assert!(sel.items.unwrap().is_empty());
}

#[test]
fn test_delete_returning_all_old_missing_item_returns_empty_items() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");

    let resp = exec(
        &db,
        "DELETE FROM \"Users\" WHERE pk = 'nonexistent' RETURNING ALL OLD *",
    );
    // A missing target is a no-op success that still returns an Items array, an
    // empty one, present rather than absent. This differs from classic
    // DeleteItem, which omits Attributes on a miss.
    let items = resp
        .items
        .expect("RETURNING must surface an Items array even on a miss");
    assert!(items.is_empty());
}

#[test]
fn test_delete_without_returning_still_returns_no_items() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");
    put_test_item(&db, "Users", "u1", "Alice");

    let resp = exec(&db, "DELETE FROM \"Users\" WHERE pk = 'u1'");
    assert!(resp.items.is_none());
}

#[test]
fn test_delete_returning_unsupported_variants_are_rejected_with_exact_message() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");

    let cases = [
        (
            "MODIFIED OLD",
            "Invalid returning clause: RETURNING MODIFIED OLD *. Only RETURNING ALL OLD * is allowed in DELETE statements.",
        ),
        (
            "ALL NEW",
            "Invalid returning clause: RETURNING ALL NEW *. Only RETURNING ALL OLD * is allowed in DELETE statements.",
        ),
        (
            "MODIFIED NEW",
            "Invalid returning clause: RETURNING MODIFIED NEW *. Only RETURNING ALL OLD * is allowed in DELETE statements.",
        ),
    ];

    for (variant, expected) in cases {
        put_test_item(&db, "Users", "u1", "Alice");
        let err = db
            .execute_statement(ExecuteStatementRequest {
                statement: format!("DELETE FROM \"Users\" WHERE pk = 'u1' RETURNING {variant} *"),
                parameters: None,
                ..Default::default()
            })
            .unwrap_err();
        match err {
            DynoxideError::ValidationException(msg) => {
                assert_eq!(msg, expected, "variant {variant}")
            }
            other => panic!("expected ValidationException for {variant}, got {other:?}"),
        }

        // The rejected statement must not have deleted the item.
        let sel = exec(&db, "SELECT * FROM \"Users\" WHERE pk = 'u1'");
        assert_eq!(sel.items.unwrap().len(), 1, "variant {variant}");
    }
}

// -----------------------------------------------------------------------
// UPDATE ... RETURNING (all four variants on a present item)
// -----------------------------------------------------------------------

#[test]
fn test_update_returning_all_old_returns_full_prior_item_with_key() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");
    put_item_with_attrs(&db, "Users", "u1", &[("data", "old"), ("keep", "same")]);

    let resp = exec(
        &db,
        "UPDATE \"Users\" SET data = 'new' WHERE pk = 'u1' RETURNING ALL OLD *",
    );
    let items = resp.items.expect("RETURNING should return items");
    assert_eq!(items.len(), 1);
    let it = &items[0];
    assert_eq!(it.get("pk"), Some(&AttributeValue::S("u1".to_string())));
    assert_eq!(it.get("data"), Some(&AttributeValue::S("old".to_string())));
    assert_eq!(it.get("keep"), Some(&AttributeValue::S("same".to_string())));

    // The write applied.
    let sel = exec(&db, "SELECT * FROM \"Users\" WHERE pk = 'u1'");
    assert_eq!(
        sel.items.unwrap()[0].get("data"),
        Some(&AttributeValue::S("new".to_string()))
    );
}

#[test]
fn test_update_returning_modified_old_excludes_key_and_untouched_attrs() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");
    put_item_with_attrs(&db, "Users", "u1", &[("data", "old"), ("keep", "same")]);

    let resp = exec(
        &db,
        "UPDATE \"Users\" SET data = 'new' WHERE pk = 'u1' RETURNING MODIFIED OLD *",
    );
    let items = resp.items.expect("RETURNING should return items");
    assert_eq!(items.len(), 1);
    let it = &items[0];
    assert_eq!(it.get("data"), Some(&AttributeValue::S("old".to_string())));
    assert!(it.get("pk").is_none(), "MODIFIED excludes the key");
    assert!(
        it.get("keep").is_none(),
        "MODIFIED excludes untouched attrs"
    );
    assert_eq!(it.len(), 1);
}

#[test]
fn test_update_returning_all_new_returns_full_new_item_with_key() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");
    put_item_with_attrs(&db, "Users", "u1", &[("data", "old"), ("keep", "same")]);

    let resp = exec(
        &db,
        "UPDATE \"Users\" SET data = 'new' WHERE pk = 'u1' RETURNING ALL NEW *",
    );
    let items = resp.items.expect("RETURNING should return items");
    assert_eq!(items.len(), 1);
    let it = &items[0];
    assert_eq!(it.get("pk"), Some(&AttributeValue::S("u1".to_string())));
    assert_eq!(it.get("data"), Some(&AttributeValue::S("new".to_string())));
    assert_eq!(it.get("keep"), Some(&AttributeValue::S("same".to_string())));
}

#[test]
fn test_update_returning_modified_new_returns_only_changed_new_value() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");
    put_item_with_attrs(&db, "Users", "u1", &[("data", "old"), ("keep", "same")]);

    let resp = exec(
        &db,
        "UPDATE \"Users\" SET data = 'new' WHERE pk = 'u1' RETURNING MODIFIED NEW *",
    );
    let items = resp.items.expect("RETURNING should return items");
    assert_eq!(items.len(), 1);
    let it = &items[0];
    assert_eq!(it.get("data"), Some(&AttributeValue::S("new".to_string())));
    assert!(it.get("pk").is_none(), "MODIFIED excludes the key");
    assert!(
        it.get("keep").is_none(),
        "MODIFIED excludes untouched attrs"
    );
    assert_eq!(it.len(), 1);
}

// -----------------------------------------------------------------------
// UPDATE MODIFIED edge cases, per the real-AWS follow-up capture: a nested
// SET projects only the changed leaf, and an empty MODIFIED projection
// returns Items: [] (no row), not a row holding an empty object.
// -----------------------------------------------------------------------

#[test]
fn test_update_returning_modified_nested_path_projects_only_changed_leaf() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");

    // MODIFIED NEW: only the changed leaf, nested; sibling and key excluded.
    put_nested_item(
        &db,
        "Users",
        "m1",
        "profile",
        &[("sub", "old"), ("sib", "keep")],
    );
    let resp = exec(
        &db,
        "UPDATE \"Users\" SET profile.sub = 'new' WHERE pk = 'm1' RETURNING MODIFIED NEW *",
    );
    let items = resp.items.expect("RETURNING should return items");
    assert_eq!(items.len(), 1);
    let it = &items[0];
    assert!(!it.contains_key("pk"), "MODIFIED excludes the key");
    let profile = match it.get("profile") {
        Some(AttributeValue::M(m)) => m,
        other => panic!("expected profile map, got {other:?}"),
    };
    assert_eq!(
        profile.get("sub"),
        Some(&AttributeValue::S("new".to_string()))
    );
    assert!(
        profile.get("sib").is_none(),
        "nested MODIFIED excludes the untouched sibling"
    );
    assert_eq!(profile.len(), 1);

    // MODIFIED OLD: the old value of the changed leaf, same shape.
    put_nested_item(
        &db,
        "Users",
        "m2",
        "profile",
        &[("sub", "old"), ("sib", "keep")],
    );
    let resp = exec(
        &db,
        "UPDATE \"Users\" SET profile.sub = 'new' WHERE pk = 'm2' RETURNING MODIFIED OLD *",
    );
    let items = resp.items.expect("RETURNING should return items");
    let profile = match items[0].get("profile") {
        Some(AttributeValue::M(m)) => m,
        other => panic!("expected profile map, got {other:?}"),
    };
    assert_eq!(
        profile.get("sub"),
        Some(&AttributeValue::S("old".to_string()))
    );
    assert_eq!(profile.len(), 1);
}

#[test]
fn test_update_returning_modified_after_remove() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");

    // MODIFIED OLD: the removed attribute at its old value.
    put_item_with_attrs(&db, "Users", "r1", &[("data", "old"), ("keep", "same")]);
    let resp = exec(
        &db,
        "UPDATE \"Users\" REMOVE data WHERE pk = 'r1' RETURNING MODIFIED OLD *",
    );
    let items = resp.items.expect("RETURNING should return items");
    assert_eq!(items.len(), 1);
    assert_eq!(
        items[0].get("data"),
        Some(&AttributeValue::S("old".to_string()))
    );
    assert_eq!(items[0].len(), 1);

    // MODIFIED NEW: the removed attribute is gone, so the projection is empty ->
    // a present but empty Items array, not a row holding an empty object.
    put_item_with_attrs(&db, "Users", "r2", &[("data", "old"), ("keep", "same")]);
    let resp = exec(
        &db,
        "UPDATE \"Users\" REMOVE data WHERE pk = 'r2' RETURNING MODIFIED NEW *",
    );
    let items = resp
        .items
        .expect("an empty MODIFIED projection still returns a present Items array");
    assert!(
        items.is_empty(),
        "MODIFIED NEW after REMOVE returns Items: [] (no row)"
    );
}

#[test]
fn test_update_returning_modified_on_newly_created_attribute() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");

    // MODIFIED OLD on an attribute that did not exist: empty projection -> Items: [].
    put_item_with_attrs(&db, "Users", "e1", &[("keep", "same")]);
    let resp = exec(
        &db,
        "UPDATE \"Users\" SET data = 'new' WHERE pk = 'e1' RETURNING MODIFIED OLD *",
    );
    let items = resp
        .items
        .expect("an empty MODIFIED projection still returns a present Items array");
    assert!(
        items.is_empty(),
        "MODIFIED OLD on a newly-created attribute returns Items: [] (no row)"
    );

    // MODIFIED NEW: the new value.
    put_item_with_attrs(&db, "Users", "e2", &[("keep", "same")]);
    let resp = exec(
        &db,
        "UPDATE \"Users\" SET data = 'new' WHERE pk = 'e2' RETURNING MODIFIED NEW *",
    );
    let items = resp.items.expect("RETURNING should return items");
    assert_eq!(items.len(), 1);
    assert_eq!(
        items[0].get("data"),
        Some(&AttributeValue::S("new".to_string()))
    );
    assert_eq!(items[0].len(), 1);
}

#[test]
fn test_update_returning_modified_merges_sibling_nested_paths() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");
    put_nested_item(
        &db,
        "Users",
        "m3",
        "profile",
        &[("a", "oldA"), ("b", "oldB"), ("keep", "same")],
    );

    // Two sibling nested SETs must merge under the shared parent map, and the
    // untouched sibling `keep` is excluded. This exercises the set_nested_value
    // merge path that reconstructs the projection.
    let resp = exec(
        &db,
        "UPDATE \"Users\" SET profile.a = 'x', profile.b = 'y' WHERE pk = 'm3' RETURNING MODIFIED NEW *",
    );
    let items = resp.items.expect("RETURNING should return items");
    assert_eq!(items.len(), 1);
    let profile = match items[0].get("profile") {
        Some(AttributeValue::M(m)) => m,
        other => panic!("expected profile map, got {other:?}"),
    };
    assert_eq!(profile.get("a"), Some(&AttributeValue::S("x".to_string())));
    assert_eq!(profile.get("b"), Some(&AttributeValue::S("y".to_string())));
    assert!(
        !profile.contains_key("keep"),
        "untouched sibling excluded from MODIFIED"
    );
    assert_eq!(profile.len(), 2);
}

#[test]
fn test_update_returning_modified_after_nested_remove() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");

    // MODIFIED OLD: the removed nested leaf at its old value, nested, no sibling.
    put_nested_item(
        &db,
        "Users",
        "n1",
        "profile",
        &[("sub", "old"), ("sib", "keep")],
    );
    let resp = exec(
        &db,
        "UPDATE \"Users\" REMOVE profile.sub WHERE pk = 'n1' RETURNING MODIFIED OLD *",
    );
    let items = resp.items.expect("RETURNING should return items");
    let profile = match items[0].get("profile") {
        Some(AttributeValue::M(m)) => m,
        other => panic!("expected profile map, got {other:?}"),
    };
    assert_eq!(
        profile.get("sub"),
        Some(&AttributeValue::S("old".to_string()))
    );
    assert_eq!(profile.len(), 1);

    // MODIFIED NEW: the removed leaf is gone, so the projection is empty -> [].
    put_nested_item(
        &db,
        "Users",
        "n2",
        "profile",
        &[("sub", "old"), ("sib", "keep")],
    );
    let resp = exec(
        &db,
        "UPDATE \"Users\" REMOVE profile.sub WHERE pk = 'n2' RETURNING MODIFIED NEW *",
    );
    let items = resp
        .items
        .expect("an empty MODIFIED projection still returns a present Items array");
    assert!(
        items.is_empty(),
        "MODIFIED NEW after a nested REMOVE returns Items: [] (no row)"
    );
}

#[test]
fn test_update_returning_modified_deep_nested_path() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");

    // Seed profile = { inner: { leaf: 'old', sib: 'keep' } } (three levels).
    let mut inner = HashMap::new();
    inner.insert("leaf".to_string(), AttributeValue::S("old".to_string()));
    inner.insert("sib".to_string(), AttributeValue::S("keep".to_string()));
    let mut profile = HashMap::new();
    profile.insert("inner".to_string(), AttributeValue::M(inner));
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("d1".to_string()));
    item.insert("profile".to_string(), AttributeValue::M(profile));
    db.put_item(PutItemRequest {
        table_name: "Users".to_string(),
        item,
        ..Default::default()
    })
    .unwrap();

    // A three-level SET projects only the changed leaf, nested all the way down.
    let resp = exec(
        &db,
        "UPDATE \"Users\" SET profile.inner.leaf = 'new' WHERE pk = 'd1' RETURNING MODIFIED NEW *",
    );
    let items = resp.items.expect("RETURNING should return items");
    let inner = match items[0].get("profile") {
        Some(AttributeValue::M(p)) => match p.get("inner") {
            Some(AttributeValue::M(i)) => i,
            other => panic!("expected inner map, got {other:?}"),
        },
        other => panic!("expected profile map, got {other:?}"),
    };
    assert_eq!(
        inner.get("leaf"),
        Some(&AttributeValue::S("new".to_string()))
    );
    assert!(
        !inner.contains_key("sib"),
        "deep MODIFIED excludes the untouched sibling"
    );
    assert_eq!(inner.len(), 1);
}

// -----------------------------------------------------------------------
// List-index SET/REMOVE: a real list-index write, and the MODIFIED projection
// returning the changed element in list shape (index collapsed to 0).
// -----------------------------------------------------------------------

#[test]
fn test_update_set_list_index_writes_the_element() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");
    put_list_item(&db, "Users", "l1", "tags", &["a", "b"]);

    exec(&db, "UPDATE \"Users\" SET tags[0] = 'x' WHERE pk = 'l1'");

    let sel = exec(&db, "SELECT * FROM \"Users\" WHERE pk = 'l1'");
    match sel.items.unwrap()[0].get("tags") {
        Some(AttributeValue::L(list)) => assert_eq!(
            list,
            &vec![
                AttributeValue::S("x".to_string()),
                AttributeValue::S("b".to_string()),
            ],
            "SET tags[0] writes the real list element, not a literal tags[0] key"
        ),
        other => panic!("expected tags list, got {other:?}"),
    }
}

#[test]
fn test_update_remove_list_index_removes_the_element() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");
    put_list_item(&db, "Users", "l1", "tags", &["a", "b", "c"]);

    exec(&db, "UPDATE \"Users\" REMOVE tags[1] WHERE pk = 'l1'");

    let sel = exec(&db, "SELECT * FROM \"Users\" WHERE pk = 'l1'");
    match sel.items.unwrap()[0].get("tags") {
        Some(AttributeValue::L(list)) => assert_eq!(
            list,
            &vec![
                AttributeValue::S("a".to_string()),
                AttributeValue::S("c".to_string()),
            ],
            "REMOVE tags[1] deletes the element and shifts the rest"
        ),
        other => panic!("expected tags list, got {other:?}"),
    }
}

#[test]
fn test_update_returning_modified_list_index_projects_changed_element() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");

    // MODIFIED NEW: only the changed element, in list shape, collapsed to [x].
    put_list_item(&db, "Users", "l1", "tags", &["a", "b"]);
    let resp = exec(
        &db,
        "UPDATE \"Users\" SET tags[0] = 'x' WHERE pk = 'l1' RETURNING MODIFIED NEW *",
    );
    let items = resp.items.expect("RETURNING should return items");
    assert_eq!(items.len(), 1);
    assert!(!items[0].contains_key("pk"), "MODIFIED excludes the key");
    match items[0].get("tags") {
        Some(AttributeValue::L(list)) => {
            assert_eq!(list, &vec![AttributeValue::S("x".to_string())])
        }
        other => panic!("expected tags list, got {other:?}"),
    }

    // MODIFIED OLD: the prior element value, same collapsed list shape.
    put_list_item(&db, "Users", "l2", "tags", &["a", "b"]);
    let resp = exec(
        &db,
        "UPDATE \"Users\" SET tags[0] = 'x' WHERE pk = 'l2' RETURNING MODIFIED OLD *",
    );
    match resp.items.unwrap()[0].get("tags") {
        Some(AttributeValue::L(list)) => {
            assert_eq!(list, &vec![AttributeValue::S("a".to_string())])
        }
        other => panic!("expected tags list, got {other:?}"),
    }
}

#[test]
fn test_update_returning_modified_after_list_remove() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");

    // MODIFIED OLD: the removed element's old value at that index.
    put_list_item(&db, "Users", "l1", "tags", &["a", "b", "c"]);
    let resp = exec(
        &db,
        "UPDATE \"Users\" REMOVE tags[1] WHERE pk = 'l1' RETURNING MODIFIED OLD *",
    );
    match resp.items.unwrap()[0].get("tags") {
        Some(AttributeValue::L(list)) => {
            assert_eq!(list, &vec![AttributeValue::S("b".to_string())])
        }
        other => panic!("expected tags list, got {other:?}"),
    }

    // MODIFIED NEW: a list REMOVE shifts rather than deletes, so `tags[1]` still
    // resolves on the new list `['a','c']` and points at the shifted-in 'c'.
    // (This differs from a map REMOVE, whose key is gone and yields no row.)
    put_list_item(&db, "Users", "l2", "tags", &["a", "b", "c"]);
    let resp = exec(
        &db,
        "UPDATE \"Users\" REMOVE tags[1] WHERE pk = 'l2' RETURNING MODIFIED NEW *",
    );
    match resp.items.unwrap()[0].get("tags") {
        Some(AttributeValue::L(list)) => {
            assert_eq!(list, &vec![AttributeValue::S("c".to_string())])
        }
        other => panic!("expected tags list, got {other:?}"),
    }
}

#[test]
fn test_update_returning_modified_list_index_non_zero_and_multiple() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");

    // A single non-zero index projects a single-element list of the changed value.
    put_list_item(&db, "Users", "l1", "tags", &["a", "b", "c"]);
    let resp = exec(
        &db,
        "UPDATE \"Users\" SET tags[2] = 'y' WHERE pk = 'l1' RETURNING MODIFIED NEW *",
    );
    match resp.items.unwrap()[0].get("tags") {
        Some(AttributeValue::L(list)) => {
            assert_eq!(list, &vec![AttributeValue::S("y".to_string())])
        }
        other => panic!("expected tags list, got {other:?}"),
    }

    // Several indices pack into a dense list in ascending index order, dropping
    // the untouched positions (not collapsed onto one slot).
    put_list_item(&db, "Users", "l2", "tags", &["a", "b", "c"]);
    let resp = exec(
        &db,
        "UPDATE \"Users\" SET tags[0] = 'x', tags[2] = 'y' WHERE pk = 'l2' RETURNING MODIFIED NEW *",
    );
    match resp.items.unwrap()[0].get("tags") {
        Some(AttributeValue::L(list)) => assert_eq!(
            list,
            &vec![
                AttributeValue::S("x".to_string()),
                AttributeValue::S("y".to_string()),
            ]
        ),
        other => panic!("expected tags list, got {other:?}"),
    }
    // The stored item keeps positions: ['x','b','y'].
    assert_string_list(&db, "l2", "tags", &["x", "b", "y"]);

    // MODIFIED OLD packs the prior values at the touched indices the same way.
    put_list_item(&db, "Users", "l3", "tags", &["a", "b", "c"]);
    let resp = exec(
        &db,
        "UPDATE \"Users\" SET tags[0] = 'x', tags[2] = 'y' WHERE pk = 'l3' RETURNING MODIFIED OLD *",
    );
    match resp.items.unwrap()[0].get("tags") {
        Some(AttributeValue::L(list)) => assert_eq!(
            list,
            &vec![
                AttributeValue::S("a".to_string()),
                AttributeValue::S("c".to_string()),
            ]
        ),
        other => panic!("expected tags list, got {other:?}"),
    }
}

#[test]
fn test_update_returning_modified_list_index_orders_by_index_not_statement() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");
    put_list_item(&db, "Users", "l1", "tags", &["a", "b", "c"]);

    // The indices are listed descending in the statement (tags[2] then tags[0]),
    // but AWS packs the projection by index, so the result is ['x','y'] (index 0
    // before index 2), not statement order ['y','x'].
    let resp = exec(
        &db,
        "UPDATE \"Users\" SET tags[2] = 'y', tags[0] = 'x' WHERE pk = 'l1' RETURNING MODIFIED NEW *",
    );
    match resp.items.unwrap()[0].get("tags") {
        Some(AttributeValue::L(list)) => assert_eq!(
            list,
            &vec![
                AttributeValue::S("x".to_string()),
                AttributeValue::S("y".to_string()),
            ]
        ),
        other => panic!("expected tags list, got {other:?}"),
    }
}

#[test]
fn test_update_returning_modified_list_index_multi_digit_ordering() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");
    // An 11-element list so index 10 exists.
    put_list_item(
        &db,
        "Users",
        "l1",
        "tags",
        &[
            "e0", "e1", "e2", "e3", "e4", "e5", "e6", "e7", "e8", "e9", "e10",
        ],
    );

    // The path set sorts "tags[10]" before "tags[2]" as strings, but the pack is
    // keyed by numeric index, so index 2's value comes before index 10's.
    let resp = exec(
        &db,
        "UPDATE \"Users\" SET tags[2] = 'y', tags[10] = 'k' WHERE pk = 'l1' RETURNING MODIFIED NEW *",
    );
    match resp.items.unwrap()[0].get("tags") {
        Some(AttributeValue::L(list)) => assert_eq!(
            list,
            &vec![
                AttributeValue::S("y".to_string()),
                AttributeValue::S("k".to_string()),
            ]
        ),
        other => panic!("expected tags list, got {other:?}"),
    }
}

#[test]
fn test_update_returning_modified_append_is_empty() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");

    // The write appends (stored ['a','b','c']), but a far-out-of-range index
    // (tags[5]) does not resolve on the new 3-element list, so MODIFIED NEW
    // projects nothing. This is specific to a far index, not appends in general
    // (see the at-length case below, where the index resolves).
    put_list_item(&db, "Users", "l1", "tags", &["a", "b"]);
    let resp = exec(
        &db,
        "UPDATE \"Users\" SET tags[5] = 'c' WHERE pk = 'l1' RETURNING MODIFIED NEW *",
    );
    let items = resp.items.expect("present Items array");
    assert!(
        items.is_empty(),
        "MODIFIED NEW over a far-out-of-range append returns Items: []"
    );
    assert_string_list(&db, "l1", "tags", &["a", "b", "c"]);

    // MODIFIED OLD: tags[5] does not resolve on the old 2-element list either.
    put_list_item(&db, "Users", "l2", "tags", &["a", "b"]);
    let resp = exec(
        &db,
        "UPDATE \"Users\" SET tags[5] = 'c' WHERE pk = 'l2' RETURNING MODIFIED OLD *",
    );
    assert!(
        resp.items.expect("present Items array").is_empty(),
        "MODIFIED OLD over a far-out-of-range append returns Items: []"
    );
}

#[test]
fn test_update_returning_modified_append_at_length() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");

    // Appending at exactly the old length: the index resolves on the new list
    // under NEW (so the element is projected) but is out of range on the old
    // list under OLD (so nothing is projected).
    put_list_item(&db, "Users", "l1", "tags", &["a", "b"]);
    let resp = exec(
        &db,
        "UPDATE \"Users\" SET tags[2] = 'c' WHERE pk = 'l1' RETURNING MODIFIED NEW *",
    );
    match resp.items.unwrap()[0].get("tags") {
        Some(AttributeValue::L(list)) => {
            assert_eq!(list, &vec![AttributeValue::S("c".to_string())])
        }
        other => panic!("expected tags list, got {other:?}"),
    }

    put_list_item(&db, "Users", "l2", "tags", &["a", "b"]);
    let resp = exec(
        &db,
        "UPDATE \"Users\" SET tags[2] = 'c' WHERE pk = 'l2' RETURNING MODIFIED OLD *",
    );
    assert!(
        resp.items.expect("present Items array").is_empty(),
        "the appended index is out of range on the old list"
    );
}

#[test]
fn test_update_returning_modified_remove_last_index() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");

    // Removing the last index: nothing shifts into it, so under NEW the index no
    // longer resolves and the projection is empty; under OLD it still resolves
    // on the pre-remove list.
    put_list_item(&db, "Users", "l1", "tags", &["a", "b", "c"]);
    let resp = exec(
        &db,
        "UPDATE \"Users\" REMOVE tags[2] WHERE pk = 'l1' RETURNING MODIFIED NEW *",
    );
    assert!(
        resp.items.expect("present Items array").is_empty(),
        "removing the last index leaves nothing to project under MODIFIED NEW"
    );
    assert_string_list(&db, "l1", "tags", &["a", "b"]);

    put_list_item(&db, "Users", "l2", "tags", &["a", "b", "c"]);
    let resp = exec(
        &db,
        "UPDATE \"Users\" REMOVE tags[2] WHERE pk = 'l2' RETURNING MODIFIED OLD *",
    );
    match resp.items.unwrap()[0].get("tags") {
        Some(AttributeValue::L(list)) => {
            assert_eq!(list, &vec![AttributeValue::S("c".to_string())])
        }
        other => panic!("expected tags list, got {other:?}"),
    }
}

#[test]
fn test_update_set_list_index_on_absent_attribute_is_rejected() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");
    put_test_item(&db, "Users", "u1", "Alice"); // no `newlist` attribute

    // You cannot set a nested path whose parent is absent; AWS rejects with this
    // exact message rather than creating the list.
    let err = db
        .execute_statement(ExecuteStatementRequest {
            statement: "UPDATE \"Users\" SET newlist[0] = 'x' WHERE pk = 'u1'".to_string(),
            parameters: None,
            ..Default::default()
        })
        .unwrap_err();
    match err {
        DynoxideError::ValidationException(msg) => assert_eq!(
            msg,
            "The document path provided in the update expression is invalid for update"
        ),
        other => panic!("expected ValidationException, got {other:?}"),
    }
    let sel = exec(&db, "SELECT * FROM \"Users\" WHERE pk = 'u1'");
    assert!(
        !sel.items.unwrap()[0].contains_key("newlist"),
        "the rejected statement must not create the attribute"
    );
}

#[test]
fn test_update_set_list_index_at_or_beyond_end_appends() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");

    // Index == length: appended.
    put_list_item(&db, "Users", "l1", "tags", &["a", "b"]);
    exec(&db, "UPDATE \"Users\" SET tags[2] = 'c' WHERE pk = 'l1'");
    assert_string_list(&db, "l1", "tags", &["a", "b", "c"]);

    // Index beyond the end: also appended (no gap), matching DynamoDB.
    put_list_item(&db, "Users", "l2", "tags", &["a", "b"]);
    exec(&db, "UPDATE \"Users\" SET tags[5] = 'c' WHERE pk = 'l2'");
    assert_string_list(&db, "l2", "tags", &["a", "b", "c"]);
}

#[test]
fn test_update_set_list_index_on_non_list_is_rejected() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");
    put_test_item(&db, "Users", "u1", "Alice"); // `name` is a scalar string

    let err = db
        .execute_statement(ExecuteStatementRequest {
            statement: "UPDATE \"Users\" SET name[0] = 'x' WHERE pk = 'u1'".to_string(),
            parameters: None,
            ..Default::default()
        })
        .unwrap_err();
    match err {
        DynoxideError::ValidationException(msg) => assert!(
            msg.contains("invalid for update"),
            "unexpected message: {msg}"
        ),
        other => panic!("expected ValidationException, got {other:?}"),
    }

    // The rejected statement must not have changed the attribute.
    let sel = exec(&db, "SELECT * FROM \"Users\" WHERE pk = 'u1'");
    assert_eq!(
        sel.items.unwrap()[0].get("name"),
        Some(&AttributeValue::S("Alice".to_string()))
    );
}

#[test]
fn test_update_set_nested_list_index_writes_element() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");

    // Seed profile = { tags: ['a', 'b'] } (a map holding a list).
    let mut inner = HashMap::new();
    inner.insert(
        "tags".to_string(),
        AttributeValue::L(vec![
            AttributeValue::S("a".to_string()),
            AttributeValue::S("b".to_string()),
        ]),
    );
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("n1".to_string()));
    item.insert("profile".to_string(), AttributeValue::M(inner));
    db.put_item(PutItemRequest {
        table_name: "Users".to_string(),
        item,
        ..Default::default()
    })
    .unwrap();

    exec(
        &db,
        "UPDATE \"Users\" SET profile.tags[0] = 'x' WHERE pk = 'n1'",
    );

    let sel = exec(&db, "SELECT * FROM \"Users\" WHERE pk = 'n1'");
    match sel.items.unwrap()[0].get("profile") {
        Some(AttributeValue::M(p)) => match p.get("tags") {
            Some(AttributeValue::L(list)) => assert_eq!(
                list,
                &vec![
                    AttributeValue::S("x".to_string()),
                    AttributeValue::S("b".to_string()),
                ]
            ),
            other => panic!("expected tags list, got {other:?}"),
        },
        other => panic!("expected profile map, got {other:?}"),
    }
}

#[test]
fn test_update_returning_modified_nested_list_index() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");

    // Seed profile = { tags: ['a', 'b'] } (a map holding a list), so the
    // projection builds a Map node holding a List node.
    let mut inner = HashMap::new();
    inner.insert(
        "tags".to_string(),
        AttributeValue::L(vec![
            AttributeValue::S("a".to_string()),
            AttributeValue::S("b".to_string()),
        ]),
    );
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("n1".to_string()));
    item.insert("profile".to_string(), AttributeValue::M(inner));
    db.put_item(PutItemRequest {
        table_name: "Users".to_string(),
        item,
        ..Default::default()
    })
    .unwrap();

    // MODIFIED NEW: only the changed nested list element, in list shape.
    let resp = exec(
        &db,
        "UPDATE \"Users\" SET profile.tags[0] = 'x' WHERE pk = 'n1' RETURNING MODIFIED NEW *",
    );
    let want_new = AttributeValue::M(HashMap::from([(
        "tags".to_string(),
        AttributeValue::L(vec![AttributeValue::S("x".to_string())]),
    )]));
    assert_eq!(resp.items.unwrap()[0].get("profile"), Some(&want_new));

    // MODIFIED OLD: the prior value at that nested index, same shape.
    let resp = exec(
        &db,
        "UPDATE \"Users\" SET profile.tags[0] = 'z' WHERE pk = 'n1' RETURNING MODIFIED OLD *",
    );
    let want_old = AttributeValue::M(HashMap::from([(
        "tags".to_string(),
        AttributeValue::L(vec![AttributeValue::S("x".to_string())]),
    )]));
    assert_eq!(resp.items.unwrap()[0].get("profile"), Some(&want_old));
}

// -----------------------------------------------------------------------
// RETURNING inside BatchExecuteStatement (honoured) and ExecuteTransaction
// (rejected), per the real-AWS ground truth.
// -----------------------------------------------------------------------

#[test]
fn test_batch_delete_returning_all_old_surfaces_item() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");
    put_test_item(&db, "Users", "b1", "Batch");

    let resp = db
        .batch_execute_statement(BatchExecuteStatementRequest {
            return_consumed_capacity: None,
            statements: vec![batch_stmt(
                "DELETE FROM \"Users\" WHERE pk = 'b1' RETURNING ALL OLD *".to_string(),
            )],
        })
        .unwrap();

    assert_eq!(resp.responses.len(), 1);
    assert!(resp.responses[0].error.is_none());
    let item = resp.responses[0]
        .item
        .as_ref()
        .expect("BatchExecuteStatement honours RETURNING on the member statement");
    assert_eq!(item.get("pk"), Some(&AttributeValue::S("b1".to_string())));
    assert_eq!(
        item.get("name"),
        Some(&AttributeValue::S("Batch".to_string()))
    );
    assert_eq!(
        resp.responses[0].table_name.as_deref(),
        Some("Users"),
        "batch echoes TableName on a successful response"
    );

    // The item is gone.
    let sel = exec(&db, "SELECT * FROM \"Users\" WHERE pk = 'b1'");
    assert!(sel.items.unwrap().is_empty());
}

#[test]
fn test_batch_update_returning_surfaces_projection() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");
    put_item_with_attrs(&db, "Users", "b1", &[("data", "old")]);

    // ALL NEW: the full new item, key included.
    let resp = db
        .batch_execute_statement(BatchExecuteStatementRequest {
            return_consumed_capacity: None,
            statements: vec![batch_stmt(
                "UPDATE \"Users\" SET data = 'new' WHERE pk = 'b1' RETURNING ALL NEW *".to_string(),
            )],
        })
        .unwrap();
    let item = resp.responses[0]
        .item
        .as_ref()
        .expect("batch honours an UPDATE member's RETURNING");
    assert_eq!(item.get("pk"), Some(&AttributeValue::S("b1".to_string())));
    assert_eq!(
        item.get("data"),
        Some(&AttributeValue::S("new".to_string()))
    );
    assert_eq!(resp.responses[0].table_name.as_deref(), Some("Users"));

    // MODIFIED NEW: only the changed attribute, no key.
    let resp = db
        .batch_execute_statement(BatchExecuteStatementRequest {
            return_consumed_capacity: None,
            statements: vec![batch_stmt(
                "UPDATE \"Users\" SET data = 'newer' WHERE pk = 'b1' RETURNING MODIFIED NEW *"
                    .to_string(),
            )],
        })
        .unwrap();
    let item = resp.responses[0]
        .item
        .as_ref()
        .expect("batch honours an UPDATE member's RETURNING");
    assert_eq!(
        item.get("data"),
        Some(&AttributeValue::S("newer".to_string()))
    );
    assert!(item.get("pk").is_none(), "MODIFIED excludes the key");
    assert_eq!(item.len(), 1);
}

#[test]
fn test_batch_update_empty_modified_omits_item() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");
    put_item_with_attrs(&db, "Users", "b1", &[("data", "old")]);

    // An empty MODIFIED projection inside a batch drops the singular Item field
    // entirely (the batch analogue of ExecuteStatement's Items: []), matching AWS.
    let resp = db
        .batch_execute_statement(BatchExecuteStatementRequest {
            return_consumed_capacity: None,
            statements: vec![batch_stmt(
                "UPDATE \"Users\" REMOVE data WHERE pk = 'b1' RETURNING MODIFIED NEW *".to_string(),
            )],
        })
        .unwrap();
    assert_eq!(resp.responses.len(), 1);
    assert!(resp.responses[0].error.is_none());
    assert!(
        resp.responses[0].item.is_none(),
        "an empty MODIFIED projection omits Item in a batch response"
    );
    assert_eq!(
        resp.responses[0].table_name.as_deref(),
        Some("Users"),
        "TableName is still echoed even when Item is omitted"
    );
}

#[test]
fn test_batch_update_list_index_modified_projects_changed_element() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");
    put_list_item(&db, "Users", "b1", "tags", &["a", "b"]);

    let resp = db
        .batch_execute_statement(BatchExecuteStatementRequest {
            return_consumed_capacity: None,
            statements: vec![batch_stmt(
                "UPDATE \"Users\" SET tags[0] = 'x' WHERE pk = 'b1' RETURNING MODIFIED NEW *"
                    .to_string(),
            )],
        })
        .unwrap();
    let item = resp.responses[0]
        .item
        .as_ref()
        .expect("batch surfaces the list-index MODIFIED projection");
    match item.get("tags") {
        Some(AttributeValue::L(list)) => {
            assert_eq!(list, &vec![AttributeValue::S("x".to_string())])
        }
        other => panic!("expected tags list, got {other:?}"),
    }
    assert_eq!(resp.responses[0].table_name.as_deref(), Some("Users"));
}

#[test]
fn test_batch_plain_update_echoes_table_name() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");
    put_item_with_attrs(&db, "Users", "b1", &[("data", "old")]);

    // A plain (no-RETURNING) member still echoes TableName, with no Item.
    let resp = db
        .batch_execute_statement(BatchExecuteStatementRequest {
            return_consumed_capacity: None,
            statements: vec![batch_stmt(
                "UPDATE \"Users\" SET data = 'new' WHERE pk = 'b1'".to_string(),
            )],
        })
        .unwrap();
    assert!(resp.responses[0].error.is_none());
    assert!(
        resp.responses[0].item.is_none(),
        "no RETURNING means no Item"
    );
    assert_eq!(resp.responses[0].table_name.as_deref(), Some("Users"));
}

#[test]
fn test_batch_parse_error_uses_short_validation_code() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");
    put_test_item(&db, "Users", "b1", "Batch");

    // A malformed member fails per-statement with the short-form code (as a
    // per-statement execution error does), while a valid sibling still executes.
    let resp = db
        .batch_execute_statement(BatchExecuteStatementRequest {
            return_consumed_capacity: None,
            statements: vec![
                batch_stmt("SLECT * FROM \"Users\"".to_string()),
                batch_stmt("SELECT * FROM \"Users\" WHERE pk = 'b1'".to_string()),
            ],
        })
        .unwrap();
    let err = resp.responses[0]
        .error
        .as_ref()
        .expect("the malformed member errors");
    assert_eq!(err.code, "ValidationError");
    assert_eq!(
        err.message,
        "Statement wasn't well formed, can't be processed: Expected data manipulation"
    );
    assert!(
        resp.responses[1].error.is_none(),
        "the valid sibling executes"
    );
    assert!(resp.responses[1].item.is_some());
}

#[test]
fn test_execute_statement_parse_error_message() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");

    // A single malformed statement rejects with the same envelope and detail.
    let err = db
        .execute_statement(ExecuteStatementRequest {
            statement: "SLECT * FROM \"Users\"".to_string(),
            parameters: None,
            ..Default::default()
        })
        .unwrap_err();
    match err {
        DynoxideError::ValidationException(msg) => assert_eq!(
            msg,
            "Statement wasn't well formed, can't be processed: Expected data manipulation"
        ),
        other => panic!("expected ValidationException, got {other:?}"),
    }
}

#[test]
fn test_batch_invalid_delete_returning_variant_is_a_per_statement_error() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");
    put_test_item(&db, "Users", "b1", "Batch");

    // The batch call itself succeeds; the invalid variant surfaces as a
    // per-statement error, not a thrown exception.
    let resp = db
        .batch_execute_statement(BatchExecuteStatementRequest {
            return_consumed_capacity: None,
            statements: vec![batch_stmt(
                "DELETE FROM \"Users\" WHERE pk = 'b1' RETURNING MODIFIED OLD *".to_string(),
            )],
        })
        .unwrap();
    assert_eq!(resp.responses.len(), 1);
    assert!(resp.responses[0].item.is_none());
    let err = resp.responses[0]
        .error
        .as_ref()
        .expect("an invalid variant surfaces a per-statement error");
    assert_eq!(err.code, "ValidationError");
    assert_eq!(
        err.message,
        "Invalid returning clause: RETURNING MODIFIED OLD *. Only RETURNING ALL OLD * is allowed in DELETE statements."
    );
    assert!(
        resp.responses[0].table_name.is_none(),
        "TableName is not echoed on a per-statement error"
    );

    // The rejected statement must not have deleted the item.
    let sel = exec(&db, "SELECT * FROM \"Users\" WHERE pk = 'b1'");
    assert_eq!(sel.items.unwrap().len(), 1);
}

#[test]
fn test_transaction_delete_returning_is_rejected() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");
    put_test_item(&db, "Users", "t1", "Trans");

    let err = db
        .execute_transaction(ExecuteTransactionRequest {
            transact_statements: vec![ParameterizedStatement {
                statement: "DELETE FROM \"Users\" WHERE pk = 't1' RETURNING ALL OLD *".to_string(),
                parameters: None,
            }],
            ..Default::default()
        })
        .unwrap_err();

    match err {
        DynoxideError::ValidationException(msg) => assert_eq!(
            msg,
            "Validation failed in TransactStatements[0]: RETURNING clause is not supported in ExecuteTransaction."
        ),
        other => panic!("expected ValidationException, got {other:?}"),
    }

    // The write must not have applied.
    let sel = exec(&db, "SELECT * FROM \"Users\" WHERE pk = 't1'");
    assert_eq!(sel.items.unwrap().len(), 1);
}

#[test]
fn test_transaction_update_returning_is_rejected() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");
    put_item_with_attrs(&db, "Users", "t1", &[("data", "old")]);

    let err = db
        .execute_transaction(ExecuteTransactionRequest {
            transact_statements: vec![ParameterizedStatement {
                statement: "UPDATE \"Users\" SET data = 'new' WHERE pk = 't1' RETURNING ALL NEW *"
                    .to_string(),
                parameters: None,
            }],
            ..Default::default()
        })
        .unwrap_err();

    match err {
        DynoxideError::ValidationException(msg) => assert_eq!(
            msg,
            "Validation failed in TransactStatements[0]: RETURNING clause is not supported in ExecuteTransaction."
        ),
        other => panic!("expected ValidationException, got {other:?}"),
    }

    // The write must not have applied; data stays 'old'.
    let sel = exec(&db, "SELECT * FROM \"Users\" WHERE pk = 't1'");
    assert_eq!(
        sel.items.unwrap()[0].get("data"),
        Some(&AttributeValue::S("old".to_string()))
    );
}

#[test]
fn test_transaction_returning_reports_offending_index_and_rolls_back() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");
    put_test_item(&db, "Users", "keep-a", "A");
    put_test_item(&db, "Users", "keep-b", "B");

    // The RETURNING clause is on the second member (index 1); the whole
    // transaction is rejected before any write, so neither delete applies.
    let err = db
        .execute_transaction(ExecuteTransactionRequest {
            transact_statements: vec![
                ParameterizedStatement {
                    statement: "DELETE FROM \"Users\" WHERE pk = 'keep-a'".to_string(),
                    parameters: None,
                },
                ParameterizedStatement {
                    statement: "DELETE FROM \"Users\" WHERE pk = 'keep-b' RETURNING ALL OLD *"
                        .to_string(),
                    parameters: None,
                },
            ],
            ..Default::default()
        })
        .unwrap_err();

    match err {
        DynoxideError::ValidationException(msg) => assert_eq!(
            msg,
            "Validation failed in TransactStatements[1]: RETURNING clause is not supported in ExecuteTransaction."
        ),
        other => panic!("expected ValidationException, got {other:?}"),
    }

    // Neither item was deleted.
    assert_eq!(
        exec(&db, "SELECT * FROM \"Users\" WHERE pk = 'keep-a'")
            .items
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        exec(&db, "SELECT * FROM \"Users\" WHERE pk = 'keep-b'")
            .items
            .unwrap()
            .len(),
        1
    );
}

// -----------------------------------------------------------------------
// Parameterized queries
// -----------------------------------------------------------------------

#[test]
fn test_parameterized_select() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");
    put_test_item(&db, "Users", "u1", "Alice");
    put_test_item(&db, "Users", "u2", "Bob");

    let resp = exec_with_params(
        &db,
        "SELECT * FROM \"Users\" WHERE pk = ?",
        vec![AttributeValue::S("u1".to_string())],
    );
    let items = resp.items.unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(
        items[0].get("name"),
        Some(&AttributeValue::S("Alice".to_string()))
    );
}

#[test]
fn test_parameterized_insert() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");

    // Parameters don't apply to INSERT VALUE literals directly,
    // but we can test that params work in general
    exec(
        &db,
        "INSERT INTO \"Users\" VALUE {'pk': 'u1', 'name': 'Dave'}",
    );

    let sel = exec_with_params(
        &db,
        "SELECT * FROM \"Users\" WHERE pk = ?",
        vec![AttributeValue::S("u1".to_string())],
    );
    let items = sel.items.unwrap();
    assert_eq!(items.len(), 1);
}

// -----------------------------------------------------------------------
// Error handling
// -----------------------------------------------------------------------

#[test]
fn test_invalid_partiql_syntax() {
    let db = Database::memory().unwrap();

    let result = db.execute_statement(ExecuteStatementRequest {
        statement: "INVALID SYNTAX HERE".to_string(),
        parameters: None,
        ..Default::default()
    });

    assert!(result.is_err());
}

#[test]
fn test_table_not_found() {
    let db = Database::memory().unwrap();

    let result = db.execute_statement(ExecuteStatementRequest {
        statement: "SELECT * FROM \"NonExistent\"".to_string(),
        parameters: None,
        ..Default::default()
    });

    assert!(result.is_err());
}

// -----------------------------------------------------------------------
// EXISTS / BEGINS_WITH functions
// -----------------------------------------------------------------------

#[test]
fn test_select_where_exists() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");
    put_test_item(&db, "Users", "u1", "Alice");

    // Add an item with an email attribute
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("u2".to_string()));
    item.insert("name".to_string(), AttributeValue::S("Bob".to_string()));
    item.insert(
        "email".to_string(),
        AttributeValue::S("bob@example.com".to_string()),
    );
    db.put_item(PutItemRequest {
        table_name: "Users".to_string(),
        item,
        condition_expression: None,
        expression_attribute_names: None,
        expression_attribute_values: None,
        return_values: None,
        ..Default::default()
    })
    .unwrap();

    let resp = exec(&db, "SELECT * FROM \"Users\" WHERE EXISTS(email)");
    let items = resp.items.unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(
        items[0].get("name"),
        Some(&AttributeValue::S("Bob".to_string()))
    );
}

#[test]
fn test_select_where_not_exists() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");
    put_test_item(&db, "Users", "u1", "Alice");

    // u2 has email
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("u2".to_string()));
    item.insert("name".to_string(), AttributeValue::S("Bob".to_string()));
    item.insert(
        "email".to_string(),
        AttributeValue::S("bob@example.com".to_string()),
    );
    db.put_item(PutItemRequest {
        table_name: "Users".to_string(),
        item,
        condition_expression: None,
        expression_attribute_names: None,
        expression_attribute_values: None,
        return_values: None,
        ..Default::default()
    })
    .unwrap();

    let resp = exec(&db, "SELECT * FROM \"Users\" WHERE NOT EXISTS(email)");
    let items = resp.items.unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(
        items[0].get("name"),
        Some(&AttributeValue::S("Alice".to_string()))
    );
}

#[test]
fn test_select_where_begins_with() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");
    put_test_item(&db, "Users", "user_1", "Alice");
    put_test_item(&db, "Users", "user_2", "Bob");
    put_test_item(&db, "Users", "admin_1", "Charlie");

    let resp = exec(
        &db,
        "SELECT * FROM \"Users\" WHERE BEGINS_WITH(pk, 'user_')",
    );
    let items = resp.items.unwrap();
    assert_eq!(items.len(), 2);

    let names: Vec<&str> = items
        .iter()
        .filter_map(|i| match i.get("name") {
            Some(AttributeValue::S(s)) => Some(s.as_str()),
            _ => None,
        })
        .collect();
    assert!(names.contains(&"Alice"));
    assert!(names.contains(&"Bob"));
}

#[test]
fn test_select_combined_conditions_with_exists() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");

    // u1 has email
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("u1".to_string()));
    item.insert("name".to_string(), AttributeValue::S("Alice".to_string()));
    item.insert(
        "email".to_string(),
        AttributeValue::S("alice@test.com".to_string()),
    );
    db.put_item(PutItemRequest {
        table_name: "Users".to_string(),
        item,
        condition_expression: None,
        expression_attribute_names: None,
        expression_attribute_values: None,
        return_values: None,
        ..Default::default()
    })
    .unwrap();

    put_test_item(&db, "Users", "u2", "Bob"); // no email

    // Combine pk equality + EXISTS
    let resp = exec(
        &db,
        "SELECT * FROM \"Users\" WHERE pk = 'u1' AND EXISTS(email)",
    );
    let items = resp.items.unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(
        items[0].get("name"),
        Some(&AttributeValue::S("Alice".to_string()))
    );
}

// -----------------------------------------------------------------------
// BatchExecuteStatement
// -----------------------------------------------------------------------

#[test]
fn test_batch_execute_mixed_operations() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");

    let resp = db
        .batch_execute_statement(BatchExecuteStatementRequest {
            return_consumed_capacity: None,
            statements: vec![
                batch_stmt("INSERT INTO \"Users\" VALUE {'pk': 'u1', 'name': 'Alice'}".to_string()),
                batch_stmt("INSERT INTO \"Users\" VALUE {'pk': 'u2', 'name': 'Bob'}".to_string()),
            ],
        })
        .unwrap();

    assert_eq!(resp.responses.len(), 2);
    assert!(resp.responses[0].error.is_none());
    assert!(resp.responses[1].error.is_none());

    // Verify items exist
    let sel = exec(&db, "SELECT * FROM \"Users\"");
    assert_eq!(sel.items.unwrap().len(), 2);
}

#[test]
fn test_batch_execute_partial_failure() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");

    // An all-write batch: a batch mixing reads and writes is rejected up front
    // (see test_batch_execute_rejects_mixed_read_and_write), so partial failure
    // is exercised with a second write against a table that does not exist.
    let resp = db
        .batch_execute_statement(BatchExecuteStatementRequest {
            return_consumed_capacity: None,
            statements: vec![
                batch_stmt("INSERT INTO \"Users\" VALUE {'pk': 'u1', 'name': 'Alice'}".to_string()),
                batch_stmt("INSERT INTO \"NonExistent\" VALUE {'pk': 'u2'}".to_string()),
            ],
        })
        .unwrap();

    assert_eq!(resp.responses.len(), 2);
    assert!(resp.responses[0].error.is_none()); // insert succeeded
    assert!(resp.responses[1].error.is_some()); // insert failed, table not found
}

#[test]
fn test_batch_execute_rejects_duplicate_item_keys() {
    // Captured against real DynamoDB: two statements against one item are
    // rejected before any of them runs, the same way a transaction rejects
    // duplicate targets.
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");

    let err = db
        .batch_execute_statement(BatchExecuteStatementRequest {
            return_consumed_capacity: None,
            statements: vec![
                batch_stmt("INSERT INTO \"Users\" VALUE {'pk': 'dup'}".to_string()),
                batch_stmt("UPDATE \"Users\" SET name='x' WHERE pk='dup'".to_string()),
            ],
        })
        .expect_err("two statements against one item are rejected");

    assert!(
        err.to_string()
            .contains("Provided list of item keys contains duplicates"),
        "unexpected error: {err}"
    );
}

#[test]
fn test_batch_execute_allows_distinct_item_keys() {
    // The mirror of the case above: distinct keys in one batch are fine.
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");

    let resp = db
        .batch_execute_statement(BatchExecuteStatementRequest {
            return_consumed_capacity: None,
            statements: vec![
                batch_stmt("INSERT INTO \"Users\" VALUE {'pk': 'one'}".to_string()),
                batch_stmt("INSERT INTO \"Users\" VALUE {'pk': 'two'}".to_string()),
            ],
        })
        .unwrap();

    assert_eq!(resp.responses.len(), 2);
    assert!(resp.responses.iter().all(|r| r.error.is_none()));
}

#[test]
fn test_batch_execute_rejects_mixed_read_and_write() {
    // Captured against real DynamoDB: a batch carrying both a read and a write
    // is rejected before any statement runs, as a top-level ValidationException
    // rather than a per-statement error.
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");

    let err = db
        .batch_execute_statement(BatchExecuteStatementRequest {
            return_consumed_capacity: None,
            statements: vec![
                batch_stmt("SELECT * FROM \"Users\" WHERE pk = 'u1'".to_string()),
                batch_stmt("INSERT INTO \"Users\" VALUE {'pk': 'u2'}".to_string()),
            ],
        })
        .expect_err("a mixed batch is rejected");

    assert!(
        err.to_string()
            .contains("Read and write requests together in the same batch is not supported"),
        "unexpected error: {err}"
    );
}

#[test]
fn test_batch_execute_exceeds_limit() {
    let db = Database::memory().unwrap();

    let stmts: Vec<BatchStatementRequest> = (0..26)
        .map(|_| batch_stmt("SELECT * FROM \"T\"".to_string()))
        .collect();

    let result = db.batch_execute_statement(BatchExecuteStatementRequest {
        statements: stmts,
        return_consumed_capacity: None,
    });

    assert!(result.is_err());
}

#[test]
fn test_batch_execute_empty_statements_rejected() {
    let db = Database::memory().unwrap();

    let result = db.batch_execute_statement(BatchExecuteStatementRequest {
        statements: vec![],
        return_consumed_capacity: None,
    });

    let err = result.unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("Member must have length greater than or equal to 1"),
        "Expected empty array error, got: {msg}"
    );
}

// -----------------------------------------------------------------------
// Fix #8: WHERE clause gaps (BETWEEN, IN, CONTAINS, IS MISSING, IS NOT MISSING)
// -----------------------------------------------------------------------

fn create_table_with_items_for_where(db: &Database) {
    create_test_table(db, "Items");

    // Insert items with varying ages and optional fields
    for (pk, name, age) in &[
        ("u1", "Alice", 25),
        ("u2", "Bob", 35),
        ("u3", "Charlie", 50),
        ("u4", "Diana", 17),
        ("u5", "Eve", 70),
    ] {
        let mut item = HashMap::new();
        item.insert("pk".to_string(), AttributeValue::S(pk.to_string()));
        item.insert("name".to_string(), AttributeValue::S(name.to_string()));
        item.insert("age".to_string(), AttributeValue::N(age.to_string()));
        db.put_item(PutItemRequest {
            table_name: "Items".to_string(),
            item,
            condition_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            return_values: None,
            ..Default::default()
        })
        .unwrap();
    }

    // u1 gets an email attribute, u2 does not
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("u6".to_string()));
    item.insert("name".to_string(), AttributeValue::S("Frank".to_string()));
    item.insert("age".to_string(), AttributeValue::N("40".to_string()));
    item.insert(
        "email".to_string(),
        AttributeValue::S("frank@example.com".to_string()),
    );
    db.put_item(PutItemRequest {
        table_name: "Items".to_string(),
        item,
        condition_expression: None,
        expression_attribute_names: None,
        expression_attribute_values: None,
        return_values: None,
        ..Default::default()
    })
    .unwrap();
}

#[test]
fn test_where_between_numeric() {
    let db = Database::memory().unwrap();
    create_table_with_items_for_where(&db);

    let resp = exec(&db, "SELECT * FROM \"Items\" WHERE age BETWEEN 18 AND 65");
    let items = resp.items.unwrap();
    // Alice(25), Bob(35), Charlie(50), Frank(40) are between 18 and 65
    // Diana(17) and Eve(70) are out of range
    assert_eq!(items.len(), 4);
    let names: Vec<String> = items
        .iter()
        .filter_map(|i| match i.get("name") {
            Some(AttributeValue::S(s)) => Some(s.clone()),
            _ => None,
        })
        .collect();
    assert!(names.contains(&"Alice".to_string()));
    assert!(names.contains(&"Bob".to_string()));
    assert!(names.contains(&"Charlie".to_string()));
    assert!(names.contains(&"Frank".to_string()));
}

#[test]
fn test_where_in_string_values() {
    let db = Database::memory().unwrap();
    create_table_with_items_for_where(&db);

    let resp = exec(
        &db,
        "SELECT * FROM \"Items\" WHERE name IN ('Alice', 'Bob', 'Eve')",
    );
    let items = resp.items.unwrap();
    assert_eq!(items.len(), 3);
}

#[test]
fn test_where_contains_string() {
    let db = Database::memory().unwrap();
    create_table_with_items_for_where(&db);

    let resp = exec(&db, "SELECT * FROM \"Items\" WHERE CONTAINS(name, 'li')");
    let items = resp.items.unwrap();
    // Alice and Charlie both contain 'li'
    assert_eq!(items.len(), 2);
    let names: Vec<String> = items
        .iter()
        .filter_map(|i| match i.get("name") {
            Some(AttributeValue::S(s)) => Some(s.clone()),
            _ => None,
        })
        .collect();
    assert!(names.contains(&"Alice".to_string()));
    assert!(names.contains(&"Charlie".to_string()));
}

#[test]
fn test_where_is_missing() {
    let db = Database::memory().unwrap();
    create_table_with_items_for_where(&db);

    let resp = exec(&db, "SELECT * FROM \"Items\" WHERE email IS MISSING");
    let items = resp.items.unwrap();
    // Only Frank (u6) has email; the other 5 do not
    assert_eq!(items.len(), 5);
}

#[test]
fn test_where_is_not_missing() {
    let db = Database::memory().unwrap();
    create_table_with_items_for_where(&db);

    let resp = exec(&db, "SELECT * FROM \"Items\" WHERE email IS NOT MISSING");
    let items = resp.items.unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(
        items[0].get("name"),
        Some(&AttributeValue::S("Frank".to_string()))
    );
}

#[test]
fn test_update_with_between() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Scores");

    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("s1".to_string()));
    item.insert("score".to_string(), AttributeValue::N("50".to_string()));
    db.put_item(PutItemRequest {
        table_name: "Scores".to_string(),
        item,
        condition_expression: None,
        expression_attribute_names: None,
        expression_attribute_values: None,
        return_values: None,
        ..Default::default()
    })
    .unwrap();

    // Update should succeed because score 50 is between 0 and 100
    // (Note: UPDATE requires pk in WHERE, BETWEEN is an additional filter)
    exec(
        &db,
        "UPDATE \"Scores\" SET score = 75 WHERE pk = 's1' AND score BETWEEN 0 AND 100",
    );

    let sel = exec(&db, "SELECT * FROM \"Scores\" WHERE pk = 's1'");
    let items = sel.items.unwrap();
    assert_eq!(
        items[0].get("score"),
        Some(&AttributeValue::N("75".to_string()))
    );
}

#[test]
fn test_delete_with_is_missing() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Data");
    put_test_item(&db, "Data", "d1", "WithName");

    // Insert an item without a 'name' attribute (only pk)
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("d2".to_string()));
    db.put_item(PutItemRequest {
        table_name: "Data".to_string(),
        item,
        condition_expression: None,
        expression_attribute_names: None,
        expression_attribute_values: None,
        return_values: None,
        ..Default::default()
    })
    .unwrap();

    // Delete uses pk equality — IS MISSING is not used to identify the item
    // but we can verify that the parser handles it in a WHERE clause
    exec(&db, "DELETE FROM \"Data\" WHERE pk = 'd2'");

    let sel = exec(&db, "SELECT * FROM \"Data\"");
    let items = sel.items.unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(
        items[0].get("pk"),
        Some(&AttributeValue::S("d1".to_string()))
    );
}

#[test]
fn test_combined_between_and_is_not_missing() {
    let db = Database::memory().unwrap();
    create_table_with_items_for_where(&db);

    let resp = exec(
        &db,
        "SELECT * FROM \"Items\" WHERE age BETWEEN 30 AND 60 AND email IS NOT MISSING",
    );
    let items = resp.items.unwrap();
    // Frank: age 40, has email — only match
    assert_eq!(items.len(), 1);
    assert_eq!(
        items[0].get("name"),
        Some(&AttributeValue::S("Frank".to_string()))
    );
}

// -----------------------------------------------------------------------
// Fix #10: Nested path projections
// -----------------------------------------------------------------------

fn create_table_with_nested_items(db: &Database) {
    create_test_table(db, "Nested");

    let mut address = HashMap::new();
    address.insert("city".to_string(), AttributeValue::S("London".to_string()));
    address.insert(
        "postcode".to_string(),
        AttributeValue::S("SW1A 1AA".to_string()),
    );

    let mut deep = HashMap::new();
    deep.insert("c".to_string(), AttributeValue::N("42".to_string()));

    let mut b_map = HashMap::new();
    b_map.insert("b".to_string(), AttributeValue::M(deep));

    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("x".to_string()));
    item.insert("address".to_string(), AttributeValue::M(address));
    item.insert("a".to_string(), AttributeValue::M(b_map));
    item.insert(
        "tags".to_string(),
        AttributeValue::L(vec![
            AttributeValue::S("first".to_string()),
            AttributeValue::S("second".to_string()),
        ]),
    );

    db.put_item(PutItemRequest {
        table_name: "Nested".to_string(),
        item,
        condition_expression: None,
        expression_attribute_names: None,
        expression_attribute_values: None,
        return_values: None,
        ..Default::default()
    })
    .unwrap();
}

#[test]
fn test_select_nested_path() {
    let db = Database::memory().unwrap();
    create_table_with_nested_items(&db);

    let resp = exec(&db, "SELECT address.city FROM \"Nested\" WHERE pk = 'x'");
    let items = resp.items.unwrap();
    assert_eq!(items.len(), 1);
    // DynamoDB PartiQL returns the leaf value keyed by the leaf attribute name
    assert_eq!(
        items[0].get("city"),
        Some(&AttributeValue::S("London".to_string()))
    );
    // Only the projected path should be present (not parent, not pk)
    assert!(!items[0].contains_key("pk"));
    assert!(!items[0].contains_key("address"));
}

#[test]
fn test_select_multiple_nested_paths() {
    let db = Database::memory().unwrap();
    create_table_with_nested_items(&db);

    let resp = exec(
        &db,
        "SELECT a.b.c, address.postcode FROM \"Nested\" WHERE pk = 'x'",
    );
    let items = resp.items.unwrap();
    assert_eq!(items.len(), 1);
    // a.b.c → leaf key "c" with value 42
    assert_eq!(
        items[0].get("c"),
        Some(&AttributeValue::N("42".to_string()))
    );
    // address.postcode → leaf key "postcode"
    assert_eq!(
        items[0].get("postcode"),
        Some(&AttributeValue::S("SW1A 1AA".to_string()))
    );
}

#[test]
fn test_select_nonexistent_nested_path() {
    let db = Database::memory().unwrap();
    create_table_with_nested_items(&db);

    let resp = exec(&db, "SELECT address.country FROM \"Nested\" WHERE pk = 'x'");
    let items = resp.items.unwrap();
    // Item exists but the projected path does not, so no key should be present
    assert_eq!(items.len(), 1);
    assert!(!items[0].contains_key("country"));
    assert!(!items[0].contains_key("address"));
}

#[test]
fn test_select_star_still_works_with_nested_data() {
    let db = Database::memory().unwrap();
    create_table_with_nested_items(&db);

    let resp = exec(&db, "SELECT * FROM \"Nested\"");
    let items = resp.items.unwrap();
    assert_eq!(items.len(), 1);
    assert!(items[0].contains_key("pk"));
    assert!(items[0].contains_key("address"));
    assert!(items[0].contains_key("a"));
}

#[test]
fn test_select_array_index_path() {
    let db = Database::memory().unwrap();
    create_table_with_nested_items(&db);

    let resp = exec(&db, "SELECT tags[0] FROM \"Nested\" WHERE pk = 'x'");
    let items = resp.items.unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(
        items[0].get("tags[0]"),
        Some(&AttributeValue::S("first".to_string()))
    );
}

#[test]
fn test_select_nested_map_path() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "NestedTest");

    let mut nested = HashMap::new();
    nested.insert("nested".to_string(), AttributeValue::S("deep".to_string()));

    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("key".to_string()));
    item.insert("mymap".to_string(), AttributeValue::M(nested));

    db.put_item(PutItemRequest {
        table_name: "NestedTest".to_string(),
        item,
        ..Default::default()
    })
    .unwrap();

    let resp = exec(
        &db,
        r#"SELECT "mymap"."nested" FROM "NestedTest" WHERE pk = 'key'"#,
    );
    let items = resp.items.unwrap();
    assert_eq!(items.len(), 1);

    // DynamoDB PartiQL returns the leaf value keyed by the leaf attribute name
    assert_eq!(
        items[0].get("nested"),
        Some(&AttributeValue::S("deep".to_string())),
        "expected leaf key 'nested' with the resolved value, got: {:?}",
        items[0]
    );
    // Should NOT have the parent map key
    assert!(
        !items[0].contains_key("mymap"),
        "should not have parent 'mymap' key in result"
    );
}

#[test]
fn test_select_multiple_nested_map_paths() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "NestedTest2");

    let mut nested = HashMap::new();
    nested.insert("alpha".to_string(), AttributeValue::S("one".to_string()));
    nested.insert("beta".to_string(), AttributeValue::S("two".to_string()));

    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("key".to_string()));
    item.insert("mymap".to_string(), AttributeValue::M(nested));

    db.put_item(PutItemRequest {
        table_name: "NestedTest2".to_string(),
        item,
        ..Default::default()
    })
    .unwrap();

    let resp = exec(
        &db,
        r#"SELECT "mymap"."alpha", "mymap"."beta" FROM "NestedTest2" WHERE pk = 'key'"#,
    );
    let items = resp.items.unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(
        items[0].get("alpha"),
        Some(&AttributeValue::S("one".to_string()))
    );
    assert_eq!(
        items[0].get("beta"),
        Some(&AttributeValue::S("two".to_string()))
    );
}

#[test]
fn test_select_nested_path_missing_returns_empty() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "NestedTest3");

    let mut nested = HashMap::new();
    nested.insert("exists".to_string(), AttributeValue::S("val".to_string()));

    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("key".to_string()));
    item.insert("mymap".to_string(), AttributeValue::M(nested));

    db.put_item(PutItemRequest {
        table_name: "NestedTest3".to_string(),
        item,
        ..Default::default()
    })
    .unwrap();

    let resp = exec(
        &db,
        r#"SELECT "mymap"."nonexistent" FROM "NestedTest3" WHERE pk = 'key'"#,
    );
    let items = resp.items.unwrap();
    assert_eq!(items.len(), 1);
    // Non-existent nested path should be absent, not error
    assert!(!items[0].contains_key("nonexistent"));
}

// ── Issue #40: bracket IN lists and negated predicates ────────────────────

#[test]
fn test_where_in_bracket_list() {
    // PartiQL must accept the bracket `IN [...]` form, not just `IN (...)`.
    let db = Database::memory().unwrap();
    create_test_table(&db, "Neg");
    put_test_item(&db, "Neg", "a", "alpha");
    put_test_item(&db, "Neg", "b", "beta");
    put_test_item(&db, "Neg", "c", "gamma");

    let resp = exec(&db, "SELECT * FROM \"Neg\" WHERE pk IN ['a','b']");
    assert_eq!(resp.items.unwrap().len(), 2);
}

#[test]
fn test_where_in_bracket_single_element() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Neg");
    put_test_item(&db, "Neg", "a", "alpha");
    put_test_item(&db, "Neg", "b", "beta");

    let resp = exec(&db, "SELECT * FROM \"Neg\" WHERE pk IN ['a']");
    let items = resp.items.unwrap();
    assert_eq!(items.len(), 1);
    assert!(matches!(items[0].get("pk"), Some(AttributeValue::S(s)) if s == "a"));
}

#[test]
fn test_where_not_begins_with_with_bracket_in() {
    // Mirrors the conformance statement: bracket IN bundled with NOT begins_with.
    // Only 'beta' does not begin with 'al'.
    let db = Database::memory().unwrap();
    create_test_table(&db, "Neg");
    put_test_item(&db, "Neg", "pq-neg-a", "alpha");
    put_test_item(&db, "Neg", "pq-neg-b", "beta");

    let resp = exec(
        &db,
        "SELECT * FROM \"Neg\" WHERE pk IN ['pq-neg-a','pq-neg-b'] AND NOT begins_with(\"name\", 'al')",
    );
    let items = resp.items.unwrap();
    assert_eq!(items.len(), 1);
    assert!(matches!(items[0].get("pk"), Some(AttributeValue::S(s)) if s == "pq-neg-b"));
}

#[test]
fn test_where_is_not_missing_combined() {
    // IS NOT MISSING already parses and evaluates; lock it in alongside an equality.
    let db = Database::memory().unwrap();
    create_test_table(&db, "Neg");
    put_test_item(&db, "Neg", "pq-neg-a", "alpha");

    let resp = exec(
        &db,
        "SELECT * FROM \"Neg\" WHERE pk = 'pq-neg-a' AND \"name\" IS NOT MISSING",
    );
    assert_eq!(resp.items.unwrap().len(), 1);
}

#[test]
fn test_where_in_parenthesised_still_works() {
    // Regression: the existing `IN (...)` form must keep working.
    let db = Database::memory().unwrap();
    create_test_table(&db, "Neg");
    put_test_item(&db, "Neg", "a", "alpha");
    put_test_item(&db, "Neg", "b", "beta");

    let resp = exec(&db, "SELECT * FROM \"Neg\" WHERE pk IN ('a','b')");
    assert_eq!(resp.items.unwrap().len(), 2);
}

// -----------------------------------------------------------------------
// ConsumedCapacity (#37)
// -----------------------------------------------------------------------

/// #37: PartiQL ExecuteStatement returns a populated ConsumedCapacity block
/// when ReturnConsumedCapacity is requested, rather than omitting it.
#[test]
fn test_execute_statement_returns_consumed_capacity() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Pql");

    db.execute_statement(ExecuteStatementRequest {
        statement: "INSERT INTO \"Pql\" VALUE { 'pk': 'cc', 'v': 1 }".to_string(),
        parameters: None,
        ..Default::default()
    })
    .unwrap();

    let resp = db
        .execute_statement(ExecuteStatementRequest {
            statement: "SELECT * FROM \"Pql\" WHERE pk = 'cc'".to_string(),
            parameters: None,
            return_consumed_capacity: Some("TOTAL".to_string()),
            ..Default::default()
        })
        .unwrap();

    let cc = resp
        .consumed_capacity
        .expect("ConsumedCapacity must be present when requested");
    assert!(
        cc.capacity_units > 0.0,
        "CapacityUnits should be greater than zero: {cc:?}"
    );
}

// -----------------------------------------------------------------------
// DELETE / UPDATE WHERE as a condition (#54)
// -----------------------------------------------------------------------

/// #54: a non-key WHERE predicate that is false on a present item makes DELETE
/// raise ConditionalCheckFailedException and leaves the item intact.
#[test]
fn test_partiql_delete_condition_false_fails_and_keeps_item() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Cond");
    exec(
        &db,
        "INSERT INTO \"Cond\" VALUE {'pk': 'p1', 'kind': 'alpha'}",
    );

    let err = db
        .execute_statement(ExecuteStatementRequest {
            statement: "DELETE FROM \"Cond\" WHERE pk = 'p1' AND kind = 'beta'".to_string(),
            parameters: None,
            ..Default::default()
        })
        .unwrap_err();
    assert!(
        err.to_string().contains("conditional request failed"),
        "expected ConditionalCheckFailed, got: {err}"
    );

    // The item must survive.
    let sel = exec(&db, "SELECT * FROM \"Cond\" WHERE pk = 'p1'");
    assert_eq!(sel.items.unwrap().len(), 1);
}

/// #54: a true non-key predicate lets DELETE through.
#[test]
fn test_partiql_delete_condition_true_deletes() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Cond");
    exec(
        &db,
        "INSERT INTO \"Cond\" VALUE {'pk': 'p1', 'kind': 'alpha'}",
    );

    exec(
        &db,
        "DELETE FROM \"Cond\" WHERE pk = 'p1' AND kind = 'alpha'",
    );

    let sel = exec(&db, "SELECT * FROM \"Cond\" WHERE pk = 'p1'");
    assert!(sel.items.unwrap().is_empty());
}

/// #54: NOT begins_with as a false condition also blocks the delete.
#[test]
fn test_partiql_delete_not_begins_with_condition_false_fails() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Cond");
    exec(
        &db,
        "INSERT INTO \"Cond\" VALUE {'pk': 'p1', 'kind': 'alpha'}",
    );

    // kind='alpha' begins with 'al', so NOT begins_with(...) is false.
    let err = db
        .execute_statement(ExecuteStatementRequest {
            statement: "DELETE FROM \"Cond\" WHERE pk = 'p1' AND NOT begins_with(\"kind\", 'al')"
                .to_string(),
            parameters: None,
            ..Default::default()
        })
        .unwrap_err();
    assert!(
        err.to_string().contains("conditional request failed"),
        "expected ConditionalCheckFailed, got: {err}"
    );
    assert_eq!(
        exec(&db, "SELECT * FROM \"Cond\" WHERE pk = 'p1'")
            .items
            .unwrap()
            .len(),
        1
    );
}

/// #54: a false non-key predicate makes UPDATE raise ConditionalCheckFailed and
/// leaves the item unchanged.
#[test]
fn test_partiql_update_condition_false_fails_and_keeps_item() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Cond");
    exec(
        &db,
        "INSERT INTO \"Cond\" VALUE {'pk': 'p1', 'kind': 'alpha'}",
    );

    let err = db
        .execute_statement(ExecuteStatementRequest {
            statement: "UPDATE \"Cond\" SET label = 'x' WHERE pk = 'p1' AND kind = 'beta'"
                .to_string(),
            parameters: None,
            ..Default::default()
        })
        .unwrap_err();
    assert!(
        err.to_string().contains("conditional request failed"),
        "expected ConditionalCheckFailed, got: {err}"
    );

    // No label was written.
    let item = &exec(&db, "SELECT * FROM \"Cond\" WHERE pk = 'p1'")
        .items
        .unwrap()[0];
    assert!(!item.contains_key("label"));
}

/// #54: a true non-key predicate lets UPDATE through.
#[test]
fn test_partiql_update_condition_true_updates() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Cond");
    exec(
        &db,
        "INSERT INTO \"Cond\" VALUE {'pk': 'p1', 'kind': 'alpha'}",
    );

    exec(
        &db,
        "UPDATE \"Cond\" SET label = 'done' WHERE pk = 'p1' AND kind = 'alpha'",
    );

    let item = &exec(&db, "SELECT * FROM \"Cond\" WHERE pk = 'p1'")
        .items
        .unwrap()[0];
    assert_eq!(item.get("label"), Some(&AttributeValue::S("done".into())));
}

/// #54: a DELETE whose key matches no item is a silent no-op, even with a
/// non-key predicate present (a missing item is not a condition failure).
#[test]
fn test_partiql_delete_missing_key_is_noop_not_condition_failure() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Cond");

    // Must not error.
    exec(
        &db,
        "DELETE FROM \"Cond\" WHERE pk = 'absent' AND kind = 'x'",
    );
}

/// #54: a write WHERE without the full primary key is a ValidationException with
/// the AWS message.
#[test]
fn test_partiql_delete_without_pk_rejected() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Cond");
    exec(
        &db,
        "INSERT INTO \"Cond\" VALUE {'pk': 'p1', 'kind': 'alpha'}",
    );

    let err = db
        .execute_statement(ExecuteStatementRequest {
            statement: "DELETE FROM \"Cond\" WHERE kind = 'alpha'".to_string(),
            parameters: None,
            ..Default::default()
        })
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("Where clause does not contain a mandatory equality on all key attributes"),
        "got: {err}"
    );
}

// ---------------------------------------------------------------------------
// SELECT pagination
// ---------------------------------------------------------------------------

/// Walk a SELECT the way a client does, returning every page's row count and
/// the primary keys seen, so a test can check for gaps and duplicates.
fn paginate(db: &Database, statement: &str, limit: usize) -> (Vec<usize>, Vec<String>) {
    let mut pages = Vec::new();
    let mut keys = Vec::new();
    let mut next_token = None;

    loop {
        let response = db
            .execute_statement(ExecuteStatementRequest {
                statement: statement.to_string(),
                limit: Some(limit),
                next_token,
                ..Default::default()
            })
            .unwrap();
        let items = response.items.unwrap_or_default();
        pages.push(items.len());
        for item in &items {
            if let Some(AttributeValue::S(pk)) = item.get("pk") {
                keys.push(pk.clone());
            }
        }
        next_token = response.next_token;
        if next_token.is_none() {
            break;
        }
        assert!(pages.len() < 20, "pagination did not terminate");
    }
    (pages, keys)
}

#[test]
fn test_select_pagination_walks_every_row_once() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Paged");
    for i in 0..7 {
        put_test_item(&db, "Paged", &format!("k{i}"), "n");
    }

    let (pages, mut keys) = paginate(&db, "SELECT * FROM \"Paged\"", 3);

    assert_eq!(
        pages,
        vec![3, 3, 1],
        "pages should fill before the remainder"
    );
    keys.sort();
    keys.dedup();
    assert_eq!(keys.len(), 7, "every row exactly once, no gaps or repeats");
}

#[test]
fn test_select_without_limit_returns_everything_and_no_token() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Paged");
    for i in 0..5 {
        put_test_item(&db, "Paged", &format!("k{i}"), "n");
    }

    let response = exec(&db, "SELECT * FROM \"Paged\"");
    assert_eq!(response.items.unwrap().len(), 5);
    assert!(response.next_token.is_none());
}

#[test]
fn test_select_limit_equal_to_the_row_count_still_offers_one_more_page() {
    // Limit bounds rows read, so a read that stops on the limit owes a token
    // even when it happened to consume the table. The page after it is empty
    // and ends the walk, which is what DynamoDB does and what Query does here.
    let db = Database::memory().unwrap();
    create_test_table(&db, "Paged");
    for i in 0..3 {
        put_test_item(&db, "Paged", &format!("k{i}"), "n");
    }

    let first = db
        .execute_statement(ExecuteStatementRequest {
            statement: "SELECT * FROM \"Paged\"".to_string(),
            limit: Some(3),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(first.items.unwrap().len(), 3);
    let token = first
        .next_token
        .expect("a read that stopped on the limit owes a token");

    let second = db
        .execute_statement(ExecuteStatementRequest {
            statement: "SELECT * FROM \"Paged\"".to_string(),
            limit: Some(3),
            next_token: Some(token),
            ..Default::default()
        })
        .unwrap();
    assert!(second.items.unwrap().is_empty());
    assert!(second.next_token.is_none(), "and the walk ends there");
}

#[test]
fn test_select_limit_counts_rows_read_not_rows_matched() {
    // DynamoDB's Limit bounds items evaluated, so a filtered SELECT can return
    // fewer than Limit - or nothing at all - and still hand back a token.
    let db = Database::memory().unwrap();
    create_test_table(&db, "Paged");
    for i in 0..6 {
        let name = if i == 5 { "keep" } else { "drop" };
        put_test_item(&db, "Paged", &format!("k{i}"), name);
    }

    let first = db
        .execute_statement(ExecuteStatementRequest {
            statement: "SELECT * FROM \"Paged\" WHERE name = 'keep'".to_string(),
            limit: Some(2),
            ..Default::default()
        })
        .unwrap();
    assert!(
        first.items.unwrap().len() < 2,
        "two rows read, at most one of them matches"
    );
    assert!(
        first.next_token.is_some(),
        "the read stopped on the limit, so more rows remain"
    );
}

#[test]
fn test_select_pagination_applies_the_where_clause_to_every_page() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Paged");
    for i in 0..6 {
        let name = if i % 2 == 0 { "keep" } else { "drop" };
        put_test_item(&db, "Paged", &format!("k{i}"), name);
    }

    let (_, mut keys) = paginate(&db, "SELECT * FROM \"Paged\" WHERE name = 'keep'", 2);
    // Page sizes vary, because Limit bounds rows read rather than rows matched.
    // What must hold is that the walk yields every match exactly once.
    keys.sort();
    assert_eq!(keys, vec!["k0", "k2", "k4"]);
}

#[test]
fn test_select_pagination_survives_a_projection_that_drops_the_key() {
    // The cursor is taken before projection, so projecting away the key must
    // not break the continuation.
    let db = Database::memory().unwrap();
    create_test_table(&db, "Paged");
    for i in 0..4 {
        put_test_item(&db, "Paged", &format!("k{i}"), "n");
    }

    let mut seen = 0;
    let mut next_token = None;
    loop {
        let response = db
            .execute_statement(ExecuteStatementRequest {
                statement: "SELECT name FROM \"Paged\"".to_string(),
                limit: Some(2),
                next_token,
                ..Default::default()
            })
            .unwrap();
        let items = response.items.unwrap_or_default();
        assert!(items.iter().all(|i| i.get("pk").is_none()));
        seen += items.len();
        next_token = response.next_token;
        if next_token.is_none() {
            break;
        }
        assert!(seen <= 4, "pagination did not terminate");
    }
    assert_eq!(seen, 4);
}

#[test]
fn test_select_rejects_a_malformed_next_token() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Paged");

    let err = db
        .execute_statement(ExecuteStatementRequest {
            statement: "SELECT * FROM \"Paged\"".to_string(),
            limit: Some(1),
            next_token: Some("not-a-token".to_string()),
            ..Default::default()
        })
        .unwrap_err();
    assert!(
        matches!(err, DynoxideError::ValidationException(ref m) if m == "Invalid NextToken"),
        "unexpected error: {err:?}"
    );
}

#[test]
fn test_next_token_minted_for_one_table_is_rejected_by_another() {
    // A token is a position in one table's ordering and means nothing in
    // another's.
    let db = Database::memory().unwrap();
    create_test_table(&db, "PagedA");
    create_test_table(&db, "PagedB");
    for i in 0..3 {
        put_test_item(&db, "PagedA", &format!("k{i}"), "n");
    }
    put_test_item(&db, "PagedB", "k0", "n");

    let token = db
        .execute_statement(ExecuteStatementRequest {
            statement: "SELECT * FROM \"PagedA\"".to_string(),
            limit: Some(1),
            ..Default::default()
        })
        .unwrap()
        .next_token
        .expect("a read that stopped on the limit owes a token");

    let err = db
        .execute_statement(ExecuteStatementRequest {
            statement: "SELECT * FROM \"PagedB\"".to_string(),
            limit: Some(1),
            next_token: Some(token),
            ..Default::default()
        })
        .unwrap_err();
    assert!(
        matches!(err, DynoxideError::ValidationException(ref m) if m == "NextToken does not match request"),
        "unexpected error: {err:?}"
    );
}

#[test]
fn test_next_token_is_rejected_on_a_write_statement() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Paged");

    let err = db
        .execute_statement(ExecuteStatementRequest {
            statement: "INSERT INTO \"Paged\" VALUE {'pk': 'k1'}".to_string(),
            next_token: Some("anything".to_string()),
            ..Default::default()
        })
        .unwrap_err();
    assert!(
        matches!(err, DynoxideError::ValidationException(_)),
        "{err:?}"
    );
}

#[test]
fn test_select_pagination_continues_after_the_cursor_row_is_deleted() {
    // The token carries the last row's storage keys, and the next page resumes
    // with an exclusive-start comparison on them, so deleting the row a page
    // stopped on does not strand the walk.
    let db = Database::memory().unwrap();
    create_test_table(&db, "Paged");
    for i in 0..6 {
        put_test_item(&db, "Paged", &format!("k{i}"), "n");
    }

    let first = db
        .execute_statement(ExecuteStatementRequest {
            statement: "SELECT * FROM \"Paged\"".to_string(),
            limit: Some(2),
            ..Default::default()
        })
        .unwrap();
    let returned = first.items.unwrap();
    let last_pk = match returned.last().unwrap().get("pk") {
        Some(AttributeValue::S(pk)) => pk.clone(),
        other => panic!("expected a string key, got {other:?}"),
    };
    exec(
        &db,
        &format!("DELETE FROM \"Paged\" WHERE pk = '{last_pk}'"),
    );

    let second = db
        .execute_statement(ExecuteStatementRequest {
            statement: "SELECT * FROM \"Paged\"".to_string(),
            limit: Some(2),
            next_token: first.next_token,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(second.items.unwrap().len(), 2, "the walk continues");
}

#[test]
fn test_select_pagination_still_sees_every_original_row_after_a_mid_walk_insert() {
    // Rows written between pages can land ahead of the cursor and push it
    // further down the order. The walk must still reach every row that was
    // there when it started.
    let db = Database::memory().unwrap();
    create_test_table(&db, "Paged");
    for i in 0..8 {
        put_test_item(&db, "Paged", &format!("orig{i}"), "n");
    }

    let mut seen = Vec::new();
    let mut next_token = None;
    let mut inserted = false;
    loop {
        let response = db
            .execute_statement(ExecuteStatementRequest {
                statement: "SELECT * FROM \"Paged\"".to_string(),
                limit: Some(2),
                next_token,
                ..Default::default()
            })
            .unwrap();
        for item in response.items.unwrap_or_default() {
            if let Some(AttributeValue::S(pk)) = item.get("pk") {
                seen.push(pk.clone());
            }
        }
        if !inserted {
            for i in 0..5 {
                put_test_item(&db, "Paged", &format!("added{i}"), "n");
            }
            inserted = true;
        }
        next_token = response.next_token;
        if next_token.is_none() {
            break;
        }
        assert!(seen.len() < 40, "pagination did not terminate");
    }

    for i in 0..8 {
        let key = format!("orig{i}");
        assert_eq!(
            seen.iter().filter(|k| **k == key).count(),
            1,
            "{key} should appear exactly once, saw {seen:?}"
        );
    }
}

#[test]
fn test_select_pagination_drains_a_table_page_by_page() {
    // Read a page, delete what it returned, ask for the next. Every row must
    // come back exactly once and the table must end up empty.
    let db = Database::memory().unwrap();
    create_test_table(&db, "Paged");
    for i in 0..10 {
        put_test_item(&db, "Paged", &format!("k{i}"), "n");
    }

    let mut drained = Vec::new();
    let mut next_token = None;
    loop {
        let response = db
            .execute_statement(ExecuteStatementRequest {
                statement: "SELECT * FROM \"Paged\"".to_string(),
                limit: Some(3),
                next_token,
                ..Default::default()
            })
            .unwrap();
        let items = response.items.unwrap_or_default();
        for item in &items {
            if let Some(AttributeValue::S(pk)) = item.get("pk") {
                drained.push(pk.clone());
                exec(&db, &format!("DELETE FROM \"Paged\" WHERE pk = '{pk}'"));
            }
        }
        next_token = response.next_token;
        if next_token.is_none() {
            break;
        }
        assert!(drained.len() <= 10, "pagination did not terminate");
    }

    drained.sort();
    let expected: Vec<String> = (0..10).map(|i| format!("k{i}")).collect();
    let mut expected_sorted = expected.clone();
    expected_sorted.sort();
    assert_eq!(drained, expected_sorted, "every row exactly once");

    let left = exec(&db, "SELECT * FROM \"Paged\"").items.unwrap();
    assert!(
        left.is_empty(),
        "the table should be drained, {left:?} left"
    );
}

#[test]
fn test_select_pagination_returns_every_surviving_row_when_the_cursor_is_deleted() {
    // Asserting the next page is non-empty is not enough: the row after the
    // deleted cursor is what goes missing.
    let db = Database::memory().unwrap();
    create_test_table(&db, "Paged");
    for i in 0..6 {
        put_test_item(&db, "Paged", &format!("k{i}"), "n");
    }

    let first = db
        .execute_statement(ExecuteStatementRequest {
            statement: "SELECT * FROM \"Paged\"".to_string(),
            limit: Some(2),
            ..Default::default()
        })
        .unwrap();
    let mut seen: Vec<String> = first
        .items
        .unwrap()
        .iter()
        .filter_map(|i| match i.get("pk") {
            Some(AttributeValue::S(pk)) => Some(pk.clone()),
            _ => None,
        })
        .collect();
    let cursor_key = seen.last().unwrap().clone();
    exec(
        &db,
        &format!("DELETE FROM \"Paged\" WHERE pk = '{cursor_key}'"),
    );

    let mut next_token = first.next_token;
    while let Some(token) = next_token {
        let response = db
            .execute_statement(ExecuteStatementRequest {
                statement: "SELECT * FROM \"Paged\"".to_string(),
                limit: Some(2),
                next_token: Some(token),
                ..Default::default()
            })
            .unwrap();
        for item in response.items.unwrap_or_default() {
            if let Some(AttributeValue::S(pk)) = item.get("pk") {
                seen.push(pk.clone());
            }
        }
        next_token = response.next_token;
        assert!(seen.len() <= 12, "pagination did not terminate");
    }

    // The deleted row may or may not appear (it was returned before deletion);
    // every other row must appear exactly once.
    seen.sort();
    for i in 0..6 {
        let key = format!("k{i}");
        if key == cursor_key {
            continue;
        }
        assert_eq!(
            seen.iter().filter(|k| **k == key).count(),
            1,
            "{key} should appear exactly once, saw {seen:?}"
        );
    }
}

#[test]
fn test_select_pagination_walks_a_partition_in_sort_key_order() {
    // The partition-constrained path reads through query_items, which orders by
    // sort key. It must ascend, so a continuation resumes after the last key
    // rather than back at the top.
    let db = Database::memory().unwrap();
    let request = CreateTableRequest {
        table_name: "Sorted".to_string(),
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
    };
    db.create_table(request).unwrap();
    for i in 0..5 {
        exec(
            &db,
            &format!("INSERT INTO \"Sorted\" VALUE {{'pk': 'p', 'sk': 's{i}'}}"),
        );
    }

    let mut seen = Vec::new();
    let mut next_token = None;
    loop {
        let response = db
            .execute_statement(ExecuteStatementRequest {
                statement: "SELECT * FROM \"Sorted\" WHERE pk = 'p'".to_string(),
                limit: Some(2),
                next_token,
                ..Default::default()
            })
            .unwrap();
        for item in response.items.unwrap_or_default() {
            if let Some(AttributeValue::S(sk)) = item.get("sk") {
                seen.push(sk.clone());
            }
        }
        next_token = response.next_token;
        if next_token.is_none() {
            break;
        }
        assert!(
            seen.len() <= 5,
            "pagination did not terminate, saw {seen:?}"
        );
    }

    assert_eq!(
        seen,
        vec!["s0", "s1", "s2", "s3", "s4"],
        "ascending, once each"
    );
}

/// A pk+sk table for sort-key condition tests.
fn create_ranged_table(db: &Database, table_name: &str) {
    let request = CreateTableRequest {
        table_name: table_name.to_string(),
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
    };
    db.create_table(request).unwrap();
}

#[test]
fn test_next_token_is_rejected_when_replayed_with_a_different_where_clause() {
    // The token is bound to the statement that minted it, so a token from one
    // WHERE clause cannot silently skip rows in another on the same table.
    let db = Database::memory().unwrap();
    create_test_table(&db, "Paged");
    for i in 0..4 {
        put_test_item(&db, "Paged", &format!("k{i}"), "keep");
    }

    let token = db
        .execute_statement(ExecuteStatementRequest {
            statement: "SELECT * FROM \"Paged\" WHERE name = 'keep'".to_string(),
            limit: Some(1),
            ..Default::default()
        })
        .unwrap()
        .next_token
        .expect("a read that stopped on the limit owes a token");

    let err = db
        .execute_statement(ExecuteStatementRequest {
            statement: "SELECT * FROM \"Paged\" WHERE name = 'drop'".to_string(),
            limit: Some(1),
            next_token: Some(token),
            ..Default::default()
        })
        .unwrap_err();
    assert!(
        matches!(err, DynoxideError::ValidationException(ref m) if m == "NextToken does not match request"),
        "unexpected error: {err:?}"
    );
}

// -----------------------------------------------------------------------
// COUNT projections are rejected (aggregates are not DynamoDB PartiQL)
// -----------------------------------------------------------------------

/// Run a statement and return the ValidationException message it fails with.
fn exec_validation_err(db: &Database, statement: &str) -> String {
    let err = db
        .execute_statement(ExecuteStatementRequest {
            statement: statement.to_string(),
            ..Default::default()
        })
        .unwrap_err();
    match err {
        DynoxideError::ValidationException(m) => m,
        other => panic!("expected ValidationException, got {other:?}"),
    }
}

#[test]
fn test_count_star_projection_is_rejected_with_the_position_of_the_token() {
    // Real DynamoDB has no aggregate functions in PartiQL: the projection is
    // rejected with a bare message naming the token's 1-based position and
    // its length of 5 (captured eu-west-2).
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");
    assert_eq!(
        exec_validation_err(&db, "SELECT COUNT(*) FROM \"Users\""),
        "Unexpected path component at 1:8:5"
    );
}

#[test]
fn test_count_in_a_second_projection_reports_its_own_column() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");
    assert_eq!(
        exec_validation_err(&db, "SELECT pk, COUNT(*) FROM \"Users\""),
        "Unexpected path component at 1:12:5"
    );
}

#[test]
fn test_lowercase_count_is_rejected_the_same_way() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");
    assert_eq!(
        exec_validation_err(&db, "SELECT count(*) FROM \"Users\""),
        "Unexpected path component at 1:8:5"
    );
}

#[test]
fn test_count_of_an_attribute_is_rejected_with_the_same_length() {
    // The length in the message is the COUNT token itself, not the whole call.
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");
    assert_eq!(
        exec_validation_err(&db, "SELECT COUNT(pk) FROM \"Users\""),
        "Unexpected path component at 1:8:5"
    );
}

#[test]
fn test_count_is_rejected_before_the_table_existence_check() {
    // Real DynamoDB fires this rejection even when the table does not exist.
    let db = Database::memory().unwrap();
    assert_eq!(
        exec_validation_err(&db, "SELECT COUNT(*) FROM \"NoSuchTable\""),
        "Unexpected path component at 1:8:5"
    );
}

#[test]
fn test_a_quoted_count_attribute_name_still_projects() {
    // The rejection is token-based, so a quoted "COUNT" identifier is an
    // ordinary attribute name and stays usable in a projection.
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");
    put_item_with_attrs(&db, "Users", "u1", &[("COUNT", "three")]);

    let resp = exec(&db, "SELECT \"COUNT\" FROM \"Users\" WHERE pk = 'u1'");
    let items = resp.items.unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(
        items[0].get("COUNT"),
        Some(&AttributeValue::S("three".to_string()))
    );
}

#[test]
fn test_count_is_rejected_per_statement_on_batch_execute() {
    // BatchExecuteStatement follows the shape captured on ExecuteStatement: the
    // bare message under the usual per-statement ValidationError code.
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");
    put_test_item(&db, "Users", "u1", "Alice");

    let resp = db
        .batch_execute_statement(BatchExecuteStatementRequest {
            return_consumed_capacity: None,
            statements: vec![
                batch_stmt("SELECT COUNT(*) FROM \"Users\"".to_string()),
                batch_stmt("SELECT * FROM \"Users\" WHERE pk = 'u1'".to_string()),
            ],
        })
        .unwrap();

    let error = resp.responses[0].error.as_ref().expect("expected an error");
    assert_eq!(error.code, "ValidationError");
    assert_eq!(error.message, "Unexpected path component at 1:8:5");
    // The other statement still executes.
    assert!(resp.responses[1].error.is_none());
    assert!(resp.responses[1].item.is_some());
}

#[test]
fn test_count_is_rejected_at_the_top_level_on_execute_transaction() {
    // ExecuteTransaction surfaces the rejection where its parse errors live,
    // at the top level, but with the bare captured message.
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");

    let err = db
        .execute_transaction(ExecuteTransactionRequest {
            transact_statements: vec![ParameterizedStatement {
                statement: "SELECT COUNT(*) FROM \"Users\"".to_string(),
                parameters: None,
            }],
            ..Default::default()
        })
        .unwrap_err();
    assert!(
        matches!(err, DynoxideError::ValidationException(ref m) if m == "Unexpected path component at 1:8:5"),
        "unexpected error: {err:?}"
    );
}

#[test]
fn test_next_token_is_rejected_when_replayed_with_different_parameters() {
    // The same statement text with different Parameters walks a different row
    // set, so the token must not carry across.
    let db = Database::memory().unwrap();
    create_test_table(&db, "Paged");
    for i in 0..4 {
        put_test_item(&db, "Paged", &format!("k{i}"), "keep");
    }

    let token = db
        .execute_statement(ExecuteStatementRequest {
            statement: "SELECT * FROM \"Paged\" WHERE name = ?".to_string(),
            parameters: Some(vec![AttributeValue::S("keep".to_string())]),
            limit: Some(1),
            ..Default::default()
        })
        .unwrap()
        .next_token
        .expect("a read that stopped on the limit owes a token");

    let err = db
        .execute_statement(ExecuteStatementRequest {
            statement: "SELECT * FROM \"Paged\" WHERE name = ?".to_string(),
            parameters: Some(vec![AttributeValue::S("drop".to_string())]),
            limit: Some(1),
            next_token: Some(token),
            ..Default::default()
        })
        .unwrap_err();
    assert!(
        matches!(err, DynoxideError::ValidationException(ref m) if m == "NextToken does not match request"),
        "unexpected error: {err:?}"
    );
}

#[test]
fn test_execute_statement_rejects_limit_zero() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Paged");

    let err = db
        .execute_statement(ExecuteStatementRequest {
            statement: "SELECT * FROM \"Paged\"".to_string(),
            limit: Some(0),
            ..Default::default()
        })
        .unwrap_err();
    assert!(
        matches!(err, DynoxideError::ValidationException(ref m) if m == "1 validation error detected: Value '0' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1"),
        "unexpected error: {err:?}"
    );
}

#[test]
fn test_limit_zero_cannot_silently_end_a_paginated_walk() {
    // A zero limit used to read nothing and mint no token, so a walk that hit
    // one ended early while looking complete. It must error instead.
    let db = Database::memory().unwrap();
    create_test_table(&db, "Paged");
    for i in 0..4 {
        put_test_item(&db, "Paged", &format!("k{i}"), "n");
    }

    let first = db
        .execute_statement(ExecuteStatementRequest {
            statement: "SELECT * FROM \"Paged\"".to_string(),
            limit: Some(2),
            ..Default::default()
        })
        .unwrap();
    let token = first.next_token.expect("more rows remain");

    let err = db
        .execute_statement(ExecuteStatementRequest {
            statement: "SELECT * FROM \"Paged\"".to_string(),
            limit: Some(0),
            next_token: Some(token),
            ..Default::default()
        })
        .unwrap_err();
    assert!(
        matches!(err, DynoxideError::ValidationException(_)),
        "unexpected error: {err:?}"
    );
}

#[test]
fn test_select_limit_paces_within_the_sort_key_range_not_the_partition() {
    // Sort-key conditions are pushed into the read, so Limit counts rows
    // evaluated within the key-condition range, as DynamoDB's does. Were the
    // range filtered after the read, this first page would burn its two reads
    // on s0 and s1 and return nothing.
    let db = Database::memory().unwrap();
    create_ranged_table(&db, "Ranged");
    for i in 0..10 {
        exec(
            &db,
            &format!("INSERT INTO \"Ranged\" VALUE {{'pk': 'p', 'sk': 's{i}'}}"),
        );
    }

    let first = db
        .execute_statement(ExecuteStatementRequest {
            statement: "SELECT * FROM \"Ranged\" WHERE pk = 'p' AND sk >= 's3' AND sk <= 's7'"
                .to_string(),
            limit: Some(2),
            ..Default::default()
        })
        .unwrap();
    let sks: Vec<String> = first
        .items
        .unwrap()
        .iter()
        .filter_map(|i| match i.get("sk") {
            Some(AttributeValue::S(sk)) => Some(sk.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        sks,
        vec!["s3", "s4"],
        "two rows read, both within the range"
    );
    assert!(first.next_token.is_some(), "the range holds more rows");
}

#[test]
fn test_select_pagination_with_a_sort_key_range_visits_every_match_once() {
    let db = Database::memory().unwrap();
    create_ranged_table(&db, "Ranged");
    for i in 0..10 {
        exec(
            &db,
            &format!("INSERT INTO \"Ranged\" VALUE {{'pk': 'p', 'sk': 's{i}'}}"),
        );
    }

    let mut seen = Vec::new();
    let mut next_token = None;
    loop {
        let response = db
            .execute_statement(ExecuteStatementRequest {
                statement: "SELECT * FROM \"Ranged\" WHERE pk = 'p' AND sk BETWEEN 's3' AND 's7'"
                    .to_string(),
                limit: Some(2),
                next_token,
                ..Default::default()
            })
            .unwrap();
        for item in response.items.unwrap_or_default() {
            if let Some(AttributeValue::S(sk)) = item.get("sk") {
                seen.push(sk.clone());
            }
        }
        next_token = response.next_token;
        if next_token.is_none() {
            break;
        }
        assert!(
            seen.len() <= 5,
            "pagination did not terminate, saw {seen:?}"
        );
    }

    assert_eq!(
        seen,
        vec!["s3", "s4", "s5", "s6", "s7"],
        "in order, once each"
    );
}

#[test]
fn test_select_with_an_untranslatable_sort_key_condition_still_filters_correctly() {
    // sk IN (...) has no key-condition form, so nothing is pushed down and the
    // read falls back to the whole partition; the WHERE still filters
    // post-read, so the result is unchanged.
    let db = Database::memory().unwrap();
    create_ranged_table(&db, "Ranged");
    for i in 0..6 {
        exec(
            &db,
            &format!("INSERT INTO \"Ranged\" VALUE {{'pk': 'p', 'sk': 's{i}'}}"),
        );
    }

    let response = exec(
        &db,
        "SELECT * FROM \"Ranged\" WHERE pk = 'p' AND sk IN ('s1', 's4')",
    );
    let sks: Vec<String> = response
        .items
        .unwrap()
        .iter()
        .filter_map(|i| match i.get("sk") {
            Some(AttributeValue::S(sk)) => Some(sk.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(sks, vec!["s1", "s4"]);
}
