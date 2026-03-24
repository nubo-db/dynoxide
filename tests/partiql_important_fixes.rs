//! Tests for PartiQL IMPORTANT correctness fixes (I12-I22).

use dynoxide::Database;
use dynoxide::actions::create_table::CreateTableRequest;
use dynoxide::actions::execute_statement::{ExecuteStatementRequest, ExecuteStatementResponse};
use dynoxide::actions::put_item::PutItemRequest;
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

fn create_composite_table(db: &Database, table_name: &str) {
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

fn exec(db: &Database, statement: &str) -> ExecuteStatementResponse {
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
) -> ExecuteStatementResponse {
    db.execute_statement(ExecuteStatementRequest {
        statement: statement.to_string(),
        parameters: Some(params),
        ..Default::default()
    })
    .unwrap()
}

fn exec_with_limit(db: &Database, statement: &str, limit: usize) -> ExecuteStatementResponse {
    db.execute_statement(ExecuteStatementRequest {
        statement: statement.to_string(),
        parameters: None,
        limit: Some(limit),
        ..Default::default()
    })
    .unwrap()
}

fn exec_err(db: &Database, statement: &str) -> String {
    db.execute_statement(ExecuteStatementRequest {
        statement: statement.to_string(),
        parameters: None,
        ..Default::default()
    })
    .unwrap_err()
    .to_string()
}

fn put_item(db: &Database, table_name: &str, item: HashMap<String, AttributeValue>) {
    db.put_item(PutItemRequest {
        table_name: table_name.to_string(),
        item,
        ..Default::default()
    })
    .unwrap();
}

// ---------------------------------------------------------------------------
// I12: UPDATE REMOVE clause
// ---------------------------------------------------------------------------

#[test]
fn test_update_remove_clause() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Tbl");

    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("k1".to_string()));
    item.insert("name".to_string(), AttributeValue::S("Alice".to_string()));
    item.insert("age".to_string(), AttributeValue::N("30".to_string()));
    item.insert(
        "email".to_string(),
        AttributeValue::S("alice@example.com".to_string()),
    );
    put_item(&db, "Tbl", item);

    // Remove age and email attributes
    exec(
        &db,
        "UPDATE \"Tbl\" SET name = 'Alicia' REMOVE age, email WHERE pk = 'k1'",
    );

    let sel = exec(&db, "SELECT * FROM \"Tbl\" WHERE pk = 'k1'");
    let items = sel.items.unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(
        items[0].get("name"),
        Some(&AttributeValue::S("Alicia".to_string()))
    );
    assert!(items[0].get("age").is_none(), "age should be removed");
    assert!(items[0].get("email").is_none(), "email should be removed");
}

#[test]
fn test_update_remove_only() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Tbl");

    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("k1".to_string()));
    item.insert("temp".to_string(), AttributeValue::S("data".to_string()));
    put_item(&db, "Tbl", item);

    exec(&db, "UPDATE \"Tbl\" REMOVE temp WHERE pk = 'k1'");

    let sel = exec(&db, "SELECT * FROM \"Tbl\" WHERE pk = 'k1'");
    let items = sel.items.unwrap();
    assert_eq!(items.len(), 1);
    assert!(items[0].get("temp").is_none(), "temp should be removed");
}

// ---------------------------------------------------------------------------
// I13: SET with nested paths
// ---------------------------------------------------------------------------

#[test]
fn test_set_nested_path() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Tbl");

    let mut address = HashMap::new();
    address.insert("city".to_string(), AttributeValue::S("London".to_string()));
    address.insert(
        "postcode".to_string(),
        AttributeValue::S("SW1A".to_string()),
    );

    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("k1".to_string()));
    item.insert("address".to_string(), AttributeValue::M(address));
    put_item(&db, "Tbl", item);

    exec(
        &db,
        "UPDATE \"Tbl\" SET address.city = 'Manchester' WHERE pk = 'k1'",
    );

    let sel = exec(&db, "SELECT * FROM \"Tbl\" WHERE pk = 'k1'");
    let items = sel.items.unwrap();
    let addr = match items[0].get("address") {
        Some(AttributeValue::M(m)) => m,
        other => panic!("Expected map, got {other:?}"),
    };
    assert_eq!(
        addr.get("city"),
        Some(&AttributeValue::S("Manchester".to_string()))
    );
    // Postcode should be untouched
    assert_eq!(
        addr.get("postcode"),
        Some(&AttributeValue::S("SW1A".to_string()))
    );
}

// ---------------------------------------------------------------------------
// I14: SET expressions (count + 1, count - 1)
// ---------------------------------------------------------------------------

#[test]
fn test_set_expression_add() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Tbl");

    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("k1".to_string()));
    item.insert("count".to_string(), AttributeValue::N("10".to_string()));
    put_item(&db, "Tbl", item);

    exec(&db, "UPDATE \"Tbl\" SET count = count + 1 WHERE pk = 'k1'");

    let sel = exec(&db, "SELECT * FROM \"Tbl\" WHERE pk = 'k1'");
    let items = sel.items.unwrap();
    assert_eq!(
        items[0].get("count"),
        Some(&AttributeValue::N("11".to_string()))
    );
}

#[test]
fn test_set_expression_subtract() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Tbl");

    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("k1".to_string()));
    item.insert("count".to_string(), AttributeValue::N("10".to_string()));
    put_item(&db, "Tbl", item);

    exec(&db, "UPDATE \"Tbl\" SET count = count - 3 WHERE pk = 'k1'");

    let sel = exec(&db, "SELECT * FROM \"Tbl\" WHERE pk = 'k1'");
    let items = sel.items.unwrap();
    assert_eq!(
        items[0].get("count"),
        Some(&AttributeValue::N("7".to_string()))
    );
}

// ---------------------------------------------------------------------------
// I15: DELETE with missing sort key
// ---------------------------------------------------------------------------

#[test]
fn test_delete_missing_sort_key_error() {
    let db = Database::memory().unwrap();
    create_composite_table(&db, "CTbl");

    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("p1".to_string()));
    item.insert("sk".to_string(), AttributeValue::S("s1".to_string()));
    item.insert("data".to_string(), AttributeValue::S("hello".to_string()));
    put_item(&db, "CTbl", item);

    let err = exec_err(&db, "DELETE FROM \"CTbl\" WHERE pk = 'p1'");
    assert!(
        err.contains("mandatory equality on all key attributes"),
        "Expected sort key error, got: {err}"
    );
}

// ---------------------------------------------------------------------------
// I16: SELECT with LIMIT
// ---------------------------------------------------------------------------

#[test]
fn test_select_with_limit() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Tbl");

    for i in 0..10 {
        let mut item = HashMap::new();
        item.insert("pk".to_string(), AttributeValue::S(format!("k{i}")));
        put_item(&db, "Tbl", item);
    }

    let resp = exec_with_limit(&db, "SELECT * FROM \"Tbl\"", 3);
    let items = resp.items.unwrap();
    assert_eq!(items.len(), 3);
}

// ---------------------------------------------------------------------------
// I17: COUNT(*) support
// ---------------------------------------------------------------------------

#[test]
fn test_select_count_star() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Tbl");

    for i in 0..5 {
        let mut item = HashMap::new();
        item.insert("pk".to_string(), AttributeValue::S(format!("k{i}")));
        put_item(&db, "Tbl", item);
    }

    let resp = exec(&db, "SELECT COUNT(*) FROM \"Tbl\"");
    let items = resp.items.unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(
        items[0].get("Count"),
        Some(&AttributeValue::N("5".to_string()))
    );
}

// ---------------------------------------------------------------------------
// I18: Set literal syntax (<< >>)
// ---------------------------------------------------------------------------

#[test]
fn test_set_literal_string_set() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Tbl");

    exec(
        &db,
        "INSERT INTO \"Tbl\" VALUE {'pk': 'k1', 'tags': <<'a', 'b', 'c'>>}",
    );

    let sel = exec(&db, "SELECT * FROM \"Tbl\" WHERE pk = 'k1'");
    let items = sel.items.unwrap();
    match items[0].get("tags") {
        Some(AttributeValue::SS(ss)) => {
            assert!(ss.contains(&"a".to_string()));
            assert!(ss.contains(&"b".to_string()));
            assert!(ss.contains(&"c".to_string()));
        }
        other => panic!("Expected SS, got {other:?}"),
    }
}

#[test]
fn test_set_literal_number_set() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Tbl");

    exec(
        &db,
        "INSERT INTO \"Tbl\" VALUE {'pk': 'k1', 'nums': <<1, 2, 3>>}",
    );

    let sel = exec(&db, "SELECT * FROM \"Tbl\" WHERE pk = 'k1'");
    let items = sel.items.unwrap();
    match items[0].get("nums") {
        Some(AttributeValue::NS(ns)) => {
            assert!(ns.contains(&"1".to_string()));
            assert!(ns.contains(&"2".to_string()));
            assert!(ns.contains(&"3".to_string()));
        }
        other => panic!("Expected NS, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// I19: OR conditions
// ---------------------------------------------------------------------------

#[test]
fn test_where_or_condition() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Tbl");

    let mut item_a = HashMap::new();
    item_a.insert("pk".to_string(), AttributeValue::S("k1".to_string()));
    item_a.insert("status".to_string(), AttributeValue::S("A".to_string()));
    put_item(&db, "Tbl", item_a);

    let mut item_b = HashMap::new();
    item_b.insert("pk".to_string(), AttributeValue::S("k2".to_string()));
    item_b.insert("status".to_string(), AttributeValue::S("B".to_string()));
    put_item(&db, "Tbl", item_b);

    let mut item_c = HashMap::new();
    item_c.insert("pk".to_string(), AttributeValue::S("k3".to_string()));
    item_c.insert("status".to_string(), AttributeValue::S("C".to_string()));
    put_item(&db, "Tbl", item_c);

    let resp = exec(
        &db,
        "SELECT * FROM \"Tbl\" WHERE status = 'A' OR status = 'B'",
    );
    let items = resp.items.unwrap();
    assert_eq!(items.len(), 2);
    let statuses: Vec<String> = items
        .iter()
        .filter_map(|i| match i.get("status") {
            Some(AttributeValue::S(s)) => Some(s.clone()),
            _ => None,
        })
        .collect();
    assert!(statuses.contains(&"A".to_string()));
    assert!(statuses.contains(&"B".to_string()));
}

// ---------------------------------------------------------------------------
// I20: Nested paths in WHERE function conditions
// ---------------------------------------------------------------------------

#[test]
fn test_begins_with_nested_path() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Tbl");

    let mut addr = HashMap::new();
    addr.insert("city".to_string(), AttributeValue::S("London".to_string()));

    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("k1".to_string()));
    item.insert("address".to_string(), AttributeValue::M(addr));
    put_item(&db, "Tbl", item);

    let mut addr2 = HashMap::new();
    addr2.insert(
        "city".to_string(),
        AttributeValue::S("Manchester".to_string()),
    );

    let mut item2 = HashMap::new();
    item2.insert("pk".to_string(), AttributeValue::S("k2".to_string()));
    item2.insert("address".to_string(), AttributeValue::M(addr2));
    put_item(&db, "Tbl", item2);

    let resp = exec(
        &db,
        "SELECT * FROM \"Tbl\" WHERE BEGINS_WITH(address.city, 'Lon')",
    );
    let items = resp.items.unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(
        items[0].get("pk"),
        Some(&AttributeValue::S("k1".to_string()))
    );
}

// ---------------------------------------------------------------------------
// I22: INSERT IF NOT EXISTS
// ---------------------------------------------------------------------------

#[test]
fn test_insert_if_not_exists_on_duplicate_succeeds() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Tbl");

    exec(
        &db,
        "INSERT INTO \"Tbl\" VALUE {'pk': 'k1', 'name': 'Alice'}",
    );

    // With IF NOT EXISTS, duplicate insert should silently succeed (no-op)
    exec(
        &db,
        "INSERT INTO \"Tbl\" VALUE {'pk': 'k1', 'name': 'Bob'} IF NOT EXISTS",
    );

    // Original item should be unchanged
    let sel = exec(&db, "SELECT * FROM \"Tbl\" WHERE pk = 'k1'");
    let items = sel.items.unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(
        items[0].get("name"),
        Some(&AttributeValue::S("Alice".to_string()))
    );
}

#[test]
fn test_insert_without_if_not_exists_on_duplicate_fails() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Tbl");

    exec(
        &db,
        "INSERT INTO \"Tbl\" VALUE {'pk': 'k1', 'name': 'Alice'}",
    );

    // Without IF NOT EXISTS, duplicate insert should fail
    let err = exec_err(&db, "INSERT INTO \"Tbl\" VALUE {'pk': 'k1', 'name': 'Bob'}");
    assert!(
        err.contains("Duplicate primary key"),
        "Expected duplicate key error, got: {err}"
    );
}
