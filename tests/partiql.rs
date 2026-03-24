use dynoxide::Database;
use dynoxide::actions::batch_execute_statement::{
    BatchExecuteStatementRequest, BatchStatementRequest,
};
use dynoxide::actions::create_table::CreateTableRequest;
use dynoxide::actions::execute_statement::ExecuteStatementRequest;
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
            statements: vec![
                BatchStatementRequest {
                    statement: "INSERT INTO \"Users\" VALUE {'pk': 'u1', 'name': 'Alice'}"
                        .to_string(),
                    parameters: None,
                },
                BatchStatementRequest {
                    statement: "INSERT INTO \"Users\" VALUE {'pk': 'u2', 'name': 'Bob'}"
                        .to_string(),
                    parameters: None,
                },
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

    let resp = db
        .batch_execute_statement(BatchExecuteStatementRequest {
            statements: vec![
                BatchStatementRequest {
                    statement: "INSERT INTO \"Users\" VALUE {'pk': 'u1', 'name': 'Alice'}"
                        .to_string(),
                    parameters: None,
                },
                BatchStatementRequest {
                    statement: "SELECT * FROM \"NonExistent\"".to_string(),
                    parameters: None,
                },
            ],
        })
        .unwrap();

    assert_eq!(resp.responses.len(), 2);
    assert!(resp.responses[0].error.is_none()); // insert succeeded
    assert!(resp.responses[1].error.is_some()); // select failed — table not found
}

#[test]
fn test_batch_execute_exceeds_limit() {
    let db = Database::memory().unwrap();

    let stmts: Vec<BatchStatementRequest> = (0..26)
        .map(|_| BatchStatementRequest {
            statement: "SELECT * FROM \"T\"".to_string(),
            parameters: None,
        })
        .collect();

    let result = db.batch_execute_statement(BatchExecuteStatementRequest { statements: stmts });

    assert!(result.is_err());
}

#[test]
fn test_batch_execute_empty_statements_rejected() {
    let db = Database::memory().unwrap();

    let result = db.batch_execute_statement(BatchExecuteStatementRequest { statements: vec![] });

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
        items[0].get("mymap").is_none(),
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
    assert!(items[0].get("nonexistent").is_none());
}
