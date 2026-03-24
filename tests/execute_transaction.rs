use dynoxide::Database;
use dynoxide::actions::create_table::CreateTableRequest;
use dynoxide::actions::execute_statement::ExecuteStatementRequest;
use dynoxide::actions::execute_transaction::{ExecuteTransactionRequest, ParameterizedStatement};
use dynoxide::types::{
    AttributeDefinition, AttributeValue, KeySchemaElement, KeyType, ScalarAttributeType,
};

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

fn select_all(
    db: &Database,
    table_name: &str,
) -> Vec<std::collections::HashMap<String, AttributeValue>> {
    let resp = db
        .execute_statement(ExecuteStatementRequest {
            statement: format!("SELECT * FROM \"{}\"", table_name),
            parameters: None,
            ..Default::default()
        })
        .unwrap();
    resp.items.unwrap_or_default()
}

// -----------------------------------------------------------------------
// Multiple INSERTs in a transaction all succeed
// -----------------------------------------------------------------------

#[test]
fn test_multiple_inserts_all_succeed() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");

    let request = ExecuteTransactionRequest {
        transact_statements: vec![
            ParameterizedStatement {
                statement: "INSERT INTO \"Users\" VALUE {'pk': 'u1', 'name': 'Alice'}".to_string(),
                parameters: None,
            },
            ParameterizedStatement {
                statement: "INSERT INTO \"Users\" VALUE {'pk': 'u2', 'name': 'Bob'}".to_string(),
                parameters: None,
            },
            ParameterizedStatement {
                statement: "INSERT INTO \"Users\" VALUE {'pk': 'u3', 'name': 'Charlie'}"
                    .to_string(),
                parameters: None,
            },
        ],
        client_request_token: None,
        return_consumed_capacity: None,
    };

    let resp = db.execute_transaction(request).unwrap();
    assert!(resp.responses.is_some());
    assert_eq!(resp.responses.unwrap().len(), 3);

    // Verify all items exist
    let items = select_all(&db, "Users");
    assert_eq!(items.len(), 3);
}

// -----------------------------------------------------------------------
// INSERT with duplicate key causes transaction rollback
// -----------------------------------------------------------------------

#[test]
fn test_insert_failure_rolls_back_entire_transaction() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");

    // Try to insert items where one targets a non-existent table, causing failure
    let request = ExecuteTransactionRequest {
        transact_statements: vec![
            ParameterizedStatement {
                statement: "INSERT INTO \"Users\" VALUE {'pk': 'u1', 'name': 'Alice'}".to_string(),
                parameters: None,
            },
            ParameterizedStatement {
                // This fails because the table does not exist
                statement: "INSERT INTO \"NonExistent\" VALUE {'pk': 'u2', 'name': 'Bob'}"
                    .to_string(),
                parameters: None,
            },
            ParameterizedStatement {
                statement: "INSERT INTO \"Users\" VALUE {'pk': 'u3', 'name': 'Charlie'}"
                    .to_string(),
                parameters: None,
            },
        ],
        client_request_token: None,
        return_consumed_capacity: None,
    };

    let result = db.execute_transaction(request);
    assert!(result.is_err());

    let err = result.unwrap_err();
    let err_str = err.to_string();
    assert!(
        err_str.contains("Transaction cancelled"),
        "Expected TransactionCanceledException, got: {}",
        err_str
    );

    // Verify that u1 and u3 were NOT inserted (rollback happened)
    let items = select_all(&db, "Users");
    assert_eq!(
        items.len(),
        0,
        "No items should exist — entire transaction rolled back"
    );
}

// -----------------------------------------------------------------------
// Mix of SELECT and INSERT works (SELECT returns items)
// -----------------------------------------------------------------------

#[test]
fn test_mixed_select_and_insert() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");

    // Pre-insert an item to SELECT
    db.execute_statement(ExecuteStatementRequest {
        statement: "INSERT INTO \"Users\" VALUE {'pk': 'u1', 'name': 'Alice'}".to_string(),
        parameters: None,
        ..Default::default()
    })
    .unwrap();

    let request = ExecuteTransactionRequest {
        transact_statements: vec![
            ParameterizedStatement {
                statement: "SELECT * FROM \"Users\" WHERE pk = 'u1'".to_string(),
                parameters: None,
            },
            ParameterizedStatement {
                statement: "INSERT INTO \"Users\" VALUE {'pk': 'u2', 'name': 'Bob'}".to_string(),
                parameters: None,
            },
        ],
        client_request_token: None,
        return_consumed_capacity: None,
    };

    let resp = db.execute_transaction(request).unwrap();
    let responses = resp.responses.unwrap();
    assert_eq!(responses.len(), 2);

    // First response should have the selected item
    let item = responses[0]
        .item
        .as_ref()
        .expect("SELECT should return an item");
    assert_eq!(
        item.get("name"),
        Some(&AttributeValue::S("Alice".to_string()))
    );

    // Second response (INSERT) should have no item
    assert!(responses[1].item.is_none());
}

// -----------------------------------------------------------------------
// More than 100 statements returns ValidationException
// -----------------------------------------------------------------------

#[test]
fn test_over_100_statements_rejected() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");

    let statements: Vec<ParameterizedStatement> = (0..101)
        .map(|i| ParameterizedStatement {
            statement: format!(
                "INSERT INTO \"Users\" VALUE {{'pk': 'u{}', 'name': 'User{}'}}",
                i, i
            ),
            parameters: None,
        })
        .collect();

    let request = ExecuteTransactionRequest {
        transact_statements: statements,
        client_request_token: None,
        return_consumed_capacity: None,
    };

    let result = db.execute_transaction(request);
    assert!(result.is_err());
    let err_str = result.unwrap_err().to_string();
    assert!(
        err_str.contains("less than or equal to 100"),
        "Expected validation error about 100 limit, got: {}",
        err_str
    );
}

// -----------------------------------------------------------------------
// Empty transaction returns ValidationException
// -----------------------------------------------------------------------

#[test]
fn test_empty_transaction_rejected() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");

    let request = ExecuteTransactionRequest {
        transact_statements: vec![],
        client_request_token: None,
        return_consumed_capacity: None,
    };

    let result = db.execute_transaction(request);
    assert!(result.is_err());
    let err_str = result.unwrap_err().to_string();
    assert!(
        err_str.contains("greater than or equal to 1"),
        "Expected validation error about minimum 1, got: {}",
        err_str
    );
}

// -----------------------------------------------------------------------
// Syntax error in statement is caught before execution
// -----------------------------------------------------------------------

#[test]
fn test_syntax_error_rejected_before_execution() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");

    let request = ExecuteTransactionRequest {
        transact_statements: vec![
            ParameterizedStatement {
                statement: "INSERT INTO \"Users\" VALUE {'pk': 'u1', 'name': 'Alice'}".to_string(),
                parameters: None,
            },
            ParameterizedStatement {
                statement: "INVALID STATEMENT HERE".to_string(),
                parameters: None,
            },
        ],
        client_request_token: None,
        return_consumed_capacity: None,
    };

    let result = db.execute_transaction(request);
    assert!(result.is_err());

    // The first INSERT should not have been executed because parsing
    // happens before execution begins
    let items = select_all(&db, "Users");
    assert_eq!(
        items.len(),
        0,
        "No items should be inserted when parsing fails"
    );
}

// -----------------------------------------------------------------------
// UPDATE within a transaction
// -----------------------------------------------------------------------

#[test]
fn test_update_within_transaction() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");

    // Pre-insert an item
    db.execute_statement(ExecuteStatementRequest {
        statement: "INSERT INTO \"Users\" VALUE {'pk': 'u1', 'name': 'Alice'}".to_string(),
        parameters: None,
        ..Default::default()
    })
    .unwrap();

    let request = ExecuteTransactionRequest {
        transact_statements: vec![
            ParameterizedStatement {
                statement: "UPDATE \"Users\" SET name = 'Alice Updated' WHERE pk = 'u1'"
                    .to_string(),
                parameters: None,
            },
            ParameterizedStatement {
                statement: "INSERT INTO \"Users\" VALUE {'pk': 'u2', 'name': 'Bob'}".to_string(),
                parameters: None,
            },
        ],
        ..Default::default()
    };

    db.execute_transaction(request).unwrap();

    let items = select_all(&db, "Users");
    assert_eq!(items.len(), 2);

    // Verify update took effect
    let u1 = items.iter().find(|i| match i.get("pk") {
        Some(AttributeValue::S(s)) => s == "u1",
        _ => false,
    });
    assert!(u1.is_some());
    match u1.unwrap().get("name") {
        Some(AttributeValue::S(s)) => assert_eq!(s, "Alice Updated"),
        other => panic!("Expected updated name, got {:?}", other),
    }
}

// -----------------------------------------------------------------------
// DELETE within a transaction
// -----------------------------------------------------------------------

#[test]
fn test_delete_within_transaction() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");

    // Pre-insert items
    db.execute_statement(ExecuteStatementRequest {
        statement: "INSERT INTO \"Users\" VALUE {'pk': 'u1', 'name': 'Alice'}".to_string(),
        parameters: None,
        ..Default::default()
    })
    .unwrap();
    db.execute_statement(ExecuteStatementRequest {
        statement: "INSERT INTO \"Users\" VALUE {'pk': 'u2', 'name': 'Bob'}".to_string(),
        parameters: None,
        ..Default::default()
    })
    .unwrap();

    let request = ExecuteTransactionRequest {
        transact_statements: vec![
            ParameterizedStatement {
                statement: "DELETE FROM \"Users\" WHERE pk = 'u1'".to_string(),
                parameters: None,
            },
            ParameterizedStatement {
                statement: "INSERT INTO \"Users\" VALUE {'pk': 'u3', 'name': 'Charlie'}"
                    .to_string(),
                parameters: None,
            },
        ],
        ..Default::default()
    };

    db.execute_transaction(request).unwrap();

    let items = select_all(&db, "Users");
    assert_eq!(items.len(), 2); // u2 + u3, u1 deleted
    let pks: Vec<String> = items
        .iter()
        .filter_map(|i| match i.get("pk") {
            Some(AttributeValue::S(s)) => Some(s.clone()),
            _ => None,
        })
        .collect();
    assert!(pks.contains(&"u2".to_string()));
    assert!(pks.contains(&"u3".to_string()));
    assert!(!pks.contains(&"u1".to_string()));
}

// -----------------------------------------------------------------------
// Parameterised statements with ? placeholders in WHERE clauses
// -----------------------------------------------------------------------

#[test]
fn test_parameterised_statements() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");

    // Pre-insert items
    db.execute_statement(ExecuteStatementRequest {
        statement: "INSERT INTO \"Users\" VALUE {'pk': 'u1', 'name': 'Alice'}".to_string(),
        parameters: None,
        ..Default::default()
    })
    .unwrap();
    db.execute_statement(ExecuteStatementRequest {
        statement: "INSERT INTO \"Users\" VALUE {'pk': 'u2', 'name': 'Bob'}".to_string(),
        parameters: None,
        ..Default::default()
    })
    .unwrap();

    // Use parameterised WHERE in a transaction: update u1 and select u2
    let request = ExecuteTransactionRequest {
        transact_statements: vec![
            ParameterizedStatement {
                statement: "UPDATE \"Users\" SET name = 'Alice V2' WHERE pk = ?".to_string(),
                parameters: Some(vec![AttributeValue::S("u1".to_string())]),
            },
            ParameterizedStatement {
                statement: "SELECT * FROM \"Users\" WHERE pk = ?".to_string(),
                parameters: Some(vec![AttributeValue::S("u2".to_string())]),
            },
        ],
        ..Default::default()
    };

    let resp = db.execute_transaction(request).unwrap();
    let responses = resp.responses.unwrap();
    assert_eq!(responses.len(), 2);

    // First response (UPDATE) has no item
    assert!(responses[0].item.is_none());

    // Second response (SELECT) has Bob
    let bob = responses[1].item.as_ref().unwrap();
    assert_eq!(bob.get("name"), Some(&AttributeValue::S("Bob".to_string())));

    // Verify the update took effect
    let items = select_all(&db, "Users");
    let u1 = items.iter().find(|i| match i.get("pk") {
        Some(AttributeValue::S(s)) => s == "u1",
        _ => false,
    });
    match u1.unwrap().get("name") {
        Some(AttributeValue::S(s)) => assert_eq!(s, "Alice V2"),
        other => panic!("Expected updated name, got {:?}", other),
    }
}
