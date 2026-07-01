use dynoxide::Database;
use dynoxide::DynoxideError;
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

// -----------------------------------------------------------------------
// An empty-string key inside ExecuteTransaction keeps the "ValidationError"
// cancellation reason - it must not regress to "InternalError".
// -----------------------------------------------------------------------

#[test]
fn test_empty_string_key_insert_keeps_validation_error_reason() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");

    let request = ExecuteTransactionRequest {
        transact_statements: vec![ParameterizedStatement {
            statement: "INSERT INTO \"Users\" VALUE {'pk': ''}".to_string(),
            parameters: None,
        }],
        ..Default::default()
    };

    let err = db.execute_transaction(request).unwrap_err();
    match err {
        DynoxideError::TransactionCanceledException(_, reasons) => {
            assert_eq!(
                reasons[0].code, "ValidationError",
                "empty-string key must keep the ValidationError reason, not InternalError: {:?}",
                reasons[0]
            );
        }
        other => panic!("expected TransactionCanceledException, got {other:?}"),
    }
}

// -----------------------------------------------------------------------
// ClientRequestToken idempotency and transactional capacity
// -----------------------------------------------------------------------

fn seed_counter(db: &Database, table: &str, pk: &str) {
    db.execute_statement(ExecuteStatementRequest {
        statement: format!("INSERT INTO \"{table}\" VALUE {{'pk': '{pk}', 'n': 0}}"),
        parameters: None,
        ..Default::default()
    })
    .unwrap();
}

fn counter_value(db: &Database, table: &str, pk: &str) -> i64 {
    let resp = db
        .execute_statement(ExecuteStatementRequest {
            statement: format!("SELECT * FROM \"{table}\" WHERE pk = '{pk}'"),
            parameters: None,
            ..Default::default()
        })
        .unwrap();
    let items = resp.items.unwrap_or_default();
    let item = items.first().expect("counter row must exist");
    match item.get("n") {
        Some(AttributeValue::N(n)) => n.parse().expect("n must be numeric"),
        other => panic!("expected numeric n, got {other:?}"),
    }
}

fn increment_counter(pk: &str) -> Vec<ParameterizedStatement> {
    vec![ParameterizedStatement {
        statement: format!("UPDATE \"Counters\" SET n = n + 1 WHERE pk = '{pk}'"),
        parameters: None,
    }]
}

fn stmt(statement: &str) -> ParameterizedStatement {
    ParameterizedStatement {
        statement: statement.to_string(),
        parameters: None,
    }
}

/// A same-token replay returns the stored result without re-applying the
/// statements: the counter moves exactly once. The first call reports
/// transactional WRITE capacity, the in-window replay transactional READ.
#[test]
fn test_execute_transaction_same_token_replays_without_reapplying() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Counters");
    seed_counter(&db, "Counters", "c1");

    let make_request = || ExecuteTransactionRequest {
        transact_statements: increment_counter("c1"),
        client_request_token: Some("counter-token".to_string()),
        return_consumed_capacity: Some("INDEXES".to_string()),
    };

    let first = db.execute_transaction(make_request()).unwrap();
    let first_cap = &first.consumed_capacity.unwrap()[0];
    assert_eq!(first_cap.capacity_units, 2.0);
    assert_eq!(first_cap.write_capacity_units, Some(2.0));
    assert_eq!(first_cap.read_capacity_units, None);

    let replay = db.execute_transaction(make_request()).unwrap();
    let replay_cap = &replay.consumed_capacity.unwrap()[0];
    assert_eq!(replay_cap.capacity_units, 2.0);
    assert_eq!(replay_cap.read_capacity_units, Some(2.0));
    assert_eq!(replay_cap.write_capacity_units, None);

    assert_eq!(
        counter_value(&db, "Counters", "c1"),
        1,
        "the replay must not re-apply the statement"
    );
}

/// A same-token replay honours the replay request's own ReturnConsumedCapacity
/// mode, not the first call's. First call INDEXES; a TOTAL replay reports read
/// capacity in TOTAL shape (no Table breakdown); a no-mode replay reports none.
#[test]
fn test_execute_transaction_replay_honours_replay_mode() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Counters");
    seed_counter(&db, "Counters", "c1");

    db.execute_transaction(ExecuteTransactionRequest {
        transact_statements: increment_counter("c1"),
        client_request_token: Some("mode-token".to_string()),
        return_consumed_capacity: Some("INDEXES".to_string()),
    })
    .unwrap();

    let total = db
        .execute_transaction(ExecuteTransactionRequest {
            transact_statements: increment_counter("c1"),
            client_request_token: Some("mode-token".to_string()),
            return_consumed_capacity: Some("TOTAL".to_string()),
        })
        .unwrap();
    let entry = &total.consumed_capacity.unwrap()[0];
    assert_eq!(entry.capacity_units, 2.0);
    assert_eq!(entry.read_capacity_units, Some(2.0));
    assert_eq!(entry.write_capacity_units, None);
    assert!(
        entry.table.is_none(),
        "TOTAL mode carries no Table breakdown"
    );

    let none = db
        .execute_transaction(ExecuteTransactionRequest {
            transact_statements: increment_counter("c1"),
            client_request_token: Some("mode-token".to_string()),
            return_consumed_capacity: None,
        })
        .unwrap();
    assert!(none.consumed_capacity.is_none());

    assert_eq!(
        counter_value(&db, "Counters", "c1"),
        1,
        "all three same-token calls apply the increment once"
    );
}

/// An all-SELECT transaction is a read set: the first call reports transactional
/// READ capacity, not write. A read set must not be mislabelled as a write.
#[test]
fn test_execute_transaction_read_set_reports_read_capacity() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");
    db.execute_statement(ExecuteStatementRequest {
        statement: "INSERT INTO \"Users\" VALUE {'pk': 'u1', 'name': 'Alice'}".to_string(),
        parameters: None,
        ..Default::default()
    })
    .unwrap();

    let resp = db
        .execute_transaction(ExecuteTransactionRequest {
            transact_statements: vec![ParameterizedStatement {
                statement: "SELECT * FROM \"Users\" WHERE pk = 'u1'".to_string(),
                parameters: None,
            }],
            client_request_token: None,
            return_consumed_capacity: Some("INDEXES".to_string()),
        })
        .unwrap();
    let entry = &resp.consumed_capacity.unwrap()[0];
    assert_eq!(entry.capacity_units, 2.0);
    assert_eq!(entry.read_capacity_units, Some(2.0));
    assert_eq!(entry.write_capacity_units, None);
    assert!(
        entry.table.is_some(),
        "INDEXES mode carries a Table breakdown"
    );
}

/// A transactional write reports write capacity under TOTAL too (top-level
/// WriteCapacityUnits, no ReadCapacityUnits, no Table breakdown).
#[test]
fn test_execute_transaction_write_set_total_mode() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");

    let resp = db
        .execute_transaction(ExecuteTransactionRequest {
            transact_statements: vec![ParameterizedStatement {
                statement: "INSERT INTO \"Users\" VALUE {'pk': 'u1'}".to_string(),
                parameters: None,
            }],
            client_request_token: None,
            return_consumed_capacity: Some("TOTAL".to_string()),
        })
        .unwrap();
    let entry = &resp.consumed_capacity.unwrap()[0];
    assert_eq!(entry.capacity_units, 2.0);
    assert_eq!(entry.write_capacity_units, Some(2.0));
    assert_eq!(entry.read_capacity_units, None);
    assert!(
        entry.table.is_none(),
        "TOTAL mode carries no Table breakdown"
    );
}

/// A same-token call with different statements is a parameter mismatch.
#[test]
fn test_execute_transaction_same_token_different_statements_mismatch() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");

    db.execute_transaction(ExecuteTransactionRequest {
        transact_statements: vec![ParameterizedStatement {
            statement: "INSERT INTO \"Users\" VALUE {'pk': 'u1'}".to_string(),
            parameters: None,
        }],
        client_request_token: Some("mismatch-token".to_string()),
        return_consumed_capacity: None,
    })
    .unwrap();

    let err = db
        .execute_transaction(ExecuteTransactionRequest {
            transact_statements: vec![ParameterizedStatement {
                statement: "INSERT INTO \"Users\" VALUE {'pk': 'u2'}".to_string(),
                parameters: None,
            }],
            client_request_token: Some("mismatch-token".to_string()),
            return_consumed_capacity: None,
        })
        .unwrap_err();
    assert!(
        matches!(err, DynoxideError::IdempotentParameterMismatchException(_)),
        "expected IdempotentParameterMismatchException, got: {err:?}"
    );
}

/// A same-token call differing only in ReturnConsumedCapacity replays rather
/// than mismatching (the hash covers the statements and parameters, not the
/// capacity mode).
#[test]
fn test_execute_transaction_same_token_differing_only_in_capacity_mode_replays() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Counters");
    seed_counter(&db, "Counters", "c1");

    db.execute_transaction(ExecuteTransactionRequest {
        transact_statements: increment_counter("c1"),
        client_request_token: Some("mode-only-token".to_string()),
        return_consumed_capacity: None,
    })
    .unwrap();

    // Same statements, same token, different capacity mode: must replay, not error.
    let replay = db
        .execute_transaction(ExecuteTransactionRequest {
            transact_statements: increment_counter("c1"),
            client_request_token: Some("mode-only-token".to_string()),
            return_consumed_capacity: Some("INDEXES".to_string()),
        })
        .unwrap();
    assert_eq!(
        replay.consumed_capacity.unwrap()[0].read_capacity_units,
        Some(2.0)
    );
    assert_eq!(counter_value(&db, "Counters", "c1"), 1);
}

/// A tokenless transaction is never cached: repeated calls re-execute.
#[test]
fn test_execute_transaction_tokenless_reexecutes() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Counters");
    seed_counter(&db, "Counters", "c1");

    let run = || {
        db.execute_transaction(ExecuteTransactionRequest {
            transact_statements: increment_counter("c1"),
            client_request_token: None,
            return_consumed_capacity: None,
        })
        .unwrap();
    };
    run();
    run();
    assert_eq!(
        counter_value(&db, "Counters", "c1"),
        2,
        "tokenless calls re-execute each time"
    );
}

/// Concurrency: N threads racing the same token apply the statements exactly
/// once. The hold-lock-across-execute guard serialises them: one executes, the
/// rest replay, so the counter moves once and every call returns Ok.
#[test]
fn test_execute_transaction_concurrent_same_token_executes_once() {
    use std::sync::{Arc, Barrier};

    let db = Database::memory().unwrap();
    create_test_table(&db, "Counters");
    seed_counter(&db, "Counters", "c1");

    const N: usize = 16;
    let barrier = Arc::new(Barrier::new(N));
    let handles: Vec<_> = (0..N)
        .map(|_| {
            let db = db.clone();
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                db.execute_transaction(ExecuteTransactionRequest {
                    transact_statements: increment_counter("c1"),
                    client_request_token: Some("race-token".to_string()),
                    return_consumed_capacity: None,
                })
                .is_ok()
            })
        })
        .collect();

    let oks = handles
        .into_iter()
        .map(|h| h.join().unwrap())
        .filter(|&ok| ok)
        .count();
    assert_eq!(
        oks, N,
        "all {N} same-token calls should return Ok (one executes, the rest replay); got {oks}"
    );
    assert_eq!(
        counter_value(&db, "Counters", "c1"),
        1,
        "the increment must apply exactly once"
    );
}

/// A ClientRequestToken longer than 36 characters is rejected up front with a
/// ValidationException, matching DynamoDB and the TransactWriteItems path.
#[test]
fn test_execute_transaction_rejects_overlong_token() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");

    let err = db
        .execute_transaction(ExecuteTransactionRequest {
            transact_statements: vec![stmt("INSERT INTO \"Users\" VALUE {'pk': 'u1'}")],
            client_request_token: Some("x".repeat(37)),
            return_consumed_capacity: None,
        })
        .unwrap_err();
    assert!(
        matches!(err, DynoxideError::ValidationException(_)),
        "expected ValidationException, got: {err:?}"
    );
    let msg = err.to_string();
    assert!(msg.contains("clientRequestToken"), "message was: {msg}");
    assert!(msg.contains("36"), "message was: {msg}");
}

/// A first call that cancels is not cached: a same-token retry with different
/// (valid) statements re-executes rather than replaying a phantom success or
/// raising IdempotentParameterMismatchException.
#[test]
fn test_execute_transaction_failed_call_not_cached() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "Users");
    // Seed u1 so the first transaction's INSERT collides and cancels.
    db.execute_statement(ExecuteStatementRequest {
        statement: "INSERT INTO \"Users\" VALUE {'pk': 'u1'}".to_string(),
        parameters: None,
        ..Default::default()
    })
    .unwrap();

    // First call under the token fails (duplicate key) and must not be cached.
    let first = db.execute_transaction(ExecuteTransactionRequest {
        transact_statements: vec![stmt("INSERT INTO \"Users\" VALUE {'pk': 'u1'}")],
        client_request_token: Some("retry-token".to_string()),
        return_consumed_capacity: None,
    });
    assert!(first.is_err(), "duplicate-key insert should cancel");

    // Same token, different valid statements: re-executes (not a mismatch, not
    // a replay), proving the failed first call left no cache entry.
    let second = db.execute_transaction(ExecuteTransactionRequest {
        transact_statements: vec![stmt("INSERT INTO \"Users\" VALUE {'pk': 'u2'}")],
        client_request_token: Some("retry-token".to_string()),
        return_consumed_capacity: None,
    });
    assert!(
        second.is_ok(),
        "failed first call must not be cached, so the retry executes: {second:?}"
    );
    let pks: Vec<String> = select_all(&db, "Users")
        .iter()
        .filter_map(|i| match i.get("pk") {
            Some(AttributeValue::S(s)) => Some(s.clone()),
            _ => None,
        })
        .collect();
    assert!(
        pks.contains(&"u2".to_string()),
        "the retry must have applied"
    );
}

/// Capacity aggregates per target table: a transaction touching two tables
/// under INDEXES reports one entry per table, each transactional write 2 units
/// with a Table breakdown.
#[test]
fn test_execute_transaction_multi_table_capacity() {
    let db = Database::memory().unwrap();
    create_test_table(&db, "TableA");
    create_test_table(&db, "TableB");

    let resp = db
        .execute_transaction(ExecuteTransactionRequest {
            transact_statements: vec![
                stmt("INSERT INTO \"TableA\" VALUE {'pk': 'a'}"),
                stmt("INSERT INTO \"TableB\" VALUE {'pk': 'b'}"),
            ],
            client_request_token: None,
            return_consumed_capacity: Some("INDEXES".to_string()),
        })
        .unwrap();
    let caps = resp.consumed_capacity.unwrap();
    assert_eq!(caps.len(), 2, "one ConsumedCapacity entry per table");
    for entry in &caps {
        assert_eq!(entry.capacity_units, 2.0);
        assert_eq!(entry.write_capacity_units, Some(2.0));
        assert_eq!(entry.read_capacity_units, None);
        assert!(entry.table.is_some(), "INDEXES carries a Table breakdown");
    }
    let mut tables: Vec<&str> = caps.iter().map(|c| c.table_name.as_str()).collect();
    tables.sort_unstable();
    assert_eq!(tables, vec!["TableA", "TableB"]);
}
