//! Parenthesised grouping, `NOT`, and the operand types an ordering comparison
//! accepts.
//!
//! `compatibility-summary.md` listed `AND`, `OR`, `NOT` and parenthesised
//! grouping as supported. Only `AND` and `OR` were, in a flat OR-of-ANDs with no
//! notion of parentheses: even `WHERE (a='1')` was a parse error. DynamoDB parses
//! all of it, confirmed eu-west-2 2026-08-15 by firing each shape at a table that
//! does not exist and watching it reach table resolution rather than fail to
//! parse.
//!
//! The clause is still flattened to an OR of ANDs for execution, so what is
//! tested here is that the flattening preserves meaning, including operator
//! precedence and De Morgan over a negated group.

use dynoxide::Database;
use dynoxide::actions::execute_statement::ExecuteStatementRequest;
use dynoxide::types::AttributeValue;
use std::collections::HashMap;

const TABLE: &str = "pq_bool";

fn seeded() -> Database {
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
    for (pk, a, b) in [("1", "x", "p"), ("2", "y", "q"), ("3", "z", "p")] {
        let mut item = HashMap::new();
        item.insert("pk".to_string(), AttributeValue::S(pk.to_string()));
        item.insert("a".to_string(), AttributeValue::S(a.to_string()));
        item.insert("b".to_string(), AttributeValue::S(b.to_string()));
        db.put_item(
            serde_json::from_value(serde_json::json!({"TableName": TABLE, "Item": item})).unwrap(),
        )
        .unwrap();
    }
    db
}

/// The sorted keys a WHERE clause selects.
fn selects(db: &Database, where_clause: &str) -> String {
    let resp = db
        .execute_statement(ExecuteStatementRequest {
            statement: format!("SELECT pk FROM \"{TABLE}\" WHERE {where_clause}"),
            ..Default::default()
        })
        .unwrap_or_else(|e| panic!("{where_clause} failed: {e}"));
    let mut keys: Vec<String> = resp
        .items
        .unwrap_or_default()
        .iter()
        .filter_map(|i| match i.get("pk") {
            Some(AttributeValue::S(s)) => Some(s.clone()),
            _ => None,
        })
        .collect();
    keys.sort();
    keys.join(",")
}

#[test]
fn parentheses_are_accepted_and_change_nothing_on_their_own() {
    let db = seeded();
    assert_eq!(selects(&db, "a='x'"), "1");
    assert_eq!(selects(&db, "(a='x')"), "1");
    assert_eq!(selects(&db, "((a='x'))"), "1");
}

#[test]
fn parentheses_override_operator_precedence() {
    let db = seeded();
    // AND binds tighter, so the unparenthesised form is a='x' OR (a='y' AND b='q').
    assert_eq!(selects(&db, "a='x' OR a='y' AND b='q'"), "1,2");
    // Parenthesising the OR distributes it over the AND: only item 1 has b='p'.
    assert_eq!(selects(&db, "(a='x' OR a='y') AND b='p'"), "1");
    assert_eq!(selects(&db, "b='p' AND (a='x' OR a='y')"), "1");
}

#[test]
fn not_negates_a_comparison() {
    let db = seeded();
    assert_eq!(selects(&db, "NOT a='x'"), "2,3");
    assert_eq!(selects(&db, "NOT NOT a='x'"), "1");
}

#[test]
fn not_over_a_group_applies_de_morgan() {
    let db = seeded();
    // NOT (a='x' OR a='y') is a<>'x' AND a<>'y'.
    assert_eq!(selects(&db, "NOT (a='x' OR a='y')"), "3");
    // NOT (a='x' AND b='p') is a<>'x' OR b<>'p', which excludes only item 1.
    assert_eq!(selects(&db, "NOT (a='x' AND b='p')"), "2,3");
}

#[test]
fn not_over_in_and_between_expands_to_the_right_shape() {
    let db = seeded();
    // NOT IN is a conjunction of inequalities.
    assert_eq!(selects(&db, "NOT a IN ['x','y']"), "3");
    // NOT BETWEEN is a disjunction, which a single condition cannot hold.
    assert_eq!(selects(&db, "NOT b BETWEEN 'p' AND 'p'"), "2");
}

#[test]
fn not_contains_is_the_complement_of_contains() {
    let db = seeded();
    assert_eq!(selects(&db, "CONTAINS(a, 'x')"), "1");
    assert_eq!(selects(&db, "NOT CONTAINS(a, 'x')"), "2,3");
}

#[test]
fn the_condition_level_negations_still_parse_as_themselves() {
    // NOT EXISTS and NOT BEGINS_WITH are single conditions with their own
    // variants, not the boolean operator applied to a function call. The
    // boolean NOT must not swallow them.
    let db = seeded();
    assert_eq!(selects(&db, "NOT EXISTS(missing)"), "1,2,3");
    assert_eq!(selects(&db, "NOT BEGINS_WITH(a, 'x')"), "2,3");
}

#[test]
fn an_unclosed_parenthesis_is_a_parse_error() {
    let db = seeded();
    let err = db
        .execute_statement(ExecuteStatementRequest {
            statement: format!("SELECT pk FROM \"{TABLE}\" WHERE (a='x'"),
            ..Default::default()
        })
        .expect_err("an unclosed group is malformed")
        .to_string();
    assert!(err.contains("wasn't well formed"), "got {err}");
}

// --- operand types on an ordering comparison ------------------------------

#[test]
fn an_ordering_comparison_rejects_an_operand_with_no_ordering() {
    // DynamoDB orders S, N and B and nothing else, and rejects the statement
    // rather than declining to match. The check fires before the table is
    // resolved. Captured eu-west-2 2026-08-15.
    let db = seeded();
    for (operand, type_name) in [
        (serde_json::json!({"BOOL": true}), "BOOL"),
        (serde_json::json!({"NULL": true}), "NULL"),
        (serde_json::json!({"L": [{"S": "a"}]}), "L"),
        (serde_json::json!({"M": {"k": {"S": "v"}}}), "M"),
        (serde_json::json!({"SS": ["a"]}), "SS"),
        (serde_json::json!({"NS": ["1"]}), "NS"),
        (serde_json::json!({"BS": ["YQ=="]}), "BS"),
    ] {
        for op in ["<", "<=", ">", ">="] {
            let err = db
                .execute_statement(
                    serde_json::from_value(serde_json::json!({
                        "Statement": format!("SELECT pk FROM \"{TABLE}\" WHERE a {op} ?"),
                        "Parameters": [operand]
                    }))
                    .unwrap(),
                )
                .expect_err("an unorderable operand is rejected")
                .to_string();
            assert!(
                err.contains(&format!(
                    "Incorrect operand type for operator or function; \
                     operator or function: {op}, operand type: {type_name}"
                )),
                "for {op} on {type_name}: got {err}"
            );
        }
    }
}

#[test]
fn between_rejects_an_operand_with_no_ordering() {
    let db = seeded();
    let err = db
        .execute_statement(
            serde_json::from_value(serde_json::json!({
                "Statement": format!("SELECT pk FROM \"{TABLE}\" WHERE a BETWEEN ? AND ?"),
                "Parameters": [{"BOOL": true}, {"BOOL": false}]
            }))
            .unwrap(),
        )
        .expect_err("an unorderable operand is rejected")
        .to_string();
    assert!(
        err.contains("operator or function: BETWEEN, operand type: BOOL"),
        "got {err}"
    );
}

#[test]
fn the_orderable_types_are_accepted() {
    // S, N and B all order. Binary in particular: the predicate work found the
    // old comparison never reached a binary arm at all.
    let db = seeded();
    for operand in [
        serde_json::json!({"S": "zz"}),
        serde_json::json!({"N": "1"}),
        serde_json::json!({"B": "YQ=="}),
    ] {
        assert!(
            db.execute_statement(
                serde_json::from_value(serde_json::json!({
                    "Statement": format!("SELECT pk FROM \"{TABLE}\" WHERE a < ?"),
                    "Parameters": [operand]
                }))
                .unwrap(),
            )
            .is_ok(),
            "an orderable operand must be accepted"
        );
    }
}

#[test]
fn equality_accepts_every_operand_type() {
    // `=` and `<>` are defined for every type, so the rejection must not reach
    // them. This is the control that keeps the check narrow.
    let db = seeded();
    for op in ["=", "<>"] {
        assert!(
            db.execute_statement(
                serde_json::from_value(serde_json::json!({
                    "Statement": format!("SELECT pk FROM \"{TABLE}\" WHERE a {op} ?"),
                    "Parameters": [{"BOOL": true}]
                }))
                .unwrap(),
            )
            .is_ok(),
            "{op} must accept any operand type"
        );
    }
}

// --- nesting is bounded ---------------------------------------------------
//
// Both halves of the parser are recursive descents, and neither had a bound.
// A statement nested past the stack's depth overflowed it, and a stack overflow
// is not a rejection: the release profile aborts on panic, so one statement took
// the host process with it rather than failing on its own. These fix the depth
// budget in place, so what used to abort now comes back as a parse error.

/// The parse rejection a statement gets, or a panic naming what came back
/// instead.
fn parse_rejection(db: &Database, statement: String) -> String {
    match db.execute_statement(ExecuteStatementRequest {
        statement,
        ..Default::default()
    }) {
        Ok(_) => panic!("a statement nested past the budget must be rejected"),
        Err(e) => e.to_string(),
    }
}

#[test]
fn a_deeply_parenthesised_clause_is_rejected_rather_than_overflowing_the_stack() {
    let db = seeded();
    let err = parse_rejection(
        &db,
        format!(
            "SELECT * FROM \"{TABLE}\" WHERE {}pk='1'{}",
            "(".repeat(500),
            ")".repeat(500)
        ),
    );
    assert!(err.contains("nested too deeply"), "got {err}");
}

#[test]
fn a_long_run_of_nots_is_rejected_rather_than_overflowing_the_stack() {
    // `NOT` recurses on its own, so it reaches the same cliff by a different
    // route: this one needs no parentheses at all.
    let db = seeded();
    let err = parse_rejection(
        &db,
        format!(
            "SELECT * FROM \"{TABLE}\" WHERE {}pk='1'",
            "NOT ".repeat(5000)
        ),
    );
    assert!(err.contains("nested too deeply"), "got {err}");
}

#[test]
fn a_clause_nested_within_the_budget_still_parses() {
    // The bound has to leave room for anything anyone would write. Ten levels
    // of parentheses is already far past that and must still run.
    let db = seeded();
    assert_eq!(
        selects(&db, &format!("{}a='x'{}", "(".repeat(10), ")".repeat(10))),
        "1"
    );
}

// --- a negated comparison and a missing attribute --------------------------

const GAP_TABLE: &str = "pq_bool_gap";

/// Three items, one of which carries no `a` at all, so a negation has something
/// to be wrong about.
fn seeded_with_a_gap() -> Database {
    let db = Database::memory().unwrap();
    db.create_table(
        serde_json::from_value(serde_json::json!({
            "TableName": GAP_TABLE,
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "BillingMode": "PAY_PER_REQUEST"
        }))
        .unwrap(),
    )
    .unwrap();
    for (pk, a) in [("1", Some("x")), ("2", Some("y")), ("3", None)] {
        let mut item = HashMap::new();
        item.insert("pk".to_string(), AttributeValue::S(pk.to_string()));
        if let Some(a) = a {
            item.insert("a".to_string(), AttributeValue::S(a.to_string()));
        }
        db.put_item(
            serde_json::from_value(serde_json::json!({"TableName": GAP_TABLE, "Item": item}))
                .unwrap(),
        )
        .unwrap();
    }
    db
}

fn selects_with_a_gap(db: &Database, where_clause: &str) -> String {
    let resp = db
        .execute_statement(ExecuteStatementRequest {
            statement: format!("SELECT pk FROM \"{GAP_TABLE}\" WHERE {where_clause}"),
            ..Default::default()
        })
        .unwrap_or_else(|e| panic!("{where_clause} failed: {e}"));
    let mut keys: Vec<String> = resp
        .items
        .unwrap_or_default()
        .iter()
        .filter_map(|i| match i.get("pk") {
            Some(AttributeValue::S(s)) => Some(s.clone()),
            _ => None,
        })
        .collect();
    keys.sort();
    keys.join(",")
}

#[test]
fn a_negated_comparison_agrees_with_not_contains_on_a_missing_attribute() {
    // `a = 'x'` is false on an item carrying no `a`, so `NOT a = 'x'` is true
    // and that row belongs in the result. Negating by flipping the operator to
    // `<>` asked a comparison of a value that is not there, which is false
    // again, so the row was dropped: `NOT a='x'` and `NOT CONTAINS(a, 'x')`
    // disagreed about item 3 while meaning the same thing about it.
    let db = seeded_with_a_gap();
    assert_eq!(selects_with_a_gap(&db, "NOT CONTAINS(a, 'x')"), "2,3");
    assert_eq!(selects_with_a_gap(&db, "NOT a='x'"), "2,3");
    // The unnegated form is the control: a missing attribute matches nothing.
    assert_eq!(selects_with_a_gap(&db, "a='x'"), "1");
}

#[test]
fn not_in_and_not_between_keep_a_missing_attribute_too() {
    // Both expand into negated comparisons rather than flipped ones, so the
    // rule holds through the expansion as well as at a single condition.
    let db = seeded_with_a_gap();
    assert_eq!(selects_with_a_gap(&db, "NOT a IN ['x']"), "2,3");
    assert_eq!(selects_with_a_gap(&db, "NOT a BETWEEN 'x' AND 'x'"), "2,3");
    // And a negated group, which reaches the same place through De Morgan.
    assert_eq!(selects_with_a_gap(&db, "NOT (a='x' OR a='y')"), "3");
}

// --- what UPDATE and DELETE refuse ----------------------------------------

#[test]
fn a_not_in_an_update_where_is_not_refused_as_an_or() {
    // UPDATE and DELETE refuse an OR. De Morgan turns a `NOT` over a
    // conjunction into a disjunction, so a clause counted after flattening
    // looked like an OR to the guard and the rejection named something the
    // author never wrote. The clause is now judged on what was written.
    let db = seeded();
    db.execute_statement(ExecuteStatementRequest {
        statement: format!("UPDATE \"{TABLE}\" SET b='z' WHERE pk='1' AND NOT (a='q' AND b='q')"),
        ..Default::default()
    })
    .expect("a NOT in an UPDATE WHERE is not an OR");
    assert_eq!(selects(&db, "b='z'"), "1");
}

#[test]
fn an_or_in_an_update_where_is_still_refused() {
    // The control. An OR the author did write is still refused, and the
    // message still names it.
    let db = seeded();
    let err = db
        .execute_statement(ExecuteStatementRequest {
            statement: format!("UPDATE \"{TABLE}\" SET b='z' WHERE pk='1' OR pk='2'"),
            ..Default::default()
        })
        .expect_err("an OR in an UPDATE WHERE is refused")
        .to_string();
    assert!(err.contains("does not support OR conditions"), "got {err}");
}
