//! The two fields `BatchStatementRequest` carries beyond `Statement` and
//! `Parameters`.
//!
//! Both were absent, so a member setting either was parsed as though it had not.
//! Measured against real DynamoDB in eu-west-2 on 15 August 2026; case labels
//! (B1, B7) refer to that capture.
//!
//! They resolve differently. `ConsistentRead` is honoured per member and changes
//! the rate a read is charged at. `ReturnValuesOnConditionCheckFailure` is inert
//! on this surface: DynamoDB returns the same response whatever it says, so
//! dynoxide accepting and ignoring it is the matching behaviour, and the tests
//! below pin that rather than assuming it.

use dynoxide::Database;
use dynoxide::types::ConsumedCapacity;

const TABLE: &str = "batch_fields";

fn seeded() -> Database {
    let db = Database::memory().unwrap();
    db.create_table(
        serde_json::from_value(serde_json::json!({
            "TableName": TABLE,
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "gsiPk", "AttributeType": "S"}
            ],
            "BillingMode": "PAY_PER_REQUEST",
            "GlobalSecondaryIndexes": [{
                "IndexName": "gsi-all",
                "KeySchema": [{"AttributeName": "gsiPk", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"}
            }]
        }))
        .unwrap(),
    )
    .unwrap();
    for (pk, gsi, val) in [("k1", "g1", "v1"), ("k2", "g2", "v2"), ("k3", "g3", "v3")] {
        db.put_item(
            serde_json::from_value(serde_json::json!({
                "TableName": TABLE,
                "Item": {"pk": {"S": pk}, "gsiPk": {"S": gsi}, "val": {"S": val}}
            }))
            .unwrap(),
        )
        .unwrap();
    }
    db
}

fn run(
    db: &Database,
    statements: serde_json::Value,
) -> dynoxide::actions::batch_execute_statement::BatchExecuteStatementResponse {
    db.batch_execute_statement(
        serde_json::from_value(serde_json::json!({
            "Statements": statements,
            "ReturnConsumedCapacity": "INDEXES"
        }))
        .unwrap(),
    )
    .unwrap()
}

fn total(cap: &Option<Vec<ConsumedCapacity>>) -> f64 {
    cap.as_ref()
        .map(|entries| entries.iter().map(|e| e.capacity_units).sum())
        .unwrap_or(0.0)
}

// --- ConsistentRead -------------------------------------------------------

#[test]
fn a_batch_read_is_charged_the_eventual_rate_without_the_flag() {
    // B1.
    let db = seeded();
    let resp = run(
        &db,
        serde_json::json!([{"Statement": format!("SELECT * FROM \"{TABLE}\" WHERE pk='k1'")}]),
    );
    assert_eq!(total(&resp.consumed_capacity), 0.5);
}

#[test]
fn a_batch_read_is_charged_the_consistent_rate_with_the_flag() {
    // B2. Before the field existed this was charged 0.5, the same as without it.
    let db = seeded();
    let resp = run(
        &db,
        serde_json::json!([{
            "Statement": format!("SELECT * FROM \"{TABLE}\" WHERE pk='k1'"),
            "ConsistentRead": true
        }]),
    );
    assert_eq!(total(&resp.consumed_capacity), 1.0);
}

#[test]
fn consistent_read_false_matches_omitting_it() {
    // B3. The control: an explicit false is not a third state.
    let db = seeded();
    let resp = run(
        &db,
        serde_json::json!([{
            "Statement": format!("SELECT * FROM \"{TABLE}\" WHERE pk='k1'"),
            "ConsistentRead": false
        }]),
    );
    assert_eq!(total(&resp.consumed_capacity), 0.5);
}

#[test]
fn the_flag_is_per_member_not_per_batch() {
    // B4. One consistent member and one eventual sum to 1.5, so each is rated
    // on its own setting rather than the batch taking one mode.
    let db = seeded();
    let resp = run(
        &db,
        serde_json::json!([
            {
                "Statement": format!("SELECT * FROM \"{TABLE}\" WHERE pk='k1'"),
                "ConsistentRead": true
            },
            {"Statement": format!("SELECT * FROM \"{TABLE}\" WHERE pk='k2'")}
        ]),
    );
    assert_eq!(total(&resp.consumed_capacity), 1.5);
    assert_eq!(resp.responses.len(), 2);
    assert!(resp.responses.iter().all(|r| r.error.is_none()));
}

#[test]
fn the_flag_does_not_change_which_rows_come_back() {
    // Every read against SQLite is already strongly consistent, so the flag is
    // a billing input here and nothing else.
    let db = seeded();
    for consistent in [true, false] {
        let resp = run(
            &db,
            serde_json::json!([{
                "Statement": format!("SELECT * FROM \"{TABLE}\" WHERE pk='k1'"),
                "ConsistentRead": consistent
            }]),
        );
        assert!(resp.responses[0].item.is_some(), "consistent={consistent}");
    }
}

// --- ReturnValuesOnConditionCheckFailure -----------------------------------

#[test]
fn a_failed_condition_returns_no_item_whatever_the_option_says() {
    // B7, B8, B9. DynamoDB returns the same response for all three, and never
    // the item. The option works on TransactWriteItems, which is what rules out
    // a bad probe, so this is DynamoDB declining rather than dynoxide dropping
    // the field.
    let db = seeded();
    for option in [Some("ALL_OLD"), Some("NONE"), None] {
        let mut member = serde_json::json!({
            "Statement": format!("UPDATE \"{TABLE}\" SET val='changed' WHERE pk='k1' AND val='nomatch'")
        });
        if let Some(value) = option {
            member["ReturnValuesOnConditionCheckFailure"] = serde_json::json!(value);
        }
        let resp = run(&db, serde_json::json!([member]));
        let response = &resp.responses[0];
        let error = response
            .error
            .as_ref()
            .unwrap_or_else(|| panic!("option {option:?} should fail the condition"));
        assert_eq!(error.code, "ConditionalCheckFailed", "option {option:?}");
        assert_eq!(error.message, "The conditional request failed");
        assert!(
            response.item.is_none(),
            "option {option:?} must not return the item"
        );
        // A member that ran and failed still echoes its table.
        assert_eq!(response.table_name.as_deref(), Some(TABLE));
    }
}

#[test]
fn a_duplicate_insert_returns_no_item_with_the_option_set() {
    // B11. A different failure kind, same answer.
    let db = seeded();
    let resp = run(
        &db,
        serde_json::json!([{
            "Statement": format!("INSERT INTO \"{TABLE}\" VALUE {{'pk':'k3','val':'dupe'}}"),
            "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
        }]),
    );
    let response = &resp.responses[0];
    let error = response.error.as_ref().expect("a duplicate insert fails");
    assert_eq!(error.code, "DuplicateItem");
    assert!(response.item.is_none());
}

#[test]
fn a_failing_member_still_carries_the_capacity_surcharge() {
    // B10. The option changes the response and not the billing: one failing
    // member alongside one that succeeds reports a total above the arms, the
    // failure being charged the write it attempted.
    let db = seeded();
    let resp = run(
        &db,
        serde_json::json!([
            {
                "Statement": format!("UPDATE \"{TABLE}\" SET val='changed' WHERE pk='k1' AND val='nomatch'"),
                "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
            },
            {"Statement": format!("UPDATE \"{TABLE}\" SET val='ok' WHERE pk='k2'")}
        ]),
    );
    let entries = resp.consumed_capacity.expect("INDEXES reports capacity");
    let entry = &entries[0];
    let arms: f64 = entry
        .table
        .as_ref()
        .map(|t| t.capacity_units)
        .unwrap_or(0.0)
        + entry
            .global_secondary_indexes
            .as_ref()
            .map(|m| m.values().map(|d| d.capacity_units).sum::<f64>())
            .unwrap_or(0.0);
    assert!(
        entry.capacity_units > arms,
        "the failed statement's write should sit on the total and not on an arm: \
         total {} arms {arms}",
        entry.capacity_units
    );
}

// --- neither field disturbs what was already there ------------------------

#[test]
fn an_unknown_member_field_is_still_ignored() {
    // Adding fields must not tighten deserialisation into rejecting shapes it
    // used to accept.
    let db = seeded();
    let resp = run(
        &db,
        serde_json::json!([{
            "Statement": format!("SELECT * FROM \"{TABLE}\" WHERE pk='k1'"),
            "SomethingDynoxideDoesNotKnow": true
        }]),
    );
    assert!(resp.responses[0].error.is_none());
}
