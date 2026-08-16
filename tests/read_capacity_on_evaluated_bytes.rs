//! A read is charged on the bytes it evaluated, not the bytes it returned.
//!
//! DynamoDB sizes a read before the filter and before the projection, so a scan
//! matching one row costs what a scan matching every row costs, and asking for
//! one attribute costs what asking for all of them costs. Measured against real
//! DynamoDB in eu-west-2 on 15 and 16 August 2026: a read matching one row and a
//! read matching nothing reported the same capacity as the unfiltered
//! `SELECT *` over the same rows on all ten fixtures carrying a filter case, and
//! a read projecting the sort key alone did the same on all six carrying a
//! projection case.
//!
//! `Scan` and `Query` already did this and nothing guarded it. PartiQL did not.
//!
//! The fixture holds four rows of a little over 3KB, so the four together cross
//! two 4KB read units and one row on its own does not. That gap is what makes a
//! charge on returned bytes visible: it reports 0.5 where the whole read is 1.5.

use dynoxide::Database;

const TABLE: &str = "read_capacity";

/// A little over 3KB per row, so four rows span three read units and one row
/// spans one. Anything smaller hides the difference inside the 4KB rounding.
const FILLER: usize = 3000;

fn table() -> Database {
    let db = Database::memory().unwrap();
    db.create_table(
        serde_json::from_value(serde_json::json!({
            "TableName": TABLE,
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"}
            ],
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"}
            ],
            "BillingMode": "PAY_PER_REQUEST"
        }))
        .unwrap(),
    )
    .unwrap();

    // One row tagged `one` and three tagged `many`, so a filter can match a
    // known fraction. This is the fixture shape the captures used.
    for i in 0..4 {
        let req = serde_json::json!({
            "TableName": TABLE,
            "Item": {
                "pk": {"S": "p"},
                "sk": {"S": format!("s{i:04}")},
                "tag": {"S": if i == 0 { "one" } else { "many" }},
                "filler": {"S": "x".repeat(FILLER)}
            }
        });
        db.put_item(serde_json::from_value(req).unwrap()).unwrap();
    }
    db
}

/// Run a PartiQL statement and return its total capacity.
fn statement(db: &Database, sql: &str) -> f64 {
    let req = serde_json::json!({"Statement": sql, "ReturnConsumedCapacity": "TOTAL"});
    db.execute_statement(serde_json::from_value(req).unwrap())
        .unwrap()
        .consumed_capacity
        .expect("TOTAL mode reports capacity")
        .capacity_units
}

/// Run a `Scan` and return its total capacity.
fn scan(db: &Database, extra: serde_json::Value) -> f64 {
    let mut req = serde_json::json!({
        "TableName": TABLE,
        "ReturnConsumedCapacity": "TOTAL"
    });
    let map = req.as_object_mut().unwrap();
    for (k, v) in extra.as_object().unwrap() {
        map.insert(k.clone(), v.clone());
    }
    db.scan(serde_json::from_value(req).unwrap())
        .unwrap()
        .consumed_capacity
        .expect("TOTAL mode reports capacity")
        .capacity_units
}

// ---------------------------------------------------------------------------
// PartiQL
// ---------------------------------------------------------------------------

#[test]
fn an_unkeyed_select_is_charged_on_every_row_it_walked() {
    let db = table();
    let all = statement(&db, &format!("SELECT * FROM \"{TABLE}\""));
    assert_eq!(all, 1.5, "four rows of ~3KB span three read units");
}

#[test]
fn a_filter_does_not_reduce_what_an_unkeyed_select_costs() {
    let db = table();
    let all = statement(&db, &format!("SELECT * FROM \"{TABLE}\""));

    let one = statement(&db, &format!("SELECT * FROM \"{TABLE}\" WHERE tag='one'"));
    assert_eq!(
        one, all,
        "the filter runs after the read, so it is not free"
    );

    let none = statement(
        &db,
        &format!("SELECT * FROM \"{TABLE}\" WHERE tag='absent'"),
    );
    assert_eq!(none, all, "matching nothing still walked every row");
}

#[test]
fn a_projection_does_not_reduce_what_an_unkeyed_select_costs() {
    let db = table();
    let all = statement(&db, &format!("SELECT * FROM \"{TABLE}\""));
    let keys_only = statement(&db, &format!("SELECT sk FROM \"{TABLE}\""));
    assert_eq!(
        keys_only, all,
        "the row is read whole and then narrowed, so the filler is still paid for"
    );
}

#[test]
fn a_keyed_select_is_charged_on_the_rows_its_key_condition_reached() {
    let db = table();
    // The key condition reaches the whole partition, and the residual on `tag`
    // is a filter over those rows rather than a narrowing of the read.
    let keyed = statement(&db, &format!("SELECT * FROM \"{TABLE}\" WHERE pk='p'"));
    assert_eq!(keyed, 1.5);

    let residual = statement(
        &db,
        &format!("SELECT * FROM \"{TABLE}\" WHERE pk='p' AND tag='one'"),
    );
    assert_eq!(residual, keyed, "a non-key residual is a filter, not a key");

    // A key condition that does narrow the read is charged on the narrower set.
    let single = statement(
        &db,
        &format!("SELECT * FROM \"{TABLE}\" WHERE pk='p' AND sk='s0000'"),
    );
    assert_eq!(single, 0.5, "one row of ~3KB is one read unit");
}

#[test]
fn consistent_read_doubles_the_charge_on_the_evaluated_bytes() {
    let db = table();
    let req = serde_json::json!({
        "Statement": format!("SELECT * FROM \"{TABLE}\" WHERE tag='one'"),
        "ConsistentRead": true,
        "ReturnConsumedCapacity": "TOTAL"
    });
    let units = db
        .execute_statement(serde_json::from_value(req).unwrap())
        .unwrap()
        .consumed_capacity
        .expect("TOTAL mode reports capacity")
        .capacity_units;
    assert_eq!(units, 3.0, "three units, undiscounted");
}

#[test]
fn a_limit_bounds_the_rows_evaluated_and_so_the_charge() {
    let db = table();
    let req = serde_json::json!({
        "Statement": format!("SELECT * FROM \"{TABLE}\" WHERE tag='absent'"),
        "Limit": 1,
        "ReturnConsumedCapacity": "TOTAL"
    });
    let units = db
        .execute_statement(serde_json::from_value(req).unwrap())
        .unwrap()
        .consumed_capacity
        .expect("TOTAL mode reports capacity")
        .capacity_units;
    assert_eq!(
        units, 0.5,
        "one row read, none matched, and one row is still charged for"
    );
}

// ---------------------------------------------------------------------------
// Scan, which already behaved this way and had nothing holding it there
// ---------------------------------------------------------------------------

#[test]
fn a_filter_does_not_reduce_what_a_scan_costs() {
    let db = table();
    let all = scan(&db, serde_json::json!({}));
    assert_eq!(all, 1.5);

    let filtered = scan(
        &db,
        serde_json::json!({
            "FilterExpression": "tag = :t",
            "ExpressionAttributeValues": {":t": {"S": "absent"}}
        }),
    );
    assert_eq!(filtered, all, "a scan is sized before its filter runs");
}

#[test]
fn a_projection_does_not_reduce_what_a_scan_costs() {
    let db = table();
    let all = scan(&db, serde_json::json!({}));
    let projected = scan(&db, serde_json::json!({"ProjectionExpression": "sk"}));
    assert_eq!(projected, all, "a scan is sized before its projection runs");
}
