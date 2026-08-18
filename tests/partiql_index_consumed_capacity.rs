//! Per-index `ConsumedCapacity` on PartiQL writes.
//!
//! Every figure asserted here was measured against real DynamoDB in eu-west-2 on
//! 13 August 2026, against a table keyed `pk`/`sk` with a GSI `gsi-inc` on
//! `gsiPk` projecting `INCLUDE [proj]` and an LSI `lsi-all` on `pk`/`lsiSk`
//! projecting `ALL`. Case labels (C1, D2, M4) refer to that capture.
//!
//! The three surfaces divide cleanly. `ExecuteStatement` and
//! `BatchExecuteStatement` are charged exactly as the equivalent single-item
//! write, with no transactional factor. `ExecuteTransaction` doubles the base
//! table arm and leaves the index arms alone, matching `TransactWriteItems`.

use dynoxide::Database;
use dynoxide::types::{CapacityDetail, ConsumedCapacity};
use std::collections::HashMap;

const TABLE: &str = "pq_idx_wcu";
const OTHER_TABLE: &str = "pq_idx_wcu_b";

fn table_def(name: &str) -> serde_json::Value {
    serde_json::json!({
        "TableName": name,
        "KeySchema": [
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"}
        ],
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "gsiPk", "AttributeType": "S"},
            {"AttributeName": "lsiSk", "AttributeType": "S"}
        ],
        "BillingMode": "PAY_PER_REQUEST",
        "GlobalSecondaryIndexes": [{
            "IndexName": "gsi-inc",
            "KeySchema": [{"AttributeName": "gsiPk", "KeyType": "HASH"}],
            "Projection": {"ProjectionType": "INCLUDE", "NonKeyAttributes": ["proj"]}
        }],
        "LocalSecondaryIndexes": [{
            "IndexName": "lsi-all",
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "lsiSk", "KeyType": "RANGE"}
            ],
            "Projection": {"ProjectionType": "ALL"}
        }]
    })
}

fn indexed_table() -> Database {
    let db = Database::memory().unwrap();
    db.create_table(serde_json::from_value(table_def(TABLE)).unwrap())
        .unwrap();
    db
}

fn pad(n: usize) -> String {
    "x".repeat(n)
}

fn seed(db: &Database, item: serde_json::Value) {
    let req = serde_json::json!({"TableName": TABLE, "Item": item});
    db.put_item(serde_json::from_value(req).unwrap()).unwrap();
}

/// Run one statement and return its capacity.
fn statement(db: &Database, sql: &str) -> ConsumedCapacity {
    let req = serde_json::json!({"Statement": sql, "ReturnConsumedCapacity": "INDEXES"});
    db.execute_statement(serde_json::from_value(req).unwrap())
        .unwrap()
        .consumed_capacity
        .expect("INDEXES mode reports capacity")
}

/// Run a PartiQL transaction and return its per-table capacity entries.
fn transaction(db: &Database, sql: &[&str], mode: &str) -> Vec<ConsumedCapacity> {
    let req = serde_json::json!({
        "TransactStatements": sql.iter().map(|s| serde_json::json!({"Statement": s}))
            .collect::<Vec<_>>(),
        "ReturnConsumedCapacity": mode
    });
    db.execute_transaction(serde_json::from_value(req).unwrap())
        .unwrap()
        .consumed_capacity
        .expect("TOTAL and INDEXES report capacity")
}

fn one_transaction(db: &Database, sql: &[&str]) -> ConsumedCapacity {
    let mut entries = transaction(db, sql, "INDEXES");
    assert_eq!(entries.len(), 1, "expected one table in the response");
    entries.remove(0)
}

/// Run a batch and return its per-table capacity entries, if any.
fn batch(db: &Database, sql: &[&str], mode: &str) -> Option<Vec<ConsumedCapacity>> {
    let req = serde_json::json!({
        "Statements": sql.iter().map(|s| serde_json::json!({"Statement": s}))
            .collect::<Vec<_>>(),
        "ReturnConsumedCapacity": mode
    });
    db.batch_execute_statement(serde_json::from_value(req).unwrap())
        .unwrap()
        .consumed_capacity
}

fn one_batch(db: &Database, sql: &[&str]) -> ConsumedCapacity {
    let mut entries = batch(db, sql, "INDEXES").expect("INDEXES mode reports capacity");
    assert_eq!(entries.len(), 1, "expected one table in the response");
    entries.remove(0)
}

fn arm(map: &Option<HashMap<String, CapacityDetail>>, name: &str) -> Option<f64> {
    map.as_ref()
        .and_then(|m| m.get(name))
        .map(|d| d.capacity_units)
}

fn gsi(cc: &ConsumedCapacity) -> Option<f64> {
    arm(&cc.global_secondary_indexes, "gsi-inc")
}

fn lsi(cc: &ConsumedCapacity) -> Option<f64> {
    arm(&cc.local_secondary_indexes, "lsi-all")
}

fn table_arm(cc: &ConsumedCapacity) -> f64 {
    cc.table
        .as_ref()
        .expect("INDEXES reports a Table detail")
        .capacity_units
}

// ---------------------------------------------------------------------------
// ExecuteStatement. Capture round C, plus P.
// ---------------------------------------------------------------------------

#[test]
fn c1_insert_of_a_member_of_both_indexes_charges_both_arms() {
    // Capture C1: total 3, table 1, gsi 1, lsi 1. No transactional factor, so
    // this is the same bill as the equivalent PutItem.
    let db = indexed_table();
    let cc = statement(
        &db,
        &format!(
            "INSERT INTO \"{TABLE}\" VALUE \
             {{'pk':'c1','sk':'1','gsiPk':'g1','lsiSk':'L1','proj':'p','other':'o'}}"
        ),
    );

    assert_eq!(cc.capacity_units, 3.0);
    assert_eq!(table_arm(&cc), 1.0);
    assert_eq!(gsi(&cc), Some(1.0));
    assert_eq!(lsi(&cc), Some(1.0));
}

#[test]
fn c2_update_outside_the_gsi_projection_charges_only_the_lsi() {
    // Capture C2: total 2, table 1, lsi 1, no GSI arm.
    let db = indexed_table();
    seed(
        &db,
        serde_json::json!({
            "pk": {"S": "c2"}, "sk": {"S": "1"},
            "gsiPk": {"S": "g1"}, "lsiSk": {"S": "L1"}, "other": {"S": "o"}
        }),
    );

    let cc = statement(
        &db,
        &format!("UPDATE \"{TABLE}\" SET other='o2' WHERE pk='c2' AND sk='1'"),
    );
    assert_eq!(cc.capacity_units, 2.0);
    assert_eq!(table_arm(&cc), 1.0);
    assert_eq!(gsi(&cc), None);
    assert_eq!(lsi(&cc), Some(1.0));
}

#[test]
fn c3_moving_a_gsi_key_charges_the_index_twice() {
    // Capture C3: total 4, table 1, gsi 2, lsi 1.
    let db = indexed_table();
    seed(
        &db,
        serde_json::json!({
            "pk": {"S": "c3"}, "sk": {"S": "1"},
            "gsiPk": {"S": "g1"}, "lsiSk": {"S": "L1"}
        }),
    );

    let cc = statement(
        &db,
        &format!("UPDATE \"{TABLE}\" SET gsiPk='g2' WHERE pk='c3' AND sk='1'"),
    );
    assert_eq!(cc.capacity_units, 4.0);
    assert_eq!(table_arm(&cc), 1.0);
    assert_eq!(gsi(&cc), Some(2.0));
    assert_eq!(lsi(&cc), Some(1.0));
}

#[test]
fn c4_delete_of_an_item_in_both_indexes_charges_both_arms() {
    // Capture C4: total 3, table 1, gsi 1, lsi 1.
    let db = indexed_table();
    seed(
        &db,
        serde_json::json!({
            "pk": {"S": "c4"}, "sk": {"S": "1"},
            "gsiPk": {"S": "g1"}, "lsiSk": {"S": "L1"}
        }),
    );

    let cc = statement(
        &db,
        &format!("DELETE FROM \"{TABLE}\" WHERE pk='c4' AND sk='1'"),
    );
    assert_eq!(cc.capacity_units, 3.0);
    assert_eq!(table_arm(&cc), 1.0);
    assert_eq!(gsi(&cc), Some(1.0));
    assert_eq!(lsi(&cc), Some(1.0));
}

#[test]
fn c5_insert_is_sized_on_the_projected_entry() {
    // Capture C5: total 6, table 3, gsi 3. `proj` is projected, so it moves both
    // the base item and the index entry.
    let db = indexed_table();
    let cc = statement(
        &db,
        &format!(
            "INSERT INTO \"{}\" VALUE {{'pk':'c5','sk':'1','gsiPk':'g1','proj':'{}'}}",
            TABLE,
            pad(3000)
        ),
    );
    assert_eq!(cc.capacity_units, 6.0);
    assert_eq!(table_arm(&cc), 3.0);
    assert_eq!(gsi(&cc), Some(3.0));
}

#[test]
fn p1_a_shrinking_update_is_sized_on_the_image_it_replaced() {
    // Capture P1: total 6, table 3, gsi 3. Sizing on the after image reports 2.
    let db = indexed_table();
    seed(
        &db,
        serde_json::json!({
            "pk": {"S": "p1"}, "sk": {"S": "1"},
            "gsiPk": {"S": "g1"}, "proj": {"S": pad(3000)}
        }),
    );

    let cc = statement(
        &db,
        &format!("UPDATE \"{TABLE}\" SET proj='p' WHERE pk='p1' AND sk='1'"),
    );
    assert_eq!(cc.capacity_units, 6.0);
    assert_eq!(table_arm(&cc), 3.0);
    assert_eq!(gsi(&cc), Some(3.0));
}

#[test]
fn p3_delete_is_sized_on_the_stored_image() {
    // Capture P3: total 6, table 3, gsi 3.
    let db = indexed_table();
    seed(
        &db,
        serde_json::json!({
            "pk": {"S": "p3"}, "sk": {"S": "1"},
            "gsiPk": {"S": "g1"}, "proj": {"S": pad(3000)}
        }),
    );

    let cc = statement(
        &db,
        &format!("DELETE FROM \"{TABLE}\" WHERE pk='p3' AND sk='1'"),
    );
    assert_eq!(cc.capacity_units, 6.0);
    assert_eq!(table_arm(&cc), 3.0);
    assert_eq!(gsi(&cc), Some(3.0));
}

#[test]
fn p5_delete_of_a_missing_target_costs_the_minimum() {
    // Capture P5: total 1, table 1, no arms.
    let db = indexed_table();
    let cc = statement(
        &db,
        &format!("DELETE FROM \"{TABLE}\" WHERE pk='p5' AND sk='1'"),
    );
    assert_eq!(cc.capacity_units, 1.0);
    assert_eq!(table_arm(&cc), 1.0);
    assert!(cc.global_secondary_indexes.is_none());
}

#[test]
fn c6_reads_are_unchanged() {
    // Capture C6 and I1: a keyed SELECT costs 0.5 eventually consistent and 1
    // with ConsistentRead, with no index arms on a base table read.
    let db = indexed_table();
    seed(
        &db,
        serde_json::json!({
            "pk": {"S": "c6"}, "sk": {"S": "1"}, "gsiPk": {"S": "g1"}
        }),
    );

    let cc = statement(
        &db,
        &format!("SELECT * FROM \"{TABLE}\" WHERE pk='c6' AND sk='1'"),
    );
    assert_eq!(cc.capacity_units, 0.5);
    assert!(cc.global_secondary_indexes.is_none());

    let req = serde_json::json!({
        "Statement": format!("SELECT * FROM \"{TABLE}\" WHERE pk='c6' AND sk='1'"),
        "ConsistentRead": true,
        "ReturnConsumedCapacity": "INDEXES"
    });
    let consistent = db
        .execute_statement(serde_json::from_value(req).unwrap())
        .unwrap()
        .consumed_capacity
        .unwrap();
    assert_eq!(consistent.capacity_units, 1.0);
}

// ---------------------------------------------------------------------------
// ExecuteTransaction. Capture round D, plus Q.
// ---------------------------------------------------------------------------

#[test]
fn d1_a_transactional_insert_doubles_the_table_arm_only() {
    // Capture D1: total 4, table 2, gsi 1, lsi 1. The same statement through
    // ExecuteStatement (C1) costs total 3 with table 1, so only the table arm
    // moves.
    let db = indexed_table();
    let cc = one_transaction(
        &db,
        &[&format!(
            "INSERT INTO \"{TABLE}\" VALUE \
             {{'pk':'d1','sk':'1','gsiPk':'g1','lsiSk':'L1','proj':'p'}}"
        )],
    );

    assert_eq!(cc.capacity_units, 4.0);
    assert_eq!(table_arm(&cc), 2.0);
    assert_eq!(gsi(&cc), Some(1.0));
    assert_eq!(lsi(&cc), Some(1.0));
}

#[test]
fn d2_a_transactional_insert_is_size_accurate() {
    // Capture D2: total 9, table 6, gsi 3. A flat per-statement unit reports 2.
    let db = indexed_table();
    let cc = one_transaction(
        &db,
        &[&format!(
            "INSERT INTO \"{}\" VALUE {{'pk':'d2','sk':'1','gsiPk':'g1','proj':'{}'}}",
            TABLE,
            pad(3000)
        )],
    );

    assert_eq!(cc.capacity_units, 9.0);
    assert_eq!(table_arm(&cc), 6.0);
    assert_eq!(gsi(&cc), Some(3.0));
}

#[test]
fn q1_a_transactional_shrinking_update_holds_the_larger_image() {
    // Capture Q1: total 9, table 6, gsi 3.
    let db = indexed_table();
    seed(
        &db,
        serde_json::json!({
            "pk": {"S": "q1"}, "sk": {"S": "1"},
            "gsiPk": {"S": "g1"}, "proj": {"S": pad(3000)}
        }),
    );

    let cc = one_transaction(
        &db,
        &[&format!(
            "UPDATE \"{TABLE}\" SET proj='p' WHERE pk='q1' AND sk='1'"
        )],
    );
    assert_eq!(cc.capacity_units, 9.0);
    assert_eq!(table_arm(&cc), 6.0);
    assert_eq!(gsi(&cc), Some(3.0));
}

#[test]
fn q3_two_statements_on_one_table_sum_every_arm() {
    // Capture Q3: total 8, table 4, gsi 2, lsi 2.
    let db = indexed_table();
    let cc = one_transaction(
        &db,
        &[
            &format!(
                "INSERT INTO \"{TABLE}\" VALUE {{'pk':'q3','sk':'1','gsiPk':'g1','lsiSk':'L1'}}"
            ),
            &format!(
                "INSERT INTO \"{TABLE}\" VALUE {{'pk':'q4','sk':'1','gsiPk':'g1','lsiSk':'L1'}}"
            ),
        ],
    );

    assert_eq!(cc.capacity_units, 8.0);
    assert_eq!(table_arm(&cc), 4.0);
    assert_eq!(gsi(&cc), Some(2.0));
    assert_eq!(lsi(&cc), Some(2.0));
}

#[test]
fn q4_each_table_carries_only_its_own_arms() {
    // Capture Q4: the two-index member reports total 4, the GSI-only member 3.
    let db = indexed_table();
    db.create_table(serde_json::from_value(table_def(OTHER_TABLE)).unwrap())
        .unwrap();

    let entries = transaction(
        &db,
        &[
            &format!(
                "INSERT INTO \"{TABLE}\" VALUE {{'pk':'q5','sk':'1','gsiPk':'g1','lsiSk':'L1'}}"
            ),
            &format!("INSERT INTO \"{OTHER_TABLE}\" VALUE {{'pk':'q5','sk':'1','gsiPk':'g1'}}"),
        ],
        "INDEXES",
    );

    assert_eq!(entries.len(), 2);
    let names: Vec<&str> = entries.iter().map(|e| e.table_name.as_str()).collect();
    assert_eq!(names, vec![TABLE, OTHER_TABLE]);
    assert_eq!(entries[0].capacity_units, 4.0);
    assert_eq!(entries[1].capacity_units, 3.0);
    assert!(entries[1].local_secondary_indexes.is_none());
}

#[test]
fn d3_a_read_set_reports_read_capacity_with_no_arms() {
    // Capture D3: total 2, r=2, table 2. A read set is charged at 4KB
    // granularity, doubled, with no index breakdown.
    let db = indexed_table();
    seed(
        &db,
        serde_json::json!({"pk": {"S": "d3"}, "sk": {"S": "1"}, "gsiPk": {"S": "g1"}}),
    );

    let mut entries = transaction(
        &db,
        &[&format!(
            "SELECT * FROM \"{TABLE}\" WHERE pk='d3' AND sk='1'"
        )],
        "INDEXES",
    );
    let cc = entries.remove(0);
    assert_eq!(cc.capacity_units, 2.0);
    assert_eq!(cc.read_capacity_units, Some(2.0));
    assert!(cc.global_secondary_indexes.is_none());
}

#[test]
fn a_transactional_write_reports_the_write_axis_at_every_level() {
    // The transactional shape mirrors its units into WriteCapacityUnits
    // everywhere, including each index arm. ExecuteStatement does not.
    let db = indexed_table();
    let cc = one_transaction(
        &db,
        &[&format!(
            "INSERT INTO \"{TABLE}\" VALUE {{'pk':'w1','sk':'1','gsiPk':'g1','lsiSk':'L1'}}"
        )],
    );
    assert_eq!(cc.write_capacity_units, Some(4.0));
    assert_eq!(cc.table.as_ref().unwrap().write_capacity_units, Some(2.0));
    assert_eq!(
        cc.global_secondary_indexes
            .as_ref()
            .unwrap()
            .get("gsi-inc")
            .unwrap()
            .write_capacity_units,
        Some(1.0)
    );

    let single = statement(
        &db,
        &format!("INSERT INTO \"{TABLE}\" VALUE {{'pk':'w2','sk':'1','gsiPk':'g1'}}"),
    );
    assert_eq!(single.write_capacity_units, None);
    assert_eq!(single.table.as_ref().unwrap().write_capacity_units, None);
    assert_eq!(
        single
            .global_secondary_indexes
            .as_ref()
            .unwrap()
            .get("gsi-inc")
            .unwrap()
            .write_capacity_units,
        None
    );
}

// ---------------------------------------------------------------------------
// BatchExecuteStatement. Capture rounds E, M and R.
// ---------------------------------------------------------------------------

#[test]
fn m2_a_single_succeeding_insert_charges_its_index() {
    // Capture M2: total 2, table 1, gsi 1.
    let db = indexed_table();
    let cc = one_batch(
        &db,
        &[&format!(
            "INSERT INTO \"{TABLE}\" VALUE {{'pk':'m2','sk':'1','gsiPk':'g1'}}"
        )],
    );
    assert_eq!(cc.capacity_units, 2.0);
    assert_eq!(table_arm(&cc), 1.0);
    assert_eq!(gsi(&cc), Some(1.0));
}

#[test]
fn m3_an_insert_touching_no_index_reports_no_arms() {
    // Capture M3: total 1, table 1.
    let db = indexed_table();
    let cc = one_batch(
        &db,
        &[&format!(
            "INSERT INTO \"{TABLE}\" VALUE {{'pk':'m3','sk':'1','other':'o'}}"
        )],
    );
    assert_eq!(cc.capacity_units, 1.0);
    assert_eq!(table_arm(&cc), 1.0);
    assert!(cc.global_secondary_indexes.is_none());
}

#[test]
fn m5_two_inserts_sum_per_table() {
    // Capture M5: total 4, table 2, gsi 2.
    let db = indexed_table();
    let cc = one_batch(
        &db,
        &[
            &format!("INSERT INTO \"{TABLE}\" VALUE {{'pk':'m5','sk':'1','gsiPk':'g1'}}"),
            &format!("INSERT INTO \"{TABLE}\" VALUE {{'pk':'m6','sk':'1','gsiPk':'g1'}}"),
        ],
    );
    assert_eq!(cc.capacity_units, 4.0);
    assert_eq!(table_arm(&cc), 2.0);
    assert_eq!(gsi(&cc), Some(2.0));
}

#[test]
fn m1_a_batch_where_nothing_succeeds_reports_no_capacity() {
    // Capture M1: a lone failing statement reports no ConsumedCapacity at all,
    // even though the same failure costs a unit when something else succeeds.
    let db = indexed_table();
    seed(
        &db,
        serde_json::json!({"pk": {"S": "m1"}, "sk": {"S": "1"}, "gsiPk": {"S": "g1"}}),
    );

    let entries = batch(
        &db,
        &[&format!(
            "INSERT INTO \"{TABLE}\" VALUE {{'pk':'m1','sk':'1','gsiPk':'g1'}}"
        )],
        "INDEXES",
    );
    assert!(entries.is_none());
}

#[test]
fn m4_a_failed_statement_reaches_the_total_but_no_arm() {
    // Capture M4: one failing and one succeeding insert reports total 3 against
    // arms summing to 2. The two rows do not reconcile into a rule, so both are
    // pinned as measured rather than derived.
    let db = indexed_table();
    seed(
        &db,
        serde_json::json!({"pk": {"S": "m4"}, "sk": {"S": "1"}, "gsiPk": {"S": "g1"}}),
    );

    let cc = one_batch(
        &db,
        &[
            &format!("INSERT INTO \"{TABLE}\" VALUE {{'pk':'m4','sk':'1','gsiPk':'g1'}}"),
            &format!("INSERT INTO \"{TABLE}\" VALUE {{'pk':'m7','sk':'1','gsiPk':'g1'}}"),
        ],
    );
    assert_eq!(cc.capacity_units, 3.0);
    assert_eq!(table_arm(&cc), 1.0);
    assert_eq!(gsi(&cc), Some(1.0));
}

#[test]
fn r1_a_batch_update_is_sized_on_the_larger_image() {
    // Capture R1: total 6, table 3, gsi 3, with no transactional factor.
    let db = indexed_table();
    seed(
        &db,
        serde_json::json!({
            "pk": {"S": "r1"}, "sk": {"S": "1"},
            "gsiPk": {"S": "g1"}, "proj": {"S": pad(3000)}
        }),
    );

    let cc = one_batch(
        &db,
        &[&format!(
            "UPDATE \"{TABLE}\" SET proj='p' WHERE pk='r1' AND sk='1'"
        )],
    );
    assert_eq!(cc.capacity_units, 6.0);
    assert_eq!(table_arm(&cc), 3.0);
    assert_eq!(gsi(&cc), Some(3.0));
}

#[test]
fn r2_a_batch_delete_charges_both_arms() {
    // Capture R2: total 3, table 1, gsi 1, lsi 1.
    let db = indexed_table();
    seed(
        &db,
        serde_json::json!({
            "pk": {"S": "r2"}, "sk": {"S": "1"},
            "gsiPk": {"S": "g1"}, "lsiSk": {"S": "L1"}
        }),
    );

    let cc = one_batch(
        &db,
        &[&format!("DELETE FROM \"{TABLE}\" WHERE pk='r2' AND sk='1'")],
    );
    assert_eq!(cc.capacity_units, 3.0);
    assert_eq!(table_arm(&cc), 1.0);
    assert_eq!(gsi(&cc), Some(1.0));
    assert_eq!(lsi(&cc), Some(1.0));
}

#[test]
fn h_a_batch_across_two_tables_reports_one_entry_each() {
    // Capture H: per-table aggregation, each entry carrying only its own arms.
    let db = indexed_table();
    db.create_table(serde_json::from_value(table_def(OTHER_TABLE)).unwrap())
        .unwrap();

    let entries = batch(
        &db,
        &[
            &format!(
                "INSERT INTO \"{}\" VALUE {{'pk':'h1','sk':'1','gsiPk':'g1','lsiSk':'L1','proj':'{}'}}",
                TABLE,
                pad(3000)
            ),
            &format!("INSERT INTO \"{OTHER_TABLE}\" VALUE {{'pk':'h2','sk':'1','gsiPk':'g1'}}"),
        ],
        "INDEXES",
    )
    .expect("INDEXES mode reports capacity");

    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].capacity_units, 9.0);
    assert_eq!(table_arm(&entries[0]), 3.0);
    assert_eq!(gsi(&entries[0]), Some(3.0));
    assert_eq!(lsi(&entries[0]), Some(3.0));

    assert_eq!(entries[1].capacity_units, 2.0);
    assert_eq!(table_arm(&entries[1]), 1.0);
    assert!(entries[1].local_secondary_indexes.is_none());
}

#[test]
fn u2_a_failed_statement_is_charged_on_the_item_already_stored() {
    // Capture U2: three failing inserts alongside one success report total 7
    // against arms summing to 2. Two failures name tiny items and cost 1 each;
    // the third also names a tiny item, but the row already at that key is 3KB,
    // and it costs 3. Sizing the failure on the statement alone reports 5.
    let db = indexed_table();
    seed(
        &db,
        serde_json::json!({"pk": {"S": "u1"}, "sk": {"S": "1"}, "proj": {"S": pad(3000)}}),
    );
    seed(
        &db,
        serde_json::json!({"pk": {"S": "u3"}, "sk": {"S": "1"}}),
    );
    seed(
        &db,
        serde_json::json!({"pk": {"S": "u4"}, "sk": {"S": "1"}}),
    );

    let cc = one_batch(
        &db,
        &[
            &format!("INSERT INTO \"{TABLE}\" VALUE {{'pk':'u3','sk':'1'}}"),
            &format!("INSERT INTO \"{TABLE}\" VALUE {{'pk':'u4','sk':'1'}}"),
            &format!("INSERT INTO \"{TABLE}\" VALUE {{'pk':'u1','sk':'1'}}"),
            &format!("INSERT INTO \"{TABLE}\" VALUE {{'pk':'u5','sk':'1','gsiPk':'g1'}}"),
        ],
    );
    assert_eq!(cc.capacity_units, 7.0);
    assert_eq!(table_arm(&cc), 1.0);
    assert_eq!(gsi(&cc), Some(1.0));
}

#[test]
fn r3_a_table_whose_statements_all_failed_is_omitted() {
    // Capture R3: a batch where one table's only statement fails and another
    // table's succeeds reports just the succeeding table, with no surcharge
    // anywhere. So the failure surcharge only ever lands on a table that also
    // had a success, and a table with nothing to attach it to gets no entry.
    let db = indexed_table();
    db.create_table(serde_json::from_value(table_def(OTHER_TABLE)).unwrap())
        .unwrap();
    seed(
        &db,
        serde_json::json!({"pk": {"S": "x0"}, "sk": {"S": "1"}, "gsiPk": {"S": "g1"}}),
    );

    let entries = batch(
        &db,
        &[
            // Fails: x0 already exists on TABLE.
            &format!("INSERT INTO \"{TABLE}\" VALUE {{'pk':'x0','sk':'1','gsiPk':'g1'}}"),
            &format!("INSERT INTO \"{OTHER_TABLE}\" VALUE {{'pk':'x1','sk':'1','gsiPk':'g1'}}"),
        ],
        "INDEXES",
    )
    .expect("the succeeding table reports capacity");

    assert_eq!(entries.len(), 1, "the all-failed table is omitted entirely");
    assert_eq!(entries[0].table_name, OTHER_TABLE);
    assert_eq!(entries[0].capacity_units, 2.0);
    assert_eq!(table_arm(&entries[0]), 1.0);
    assert_eq!(gsi(&entries[0]), Some(1.0));
}

#[test]
fn a_count_projection_rejection_keeps_its_place_in_the_response() {
    // The execution loop pairs each request statement with what was parsed for
    // it. `test_count_is_rejected_per_statement_on_batch_execute` already covers
    // a rejection at the head of a batch; this one puts it in the middle, where
    // a misalignment would be visible as the error landing on a neighbour.
    //
    // Nothing can misalign them today, because the pairing advances both sides
    // together. The guard is against a later change that filters or reorders
    // what the preparation pass returns, which would go unnoticed with the
    // rejection first.
    let db = indexed_table();
    let req = serde_json::json!({
        "Statements": [
            {"Statement": format!("INSERT INTO \"{TABLE}\" VALUE {{'pk':'c1','sk':'1'}}")},
            {"Statement": format!("SELECT COUNT(*) FROM \"{TABLE}\"")},
            {"Statement": format!("INSERT INTO \"{TABLE}\" VALUE {{'pk':'c2','sk':'1'}}")}
        ]
    });
    let resp = db
        .batch_execute_statement(serde_json::from_value(req).unwrap())
        .expect("a COUNT rejection is per-statement");

    assert_eq!(resp.responses.len(), 3);
    assert!(resp.responses[0].error.is_none());
    assert_eq!(
        resp.responses[1].error.as_ref().map(|e| e.code.as_str()),
        Some("ValidationError"),
        "the rejection stays on the statement that caused it"
    );
    assert!(resp.responses[2].error.is_none());
}

#[test]
fn a_no_op_write_still_names_its_table() {
    // An `IF NOT EXISTS` duplicate writes nothing, and the entry it produces
    // must still carry the table. Building it from a defaulted record leaves
    // the name empty and surfaces a nameless entry in the response.
    let db = indexed_table();
    seed(
        &db,
        serde_json::json!({"pk": {"S": "noop"}, "sk": {"S": "1"}, "gsiPk": {"S": "g1"}}),
    );

    let cc = one_batch(
        &db,
        &[&format!(
            "INSERT INTO \"{TABLE}\" VALUE {{'pk':'noop','sk':'1'}} IF NOT EXISTS"
        )],
    );
    assert_eq!(cc.table_name, TABLE);
    assert!(cc.global_secondary_indexes.is_none());
}

#[test]
fn v2_two_keyed_selects_on_one_item_are_rejected() {
    // Capture V2: a read batch naming one item twice is rejected, the same way
    // a write batch is. A SELECT that pins every key attribute resolves to a
    // target; one spanning a partition does not.
    let db = indexed_table();
    seed(
        &db,
        serde_json::json!({"pk": {"S": "v1"}, "sk": {"S": "1"}}),
    );

    let req = serde_json::json!({
        "Statements": [
            {"Statement": format!("SELECT * FROM \"{TABLE}\" WHERE pk='v1' AND sk='1'")},
            {"Statement": format!("SELECT * FROM \"{TABLE}\" WHERE pk='v1' AND sk='1'")}
        ],
        "ReturnConsumedCapacity": "INDEXES"
    });
    let err = db
        .batch_execute_statement(serde_json::from_value(req).unwrap())
        .expect_err("two reads of one item are rejected");
    assert!(
        err.to_string()
            .contains("Provided list of item keys contains duplicates"),
        "unexpected error: {err}"
    );
}

#[test]
fn v1_two_keyed_selects_on_distinct_items_are_charged_as_reads() {
    // Capture V1: two keyed reads at 0.5 each, summed per table, no index arms.
    let db = indexed_table();
    seed(
        &db,
        serde_json::json!({"pk": {"S": "v1"}, "sk": {"S": "1"}}),
    );
    seed(
        &db,
        serde_json::json!({"pk": {"S": "v2"}, "sk": {"S": "1"}}),
    );

    let cc = one_batch(
        &db,
        &[
            &format!("SELECT * FROM \"{TABLE}\" WHERE pk='v1' AND sk='1'"),
            &format!("SELECT * FROM \"{TABLE}\" WHERE pk='v2' AND sk='1'"),
        ],
    );
    assert_eq!(cc.capacity_units, 1.0);
    assert!(cc.global_secondary_indexes.is_none());
}

#[test]
fn w1_key_values_carrying_the_delimiter_are_not_confused() {
    // Capture W1: AWS accepts these as two distinct items. Joining table, pk
    // and sk on a delimiter is not injective once a key value contains it, so
    // a naive dedup key rejects a batch that should be written.
    let db = indexed_table();
    let cc = one_batch(
        &db,
        &[
            &format!("INSERT INTO \"{TABLE}\" VALUE {{'pk':'a#S:b','sk':'c'}}"),
            &format!("INSERT INTO \"{TABLE}\" VALUE {{'pk':'a','sk':'b#S:c'}}"),
        ],
    );
    assert_eq!(cc.capacity_units, 2.0);
    assert_eq!(table_arm(&cc), 2.0);
}

#[test]
fn a_batch_reports_nothing_without_a_mode() {
    let db = indexed_table();
    let req = serde_json::json!({
        "Statements": [{
            "Statement": format!(
                "INSERT INTO \"{TABLE}\" VALUE {{'pk':'n1','sk':'1','gsiPk':'g1'}}"
            )
        }]
    });
    let response = db
        .batch_execute_statement(serde_json::from_value(req).unwrap())
        .unwrap();
    assert!(response.consumed_capacity.is_none());
}

#[test]
fn total_mode_folds_the_arms_in_without_a_breakdown() {
    let db = indexed_table();
    let mut entries = batch(
        &db,
        &[&format!(
            "INSERT INTO \"{TABLE}\" VALUE {{'pk':'t1','sk':'1','gsiPk':'g1','lsiSk':'L1'}}"
        )],
        "TOTAL",
    )
    .expect("TOTAL reports capacity");

    let cc = entries.remove(0);
    assert_eq!(cc.capacity_units, 3.0);
    assert!(cc.table.is_none());
    assert!(cc.global_secondary_indexes.is_none());
    assert!(cc.local_secondary_indexes.is_none());
}
