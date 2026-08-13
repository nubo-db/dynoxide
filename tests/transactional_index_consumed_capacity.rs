//! Per-index `ConsumedCapacity` on transactional writes.
//!
//! Every figure asserted here was measured against real DynamoDB in eu-west-2 on
//! 13 August 2026, against a table keyed `pk`/`sk` with a GSI `gsi-inc` on
//! `gsiPk` projecting `INCLUDE [proj]` and an LSI `lsi-all` on `pk`/`lsiSk`
//! projecting `ALL`. Case labels (A1, B7, K3) refer to that capture.
//!
//! The rule the capture settled, and the one these tests exist to hold: the
//! transactional 2x factor applies to the base table arm alone. Index arms
//! inside a transaction cost what the same write costs outside one. A GSI key
//! move at 1517B per side charges the index 4 while the table arm on that same
//! write doubles from 2 to 4, and the two fours have different arithmetic
//! behind them.

use dynoxide::Database;
use dynoxide::types::{CapacityDetail, ConsumedCapacity};
use std::collections::HashMap;

const TABLE: &str = "tx_idx_wcu";
const OTHER_TABLE: &str = "tx_idx_wcu_b";

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

/// PutItem without asking for capacity, for setting a case up.
fn seed(db: &Database, table: &str, item: serde_json::Value) {
    let req = serde_json::json!({"TableName": table, "Item": item});
    db.put_item(serde_json::from_value(req).unwrap()).unwrap();
}

/// Run a transaction and return its per-table capacity entries.
fn tx(db: &Database, actions: serde_json::Value, mode: &str) -> Vec<ConsumedCapacity> {
    let req = serde_json::json!({
        "TransactItems": actions,
        "ReturnConsumedCapacity": mode
    });
    db.transact_write_items(serde_json::from_value(req).unwrap())
        .unwrap()
        .consumed_capacity
        .expect("TOTAL and INDEXES report capacity")
}

/// Run a single-table transaction and return that table's entry.
fn one(db: &Database, actions: serde_json::Value) -> ConsumedCapacity {
    let mut entries = tx(db, actions, "INDEXES");
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

fn put_action(table: &str, item: serde_json::Value) -> serde_json::Value {
    serde_json::json!({"Put": {"TableName": table, "Item": item}})
}

// ---------------------------------------------------------------------------
// Structure, all sub-1KB. Capture round A.
// ---------------------------------------------------------------------------

#[test]
fn a1_put_of_a_gsi_member_charges_the_index_undoubled() {
    // Capture A1: total 3, table 2, gsi 1. The single-item equivalent is
    // total 2, table 1, gsi 1, so the table arm doubles and the index arm
    // does not. This is the assertion the whole change rests on.
    let db = indexed_table();
    let cc = one(
        &db,
        serde_json::json!([put_action(
            TABLE,
            serde_json::json!({"pk": {"S": "a1"}, "sk": {"S": "1"}, "gsiPk": {"S": "g1"}})
        )]),
    );

    assert_eq!(cc.capacity_units, 3.0);
    assert_eq!(table_arm(&cc), 2.0);
    assert_eq!(gsi(&cc), Some(1.0));
    assert_eq!(lsi(&cc), None);
}

#[test]
fn a2_put_of_a_member_of_neither_index_reports_no_arms() {
    // Capture A2: total 2, table 2, no arms at all.
    let db = indexed_table();
    let cc = one(
        &db,
        serde_json::json!([put_action(
            TABLE,
            serde_json::json!({"pk": {"S": "a2"}, "sk": {"S": "1"}, "other": {"S": "o"}})
        )]),
    );

    assert_eq!(cc.capacity_units, 2.0);
    assert_eq!(table_arm(&cc), 2.0);
    assert!(cc.global_secondary_indexes.is_none());
    assert!(cc.local_secondary_indexes.is_none());
}

#[test]
fn a3_put_of_a_member_of_both_indexes_charges_both_arms() {
    // Capture A3: total 4, table 2, gsi 1, lsi 1.
    let db = indexed_table();
    let cc = one(
        &db,
        serde_json::json!([put_action(
            TABLE,
            serde_json::json!({
                "pk": {"S": "a3"}, "sk": {"S": "1"},
                "gsiPk": {"S": "g1"}, "lsiSk": {"S": "L1"}, "proj": {"S": "p"}
            })
        )]),
    );

    assert_eq!(cc.capacity_units, 4.0);
    assert_eq!(table_arm(&cc), 2.0);
    assert_eq!(gsi(&cc), Some(1.0));
    assert_eq!(lsi(&cc), Some(1.0));
}

#[test]
fn a4_identical_overwrite_charges_the_table_and_no_index() {
    // Capture A4: total 2, table 2, no arms. The write still costs its table
    // arm; only the untouched index views go free.
    let db = indexed_table();
    let item = serde_json::json!({
        "pk": {"S": "a4"}, "sk": {"S": "1"},
        "gsiPk": {"S": "g1"}, "lsiSk": {"S": "L1"}
    });
    seed(&db, TABLE, item.clone());

    let cc = one(&db, serde_json::json!([put_action(TABLE, item)]));
    assert_eq!(cc.capacity_units, 2.0);
    assert_eq!(table_arm(&cc), 2.0);
    assert_eq!(gsi(&cc), None);
    assert_eq!(lsi(&cc), None);
}

#[test]
fn a5_update_outside_the_gsi_projection_charges_only_the_lsi() {
    // Capture A5: total 3, table 2, lsi 1, no GSI arm. `other` is outside the
    // GSI's INCLUDE list but inside the LSI's ALL projection.
    let db = indexed_table();
    seed(
        &db,
        TABLE,
        serde_json::json!({
            "pk": {"S": "a5"}, "sk": {"S": "1"},
            "gsiPk": {"S": "g1"}, "lsiSk": {"S": "L1"}, "other": {"S": "o"}
        }),
    );

    let cc = one(
        &db,
        serde_json::json!([{"Update": {
            "TableName": TABLE,
            "Key": {"pk": {"S": "a5"}, "sk": {"S": "1"}},
            "UpdateExpression": "SET #o = :v",
            "ExpressionAttributeNames": {"#o": "other"},
            "ExpressionAttributeValues": {":v": {"S": "o2"}}
        }}]),
    );

    assert_eq!(cc.capacity_units, 3.0);
    assert_eq!(table_arm(&cc), 2.0);
    assert_eq!(gsi(&cc), None);
    assert_eq!(lsi(&cc), Some(1.0));
}

#[test]
fn a7_moving_a_gsi_key_charges_the_index_twice_undoubled() {
    // Capture A7: total 5, table 2, gsi 2, lsi 1. A move is a delete plus an
    // insert; the transactional factor does not touch either half.
    let db = indexed_table();
    seed(
        &db,
        TABLE,
        serde_json::json!({
            "pk": {"S": "a7"}, "sk": {"S": "1"},
            "gsiPk": {"S": "g1"}, "lsiSk": {"S": "L1"}
        }),
    );

    let cc = one(
        &db,
        serde_json::json!([{"Update": {
            "TableName": TABLE,
            "Key": {"pk": {"S": "a7"}, "sk": {"S": "1"}},
            "UpdateExpression": "SET gsiPk = :v",
            "ExpressionAttributeValues": {":v": {"S": "g2"}}
        }}]),
    );

    assert_eq!(cc.capacity_units, 5.0);
    assert_eq!(table_arm(&cc), 2.0);
    assert_eq!(gsi(&cc), Some(2.0));
    assert_eq!(lsi(&cc), Some(1.0));
}

#[test]
fn a10_delete_of_an_item_in_both_indexes_charges_both_arms() {
    // Capture A10: total 4, table 2, gsi 1, lsi 1.
    let db = indexed_table();
    seed(
        &db,
        TABLE,
        serde_json::json!({
            "pk": {"S": "a10"}, "sk": {"S": "1"},
            "gsiPk": {"S": "g1"}, "lsiSk": {"S": "L1"}
        }),
    );

    let cc = one(
        &db,
        serde_json::json!([{"Delete": {
            "TableName": TABLE,
            "Key": {"pk": {"S": "a10"}, "sk": {"S": "1"}}
        }}]),
    );

    assert_eq!(cc.capacity_units, 4.0);
    assert_eq!(table_arm(&cc), 2.0);
    assert_eq!(gsi(&cc), Some(1.0));
    assert_eq!(lsi(&cc), Some(1.0));
}

#[test]
fn a12_condition_check_charges_the_table_and_no_index() {
    // Capture A12: total 2, table 2, no arms. A check touches no index view.
    let db = indexed_table();
    seed(
        &db,
        TABLE,
        serde_json::json!({
            "pk": {"S": "a12"}, "sk": {"S": "1"},
            "gsiPk": {"S": "g1"}, "lsiSk": {"S": "L1"}
        }),
    );

    let cc = one(
        &db,
        serde_json::json!([{"ConditionCheck": {
            "TableName": TABLE,
            "Key": {"pk": {"S": "a12"}, "sk": {"S": "1"}},
            "ConditionExpression": "attribute_exists(pk)"
        }}]),
    );

    assert_eq!(cc.capacity_units, 2.0);
    assert_eq!(table_arm(&cc), 2.0);
    assert!(cc.global_secondary_indexes.is_none());
    assert!(cc.local_secondary_indexes.is_none());
}

#[test]
fn a13_two_actions_on_one_table_sum_every_arm() {
    // Capture A13: total 8, table 4, gsi 2, lsi 2.
    let db = indexed_table();
    let cc = one(
        &db,
        serde_json::json!([
            put_action(
                TABLE,
                serde_json::json!({
                    "pk": {"S": "a13"}, "sk": {"S": "1"},
                    "gsiPk": {"S": "g1"}, "lsiSk": {"S": "L1"}
                })
            ),
            put_action(
                TABLE,
                serde_json::json!({
                    "pk": {"S": "a14"}, "sk": {"S": "1"},
                    "gsiPk": {"S": "g1"}, "lsiSk": {"S": "L1"}
                })
            )
        ]),
    );

    assert_eq!(cc.capacity_units, 8.0);
    assert_eq!(table_arm(&cc), 4.0);
    assert_eq!(gsi(&cc), Some(2.0));
    assert_eq!(lsi(&cc), Some(2.0));
}

// ---------------------------------------------------------------------------
// Sizing past 1KB. Capture round B, plus F and J.
// ---------------------------------------------------------------------------

#[test]
fn b5_delete_is_sized_on_the_stored_image_not_the_key() {
    // Capture B5: total 9, table 6, gsi 3. Sizing the delete on its request
    // payload, which carries only the key, reports total 2.
    let db = indexed_table();
    seed(
        &db,
        TABLE,
        serde_json::json!({
            "pk": {"S": "b5"}, "sk": {"S": "1"},
            "gsiPk": {"S": "g1"}, "proj": {"S": pad(3000)}
        }),
    );

    let cc = one(
        &db,
        serde_json::json!([{"Delete": {
            "TableName": TABLE,
            "Key": {"pk": {"S": "b5"}, "sk": {"S": "1"}}
        }}]),
    );

    assert_eq!(table_arm(&cc), 6.0);
    assert_eq!(gsi(&cc), Some(3.0));
    assert_eq!(cc.capacity_units, 9.0);
}

#[test]
fn b3_a_shrinking_put_is_sized_on_the_image_it_replaced() {
    // Capture B3: total 6, table 6. Sizing on the request item alone reports 2.
    let db = indexed_table();
    seed(
        &db,
        TABLE,
        serde_json::json!({"pk": {"S": "b3"}, "sk": {"S": "1"}, "other": {"S": pad(3000)}}),
    );

    let cc = one(
        &db,
        serde_json::json!([put_action(
            TABLE,
            serde_json::json!({"pk": {"S": "b3"}, "sk": {"S": "1"}})
        )]),
    );

    assert_eq!(cc.capacity_units, 6.0);
    assert_eq!(table_arm(&cc), 6.0);
}

#[test]
fn b7_a_key_move_doubles_its_table_arm_and_leaves_its_index_arm_alone() {
    // Capture B7: total 8, table 4, gsi 4. Both arms read 4 and neither is the
    // other's arithmetic: the table is 2 x ceil(1.5KB) and the GSI is
    // ceil(1517) + ceil(1517), undoubled. Doubling the index arm reports 12.
    let db = indexed_table();
    seed(
        &db,
        TABLE,
        serde_json::json!({
            "pk": {"S": "b7"}, "sk": {"S": "1"},
            "gsiPk": {"S": "g1"}, "proj": {"S": pad(1500)}
        }),
    );

    let cc = one(
        &db,
        serde_json::json!([{"Update": {
            "TableName": TABLE,
            "Key": {"pk": {"S": "b7"}, "sk": {"S": "1"}},
            "UpdateExpression": "SET gsiPk = :v",
            "ExpressionAttributeValues": {":v": {"S": "g9"}}
        }}]),
    );

    assert_eq!(cc.capacity_units, 8.0);
    assert_eq!(table_arm(&cc), 4.0);
    assert_eq!(gsi(&cc), Some(4.0));
}

#[test]
fn j3_an_update_shrinking_an_entry_holds_the_larger_image() {
    // Capture J3: total 9, table 6, gsi 3. Both arms hold the pre-image.
    let db = indexed_table();
    seed(
        &db,
        TABLE,
        serde_json::json!({
            "pk": {"S": "j3"}, "sk": {"S": "1"},
            "gsiPk": {"S": "g1"}, "proj": {"S": pad(3000)}
        }),
    );

    let cc = one(
        &db,
        serde_json::json!([{"Update": {
            "TableName": TABLE,
            "Key": {"pk": {"S": "j3"}, "sk": {"S": "1"}},
            "UpdateExpression": "SET proj = :v",
            "ExpressionAttributeValues": {":v": {"S": "p"}}
        }}]),
    );

    assert_eq!(cc.capacity_units, 9.0);
    assert_eq!(table_arm(&cc), 6.0);
    assert_eq!(gsi(&cc), Some(3.0));
}

#[test]
fn f1_condition_check_is_sized_on_the_image_it_reads() {
    // Capture F1: total 6, table 6. A check writes nothing and is still charged
    // on the item it looked at. Sizing on the key reports 2.
    let db = indexed_table();
    seed(
        &db,
        TABLE,
        serde_json::json!({"pk": {"S": "f1"}, "sk": {"S": "1"}, "other": {"S": pad(3000)}}),
    );

    let cc = one(
        &db,
        serde_json::json!([{"ConditionCheck": {
            "TableName": TABLE,
            "Key": {"pk": {"S": "f1"}, "sk": {"S": "1"}},
            "ConditionExpression": "attribute_exists(pk)"
        }}]),
    );

    assert_eq!(cc.capacity_units, 6.0);
    assert_eq!(table_arm(&cc), 6.0);
}

#[test]
fn f4_an_update_that_creates_the_item_charges_each_index_once() {
    // Capture F4: total 4, table 2, gsi 1, lsi 1. The update injects the key
    // attributes for the upsert, so charging against the assembled item rather
    // than the stored one would read as a key move and report gsi 2.
    let db = indexed_table();
    let cc = one(
        &db,
        serde_json::json!([{"Update": {
            "TableName": TABLE,
            "Key": {"pk": {"S": "f4"}, "sk": {"S": "1"}},
            "UpdateExpression": "SET gsiPk = :g, lsiSk = :l",
            "ExpressionAttributeValues": {":g": {"S": "g1"}, ":l": {"S": "L1"}}
        }}]),
    );

    assert_eq!(cc.capacity_units, 4.0);
    assert_eq!(table_arm(&cc), 2.0);
    assert_eq!(gsi(&cc), Some(1.0));
    assert_eq!(lsi(&cc), Some(1.0));
}

// ---------------------------------------------------------------------------
// Missing targets and no-ops. Capture round K.
// ---------------------------------------------------------------------------

#[test]
fn k1_delete_of_a_missing_target_costs_the_minimum() {
    // Capture K1: total 2, table 2, no arms.
    let db = indexed_table();
    let cc = one(
        &db,
        serde_json::json!([{"Delete": {
            "TableName": TABLE,
            "Key": {"pk": {"S": "k1"}, "sk": {"S": "1"}}
        }}]),
    );

    assert_eq!(cc.capacity_units, 2.0);
    assert_eq!(table_arm(&cc), 2.0);
    assert!(cc.global_secondary_indexes.is_none());
}

#[test]
fn k3_identical_overwrite_of_a_large_item_charges_the_table_alone() {
    // Capture K3: total 6, table 6, no arms. The table arm and the index arms
    // are independent readings of the same write.
    let db = indexed_table();
    let item = serde_json::json!({
        "pk": {"S": "k3"}, "sk": {"S": "1"},
        "gsiPk": {"S": "g1"}, "proj": {"S": pad(3000)}
    });
    seed(&db, TABLE, item.clone());

    let cc = one(&db, serde_json::json!([put_action(TABLE, item)]));
    assert_eq!(cc.capacity_units, 6.0);
    assert_eq!(table_arm(&cc), 6.0);
    assert_eq!(gsi(&cc), None);
}

// ---------------------------------------------------------------------------
// Multi-table, modes, and the cancellation path.
// ---------------------------------------------------------------------------

#[test]
fn g1_each_table_carries_only_its_own_arms() {
    // Capture G1: the indexed member reports total 4 with both arms, the
    // GSI-only member total 3 with one. Entries are sorted by table name so the
    // response order does not shift between calls.
    let db = indexed_table();
    db.create_table(serde_json::from_value(table_def(OTHER_TABLE)).unwrap())
        .unwrap();

    let entries = tx(
        &db,
        serde_json::json!([
            put_action(
                TABLE,
                serde_json::json!({
                    "pk": {"S": "g1"}, "sk": {"S": "1"},
                    "gsiPk": {"S": "g1"}, "lsiSk": {"S": "L1"}
                })
            ),
            put_action(
                OTHER_TABLE,
                serde_json::json!({
                    "pk": {"S": "g1"}, "sk": {"S": "1"}, "gsiPk": {"S": "g1"}
                })
            )
        ]),
        "INDEXES",
    );

    assert_eq!(entries.len(), 2);
    let names: Vec<&str> = entries.iter().map(|e| e.table_name.as_str()).collect();
    assert_eq!(names, vec![TABLE, OTHER_TABLE]);

    let first = &entries[0];
    assert_eq!(first.capacity_units, 4.0);
    assert_eq!(gsi(first), Some(1.0));
    assert_eq!(lsi(first), Some(1.0));

    let second = &entries[1];
    assert_eq!(second.capacity_units, 3.0);
    assert_eq!(gsi(second), Some(1.0));
    assert!(second.local_secondary_indexes.is_none());
}

#[test]
fn total_mode_folds_the_index_arms_in_and_reports_no_breakdown() {
    // Capture A3 under TOTAL: the same total, with no Table detail and no arms.
    let db = indexed_table();
    let mut entries = tx(
        &db,
        serde_json::json!([put_action(
            TABLE,
            serde_json::json!({
                "pk": {"S": "t1"}, "sk": {"S": "1"},
                "gsiPk": {"S": "g1"}, "lsiSk": {"S": "L1"}
            })
        )]),
        "TOTAL",
    );

    let cc = entries.remove(0);
    assert_eq!(cc.capacity_units, 4.0);
    assert_eq!(cc.write_capacity_units, Some(4.0));
    assert!(cc.table.is_none());
    assert!(cc.global_secondary_indexes.is_none());
    assert!(cc.local_secondary_indexes.is_none());
}

#[test]
fn the_write_axis_is_reported_at_every_level() {
    // The transactional shape mirrors its units into WriteCapacityUnits at the
    // top level, on the Table detail, and on each index arm. The single-item
    // shape reports CapacityUnits alone, which is why the two builders differ.
    let db = indexed_table();
    let cc = one(
        &db,
        serde_json::json!([put_action(
            TABLE,
            serde_json::json!({
                "pk": {"S": "w1"}, "sk": {"S": "1"},
                "gsiPk": {"S": "g1"}, "lsiSk": {"S": "L1"}
            })
        )]),
    );

    assert_eq!(cc.write_capacity_units, Some(4.0));
    assert_eq!(
        cc.table.as_ref().unwrap().write_capacity_units,
        Some(2.0),
        "the Table detail carries the write axis"
    );
    let gsi_detail = cc.global_secondary_indexes.as_ref().unwrap();
    assert_eq!(
        gsi_detail.get("gsi-inc").unwrap().write_capacity_units,
        Some(1.0),
        "each index arm carries the write axis too"
    );
}

#[test]
fn no_capacity_is_reported_without_a_mode() {
    let db = indexed_table();
    let req = serde_json::json!({
        "TransactItems": [put_action(
            TABLE,
            serde_json::json!({"pk": {"S": "n1"}, "sk": {"S": "1"}, "gsiPk": {"S": "g1"}})
        )]
    });
    let response = db
        .transact_write_items(serde_json::from_value(req).unwrap())
        .unwrap();
    assert!(response.consumed_capacity.is_none());
}

// ---------------------------------------------------------------------------
// Same-token replay. Captured 13 August 2026, round S.
// ---------------------------------------------------------------------------

/// Run the same tokened transaction twice and return both capacity totals.
fn first_and_replay(db: &Database, actions: serde_json::Value, token: &str) -> (f64, f64) {
    let req = serde_json::json!({
        "TransactItems": actions,
        "ReturnConsumedCapacity": "TOTAL",
        "ClientRequestToken": token
    });
    let total = |r: dynoxide::actions::transact_write_items::TransactWriteItemsResponse| {
        r.consumed_capacity
            .expect("TOTAL reports capacity")
            .iter()
            .map(|c| c.capacity_units)
            .sum::<f64>()
    };
    let first = total(
        db.transact_write_items(serde_json::from_value(req.clone()).unwrap())
            .unwrap(),
    );
    let replay = total(
        db.transact_write_items(serde_json::from_value(req).unwrap())
            .unwrap(),
    );
    (first, replay)
}

#[test]
fn s1_a_replayed_shrinking_put_is_sized_on_the_image_it_replaced() {
    // Capture S1: a put shrinking a ~9KB item reports 18 on the first call and
    // 6 on the replay. Sizing the replay on the after image alone reports 2, so
    // this row is what rules that reading out.
    let db = indexed_table();
    seed(
        &db,
        TABLE,
        serde_json::json!({"pk": {"S": "s1"}, "sk": {"S": "1"}, "proj": {"S": pad(9000)}}),
    );

    let (first, replay) = first_and_replay(
        &db,
        serde_json::json!([put_action(
            TABLE,
            serde_json::json!({"pk": {"S": "s1"}, "sk": {"S": "1"}})
        )]),
        "replay-shrink",
    );
    assert_eq!(first, 18.0);
    assert_eq!(replay, 6.0);
}

#[test]
fn s2_a_replayed_growing_put_is_sized_on_the_image_it_wrote() {
    // Capture S2: the mirror of S1, ruling out sizing on the before image
    // alone, which would report 2.
    let db = indexed_table();
    seed(
        &db,
        TABLE,
        serde_json::json!({"pk": {"S": "s2"}, "sk": {"S": "1"}, "other": {"S": "o"}}),
    );

    let (first, replay) = first_and_replay(
        &db,
        serde_json::json!([put_action(
            TABLE,
            serde_json::json!({"pk": {"S": "s2"}, "sk": {"S": "1"}, "proj": {"S": pad(9000)}})
        )]),
        "replay-grow",
    );
    assert_eq!(first, 18.0);
    assert_eq!(replay, 6.0);
}

#[test]
fn r1_a_replayed_delete_is_sized_on_the_stored_image_not_the_key() {
    // Capture R1: a delete of a ~9KB item reports 18 on the first call and 6 on
    // the replay. The request carries only the key, so sizing the replay from
    // the request reports 2.
    let db = indexed_table();
    seed(
        &db,
        TABLE,
        serde_json::json!({"pk": {"S": "r1"}, "sk": {"S": "1"}, "proj": {"S": pad(9000)}}),
    );

    let (first, replay) = first_and_replay(
        &db,
        serde_json::json!([{"Delete": {
            "TableName": TABLE,
            "Key": {"pk": {"S": "r1"}, "sk": {"S": "1"}}
        }}]),
        "replay-delete",
    );
    assert_eq!(first, 18.0);
    assert_eq!(replay, 6.0);
}

#[test]
fn r2_a_replayed_condition_check_is_sized_on_the_image_it_read() {
    // Capture R2: same figures as the delete, against an action that writes
    // nothing at all.
    let db = indexed_table();
    seed(
        &db,
        TABLE,
        serde_json::json!({"pk": {"S": "r2"}, "sk": {"S": "1"}, "proj": {"S": pad(9000)}}),
    );

    let (first, replay) = first_and_replay(
        &db,
        serde_json::json!([{"ConditionCheck": {
            "TableName": TABLE,
            "Key": {"pk": {"S": "r2"}, "sk": {"S": "1"}},
            "ConditionExpression": "attribute_exists(pk)"
        }}]),
        "replay-check",
    );
    assert_eq!(first, 18.0);
    assert_eq!(replay, 6.0);
}

#[test]
fn s3_a_replay_sums_its_actions_before_the_transactional_factor() {
    // Capture S3: a ~9KB delete alongside a small put reports 20 on the first
    // call and 8 on the replay, which is 6 + 2 rather than one rounding of the
    // summed bytes.
    let db = indexed_table();
    seed(
        &db,
        TABLE,
        serde_json::json!({"pk": {"S": "s3"}, "sk": {"S": "1"}, "proj": {"S": pad(9000)}}),
    );

    let (first, replay) = first_and_replay(
        &db,
        serde_json::json!([
            {"Delete": {"TableName": TABLE, "Key": {"pk": {"S": "s3"}, "sk": {"S": "1"}}}},
            put_action(
                TABLE,
                serde_json::json!({"pk": {"S": "s4"}, "sk": {"S": "1"}, "other": {"S": "o"}})
            )
        ]),
        "replay-pair",
    );
    assert_eq!(first, 20.0);
    assert_eq!(replay, 8.0);
}

#[test]
fn a_cancelled_transaction_reports_no_capacity_at_all() {
    // The per-action records accumulate inside the transaction and are dropped
    // with the error, so a cancellation carries no partial bill.
    let db = indexed_table();
    let req = serde_json::json!({
        "TransactItems": [
            put_action(
                TABLE,
                serde_json::json!({
                    "pk": {"S": "c1"}, "sk": {"S": "1"}, "gsiPk": {"S": "g1"}
                })
            ),
            {"ConditionCheck": {
                "TableName": TABLE,
                "Key": {"pk": {"S": "missing"}, "sk": {"S": "1"}},
                "ConditionExpression": "attribute_exists(pk)"
            }}
        ],
        "ReturnConsumedCapacity": "INDEXES"
    });

    let err = db
        .transact_write_items(serde_json::from_value(req).unwrap())
        .expect_err("the condition check fails, cancelling the transaction");
    assert!(
        err.to_string().contains("Transaction cancelled"),
        "unexpected error: {err}"
    );

    // The failed transaction rolled back, so the put left nothing behind.
    let get = serde_json::json!({
        "TableName": TABLE,
        "Key": {"pk": {"S": "c1"}, "sk": {"S": "1"}}
    });
    let found = db
        .get_item(serde_json::from_value(get).unwrap())
        .unwrap()
        .item;
    assert!(found.is_none(), "a cancelled transaction writes nothing");
}
