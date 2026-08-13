//! Per-index `ConsumedCapacity` on write operations.
//!
//! Every figure asserted here was measured against real DynamoDB in eu-west-2.
//! The twelve `#176` rows come from the bug report's capture on 12 August 2026,
//! against a table keyed `pk`/`sk` with a GSI `gsi-inc` on `gsiPk` projecting
//! `INCLUDE [proj]` and an LSI `lsi1` on `pk`/`lsiSk` projecting `ALL`.
//!
//! The sizing rows come from a follow-up capture on 13 August 2026 that pushed
//! index entries past the 1KB boundary, against the same table shape but with the
//! LSI projecting `INCLUDE [proj]` so `proj` moves both index entries and `other`
//! moves the base item alone. Case labels (S1, R3, L2) refer to that capture.

use dynoxide::Database;
use dynoxide::types::ConsumedCapacity;
use std::collections::HashMap;

const REPORT_TABLE: &str = "idx_wcu";
const SIZING_TABLE: &str = "idx_sizing";

/// The table from the `#176` report: GSI projecting `INCLUDE [proj]`, LSI
/// projecting `ALL`.
fn report_table() -> Database {
    let db = Database::memory().unwrap();
    let req = serde_json::json!({
        "TableName": REPORT_TABLE,
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
            "IndexName": "lsi1",
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "lsiSk", "KeyType": "RANGE"}
            ],
            "Projection": {"ProjectionType": "ALL"}
        }]
    });
    db.create_table(serde_json::from_value(req).unwrap())
        .unwrap();
    db
}

/// The sizing capture's table: both indexes project `INCLUDE [proj]`, so `proj`
/// is the size lever for index entries and `other` for the base item alone.
fn sizing_table() -> Database {
    let db = Database::memory().unwrap();
    let req = serde_json::json!({
        "TableName": SIZING_TABLE,
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
            "IndexName": "lsi-inc",
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "lsiSk", "KeyType": "RANGE"}
            ],
            "Projection": {"ProjectionType": "INCLUDE", "NonKeyAttributes": ["proj"]}
        }]
    });
    db.create_table(serde_json::from_value(req).unwrap())
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

fn put(db: &Database, table: &str, item: serde_json::Value) -> ConsumedCapacity {
    let req = serde_json::json!({
        "TableName": table,
        "Item": item,
        "ReturnConsumedCapacity": "INDEXES"
    });
    db.put_item(serde_json::from_value(req).unwrap())
        .unwrap()
        .consumed_capacity
        .expect("INDEXES mode reports capacity")
}

fn update(
    db: &Database,
    table: &str,
    key: serde_json::Value,
    mut req: serde_json::Value,
) -> ConsumedCapacity {
    req["TableName"] = serde_json::json!(table);
    req["Key"] = key;
    req["ReturnConsumedCapacity"] = serde_json::json!("INDEXES");
    db.update_item(serde_json::from_value(req).unwrap())
        .unwrap()
        .consumed_capacity
        .expect("INDEXES mode reports capacity")
}

fn delete(db: &Database, table: &str, key: serde_json::Value) -> ConsumedCapacity {
    let req = serde_json::json!({
        "TableName": table,
        "Key": key,
        "ReturnConsumedCapacity": "INDEXES"
    });
    db.delete_item(serde_json::from_value(req).unwrap())
        .unwrap()
        .consumed_capacity
        .expect("INDEXES mode reports capacity")
}

fn arm(map: &Option<HashMap<String, dynoxide::types::CapacityDetail>>, name: &str) -> Option<f64> {
    map.as_ref()
        .and_then(|m| m.get(name))
        .map(|d| d.capacity_units)
}

fn gsi(cc: &ConsumedCapacity) -> Option<f64> {
    arm(&cc.global_secondary_indexes, "gsi-inc")
}

fn lsi(cc: &ConsumedCapacity, name: &str) -> Option<f64> {
    arm(&cc.local_secondary_indexes, name)
}

fn table_units(cc: &ConsumedCapacity) -> Option<f64> {
    cc.table.as_ref().map(|d| d.capacity_units)
}

/// Assert the whole shape at once: total, table arm, GSI arm, LSI arm. `None`
/// means the arm must be absent rather than present and zeroed, which is what
/// DynamoDB does for an index a write leaves untouched.
fn assert_capacity(
    cc: &ConsumedCapacity,
    total: f64,
    table: f64,
    gsi_units: Option<f64>,
    lsi_units: Option<f64>,
    lsi_name: &str,
) {
    assert_eq!(cc.capacity_units, total, "total capacity");
    assert_eq!(table_units(cc), Some(table), "table arm");
    assert_eq!(gsi(cc), gsi_units, "gsi arm");
    assert_eq!(lsi(cc, lsi_name), lsi_units, "lsi arm");
    if gsi_units.is_none() {
        assert!(
            cc.global_secondary_indexes.is_none(),
            "an untouched GSI reports no map at all"
        );
    }
    if lsi_units.is_none() {
        assert!(
            cc.local_secondary_indexes.is_none(),
            "an untouched LSI reports no map at all"
        );
    }
}

// ---------------------------------------------------------------------------
// The twelve rows from the #176 capture.
// ---------------------------------------------------------------------------

#[test]
fn put_item_carrying_only_the_gsi_key() {
    // Real DynamoDB: total 2, table 1, gsi 1.
    let db = report_table();
    let cc = put(
        &db,
        REPORT_TABLE,
        serde_json::json!({"pk": {"S": "a"}, "sk": {"S": "1"}, "gsiPk": {"S": "g"}}),
    );
    assert_capacity(&cc, 2.0, 1.0, Some(1.0), None, "lsi1");
}

#[test]
fn put_item_carrying_neither_index_attribute() {
    // Real DynamoDB: total 1, table 1.
    let db = report_table();
    let cc = put(
        &db,
        REPORT_TABLE,
        serde_json::json!({"pk": {"S": "a"}, "sk": {"S": "1"}, "other": {"S": "o"}}),
    );
    assert_capacity(&cc, 1.0, 1.0, None, None, "lsi1");
}

#[test]
fn put_item_carrying_only_the_lsi_sort_key() {
    // Real DynamoDB: total 2, table 1, lsi 1. dynoxide reported total 1 and no
    // LSI arm at all before this fix.
    let db = report_table();
    let cc = put(
        &db,
        REPORT_TABLE,
        serde_json::json!({"pk": {"S": "a"}, "sk": {"S": "1"}, "lsiSk": {"S": "L1"}}),
    );
    assert_capacity(&cc, 2.0, 1.0, None, Some(1.0), "lsi1");
}

#[test]
fn put_item_carrying_both_index_attributes() {
    // Real DynamoDB: total 3, table 1, gsi 1, lsi 1.
    let db = report_table();
    let cc = put(
        &db,
        REPORT_TABLE,
        serde_json::json!({
            "pk": {"S": "a"}, "sk": {"S": "1"},
            "gsiPk": {"S": "g"}, "lsiSk": {"S": "L1"}
        }),
    );
    assert_capacity(&cc, 3.0, 1.0, Some(1.0), Some(1.0), "lsi1");
}

#[test]
fn put_item_identical_overwrite() {
    // Real DynamoDB: total 1, table 1. Neither index's stored view changes, so
    // neither is charged. dynoxide charged the GSI again before this fix.
    let db = report_table();
    let item = serde_json::json!({
        "pk": {"S": "a"}, "sk": {"S": "1"},
        "gsiPk": {"S": "g"}, "proj": {"S": "p"}
    });
    seed(&db, REPORT_TABLE, item.clone());
    let cc = put(&db, REPORT_TABLE, item);
    assert_capacity(&cc, 1.0, 1.0, None, None, "lsi1");
}

/// The item rows 6 to 10 mutate: in both indexes, with a projected `proj` and a
/// non-projected `other`.
fn seed_indexed_item(db: &Database) {
    seed(
        db,
        REPORT_TABLE,
        serde_json::json!({
            "pk": {"S": "b"}, "sk": {"S": "1"},
            "gsiPk": {"S": "g"}, "lsiSk": {"S": "L1"},
            "proj": {"S": "p"}, "other": {"S": "o"}
        }),
    );
}

#[test]
fn update_item_setting_a_non_projected_attribute() {
    // Real DynamoDB: total 2, table 1, lsi 1. The GSI projects INCLUDE [proj],
    // so `other` never reaches it and its stored view is unchanged. The LSI
    // projects ALL, so it is charged.
    let db = report_table();
    seed_indexed_item(&db);
    let cc = update(
        &db,
        REPORT_TABLE,
        serde_json::json!({"pk": {"S": "b"}, "sk": {"S": "1"}}),
        serde_json::json!({
            "UpdateExpression": "SET #o = :v",
            "ExpressionAttributeNames": {"#o": "other"},
            "ExpressionAttributeValues": {":v": {"S": "o2"}}
        }),
    );
    assert_capacity(&cc, 2.0, 1.0, None, Some(1.0), "lsi1");
}

#[test]
fn update_item_setting_a_projected_non_key_attribute() {
    // Real DynamoDB: total 3, table 1, gsi 1, lsi 1.
    let db = report_table();
    seed_indexed_item(&db);
    let cc = update(
        &db,
        REPORT_TABLE,
        serde_json::json!({"pk": {"S": "b"}, "sk": {"S": "1"}}),
        serde_json::json!({
            "UpdateExpression": "SET proj = :v",
            "ExpressionAttributeValues": {":v": {"S": "p2"}}
        }),
    );
    assert_capacity(&cc, 3.0, 1.0, Some(1.0), Some(1.0), "lsi1");
}

#[test]
fn update_item_moving_the_gsi_key() {
    // Real DynamoDB: total 4, table 1, gsi 2, lsi 1. The GSI entry is deleted
    // from the old partition and inserted into the new one, so it costs two.
    let db = report_table();
    seed_indexed_item(&db);
    let cc = update(
        &db,
        REPORT_TABLE,
        serde_json::json!({"pk": {"S": "b"}, "sk": {"S": "1"}}),
        serde_json::json!({
            "UpdateExpression": "SET gsiPk = :v",
            "ExpressionAttributeValues": {":v": {"S": "g2"}}
        }),
    );
    assert_capacity(&cc, 4.0, 1.0, Some(2.0), Some(1.0), "lsi1");
}

#[test]
fn update_item_removing_the_gsi_key() {
    // Real DynamoDB: total 3, table 1, gsi 1, lsi 1. The item de-indexes, so
    // only the delete half is charged. dynoxide charged nothing before this fix.
    let db = report_table();
    seed_indexed_item(&db);
    let cc = update(
        &db,
        REPORT_TABLE,
        serde_json::json!({"pk": {"S": "b"}, "sk": {"S": "1"}}),
        serde_json::json!({"UpdateExpression": "REMOVE gsiPk"}),
    );
    assert_capacity(&cc, 3.0, 1.0, Some(1.0), Some(1.0), "lsi1");
}

#[test]
fn update_item_moving_the_lsi_sort_key() {
    // Real DynamoDB: total 3, table 1, lsi 2. `lsiSk` is outside the GSI's
    // projection, so the GSI reports no arm at all.
    let db = report_table();
    seed_indexed_item(&db);
    let cc = update(
        &db,
        REPORT_TABLE,
        serde_json::json!({"pk": {"S": "b"}, "sk": {"S": "1"}}),
        serde_json::json!({
            "UpdateExpression": "SET lsiSk = :v",
            "ExpressionAttributeValues": {":v": {"S": "L2"}}
        }),
    );
    assert_capacity(&cc, 3.0, 1.0, None, Some(2.0), "lsi1");
}

#[test]
fn delete_item_in_both_indexes() {
    // Real DynamoDB: total 3, table 1, gsi 1, lsi 1.
    let db = report_table();
    seed_indexed_item(&db);
    let cc = delete(
        &db,
        REPORT_TABLE,
        serde_json::json!({"pk": {"S": "b"}, "sk": {"S": "1"}}),
    );
    assert_capacity(&cc, 3.0, 1.0, Some(1.0), Some(1.0), "lsi1");
}

#[test]
fn delete_item_in_no_index() {
    // Real DynamoDB: total 1, table 1. dynoxide reported a GSI arm regardless of
    // membership before this fix.
    let db = report_table();
    seed(
        &db,
        REPORT_TABLE,
        serde_json::json!({"pk": {"S": "c"}, "sk": {"S": "1"}, "other": {"S": "o"}}),
    );
    let cc = delete(
        &db,
        REPORT_TABLE,
        serde_json::json!({"pk": {"S": "c"}, "sk": {"S": "1"}}),
    );
    assert_capacity(&cc, 1.0, 1.0, None, None, "lsi1");
}

// ---------------------------------------------------------------------------
// Sizing, from the 13 August capture. All figures above the 1KB boundary.
// ---------------------------------------------------------------------------

#[test]
fn index_arm_is_sized_on_the_projected_entry() {
    // Capture S1: a 3023B base item whose GSI entry is 18B reports table 3,
    // gsi 1. The 3KB lives in `other`, which neither index projects.
    let db = sizing_table();
    let cc = put(
        &db,
        SIZING_TABLE,
        serde_json::json!({
            "pk": {"S": "s1"}, "sk": {"S": "1"}, "gsiPk": {"S": "g"},
            "proj": {"S": "p"}, "other": {"S": pad(3000)}
        }),
    );
    assert_capacity(&cc, 4.0, 3.0, Some(1.0), None, "lsi-inc");
}

#[test]
fn in_place_entry_update_is_sized_on_the_larger_image() {
    // Capture S2 and S3: 3017B to 18B and 18B to 3020B both report gsi 3.
    let db = sizing_table();

    seed(
        &db,
        SIZING_TABLE,
        serde_json::json!({
            "pk": {"S": "s2"}, "sk": {"S": "1"},
            "gsiPk": {"S": "g"}, "proj": {"S": pad(3000)}
        }),
    );
    let shrink = update(
        &db,
        SIZING_TABLE,
        serde_json::json!({"pk": {"S": "s2"}, "sk": {"S": "1"}}),
        serde_json::json!({
            "UpdateExpression": "SET proj = :v",
            "ExpressionAttributeValues": {":v": {"S": "p"}}
        }),
    );
    assert_eq!(gsi(&shrink), Some(3.0), "shrink holds the old image's size");

    seed(
        &db,
        SIZING_TABLE,
        serde_json::json!({
            "pk": {"S": "s3"}, "sk": {"S": "1"},
            "gsiPk": {"S": "g"}, "proj": {"S": "p"}
        }),
    );
    let grow = update(
        &db,
        SIZING_TABLE,
        serde_json::json!({"pk": {"S": "s3"}, "sk": {"S": "1"}}),
        serde_json::json!({
            "UpdateExpression": "SET proj = :v",
            "ExpressionAttributeValues": {":v": {"S": pad(3000)}}
        }),
    );
    assert_eq!(gsi(&grow), Some(3.0), "grow takes the new image's size");
}

#[test]
fn in_place_entry_update_rounds_once_not_on_the_summed_bytes() {
    // Capture R2: an equal-sized in-place change at 1517B reports 2. Summing the
    // two images and rounding once would report 3.
    let db = sizing_table();
    seed(
        &db,
        SIZING_TABLE,
        serde_json::json!({
            "pk": {"S": "r2"}, "sk": {"S": "1"},
            "gsiPk": {"S": "g"}, "proj": {"S": pad(1500)}
        }),
    );
    let cc = update(
        &db,
        SIZING_TABLE,
        serde_json::json!({"pk": {"S": "r2"}, "sk": {"S": "1"}}),
        serde_json::json!({
            "UpdateExpression": "SET proj = :v",
            "ExpressionAttributeValues": {":v": {"S": "y".repeat(1500)}}
        }),
    );
    assert_eq!(gsi(&cc), Some(2.0));
}

#[test]
fn key_move_charges_each_half_on_its_own_image() {
    // Capture S4: 1517B on both sides reports gsi 4, not the 3 that summing the
    // bytes and rounding once would give.
    let db = sizing_table();
    seed(
        &db,
        SIZING_TABLE,
        serde_json::json!({
            "pk": {"S": "s4"}, "sk": {"S": "1"},
            "gsiPk": {"S": "A"}, "proj": {"S": pad(1500)}
        }),
    );
    let equal_halves = update(
        &db,
        SIZING_TABLE,
        serde_json::json!({"pk": {"S": "s4"}, "sk": {"S": "1"}}),
        serde_json::json!({
            "UpdateExpression": "SET gsiPk = :v",
            "ExpressionAttributeValues": {":v": {"S": "B"}}
        }),
    );
    assert_eq!(gsi(&equal_halves), Some(4.0));

    // Capture S5: 3017B collapsing to an 18B entry reports gsi 4, which is
    // 3 + 1 rather than twice the larger half.
    seed(
        &db,
        SIZING_TABLE,
        serde_json::json!({
            "pk": {"S": "s5"}, "sk": {"S": "1"},
            "gsiPk": {"S": "A"}, "proj": {"S": pad(3000)}
        }),
    );
    let asymmetric = update(
        &db,
        SIZING_TABLE,
        serde_json::json!({"pk": {"S": "s5"}, "sk": {"S": "1"}}),
        serde_json::json!({
            "UpdateExpression": "SET gsiPk = :g, proj = :p",
            "ExpressionAttributeValues": {":g": {"S": "B"}, ":p": {"S": "p"}}
        }),
    );
    assert_eq!(gsi(&asymmetric), Some(4.0));
}

#[test]
fn delete_is_sized_on_the_entry_the_item_held() {
    // Capture S6: deleting an item with a 3017B GSI entry reports gsi 3.
    let db = sizing_table();
    seed(
        &db,
        SIZING_TABLE,
        serde_json::json!({
            "pk": {"S": "s6"}, "sk": {"S": "1"},
            "gsiPk": {"S": "g"}, "proj": {"S": pad(3000)}
        }),
    );
    let cc = delete(
        &db,
        SIZING_TABLE,
        serde_json::json!({"pk": {"S": "s6"}, "sk": {"S": "1"}}),
    );
    assert_eq!(gsi(&cc), Some(3.0));
}

#[test]
fn removing_the_index_key_is_sized_on_the_old_entry() {
    // Capture S7: de-indexing an item with a 3017B GSI entry reports gsi 3.
    let db = sizing_table();
    seed(
        &db,
        SIZING_TABLE,
        serde_json::json!({
            "pk": {"S": "s7"}, "sk": {"S": "1"},
            "gsiPk": {"S": "g"}, "proj": {"S": pad(3000)}
        }),
    );
    let cc = update(
        &db,
        SIZING_TABLE,
        serde_json::json!({"pk": {"S": "s7"}, "sk": {"S": "1"}}),
        serde_json::json!({"UpdateExpression": "REMOVE gsiPk"}),
    );
    assert_eq!(gsi(&cc), Some(3.0));
}

#[test]
fn lsi_sizing_mirrors_the_gsi() {
    // Capture L1 and L2: an LSI in-place grow reports 3, and an LSI key move
    // from a 3017B entry to a tiny one reports 4.
    let db = sizing_table();
    seed(
        &db,
        SIZING_TABLE,
        serde_json::json!({
            "pk": {"S": "l1"}, "sk": {"S": "1"},
            "lsiSk": {"S": "L"}, "proj": {"S": "p"}
        }),
    );
    let grow = update(
        &db,
        SIZING_TABLE,
        serde_json::json!({"pk": {"S": "l1"}, "sk": {"S": "1"}}),
        serde_json::json!({
            "UpdateExpression": "SET proj = :v",
            "ExpressionAttributeValues": {":v": {"S": pad(3000)}}
        }),
    );
    assert_eq!(lsi(&grow, "lsi-inc"), Some(3.0));

    seed(
        &db,
        SIZING_TABLE,
        serde_json::json!({
            "pk": {"S": "l2"}, "sk": {"S": "1"},
            "lsiSk": {"S": "A"}, "proj": {"S": pad(3000)}
        }),
    );
    let moved = update(
        &db,
        SIZING_TABLE,
        serde_json::json!({"pk": {"S": "l2"}, "sk": {"S": "1"}}),
        serde_json::json!({
            "UpdateExpression": "SET lsiSk = :s, proj = :p",
            "ExpressionAttributeValues": {":s": {"S": "B"}, ":p": {"S": "p"}}
        }),
    );
    assert_eq!(lsi(&moved, "lsi-inc"), Some(4.0));
}

// ---------------------------------------------------------------------------
// The base table arm, from the 13 August capture. Not part of #176: below 1KB
// every one of these reports 1 either way, which is why the report missed it.
// ---------------------------------------------------------------------------

#[test]
fn table_arm_is_sized_on_the_larger_image() {
    // Capture S2 and S3: a base item swinging between 3017B and 18B reports
    // table 3 in both directions. Sizing on the finished item would report 1
    // for the shrink.
    let db = sizing_table();

    seed(
        &db,
        SIZING_TABLE,
        serde_json::json!({
            "pk": {"S": "t1"}, "sk": {"S": "1"}, "proj": {"S": pad(3000)}
        }),
    );
    let shrink = put(
        &db,
        SIZING_TABLE,
        serde_json::json!({"pk": {"S": "t1"}, "sk": {"S": "1"}, "proj": {"S": "p"}}),
    );
    assert_eq!(table_units(&shrink), Some(3.0), "put holds the old image");

    seed(
        &db,
        SIZING_TABLE,
        serde_json::json!({"pk": {"S": "t2"}, "sk": {"S": "1"}, "proj": {"S": "p"}}),
    );
    let grow = put(
        &db,
        SIZING_TABLE,
        serde_json::json!({
            "pk": {"S": "t2"}, "sk": {"S": "1"}, "proj": {"S": pad(3000)}
        }),
    );
    assert_eq!(table_units(&grow), Some(3.0), "put takes the new image");
}

#[test]
fn update_shrinking_an_item_holds_the_old_image_size() {
    // Capture R3: a base item of 2017B updated down to 117B reports table 2.
    let db = sizing_table();
    seed(
        &db,
        SIZING_TABLE,
        serde_json::json!({
            "pk": {"S": "r3"}, "sk": {"S": "1"},
            "gsiPk": {"S": "g"}, "proj": {"S": pad(2000)}
        }),
    );
    let cc = update(
        &db,
        SIZING_TABLE,
        serde_json::json!({"pk": {"S": "r3"}, "sk": {"S": "1"}}),
        serde_json::json!({
            "UpdateExpression": "SET proj = :v",
            "ExpressionAttributeValues": {":v": {"S": pad(100)}}
        }),
    );
    assert_eq!(table_units(&cc), Some(2.0));
}

#[test]
fn a_fresh_write_is_sized_on_the_new_item_alone() {
    let db = sizing_table();
    let cc = put(
        &db,
        SIZING_TABLE,
        serde_json::json!({
            "pk": {"S": "fresh"}, "sk": {"S": "1"}, "proj": {"S": pad(2000)}
        }),
    );
    assert_eq!(table_units(&cc), Some(2.0));

    // And an upsert through UpdateItem likewise has no prior image to hold.
    let upsert = update(
        &db,
        SIZING_TABLE,
        serde_json::json!({"pk": {"S": "fresh2"}, "sk": {"S": "1"}}),
        serde_json::json!({
            "UpdateExpression": "SET proj = :v",
            "ExpressionAttributeValues": {":v": {"S": "p"}}
        }),
    );
    assert_eq!(table_units(&upsert), Some(1.0));
}

#[test]
fn batch_put_sizes_the_table_arm_on_the_larger_image() {
    let db = sizing_table();
    seed(
        &db,
        SIZING_TABLE,
        serde_json::json!({
            "pk": {"S": "b1"}, "sk": {"S": "1"}, "proj": {"S": pad(3000)}
        }),
    );

    let req = serde_json::json!({
        "RequestItems": {
            SIZING_TABLE: [{"PutRequest": {"Item": {
                "pk": {"S": "b1"}, "sk": {"S": "1"}, "proj": {"S": "p"}
            }}}]
        },
        "ReturnConsumedCapacity": "INDEXES"
    });
    let caps = db
        .batch_write_item(serde_json::from_value(req).unwrap())
        .unwrap()
        .consumed_capacity
        .expect("INDEXES mode reports capacity");
    let cc = caps.iter().find(|c| c.table_name == SIZING_TABLE).unwrap();

    assert_eq!(table_units(cc), Some(3.0));
}

// ---------------------------------------------------------------------------
// TOTAL mode, upserts, and BatchWriteItem.
// ---------------------------------------------------------------------------

#[test]
fn total_mode_reports_the_same_totals_without_breakdown_arms() {
    let db = report_table();
    seed_indexed_item(&db);
    let req = serde_json::json!({
        "TableName": REPORT_TABLE,
        "Key": {"pk": {"S": "b"}, "sk": {"S": "1"}},
        "UpdateExpression": "SET gsiPk = :v",
        "ExpressionAttributeValues": {":v": {"S": "g2"}},
        "ReturnConsumedCapacity": "TOTAL"
    });
    let cc = db
        .update_item(serde_json::from_value(req).unwrap())
        .unwrap()
        .consumed_capacity
        .expect("TOTAL mode reports capacity");

    // Same total as the INDEXES case, with no per-resource detail.
    assert_eq!(cc.capacity_units, 4.0);
    assert!(cc.table.is_none());
    assert!(cc.global_secondary_indexes.is_none());
    assert!(cc.local_secondary_indexes.is_none());
}

#[test]
fn create_through_update_charges_each_index_once() {
    // An UpdateItem against a key that does not exist is an upsert. The old
    // image is genuinely absent, so each index takes a single insert charge
    // rather than a spurious key move.
    let db = report_table();
    let cc = update(
        &db,
        REPORT_TABLE,
        serde_json::json!({"pk": {"S": "new"}, "sk": {"S": "1"}}),
        serde_json::json!({
            "UpdateExpression": "SET gsiPk = :g, lsiSk = :l",
            "ExpressionAttributeValues": {":g": {"S": "g"}, ":l": {"S": "L1"}}
        }),
    );
    assert_capacity(&cc, 3.0, 1.0, Some(1.0), Some(1.0), "lsi1");
}

#[test]
fn batch_write_sums_index_arms_across_the_batch() {
    let db = report_table();
    let req = serde_json::json!({
        "RequestItems": {
            REPORT_TABLE: [
                {"PutRequest": {"Item": {
                    "pk": {"S": "x1"}, "sk": {"S": "1"},
                    "gsiPk": {"S": "g"}, "lsiSk": {"S": "L1"}
                }}},
                {"PutRequest": {"Item": {
                    "pk": {"S": "x2"}, "sk": {"S": "1"},
                    "gsiPk": {"S": "g"}
                }}},
                {"PutRequest": {"Item": {
                    "pk": {"S": "x3"}, "sk": {"S": "1"},
                    "other": {"S": "o"}
                }}}
            ]
        },
        "ReturnConsumedCapacity": "INDEXES"
    });
    let caps = db
        .batch_write_item(serde_json::from_value(req).unwrap())
        .unwrap()
        .consumed_capacity
        .expect("INDEXES mode reports capacity");
    let cc = caps.iter().find(|c| c.table_name == REPORT_TABLE).unwrap();

    // Three table writes; two items join the GSI and one joins the LSI.
    assert_eq!(table_units(cc), Some(3.0));
    assert_eq!(gsi(cc), Some(2.0));
    assert_eq!(lsi(cc, "lsi1"), Some(1.0));
    assert_eq!(cc.capacity_units, 6.0);
}

#[test]
fn batch_write_does_not_charge_an_identical_overwrite() {
    let db = report_table();
    let item = serde_json::json!({
        "pk": {"S": "y1"}, "sk": {"S": "1"}, "gsiPk": {"S": "g"}
    });
    seed(&db, REPORT_TABLE, item.clone());

    let req = serde_json::json!({
        "RequestItems": {REPORT_TABLE: [{"PutRequest": {"Item": item}}]},
        "ReturnConsumedCapacity": "INDEXES"
    });
    let caps = db
        .batch_write_item(serde_json::from_value(req).unwrap())
        .unwrap()
        .consumed_capacity
        .expect("INDEXES mode reports capacity");
    let cc = caps.iter().find(|c| c.table_name == REPORT_TABLE).unwrap();

    assert_eq!(table_units(cc), Some(1.0));
    assert_eq!(cc.capacity_units, 1.0);
    assert!(cc.global_secondary_indexes.is_none());
    assert!(cc.local_secondary_indexes.is_none());
}

#[test]
fn batch_delete_charges_only_indexes_the_item_belonged_to() {
    let db = report_table();
    seed(
        &db,
        REPORT_TABLE,
        serde_json::json!({"pk": {"S": "z1"}, "sk": {"S": "1"}, "other": {"S": "o"}}),
    );

    let req = serde_json::json!({
        "RequestItems": {
            REPORT_TABLE: [{"DeleteRequest": {"Key": {
                "pk": {"S": "z1"}, "sk": {"S": "1"}
            }}}]
        },
        "ReturnConsumedCapacity": "INDEXES"
    });
    let caps = db
        .batch_write_item(serde_json::from_value(req).unwrap())
        .unwrap()
        .consumed_capacity
        .expect("INDEXES mode reports capacity");
    let cc = caps.iter().find(|c| c.table_name == REPORT_TABLE).unwrap();

    assert_eq!(cc.capacity_units, 1.0);
    assert!(cc.global_secondary_indexes.is_none());
    assert!(cc.local_secondary_indexes.is_none());
}
