//! Indexes are maintained the same way whether or not capacity was asked for.
//!
//! Sizing an index write is skipped when `ReturnConsumedCapacity` asks for
//! nothing, which is the default. The saving is real, and so is the risk: the
//! sizing sits in the same per-index loop as the write operations themselves, so
//! a gate placed one line out would stop maintaining the index rather than stop
//! measuring it, and no capacity assertion anywhere would notice, because those
//! all run in a mode that does the work.
//!
//! So these read the indexes back through a query instead. Each case performs a
//! write in the default mode and then asks the index what it holds.

use dynoxide::Database;

const TABLE: &str = "idx_no_capacity";

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
        }))
        .unwrap(),
    )
    .unwrap();
    db
}

/// Put an item without asking for capacity, which is the gated path.
fn put(db: &Database, sk: &str, gsi_pk: &str, lsi_sk: &str) {
    let req = serde_json::json!({
        "TableName": TABLE,
        "Item": {
            "pk": {"S": "p"}, "sk": {"S": sk},
            "gsiPk": {"S": gsi_pk}, "lsiSk": {"S": lsi_sk},
            "proj": {"S": "v"}, "other": {"S": "w"}
        }
    });
    db.put_item(serde_json::from_value(req).unwrap()).unwrap();
}

/// What one index holds for a given key, read back through a query.
fn index_rows(db: &Database, index: &str, key_attr: &str, key: &str) -> Vec<serde_json::Value> {
    let req = serde_json::json!({
        "TableName": TABLE,
        "IndexName": index,
        "KeyConditionExpression": format!("{key_attr} = :k"),
        "ExpressionAttributeValues": {":k": {"S": key}}
    });
    let resp = db.query(serde_json::from_value(req).unwrap()).unwrap();
    serde_json::to_value(resp.items.unwrap_or_default())
        .unwrap()
        .as_array()
        .cloned()
        .unwrap_or_default()
}

#[test]
fn an_insert_populates_both_indexes_with_no_capacity_asked_for() {
    let db = table();
    put(&db, "s1", "g1", "l1");

    assert_eq!(index_rows(&db, "gsi-inc", "gsiPk", "g1").len(), 1);
    assert_eq!(index_rows(&db, "lsi-all", "pk", "p").len(), 1);
}

#[test]
fn an_overwrite_moves_the_index_entry_with_no_capacity_asked_for() {
    let db = table();
    put(&db, "s1", "g1", "l1");
    // The overwrite is the case the gate changes: it is the only one that used
    // to project the old image.
    put(&db, "s1", "g2", "l2");

    assert!(
        index_rows(&db, "gsi-inc", "gsiPk", "g1").is_empty(),
        "the entry at the old GSI key should have been removed"
    );
    assert_eq!(
        index_rows(&db, "gsi-inc", "gsiPk", "g2").len(),
        1,
        "the entry should sit at the new GSI key"
    );

    let lsi = index_rows(&db, "lsi-all", "pk", "p");
    assert_eq!(lsi.len(), 1, "one row, moved rather than duplicated");
    assert_eq!(lsi[0]["lsiSk"]["S"], "l2");
}

#[test]
fn a_delete_clears_both_indexes_with_no_capacity_asked_for() {
    let db = table();
    put(&db, "s1", "g1", "l1");

    let req = serde_json::json!({
        "TableName": TABLE,
        "Key": {"pk": {"S": "p"}, "sk": {"S": "s1"}}
    });
    db.delete_item(serde_json::from_value(req).unwrap())
        .unwrap();

    assert!(index_rows(&db, "gsi-inc", "gsiPk", "g1").is_empty());
    assert!(index_rows(&db, "lsi-all", "pk", "p").is_empty());
}

#[test]
fn an_item_leaving_an_index_is_removed_from_it_with_no_capacity_asked_for() {
    let db = table();
    put(&db, "s1", "g1", "l1");

    // Dropping `gsiPk` takes the item out of the sparse GSI while leaving it in
    // the table and the LSI. Nothing is inserted for the GSI on this write, so
    // the removal is the only thing keeping the index honest.
    let req = serde_json::json!({
        "TableName": TABLE,
        "Item": {
            "pk": {"S": "p"}, "sk": {"S": "s1"},
            "lsiSk": {"S": "l1"}, "proj": {"S": "v"}
        }
    });
    db.put_item(serde_json::from_value(req).unwrap()).unwrap();

    assert!(
        index_rows(&db, "gsi-inc", "gsiPk", "g1").is_empty(),
        "an item that left the sparse GSI should not still be in it"
    );
    assert_eq!(index_rows(&db, "lsi-all", "pk", "p").len(), 1);
}

#[test]
fn a_partiql_write_maintains_both_indexes_with_no_capacity_asked_for() {
    let db = table();
    db.execute_statement(
        serde_json::from_value(serde_json::json!({
            "Statement": format!(
                "INSERT INTO \"{TABLE}\" VALUE \
                 {{'pk':'p','sk':'s1','gsiPk':'g1','lsiSk':'l1','proj':'v'}}"
            )
        }))
        .unwrap(),
    )
    .unwrap();

    assert_eq!(index_rows(&db, "gsi-inc", "gsiPk", "g1").len(), 1);
    assert_eq!(index_rows(&db, "lsi-all", "pk", "p").len(), 1);
}

#[test]
fn a_transactional_write_maintains_both_indexes_with_no_capacity_asked_for() {
    let db = table();
    let req = serde_json::json!({
        "TransactItems": [{"Put": {
            "TableName": TABLE,
            "Item": {
                "pk": {"S": "p"}, "sk": {"S": "s1"},
                "gsiPk": {"S": "g1"}, "lsiSk": {"S": "l1"}, "proj": {"S": "v"}
            }
        }}]
    });
    db.transact_write_items(serde_json::from_value(req).unwrap())
        .unwrap();

    assert_eq!(index_rows(&db, "gsi-inc", "gsiPk", "g1").len(), 1);
    assert_eq!(index_rows(&db, "lsi-all", "pk", "p").len(), 1);
}
