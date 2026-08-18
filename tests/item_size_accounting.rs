//! Item-size accounting for numbers, and the agreement it buys between write paths.
//!
//! DynamoDB sizes a number by its significant digits, so the figure survives the
//! normalisation that expands scientific notation on storage. The write paths
//! check the 400KB limit at different points either side of that normalisation,
//! which is only safe while the measure does not move.

use dynoxide::Database;
use dynoxide::actions::batch_write_item::{BatchWriteItemRequest, PutRequest, WriteRequest};
use dynoxide::actions::create_table::CreateTableRequest;
use dynoxide::actions::put_item::PutItemRequest;
use dynoxide::actions::transact_write_items::{
    TransactPut, TransactUpdate, TransactWriteItem, TransactWriteItemsRequest,
};
use dynoxide::actions::update_item::UpdateItemRequest;
use dynoxide::types::*;
use std::collections::HashMap;

fn make_db() -> Database {
    let db = Database::memory().unwrap();
    db.create_table(CreateTableRequest {
        table_name: "Items".to_string(),
        key_schema: vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::HASH,
        }],
        attribute_definitions: vec![AttributeDefinition {
            attribute_name: "pk".to_string(),
            attribute_type: ScalarAttributeType::S,
        }],
        ..Default::default()
    })
    .unwrap();
    db
}

/// An item whose numbers all expand hugely when normalised. Under a measure
/// taken over the digits of the stored string this crosses the limit; under
/// DynamoDB's it stays well under.
fn scientific_notation_item(pk: &str, count: usize) -> HashMap<String, AttributeValue> {
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S(pk.to_string()));
    for i in 0..count {
        item.insert(format!("n{i:04}"), AttributeValue::N("1E125".to_string()));
    }
    item
}

#[test]
fn every_write_path_accepts_an_item_that_normalising_expands() {
    let db = make_db();
    let item = scientific_notation_item("k", 8000);

    db.put_item(PutItemRequest {
        table_name: "Items".to_string(),
        item: item.clone(),
        ..Default::default()
    })
    .expect("PutItem");

    db.batch_write_item(BatchWriteItemRequest {
        request_items: HashMap::from([(
            "Items".to_string(),
            vec![WriteRequest {
                put_request: Some(PutRequest {
                    item: scientific_notation_item("k-batch", 8000),
                }),
                delete_request: None,
            }],
        )]),
        ..Default::default()
    })
    .expect("BatchWriteItem");

    db.transact_write_items(TransactWriteItemsRequest {
        transact_items: vec![TransactWriteItem {
            put: Some(TransactPut {
                table_name: "Items".to_string(),
                item: scientific_notation_item("k-transact", 8000),
                ..Default::default()
            }),
            ..Default::default()
        }],
        ..Default::default()
    })
    .expect("TransactWriteItems");
}

#[test]
fn the_size_recorded_for_a_stored_item_is_the_size_that_was_checked() {
    // The reported bug: PutItem admitted an item on one measure and the engine
    // then recorded it under another, feeding table statistics and the item
    // collection size estimate a figure the limit had never been applied to.
    let db = make_db();
    let item = scientific_notation_item("k", 8000);
    let checked = item_size(&item);

    db.put_item(PutItemRequest {
        table_name: "Items".to_string(),
        item,
        ..Default::default()
    })
    .unwrap();

    let stats = db.table_stats().unwrap();
    let stored = stats.iter().find(|s| s.table_name == "Items").unwrap();
    assert_eq!(stored.size_bytes as usize, checked);
    assert!(checked < MAX_ITEM_SIZE);
}

#[test]
fn update_item_sizes_expanding_numbers_the_same_way() {
    // Built the other way round from the PutItem cases: a base item already near
    // the limit, then an update that adds numbers which expand on storage. The
    // update expression stays small, so this isolates the size measure from the
    // separate 4KB expression limit. Sized to clear UpdateItem's own lower
    // ceiling, which the tests below cover.
    let db = make_db();
    let mut base = HashMap::new();
    base.insert("pk".to_string(), AttributeValue::S("k".to_string()));
    base.insert("b".to_string(), AttributeValue::S("x".repeat(408_000)));
    db.put_item(PutItemRequest {
        table_name: "Items".to_string(),
        item: base,
        ..Default::default()
    })
    .expect("PutItem base");

    let mut sets = Vec::new();
    let mut values = HashMap::new();
    for i in 0..50 {
        sets.push(format!("n{i:04} = :v{i}"));
        values.insert(format!(":v{i}"), AttributeValue::N("1E125".to_string()));
    }

    db.update_item(UpdateItemRequest {
        table_name: "Items".to_string(),
        key: HashMap::from([("pk".to_string(), AttributeValue::S("k".to_string()))]),
        update_expression: Some(format!("SET {}", sets.join(", "))),
        expression_attribute_values: Some(values),
        ..Default::default()
    })
    .expect("UpdateItem");

    // Same item shape through PutItem, which measured it at a different point.
    let stats = db.table_stats().unwrap();
    let stored = stats.iter().find(|s| s.table_name == "Items").unwrap();
    assert!((stored.size_bytes as usize) < MAX_ITEM_SIZE);
}

/// One `SET` of a blob of `blob_len` bytes onto key `pk`, which upserts.
fn set_blob(db: &Database, pk: &str, blob_len: usize) -> Result<(), String> {
    db.update_item(UpdateItemRequest {
        table_name: "Items".to_string(),
        key: HashMap::from([("pk".to_string(), AttributeValue::S(pk.to_string()))]),
        update_expression: Some("SET #b = :v".to_string()),
        expression_attribute_names: Some(HashMap::from([("#b".to_string(), "b".to_string())])),
        expression_attribute_values: Some(HashMap::from([(
            ":v".to_string(),
            AttributeValue::S("x".repeat(blob_len)),
        )])),
        ..Default::default()
    })
    .map(|_| ())
    .map_err(|e| e.to_string())
}

#[test]
fn update_item_is_measured_against_a_lower_ceiling_than_put_item() {
    // Captured against eu-west-2. An update is not sized the way a put is: the
    // key attributes come out of the figure and each action adds a fixed cost,
    // three bytes for the update plus nineteen for a SET. With a one-character
    // key the resulting item may reach 409,581, where PutItem accepts 409,600.
    //
    //   resulting item = pk (2 + 1) + "b" (1) + blob
    let db = make_db();
    let ceiling = 409_581 - 4;

    set_blob(&db, "k", ceiling).expect("at the ceiling");
    let err = set_blob(&db, "k2", ceiling + 1).expect_err("one byte over");
    assert_eq!(
        err,
        "Item size to update has exceeded the maximum allowed size"
    );

    // The same finished item is within reach of PutItem, which is the asymmetry
    // the capture found: an item can be puttable and not updatable.
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("k3".to_string()));
    item.insert("b".to_string(), AttributeValue::S("x".repeat(ceiling + 1)));
    db.put_item(PutItemRequest {
        table_name: "Items".to_string(),
        item,
        ..Default::default()
    })
    .expect("PutItem accepts what UpdateItem refused");
}

#[test]
fn each_update_action_adds_to_what_the_gate_measures() {
    // A second SET costs nineteen more, so the ceiling drops by nineteen.
    let db = make_db();
    let two_set_ceiling = 409_581 - 19 - 4 - 2; // second attribute is name + 1 byte

    let run = |pk: &str, blob_len: usize| {
        db.update_item(UpdateItemRequest {
            table_name: "Items".to_string(),
            key: HashMap::from([("pk".to_string(), AttributeValue::S(pk.to_string()))]),
            update_expression: Some("SET #b = :v, #c = :w".to_string()),
            expression_attribute_names: Some(HashMap::from([
                ("#b".to_string(), "b".to_string()),
                ("#c".to_string(), "c".to_string()),
            ])),
            expression_attribute_values: Some(HashMap::from([
                (":v".to_string(), AttributeValue::S("x".repeat(blob_len))),
                (":w".to_string(), AttributeValue::S("z".to_string())),
            ])),
            ..Default::default()
        })
        .map(|_| ())
        .map_err(|e| e.to_string())
    };

    run("k", two_set_ceiling).expect("at the two-action ceiling");
    run("k2", two_set_ceiling + 1).expect_err("one byte over");
}

#[test]
fn a_transacted_write_reports_an_oversized_item_the_way_dynamodb_does() {
    // Captured against eu-west-2. The two actions differ, because a put's size is
    // knowable from the request and an update's is not:
    //   Put    -> a plain ValidationException, before the transaction runs
    //   Update -> a cancellation reason, and the message names it as an update
    let db = make_db();

    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("k".to_string()));
    item.insert("b".to_string(), AttributeValue::S("x".repeat(409_601)));
    let err = db
        .transact_write_items(TransactWriteItemsRequest {
            transact_items: vec![TransactWriteItem {
                put: Some(TransactPut {
                    table_name: "Items".to_string(),
                    item,
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "Item size has exceeded the maximum allowed size"
    );

    let err = db
        .transact_write_items(TransactWriteItemsRequest {
            transact_items: vec![TransactWriteItem {
                update: Some(TransactUpdate {
                    table_name: "Items".to_string(),
                    key: HashMap::from([("pk".to_string(), AttributeValue::S("k2".to_string()))]),
                    update_expression: "SET b = :v".to_string(),
                    expression_attribute_values: Some(HashMap::from([(
                        ":v".to_string(),
                        AttributeValue::S("x".repeat(409_601)),
                    )])),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        })
        .unwrap_err();
    match err {
        dynoxide::errors::DynoxideError::TransactionCanceledException(_, reasons) => {
            assert_eq!(reasons[0].code, "ValidationError");
            assert_eq!(
                reasons[0].message.as_deref(),
                Some("Item size to update has exceeded the maximum allowed size")
            );
        }
        other => panic!("expected a cancellation, got {other:?}"),
    }
}

#[test]
fn a_transacted_update_is_not_charged_the_standalone_update_overhead() {
    // The standalone UpdateItem takes the key attributes out and charges per
    // action; a transacted one is measured flat against the resulting item. So
    // an item that UpdateItem refuses goes through inside a transaction.
    let db = make_db();
    let just_over_update_ceiling = 409_581 - 4 + 1;

    set_blob(&db, "k", just_over_update_ceiling).expect_err("UpdateItem refuses it");

    db.transact_write_items(TransactWriteItemsRequest {
        transact_items: vec![TransactWriteItem {
            update: Some(TransactUpdate {
                table_name: "Items".to_string(),
                key: HashMap::from([("pk".to_string(), AttributeValue::S("k".to_string()))]),
                update_expression: "SET b = :v".to_string(),
                expression_attribute_values: Some(HashMap::from([(
                    ":v".to_string(),
                    AttributeValue::S("x".repeat(just_over_update_ceiling)),
                )])),
                ..Default::default()
            }),
            ..Default::default()
        }],
        ..Default::default()
    })
    .expect("a transacted update accepts it");
}
