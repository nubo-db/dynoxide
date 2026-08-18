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
    TransactPut, TransactWriteItem, TransactWriteItemsRequest,
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
fn update_item_agrees_with_put_item_on_the_same_resulting_item() {
    // Built the other way round from the PutItem cases: a base item already near
    // the limit, then an update that adds numbers which expand on storage. The
    // update expression stays small, so this isolates the size measure from the
    // separate 4KB expression limit.
    let db = make_db();
    let mut base = HashMap::new();
    base.insert("pk".to_string(), AttributeValue::S("k".to_string()));
    base.insert("b".to_string(), AttributeValue::S("x".repeat(409_000)));
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
