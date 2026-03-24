//! Tests for Fix #6: Parallel Scan (Segment/TotalSegments)
//!
//! Tests that parallel scan segments return disjoint, complete subsets
//! with SQLite-level filtering via FNV-1a hash.

use dynoxide::Database;
use dynoxide::actions::create_table::CreateTableRequest;
use dynoxide::actions::put_item::PutItemRequest;
use dynoxide::actions::scan::ScanRequest;
use dynoxide::types::*;
use std::collections::{HashMap, HashSet};

fn make_db_with_items(count: usize) -> Database {
    let db = Database::memory().unwrap();
    db.create_table(CreateTableRequest {
        table_name: "Tbl".to_string(),
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

    for i in 0..count {
        let mut item = HashMap::new();
        item.insert("pk".to_string(), AttributeValue::S(format!("item-{i:04}")));
        item.insert("data".to_string(), AttributeValue::S(format!("value-{i}")));
        db.put_item(PutItemRequest {
            table_name: "Tbl".to_string(),
            item,
            ..Default::default()
        })
        .unwrap();
    }
    db
}

fn scan_req(table: &str, segment: Option<u32>, total: Option<u32>) -> ScanRequest {
    let mut req = ScanRequest::default();
    req.table_name = table.to_string();
    req.segment = segment;
    req.total_segments = total;
    req
}

#[test]
fn test_parallel_scan_returns_subset() {
    let db = make_db_with_items(20);
    let resp = db.scan(scan_req("Tbl", Some(0), Some(4))).unwrap();
    // Should return a subset, not all 20
    assert!(resp.count < 20, "segment should return a subset");
    assert!(resp.count > 0, "segment should not be empty");
}

#[test]
fn test_all_segments_return_all_items() {
    let db = make_db_with_items(50);
    let total_segments = 4;
    let mut all_pks = HashSet::new();

    for segment in 0..total_segments {
        let resp = db
            .scan(scan_req("Tbl", Some(segment), Some(total_segments)))
            .unwrap();
        for item in resp.items.unwrap_or_default() {
            if let Some(AttributeValue::S(pk)) = item.get("pk") {
                let was_new = all_pks.insert(pk.clone());
                assert!(was_new, "duplicate pk across segments: {pk}");
            }
        }
    }

    assert_eq!(
        all_pks.len(),
        50,
        "all segments combined should return all 50 items"
    );
}

#[test]
fn test_segment_ge_total_segments_rejected() {
    let db = make_db_with_items(5);
    let err = db.scan(scan_req("Tbl", Some(4), Some(4))).unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("Segment") && msg.contains("less than") && msg.contains("TotalSegments"),
        "unexpected error: {msg}"
    );
}

#[test]
fn test_total_segments_zero_rejected() {
    let db = make_db_with_items(5);
    let err = db.scan(scan_req("Tbl", Some(0), Some(0))).unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("totalSegments") && msg.contains("between 1 and 1000000"),
        "unexpected error: {msg}"
    );
}

#[test]
fn test_segment_without_total_segments_rejected() {
    let db = make_db_with_items(5);
    let err = db.scan(scan_req("Tbl", Some(0), None)).unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("TotalSegments parameter is required"),
        "unexpected error: {msg}"
    );
}

#[test]
fn test_paginated_parallel_scan() {
    let db = make_db_with_items(30);
    let total_segments = 2;
    let mut all_pks = HashSet::new();

    for segment in 0..total_segments {
        let mut start_key: Option<HashMap<String, AttributeValue>> = None;
        loop {
            let mut req = ScanRequest::default();
            req.table_name = "Tbl".to_string();
            req.segment = Some(segment);
            req.total_segments = Some(total_segments);
            req.limit = Some(5);
            req.exclusive_start_key = start_key.clone();
            let resp = db.scan(req).unwrap();

            for item in resp.items.unwrap_or_default() {
                if let Some(AttributeValue::S(pk)) = item.get("pk") {
                    all_pks.insert(pk.clone());
                }
            }

            if resp.last_evaluated_key.is_none() {
                break;
            }
            start_key = resp.last_evaluated_key;
        }
    }

    assert_eq!(
        all_pks.len(),
        30,
        "paginated parallel scan should return all 30 items"
    );
}
