use dynoxide::Database;
use dynoxide::actions::batch_get_item::BatchGetItemRequest;
use dynoxide::actions::batch_write_item::BatchWriteItemRequest;
use dynoxide::actions::create_table::CreateTableRequest;
use dynoxide::actions::put_item::PutItemRequest;

fn setup_db() -> Database {
    Database::memory().unwrap()
}

fn create_test_table(db: &Database, name: &str) {
    let req: CreateTableRequest = serde_json::from_value(serde_json::json!({
        "TableName": name,
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
    .unwrap();
    db.create_table(req).unwrap();
}

fn put(db: &Database, table: &str, item: serde_json::Value) {
    let req: PutItemRequest = serde_json::from_value(serde_json::json!({
        "TableName": table,
        "Item": item
    }))
    .unwrap();
    db.put_item(req).unwrap();
}

// =============================================================================
// BatchGetItem tests
// =============================================================================

#[test]
fn test_batch_get_single_table() {
    let db = setup_db();
    create_test_table(&db, "Tbl");

    put(
        &db,
        "Tbl",
        serde_json::json!({"pk": {"S": "a"}, "sk": {"S": "1"}, "name": {"S": "Alice"}}),
    );
    put(
        &db,
        "Tbl",
        serde_json::json!({"pk": {"S": "b"}, "sk": {"S": "1"}, "name": {"S": "Bob"}}),
    );

    let req: BatchGetItemRequest = serde_json::from_value(serde_json::json!({
        "RequestItems": {
            "Tbl": {
                "Keys": [
                    {"pk": {"S": "a"}, "sk": {"S": "1"}},
                    {"pk": {"S": "b"}, "sk": {"S": "1"}}
                ]
            }
        }
    }))
    .unwrap();

    let resp = db.batch_get_item(req).unwrap();
    assert_eq!(resp.responses["Tbl"].len(), 2);
    assert!(resp.unprocessed_keys.is_empty());
}

#[test]
fn test_batch_get_multiple_tables() {
    let db = setup_db();
    create_test_table(&db, "TableA");
    create_test_table(&db, "TableB");

    put(
        &db,
        "TableA",
        serde_json::json!({"pk": {"S": "a"}, "sk": {"S": "1"}}),
    );
    put(
        &db,
        "TableB",
        serde_json::json!({"pk": {"S": "b"}, "sk": {"S": "1"}}),
    );

    let req: BatchGetItemRequest = serde_json::from_value(serde_json::json!({
        "RequestItems": {
            "TableA": {"Keys": [{"pk": {"S": "a"}, "sk": {"S": "1"}}]},
            "TableB": {"Keys": [{"pk": {"S": "b"}, "sk": {"S": "1"}}]}
        }
    }))
    .unwrap();

    let resp = db.batch_get_item(req).unwrap();
    assert_eq!(resp.responses["TableA"].len(), 1);
    assert_eq!(resp.responses["TableB"].len(), 1);
}

#[test]
fn test_batch_get_with_projection() {
    let db = setup_db();
    create_test_table(&db, "Tbl");

    put(
        &db,
        "Tbl",
        serde_json::json!({"pk": {"S": "a"}, "sk": {"S": "1"}, "name": {"S": "Alice"}, "age": {"N": "30"}}),
    );

    let req: BatchGetItemRequest = serde_json::from_value(serde_json::json!({
        "RequestItems": {
            "Tbl": {
                "Keys": [{"pk": {"S": "a"}, "sk": {"S": "1"}}],
                "ProjectionExpression": "#n",
                "ExpressionAttributeNames": {"#n": "name"}
            }
        }
    }))
    .unwrap();

    let resp = db.batch_get_item(req).unwrap();
    let items = &resp.responses["Tbl"];
    assert_eq!(items.len(), 1);
    // BatchGetItem does NOT auto-include key attributes in projections
    assert!(!items[0].contains_key("pk"));
    assert!(items[0].contains_key("name")); // Projected
    assert!(!items[0].contains_key("age")); // Not projected
}

#[test]
fn test_batch_get_key_not_found() {
    let db = setup_db();
    create_test_table(&db, "Tbl");

    put(
        &db,
        "Tbl",
        serde_json::json!({"pk": {"S": "a"}, "sk": {"S": "1"}}),
    );

    let req: BatchGetItemRequest = serde_json::from_value(serde_json::json!({
        "RequestItems": {
            "Tbl": {
                "Keys": [
                    {"pk": {"S": "a"}, "sk": {"S": "1"}},
                    {"pk": {"S": "missing"}, "sk": {"S": "1"}}
                ]
            }
        }
    }))
    .unwrap();

    let resp = db.batch_get_item(req).unwrap();
    assert_eq!(resp.responses["Tbl"].len(), 1); // Only found item returned
}

#[test]
fn test_batch_get_exceeds_100_keys() {
    let db = setup_db();
    create_test_table(&db, "Tbl");

    let keys: Vec<serde_json::Value> = (0..101)
        .map(|i| serde_json::json!({"pk": {"S": format!("k{}", i)}, "sk": {"S": "x"}}))
        .collect();

    let req: BatchGetItemRequest = serde_json::from_value(serde_json::json!({
        "RequestItems": {
            "Tbl": {"Keys": keys}
        }
    }))
    .unwrap();

    let err = db.batch_get_item(req).unwrap_err();
    let msg = format!("{err:?}");
    assert!(
        msg.contains("Member must have length less than or equal to 100"),
        "Got: {msg}"
    );
    assert!(
        msg.contains("RequestItems.Tbl.member.Keys"),
        "Got: {msg}"
    );
}

// =============================================================================
// BatchWriteItem tests
// =============================================================================

#[test]
fn test_batch_write_puts_and_deletes() {
    let db = setup_db();
    create_test_table(&db, "Tbl");

    // Pre-existing item to delete
    put(
        &db,
        "Tbl",
        serde_json::json!({"pk": {"S": "del"}, "sk": {"S": "1"}}),
    );

    let req: BatchWriteItemRequest = serde_json::from_value(serde_json::json!({
        "RequestItems": {
            "Tbl": [
                {"PutRequest": {"Item": {"pk": {"S": "new1"}, "sk": {"S": "1"}, "val": {"S": "hello"}}}},
                {"PutRequest": {"Item": {"pk": {"S": "new2"}, "sk": {"S": "1"}, "val": {"S": "world"}}}},
                {"DeleteRequest": {"Key": {"pk": {"S": "del"}, "sk": {"S": "1"}}}}
            ]
        }
    }))
    .unwrap();

    let resp = db.batch_write_item(req).unwrap();
    assert!(resp.unprocessed_items.is_empty());

    // Verify items were created
    let scan = db
        .scan(serde_json::from_value(serde_json::json!({"TableName": "Tbl"})).unwrap())
        .unwrap();
    assert_eq!(scan.count, 2); // new1 and new2 (del was deleted)
}

#[test]
fn test_batch_write_multiple_tables() {
    let db = setup_db();
    create_test_table(&db, "TableA");
    create_test_table(&db, "TableB");

    let req: BatchWriteItemRequest = serde_json::from_value(serde_json::json!({
        "RequestItems": {
            "TableA": [
                {"PutRequest": {"Item": {"pk": {"S": "a"}, "sk": {"S": "1"}}}}
            ],
            "TableB": [
                {"PutRequest": {"Item": {"pk": {"S": "b"}, "sk": {"S": "1"}}}}
            ]
        }
    }))
    .unwrap();

    db.batch_write_item(req).unwrap();

    let scan_a = db
        .scan(serde_json::from_value(serde_json::json!({"TableName": "TableA"})).unwrap())
        .unwrap();
    let scan_b = db
        .scan(serde_json::from_value(serde_json::json!({"TableName": "TableB"})).unwrap())
        .unwrap();
    assert_eq!(scan_a.count, 1);
    assert_eq!(scan_b.count, 1);
}

#[test]
fn test_batch_write_exceeds_25_items() {
    let db = setup_db();
    create_test_table(&db, "Tbl");

    let items: Vec<serde_json::Value> = (0..26)
        .map(|i| {
            serde_json::json!({"PutRequest": {"Item": {"pk": {"S": format!("k{}", i)}, "sk": {"S": "x"}}}})
        })
        .collect();

    let req: BatchWriteItemRequest = serde_json::from_value(serde_json::json!({
        "RequestItems": {
            "Tbl": items
        }
    }))
    .unwrap();

    let err = db.batch_write_item(req).unwrap_err();
    let msg = format!("{err:?}");
    assert!(
        msg.contains("Member must have length less than or equal to 25"),
        "Got: {msg}"
    );
    assert!(msg.contains("at 'requestItems'"), "Got: {msg}");
}

#[test]
fn test_batch_write_item_too_large() {
    let db = setup_db();
    create_test_table(&db, "Tbl");

    // Create a string that exceeds 400KB
    let big_string = "x".repeat(400 * 1024 + 1);
    let req: BatchWriteItemRequest = serde_json::from_value(serde_json::json!({
        "RequestItems": {
            "Tbl": [
                {"PutRequest": {"Item": {"pk": {"S": "a"}, "sk": {"S": "1"}, "data": {"S": big_string}}}}
            ]
        }
    }))
    .unwrap();

    let err = db.batch_write_item(req).unwrap_err();
    assert!(format!("{err:?}").contains("Item size"), "Got: {err:?}");
}

#[test]
fn test_batch_write_nonexistent_table() {
    let db = setup_db();

    let req: BatchWriteItemRequest = serde_json::from_value(serde_json::json!({
        "RequestItems": {
            "NonExistent": [
                {"PutRequest": {"Item": {"pk": {"S": "a"}, "sk": {"S": "1"}}}}
            ]
        }
    }))
    .unwrap();

    let err = db.batch_write_item(req).unwrap_err();
    assert!(format!("{err:?}").contains("not found"), "Got: {err:?}");
}

#[test]
fn test_batch_get_nonexistent_table() {
    let db = setup_db();

    let req: BatchGetItemRequest = serde_json::from_value(serde_json::json!({
        "RequestItems": {
            "NonExistent": {
                "Keys": [{"pk": {"S": "a"}, "sk": {"S": "1"}}]
            }
        }
    }))
    .unwrap();

    let err = db.batch_get_item(req).unwrap_err();
    assert!(format!("{err:?}").contains("not found"), "Got: {err:?}");
}

// =============================================================================
// 16MB Batch Size Limit tests
// =============================================================================

#[test]
fn test_batch_get_returns_unprocessed_keys_over_16mb() {
    let db = setup_db();

    // Use a hash-only table for simpler key structure
    let req: CreateTableRequest = serde_json::from_value(serde_json::json!({
        "TableName": "BigTbl",
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
    }))
    .unwrap();
    db.create_table(req).unwrap();

    // Each item has ~350KB data (under 400KB single-item limit).
    // 50 items * 350KB ≈ 17.5MB > 16MB.
    let big_val = "x".repeat(350 * 1024);
    for i in 0..50 {
        let item_req: PutItemRequest = serde_json::from_value(serde_json::json!({
            "TableName": "BigTbl",
            "Item": {"pk": {"S": format!("k{i}")}, "data": {"S": big_val}}
        }))
        .unwrap();
        db.put_item(item_req).unwrap();
    }

    // Request all 50 items
    let keys: Vec<serde_json::Value> = (0..50)
        .map(|i| serde_json::json!({"pk": {"S": format!("k{i}")}}))
        .collect();

    let req: BatchGetItemRequest = serde_json::from_value(serde_json::json!({
        "RequestItems": {
            "BigTbl": {"Keys": keys}
        }
    }))
    .unwrap();

    let resp = db.batch_get_item(req).unwrap();
    // Should have returned some items but not all (16MB limit hit)
    let returned = resp.responses.get("BigTbl").map_or(0, |v| v.len());
    assert!(returned > 0, "Should have returned some items");
    assert!(
        returned < 50,
        "Should not have returned all 50 items (16MB limit), got {returned}"
    );
    assert!(
        !resp.unprocessed_keys.is_empty(),
        "Should have unprocessed keys"
    );
}

#[test]
fn test_batch_write_exceeds_16mb_aggregate() {
    let db = setup_db();

    // Use a hash-only table
    let req: CreateTableRequest = serde_json::from_value(serde_json::json!({
        "TableName": "BigTbl",
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
    }))
    .unwrap();
    db.create_table(req).unwrap();

    // 25 items (max BatchWrite count) each ~350KB = ~8.75MB (under 16MB)
    // But if we create fewer large items to push over 16MB limit...
    // Actually we're limited to 25 items max in BatchWriteItem.
    // 25 * 350KB ≈ 8.75MB which is under 16MB. So let's make each item bigger.
    // We can't exceed 400KB per item, so maximum is 25 * 400KB ≈ 10MB.
    // This means it's actually impossible to exceed 16MB with BatchWriteItem's
    // 25-item limit and 400KB per-item limit. The aggregate limit matters for
    // the raw request payload including serialization overhead.
    // For testing purposes, we'll verify that a large-but-valid batch succeeds.
    let big_val = "x".repeat(300 * 1024);
    let items: Vec<serde_json::Value> = (0..25)
        .map(|i| {
            serde_json::json!({"PutRequest": {"Item": {"pk": {"S": format!("k{i}")}, "data": {"S": big_val}}}})
        })
        .collect();

    let req: BatchWriteItemRequest = serde_json::from_value(serde_json::json!({
        "RequestItems": {
            "BigTbl": items
        }
    }))
    .unwrap();

    // Should succeed — 25 * 300KB ≈ 7.5MB, under 16MB
    db.batch_write_item(req).unwrap();
}
