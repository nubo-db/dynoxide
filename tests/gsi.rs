//! GSI (Global Secondary Index) integration tests.

use dynoxide::Database;
use dynoxide::types::AttributeValue;
use std::collections::HashMap;

/// Helper: create a table with a GSI.
/// Table: pk=UserId(S), sk=Timestamp(S)
/// GSI "StatusIndex": pk=Status(S), sk=Timestamp(S), projection=ALL
fn create_table_with_gsi(db: &Database) {
    let req: serde_json::Value = serde_json::json!({
        "TableName": "Orders",
        "KeySchema": [
            {"AttributeName": "UserId", "KeyType": "HASH"},
            {"AttributeName": "Timestamp", "KeyType": "RANGE"}
        ],
        "AttributeDefinitions": [
            {"AttributeName": "UserId", "AttributeType": "S"},
            {"AttributeName": "Timestamp", "AttributeType": "S"},
            {"AttributeName": "Status", "AttributeType": "S"}
        ],
        "GlobalSecondaryIndexes": [
            {
                "IndexName": "StatusIndex",
                "KeySchema": [
                    {"AttributeName": "Status", "KeyType": "HASH"},
                    {"AttributeName": "Timestamp", "KeyType": "RANGE"}
                ],
                "Projection": {"ProjectionType": "ALL"}
            }
        ]
    });
    let create_req = serde_json::from_value(req).unwrap();
    db.create_table(create_req).unwrap();
}

/// Helper: put an order item.
fn put_order(db: &Database, user_id: &str, timestamp: &str, status: &str, amount: &str) {
    let req: serde_json::Value = serde_json::json!({
        "TableName": "Orders",
        "Item": {
            "UserId": {"S": user_id},
            "Timestamp": {"S": timestamp},
            "Status": {"S": status},
            "Amount": {"N": amount}
        }
    });
    let put_req = serde_json::from_value(req).unwrap();
    db.put_item(put_req).unwrap();
}

#[test]
fn test_gsi_query_basic() {
    let db = Database::memory().unwrap();
    create_table_with_gsi(&db);

    put_order(&db, "user1", "2024-01-01", "SHIPPED", "100");
    put_order(&db, "user1", "2024-01-02", "PENDING", "200");
    put_order(&db, "user2", "2024-01-01", "SHIPPED", "300");

    // Query the GSI by Status
    let req: serde_json::Value = serde_json::json!({
        "TableName": "Orders",
        "IndexName": "StatusIndex",
        "KeyConditionExpression": "#st = :s",
        "ExpressionAttributeNames": {
            "#st": "Status"
        },
        "ExpressionAttributeValues": {
            ":s": {"S": "SHIPPED"}
        }
    });
    let query_req = serde_json::from_value(req).unwrap();
    let resp = db.query(query_req).unwrap();

    assert_eq!(resp.count, 2);
    let items = resp.items.unwrap();
    assert_eq!(items.len(), 2);

    // Both SHIPPED orders should be returned
    let statuses: Vec<&str> = items
        .iter()
        .filter_map(|i| match i.get("Status") {
            Some(AttributeValue::S(s)) => Some(s.as_str()),
            _ => None,
        })
        .collect();
    assert!(statuses.iter().all(|s| *s == "SHIPPED"));
}

#[test]
fn test_gsi_query_with_sk_condition() {
    let db = Database::memory().unwrap();
    create_table_with_gsi(&db);

    put_order(&db, "user1", "2024-01-01", "SHIPPED", "100");
    put_order(&db, "user1", "2024-02-01", "SHIPPED", "200");
    put_order(&db, "user2", "2024-03-01", "SHIPPED", "300");

    // Query GSI with SK range
    let req: serde_json::Value = serde_json::json!({
        "TableName": "Orders",
        "IndexName": "StatusIndex",
        "KeyConditionExpression": "#st = :s AND #ts BETWEEN :start AND :end",
        "ExpressionAttributeNames": {
            "#st": "Status",
            "#ts": "Timestamp"
        },
        "ExpressionAttributeValues": {
            ":s": {"S": "SHIPPED"},
            ":start": {"S": "2024-01-01"},
            ":end": {"S": "2024-02-28"}
        }
    });
    let query_req = serde_json::from_value(req).unwrap();
    let resp = db.query(query_req).unwrap();

    assert_eq!(resp.count, 2);
}

#[test]
fn test_gsi_query_with_filter() {
    let db = Database::memory().unwrap();
    create_table_with_gsi(&db);

    put_order(&db, "user1", "2024-01-01", "SHIPPED", "100");
    put_order(&db, "user2", "2024-01-02", "SHIPPED", "500");
    put_order(&db, "user3", "2024-01-03", "SHIPPED", "200");

    // Query GSI with filter on Amount
    let req: serde_json::Value = serde_json::json!({
        "TableName": "Orders",
        "IndexName": "StatusIndex",
        "KeyConditionExpression": "#st = :s",
        "FilterExpression": "Amount > :min",
        "ExpressionAttributeNames": {
            "#st": "Status"
        },
        "ExpressionAttributeValues": {
            ":s": {"S": "SHIPPED"},
            ":min": {"N": "150"}
        }
    });
    let query_req = serde_json::from_value(req).unwrap();
    let resp = db.query(query_req).unwrap();

    assert_eq!(resp.count, 2); // 500 and 200
    assert_eq!(resp.scanned_count, 3); // all 3 scanned
}

#[test]
fn test_gsi_scan() {
    let db = Database::memory().unwrap();
    create_table_with_gsi(&db);

    put_order(&db, "user1", "2024-01-01", "SHIPPED", "100");
    put_order(&db, "user1", "2024-01-02", "PENDING", "200");
    put_order(&db, "user2", "2024-01-01", "SHIPPED", "300");

    // Scan the GSI
    let req: serde_json::Value = serde_json::json!({
        "TableName": "Orders",
        "IndexName": "StatusIndex"
    });
    let scan_req = serde_json::from_value(req).unwrap();
    let resp = db.scan(scan_req).unwrap();

    assert_eq!(resp.count, 3);
}

#[test]
fn test_gsi_scan_with_filter() {
    let db = Database::memory().unwrap();
    create_table_with_gsi(&db);

    put_order(&db, "user1", "2024-01-01", "SHIPPED", "100");
    put_order(&db, "user1", "2024-01-02", "PENDING", "200");
    put_order(&db, "user2", "2024-01-01", "SHIPPED", "300");

    // Scan GSI with filter
    let req: serde_json::Value = serde_json::json!({
        "TableName": "Orders",
        "IndexName": "StatusIndex",
        "FilterExpression": "Amount >= :min",
        "ExpressionAttributeValues": {
            ":min": {"N": "200"}
        }
    });
    let scan_req = serde_json::from_value(req).unwrap();
    let resp = db.scan(scan_req).unwrap();

    assert_eq!(resp.count, 2);
    assert_eq!(resp.scanned_count, 3);
}

#[test]
fn test_gsi_maintained_on_update() {
    let db = Database::memory().unwrap();
    create_table_with_gsi(&db);

    put_order(&db, "user1", "2024-01-01", "PENDING", "100");

    // Verify it appears in GSI as PENDING
    let req: serde_json::Value = serde_json::json!({
        "TableName": "Orders",
        "IndexName": "StatusIndex",
        "KeyConditionExpression": "#st = :s",
        "ExpressionAttributeNames": {
            "#st": "Status"
        },
        "ExpressionAttributeValues": {
            ":s": {"S": "PENDING"}
        }
    });
    let query_req = serde_json::from_value(req).unwrap();
    let resp = db.query(query_req).unwrap();
    assert_eq!(resp.count, 1);

    // Update the status to SHIPPED
    let update_req: serde_json::Value = serde_json::json!({
        "TableName": "Orders",
        "Key": {
            "UserId": {"S": "user1"},
            "Timestamp": {"S": "2024-01-01"}
        },
        "UpdateExpression": "SET #st = :new_status",
        "ExpressionAttributeNames": {
            "#st": "Status"
        },
        "ExpressionAttributeValues": {
            ":new_status": {"S": "SHIPPED"}
        }
    });
    let update_req = serde_json::from_value(update_req).unwrap();
    db.update_item(update_req).unwrap();

    // Now PENDING should be empty
    let req: serde_json::Value = serde_json::json!({
        "TableName": "Orders",
        "IndexName": "StatusIndex",
        "KeyConditionExpression": "#st = :s",
        "ExpressionAttributeNames": {
            "#st": "Status"
        },
        "ExpressionAttributeValues": {
            ":s": {"S": "PENDING"}
        }
    });
    let query_req = serde_json::from_value(req).unwrap();
    let resp = db.query(query_req).unwrap();
    assert_eq!(resp.count, 0);

    // And SHIPPED should have the item
    let req: serde_json::Value = serde_json::json!({
        "TableName": "Orders",
        "IndexName": "StatusIndex",
        "KeyConditionExpression": "#st = :s",
        "ExpressionAttributeNames": {
            "#st": "Status"
        },
        "ExpressionAttributeValues": {
            ":s": {"S": "SHIPPED"}
        }
    });
    let query_req = serde_json::from_value(req).unwrap();
    let resp = db.query(query_req).unwrap();
    assert_eq!(resp.count, 1);
    let items = resp.items.unwrap();
    let amount = &items[0]["Amount"];
    assert!(matches!(amount, AttributeValue::N(n) if n == "100"));
}

#[test]
fn test_gsi_maintained_on_delete() {
    let db = Database::memory().unwrap();
    create_table_with_gsi(&db);

    put_order(&db, "user1", "2024-01-01", "SHIPPED", "100");
    put_order(&db, "user2", "2024-01-02", "SHIPPED", "200");

    // Verify both in GSI
    let req: serde_json::Value = serde_json::json!({
        "TableName": "Orders",
        "IndexName": "StatusIndex",
        "KeyConditionExpression": "#st = :s",
        "ExpressionAttributeNames": {
            "#st": "Status"
        },
        "ExpressionAttributeValues": {
            ":s": {"S": "SHIPPED"}
        }
    });
    let query_req = serde_json::from_value(req).unwrap();
    let resp = db.query(query_req).unwrap();
    assert_eq!(resp.count, 2);

    // Delete one
    let del_req: serde_json::Value = serde_json::json!({
        "TableName": "Orders",
        "Key": {
            "UserId": {"S": "user1"},
            "Timestamp": {"S": "2024-01-01"}
        }
    });
    let del_req = serde_json::from_value(del_req).unwrap();
    db.delete_item(del_req).unwrap();

    // GSI should now have only 1
    let req: serde_json::Value = serde_json::json!({
        "TableName": "Orders",
        "IndexName": "StatusIndex",
        "KeyConditionExpression": "#st = :s",
        "ExpressionAttributeNames": {
            "#st": "Status"
        },
        "ExpressionAttributeValues": {
            ":s": {"S": "SHIPPED"}
        }
    });
    let query_req = serde_json::from_value(req).unwrap();
    let resp = db.query(query_req).unwrap();
    assert_eq!(resp.count, 1);
}

#[test]
fn test_gsi_maintained_on_batch_write() {
    let db = Database::memory().unwrap();
    create_table_with_gsi(&db);

    // Batch write 3 items
    let req: serde_json::Value = serde_json::json!({
        "RequestItems": {
            "Orders": [
                {"PutRequest": {"Item": {"UserId": {"S": "u1"}, "Timestamp": {"S": "t1"}, "Status": {"S": "SHIPPED"}, "Amount": {"N": "10"}}}},
                {"PutRequest": {"Item": {"UserId": {"S": "u2"}, "Timestamp": {"S": "t2"}, "Status": {"S": "PENDING"}, "Amount": {"N": "20"}}}},
                {"PutRequest": {"Item": {"UserId": {"S": "u3"}, "Timestamp": {"S": "t3"}, "Status": {"S": "SHIPPED"}, "Amount": {"N": "30"}}}}
            ]
        }
    });
    let batch_req = serde_json::from_value(req).unwrap();
    db.batch_write_item(batch_req).unwrap();

    // Query GSI for SHIPPED - should find 2
    let req: serde_json::Value = serde_json::json!({
        "TableName": "Orders",
        "IndexName": "StatusIndex",
        "KeyConditionExpression": "#st = :s",
        "ExpressionAttributeNames": {
            "#st": "Status"
        },
        "ExpressionAttributeValues": {
            ":s": {"S": "SHIPPED"}
        }
    });
    let query_req = serde_json::from_value(req).unwrap();
    let resp = db.query(query_req).unwrap();
    assert_eq!(resp.count, 2);

    // Batch delete one SHIPPED item
    let req: serde_json::Value = serde_json::json!({
        "RequestItems": {
            "Orders": [
                {"DeleteRequest": {"Key": {"UserId": {"S": "u1"}, "Timestamp": {"S": "t1"}}}}
            ]
        }
    });
    let batch_req = serde_json::from_value(req).unwrap();
    db.batch_write_item(batch_req).unwrap();

    // Now only 1 SHIPPED
    let req: serde_json::Value = serde_json::json!({
        "TableName": "Orders",
        "IndexName": "StatusIndex",
        "KeyConditionExpression": "#st = :s",
        "ExpressionAttributeNames": {
            "#st": "Status"
        },
        "ExpressionAttributeValues": {
            ":s": {"S": "SHIPPED"}
        }
    });
    let query_req = serde_json::from_value(req).unwrap();
    let resp = db.query(query_req).unwrap();
    assert_eq!(resp.count, 1);
}

#[test]
fn test_gsi_query_nonexistent_index() {
    let db = Database::memory().unwrap();
    create_table_with_gsi(&db);

    let req: serde_json::Value = serde_json::json!({
        "TableName": "Orders",
        "IndexName": "NonExistentIndex",
        "KeyConditionExpression": "#st = :s",
        "ExpressionAttributeNames": {
            "#st": "Status"
        },
        "ExpressionAttributeValues": {
            ":s": {"S": "SHIPPED"}
        }
    });
    let query_req = serde_json::from_value(req).unwrap();
    let err = db.query(query_req).unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("does not have the specified index"));
}

#[test]
fn test_gsi_keys_only_projection() {
    let db = Database::memory().unwrap();

    // Create table with KEYS_ONLY GSI
    let req: serde_json::Value = serde_json::json!({
        "TableName": "Items",
        "KeySchema": [
            {"AttributeName": "PK", "KeyType": "HASH"},
            {"AttributeName": "SK", "KeyType": "RANGE"}
        ],
        "AttributeDefinitions": [
            {"AttributeName": "PK", "AttributeType": "S"},
            {"AttributeName": "SK", "AttributeType": "S"},
            {"AttributeName": "GSI_PK", "AttributeType": "S"}
        ],
        "GlobalSecondaryIndexes": [
            {
                "IndexName": "KeysOnlyIndex",
                "KeySchema": [
                    {"AttributeName": "GSI_PK", "KeyType": "HASH"}
                ],
                "Projection": {"ProjectionType": "KEYS_ONLY"}
            }
        ]
    });
    let create_req = serde_json::from_value(req).unwrap();
    db.create_table(create_req).unwrap();

    // Put an item with extra attributes
    let req: serde_json::Value = serde_json::json!({
        "TableName": "Items",
        "Item": {
            "PK": {"S": "pk1"},
            "SK": {"S": "sk1"},
            "GSI_PK": {"S": "gsi_val"},
            "ExtraAttr": {"S": "should_not_appear"},
            "AnotherAttr": {"N": "42"}
        }
    });
    let put_req = serde_json::from_value(req).unwrap();
    db.put_item(put_req).unwrap();

    // Query via GSI - should only have key attrs
    let req: serde_json::Value = serde_json::json!({
        "TableName": "Items",
        "IndexName": "KeysOnlyIndex",
        "KeyConditionExpression": "GSI_PK = :v",
        "ExpressionAttributeValues": {
            ":v": {"S": "gsi_val"}
        }
    });
    let query_req = serde_json::from_value(req).unwrap();
    let resp = db.query(query_req).unwrap();

    assert_eq!(resp.count, 1);
    let item = &resp.items.unwrap()[0];
    // Should have table keys (PK, SK) and GSI key (GSI_PK) but NOT ExtraAttr/AnotherAttr
    assert!(item.contains_key("PK"));
    assert!(item.contains_key("SK"));
    assert!(item.contains_key("GSI_PK"));
    assert!(!item.contains_key("ExtraAttr"));
    assert!(!item.contains_key("AnotherAttr"));
}

#[test]
fn test_gsi_include_projection() {
    let db = Database::memory().unwrap();

    // Create table with INCLUDE GSI
    let req: serde_json::Value = serde_json::json!({
        "TableName": "Items",
        "KeySchema": [
            {"AttributeName": "PK", "KeyType": "HASH"},
            {"AttributeName": "SK", "KeyType": "RANGE"}
        ],
        "AttributeDefinitions": [
            {"AttributeName": "PK", "AttributeType": "S"},
            {"AttributeName": "SK", "AttributeType": "S"},
            {"AttributeName": "GSI_PK", "AttributeType": "S"}
        ],
        "GlobalSecondaryIndexes": [
            {
                "IndexName": "IncludeIndex",
                "KeySchema": [
                    {"AttributeName": "GSI_PK", "KeyType": "HASH"}
                ],
                "Projection": {
                    "ProjectionType": "INCLUDE",
                    "NonKeyAttributes": ["IncludedAttr"]
                }
            }
        ]
    });
    let create_req = serde_json::from_value(req).unwrap();
    db.create_table(create_req).unwrap();

    // Put an item
    let req: serde_json::Value = serde_json::json!({
        "TableName": "Items",
        "Item": {
            "PK": {"S": "pk1"},
            "SK": {"S": "sk1"},
            "GSI_PK": {"S": "gsi_val"},
            "IncludedAttr": {"S": "included_value"},
            "ExcludedAttr": {"S": "excluded_value"}
        }
    });
    let put_req = serde_json::from_value(req).unwrap();
    db.put_item(put_req).unwrap();

    // Query via GSI
    let req: serde_json::Value = serde_json::json!({
        "TableName": "Items",
        "IndexName": "IncludeIndex",
        "KeyConditionExpression": "GSI_PK = :v",
        "ExpressionAttributeValues": {
            ":v": {"S": "gsi_val"}
        }
    });
    let query_req = serde_json::from_value(req).unwrap();
    let resp = db.query(query_req).unwrap();

    assert_eq!(resp.count, 1);
    let item = &resp.items.unwrap()[0];
    // Should have keys + IncludedAttr, but NOT ExcludedAttr
    assert!(item.contains_key("PK"));
    assert!(item.contains_key("SK"));
    assert!(item.contains_key("GSI_PK"));
    assert!(item.contains_key("IncludedAttr"));
    assert!(!item.contains_key("ExcludedAttr"));
}

#[test]
fn test_gsi_item_without_gsi_key_not_projected() {
    let db = Database::memory().unwrap();
    create_table_with_gsi(&db);

    // Put an item WITHOUT the GSI pk attribute (Status)
    let req: serde_json::Value = serde_json::json!({
        "TableName": "Orders",
        "Item": {
            "UserId": {"S": "user1"},
            "Timestamp": {"S": "2024-01-01"},
            "Amount": {"N": "100"}
        }
    });
    let put_req = serde_json::from_value(req).unwrap();
    db.put_item(put_req).unwrap();

    // Scan GSI - should be empty (item has no Status attribute)
    let req: serde_json::Value = serde_json::json!({
        "TableName": "Orders",
        "IndexName": "StatusIndex"
    });
    let scan_req = serde_json::from_value(req).unwrap();
    let resp = db.scan(scan_req).unwrap();
    assert_eq!(resp.count, 0);
}

/// Sparse GSI: an item missing the GSI sort key (when the index defines one)
/// must be excluded from the index entirely, the same way a missing partition
/// key excludes it. The GSI sort key here is a non-key attribute, so it can be
/// legitimately absent on some items. See issue #91.
#[test]
fn test_gsi_scan_excludes_item_missing_sort_key() {
    let db = Database::memory().unwrap();

    let req: serde_json::Value = serde_json::json!({
        "TableName": "table",
        "KeySchema": [
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"}
        ],
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "sparse_attribute", "AttributeType": "S"}
        ],
        "GlobalSecondaryIndexes": [
            {
                "IndexName": "sparse_index",
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "sparse_attribute", "KeyType": "RANGE"}
                ],
                "Projection": {"ProjectionType": "ALL"}
            }
        ]
    });
    db.create_table(serde_json::from_value(req).unwrap())
        .unwrap();

    // Item missing the GSI sort key (sparse_attribute) -- excluded from the index.
    let req: serde_json::Value = serde_json::json!({
        "TableName": "table",
        "Item": {"pk": {"S": "partition1"}, "sk": {"S": "sort1"}}
    });
    db.put_item(serde_json::from_value(req).unwrap()).unwrap();

    // Item with the GSI sort key -- included in the index.
    let req: serde_json::Value = serde_json::json!({
        "TableName": "table",
        "Item": {"pk": {"S": "partition2"}, "sk": {"S": "sort2"}, "sparse_attribute": {"S": "hello"}}
    });
    db.put_item(serde_json::from_value(req).unwrap()).unwrap();

    let req: serde_json::Value = serde_json::json!({
        "TableName": "table",
        "IndexName": "sparse_index"
    });
    let resp = db.scan(serde_json::from_value(req).unwrap()).unwrap();

    assert_eq!(
        resp.count, 1,
        "only the item with the GSI sort key belongs in the index"
    );
    assert_eq!(
        resp.scanned_count, 1,
        "the item missing the GSI sort key must not be scanned"
    );
}

/// Table with a composite GSI whose sort key is a non-key attribute, so it can
/// legitimately be absent on some items. Used by the sparse-index tests below.
fn create_sparse_gsi_table(db: &Database) {
    let req: serde_json::Value = serde_json::json!({
        "TableName": "table",
        "KeySchema": [
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"}
        ],
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "sparse_attribute", "AttributeType": "S"}
        ],
        "GlobalSecondaryIndexes": [
            {
                "IndexName": "sparse_index",
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "sparse_attribute", "KeyType": "RANGE"}
                ],
                "Projection": {"ProjectionType": "ALL"}
            }
        ]
    });
    db.create_table(serde_json::from_value(req).unwrap())
        .unwrap();
}

fn put_json(db: &Database, item: serde_json::Value) {
    let req: serde_json::Value = serde_json::json!({"TableName": "table", "Item": item});
    db.put_item(serde_json::from_value(req).unwrap()).unwrap();
}

fn scan_sparse_index(db: &Database) -> dynoxide::actions::scan::ScanResponse {
    let req: serde_json::Value =
        serde_json::json!({"TableName": "table", "IndexName": "sparse_index"});
    db.scan(serde_json::from_value(req).unwrap()).unwrap()
}

/// Query on the index excludes a sort-key-less item from its partition too.
#[test]
fn test_gsi_query_excludes_item_missing_sort_key() {
    let db = Database::memory().unwrap();
    create_sparse_gsi_table(&db);
    put_json(
        &db,
        serde_json::json!({"pk": {"S": "p1"}, "sk": {"S": "s1"}}),
    );
    put_json(
        &db,
        serde_json::json!({"pk": {"S": "p2"}, "sk": {"S": "s2"}, "sparse_attribute": {"S": "hi"}}),
    );

    let req: serde_json::Value = serde_json::json!({
        "TableName": "table",
        "IndexName": "sparse_index",
        "KeyConditionExpression": "pk = :p",
        "ExpressionAttributeValues": {":p": {"S": "p1"}}
    });
    let resp = db.query(serde_json::from_value(req).unwrap()).unwrap();
    assert_eq!(
        resp.count, 0,
        "sort-key-less item must not appear in an index query"
    );
}

/// A hash-only GSI still includes an item that has the GSI partition key, even
/// with no sort key defined. Guards the helper from over-excluding.
#[test]
fn test_gsi_hash_only_index_includes_item() {
    let db = Database::memory().unwrap();
    let req: serde_json::Value = serde_json::json!({
        "TableName": "table",
        "KeySchema": [
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"}
        ],
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "gsi_pk", "AttributeType": "S"}
        ],
        "GlobalSecondaryIndexes": [{
            "IndexName": "hash_only",
            "KeySchema": [{"AttributeName": "gsi_pk", "KeyType": "HASH"}],
            "Projection": {"ProjectionType": "ALL"}
        }]
    });
    db.create_table(serde_json::from_value(req).unwrap())
        .unwrap();
    put_json(
        &db,
        serde_json::json!({"pk": {"S": "p1"}, "sk": {"S": "s1"}, "gsi_pk": {"S": "g1"}}),
    );

    let req: serde_json::Value =
        serde_json::json!({"TableName": "table", "IndexName": "hash_only"});
    let resp = db.scan(serde_json::from_value(req).unwrap()).unwrap();
    assert_eq!(resp.count, 1);
}

/// Overwriting an indexed item with a sort-key-less version evicts it from the
/// index (the unconditional delete fires; the gated insert is skipped).
#[test]
fn test_gsi_overwrite_with_missing_sort_key_evicts() {
    let db = Database::memory().unwrap();
    create_sparse_gsi_table(&db);
    put_json(
        &db,
        serde_json::json!({"pk": {"S": "p1"}, "sk": {"S": "s1"}, "sparse_attribute": {"S": "hi"}}),
    );
    assert_eq!(scan_sparse_index(&db).count, 1);

    put_json(
        &db,
        serde_json::json!({"pk": {"S": "p1"}, "sk": {"S": "s1"}}),
    );
    assert_eq!(
        scan_sparse_index(&db).count,
        0,
        "overwrite without the sort key must evict the stale index entry"
    );
}

/// REMOVE-ing the sort key via UpdateItem evicts the item from the index.
#[test]
fn test_gsi_update_remove_sort_key_evicts() {
    let db = Database::memory().unwrap();
    create_sparse_gsi_table(&db);
    put_json(
        &db,
        serde_json::json!({"pk": {"S": "p1"}, "sk": {"S": "s1"}, "sparse_attribute": {"S": "hi"}}),
    );
    assert_eq!(scan_sparse_index(&db).count, 1);

    let req: serde_json::Value = serde_json::json!({
        "TableName": "table",
        "Key": {"pk": {"S": "p1"}, "sk": {"S": "s1"}},
        "UpdateExpression": "REMOVE sparse_attribute"
    });
    db.update_item(serde_json::from_value(req).unwrap())
        .unwrap();
    assert_eq!(scan_sparse_index(&db).count, 0);
}

/// A present-but-non-scalar sort key cannot form a key, so the item is excluded
/// rather than indexed at a phantom empty-string position.
#[test]
fn test_gsi_non_scalar_sort_key_excluded() {
    let db = Database::memory().unwrap();
    create_sparse_gsi_table(&db);
    put_json(
        &db,
        serde_json::json!({
            "pk": {"S": "p1"}, "sk": {"S": "s1"},
            "sparse_attribute": {"L": [{"S": "not a scalar key"}]}
        }),
    );
    assert_eq!(scan_sparse_index(&db).count, 0);
}

/// Membership is decided per index: an item can qualify for one GSI and not
/// another, and a GSI keyed on a non-key attribute excludes items lacking it.
#[test]
fn test_gsi_membership_is_per_index() {
    let db = Database::memory().unwrap();
    let req: serde_json::Value = serde_json::json!({
        "TableName": "table",
        "KeySchema": [
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"}
        ],
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "attr_a", "AttributeType": "S"},
            {"AttributeName": "attr_b", "AttributeType": "S"}
        ],
        "GlobalSecondaryIndexes": [
            {
                "IndexName": "index_a",
                "KeySchema": [{"AttributeName": "attr_a", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"}
            },
            {
                "IndexName": "index_b",
                "KeySchema": [{"AttributeName": "attr_b", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"}
            }
        ]
    });
    db.create_table(serde_json::from_value(req).unwrap())
        .unwrap();
    // Item carries attr_a but not attr_b.
    put_json(
        &db,
        serde_json::json!({"pk": {"S": "p1"}, "sk": {"S": "s1"}, "attr_a": {"S": "a"}}),
    );

    let scan = |index: &str| {
        let req: serde_json::Value = serde_json::json!({"TableName": "table", "IndexName": index});
        db.scan(serde_json::from_value(req).unwrap()).unwrap().count
    };
    assert_eq!(scan("index_a"), 1, "item has attr_a, belongs in index_a");
    assert_eq!(
        scan("index_b"),
        0,
        "item lacks attr_b, excluded from index_b"
    );
}

/// Projection type is orthogonal to membership: a KEYS_ONLY index still excludes
/// a sort-key-less item.
#[test]
fn test_gsi_keys_only_excludes_item_missing_sort_key() {
    let db = Database::memory().unwrap();
    let req: serde_json::Value = serde_json::json!({
        "TableName": "table",
        "KeySchema": [
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"}
        ],
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "sparse_attribute", "AttributeType": "S"}
        ],
        "GlobalSecondaryIndexes": [{
            "IndexName": "sparse_index",
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sparse_attribute", "KeyType": "RANGE"}
            ],
            "Projection": {"ProjectionType": "KEYS_ONLY"}
        }]
    });
    db.create_table(serde_json::from_value(req).unwrap())
        .unwrap();
    put_json(
        &db,
        serde_json::json!({"pk": {"S": "p1"}, "sk": {"S": "s1"}}),
    );
    put_json(
        &db,
        serde_json::json!({"pk": {"S": "p2"}, "sk": {"S": "s2"}, "sparse_attribute": {"S": "hi"}}),
    );
    assert_eq!(scan_sparse_index(&db).count, 1);
}

/// A numeric (N) GSI sort key forms a valid key, so sparse membership works the
/// same as for string keys: the item with the key is included, the one without
/// is excluded.
#[test]
fn test_gsi_numeric_sort_key_sparse_membership() {
    let db = Database::memory().unwrap();
    let req: serde_json::Value = serde_json::json!({
        "TableName": "table",
        "KeySchema": [
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"}
        ],
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "score", "AttributeType": "N"}
        ],
        "GlobalSecondaryIndexes": [{
            "IndexName": "sparse_index",
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "score", "KeyType": "RANGE"}
            ],
            "Projection": {"ProjectionType": "ALL"}
        }]
    });
    db.create_table(serde_json::from_value(req).unwrap())
        .unwrap();
    put_json(
        &db,
        serde_json::json!({"pk": {"S": "p1"}, "sk": {"S": "s1"}}),
    );
    put_json(
        &db,
        serde_json::json!({"pk": {"S": "p2"}, "sk": {"S": "s2"}, "score": {"N": "42"}}),
    );
    assert_eq!(scan_sparse_index(&db).count, 1);
}

#[test]
fn test_gsi_overwrite_updates_index() {
    let db = Database::memory().unwrap();
    create_table_with_gsi(&db);

    // Put item with PENDING status
    put_order(&db, "user1", "2024-01-01", "PENDING", "100");

    // Overwrite with SHIPPED status (same pk/sk)
    put_order(&db, "user1", "2024-01-01", "SHIPPED", "100");

    // PENDING should have 0 items
    let req: serde_json::Value = serde_json::json!({
        "TableName": "Orders",
        "IndexName": "StatusIndex",
        "KeyConditionExpression": "#st = :s",
        "ExpressionAttributeNames": {
            "#st": "Status"
        },
        "ExpressionAttributeValues": {
            ":s": {"S": "PENDING"}
        }
    });
    let query_req = serde_json::from_value(req).unwrap();
    let resp = db.query(query_req).unwrap();
    assert_eq!(resp.count, 0);

    // SHIPPED should have 1
    let req: serde_json::Value = serde_json::json!({
        "TableName": "Orders",
        "IndexName": "StatusIndex",
        "KeyConditionExpression": "#st = :s",
        "ExpressionAttributeNames": {
            "#st": "Status"
        },
        "ExpressionAttributeValues": {
            ":s": {"S": "SHIPPED"}
        }
    });
    let query_req = serde_json::from_value(req).unwrap();
    let resp = db.query(query_req).unwrap();
    assert_eq!(resp.count, 1);
}

#[test]
fn test_gsi_sort_order() {
    let db = Database::memory().unwrap();
    create_table_with_gsi(&db);

    put_order(&db, "user1", "2024-03-01", "SHIPPED", "300");
    put_order(&db, "user2", "2024-01-01", "SHIPPED", "100");
    put_order(&db, "user3", "2024-02-01", "SHIPPED", "200");

    // Query GSI ascending (default)
    let req: serde_json::Value = serde_json::json!({
        "TableName": "Orders",
        "IndexName": "StatusIndex",
        "KeyConditionExpression": "#st = :s",
        "ExpressionAttributeNames": {
            "#st": "Status"
        },
        "ExpressionAttributeValues": {
            ":s": {"S": "SHIPPED"}
        }
    });
    let query_req = serde_json::from_value(req).unwrap();
    let resp = db.query(query_req).unwrap();
    let items = resp.items.unwrap();
    let timestamps: Vec<&str> = items
        .iter()
        .filter_map(|i| match i.get("Timestamp") {
            Some(AttributeValue::S(s)) => Some(s.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(timestamps, vec!["2024-01-01", "2024-02-01", "2024-03-01"]);

    // Query GSI descending
    let req: serde_json::Value = serde_json::json!({
        "TableName": "Orders",
        "IndexName": "StatusIndex",
        "KeyConditionExpression": "#st = :s",
        "ScanIndexForward": false,
        "ExpressionAttributeNames": {
            "#st": "Status"
        },
        "ExpressionAttributeValues": {
            ":s": {"S": "SHIPPED"}
        }
    });
    let query_req = serde_json::from_value(req).unwrap();
    let resp = db.query(query_req).unwrap();
    let items = resp.items.unwrap();
    let timestamps: Vec<&str> = items
        .iter()
        .filter_map(|i| match i.get("Timestamp") {
            Some(AttributeValue::S(s)) => Some(s.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(timestamps, vec!["2024-03-01", "2024-02-01", "2024-01-01"]);
}

// ---------------------------------------------------------------------------
// Bug: GSI scan pagination breaks after the first page
// ---------------------------------------------------------------------------

/// Paginated scan on a GSI should return all items across multiple pages.
///
/// Root cause: `scan_gsi_items` used `(gsi_pk, gsi_sk) > (?, ?)` for the
/// cursor, but the GSI table's primary key is `(gsi_pk, gsi_sk, table_pk,
/// table_sk)`. When multiple items share the same GSI key, the 2-column
/// cursor skips all remaining rows with that key on the second page.
#[test]
fn test_gsi_scan_pagination_returns_all_items() {
    let db = Database::memory().unwrap();

    // Table: pk=ID(S), GSI1: pk=Type(S) sk=ID(S), projection=ALL
    let req: serde_json::Value = serde_json::json!({
        "TableName": "Items",
        "KeySchema": [{"AttributeName": "ID", "KeyType": "HASH"}],
        "AttributeDefinitions": [
            {"AttributeName": "ID", "AttributeType": "S"},
            {"AttributeName": "Type", "AttributeType": "S"}
        ],
        "GlobalSecondaryIndexes": [{
            "IndexName": "TypeIndex",
            "KeySchema": [
                {"AttributeName": "Type", "KeyType": "HASH"},
                {"AttributeName": "ID", "KeyType": "RANGE"}
            ],
            "Projection": {"ProjectionType": "ALL"}
        }]
    });
    db.create_table(serde_json::from_value(req).unwrap())
        .unwrap();

    // Insert 50 items all with the same GSI PK ("widget")
    for i in 0..50 {
        let req: serde_json::Value = serde_json::json!({
            "TableName": "Items",
            "Item": {
                "ID": {"S": format!("item-{:03}", i)},
                "Type": {"S": "widget"},
                "Data": {"S": format!("payload-{}", i)}
            }
        });
        db.put_item(serde_json::from_value(req).unwrap()).unwrap();
    }

    // Paginated scan on GSI with limit=10, should get all 50 items across 5 pages
    let mut all_items = Vec::new();
    let mut exclusive_start_key: Option<std::collections::HashMap<String, AttributeValue>> = None;
    let mut pages = 0;

    loop {
        let mut req: serde_json::Value = serde_json::json!({
            "TableName": "Items",
            "IndexName": "TypeIndex",
            "Limit": 10
        });
        if let Some(ref esk) = exclusive_start_key {
            req["ExclusiveStartKey"] = serde_json::to_value(esk).unwrap();
        }

        let resp = db.scan(serde_json::from_value(req).unwrap()).unwrap();
        pages += 1;

        if let Some(items) = &resp.items {
            all_items.extend(items.clone());
        }

        match resp.last_evaluated_key {
            Some(lek) => exclusive_start_key = Some(lek),
            None => break,
        }

        assert!(pages <= 10, "too many pages — pagination may be looping");
    }

    assert_eq!(
        all_items.len(),
        50,
        "expected all 50 items across paginated GSI scan, got {}",
        all_items.len()
    );
}

/// Paginated scan on a GSI with a filter expression should still paginate correctly.
#[test]
fn test_gsi_scan_pagination_with_filter() {
    let db = Database::memory().unwrap();

    let req: serde_json::Value = serde_json::json!({
        "TableName": "Filtered",
        "KeySchema": [{"AttributeName": "ID", "KeyType": "HASH"}],
        "AttributeDefinitions": [
            {"AttributeName": "ID", "AttributeType": "S"},
            {"AttributeName": "Type", "AttributeType": "S"}
        ],
        "GlobalSecondaryIndexes": [{
            "IndexName": "TypeIndex",
            "KeySchema": [
                {"AttributeName": "Type", "KeyType": "HASH"},
                {"AttributeName": "ID", "KeyType": "RANGE"}
            ],
            "Projection": {"ProjectionType": "ALL"}
        }]
    });
    db.create_table(serde_json::from_value(req).unwrap())
        .unwrap();

    // Insert 100 items, every 5th one is "special"
    for i in 0..100 {
        let req: serde_json::Value = serde_json::json!({
            "TableName": "Filtered",
            "Item": {
                "ID": {"S": format!("item-{:03}", i)},
                "Type": {"S": "widget"},
                "Special": {"BOOL": i % 5 == 0}
            }
        });
        db.put_item(serde_json::from_value(req).unwrap()).unwrap();
    }

    // Paginated scan on GSI with filter, limit=10 per page
    let mut all_items = Vec::new();
    let mut exclusive_start_key: Option<std::collections::HashMap<String, AttributeValue>> = None;
    let mut pages = 0;

    loop {
        let mut req: serde_json::Value = serde_json::json!({
            "TableName": "Filtered",
            "IndexName": "TypeIndex",
            "Limit": 10,
            "FilterExpression": "Special = :t",
            "ExpressionAttributeValues": {":t": {"BOOL": true}}
        });
        if let Some(ref esk) = exclusive_start_key {
            req["ExclusiveStartKey"] = serde_json::to_value(esk).unwrap();
        }

        let resp = db.scan(serde_json::from_value(req).unwrap()).unwrap();
        pages += 1;

        if let Some(items) = &resp.items {
            all_items.extend(items.clone());
        }

        match resp.last_evaluated_key {
            Some(lek) => exclusive_start_key = Some(lek),
            None => break,
        }

        assert!(pages <= 20, "too many pages — pagination may be looping");
    }

    assert_eq!(
        all_items.len(),
        20,
        "expected 20 special items out of 100, got {}",
        all_items.len()
    );
}

/// Regression (#38): paginating a Scan over a GSI whose items share the same
/// index partition + sort key must visit every tied item exactly once. This
/// uses a **hash-only base table**, which is the case that actually breaks:
/// with no base sort key, the GSI rows carry the empty-string default in their
/// table_sk column, so the cursor must still disambiguate tied index keys by
/// the base partition key. The earlier defect collapsed the cursor to
/// `(gsi_pk, gsi_sk)` and stopped after the first page.
#[test]
fn test_scan_gsi_pagination_visits_all_tied_sort_keys() {
    let db = Database::memory().unwrap();

    // Hash-only base table (ID); GSI TieIndex (GType hash, GSort range).
    let create_req = serde_json::json!({
        "TableName": "TieScan",
        "KeySchema": [{"AttributeName": "ID", "KeyType": "HASH"}],
        "AttributeDefinitions": [
            {"AttributeName": "ID", "AttributeType": "S"},
            {"AttributeName": "GType", "AttributeType": "S"},
            {"AttributeName": "GSort", "AttributeType": "S"}
        ],
        "GlobalSecondaryIndexes": [{
            "IndexName": "TieIndex",
            "KeySchema": [
                {"AttributeName": "GType", "KeyType": "HASH"},
                {"AttributeName": "GSort", "KeyType": "RANGE"}
            ],
            "Projection": {"ProjectionType": "ALL"}
        }],
        "BillingMode": "PAY_PER_REQUEST"
    });
    db.create_table(serde_json::from_value(create_req).unwrap())
        .unwrap();

    // All five items share the GSI key (GType="tie", GSort="same") and differ
    // only by the base partition key (ID).
    let ids = ["id-0", "id-1", "id-2", "id-3", "id-4"];
    for id in ids {
        let put = serde_json::json!({
            "TableName": "TieScan",
            "Item": {
                "ID": {"S": id},
                "GType": {"S": "tie"},
                "GSort": {"S": "same"}
            }
        });
        db.put_item(serde_json::from_value(put).unwrap()).unwrap();
    }

    // Page through the GSI one item at a time, following LastEvaluatedKey.
    let mut seen: Vec<String> = Vec::new();
    let mut exclusive_start_key: Option<HashMap<String, AttributeValue>> = None;
    let mut pages = 0;
    loop {
        pages += 1;
        assert!(
            pages <= ids.len() + 1,
            "paged GSI scan did not terminate (looping or stalling on tied keys)"
        );

        let mut req = serde_json::json!({
            "TableName": "TieScan",
            "IndexName": "TieIndex",
            "Limit": 1
        });
        if let Some(ref lek) = exclusive_start_key {
            req["ExclusiveStartKey"] = serde_json::to_value(lek).unwrap();
        }
        let resp = db.scan(serde_json::from_value(req).unwrap()).unwrap();

        if let Some(items) = resp.items {
            for item in items {
                if let Some(AttributeValue::S(id)) = item.get("ID") {
                    seen.push(id.clone());
                }
            }
        }

        match resp.last_evaluated_key {
            Some(lek) => exclusive_start_key = Some(lek),
            None => break,
        }
    }

    seen.sort();
    assert_eq!(
        seen,
        vec!["id-0", "id-1", "id-2", "id-3", "id-4"],
        "every tied GSI item should be visited exactly once across the paged scan"
    );
}

/// Query counterpart of #38 (issue #52): paging a GSI Query over a hash-only
/// base table must visit every tied item, not stall after the first. The Scan
/// path was fixed in #47; the Query path carried the same cursor defect.
#[test]
fn test_query_gsi_pagination_visits_all_tied_sort_keys() {
    let db = Database::memory().unwrap();

    // Hash-only base table (ID); GSI TieIndex (GType hash, GSort range).
    let create_req = serde_json::json!({
        "TableName": "TieQuery",
        "KeySchema": [{"AttributeName": "ID", "KeyType": "HASH"}],
        "AttributeDefinitions": [
            {"AttributeName": "ID", "AttributeType": "S"},
            {"AttributeName": "GType", "AttributeType": "S"},
            {"AttributeName": "GSort", "AttributeType": "S"}
        ],
        "GlobalSecondaryIndexes": [{
            "IndexName": "TieIndex",
            "KeySchema": [
                {"AttributeName": "GType", "KeyType": "HASH"},
                {"AttributeName": "GSort", "KeyType": "RANGE"}
            ],
            "Projection": {"ProjectionType": "ALL"}
        }],
        "BillingMode": "PAY_PER_REQUEST"
    });
    db.create_table(serde_json::from_value(create_req).unwrap())
        .unwrap();

    // All five items share the GSI key (GType="tie", GSort="same") and differ
    // only by the base partition key (ID).
    let ids = ["id-0", "id-1", "id-2", "id-3", "id-4"];
    for id in ids {
        let put = serde_json::json!({
            "TableName": "TieQuery",
            "Item": {
                "ID": {"S": id},
                "GType": {"S": "tie"},
                "GSort": {"S": "same"}
            }
        });
        db.put_item(serde_json::from_value(put).unwrap()).unwrap();
    }

    // Page through the GSI one item at a time, following LastEvaluatedKey.
    let mut seen: Vec<String> = Vec::new();
    let mut exclusive_start_key: Option<HashMap<String, AttributeValue>> = None;
    let mut pages = 0;
    loop {
        pages += 1;
        assert!(
            pages <= ids.len() + 1,
            "paged GSI query did not terminate (looping or stalling on tied keys)"
        );

        let mut req = serde_json::json!({
            "TableName": "TieQuery",
            "IndexName": "TieIndex",
            "KeyConditionExpression": "GType = :t",
            "ExpressionAttributeValues": {":t": {"S": "tie"}},
            "Limit": 1
        });
        if let Some(ref lek) = exclusive_start_key {
            req["ExclusiveStartKey"] = serde_json::to_value(lek).unwrap();
        }
        let resp = db.query(serde_json::from_value(req).unwrap()).unwrap();

        if let Some(items) = resp.items {
            for item in items {
                if let Some(AttributeValue::S(id)) = item.get("ID") {
                    seen.push(id.clone());
                }
            }
        }

        match resp.last_evaluated_key {
            Some(lek) => exclusive_start_key = Some(lek),
            None => break,
        }
    }

    seen.sort();
    assert_eq!(
        seen,
        vec!["id-0", "id-1", "id-2", "id-3", "id-4"],
        "every tied GSI item should be visited exactly once across the paged query"
    );
}
