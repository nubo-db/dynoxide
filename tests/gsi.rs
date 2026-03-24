//! GSI (Global Secondary Index) integration tests.

use dynoxide::Database;
use dynoxide::types::AttributeValue;

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
