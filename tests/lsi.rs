//! Local Secondary Index (LSI) integration tests.

use dynoxide::Database;
use dynoxide::types::AttributeValue;

/// Helper: create a table with an LSI.
/// Table: pk=UserId(S), sk=Timestamp(S)
/// LSI "StatusIndex": pk=UserId(S), sk=Status(S), projection=ALL
fn create_table_with_lsi(db: &Database) {
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
        "LocalSecondaryIndexes": [
            {
                "IndexName": "StatusIndex",
                "KeySchema": [
                    {"AttributeName": "UserId", "KeyType": "HASH"},
                    {"AttributeName": "Status", "KeyType": "RANGE"}
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
fn test_create_table_with_valid_lsis() {
    let db = Database::memory().unwrap();
    create_table_with_lsi(&db);

    // Table should be created successfully
    let desc_req: serde_json::Value = serde_json::json!({
        "TableName": "Orders"
    });
    let desc = db
        .describe_table(serde_json::from_value(desc_req).unwrap())
        .unwrap();
    assert_eq!(desc.table.table_name, "Orders");
}

#[test]
fn test_create_table_with_more_than_5_lsis_fails() {
    let db = Database::memory().unwrap();
    let req: serde_json::Value = serde_json::json!({
        "TableName": "TooManyLSI",
        "KeySchema": [
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"}
        ],
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "a", "AttributeType": "S"},
            {"AttributeName": "b", "AttributeType": "S"},
            {"AttributeName": "c", "AttributeType": "S"},
            {"AttributeName": "d", "AttributeType": "S"},
            {"AttributeName": "e", "AttributeType": "S"},
            {"AttributeName": "f", "AttributeType": "S"}
        ],
        "LocalSecondaryIndexes": [
            {"IndexName": "lsi-a", "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "a", "KeyType": "RANGE"}], "Projection": {"ProjectionType": "ALL"}},
            {"IndexName": "lsi-b", "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "b", "KeyType": "RANGE"}], "Projection": {"ProjectionType": "ALL"}},
            {"IndexName": "lsi-c", "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "c", "KeyType": "RANGE"}], "Projection": {"ProjectionType": "ALL"}},
            {"IndexName": "lsi-d", "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "d", "KeyType": "RANGE"}], "Projection": {"ProjectionType": "ALL"}},
            {"IndexName": "lsi-e", "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "e", "KeyType": "RANGE"}], "Projection": {"ProjectionType": "ALL"}},
            {"IndexName": "lsi-f", "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "f", "KeyType": "RANGE"}], "Projection": {"ProjectionType": "ALL"}}
        ]
    });
    let result = db.create_table(serde_json::from_value(req).unwrap());
    assert!(result.is_err());
    let err = format!("{}", result.unwrap_err());
    assert!(err.contains("Number of LocalSecondaryIndexes exceeds per-table limit of 5"));
}

#[test]
fn test_create_table_lsi_pk_must_match_table_pk() {
    let db = Database::memory().unwrap();
    let req: serde_json::Value = serde_json::json!({
        "TableName": "BadLSI",
        "KeySchema": [
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"}
        ],
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "other_pk", "AttributeType": "S"},
            {"AttributeName": "lsi_sk", "AttributeType": "S"}
        ],
        "LocalSecondaryIndexes": [
            {
                "IndexName": "bad-lsi",
                "KeySchema": [
                    {"AttributeName": "other_pk", "KeyType": "HASH"},
                    {"AttributeName": "lsi_sk", "KeyType": "RANGE"}
                ],
                "Projection": {"ProjectionType": "ALL"}
            }
        ]
    });
    let result = db.create_table(serde_json::from_value(req).unwrap());
    assert!(result.is_err());
    let err = format!("{}", result.unwrap_err());
    assert!(
        err.contains("Table KeySchema") || err.contains("KeySchema"),
        "Expected error about KeySchema mismatch, got: {err}"
    );
}

#[test]
fn test_create_table_lsi_sk_same_as_table_sk_succeeds() {
    // DynamoDB allows LSIs with the same sort key as the table.
    // Verified via the Dynalite conformance suite against real DynamoDB.
    let db = Database::memory().unwrap();
    let req: serde_json::Value = serde_json::json!({
        "TableName": "SameSKLSI",
        "KeySchema": [
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"}
        ],
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"}
        ],
        "LocalSecondaryIndexes": [
            {
                "IndexName": "same-sk",
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "sk", "KeyType": "RANGE"}
                ],
                "Projection": {"ProjectionType": "ALL"}
            }
        ]
    });
    let result = db.create_table(serde_json::from_value(req).unwrap());
    assert!(
        result.is_ok(),
        "LSI with same SK as table should succeed: {:?}",
        result.err()
    );
}

#[test]
fn test_describe_table_shows_lsi_definitions() {
    let db = Database::memory().unwrap();
    create_table_with_lsi(&db);

    let desc_req: serde_json::Value = serde_json::json!({
        "TableName": "Orders"
    });
    let desc = db
        .describe_table(serde_json::from_value(desc_req).unwrap())
        .unwrap();
    let lsis = desc.table.local_secondary_indexes.unwrap();
    assert_eq!(lsis.len(), 1);
    assert_eq!(lsis[0].index_name, "StatusIndex");
    assert_eq!(lsis[0].key_schema.len(), 2);
}

#[test]
fn test_put_item_and_query_via_lsi() {
    let db = Database::memory().unwrap();
    create_table_with_lsi(&db);

    put_order(&db, "user1", "2024-01-01", "SHIPPED", "100");
    put_order(&db, "user1", "2024-01-02", "PENDING", "200");
    put_order(&db, "user1", "2024-01-03", "SHIPPED", "300");
    put_order(&db, "user2", "2024-01-01", "SHIPPED", "400");

    // Query the LSI by UserId + Status
    let req: serde_json::Value = serde_json::json!({
        "TableName": "Orders",
        "IndexName": "StatusIndex",
        "KeyConditionExpression": "UserId = :u AND #s = :s",
        "ExpressionAttributeNames": {"#s": "Status"},
        "ExpressionAttributeValues": {
            ":u": {"S": "user1"},
            ":s": {"S": "SHIPPED"}
        }
    });
    let query_req = serde_json::from_value(req).unwrap();
    let resp = db.query(query_req).unwrap();

    assert_eq!(resp.count, 2);
    let items = resp.items.unwrap();
    assert_eq!(items.len(), 2);

    // All returned items should be SHIPPED for user1
    for item in &items {
        assert_eq!(
            item.get("UserId"),
            Some(&AttributeValue::S("user1".to_string()))
        );
        assert_eq!(
            item.get("Status"),
            Some(&AttributeValue::S("SHIPPED".to_string()))
        );
    }
}

#[test]
fn test_query_lsi_returns_correct_sort_order() {
    let db = Database::memory().unwrap();
    create_table_with_lsi(&db);

    put_order(&db, "user1", "2024-01-01", "C-status", "100");
    put_order(&db, "user1", "2024-01-02", "A-status", "200");
    put_order(&db, "user1", "2024-01-03", "B-status", "300");

    // Query all user1 items via LSI — should be sorted by Status
    let req: serde_json::Value = serde_json::json!({
        "TableName": "Orders",
        "IndexName": "StatusIndex",
        "KeyConditionExpression": "UserId = :u",
        "ExpressionAttributeValues": {
            ":u": {"S": "user1"}
        }
    });
    let query_req = serde_json::from_value(req).unwrap();
    let resp = db.query(query_req).unwrap();

    let items = resp.items.unwrap();
    assert_eq!(items.len(), 3);

    let statuses: Vec<&str> = items
        .iter()
        .filter_map(|i| match i.get("Status") {
            Some(AttributeValue::S(s)) => Some(s.as_str()),
            _ => None,
        })
        .collect();
    // LSI sorts by Status, so should be alphabetical
    assert_eq!(statuses, vec!["A-status", "B-status", "C-status"]);
}

#[test]
fn test_query_lsi_with_keys_only_projection() {
    let db = Database::memory().unwrap();

    let req: serde_json::Value = serde_json::json!({
        "TableName": "KeysOnlyLSI",
        "KeySchema": [
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"}
        ],
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "lsi_sk", "AttributeType": "S"}
        ],
        "LocalSecondaryIndexes": [
            {
                "IndexName": "KeysOnlyIdx",
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "lsi_sk", "KeyType": "RANGE"}
                ],
                "Projection": {"ProjectionType": "KEYS_ONLY"}
            }
        ]
    });
    db.create_table(serde_json::from_value(req).unwrap())
        .unwrap();

    // Put an item with extra attributes
    let put_req: serde_json::Value = serde_json::json!({
        "TableName": "KeysOnlyLSI",
        "Item": {
            "pk": {"S": "p1"},
            "sk": {"S": "s1"},
            "lsi_sk": {"S": "lsk1"},
            "extra_attr": {"S": "should_not_appear"}
        }
    });
    db.put_item(serde_json::from_value(put_req).unwrap())
        .unwrap();

    // Query via LSI
    let query_req: serde_json::Value = serde_json::json!({
        "TableName": "KeysOnlyLSI",
        "IndexName": "KeysOnlyIdx",
        "KeyConditionExpression": "pk = :p",
        "ExpressionAttributeValues": {
            ":p": {"S": "p1"}
        }
    });
    let resp = db
        .query(serde_json::from_value(query_req).unwrap())
        .unwrap();
    let items = resp.items.unwrap();
    assert_eq!(items.len(), 1);

    let item = &items[0];
    // Should have key attrs only: pk, sk, lsi_sk
    assert!(item.contains_key("pk"));
    assert!(item.contains_key("sk"));
    assert!(item.contains_key("lsi_sk"));
    // Should NOT have non-key attributes
    assert!(!item.contains_key("extra_attr"));
}

#[test]
fn test_delete_item_removes_from_lsi() {
    let db = Database::memory().unwrap();
    create_table_with_lsi(&db);

    put_order(&db, "user1", "2024-01-01", "SHIPPED", "100");

    // Verify it's in the LSI
    let query_req: serde_json::Value = serde_json::json!({
        "TableName": "Orders",
        "IndexName": "StatusIndex",
        "KeyConditionExpression": "UserId = :u",
        "ExpressionAttributeValues": {
            ":u": {"S": "user1"}
        }
    });
    let resp = db
        .query(serde_json::from_value(query_req.clone()).unwrap())
        .unwrap();
    assert_eq!(resp.count, 1);

    // Delete it
    let del_req: serde_json::Value = serde_json::json!({
        "TableName": "Orders",
        "Key": {
            "UserId": {"S": "user1"},
            "Timestamp": {"S": "2024-01-01"}
        }
    });
    db.delete_item(serde_json::from_value(del_req).unwrap())
        .unwrap();

    // Should be gone from LSI
    let resp = db
        .query(serde_json::from_value(query_req).unwrap())
        .unwrap();
    assert_eq!(resp.count, 0);
}

#[test]
fn test_sparse_lsi_item_without_lsi_sk_not_in_index() {
    let db = Database::memory().unwrap();
    create_table_with_lsi(&db);

    // Put an item WITHOUT the Status attribute (LSI sort key)
    let req: serde_json::Value = serde_json::json!({
        "TableName": "Orders",
        "Item": {
            "UserId": {"S": "user1"},
            "Timestamp": {"S": "2024-01-01"},
            "Amount": {"N": "100"}
        }
    });
    db.put_item(serde_json::from_value(req).unwrap()).unwrap();

    // Query LSI — should return nothing
    let query_req: serde_json::Value = serde_json::json!({
        "TableName": "Orders",
        "IndexName": "StatusIndex",
        "KeyConditionExpression": "UserId = :u",
        "ExpressionAttributeValues": {
            ":u": {"S": "user1"}
        }
    });
    let resp = db
        .query(serde_json::from_value(query_req).unwrap())
        .unwrap();
    assert_eq!(resp.count, 0);
}

#[test]
fn test_update_item_adds_lsi_sk_appears_in_lsi() {
    let db = Database::memory().unwrap();
    create_table_with_lsi(&db);

    // Put item without Status
    let req: serde_json::Value = serde_json::json!({
        "TableName": "Orders",
        "Item": {
            "UserId": {"S": "user1"},
            "Timestamp": {"S": "2024-01-01"},
            "Amount": {"N": "100"}
        }
    });
    db.put_item(serde_json::from_value(req).unwrap()).unwrap();

    // Verify not in LSI
    let query_req: serde_json::Value = serde_json::json!({
        "TableName": "Orders",
        "IndexName": "StatusIndex",
        "KeyConditionExpression": "UserId = :u",
        "ExpressionAttributeValues": {
            ":u": {"S": "user1"}
        }
    });
    let resp = db
        .query(serde_json::from_value(query_req.clone()).unwrap())
        .unwrap();
    assert_eq!(resp.count, 0);

    // Update to add Status
    let update_req: serde_json::Value = serde_json::json!({
        "TableName": "Orders",
        "Key": {
            "UserId": {"S": "user1"},
            "Timestamp": {"S": "2024-01-01"}
        },
        "UpdateExpression": "SET #s = :s",
        "ExpressionAttributeNames": {"#s": "Status"},
        "ExpressionAttributeValues": {
            ":s": {"S": "PENDING"}
        }
    });
    db.update_item(serde_json::from_value(update_req).unwrap())
        .unwrap();

    // Now should appear in LSI
    let resp = db
        .query(serde_json::from_value(query_req).unwrap())
        .unwrap();
    assert_eq!(resp.count, 1);
    let items = resp.items.unwrap();
    assert_eq!(
        items[0].get("Status"),
        Some(&AttributeValue::S("PENDING".to_string()))
    );
}

#[test]
fn test_update_item_removes_lsi_sk_disappears_from_lsi() {
    let db = Database::memory().unwrap();
    create_table_with_lsi(&db);

    // Put item with Status
    put_order(&db, "user1", "2024-01-01", "SHIPPED", "100");

    // Verify in LSI
    let query_req: serde_json::Value = serde_json::json!({
        "TableName": "Orders",
        "IndexName": "StatusIndex",
        "KeyConditionExpression": "UserId = :u",
        "ExpressionAttributeValues": {
            ":u": {"S": "user1"}
        }
    });
    let resp = db
        .query(serde_json::from_value(query_req.clone()).unwrap())
        .unwrap();
    assert_eq!(resp.count, 1);

    // Remove Status via REMOVE
    let update_req: serde_json::Value = serde_json::json!({
        "TableName": "Orders",
        "Key": {
            "UserId": {"S": "user1"},
            "Timestamp": {"S": "2024-01-01"}
        },
        "UpdateExpression": "REMOVE #s",
        "ExpressionAttributeNames": {"#s": "Status"}
    });
    db.update_item(serde_json::from_value(update_req).unwrap())
        .unwrap();

    // Should no longer be in LSI
    let resp = db
        .query(serde_json::from_value(query_req).unwrap())
        .unwrap();
    assert_eq!(resp.count, 0);
}

#[test]
fn test_scan_lsi() {
    let db = Database::memory().unwrap();
    create_table_with_lsi(&db);

    put_order(&db, "user1", "2024-01-01", "SHIPPED", "100");
    put_order(&db, "user1", "2024-01-02", "PENDING", "200");
    put_order(&db, "user2", "2024-01-01", "SHIPPED", "300");

    // Scan the LSI
    let req: serde_json::Value = serde_json::json!({
        "TableName": "Orders",
        "IndexName": "StatusIndex"
    });
    let scan_req = serde_json::from_value(req).unwrap();
    let resp = db.scan(scan_req).unwrap();

    assert_eq!(resp.count, 3);
    let items = resp.items.unwrap();
    assert_eq!(items.len(), 3);
}

#[test]
fn test_create_table_duplicate_index_names_rejected() {
    let db = Database::memory().unwrap();
    let req: serde_json::Value = serde_json::json!({
        "TableName": "DupIdx",
        "KeySchema": [
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"}
        ],
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "gsi_pk", "AttributeType": "S"},
            {"AttributeName": "lsi_sk", "AttributeType": "S"}
        ],
        "GlobalSecondaryIndexes": [
            {
                "IndexName": "SharedName",
                "KeySchema": [
                    {"AttributeName": "gsi_pk", "KeyType": "HASH"}
                ],
                "Projection": {"ProjectionType": "ALL"}
            }
        ],
        "LocalSecondaryIndexes": [
            {
                "IndexName": "SharedName",
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "lsi_sk", "KeyType": "RANGE"}
                ],
                "Projection": {"ProjectionType": "ALL"}
            }
        ]
    });
    let result = db.create_table(serde_json::from_value(req).unwrap());
    assert!(result.is_err());
    let err = format!("{}", result.unwrap_err());
    assert!(
        err.contains("Duplicate index name: SharedName"),
        "Expected duplicate index name error, got: {err}"
    );
}

#[test]
fn test_put_item_overwrite_changes_lsi_sk() {
    let db = Database::memory().unwrap();
    create_table_with_lsi(&db);

    // Put item with Status = "A"
    put_order(&db, "user1", "2024-01-01", "A", "100");

    // Verify it appears when querying for Status = "A"
    let query_a: serde_json::Value = serde_json::json!({
        "TableName": "Orders",
        "IndexName": "StatusIndex",
        "KeyConditionExpression": "UserId = :u AND #s = :s",
        "ExpressionAttributeNames": {"#s": "Status"},
        "ExpressionAttributeValues": {
            ":u": {"S": "user1"},
            ":s": {"S": "A"}
        }
    });
    let resp = db
        .query(serde_json::from_value(query_a.clone()).unwrap())
        .unwrap();
    assert_eq!(resp.count, 1);

    // Overwrite with Status = "B"
    put_order(&db, "user1", "2024-01-01", "B", "100");

    // Query for "A" should now return nothing
    let resp = db.query(serde_json::from_value(query_a).unwrap()).unwrap();
    assert_eq!(resp.count, 0, "Old LSI sort key 'A' should no longer match");

    // Query for "B" should return the item
    let query_b: serde_json::Value = serde_json::json!({
        "TableName": "Orders",
        "IndexName": "StatusIndex",
        "KeyConditionExpression": "UserId = :u AND #s = :s",
        "ExpressionAttributeNames": {"#s": "Status"},
        "ExpressionAttributeValues": {
            ":u": {"S": "user1"},
            ":s": {"S": "B"}
        }
    });
    let resp = db.query(serde_json::from_value(query_b).unwrap()).unwrap();
    assert_eq!(resp.count, 1, "New LSI sort key 'B' should match");
    let items = resp.items.unwrap();
    assert_eq!(
        items[0].get("Status"),
        Some(&AttributeValue::S("B".to_string()))
    );
}
