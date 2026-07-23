use dynoxide::Database;
use dynoxide::actions::create_table::CreateTableRequest;
use dynoxide::actions::put_item::PutItemRequest;
use dynoxide::actions::query::QueryRequest;
use dynoxide::actions::scan::ScanRequest;
use dynoxide::types::AttributeValue;

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

fn create_hash_only_table(db: &Database, name: &str) {
    let req: CreateTableRequest = serde_json::from_value(serde_json::json!({
        "TableName": name,
        "KeySchema": [
            {"AttributeName": "pk", "KeyType": "HASH"}
        ],
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"}
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
// Query tests
// =============================================================================

#[test]
fn test_query_simple_pk_lookup() {
    let db = setup_db();
    create_test_table(&db, "Orders");

    put(
        &db,
        "Orders",
        serde_json::json!({"pk": {"S": "user#1"}, "sk": {"S": "order#001"}, "total": {"N": "10"}}),
    );
    put(
        &db,
        "Orders",
        serde_json::json!({"pk": {"S": "user#1"}, "sk": {"S": "order#002"}, "total": {"N": "20"}}),
    );
    put(
        &db,
        "Orders",
        serde_json::json!({"pk": {"S": "user#2"}, "sk": {"S": "order#001"}, "total": {"N": "30"}}),
    );

    let req: QueryRequest = serde_json::from_value(serde_json::json!({
        "TableName": "Orders",
        "KeyConditionExpression": "pk = :pk",
        "ExpressionAttributeValues": {":pk": {"S": "user#1"}}
    }))
    .unwrap();

    let resp = db.query(req).unwrap();
    assert_eq!(resp.count, 2);
    assert_eq!(resp.scanned_count, 2);
    assert!(resp.last_evaluated_key.is_none());
    let items = resp.items.unwrap();
    assert_eq!(items.len(), 2);
}

#[test]
fn test_query_pk_and_sk_range() {
    let db = setup_db();
    create_test_table(&db, "Events");

    for i in 1..=10 {
        put(
            &db,
            "Events",
            serde_json::json!({"pk": {"S": "device#1"}, "sk": {"S": format!("2024-01-{:02}", i)}, "data": {"S": format!("event{}", i)}}),
        );
    }

    // sk BETWEEN
    let req: QueryRequest = serde_json::from_value(serde_json::json!({
        "TableName": "Events",
        "KeyConditionExpression": "pk = :pk AND sk BETWEEN :lo AND :hi",
        "ExpressionAttributeValues": {
            ":pk": {"S": "device#1"},
            ":lo": {"S": "2024-01-03"},
            ":hi": {"S": "2024-01-07"}
        }
    }))
    .unwrap();

    let resp = db.query(req).unwrap();
    assert_eq!(resp.count, 5);

    // sk begins_with
    let req: QueryRequest = serde_json::from_value(serde_json::json!({
        "TableName": "Events",
        "KeyConditionExpression": "pk = :pk AND begins_with(sk, :prefix)",
        "ExpressionAttributeValues": {
            ":pk": {"S": "device#1"},
            ":prefix": {"S": "2024-01-0"}
        }
    }))
    .unwrap();

    let resp = db.query(req).unwrap();
    assert_eq!(resp.count, 9); // 01 through 09
}

#[test]
fn test_query_sk_comparisons() {
    let db = setup_db();
    create_test_table(&db, "Tbl");

    for i in 1..=5 {
        put(
            &db,
            "Tbl",
            serde_json::json!({"pk": {"S": "a"}, "sk": {"S": format!("{}", i)}}),
        );
    }

    // sk > "3"
    let req: QueryRequest = serde_json::from_value(serde_json::json!({
        "TableName": "Tbl",
        "KeyConditionExpression": "pk = :pk AND sk > :sk",
        "ExpressionAttributeValues": {":pk": {"S": "a"}, ":sk": {"S": "3"}}
    }))
    .unwrap();
    assert_eq!(db.query(req).unwrap().count, 2); // 4, 5

    // sk <= "3"
    let req: QueryRequest = serde_json::from_value(serde_json::json!({
        "TableName": "Tbl",
        "KeyConditionExpression": "pk = :pk AND sk <= :sk",
        "ExpressionAttributeValues": {":pk": {"S": "a"}, ":sk": {"S": "3"}}
    }))
    .unwrap();
    assert_eq!(db.query(req).unwrap().count, 3); // 1, 2, 3
}

#[test]
fn test_query_scan_index_forward() {
    let db = setup_db();
    create_test_table(&db, "Tbl");

    put(
        &db,
        "Tbl",
        serde_json::json!({"pk": {"S": "a"}, "sk": {"S": "1"}}),
    );
    put(
        &db,
        "Tbl",
        serde_json::json!({"pk": {"S": "a"}, "sk": {"S": "2"}}),
    );
    put(
        &db,
        "Tbl",
        serde_json::json!({"pk": {"S": "a"}, "sk": {"S": "3"}}),
    );

    // Forward (default)
    let req: QueryRequest = serde_json::from_value(serde_json::json!({
        "TableName": "Tbl",
        "KeyConditionExpression": "pk = :pk",
        "ExpressionAttributeValues": {":pk": {"S": "a"}},
        "ScanIndexForward": true
    }))
    .unwrap();
    let items = db.query(req).unwrap().items.unwrap();
    assert_eq!(items[0].get("sk").unwrap(), &AttributeValue::S("1".into()));
    assert_eq!(items[2].get("sk").unwrap(), &AttributeValue::S("3".into()));

    // Reverse
    let req: QueryRequest = serde_json::from_value(serde_json::json!({
        "TableName": "Tbl",
        "KeyConditionExpression": "pk = :pk",
        "ExpressionAttributeValues": {":pk": {"S": "a"}},
        "ScanIndexForward": false
    }))
    .unwrap();
    let items = db.query(req).unwrap().items.unwrap();
    assert_eq!(items[0].get("sk").unwrap(), &AttributeValue::S("3".into()));
    assert_eq!(items[2].get("sk").unwrap(), &AttributeValue::S("1".into()));
}

#[test]
fn test_query_with_filter_expression() {
    let db = setup_db();
    create_test_table(&db, "Tbl");

    put(
        &db,
        "Tbl",
        serde_json::json!({"pk": {"S": "a"}, "sk": {"S": "1"}, "status": {"S": "active"}}),
    );
    put(
        &db,
        "Tbl",
        serde_json::json!({"pk": {"S": "a"}, "sk": {"S": "2"}, "status": {"S": "inactive"}}),
    );
    put(
        &db,
        "Tbl",
        serde_json::json!({"pk": {"S": "a"}, "sk": {"S": "3"}, "status": {"S": "active"}}),
    );

    let req: QueryRequest = serde_json::from_value(serde_json::json!({
        "TableName": "Tbl",
        "KeyConditionExpression": "pk = :pk",
        "FilterExpression": "#s = :status",
        "ExpressionAttributeNames": {"#s": "status"},
        "ExpressionAttributeValues": {":pk": {"S": "a"}, ":status": {"S": "active"}}
    }))
    .unwrap();

    let resp = db.query(req).unwrap();
    assert_eq!(resp.count, 2); // Two active items
    assert_eq!(resp.scanned_count, 3); // Three items scanned
}

#[test]
fn test_query_with_projection() {
    let db = setup_db();
    create_test_table(&db, "Tbl");

    put(
        &db,
        "Tbl",
        serde_json::json!({"pk": {"S": "a"}, "sk": {"S": "1"}, "name": {"S": "Alice"}, "age": {"N": "30"}}),
    );

    let req: QueryRequest = serde_json::from_value(serde_json::json!({
        "TableName": "Tbl",
        "KeyConditionExpression": "pk = :pk",
        "ProjectionExpression": "#n",
        "ExpressionAttributeNames": {"#n": "name"},
        "ExpressionAttributeValues": {":pk": {"S": "a"}}
    }))
    .unwrap();

    let resp = db.query(req).unwrap();
    let items = resp.items.unwrap();
    assert_eq!(items.len(), 1);
    // DynamoDB does NOT auto-include key attributes when ProjectionExpression is set
    assert!(!items[0].contains_key("pk"));
    assert!(!items[0].contains_key("sk"));
    assert!(items[0].contains_key("name")); // Projected
    assert!(!items[0].contains_key("age")); // Not projected
}

#[test]
fn test_query_pagination_with_limit() {
    let db = setup_db();
    create_test_table(&db, "Tbl");

    for i in 1..=5 {
        put(
            &db,
            "Tbl",
            serde_json::json!({"pk": {"S": "a"}, "sk": {"S": format!("{:03}", i)}}),
        );
    }

    // Page 1: limit 2
    let req: QueryRequest = serde_json::from_value(serde_json::json!({
        "TableName": "Tbl",
        "KeyConditionExpression": "pk = :pk",
        "ExpressionAttributeValues": {":pk": {"S": "a"}},
        "Limit": 2
    }))
    .unwrap();

    let resp = db.query(req).unwrap();
    assert_eq!(resp.count, 2);
    assert!(resp.last_evaluated_key.is_some());
    let lek = resp.last_evaluated_key.unwrap();
    assert_eq!(lek.get("sk").unwrap(), &AttributeValue::S("002".into()));

    // Page 2: resume from LEK
    let req: QueryRequest = serde_json::from_value(serde_json::json!({
        "TableName": "Tbl",
        "KeyConditionExpression": "pk = :pk",
        "ExpressionAttributeValues": {":pk": {"S": "a"}},
        "ExclusiveStartKey": {"pk": {"S": "a"}, "sk": {"S": "002"}},
        "Limit": 2
    }))
    .unwrap();

    let resp = db.query(req).unwrap();
    assert_eq!(resp.count, 2);
    let items = resp.items.unwrap();
    assert_eq!(
        items[0].get("sk").unwrap(),
        &AttributeValue::S("003".into())
    );

    // Page 3: last page
    let req: QueryRequest = serde_json::from_value(serde_json::json!({
        "TableName": "Tbl",
        "KeyConditionExpression": "pk = :pk",
        "ExpressionAttributeValues": {":pk": {"S": "a"}},
        "ExclusiveStartKey": {"pk": {"S": "a"}, "sk": {"S": "004"}},
        "Limit": 2
    }))
    .unwrap();

    let resp = db.query(req).unwrap();
    assert_eq!(resp.count, 1);
    assert!(resp.last_evaluated_key.is_none()); // No more data
}

#[test]
fn test_query_empty_result() {
    let db = setup_db();
    create_test_table(&db, "Tbl");

    let req: QueryRequest = serde_json::from_value(serde_json::json!({
        "TableName": "Tbl",
        "KeyConditionExpression": "pk = :pk",
        "ExpressionAttributeValues": {":pk": {"S": "nonexistent"}}
    }))
    .unwrap();

    let resp = db.query(req).unwrap();
    assert_eq!(resp.count, 0);
    assert_eq!(resp.scanned_count, 0);
    assert!(resp.last_evaluated_key.is_none());
}

#[test]
fn test_query_hash_only_table() {
    let db = setup_db();
    create_hash_only_table(&db, "Users");

    put(
        &db,
        "Users",
        serde_json::json!({"pk": {"S": "user#1"}, "name": {"S": "Alice"}}),
    );
    put(
        &db,
        "Users",
        serde_json::json!({"pk": {"S": "user#2"}, "name": {"S": "Bob"}}),
    );

    let req: QueryRequest = serde_json::from_value(serde_json::json!({
        "TableName": "Users",
        "KeyConditionExpression": "pk = :pk",
        "ExpressionAttributeValues": {":pk": {"S": "user#1"}}
    }))
    .unwrap();

    let resp = db.query(req).unwrap();
    assert_eq!(resp.count, 1);
    let items = resp.items.unwrap();
    assert_eq!(
        items[0].get("name").unwrap(),
        &AttributeValue::S("Alice".into())
    );
}

#[test]
fn test_query_select_count() {
    let db = setup_db();
    create_test_table(&db, "Tbl");

    for i in 1..=5 {
        put(
            &db,
            "Tbl",
            serde_json::json!({"pk": {"S": "a"}, "sk": {"S": format!("{}", i)}}),
        );
    }

    let req: QueryRequest = serde_json::from_value(serde_json::json!({
        "TableName": "Tbl",
        "KeyConditionExpression": "pk = :pk",
        "ExpressionAttributeValues": {":pk": {"S": "a"}},
        "Select": "COUNT"
    }))
    .unwrap();

    let resp = db.query(req).unwrap();
    assert_eq!(resp.count, 5);
    assert!(resp.items.is_none()); // No items returned for COUNT
}

// =============================================================================
// Scan tests
// =============================================================================

#[test]
fn test_scan_full_table() {
    let db = setup_db();
    create_test_table(&db, "Tbl");

    put(
        &db,
        "Tbl",
        serde_json::json!({"pk": {"S": "a"}, "sk": {"S": "1"}}),
    );
    put(
        &db,
        "Tbl",
        serde_json::json!({"pk": {"S": "b"}, "sk": {"S": "1"}}),
    );
    put(
        &db,
        "Tbl",
        serde_json::json!({"pk": {"S": "c"}, "sk": {"S": "1"}}),
    );

    let req: ScanRequest = serde_json::from_value(serde_json::json!({
        "TableName": "Tbl"
    }))
    .unwrap();

    let resp = db.scan(req).unwrap();
    assert_eq!(resp.count, 3);
    assert_eq!(resp.scanned_count, 3);
    assert!(resp.last_evaluated_key.is_none());
}

#[test]
fn test_scan_with_filter() {
    let db = setup_db();
    create_test_table(&db, "Tbl");

    put(
        &db,
        "Tbl",
        serde_json::json!({"pk": {"S": "a"}, "sk": {"S": "1"}, "color": {"S": "red"}}),
    );
    put(
        &db,
        "Tbl",
        serde_json::json!({"pk": {"S": "b"}, "sk": {"S": "1"}, "color": {"S": "blue"}}),
    );
    put(
        &db,
        "Tbl",
        serde_json::json!({"pk": {"S": "c"}, "sk": {"S": "1"}, "color": {"S": "red"}}),
    );

    let req: ScanRequest = serde_json::from_value(serde_json::json!({
        "TableName": "Tbl",
        "FilterExpression": "color = :c",
        "ExpressionAttributeValues": {":c": {"S": "red"}}
    }))
    .unwrap();

    let resp = db.scan(req).unwrap();
    assert_eq!(resp.count, 2); // Two red items
    assert_eq!(resp.scanned_count, 3); // All three scanned
}

#[test]
fn test_scan_pagination() {
    let db = setup_db();
    create_test_table(&db, "Tbl");

    for i in 1..=5 {
        put(
            &db,
            "Tbl",
            serde_json::json!({"pk": {"S": format!("{:03}", i)}, "sk": {"S": "x"}}),
        );
    }

    // Page 1
    let req: ScanRequest = serde_json::from_value(serde_json::json!({
        "TableName": "Tbl",
        "Limit": 2
    }))
    .unwrap();

    let resp = db.scan(req).unwrap();
    assert_eq!(resp.count, 2);
    assert!(resp.last_evaluated_key.is_some());

    // Page 2
    let lek = resp.last_evaluated_key.unwrap();
    let req: ScanRequest = serde_json::from_value(serde_json::json!({
        "TableName": "Tbl",
        "Limit": 2,
        "ExclusiveStartKey": lek
    }))
    .unwrap();

    let resp = db.scan(req).unwrap();
    assert_eq!(resp.count, 2);

    // Page 3
    let lek = resp.last_evaluated_key.unwrap();
    let req: ScanRequest = serde_json::from_value(serde_json::json!({
        "TableName": "Tbl",
        "Limit": 2,
        "ExclusiveStartKey": lek
    }))
    .unwrap();

    let resp = db.scan(req).unwrap();
    assert_eq!(resp.count, 1);
    assert!(resp.last_evaluated_key.is_none());
}

#[test]
fn test_scan_empty_table() {
    let db = setup_db();
    create_test_table(&db, "Tbl");

    let req: ScanRequest = serde_json::from_value(serde_json::json!({
        "TableName": "Tbl"
    }))
    .unwrap();

    let resp = db.scan(req).unwrap();
    assert_eq!(resp.count, 0);
    assert_eq!(resp.scanned_count, 0);
    assert!(resp.last_evaluated_key.is_none());
}

#[test]
fn test_scan_with_projection() {
    let db = setup_db();
    create_test_table(&db, "Tbl");

    put(
        &db,
        "Tbl",
        serde_json::json!({"pk": {"S": "a"}, "sk": {"S": "1"}, "name": {"S": "Alice"}, "age": {"N": "30"}}),
    );

    let req: ScanRequest = serde_json::from_value(serde_json::json!({
        "TableName": "Tbl",
        "ProjectionExpression": "#n",
        "ExpressionAttributeNames": {"#n": "name"}
    }))
    .unwrap();

    let resp = db.scan(req).unwrap();
    let items = resp.items.unwrap();
    assert_eq!(items.len(), 1);
    // DynamoDB does NOT auto-include key attributes when ProjectionExpression is set
    assert!(!items[0].contains_key("pk"));
    assert!(items[0].contains_key("name")); // Projected
    assert!(!items[0].contains_key("age")); // Not projected
}

#[test]
fn test_scan_select_count() {
    let db = setup_db();
    create_test_table(&db, "Tbl");

    put(
        &db,
        "Tbl",
        serde_json::json!({"pk": {"S": "a"}, "sk": {"S": "1"}}),
    );
    put(
        &db,
        "Tbl",
        serde_json::json!({"pk": {"S": "b"}, "sk": {"S": "1"}}),
    );

    let req: ScanRequest = serde_json::from_value(serde_json::json!({
        "TableName": "Tbl",
        "Select": "COUNT"
    }))
    .unwrap();

    let resp = db.scan(req).unwrap();
    assert_eq!(resp.count, 2);
    assert!(resp.items.is_none());
}

#[test]
fn test_query_filter_produces_empty_page_with_lek() {
    let db = setup_db();
    create_test_table(&db, "Tbl");

    // All items for pk "a" have status "inactive"
    for i in 1..=3 {
        put(
            &db,
            "Tbl",
            serde_json::json!({"pk": {"S": "a"}, "sk": {"S": format!("{}", i)}, "status": {"S": "inactive"}}),
        );
    }

    // Query with filter that matches nothing, but with Limit
    let req: QueryRequest = serde_json::from_value(serde_json::json!({
        "TableName": "Tbl",
        "KeyConditionExpression": "pk = :pk",
        "FilterExpression": "#s = :active",
        "ExpressionAttributeNames": {"#s": "status"},
        "ExpressionAttributeValues": {":pk": {"S": "a"}, ":active": {"S": "active"}},
        "Limit": 2
    }))
    .unwrap();

    let resp = db.query(req).unwrap();
    // All 2 scanned items were filtered out, so count is 0
    assert_eq!(resp.count, 0);
    assert_eq!(resp.scanned_count, 2);
    // LastEvaluatedKey should still be present because Limit was hit
    assert!(resp.last_evaluated_key.is_some());
}

#[test]
fn test_query_with_attribute_names() {
    let db = setup_db();
    create_test_table(&db, "Tbl");

    put(
        &db,
        "Tbl",
        serde_json::json!({"pk": {"S": "a"}, "sk": {"S": "1"}, "status": {"S": "ok"}}),
    );

    let req: QueryRequest = serde_json::from_value(serde_json::json!({
        "TableName": "Tbl",
        "KeyConditionExpression": "#pk = :pk",
        "ExpressionAttributeNames": {"#pk": "pk"},
        "ExpressionAttributeValues": {":pk": {"S": "a"}}
    }))
    .unwrap();

    let resp = db.query(req).unwrap();
    assert_eq!(resp.count, 1);
}

// ---------------------------------------------------------------------------
// Select=SPECIFIC_ATTRIBUTES without ProjectionExpression should be rejected
// ---------------------------------------------------------------------------

#[test]
fn test_query_specific_attributes_without_projection_rejected() {
    let db = setup_db();
    create_test_table(&db, "Tbl2");
    put(
        &db,
        "Tbl2",
        serde_json::json!({"pk": {"S": "a"}, "sk": {"S": "1"}}),
    );

    let req: QueryRequest = serde_json::from_value(serde_json::json!({
        "TableName": "Tbl2",
        "KeyConditionExpression": "pk = :pk",
        "ExpressionAttributeValues": {":pk": {"S": "a"}},
        "Select": "SPECIFIC_ATTRIBUTES"
    }))
    .unwrap();

    let err = db.query(req).unwrap_err();
    // Query wraps the phrase in the "1 validation error detected:" envelope;
    // Scan returns it bare. Verified against real AWS by the conformance suite.
    assert_eq!(
        err.to_string(),
        "1 validation error detected: Must specify the AttributesToGet or ProjectionExpression when choosing to get SPECIFIC_ATTRIBUTES",
        "expected DynamoDB's exact Query message, got: {err}"
    );
}

#[test]
fn test_scan_specific_attributes_without_projection_rejected() {
    let db = setup_db();
    create_test_table(&db, "Tbl3");

    let req: ScanRequest = serde_json::from_value(serde_json::json!({
        "TableName": "Tbl3",
        "Select": "SPECIFIC_ATTRIBUTES"
    }))
    .unwrap();

    let err = db.scan(req).unwrap_err();
    assert_eq!(
        err.to_string(),
        "Must specify the AttributesToGet or ProjectionExpression when choosing to get SPECIFIC_ATTRIBUTES",
        "expected DynamoDB's exact message, got: {err}"
    );
}

#[test]
fn test_query_specific_attributes_with_projection_succeeds() {
    let db = setup_db();
    create_test_table(&db, "Tbl4");
    put(
        &db,
        "Tbl4",
        serde_json::json!({"pk": {"S": "a"}, "sk": {"S": "1"}, "val": {"S": "hello"}}),
    );

    let req: QueryRequest = serde_json::from_value(serde_json::json!({
        "TableName": "Tbl4",
        "KeyConditionExpression": "pk = :pk",
        "ExpressionAttributeValues": {":pk": {"S": "a"}},
        "Select": "SPECIFIC_ATTRIBUTES",
        "ProjectionExpression": "val"
    }))
    .unwrap();

    let resp = db.query(req).unwrap();
    assert_eq!(resp.count, 1);
}

#[test]
fn test_query_projection_with_nested_path_and_list_index() {
    let db = setup_db();
    create_test_table(&db, "ProjTest");
    put(
        &db,
        "ProjTest",
        serde_json::json!({
            "pk": {"S": "x"}, "sk": {"S": "1"},
            "mymap": {"M": {"nested": {"S": "deep"}}},
            "mylist": {"L": [{"S": "zero"}, {"S": "one"}, {"S": "two"}]}
        }),
    );

    let req: QueryRequest = serde_json::from_value(serde_json::json!({
        "TableName": "ProjTest",
        "KeyConditionExpression": "pk = :pk",
        "ExpressionAttributeValues": {":pk": {"S": "x"}},
        "ProjectionExpression": "#m.#n, #l[1]",
        "ExpressionAttributeNames": {"#m": "mymap", "#n": "nested", "#l": "mylist"}
    }))
    .unwrap();

    let resp = db.query(req).unwrap();
    assert_eq!(resp.count, 1);
    let item = &resp.items.unwrap()[0];

    // mymap.nested → {"mymap": {"nested": "deep"}}
    let mymap = item.get("mymap").expect("should have mymap");
    match mymap {
        AttributeValue::M(map) => {
            assert_eq!(map.get("nested"), Some(&AttributeValue::S("deep".into())));
        }
        other => panic!("expected map for mymap, got {:?}", other),
    }

    // mylist[1] → {"mylist": ["one"]}
    let mylist = item.get("mylist").expect("should have mylist");
    match mylist {
        AttributeValue::L(list) => {
            assert_eq!(list.len(), 1);
            assert_eq!(list[0], AttributeValue::S("one".into()));
        }
        other => panic!("expected list for mylist, got {:?}", other),
    }
}

// ---------------------------------------------------------------------------
// #96: Select / ProjectionExpression / IndexName combinations DynamoDB rejects
// ---------------------------------------------------------------------------

fn create_gsi_table(db: &Database, name: &str) {
    let req: CreateTableRequest = serde_json::from_value(serde_json::json!({
        "TableName": name,
        "KeySchema": [
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"}
        ],
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "gsipk", "AttributeType": "S"}
        ],
        "GlobalSecondaryIndexes": [{
            "IndexName": "GsiIndex",
            "KeySchema": [{"AttributeName": "gsipk", "KeyType": "HASH"}],
            "Projection": {"ProjectionType": "ALL"}
        }],
        "BillingMode": "PAY_PER_REQUEST"
    }))
    .unwrap();
    db.create_table(req).unwrap();
}

#[test]
fn test_query_all_attributes_with_projection_rejected() {
    let db = setup_db();
    create_test_table(&db, "Tbl96");
    let req: QueryRequest = serde_json::from_value(serde_json::json!({
        "TableName": "Tbl96",
        "KeyConditionExpression": "pk = :pk",
        "ExpressionAttributeValues": {":pk": {"S": "a"}},
        "Select": "ALL_ATTRIBUTES",
        "ProjectionExpression": "sk"
    }))
    .unwrap();
    let err = db.query(req).unwrap_err();
    assert!(
        err.to_string().contains(
            "Cannot specify the ProjectionExpression when choosing to get ALL_ATTRIBUTES"
        ),
        "got: {err}"
    );
}

#[test]
fn test_scan_all_attributes_with_projection_rejected() {
    let db = setup_db();
    create_test_table(&db, "Tbl96");
    let req: ScanRequest = serde_json::from_value(serde_json::json!({
        "TableName": "Tbl96",
        "Select": "ALL_ATTRIBUTES",
        "ProjectionExpression": "sk"
    }))
    .unwrap();
    let err = db.scan(req).unwrap_err();
    assert!(
        err.to_string().contains(
            "Cannot specify the ProjectionExpression when choosing to get ALL_ATTRIBUTES"
        ),
        "got: {err}"
    );
}

#[test]
fn test_query_all_projected_without_index_rejected() {
    let db = setup_db();
    create_test_table(&db, "Tbl96");
    let req: QueryRequest = serde_json::from_value(serde_json::json!({
        "TableName": "Tbl96",
        "KeyConditionExpression": "pk = :pk",
        "ExpressionAttributeValues": {":pk": {"S": "a"}},
        "Select": "ALL_PROJECTED_ATTRIBUTES"
    }))
    .unwrap();
    let err = db.query(req).unwrap_err();
    assert!(
        err.to_string()
            .contains("ALL_PROJECTED_ATTRIBUTES can be used only when Querying using an IndexName"),
        "got: {err}"
    );
}

#[test]
fn test_scan_all_projected_without_index_rejected() {
    let db = setup_db();
    create_test_table(&db, "Tbl96");
    let req: ScanRequest = serde_json::from_value(serde_json::json!({
        "TableName": "Tbl96",
        "Select": "ALL_PROJECTED_ATTRIBUTES"
    }))
    .unwrap();
    let err = db.scan(req).unwrap_err();
    assert!(
        err.to_string()
            .contains("ALL_PROJECTED_ATTRIBUTES can be used only when Querying using an IndexName"),
        "got: {err}"
    );
}

#[test]
fn test_query_count_with_projection_rejected() {
    let db = setup_db();
    create_test_table(&db, "Tbl96");
    let req: QueryRequest = serde_json::from_value(serde_json::json!({
        "TableName": "Tbl96",
        "KeyConditionExpression": "pk = :pk",
        "ExpressionAttributeValues": {":pk": {"S": "a"}},
        "Select": "COUNT",
        "ProjectionExpression": "sk"
    }))
    .unwrap();
    let err = db.query(req).unwrap_err();
    assert!(
        err.to_string().contains(
            "Cannot specify the ProjectionExpression when choosing to get only the Count"
        ),
        "got: {err}"
    );
}

#[test]
fn test_query_all_projected_with_index_accepted() {
    // The enum-conditional rule must NOT reject when an IndexName is present.
    let db = setup_db();
    create_gsi_table(&db, "Gsi96");
    let req: QueryRequest = serde_json::from_value(serde_json::json!({
        "TableName": "Gsi96",
        "IndexName": "GsiIndex",
        "KeyConditionExpression": "gsipk = :g",
        "ExpressionAttributeValues": {":g": {"S": "x"}},
        "Select": "ALL_PROJECTED_ATTRIBUTES"
    }))
    .unwrap();
    db.query(req).unwrap();
}

#[test]
fn test_query_projection_without_select_accepted() {
    // A ProjectionExpression with no Select is legal (mutual-exclusion must not over-fire).
    let db = setup_db();
    create_test_table(&db, "Tbl96");
    put(
        &db,
        "Tbl96",
        serde_json::json!({"pk": {"S": "a"}, "sk": {"S": "1"}, "val": {"S": "v"}}),
    );
    let req: QueryRequest = serde_json::from_value(serde_json::json!({
        "TableName": "Tbl96",
        "KeyConditionExpression": "pk = :pk",
        "ExpressionAttributeValues": {":pk": {"S": "a"}},
        "ProjectionExpression": "val"
    }))
    .unwrap();
    db.query(req).unwrap();
}

#[test]
fn test_scan_count_with_projection_rejected() {
    let db = setup_db();
    create_test_table(&db, "Tbl96");
    let req: ScanRequest = serde_json::from_value(serde_json::json!({
        "TableName": "Tbl96",
        "Select": "COUNT",
        "ProjectionExpression": "sk"
    }))
    .unwrap();
    let err = db.scan(req).unwrap_err();
    assert!(
        err.to_string().contains(
            "Cannot specify the ProjectionExpression when choosing to get only the Count"
        ),
        "got: {err}"
    );
}

#[test]
fn test_scan_all_projected_with_index_accepted() {
    // The enum-conditional rule must NOT reject a Scan when an IndexName is present.
    let db = setup_db();
    create_gsi_table(&db, "Gsi96");
    let req: ScanRequest = serde_json::from_value(serde_json::json!({
        "TableName": "Gsi96",
        "IndexName": "GsiIndex",
        "Select": "ALL_PROJECTED_ATTRIBUTES"
    }))
    .unwrap();
    db.scan(req).unwrap();
}

#[test]
fn test_scan_projection_without_select_accepted() {
    // A ProjectionExpression with no Select is legal on Scan too.
    let db = setup_db();
    create_test_table(&db, "Tbl96");
    let req: ScanRequest = serde_json::from_value(serde_json::json!({
        "TableName": "Tbl96",
        "ProjectionExpression": "sk"
    }))
    .unwrap();
    db.scan(req).unwrap();
}

#[test]
fn test_all_projected_with_projection_and_no_index_prefers_mutual_exclusion() {
    // ALL_PROJECTED_ATTRIBUTES + ProjectionExpression + no IndexName breaks both rules.
    // DynamoDB reports the mutual-exclusion message first; pin that ordering.
    let db = setup_db();
    create_test_table(&db, "Tbl96");
    let req: QueryRequest = serde_json::from_value(serde_json::json!({
        "TableName": "Tbl96",
        "KeyConditionExpression": "pk = :pk",
        "ExpressionAttributeValues": {":pk": {"S": "a"}},
        "Select": "ALL_PROJECTED_ATTRIBUTES",
        "ProjectionExpression": "sk"
    }))
    .unwrap();
    let err = db.query(req).unwrap_err();
    assert!(
        err.to_string().contains(
            "Cannot specify the ProjectionExpression when choosing to get ALL_PROJECTED_ATTRIBUTES"
        ),
        "got: {err}"
    );
}

#[test]
fn scan_rejects_undefined_projection_name_on_zero_match() {
    let db = setup_db();
    create_test_table(&db, "ProjZeroScan");
    // No items inserted, so the scan matches nothing; the undefined name must
    // still reject rather than returning an empty result.
    let req: ScanRequest = serde_json::from_value(serde_json::json!({
        "TableName": "ProjZeroScan",
        "ProjectionExpression": "#undef"
    }))
    .unwrap();
    let err = db.scan(req).unwrap_err();
    assert!(
        err.to_string()
            .contains("An expression attribute name used in the document path is not defined"),
        "undefined projection name must reject even with zero matches, got: {err}"
    );
}

#[test]
fn query_rejects_overlapping_projection_paths_on_zero_match() {
    let db = setup_db();
    create_test_table(&db, "ProjOverlapQuery");
    let req: QueryRequest = serde_json::from_value(serde_json::json!({
        "TableName": "ProjOverlapQuery",
        "KeyConditionExpression": "pk = :pk",
        "ExpressionAttributeValues": { ":pk": { "S": "no-such" } },
        "ProjectionExpression": "#a, #a.#b",
        "ExpressionAttributeNames": { "#a": "a", "#b": "b" }
    }))
    .unwrap();
    let err = db.query(req).unwrap_err();
    assert!(
        err.to_string().contains("Two document paths overlap"),
        "overlapping projection paths must reject, got: {err}"
    );
}

// ---------------------------------------------------------------------------
// Expression size limit (4096 bytes) - FilterExpression on Query and Scan.
// Confirmed against real DynamoDB (eu-west-2).
// ---------------------------------------------------------------------------

// Filter and key-condition surfaces prefix the size error with
// `Invalid <Type>Expression:`, matching real DynamoDB (eu-west-2).
const FILTER_OVERSIZE_MSG: &str =
    "Invalid FilterExpression: Expression size has exceeded the maximum allowed size";
const KEY_CONDITION_OVERSIZE_MSG: &str =
    "Invalid KeyConditionExpression: Expression size has exceeded the maximum allowed size";

#[test]
fn test_query_filter_expression_over_size_limit_rejected() {
    let db = setup_db();
    create_test_table(&db, "Tbl");

    // 4097-byte FilterExpression: one byte past the 4096 limit.
    let filter = format!("{} = :v", "a".repeat(4092));
    assert_eq!(filter.len(), 4097);

    let req: QueryRequest = serde_json::from_value(serde_json::json!({
        "TableName": "Tbl",
        "KeyConditionExpression": "pk = :pk",
        "FilterExpression": filter,
        "ExpressionAttributeValues": {":pk": {"S": "a"}, ":v": {"S": "x"}}
    }))
    .unwrap();

    let err = db.query(req).unwrap_err();
    assert!(
        err.to_string().contains(FILTER_OVERSIZE_MSG),
        "unexpected error: {err}"
    );
}

#[test]
fn test_scan_filter_expression_over_size_limit_rejected() {
    let db = setup_db();
    create_test_table(&db, "Tbl");

    // 4097-byte FilterExpression.
    let filter = format!("{} = :v", "a".repeat(4092));
    assert_eq!(filter.len(), 4097);

    let req: ScanRequest = serde_json::from_value(serde_json::json!({
        "TableName": "Tbl",
        "FilterExpression": filter,
        "ExpressionAttributeValues": {":v": {"S": "x"}}
    }))
    .unwrap();

    let err = db.scan(req).unwrap_err();
    assert!(
        err.to_string().contains(FILTER_OVERSIZE_MSG),
        "unexpected error: {err}"
    );
}

#[test]
fn test_query_key_condition_expression_over_size_limit_rejected() {
    // KeyConditionExpression is subject to the same 4096-byte limit, and the
    // size check wins over syntax: real DynamoDB returns the size error even
    // when the key condition is otherwise malformed (here, `pk = :pk AND aaa…`
    // is invalid past the AND). Confirmed against real DynamoDB (eu-west-2).
    let db = setup_db();
    create_test_table(&db, "Tbl");

    // Over the 4096-byte limit; the size guard runs before parsing.
    let kce = format!("pk = :pk AND {}", "a".repeat(4090));
    assert!(kce.len() > 4096);

    let req: QueryRequest = serde_json::from_value(serde_json::json!({
        "TableName": "Tbl",
        "KeyConditionExpression": kce,
        "ExpressionAttributeValues": {":pk": {"S": "a"}}
    }))
    .unwrap();

    let err = db.query(req).unwrap_err();
    assert!(
        err.to_string().contains(KEY_CONDITION_OVERSIZE_MSG),
        "unexpected error: {err}"
    );
}

// =============================================================================
// {"NULL": false} in ExclusiveStartKey and ScanFilter
// =============================================================================

#[test]
fn test_scan_esk_null_false_starting_key_invalid() {
    // {"NULL": false} in ExclusiveStartKey keeps its starting-key message,
    // with no internal marker or serde position suffix leaking through.
    let db = setup_db();
    create_hash_only_table(&db, "Items");

    let req: ScanRequest = serde_json::from_value(serde_json::json!({
        "TableName": "Items",
        "ExclusiveStartKey": {"pk": {"NULL": false}}
    }))
    .unwrap();

    let err = db.scan(req).unwrap_err();
    assert_eq!(
        err.to_string(),
        "The provided starting key is invalid: \
         One or more parameter values were invalid: \
         Null attribute value types must have the value of true"
    );
}

#[test]
fn test_query_esk_null_false_starting_key_invalid() {
    let db = setup_db();
    create_hash_only_table(&db, "Items");

    let req: QueryRequest = serde_json::from_value(serde_json::json!({
        "TableName": "Items",
        "KeyConditionExpression": "pk = :pk",
        "ExpressionAttributeValues": {":pk": {"S": "a"}},
        "ExclusiveStartKey": {"pk": {"NULL": false}}
    }))
    .unwrap();

    let err = db.query(req).unwrap_err();
    assert_eq!(
        err.to_string(),
        "The provided starting key is invalid: \
         One or more parameter values were invalid: \
         Null attribute value types must have the value of true"
    );
}

#[test]
fn test_scan_filter_null_false_no_marker_leak() {
    // A {"NULL": false} inside a ScanFilter AttributeValueList fails to
    // deserialise; the raw filter validation lets it fall through and the
    // unparseable filter is dropped, so the scan succeeds. What matters here
    // is that the internal marker on the serde message never surfaces.
    let db = setup_db();
    create_hash_only_table(&db, "Items");

    put(&db, "Items", serde_json::json!({"pk": {"S": "k1"}}));

    let req: ScanRequest = serde_json::from_value(serde_json::json!({
        "TableName": "Items",
        "ScanFilter": {
            "flag": {"ComparisonOperator": "EQ", "AttributeValueList": [{"NULL": false}]}
        }
    }))
    .unwrap();

    match db.scan(req) {
        Ok(resp) => assert_eq!(resp.count, 1),
        Err(err) => panic!("scan unexpectedly failed: {err}"),
    }
}
