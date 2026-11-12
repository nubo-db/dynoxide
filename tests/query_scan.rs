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
        "1 validation error detected: Must specify the AttributesToGet or ProjectionExpression when choosing to get SPECIFIC_ATTRIBUTES",
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
// Operand types in a KeyConditionExpression
//
// begins_with takes a string or binary operand, and the ordering comparisons
// and BETWEEN take a string, number or binary one. DynamoDB rejects anything
// else before it resolves the table, reporting the operator as written.
// Captured eu-west-2, 2026-10-08.
// =============================================================================

/// Every operand type, as (value, type name).
fn every_operand_type() -> Vec<(serde_json::Value, &'static str)> {
    vec![
        (serde_json::json!({"S": "a"}), "S"),
        (serde_json::json!({"N": "1"}), "N"),
        (serde_json::json!({"B": "YQ=="}), "B"),
        (serde_json::json!({"BOOL": true}), "BOOL"),
        (serde_json::json!({"NULL": true}), "NULL"),
        (serde_json::json!({"L": [{"S": "a"}]}), "L"),
        (serde_json::json!({"M": {"k": {"S": "v"}}}), "M"),
        (serde_json::json!({"SS": ["a"]}), "SS"),
        (serde_json::json!({"NS": ["1"]}), "NS"),
        (serde_json::json!({"BS": ["YQ=="]}), "BS"),
    ]
}

fn operand_type_error(op: &str, type_name: &str) -> String {
    format!(
        "Invalid KeyConditionExpression: Incorrect operand type for operator or function; \
         operator or function: {op}, operand type: {type_name}"
    )
}

/// A Query on `table` with `pk = :pk AND <sk_condition>`, `:v` bound to `value`.
fn query_key_condition(
    db: &Database,
    table: &str,
    key_condition: &str,
    value: serde_json::Value,
) -> Result<dynoxide::actions::query::QueryResponse, dynoxide::errors::DynoxideError> {
    db.query(
        serde_json::from_value(serde_json::json!({
            "TableName": table,
            "KeyConditionExpression": key_condition,
            "ExpressionAttributeValues": {":pk": {"S": "a"}, ":v": value}
        }))
        .unwrap(),
    )
}

#[test]
fn begins_with_in_a_key_condition_rejects_an_operand_that_is_not_a_string_or_binary() {
    // Before the fix a number was run as a prefix match on the stored key, and
    // a type with no key form became an empty prefix that matched the whole
    // partition.
    let db = setup_db();
    create_test_table(&db, "BwOperand");
    put(
        &db,
        "BwOperand",
        serde_json::json!({"pk": {"S": "a"}, "sk": {"S": "1x"}}),
    );

    let mut checked = 0;
    for (value, type_name) in every_operand_type() {
        if matches!(type_name, "S" | "B") {
            continue;
        }
        let err = query_key_condition(&db, "BwOperand", "pk = :pk AND begins_with(sk, :v)", value)
            .expect_err("a begins_with operand that is not S or B is rejected")
            .to_string();
        assert!(
            err.contains(&operand_type_error("begins_with", type_name)),
            "for {type_name}: got {err}"
        );
        checked += 1;
    }
    assert_eq!(checked, 8);
}

#[test]
fn an_ordering_key_condition_rejects_an_operand_with_no_ordering() {
    let db = setup_db();
    create_test_table(&db, "OrdOperand");
    put(
        &db,
        "OrdOperand",
        serde_json::json!({"pk": {"S": "a"}, "sk": {"S": "m"}}),
    );

    let mut checked = 0;
    for (value, type_name) in every_operand_type() {
        if matches!(type_name, "S" | "N" | "B") {
            continue;
        }
        for op in ["<", "<=", ">", ">="] {
            let err = query_key_condition(
                &db,
                "OrdOperand",
                &format!("pk = :pk AND sk {op} :v"),
                value.clone(),
            )
            .expect_err("an unorderable operand is rejected")
            .to_string();
            assert!(
                err.contains(&operand_type_error(op, type_name)),
                "for {op} on {type_name}: got {err}"
            );
            checked += 1;
        }
    }
    assert_eq!(checked, 28);
}

#[test]
fn between_in_a_key_condition_rejects_an_operand_with_no_ordering_on_either_bound() {
    // The operand check comes before the same-type check, so a BOOL against a
    // number reports the BOOL rather than the mismatch.
    let db = setup_db();
    create_test_table(&db, "BetweenOperand");
    for (lo, hi) in [
        (
            serde_json::json!({"BOOL": true}),
            serde_json::json!({"BOOL": false}),
        ),
        (
            serde_json::json!({"BOOL": true}),
            serde_json::json!({"N": "1"}),
        ),
        (
            serde_json::json!({"N": "1"}),
            serde_json::json!({"BOOL": true}),
        ),
    ] {
        let err = db
            .query(
                serde_json::from_value(serde_json::json!({
                    "TableName": "BetweenOperand",
                    "KeyConditionExpression": "pk = :pk AND sk BETWEEN :lo AND :hi",
                    "ExpressionAttributeValues": {":pk": {"S": "a"}, ":lo": lo, ":hi": hi}
                }))
                .unwrap(),
            )
            .expect_err("an unorderable BETWEEN bound is rejected")
            .to_string();
        assert!(
            err.contains(&operand_type_error("BETWEEN", "BOOL")),
            "got {err}"
        );
    }
}

#[test]
fn a_reversed_key_comparison_reports_the_operator_as_written() {
    // `:v < sk` is stored as `sk > :v`, but DynamoDB names the `<` it was given.
    let db = setup_db();
    create_test_table(&db, "ReversedOperand");
    for op in ["<", "<=", ">", ">="] {
        let err = query_key_condition(
            &db,
            "ReversedOperand",
            &format!("pk = :pk AND :v {op} sk"),
            serde_json::json!({"BOOL": true}),
        )
        .expect_err("an unorderable operand is rejected")
        .to_string();
        assert!(
            err.contains(&operand_type_error(op, "BOOL")),
            "for {op}: got {err}"
        );
    }
}

#[test]
fn the_key_condition_operand_check_runs_before_the_table_and_key_schema() {
    let db = setup_db();
    create_test_table(&db, "OperandOrder");
    let n = || serde_json::json!({"N": "1"});
    let begins_with_n = operand_type_error("begins_with", "N");

    // A table that does not exist.
    let err = query_key_condition(&db, "NoSuchTable", "pk = :pk AND begins_with(sk, :v)", n())
        .unwrap_err()
        .to_string();
    assert!(err.contains(&begins_with_n), "missing table: got {err}");

    // An attribute that is not the sort key.
    let err = query_key_condition(&db, "OperandOrder", "pk = :pk AND begins_with(x, :v)", n())
        .unwrap_err()
        .to_string();
    assert!(err.contains(&begins_with_n), "non-key attribute: got {err}");

    // begins_with on the partition key, alone and alongside a sort key clause.
    for kc in ["begins_with(pk, :v)", "begins_with(pk, :v) AND sk = :pk"] {
        let err = query_key_condition(&db, "OperandOrder", kc, n())
            .unwrap_err()
            .to_string();
        assert!(err.contains(&begins_with_n), "{kc}: got {err}");
    }

    // An index the table does not have.
    let err = db
        .query(
            serde_json::from_value(serde_json::json!({
                "TableName": "OperandOrder",
                "IndexName": "nope",
                "KeyConditionExpression": "pk = :pk AND begins_with(sk, :v)",
                "ExpressionAttributeValues": {":pk": {"S": "a"}, ":v": n()}
            }))
            .unwrap(),
        )
        .unwrap_err()
        .to_string();
    assert!(err.contains(&begins_with_n), "missing index: got {err}");

    // An unused value, which DynamoDB reports only once the operands pass.
    let err = db
        .query(
            serde_json::from_value(serde_json::json!({
                "TableName": "OperandOrder",
                "KeyConditionExpression": "pk = :pk AND begins_with(sk, :v)",
                "ExpressionAttributeValues": {":pk": {"S": "a"}, ":v": n(), ":unused": n()}
            }))
            .unwrap(),
        )
        .unwrap_err()
        .to_string();
    assert!(err.contains(&begins_with_n), "unused value: got {err}");
}

#[test]
fn a_key_condition_syntax_error_wins_over_an_operand_type() {
    // DynamoDB parses the whole expression before it looks at operand types.
    let db = setup_db();
    create_test_table(&db, "OperandSyntax");
    let err = query_key_condition(
        &db,
        "OperandSyntax",
        "pk = :pk AND begins_with(sk, :v) junk",
        serde_json::json!({"N": "1"}),
    )
    .unwrap_err()
    .to_string();
    // The syntax error comes in DynamoDB's token and near form (captured
    // us-east-1 and eu-west-2, 2026-10-09).
    assert_eq!(
        err,
        "Invalid KeyConditionExpression: Syntax error; token: \"junk\", near: \") junk\""
    );
}

#[test]
fn a_third_key_condition_clause_still_has_its_operands_checked() {
    // DynamoDB parses every AND clause and checks the operands before it
    // objects to the number of clauses, so the bad operand is what it reports.
    let db = setup_db();
    create_test_table(&db, "OperandThirdClause");
    let err = query_key_condition(
        &db,
        "OperandThirdClause",
        "pk = :pk AND begins_with(sk, :v) AND x = :pk",
        serde_json::json!({"N": "1"}),
    )
    .unwrap_err()
    .to_string();
    assert!(
        err.contains(&operand_type_error("begins_with", "N")),
        "got {err}"
    );

    // With every operand acceptable, the third clause is still refused, counted
    // against the two attributes of the table's key (captured eu-west-2,
    // 2026-10-08, for `pk = :p AND sk = :s AND x = :p`).
    let err = query_key_condition(
        &db,
        "OperandThirdClause",
        "pk = :pk AND begins_with(sk, :v) AND x = :pk",
        serde_json::json!({"S": "1"}),
    )
    .unwrap_err()
    .to_string();
    assert!(
        err.contains(
            "The number of query conditions (3) exceeds the number of key attributes \
             defined in the schema (2)"
        ),
        "got {err}"
    );
}

#[test]
fn a_second_key_condition_on_a_hash_only_table_is_counted_against_its_one_key_attribute() {
    // The same rule as the three-clause case above, applied to a table whose
    // key has one attribute.
    let db = setup_db();
    create_hash_only_table(&db, "OneKeyAttribute");
    let err = query_key_condition(
        &db,
        "OneKeyAttribute",
        "pk = :pk AND x = :v",
        serde_json::json!({"S": "1"}),
    )
    .unwrap_err()
    .to_string();
    assert!(
        err.contains(
            "The number of query conditions (2) exceeds the number of key attributes \
             defined in the schema (1)"
        ),
        "got {err}"
    );
}

#[test]
fn a_key_condition_may_name_the_sort_key_before_the_partition_key() {
    let db = setup_db();
    create_test_table(&db, "SortKeyFirst");
    put(
        &db,
        "SortKeyFirst",
        serde_json::json!({"pk": {"S": "a"}, "sk": {"S": "1"}}),
    );
    let response = query_key_condition(
        &db,
        "SortKeyFirst",
        "sk = :v AND pk = :pk",
        serde_json::json!({"S": "1"}),
    )
    .unwrap();
    assert_eq!(response.count, 1);
}

#[test]
fn begins_with_on_a_sort_key_is_case_sensitive() {
    // DynamoDB compares string keys byte by byte, so a lower-case prefix does
    // not match an upper-case key. SQLite's LIKE folds ASCII case, and the
    // key condition was pushed into the read as a LIKE.
    let db = setup_db();
    create_test_table(&db, "PrefixCase");
    for sk in ["ABCdef", "abcdef", "abd"] {
        put(
            &db,
            "PrefixCase",
            serde_json::json!({"pk": {"S": "a"}, "sk": {"S": sk}}),
        );
    }
    let response = query_key_condition(
        &db,
        "PrefixCase",
        "pk = :pk AND begins_with(sk, :v)",
        serde_json::json!({"S": "abc"}),
    )
    .unwrap();
    let got: Vec<_> = response
        .items
        .unwrap()
        .into_iter()
        .map(|i| i.get("sk").cloned())
        .collect();
    assert_eq!(got, vec![Some(AttributeValue::S("abcdef".into()))]);
}

#[test]
fn a_bad_operand_in_the_third_key_condition_clause_is_reported() {
    // The operands of every clause are checked before the clause count, so a
    // bad one in the third clause outranks the extra clause too.
    let db = setup_db();
    create_test_table(&db, "OperandInThirdClause");
    let err = query_key_condition(
        &db,
        "OperandInThirdClause",
        "pk = :pk AND sk > :pk AND begins_with(x, :v)",
        serde_json::json!({"N": "1"}),
    )
    .unwrap_err()
    .to_string();
    assert!(
        err.contains(&operand_type_error("begins_with", "N")),
        "got {err}"
    );
}

#[test]
fn the_first_bad_key_condition_operand_in_written_order_is_reported() {
    let db = setup_db();
    create_test_table(&db, "OperandWrittenOrder");
    let err = db
        .query(
            serde_json::from_value(serde_json::json!({
                "TableName": "OperandWrittenOrder",
                "KeyConditionExpression": "begins_with(sk, :n) AND pk < :b",
                "ExpressionAttributeValues": {":n": {"N": "1"}, ":b": {"BOOL": true}}
            }))
            .unwrap(),
        )
        .unwrap_err()
        .to_string();
    assert!(
        err.contains(&operand_type_error("begins_with", "N")),
        "got {err}"
    );
}

#[test]
fn a_binary_sort_key_takes_a_binary_operand_in_every_key_condition() {
    // Binary orders and has prefixes, so begins_with, the ordering comparisons
    // and BETWEEN all accept it. begins_with on a binary sort key is captured
    // eu-west-2, 2026-10-08.
    let db = setup_db();
    db.create_table(
        serde_json::from_value(serde_json::json!({
            "TableName": "BinarySk",
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"}
            ],
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "B"}
            ],
            "BillingMode": "PAY_PER_REQUEST"
        }))
        .unwrap(),
    )
    .unwrap();
    // "a", "ab" and "b".
    for sk in ["YQ==", "YWI=", "Yg=="] {
        put(
            &db,
            "BinarySk",
            serde_json::json!({"pk": {"S": "a"}, "sk": {"B": sk}}),
        );
    }

    let count = |kc: &str, values: serde_json::Value| {
        let mut eav = serde_json::json!({":pk": {"S": "a"}});
        eav.as_object_mut()
            .unwrap()
            .extend(values.as_object().unwrap().clone());
        db.query(
            serde_json::from_value(serde_json::json!({
                "TableName": "BinarySk",
                "KeyConditionExpression": kc,
                "ExpressionAttributeValues": eav
            }))
            .unwrap(),
        )
        .unwrap_or_else(|e| panic!("{kc} must accept a binary operand, got {e}"))
        .count
    };
    assert_eq!(
        count(
            "pk = :pk AND begins_with(sk, :v)",
            serde_json::json!({":v": {"B": "YQ=="}})
        ),
        2
    );
    assert_eq!(
        count(
            "pk = :pk AND sk < :v",
            serde_json::json!({":v": {"B": "Yg=="}})
        ),
        2
    );
    assert_eq!(
        count(
            "pk = :pk AND sk BETWEEN :lo AND :hi",
            serde_json::json!({":lo": {"B": "YQ=="}, ":hi": {"B": "YWI="}})
        ),
        2
    );
}

#[test]
fn an_undefined_key_condition_value_is_reported_ahead_of_an_operand_type() {
    // The undefined value sits in the partition key clause, ahead of the bad
    // operand, and DynamoDB reports it rather than the operand type.
    let db = setup_db();
    create_test_table(&db, "OperandUndefined");
    let err = db
        .query(
            serde_json::from_value(serde_json::json!({
                "TableName": "OperandUndefined",
                "KeyConditionExpression": "pk = :undef AND begins_with(sk, :v)",
                "ExpressionAttributeValues": {":v": {"N": "1"}}
            }))
            .unwrap(),
        )
        .unwrap_err()
        .to_string();
    assert!(
        err.contains(
            "An expression attribute value used in expression is not defined; \
             attribute value: :undef"
        ),
        "got {err}"
    );
}

#[test]
fn a_bad_operand_ahead_of_an_undefined_value_is_reported_first() {
    // The reverse order: DynamoDB reports whichever problem comes first as it
    // walks the clauses, so a bad operand ahead of an undefined value wins.
    // Captured eu-west-2, 2026-10-09.
    let db = setup_db();
    create_test_table(&db, "OperandFirst");
    let err = db
        .query(
            serde_json::from_value(serde_json::json!({
                "TableName": "OperandFirst",
                "KeyConditionExpression": "begins_with(sk, :n) AND pk = :missing",
                "ExpressionAttributeValues": {":n": {"N": "1"}}
            }))
            .unwrap(),
        )
        .unwrap_err()
        .to_string();
    assert!(
        err.contains(
            "Invalid KeyConditionExpression: Incorrect operand type for operator or function; \
             operator or function: begins_with, operand type: N"
        ),
        "got {err}"
    );
}

#[test]
fn a_bad_operand_in_a_middle_clause_is_reported_ahead_of_a_later_undefined_value() {
    // Three clauses, with the bad operand in the second and an undefined value
    // in the third. DynamoDB reports the operand, not the undefined value and
    // not the third clause. Captured eu-west-2, 2026-10-09.
    let db = setup_db();
    create_test_table(&db, "OperandMiddle");
    let err = db
        .query(
            serde_json::from_value(serde_json::json!({
                "TableName": "OperandMiddle",
                "KeyConditionExpression": "pk = :pk AND begins_with(sk, :n) AND sk = :missing",
                "ExpressionAttributeValues": {":pk": {"S": "a"}, ":n": {"N": "1"}}
            }))
            .unwrap(),
        )
        .unwrap_err()
        .to_string();
    assert!(
        err.contains(
            "Invalid KeyConditionExpression: Incorrect operand type for operator or function; \
             operator or function: begins_with, operand type: N"
        ),
        "got {err}"
    );
}

#[test]
fn begins_with_in_a_key_condition_accepts_a_string_operand() {
    let db = setup_db();
    create_test_table(&db, "BwString");
    put(
        &db,
        "BwString",
        serde_json::json!({"pk": {"S": "a"}, "sk": {"S": "1x"}}),
    );
    put(
        &db,
        "BwString",
        serde_json::json!({"pk": {"S": "a"}, "sk": {"S": "2x"}}),
    );
    let resp = query_key_condition(
        &db,
        "BwString",
        "pk = :pk AND begins_with(sk, :v)",
        serde_json::json!({"S": "1"}),
    )
    .unwrap();
    assert_eq!(resp.count, 1);
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
    // A Query reports {"NULL": false} without the starting-key prefix a Scan
    // can add (eu-west-2 and us-east-1, 2026-10-09).
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
        "One or more parameter values were invalid: \
         Null attribute value types must have the value of true"
    );
}

#[test]
fn test_scan_filter_null_false_is_refused() {
    // A {"NULL": false} inside a ScanFilter AttributeValueList is refused, bare
    // and with no internal marker, ahead of the condition's operator checks
    // (captured eu-west-2 and us-east-1, 2026-10-10). It used to fail to read
    // and be dropped, so the scan ran unfiltered and returned every item.
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

    let err = db.scan(req).unwrap_err();
    assert_eq!(
        err.to_string(),
        "One or more parameter values were invalid: \
         Null attribute value types must have the value of true"
    );
}

/// A table keyed on a string partition key and a number sort key.
fn create_number_sort_key_table(db: &Database, name: &str) {
    let req: CreateTableRequest = serde_json::from_value(serde_json::json!({
        "TableName": name,
        "KeySchema": [
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"}
        ],
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "N"}
        ],
        "BillingMode": "PAY_PER_REQUEST"
    }))
    .unwrap();
    db.create_table(req).unwrap();
}

const SCHEMA_TYPE_MISMATCH: &str = "One or more parameter values were invalid: Condition parameter type does not match schema type";

#[test]
fn a_key_condition_value_of_another_key_type_fails_the_schema_check() {
    // Captured eu-west-2, October 2026. A string is a fine operand for
    // begins_with and a boolean for `=`, so neither is an operand error; both
    // fail once the condition is checked against the number sort key.
    let db = setup_db();
    create_number_sort_key_table(&db, "NumberSortKey");
    for (key_condition, value) in [
        (
            "pk = :pk AND begins_with(sk, :v)",
            serde_json::json!({"S": "a"}),
        ),
        ("pk = :pk AND sk = :v", serde_json::json!({"BOOL": true})),
        ("pk = :pk AND sk > :v", serde_json::json!({"S": "1"})),
    ] {
        let err = query_key_condition(&db, "NumberSortKey", key_condition, value)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains(SCHEMA_TYPE_MISMATCH),
            "{key_condition}: got {err}"
        );
    }
}

#[test]
fn a_partition_key_value_of_another_key_type_fails_the_schema_check() {
    let db = setup_db();
    create_number_sort_key_table(&db, "NumberSortKeyPk");
    let err = db
        .query(
            serde_json::from_value(serde_json::json!({
                "TableName": "NumberSortKeyPk",
                "KeyConditionExpression": "pk = :pk",
                "ExpressionAttributeValues": {":pk": {"N": "1"}}
            }))
            .unwrap(),
        )
        .unwrap_err()
        .to_string();
    assert!(err.contains(SCHEMA_TYPE_MISMATCH), "got {err}");
}

#[test]
fn two_conditions_on_one_key_attribute_are_refused() {
    // Captured eu-west-2, October 2026: reported ahead of the clause count,
    // although three clauses exceed the two key attributes.
    let db = setup_db();
    create_number_sort_key_table(&db, "OneConditionPerKey");
    let err = db
        .query(
            serde_json::from_value(serde_json::json!({
                "TableName": "OneConditionPerKey",
                "KeyConditionExpression": "pk = :pk AND sk > :a AND sk < :b",
                "ExpressionAttributeValues": {
                    ":pk": {"S": "q"}, ":a": {"N": "1"}, ":b": {"N": "5"}
                }
            }))
            .unwrap(),
        )
        .unwrap_err()
        .to_string();
    assert!(
        err.contains(
            "Invalid KeyConditionExpression: KeyConditionExpressions must only contain one \
             condition per key"
        ),
        "got {err}"
    );
}

#[test]
fn a_range_condition_on_a_one_attribute_partition_key_is_not_supported() {
    // Captured eu-west-2, 2026-10-08 (`begins_with(pk, :v)`) and 2026-10-09
    // (`pk > :a AND sk = :b`).
    let db = setup_db();
    create_test_table(&db, "RangeOnPartition");
    // Each request defines only the values its condition uses, as an unused
    // one is refused ahead of the key schema check.
    for (key_condition, values) in [
        (
            "pk > :pk AND sk = :v",
            serde_json::json!({":pk": {"S": "a"}, ":v": {"S": "1"}}),
        ),
        ("begins_with(pk, :v)", serde_json::json!({":v": {"S": "1"}})),
    ] {
        let err = db
            .query(
                serde_json::from_value(serde_json::json!({
                    "TableName": "RangeOnPartition",
                    "KeyConditionExpression": key_condition,
                    "ExpressionAttributeValues": values
                }))
                .unwrap(),
            )
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("Query key condition not supported"),
            "{key_condition}: got {err}"
        );
    }
}

#[test]
fn a_key_condition_problem_that_needs_the_key_waits_for_the_table_and_index() {
    // Captured eu-west-2 and us-east-1, 2026-10-09. A missing table reports
    // itself, and a missing index names itself, ahead of every key condition
    // problem that has to be checked against the key. Only a second condition
    // on one attribute is refused first, since that needs no key.
    let db = setup_db();
    create_number_sort_key_table(&db, "OrderingTable");
    let shapes: [(&str, serde_json::Value); 5] = [
        ("pk > :a", serde_json::json!({":a": {"S": "a"}})),
        ("begins_with(pk, :a)", serde_json::json!({":a": {"S": "a"}})),
        (
            "pk > :a AND sk = :b",
            serde_json::json!({":a": {"S": "a"}, ":b": {"N": "1"}}),
        ),
        (
            "pk = :a AND sk > :b AND x = :c",
            serde_json::json!({":a": {"S": "a"}, ":b": {"N": "1"}, ":c": {"S": "c"}}),
        ),
        (
            "pk = :a AND sk = :b",
            serde_json::json!({":a": {"S": "a"}, ":b": {"S": "x"}}),
        ),
    ];
    for (key_condition, values) in &shapes {
        let missing_table = db
            .query(
                serde_json::from_value(serde_json::json!({
                    "TableName": "NoSuchTable",
                    "KeyConditionExpression": key_condition,
                    "ExpressionAttributeValues": values
                }))
                .unwrap(),
            )
            .unwrap_err();
        assert!(
            matches!(
                missing_table,
                dynoxide::errors::DynoxideError::ResourceNotFoundException(_)
            ),
            "{key_condition}: got {missing_table}"
        );
        let missing_index = db
            .query(
                serde_json::from_value(serde_json::json!({
                    "TableName": "OrderingTable",
                    "IndexName": "nope",
                    "KeyConditionExpression": key_condition,
                    "ExpressionAttributeValues": values
                }))
                .unwrap(),
            )
            .unwrap_err()
            .to_string();
        assert!(
            missing_index.contains("The table does not have the specified index: nope"),
            "{key_condition}: got {missing_index}"
        );
    }
    let duplicate = db
        .query(
            serde_json::from_value(serde_json::json!({
                "TableName": "NoSuchTable",
                "KeyConditionExpression": "pk = :a AND pk = :b",
                "ExpressionAttributeValues": {":a": {"S": "a"}, ":b": {"S": "b"}}
            }))
            .unwrap(),
        )
        .unwrap_err()
        .to_string();
    assert!(
        duplicate.contains("KeyConditionExpressions must only contain one condition per key"),
        "got {duplicate}"
    );
}
