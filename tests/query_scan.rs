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
    assert!(
        err.to_string().contains("SPECIFIC_ATTRIBUTES"),
        "expected error mentioning SPECIFIC_ATTRIBUTES, got: {}",
        err
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
    assert!(
        err.to_string().contains("SPECIFIC_ATTRIBUTES"),
        "expected error mentioning SPECIFIC_ATTRIBUTES, got: {}",
        err
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
