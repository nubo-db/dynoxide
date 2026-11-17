//! Query on a table whose key is a partition key alone.
//!
//! A partition of such a table holds at most one item, so a Query of it never
//! returns a `LastEvaluatedKey`, whatever the `Limit`, direction, filter or
//! `Select`, and any well-formed `ExclusiveStartKey` is refused with
//! "The provided exclusive start key is invalid". A malformed starting key
//! keeps its own answer. Indexes of such a table page as any index does. All
//! captured on DynamoDB in eu-west-2 and us-east-1 on 14 November 2026.

use dynoxide::types::AttributeValue;
use dynoxide::{Database, DynoxideError};
use serde_json::{Value, json};
use std::collections::HashMap;

const PARTITIONS: [&str; 3] = ["p1", "p2", "p3"];
const PAGE_STOP: usize = 10;
const REFUSED: &str = "The provided exclusive start key is invalid";
const MALFORMED: &str = "The provided starting key is invalid";

fn create(db: &Database, req: Value) {
    db.create_table(serde_json::from_value(req).unwrap())
        .unwrap();
}

fn put(db: &Database, table: &str, item: Value) {
    db.put_item(serde_json::from_value(json!({"TableName": table, "Item": item})).unwrap())
        .unwrap();
}

/// `hashonly`, keyed on `pk` alone, with one item in each partition, all
/// under one value of `g`. It has a GSI on `g` alone and one on `g` and `r`.
fn hash_only_db() -> Database {
    let db = Database::memory().unwrap();
    create(
        &db,
        json!({
            "TableName": "hashonly",
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "g", "AttributeType": "S"},
                {"AttributeName": "r", "AttributeType": "S"}
            ],
            "GlobalSecondaryIndexes": [
                {
                    "IndexName": "g-hash",
                    "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "ALL"}
                },
                {
                    "IndexName": "g-range",
                    "KeySchema": [
                        {"AttributeName": "g", "KeyType": "HASH"},
                        {"AttributeName": "r", "KeyType": "RANGE"}
                    ],
                    "Projection": {"ProjectionType": "ALL"}
                }
            ],
            "BillingMode": "PAY_PER_REQUEST"
        }),
    );
    for p in PARTITIONS {
        put(
            &db,
            "hashonly",
            json!({"pk": {"S": p}, "g": {"S": "G"}, "r": {"S": format!("r-{p}")}, "x": {"N": "1"}}),
        );
    }
    db
}

/// `ranged`, keyed on `pk` and `sk`, with two items in each partition and an
/// LSI on `pk` and `l`.
fn ranged_db() -> Database {
    let db = Database::memory().unwrap();
    create(
        &db,
        json!({
            "TableName": "ranged",
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"}
            ],
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
                {"AttributeName": "l", "AttributeType": "S"}
            ],
            "LocalSecondaryIndexes": [{
                "IndexName": "byl",
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "l", "KeyType": "RANGE"}
                ],
                "Projection": {"ProjectionType": "ALL"}
            }],
            "BillingMode": "PAY_PER_REQUEST"
        }),
    );
    for p in PARTITIONS {
        for s in ["s1", "s2"] {
            put(
                &db,
                "ranged",
                json!({"pk": {"S": p}, "sk": {"S": s}, "l": {"S": format!("l-{s}")}, "x": {"N": "1"}}),
            );
        }
    }
    db
}

fn query(db: &Database, req: &Value) -> dynoxide::actions::query::QueryResponse {
    db.query(serde_json::from_value(req.clone()).unwrap())
        .unwrap_or_else(|e| panic!("{req}: {e}"))
}

fn query_error(db: &Database, req: &Value) -> String {
    match db.query(serde_json::from_value(req.clone()).unwrap()) {
        Ok(resp) => panic!("{req}: answered with {} items, not an error", resp.count),
        Err(DynoxideError::ValidationException(message)) => message,
        Err(other) => panic!("{req}: {other}"),
    }
}

fn pk_of(item: &HashMap<String, AttributeValue>) -> String {
    match item.get("pk") {
        Some(AttributeValue::S(pk)) => pk.clone(),
        other => panic!("no string pk: {other:?}"),
    }
}

/// What a walk saw: the `LastEvaluatedKey` of every page, in order, and the
/// `pk` of every item returned.
struct Walk {
    keys: Vec<Option<HashMap<String, AttributeValue>>>,
    items: Vec<String>,
}

impl Walk {
    fn pages(&self) -> usize {
        self.keys.len()
    }
}

/// Follow `LastEvaluatedKey` from `base` until a page carries none, failing
/// at the tenth page so a walk that never ends cannot hang the test.
fn walk(db: &Database, base: &Value) -> Walk {
    let mut start: Option<HashMap<String, AttributeValue>> = None;
    let mut walk = Walk {
        keys: Vec::new(),
        items: Vec::new(),
    };
    loop {
        assert!(
            walk.pages() < PAGE_STOP,
            "Query walk did not end within {PAGE_STOP} pages: {base}"
        );
        let mut req = base.clone();
        if let Some(ref key) = start {
            req["ExclusiveStartKey"] = serde_json::to_value(key).unwrap();
        }
        let resp = query(db, &req);
        walk.items
            .extend(resp.items.unwrap_or_default().iter().map(pk_of));
        walk.keys.push(resp.last_evaluated_key.clone());
        match resp.last_evaluated_key {
            Some(key) => start = Some(key),
            None => return walk,
        }
    }
}

fn key_condition_query(p: &str) -> Value {
    json!({
        "TableName": "hashonly",
        "KeyConditionExpression": "pk = :p",
        "ExpressionAttributeValues": {":p": {"S": p}}
    })
}

/// A kind of Query of one partition, with the `Count` and `ScannedCount`
/// DynamoDB answers and whether the item comes back.
struct Variant {
    name: &'static str,
    req: Value,
    count: usize,
    scanned: usize,
    returns_item: bool,
}

/// Every kind of Query of partition `p`: seven shapes, each ascending and
/// descending, at `Limit` 1, 2 and none.
fn variants(p: &str) -> Vec<Variant> {
    let plain = key_condition_query(p);
    let mut matching = plain.clone();
    matching["FilterExpression"] = json!("x = :o");
    matching["ExpressionAttributeValues"][":o"] = json!({"N": "1"});
    let mut missing = plain.clone();
    missing["FilterExpression"] = json!("x = :o");
    missing["ExpressionAttributeValues"][":o"] = json!({"N": "7"});
    let mut count = plain.clone();
    count["Select"] = json!("COUNT");
    let mut all = plain.clone();
    all["Select"] = json!("ALL_ATTRIBUTES");
    let mut specific = plain.clone();
    specific["ProjectionExpression"] = json!("pk, x");
    let legacy = json!({
        "TableName": "hashonly",
        "KeyConditions": {
            "pk": {"ComparisonOperator": "EQ", "AttributeValueList": [{"S": p}]}
        }
    });
    let shapes = [
        ("no filter", plain, 1, true),
        ("a filter that matches", matching, 1, true),
        ("a filter that does not match", missing, 0, false),
        ("Select COUNT", count, 1, false),
        ("Select ALL_ATTRIBUTES", all, 1, true),
        ("a ProjectionExpression", specific, 1, true),
        ("KeyConditions", legacy, 1, true),
    ];
    let mut out = Vec::new();
    for forward in [true, false] {
        for (name, req, count, returns_item) in &shapes {
            for limit in [Some(1), Some(2), None] {
                let mut req = req.clone();
                req["ScanIndexForward"] = json!(forward);
                if let Some(l) = limit {
                    req["Limit"] = json!(l);
                }
                out.push(Variant {
                    name,
                    req,
                    count: *count,
                    scanned: 1,
                    returns_item: *returns_item,
                });
            }
        }
    }
    out
}

#[test]
fn a_query_of_a_table_without_a_sort_key_never_returns_a_key() {
    let db = hash_only_db();
    let mut checked = 0;
    for p in PARTITIONS {
        for v in variants(p) {
            let (name, req) = (v.name, &v.req);
            let resp = query(&db, req);
            assert!(
                resp.last_evaluated_key.is_none(),
                "{name}: a LastEvaluatedKey on {req}"
            );
            assert_eq!(resp.count, v.count, "{name}: Count on {req}");
            assert_eq!(
                resp.scanned_count, v.scanned,
                "{name}: ScannedCount on {req}"
            );
            let got: Vec<String> = resp.items.unwrap_or_default().iter().map(pk_of).collect();
            let want = if v.returns_item {
                vec![p.to_string()]
            } else {
                vec![]
            };
            assert_eq!(got, want, "{name}: items on {req}");
            checked += 1;
        }
    }
    assert_eq!(checked, PARTITIONS.len() * 42);
}

#[test]
fn every_walk_of_a_table_without_a_sort_key_is_one_page() {
    let db = hash_only_db();
    for p in PARTITIONS {
        for v in variants(p) {
            assert_eq!(walk(&db, &v.req).pages(), 1, "{}: {}", v.name, v.req);
        }
    }
}

/// The starting-key cases DynamoDB refuses, each on its own Query.
fn refused_cases() -> Vec<(&'static str, Value)> {
    let with_key = |p: &str, esk: &str| {
        let mut req = key_condition_query(p);
        req["ExclusiveStartKey"] = json!({"pk": {"S": esk}});
        req
    };
    let own = with_key("p1", "p1");
    let mut own_limit = own.clone();
    own_limit["Limit"] = json!(1);
    let mut descending = own_limit.clone();
    descending["ScanIndexForward"] = json!(false);
    let mut filtered = own_limit.clone();
    filtered["FilterExpression"] = json!("x = :o");
    filtered["ExpressionAttributeValues"][":o"] = json!({"N": "1"});
    let mut count = own.clone();
    count["Select"] = json!("COUNT");
    let mut eventual = own_limit.clone();
    eventual["ConsistentRead"] = json!(false);
    let mut strong = own_limit.clone();
    strong["ConsistentRead"] = json!(true);
    let mut empty_partition = with_key("p9", "p9");
    empty_partition["Limit"] = json!(1);
    let mut other_with_item = with_key("p1", "p2");
    other_with_item["Limit"] = json!(1);
    let mut other_without_item = with_key("p1", "p9");
    other_without_item["Limit"] = json!(1);
    let legacy = json!({
        "TableName": "hashonly",
        "KeyConditions": {
            "pk": {"ComparisonOperator": "EQ", "AttributeValueList": [{"S": "p1"}]}
        },
        "Limit": 1,
        "ExclusiveStartKey": {"pk": {"S": "p1"}}
    });
    vec![
        ("the item's own key, Limit 1", own_limit),
        ("the item's own key, no Limit", own),
        ("the item's own key, descending", descending),
        ("the item's own key, with a filter", filtered),
        ("the item's own key, Select COUNT", count),
        ("the item's own key, eventually consistent", eventual),
        ("the item's own key, strongly consistent", strong),
        (
            "key and condition both name a partition with no item",
            empty_partition,
        ),
        ("another partition that has an item", other_with_item),
        ("another partition with no item", other_without_item),
        ("the item's own key, through KeyConditions", legacy),
    ]
}

#[test]
fn any_well_formed_start_key_on_a_table_without_a_sort_key_is_refused() {
    let db = hash_only_db();
    let cases = refused_cases();
    for (name, req) in &cases {
        assert_eq!(query_error(&db, req), REFUSED, "{name}");
    }
    assert_eq!(cases.len(), 11);
}

#[test]
fn a_malformed_start_key_keeps_its_own_answer() {
    let db = hash_only_db();
    let cases = [
        (
            "an extra attribute",
            json!({"pk": {"S": "p1"}, "x": {"N": "1"}}),
        ),
        ("a number key", json!({"pk": {"N": "1"}})),
        ("no attributes", json!({})),
        (
            "another attribute in place of the key",
            json!({"x": {"N": "1"}}),
        ),
    ];
    for (name, esk) in cases {
        let mut req = key_condition_query("p1");
        req["Limit"] = json!(1);
        req["ExclusiveStartKey"] = esk;
        assert_eq!(query_error(&db, &req), MALFORMED, "{name}");
    }
}

/// Walk an index of `hashonly` at `Limit: 1`: the first page carries a key
/// holding the index and table keys, the walk returns every item once, and
/// resuming from the first page's key returns the next item.
fn assert_index_pages(db: &Database, index: &str, forward: bool) {
    let req = json!({
        "TableName": "hashonly",
        "IndexName": index,
        "KeyConditionExpression": "g = :g",
        "ExpressionAttributeValues": {":g": {"S": "G"}},
        "ScanIndexForward": forward,
        "Limit": 1
    });
    let walk = walk(db, &req);
    let case = format!("{index}, forward {forward}");
    assert!(
        walk.pages() >= PARTITIONS.len(),
        "{case}: {} pages",
        walk.pages()
    );
    let first_key = walk.keys[0]
        .as_ref()
        .unwrap_or_else(|| panic!("{case}: no key on the first page"));
    assert!(
        first_key.contains_key("pk") && first_key.contains_key("g"),
        "{case}: {first_key:?}"
    );
    let mut items = walk.items.clone();
    assert_ne!(
        items[0], items[1],
        "{case}: the resume returned the same item"
    );
    items.sort();
    assert_eq!(items, PARTITIONS.map(String::from).to_vec(), "{case}");
}

#[test]
fn indexes_of_a_table_without_a_sort_key_still_page() {
    let db = hash_only_db();
    for forward in [true, false] {
        assert_index_pages(&db, "g-hash", forward);
        assert_index_pages(&db, "g-range", forward);
    }
}

#[test]
fn a_table_with_a_sort_key_and_its_lsi_still_page() {
    let db = ranged_db();
    for p in PARTITIONS {
        for index in [None, Some("byl")] {
            for forward in [true, false] {
                let mut req = json!({
                    "TableName": "ranged",
                    "KeyConditionExpression": "pk = :p",
                    "ExpressionAttributeValues": {":p": {"S": p}},
                    "ScanIndexForward": forward,
                    "Limit": 1
                });
                if let Some(i) = index {
                    req["IndexName"] = json!(i);
                }
                let walk = walk(&db, &req);
                let case = format!("{p}, index {index:?}, forward {forward}");
                assert!(walk.keys[0].is_some(), "{case}: no key on the first page");
                assert!(walk.pages() >= 2, "{case}: {} pages", walk.pages());
                assert_eq!(walk.items, vec![p.to_string(); 2], "{case}");
            }
        }
    }
}

#[test]
fn a_paged_scan_of_a_table_without_a_sort_key_resumes_from_an_items_key() {
    let db = hash_only_db();
    let mut start: Option<HashMap<String, AttributeValue>> = None;
    let mut items = Vec::new();
    let mut pages = 0;
    loop {
        pages += 1;
        assert!(pages <= PAGE_STOP, "Scan walk did not end");
        let mut req = json!({"TableName": "hashonly", "Limit": 1});
        if let Some(ref key) = start {
            req["ExclusiveStartKey"] = serde_json::to_value(key).unwrap();
        }
        let resp = db.scan(serde_json::from_value(req).unwrap()).unwrap();
        items.extend(resp.items.unwrap_or_default().iter().map(pk_of));
        match resp.last_evaluated_key {
            Some(key) => start = Some(key),
            None => break,
        }
    }
    items.sort();
    assert_eq!(items, PARTITIONS.map(String::from).to_vec());
    assert_eq!(pages, PARTITIONS.len() + 1);
}
