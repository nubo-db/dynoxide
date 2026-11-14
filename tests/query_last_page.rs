//! Where a paged Query stops.
//!
//! An ascending Query with no filter, whose sort key condition is absent or
//! closed above (`BETWEEN`, `<=`, `<`, `=`), hands back no `LastEvaluatedKey`
//! on the page holding its last matching row, on the table and on both kinds
//! of index. On an index an ascending `>=` ends the same way. Every other read
//! keeps its trailing empty page, `begins_with` among them, since DynamoDB
//! varies for it. Captured on DynamoDB in eu-west-2 and us-east-1 on 6 and
//! 13 November 2026, at `Limit` 1, 2, 3 and 4 over a partition of four rows.
use dynoxide::Database;
use serde_json::{Value, json};

const TABLE: &str = "LastPage";

fn setup() -> Database {
    let db = Database::memory().unwrap();
    db.create_table(
        serde_json::from_value(json!({
            "TableName": TABLE, "BillingMode": "PAY_PER_REQUEST",
            "AttributeDefinitions": [
                {"AttributeName":"pk","AttributeType":"S"},
                {"AttributeName":"sk","AttributeType":"S"},
                {"AttributeName":"g","AttributeType":"S"},
                {"AttributeName":"l","AttributeType":"S"}
            ],
            "KeySchema": [{"AttributeName":"pk","KeyType":"HASH"},
                          {"AttributeName":"sk","KeyType":"RANGE"}],
            "LocalSecondaryIndexes": [{"IndexName":"l-all",
                "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                             {"AttributeName":"l","KeyType":"RANGE"}],
                "Projection":{"ProjectionType":"ALL"}}],
            "GlobalSecondaryIndexes": [{"IndexName":"g-keys",
                "KeySchema":[{"AttributeName":"g","KeyType":"HASH"},
                             {"AttributeName":"sk","KeyType":"RANGE"}],
                "Projection":{"ProjectionType":"KEYS_ONLY"}}]
        }))
        .unwrap(),
    )
    .unwrap();
    for n in 1..=4 {
        put(
            &db,
            json!({
                "pk":{"S":"k"}, "sk":{"S":format!("row-{n}")}, "x":{"N":n.to_string()},
                "g":{"S":"k"}, "l":{"S":format!("row-{n}")}
            }),
        );
    }
    // Three rows of about 5 KB, each read costing 1.0 eventually consistent.
    for n in 1..=3 {
        put(
            &db,
            json!({"pk":{"S":"big"}, "sk":{"S":format!("row-{n}")},
                   "pad":{"S":"x".repeat(5_000)}}),
        );
    }
    // Four rows of about 380 KB: two fit under the 1 MB page budget.
    for n in 1..=4 {
        put(
            &db,
            json!({"pk":{"S":"wide"}, "sk":{"S":format!("row-{n}")},
                   "pad":{"S":"x".repeat(380_000)}}),
        );
    }
    db
}

fn put(db: &Database, item: Value) {
    db.put_item(serde_json::from_value(json!({"TableName": TABLE, "Item": item})).unwrap())
        .unwrap();
}

/// One page of a walk: what it returned and whether it handed back a key.
#[derive(Debug, PartialEq)]
struct Page {
    count: u64,
    scanned: u64,
    units: f64,
    token: bool,
}

/// Walks a Query to its end, sending each `LastEvaluatedKey` back.
fn walk(db: &Database, request: Value) -> Vec<Page> {
    let mut pages = Vec::new();
    let mut start: Option<Value> = None;
    loop {
        let mut request = request.clone();
        request["TableName"] = json!(TABLE);
        request["ReturnConsumedCapacity"] = json!("TOTAL");
        if let Some(key) = start.take() {
            request["ExclusiveStartKey"] = key;
        }
        let response =
            serde_json::to_value(db.query(serde_json::from_value(request).unwrap()).unwrap())
                .unwrap();
        let key = response.get("LastEvaluatedKey").cloned();
        pages.push(Page {
            count: response["Count"].as_u64().unwrap(),
            scanned: response["ScannedCount"].as_u64().unwrap(),
            units: response["ConsumedCapacity"]["CapacityUnits"]
                .as_f64()
                .unwrap(),
            token: key.is_some(),
        });
        match key {
            Some(key) if pages.len() < 60 => start = Some(key),
            _ => return pages,
        }
    }
}

/// Each page's `Count` and whether it carried a key.
fn shape(pages: &[Page]) -> Vec<(u64, bool)> {
    pages.iter().map(|p| (p.count, p.token)).collect()
}

/// `pages` full pages of `per_page` rows, the last with no key.
fn ends_on_last_row(pages: usize, per_page: u64) -> Vec<(u64, bool)> {
    (1..=pages).map(|n| (per_page, n < pages)).collect()
}

fn query(index: Option<&str>, condition: &str, limit: u32) -> Value {
    let mut request = json!({
        "KeyConditionExpression": condition,
        "ExpressionAttributeValues": {
            ":k": {"S": "k"}, ":a": {"S": "row-1"}, ":b": {"S": "row-2"},
            ":c": {"S": "row-3"}, ":d": {"S": "row-4"}, ":p": {"S": "row-"}
        },
        "Limit": limit
    });
    if let Some(index) = index {
        request["IndexName"] = json!(index);
    }
    // A key condition names only the values it uses.
    let used: serde_json::Map<String, Value> = request["ExpressionAttributeValues"]
        .as_object()
        .unwrap()
        .iter()
        .filter(|(name, _)| condition.contains(name.as_str()))
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect();
    request["ExpressionAttributeValues"] = Value::Object(used);
    request
}

#[test]
fn a_whole_partition_ends_on_its_last_row() {
    let db = setup();
    assert_eq!(
        shape(&walk(&db, query(None, "pk = :k", 1))),
        ends_on_last_row(4, 1)
    );
    assert_eq!(
        shape(&walk(&db, query(None, "pk = :k", 4))),
        ends_on_last_row(1, 4)
    );
}

#[test]
fn a_range_closed_above_ends_on_its_last_row() {
    let db = setup();
    for (condition, limit, pages, per_page) in [
        ("pk = :k AND sk BETWEEN :a AND :c", 3, 1, 3),
        ("pk = :k AND sk BETWEEN :a AND :c", 1, 3, 1),
        ("pk = :k AND sk <= :c", 3, 1, 3),
        ("pk = :k AND sk < :d", 3, 1, 3),
        ("pk = :k AND sk = :b", 1, 1, 1),
    ] {
        assert_eq!(
            shape(&walk(&db, query(None, condition, limit))),
            ends_on_last_row(pages, per_page),
            "{condition} at Limit {limit}"
        );
    }
}

/// `begins_with` is not consistent on DynamoDB: over four rows a Query at
/// `Limit` 4 kept a trailing empty page on 6 walks out of 10. Dynoxide keeps the trailing page for it, on the
/// table and on both kinds of index.
#[test]
fn begins_with_keeps_its_trailing_page() {
    let db = setup();
    for (index, condition) in [
        (None, "pk = :k AND begins_with(sk, :p)"),
        (Some("g-keys"), "g = :k AND begins_with(sk, :p)"),
        (Some("l-all"), "pk = :k AND begins_with(l, :p)"),
    ] {
        assert_eq!(
            shape(&walk(&db, query(index, condition, 1))),
            [(1, true), (1, true), (1, true), (1, true), (0, false)],
            "{index:?} at Limit 1"
        );
        assert_eq!(
            shape(&walk(&db, query(index, condition, 4))),
            [(4, true), (0, false)],
            "{index:?} at Limit 4"
        );
    }
}

/// The row read to see whether anything follows is neither counted nor
/// charged. Three 5 KB rows take three pages at 1.0 each, as captured on
/// 6 November. On 13 November a partition of three rows sometimes took a
/// fourth, empty, page as well; Dynoxide ends on the third.
#[test]
fn the_row_read_ahead_is_not_counted_or_charged() {
    let db = setup();
    let request = json!({
        "KeyConditionExpression": "pk = :k",
        "ExpressionAttributeValues": {":k": {"S": "big"}},
        "Limit": 1
    });
    let pages = walk(&db, request);
    assert_eq!(
        pages,
        (1..=3)
            .map(|n| Page {
                count: 1,
                scanned: 1,
                units: 1.0,
                token: n < 3,
            })
            .collect::<Vec<_>>()
    );
}

#[test]
fn an_index_partition_ends_on_its_last_row() {
    let db = setup();
    for (index, key) in [("g-keys", "g = :k"), ("l-all", "pk = :k")] {
        assert_eq!(
            shape(&walk(&db, query(Some(index), key, 1))),
            ends_on_last_row(4, 1),
            "{index} at Limit 1"
        );
        assert_eq!(
            shape(&walk(&db, query(Some(index), key, 4))),
            ends_on_last_row(1, 4),
            "{index} at Limit 4"
        );
    }
}

/// On an index `>=` ended on its last row in all six walks on each index,
/// where the table kept a trailing page on some.
#[test]
fn an_index_range_ends_on_its_last_row() {
    let db = setup();
    for (index, sk) in [("g-keys", "g = :k AND sk"), ("l-all", "pk = :k AND l")] {
        for condition in [format!("{sk} BETWEEN :a AND :c"), format!("{sk} >= :b")] {
            assert_eq!(
                shape(&walk(&db, query(Some(index), &condition, 3))),
                ends_on_last_row(1, 3),
                "{index} {condition}"
            );
        }
    }
}

/// These reads kept the trailing page on some walks and not on others, so
/// Dynoxide keeps it on every walk.
#[test]
fn open_descending_and_filtered_reads_keep_their_trailing_page() {
    let db = setup();
    assert_eq!(
        shape(&walk(&db, query(None, "pk = :k AND sk >= :b", 3))),
        [(3, true), (0, false)]
    );

    let mut descending = query(None, "pk = :k", 4);
    descending["ScanIndexForward"] = json!(false);
    assert_eq!(shape(&walk(&db, descending)), [(4, true), (0, false)]);

    let mut filtered = query(None, "pk = :k", 4);
    filtered["FilterExpression"] = json!("x > :z");
    filtered["ExpressionAttributeValues"][":z"] = json!({"N": "0"});
    assert_eq!(shape(&walk(&db, filtered)), [(4, true), (0, false)]);
}

/// A page cut by the 1 MB budget keeps its key whatever the `Limit`, since a
/// row is waiting behind it.
#[test]
fn a_page_cut_at_one_megabyte_keeps_its_key() {
    let db = setup();
    for limit in [None, Some(3)] {
        let mut request = json!({
            "KeyConditionExpression": "pk = :k",
            "ExpressionAttributeValues": {":k": {"S": "wide"}}
        });
        if let Some(limit) = limit {
            request["Limit"] = json!(limit);
        }
        assert_eq!(
            shape(&walk(&db, request)),
            [(2, true), (2, false)],
            "Limit {limit:?}"
        );
    }
}

#[test]
fn a_count_pages_through_the_same_rows() {
    let db = setup();
    let mut request = query(None, "pk = :k", 1);
    request["Select"] = json!("COUNT");
    let pages = walk(&db, request);
    assert_eq!(pages.iter().map(|p| p.count).sum::<u64>(), 4);
    assert_eq!(pages.iter().map(|p| p.scanned).sum::<u64>(), 4);
    assert!(pages.iter().all(|p| p.count <= 1), "{pages:?}");
}

/// Scan keeps its trailing empty page, in DynamoDB too: 46 rows take 47
/// pages at `Limit` 1.
#[test]
fn a_scan_keeps_its_trailing_page() {
    let db = Database::memory().unwrap();
    db.create_table(
        serde_json::from_value(json!({
            "TableName": "ScanLast", "BillingMode": "PAY_PER_REQUEST",
            "AttributeDefinitions": [{"AttributeName":"pk","AttributeType":"S"}],
            "KeySchema": [{"AttributeName":"pk","KeyType":"HASH"}]
        }))
        .unwrap(),
    )
    .unwrap();
    for n in 0..46 {
        db.put_item(
            serde_json::from_value(json!({
                "TableName": "ScanLast", "Item": {"pk": {"S": format!("p{n}")}}
            }))
            .unwrap(),
        )
        .unwrap();
    }
    let mut pages = Vec::new();
    let mut start: Option<Value> = None;
    loop {
        let mut request = json!({"TableName": "ScanLast", "Limit": 1});
        if let Some(key) = start.take() {
            request["ExclusiveStartKey"] = key;
        }
        let response =
            serde_json::to_value(db.scan(serde_json::from_value(request).unwrap()).unwrap())
                .unwrap();
        pages.push(response["Count"].as_u64().unwrap());
        match response.get("LastEvaluatedKey") {
            Some(key) if pages.len() < 60 => start = Some(key.clone()),
            _ => break,
        }
    }
    assert_eq!(pages.len(), 47);
    assert_eq!(pages.iter().sum::<u64>(), 46);
    assert_eq!(pages.last(), Some(&0));
}

/// A global index keyed on two partition attributes and two sort attributes,
/// holding five items under one partition, all with the same first sort
/// attribute.
fn multi_attribute_index() -> Database {
    let db = Database::memory().unwrap();
    db.create_table(
        serde_json::from_value(json!({
            "TableName": "LastPageMulti", "BillingMode": "PAY_PER_REQUEST",
            "AttributeDefinitions": [
                {"AttributeName":"pk","AttributeType":"S"},
                {"AttributeName":"h1","AttributeType":"S"},
                {"AttributeName":"h2","AttributeType":"S"},
                {"AttributeName":"r1","AttributeType":"S"},
                {"AttributeName":"r2","AttributeType":"S"},
                {"AttributeName":"r","AttributeType":"S"}
            ],
            "KeySchema": [{"AttributeName":"pk","KeyType":"HASH"}],
            "GlobalSecondaryIndexes": [{"IndexName":"gsi",
                "KeySchema":[{"AttributeName":"h1","KeyType":"HASH"},
                             {"AttributeName":"h2","KeyType":"HASH"},
                             {"AttributeName":"r1","KeyType":"RANGE"},
                             {"AttributeName":"r2","KeyType":"RANGE"}],
                "Projection":{"ProjectionType":"ALL"}},
                {"IndexName":"pair",
                "KeySchema":[{"AttributeName":"h1","KeyType":"HASH"},
                             {"AttributeName":"h2","KeyType":"HASH"},
                             {"AttributeName":"r","KeyType":"RANGE"}],
                "Projection":{"ProjectionType":"ALL"}}]
        }))
        .unwrap(),
    )
    .unwrap();
    for n in 1..=5 {
        db.put_item(
            serde_json::from_value(json!({"TableName": "LastPageMulti", "Item": {
                "pk":{"S":format!("i{n}")}, "h1":{"S":"a"}, "h2":{"S":"b"},
                "r1":{"S":"x"}, "r2":{"S":format!("{n}")}, "r":{"S":format!("{n}")}
            }}))
            .unwrap(),
        )
        .unwrap();
    }
    db
}

/// Each page's `Count` and whether it carried a key, walking the
/// multi-attribute index with `condition` at `limit`.
fn walk_multi(db: &Database, condition: &str, limit: u32) -> Vec<(u64, bool)> {
    walk_index(db, "gsi", condition, limit)
}

fn walk_index(db: &Database, index: &str, condition: &str, limit: u32) -> Vec<(u64, bool)> {
    let mut pages = Vec::new();
    let mut start: Option<Value> = None;
    loop {
        let mut request = json!({
            "TableName": "LastPageMulti", "IndexName": index,
            "KeyConditionExpression": condition,
            "ExpressionAttributeValues": {":a": {"S": "a"}, ":b": {"S": "b"}},
            "Limit": limit
        });
        if condition.contains(":x") {
            request["ExpressionAttributeValues"][":x"] = json!({"S": "x"});
        }
        if let Some(key) = start.take() {
            request["ExclusiveStartKey"] = key;
        }
        let response =
            serde_json::to_value(db.query(serde_json::from_value(request).unwrap()).unwrap())
                .unwrap();
        let key = response.get("LastEvaluatedKey").cloned();
        pages.push((response["Count"].as_u64().unwrap(), key.is_some()));
        match key {
            Some(key) if pages.len() < 10 => start = Some(key),
            _ => return pages,
        }
    }
}

/// An equality on the first sort attribute of a multi-attribute index ends on
/// the last row. Captured on DynamoDB in eu-west-2 and us-east-1 on
/// 14 November 2026, at `Limit` 1, 4 and 5 over five items.
#[test]
fn a_multi_attribute_sort_prefix_ends_on_its_last_row() {
    let db = multi_attribute_index();
    let condition = "h1 = :a AND h2 = :b AND r1 = :x";
    assert_eq!(walk_multi(&db, condition, 1), ends_on_last_row(5, 1));
    assert_eq!(walk_multi(&db, condition, 4), [(4, true), (1, false)]);
    assert_eq!(walk_multi(&db, condition, 5), ends_on_last_row(1, 5));
}

/// With the partition attributes alone, DynamoDB varies: over five items at
/// `Limit` 1 it took 5 pages on some walks and 6, the last empty, on others,
/// and at `Limit` 5 it added an empty page on five walks out of six (captured
/// in eu-west-2 and us-east-1 on 14 November 2026). Dynoxide keeps the
/// trailing page. PartiQL refuses a multi-attribute index outright.
#[test]
fn a_multi_attribute_partition_alone_keeps_its_trailing_page() {
    let db = multi_attribute_index();
    let condition = "h1 = :a AND h2 = :b";
    assert_eq!(
        walk_multi(&db, condition, 1),
        [
            (1, true),
            (1, true),
            (1, true),
            (1, true),
            (1, true),
            (0, false)
        ]
    );
    assert_eq!(walk_multi(&db, condition, 4), [(4, true), (1, false)]);
    assert_eq!(walk_multi(&db, condition, 5), [(5, true), (0, false)]);
    // Two partition attributes and one sort attribute read the same way.
    assert_eq!(
        walk_index(&db, "pair", condition, 1),
        [
            (1, true),
            (1, true),
            (1, true),
            (1, true),
            (1, true),
            (0, false)
        ]
    );
    assert_eq!(
        walk_index(&db, "pair", condition, 5),
        [(5, true), (0, false)]
    );

    let refused = db.execute_statement(
        dynoxide::actions::execute_statement::ExecuteStatementRequest {
            statement: "SELECT * FROM \"LastPageMulti\".\"gsi\" WHERE h1 = 'a' AND h2 = 'b'"
                .to_string(),
            limit: Some(1),
            ..Default::default()
        },
    );
    assert!(refused.is_err(), "{refused:?}");
}

/// Each page of a PartiQL walk at `limit`: its rows and whether it carried a
/// token.
fn statement_pages(db: &Database, sql: &str, limit: usize) -> Vec<(usize, bool)> {
    let mut pages = Vec::new();
    let mut next_token = None;
    loop {
        let response = db
            .execute_statement(
                dynoxide::actions::execute_statement::ExecuteStatementRequest {
                    statement: sql.to_string(),
                    limit: Some(limit),
                    next_token: next_token.take(),
                    ..Default::default()
                },
            )
            .unwrap();
        let more = response.next_token.is_some();
        pages.push((response.items.unwrap_or_default().len(), more));
        if !more || pages.len() > 9 {
            return pages;
        }
        next_token = response.next_token;
    }
}

/// A table keyed on its partition key alone, holding `p1` to `p3`, with a
/// global index keyed on `g` alone, which all three share.
fn no_sort_key() -> Database {
    let db = Database::memory().unwrap();
    db.create_table(
        serde_json::from_value(json!({
            "TableName": "LastPageHashOnly", "BillingMode": "PAY_PER_REQUEST",
            "AttributeDefinitions": [
                {"AttributeName":"pk","AttributeType":"S"},
                {"AttributeName":"g","AttributeType":"S"}
            ],
            "KeySchema": [{"AttributeName":"pk","KeyType":"HASH"}],
            "GlobalSecondaryIndexes": [{"IndexName":"g-only",
                "KeySchema":[{"AttributeName":"g","KeyType":"HASH"}],
                "Projection":{"ProjectionType":"ALL"}}]
        }))
        .unwrap(),
    )
    .unwrap();
    for n in 1..=3 {
        db.put_item(
            serde_json::from_value(json!({"TableName": "LastPageHashOnly", "Item": {
                "pk":{"S":format!("p{n}")}, "g":{"S":"k"}
            }}))
            .unwrap(),
        )
        .unwrap();
    }
    db
}

/// A PartiQL read of a table with no sort key hands back a token on the page
/// holding its item, as captured on DynamoDB in eu-west-2 and us-east-1 on
/// 14 November 2026, and the next page is empty.
#[test]
fn a_statement_on_a_table_with_no_sort_key_keeps_its_token() {
    let db = no_sort_key();
    assert_eq!(
        statement_pages(&db, "SELECT * FROM \"LastPageHashOnly\" WHERE pk = 'p1'", 1),
        [(1, true), (0, false)]
    );
}

/// An index with no sort key keeps the trailing page, by Query and by PartiQL,
/// as no walk of one has been captured.
#[test]
fn an_index_with_no_sort_key_keeps_its_trailing_page() {
    let db = no_sort_key();
    let mut pages = Vec::new();
    let mut start: Option<Value> = None;
    loop {
        let mut request = json!({
            "TableName": "LastPageHashOnly", "IndexName": "g-only",
            "KeyConditionExpression": "g = :k",
            "ExpressionAttributeValues": {":k": {"S": "k"}},
            "Limit": 1
        });
        if let Some(key) = start.take() {
            request["ExclusiveStartKey"] = key;
        }
        let response =
            serde_json::to_value(db.query(serde_json::from_value(request).unwrap()).unwrap())
                .unwrap();
        let key = response.get("LastEvaluatedKey").cloned();
        pages.push((response["Count"].as_u64().unwrap(), key.is_some()));
        match key {
            Some(key) if pages.len() < 10 => start = Some(key),
            _ => break,
        }
    }
    assert_eq!(pages, [(1, true), (1, true), (1, true), (0, false)]);
    assert_eq!(
        statement_pages(
            &db,
            "SELECT * FROM \"LastPageHashOnly\".\"g-only\" WHERE g = 'k'",
            1
        ),
        [(1, true), (1, true), (1, true), (0, false)]
    );
}
