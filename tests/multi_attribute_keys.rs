//! Global secondary indexes whose key has more than one attribute on either
//! side, up to four partition key and four sort key attributes.
//!
//! The expected answers are DynamoDB's: the four-attribute sort key captured in
//! eu-central-1 and us-east-1 on 2026-10-04 and again in eu-west-2 on
//! 2026-10-09 against the table and six items built by `orders_table` below,
//! and the rest in eu-west-2 on 2026-10-09:
//!
//! - items sort over `a`, `b`, `c` and `d` in turn, and an item missing any one
//!   of them is not in the index;
//! - a range condition is allowed only on the last sort key attribute named,
//!   every attribute before it needs an equality, and clause order is free;
//! - `LastEvaluatedKey` carries the table key and every index key attribute.

use dynoxide::Database;
use dynoxide::types::AttributeValue;
use std::collections::HashMap;

type Item = HashMap<String, AttributeValue>;

fn json<T: serde::de::DeserializeOwned>(value: serde_json::Value) -> T {
    serde_json::from_value(value).unwrap()
}

/// The captured table: `PK` alone as the table key, and `gsi` keyed on `h`
/// then `a`, `b`, `c`, `d`. `p6` has no `c`, so it stays out of the index.
fn orders_table(db: &Database) {
    db.create_table(json(serde_json::json!({
        "TableName": "Orders",
        "BillingMode": "PAY_PER_REQUEST",
        "AttributeDefinitions": [
            {"AttributeName": "PK", "AttributeType": "S"},
            {"AttributeName": "h", "AttributeType": "S"},
            {"AttributeName": "a", "AttributeType": "S"},
            {"AttributeName": "b", "AttributeType": "S"},
            {"AttributeName": "c", "AttributeType": "S"},
            {"AttributeName": "d", "AttributeType": "S"}
        ],
        "KeySchema": [{"AttributeName": "PK", "KeyType": "HASH"}],
        "GlobalSecondaryIndexes": [{
            "IndexName": "gsi",
            "KeySchema": [
                {"AttributeName": "h", "KeyType": "HASH"},
                {"AttributeName": "a", "KeyType": "RANGE"},
                {"AttributeName": "b", "KeyType": "RANGE"},
                {"AttributeName": "c", "KeyType": "RANGE"},
                {"AttributeName": "d", "KeyType": "RANGE"}
            ],
            "Projection": {"ProjectionType": "ALL"}
        }]
    })))
    .unwrap();
    for (pk, a, b, c, d) in [
        ("p1", "T", "live", Some("b"), "r1"),
        ("p2", "T", "live", Some("a"), "r2"),
        ("p3", "T", "live", Some("a"), "r1"),
        ("p4", "T", "draft", Some("c"), "r1"),
        ("p5", "F", "live", Some("a"), "r0"),
        ("p6", "T", "live", None, "r1"),
    ] {
        let mut item = serde_json::json!({
            "PK": {"S": pk}, "h": {"S": "o1"}, "a": {"S": a}, "b": {"S": b}, "d": {"S": d}
        });
        if let Some(c) = c {
            item["c"] = serde_json::json!({"S": c});
        }
        db.put_item(json(
            serde_json::json!({"TableName": "Orders", "Item": item}),
        ))
        .unwrap();
    }
}

/// Query `gsi` with string values for every placeholder.
fn query(
    db: &Database,
    key_condition: &str,
    values: &[(&str, &str)],
    extra: serde_json::Value,
) -> Result<dynoxide::actions::query::QueryResponse, dynoxide::errors::DynoxideError> {
    let mut request = serde_json::json!({
        "TableName": "Orders",
        "IndexName": "gsi",
        "KeyConditionExpression": key_condition,
        "ExpressionAttributeValues": values
            .iter()
            .map(|(k, v)| (k.to_string(), serde_json::json!({"S": v})))
            .collect::<serde_json::Map<_, _>>(),
    });
    if let serde_json::Value::Object(extra) = extra {
        for (k, v) in extra {
            request[k] = v;
        }
    }
    db.query(json(request))
}

fn pks(items: &[Item]) -> Vec<String> {
    items
        .iter()
        .map(|i| match i.get("PK") {
            Some(AttributeValue::S(s)) => s.clone(),
            other => panic!("item without a string PK: {other:?}"),
        })
        .collect()
}

fn ok_pks(
    result: Result<dynoxide::actions::query::QueryResponse, dynoxide::errors::DynoxideError>,
) -> Vec<String> {
    pks(&result.unwrap().items.unwrap())
}

fn err_text(
    result: Result<dynoxide::actions::query::QueryResponse, dynoxide::errors::DynoxideError>,
) -> String {
    result.unwrap_err().to_string()
}

#[test]
fn a_multi_attribute_sort_key_orders_over_every_attribute_and_leaves_out_an_item_missing_one() {
    let db = Database::memory().unwrap();
    orders_table(&db);
    assert_eq!(
        ok_pks(query(&db, "h = :h", &[(":h", "o1")], serde_json::json!({}))),
        ["p5", "p4", "p3", "p2", "p1"]
    );

    let scanned = db
        .scan(json(
            serde_json::json!({"TableName": "Orders", "IndexName": "gsi"}),
        ))
        .unwrap();
    assert_eq!(
        pks(&scanned.items.unwrap()),
        ["p5", "p4", "p3", "p2", "p1"],
        "a Scan of the index sees the same members in the same order"
    );
}

#[test]
fn equality_on_a_prefix_of_the_sort_key_narrows_the_read_whatever_the_clause_order() {
    let db = Database::memory().unwrap();
    orders_table(&db);
    let h_a = [(":h", "o1"), (":a", "T")];
    assert_eq!(
        ok_pks(query(&db, "h = :h AND a = :a", &h_a, serde_json::json!({}))),
        ["p4", "p3", "p2", "p1"]
    );
    assert_eq!(
        ok_pks(query(
            &db,
            "h = :h AND a = :a",
            &h_a,
            serde_json::json!({"ScanIndexForward": false})
        )),
        ["p1", "p2", "p3", "p4"]
    );
    let h_a_b = [(":h", "o1"), (":a", "T"), (":b", "live")];
    assert_eq!(
        ok_pks(query(
            &db,
            "h = :h AND a = :a AND b = :b",
            &h_a_b,
            serde_json::json!({})
        )),
        ["p3", "p2", "p1"]
    );
    assert_eq!(
        ok_pks(query(
            &db,
            "b = :b AND h = :h AND a = :a",
            &h_a_b,
            serde_json::json!({})
        )),
        ["p3", "p2", "p1"]
    );
    assert_eq!(
        ok_pks(query(
            &db,
            "h = :h AND a = :a AND b = :b AND c = :c",
            &[(":h", "o1"), (":a", "T"), (":b", "live"), (":c", "a")],
            serde_json::json!({})
        )),
        ["p3", "p2"]
    );
}

#[test]
fn a_range_condition_applies_to_the_last_sort_key_attribute_named() {
    let db = Database::memory().unwrap();
    orders_table(&db);
    let prefix = [(":h", "o1"), (":a", "T"), (":b", "live")];
    let with = |extra: &[(&'static str, &'static str)]| {
        let mut v = prefix.to_vec();
        v.extend_from_slice(extra);
        v
    };
    assert_eq!(
        ok_pks(query(
            &db,
            "h = :h AND a = :a AND b = :b AND c > :c",
            &with(&[(":c", "a")]),
            serde_json::json!({})
        )),
        ["p1"]
    );
    assert_eq!(
        ok_pks(query(
            &db,
            "h = :h AND a = :a AND b = :b AND c = :c AND begins_with(d, :d)",
            &with(&[(":c", "a"), (":d", "r")]),
            serde_json::json!({})
        )),
        ["p3", "p2"]
    );
    assert_eq!(
        ok_pks(query(
            &db,
            "h = :h AND a = :a AND b = :b AND c = :c AND d BETWEEN :x AND :y",
            &with(&[(":c", "a"), (":x", "r1"), (":y", "r1")]),
            serde_json::json!({})
        )),
        ["p3"]
    );
    assert_eq!(
        ok_pks(query(
            &db,
            "h = :h AND a = :a AND b < :b",
            &prefix,
            serde_json::json!({})
        )),
        ["p4"]
    );
}

#[test]
fn a_gap_in_the_equality_prefix_is_refused_with_dynamodbs_message() {
    let db = Database::memory().unwrap();
    orders_table(&db);
    let msg = |missing: &str, present: &str| {
        format!(
            "RANGE key attributes {missing} must have equality conditions specified in the \
             query because a condition is present on key attribute {present}"
        )
    };
    let err = err_text(query(
        &db,
        "h = :h AND a = :a AND c = :c",
        &[(":h", "o1"), (":a", "T"), (":c", "a")],
        serde_json::json!({}),
    ));
    assert!(err.contains(&msg("b", "c")), "got {err}");

    let err = err_text(query(
        &db,
        "h = :h AND a = :a AND b > :b AND c = :c",
        &[(":h", "o1"), (":a", "T"), (":b", "live"), (":c", "a")],
        serde_json::json!({}),
    ));
    assert!(err.contains(&msg("b", "c")), "got {err}");

    let err = err_text(query(
        &db,
        "h = :h AND b = :b",
        &[(":h", "o1"), (":b", "live")],
        serde_json::json!({}),
    ));
    assert!(err.contains(&msg("a", "b")), "got {err}");
}

#[test]
fn pagination_carries_every_index_key_attribute_and_resumes_from_it() {
    let db = Database::memory().unwrap();
    orders_table(&db);
    let h_a = [(":h", "o1"), (":a", "T")];
    let first = query(
        &db,
        "h = :h AND a = :a",
        &h_a,
        serde_json::json!({"Limit": 2}),
    )
    .unwrap();
    assert_eq!(pks(first.items.as_ref().unwrap()), ["p4", "p3"]);
    let lek = first.last_evaluated_key.expect("a full page carries a key");
    let mut names: Vec<&str> = lek.keys().map(String::as_str).collect();
    names.sort_unstable();
    assert_eq!(names, ["PK", "a", "b", "c", "d", "h"]);
    assert_eq!(lek.get("PK"), Some(&AttributeValue::S("p3".into())));
    assert_eq!(lek.get("c"), Some(&AttributeValue::S("a".into())));
    assert_eq!(lek.get("d"), Some(&AttributeValue::S("r1".into())));

    let second = query(
        &db,
        "h = :h AND a = :a",
        &h_a,
        serde_json::json!({"Limit": 2, "ExclusiveStartKey": lek}),
    )
    .unwrap();
    assert_eq!(pks(second.items.as_ref().unwrap()), ["p2", "p1"]);
}

#[test]
fn more_conditions_than_key_attributes_are_counted_against_the_schema() {
    // Captured on a table keyed on `pk` and `sk`, eu-west-2, 2026-10-08:
    // `pk = :p AND sk = :s AND x = :p`. The same rule bounds a multi-attribute
    // index by its own attribute count, five here.
    let db = Database::memory().unwrap();
    orders_table(&db);
    let err = err_text(query(
        &db,
        "h = :h AND a = :a AND b = :b AND c = :c AND d = :d AND PK = :h",
        &[
            (":h", "o1"),
            (":a", "T"),
            (":b", "live"),
            (":c", "a"),
            (":d", "r1"),
        ],
        serde_json::json!({}),
    ));
    assert!(
        err.contains(
            "The number of query conditions (6) exceeds the number of key attributes defined \
             in the schema (5)"
        ),
        "got {err}"
    );
}

/// A table with a two-attribute partition key (`h1` S, `h2` N) and a number
/// sort key, and one with a number then binary sort key, to check composite
/// keys keep each type's own order.
fn typed_table(db: &Database) {
    db.create_table(json(serde_json::json!({
        "TableName": "Typed",
        "BillingMode": "PAY_PER_REQUEST",
        "AttributeDefinitions": [
            {"AttributeName": "PK", "AttributeType": "S"},
            {"AttributeName": "h1", "AttributeType": "S"},
            {"AttributeName": "h2", "AttributeType": "N"},
            {"AttributeName": "s", "AttributeType": "N"},
            {"AttributeName": "h", "AttributeType": "S"},
            {"AttributeName": "n1", "AttributeType": "N"},
            {"AttributeName": "bb", "AttributeType": "B"}
        ],
        "KeySchema": [{"AttributeName": "PK", "KeyType": "HASH"}],
        "GlobalSecondaryIndexes": [
            {
                "IndexName": "byPair",
                "KeySchema": [
                    {"AttributeName": "h1", "KeyType": "HASH"},
                    {"AttributeName": "h2", "KeyType": "HASH"},
                    {"AttributeName": "s", "KeyType": "RANGE"}
                ],
                "Projection": {"ProjectionType": "ALL"}
            },
            {
                "IndexName": "byNumberThenBytes",
                "KeySchema": [
                    {"AttributeName": "h", "KeyType": "HASH"},
                    {"AttributeName": "n1", "KeyType": "RANGE"},
                    {"AttributeName": "bb", "KeyType": "RANGE"}
                ],
                "Projection": {"ProjectionType": "KEYS_ONLY"}
            }
        ]
    })))
    .unwrap();
    let items = serde_json::json!([
        {"PK": {"S": "q1"}, "h1": {"S": "g"}, "h2": {"N": "1"}, "s": {"N": "-5"}},
        {"PK": {"S": "q2"}, "h1": {"S": "g"}, "h2": {"N": "1"}, "s": {"N": "10"}},
        {"PK": {"S": "q3"}, "h1": {"S": "g"}, "h2": {"N": "1"}, "s": {"N": "2.5"}},
        {"PK": {"S": "q4"}, "h1": {"S": "g"}, "h2": {"N": "2"}, "s": {"N": "7"}},
        {"PK": {"S": "q5"}, "h1": {"S": "g"}, "s": {"N": "1"}},
        {"PK": {"S": "q6"}, "h1": {"S": "g"}, "h2": {"N": "1.0"}, "s": {"N": "100"}},
        {"PK": {"S": "r1"}, "h": {"S": "n"}, "n1": {"N": "-10"}, "bb": {"B": "AA=="}},
        {"PK": {"S": "r2"}, "h": {"S": "n"}, "n1": {"N": "2"}, "bb": {"B": "/w=="}},
        {"PK": {"S": "r3"}, "h": {"S": "n"}, "n1": {"N": "2"}, "bb": {"B": "AQ=="}},
        {"PK": {"S": "r4"}, "h": {"S": "n"}, "n1": {"N": "10"}, "bb": {"B": "AA=="}},
        {"PK": {"S": "r5"}, "h": {"S": "n"}, "n1": {"N": "0.5"}, "bb": {"B": "EA=="}},
        {"PK": {"S": "r6"}, "h": {"S": "n"}, "n1": {"N": "2"}},
        {"PK": {"S": "r7"}, "h": {"S": "n"}, "n1": {"N": "2"}, "bb": {"B": "AAE="}}
    ]);
    for item in items.as_array().unwrap() {
        db.put_item(json(
            serde_json::json!({"TableName": "Typed", "Item": item}),
        ))
        .unwrap();
    }
}

fn typed_query(
    db: &Database,
    index: &str,
    key_condition: &str,
    values: serde_json::Value,
    extra: serde_json::Value,
) -> Result<dynoxide::actions::query::QueryResponse, dynoxide::errors::DynoxideError> {
    let mut request = serde_json::json!({
        "TableName": "Typed",
        "IndexName": index,
        "KeyConditionExpression": key_condition,
        "ExpressionAttributeValues": values,
    });
    if let serde_json::Value::Object(extra) = extra {
        for (k, v) in extra {
            request[k] = v;
        }
    }
    db.query(json(request))
}

#[test]
fn a_multi_attribute_partition_key_needs_every_attribute_and_matches_numbers_by_value() {
    let db = Database::memory().unwrap();
    typed_table(&db);
    // `q6` stores `1.0` for `h2`, which is the same number as `1`.
    let all = typed_query(
        &db,
        "byPair",
        "h1 = :g AND h2 = :one",
        serde_json::json!({":g": {"S": "g"}, ":one": {"N": "1"}}),
        serde_json::json!({}),
    );
    assert_eq!(ok_pks(all), ["q1", "q3", "q2", "q6"]);

    let reordered = typed_query(
        &db,
        "byPair",
        "s > :z AND h2 = :one AND h1 = :g",
        serde_json::json!({":g": {"S": "g"}, ":one": {"N": "1"}, ":z": {"N": "0"}}),
        serde_json::json!({}),
    );
    assert_eq!(ok_pks(reordered), ["q3", "q2", "q6"]);

    let page = typed_query(
        &db,
        "byPair",
        "h1 = :g AND h2 = :one",
        serde_json::json!({":g": {"S": "g"}, ":one": {"N": "1"}}),
        serde_json::json!({"Limit": 1}),
    )
    .unwrap();
    let lek = page.last_evaluated_key.unwrap();
    let mut names: Vec<&str> = lek.keys().map(String::as_str).collect();
    names.sort_unstable();
    assert_eq!(names, ["PK", "h1", "h2", "s"]);

    let err = typed_query(
        &db,
        "byPair",
        "h1 = :g",
        serde_json::json!({":g": {"S": "g"}}),
        serde_json::json!({}),
    )
    .unwrap_err()
    .to_string();
    assert!(
        err.contains("HASH key attributes h2 must have equality conditions specified in the query"),
        "got {err}"
    );

    // A range condition on a partition key attribute counts as missing it, and
    // every missing attribute is named.
    // Each request defines only the values its condition uses, as an unused
    // one is refused ahead of the key schema check.
    for (key_condition, values, missing) in [
        (
            "h1 = :g AND h2 > :one",
            serde_json::json!({":g": {"S": "g"}, ":one": {"N": "1"}}),
            "h2",
        ),
        (
            "s = :one",
            serde_json::json!({":one": {"N": "1"}}),
            "h1, h2",
        ),
        ("h2 = :one", serde_json::json!({":one": {"N": "1"}}), "h1"),
    ] {
        let err = typed_query(&db, "byPair", key_condition, values, serde_json::json!({}))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains(&format!(
                "HASH key attributes {missing} must have equality conditions specified in the query"
            )),
            "{key_condition}: got {err}"
        );
    }
}

#[test]
fn a_multi_attribute_key_names_a_missing_partition_key_attribute_its_own_way() {
    // Captured eu-west-2, 2026-10-09. A one-attribute key keeps "Query
    // condition missed key schema element"; a key with more than one attribute
    // on either side uses this form, even when its partition key has just one.
    let db = Database::memory().unwrap();
    orders_table(&db);
    let err = err_text(query(&db, "a = :a", &[(":a", "T")], serde_json::json!({})));
    assert!(
        err.contains("HASH key attributes h must have equality conditions specified in the query"),
        "got {err}"
    );
    // A clause on an attribute outside the key leaves the first unmatched sort
    // key attribute missing.
    let err = err_text(query(
        &db,
        "h = :h AND x = :x",
        &[(":h", "o1"), (":x", "x")],
        serde_json::json!({}),
    ));
    assert!(
        err.contains("Query condition missed key schema element: a"),
        "got {err}"
    );
    // Every attribute ahead of the last one named must be an equality.
    let err = err_text(query(
        &db,
        "h = :h AND d = :d",
        &[(":h", "o1"), (":d", "r1")],
        serde_json::json!({}),
    ));
    assert!(
        err.contains(
            "RANGE key attributes a, b, c must have equality conditions specified in the query \
             because a condition is present on key attribute d"
        ),
        "got {err}"
    );
}

/// Create a table whose one GSI has the given key schema.
fn create_with_gsi_key(
    db: &Database,
    name: &str,
    key_schema: serde_json::Value,
) -> Result<dynoxide::actions::create_table::CreateTableResponse, dynoxide::errors::DynoxideError> {
    let mut defs = vec![serde_json::json!({"AttributeName": "PK", "AttributeType": "S"})];
    for element in key_schema.as_array().unwrap() {
        let attr = element["AttributeName"].as_str().unwrap();
        if !defs.iter().any(|d| d["AttributeName"] == attr) {
            defs.push(serde_json::json!({"AttributeName": attr, "AttributeType": "S"}));
        }
    }
    db.create_table(json(serde_json::json!({
        "TableName": name,
        "BillingMode": "PAY_PER_REQUEST",
        "AttributeDefinitions": defs,
        "KeySchema": [{"AttributeName": "PK", "KeyType": "HASH"}],
        "GlobalSecondaryIndexes": [{
            "IndexName": "gsi",
            "KeySchema": key_schema,
            "Projection": {"ProjectionType": "KEYS_ONLY"}
        }]
    })))
}

fn ks(elements: &[(&str, &str)]) -> serde_json::Value {
    serde_json::Value::Array(
        elements
            .iter()
            .map(|(n, t)| serde_json::json!({"AttributeName": n, "KeyType": t}))
            .collect(),
    )
}

#[test]
fn a_gsi_key_schema_is_checked_against_the_multi_attribute_rules() {
    // Captured eu-west-2, 2026-10-09, on CreateTable and UpdateTable alike.
    let db = Database::memory().unwrap();
    let cases: [(&str, serde_json::Value, &str); 6] = [
        (
            "FiveHash",
            ks(&[
                ("a", "HASH"),
                ("b", "HASH"),
                ("c", "HASH"),
                ("d", "HASH"),
                ("e", "HASH"),
            ]),
            "One or more parameter values were invalid: The KeySchema exceeds the maximum allowed number of HASH key attributes",
        ),
        (
            "FiveRange",
            ks(&[
                ("h", "HASH"),
                ("a", "RANGE"),
                ("b", "RANGE"),
                ("c", "RANGE"),
                ("d", "RANGE"),
                ("e", "RANGE"),
            ]),
            "One or more parameter values were invalid: The KeySchema exceeds the maximum allowed number of RANGE key attributes",
        ),
        (
            "DuplicateRange",
            ks(&[("h", "HASH"), ("a", "RANGE"), ("a", "RANGE")]),
            "The KeySchema contains multiple key attributes with the same name",
        ),
        (
            "DuplicateHash",
            ks(&[("h", "HASH"), ("h", "HASH")]),
            "The KeySchema contains multiple key attributes with the same name",
        ),
        (
            "Interleaved",
            ks(&[("h", "HASH"), ("a", "RANGE"), ("h2", "HASH")]),
            "One or more parameter values were invalid: All HASH key attributes must precede RANGE key attributes in the KeySchema",
        ),
        (
            "HashAndRangeSame",
            ks(&[("h", "HASH"), ("h", "RANGE")]),
            "Both the Hash Key and the Range Key element in the KeySchema have the same name",
        ),
    ];
    for (name, key_schema, message) in cases {
        let err = create_with_gsi_key(&db, name, key_schema)
            .unwrap_err()
            .to_string();
        assert!(err.contains(message), "{name}: got {err}");
    }

    // Four of each, and a partition key with no sort key, are fine, and the
    // key schema reads back in the order it was given.
    create_with_gsi_key(
        &db,
        "FourAndFour",
        ks(&[
            ("h1", "HASH"),
            ("h2", "HASH"),
            ("h3", "HASH"),
            ("h4", "HASH"),
            ("r1", "RANGE"),
            ("r2", "RANGE"),
            ("r3", "RANGE"),
            ("r4", "RANGE"),
        ]),
    )
    .unwrap();
    let described = db
        .describe_table(json(serde_json::json!({"TableName": "FourAndFour"})))
        .unwrap();
    let described = serde_json::to_value(described).unwrap();
    let names: Vec<&str> = described["Table"]["GlobalSecondaryIndexes"][0]["KeySchema"]
        .as_array()
        .unwrap()
        .iter()
        .map(|k| k["AttributeName"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["h1", "h2", "h3", "h4", "r1", "r2", "r3", "r4"]);
    create_with_gsi_key(&db, "TwoHash", ks(&[("h1", "HASH"), ("h2", "HASH")])).unwrap();

    // UpdateTable applies the same rules.
    let err = db
        .update_table(json(serde_json::json!({
            "TableName": "TwoHash",
            "AttributeDefinitions": [
                {"AttributeName": "h1", "AttributeType": "S"},
                {"AttributeName": "h2", "AttributeType": "S"}
            ],
            "GlobalSecondaryIndexUpdates": [{"Create": {
                "IndexName": "added",
                "KeySchema": [
                    {"AttributeName": "h1", "KeyType": "HASH"},
                    {"AttributeName": "h2", "KeyType": "RANGE"},
                    {"AttributeName": "h1", "KeyType": "RANGE"}
                ],
                "Projection": {"ProjectionType": "KEYS_ONLY"}
            }}]
        })))
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("The KeySchema contains multiple key attributes with the same name"),
        "got {err}"
    );
}

#[test]
fn a_local_secondary_index_takes_one_sort_key_attribute() {
    // Captured eu-west-2, 2026-10-09. Multi-attribute keys are for global
    // secondary indexes only.
    let db = Database::memory().unwrap();
    // On the JSON path the request is refused as it is read, as AWS does.
    let request: Result<dynoxide::actions::create_table::CreateTableRequest, _> =
        serde_json::from_value(serde_json::json!({
            "TableName": "LocalPair",
            "BillingMode": "PAY_PER_REQUEST",
            "AttributeDefinitions": [
                {"AttributeName": "PK", "AttributeType": "S"},
                {"AttributeName": "SK", "AttributeType": "S"},
                {"AttributeName": "l1", "AttributeType": "S"},
                {"AttributeName": "l2", "AttributeType": "S"}
            ],
            "KeySchema": [
                {"AttributeName": "PK", "KeyType": "HASH"},
                {"AttributeName": "SK", "KeyType": "RANGE"}
            ],
            "LocalSecondaryIndexes": [{
                "IndexName": "local",
                "KeySchema": [
                    {"AttributeName": "PK", "KeyType": "HASH"},
                    {"AttributeName": "l1", "KeyType": "RANGE"},
                    {"AttributeName": "l2", "KeyType": "RANGE"}
                ],
                "Projection": {"ProjectionType": "ALL"}
            }]
        }));
    let err = match request {
        Err(e) => e.to_string(),
        Ok(request) => db.create_table(request).unwrap_err().to_string(),
    };
    assert!(
        err.contains("at 'keySchema' failed to satisfy constraint: Member must have length less than or equal to 2"),
        "got {err}"
    );
}

#[test]
fn a_composite_sort_key_keeps_number_and_binary_order() {
    let db = Database::memory().unwrap();
    typed_table(&db);
    let h = serde_json::json!({":h": {"S": "n"}});
    // Numbers by value, then bytes; `r6` has no `bb` and is not indexed.
    assert_eq!(
        ok_pks(typed_query(
            &db,
            "byNumberThenBytes",
            "h = :h",
            h.clone(),
            serde_json::json!({})
        )),
        ["r1", "r5", "r7", "r3", "r2", "r4"]
    );
    assert_eq!(
        ok_pks(typed_query(
            &db,
            "byNumberThenBytes",
            "h = :h",
            h,
            serde_json::json!({"ScanIndexForward": false})
        )),
        ["r4", "r2", "r3", "r7", "r5", "r1"]
    );
    assert_eq!(
        ok_pks(typed_query(
            &db,
            "byNumberThenBytes",
            "h = :h AND n1 = :two AND bb > :b",
            serde_json::json!({":h": {"S": "n"}, ":two": {"N": "2.0"}, ":b": {"B": "AA=="}}),
            serde_json::json!({})
        )),
        ["r7", "r3", "r2"]
    );
    assert_eq!(
        ok_pks(typed_query(
            &db,
            "byNumberThenBytes",
            "h = :h AND n1 = :two AND begins_with(bb, :p)",
            serde_json::json!({":h": {"S": "n"}, ":two": {"N": "2"}, ":p": {"B": "AA=="}}),
            serde_json::json!({})
        )),
        ["r7"]
    );
    assert_eq!(
        ok_pks(typed_query(
            &db,
            "byNumberThenBytes",
            "h = :h AND n1 BETWEEN :lo AND :hi",
            serde_json::json!({":h": {"S": "n"}, ":lo": {"N": "0"}, ":hi": {"N": "5"}}),
            serde_json::json!({})
        )),
        ["r5", "r7", "r3", "r2"]
    );
}

#[test]
fn every_attribute_of_a_multi_attribute_key_is_type_checked_on_write() {
    let db = Database::memory().unwrap();
    orders_table(&db);
    let err = db
        .put_item(json(serde_json::json!({
            "TableName": "Orders",
            "Item": {
                "PK": {"S": "w1"}, "h": {"S": "o1"}, "a": {"S": "T"},
                "b": {"S": "live"}, "c": {"N": "1"}, "d": {"S": "r1"}
            }
        })))
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("Type mismatch for Index Key c Expected: S Actual: N IndexName: gsi"),
        "got {err}"
    );
}

#[test]
fn update_table_can_add_a_multi_attribute_index_and_backfills_it() {
    let db = Database::memory().unwrap();
    orders_table(&db);
    db.update_table(json(serde_json::json!({
        "TableName": "Orders",
        "AttributeDefinitions": [
            {"AttributeName": "h", "AttributeType": "S"},
            {"AttributeName": "b", "AttributeType": "S"},
            {"AttributeName": "d", "AttributeType": "S"}
        ],
        "GlobalSecondaryIndexUpdates": [{"Create": {
            "IndexName": "byStatus",
            "KeySchema": [
                {"AttributeName": "h", "KeyType": "HASH"},
                {"AttributeName": "b", "KeyType": "RANGE"},
                {"AttributeName": "d", "KeyType": "RANGE"}
            ],
            "Projection": {"ProjectionType": "KEYS_ONLY"}
        }}]
    })))
    .unwrap();
    let response = db
        .query(json(serde_json::json!({
            "TableName": "Orders",
            "IndexName": "byStatus",
            "KeyConditionExpression": "h = :h AND b = :b",
            "ExpressionAttributeValues": {":h": {"S": "o1"}, ":b": {"S": "live"}}
        })))
        .unwrap();
    // p6 has `b` and `d`, so it belongs to this index even though it is not
    // in `gsi`. Ties on `d` fall back to the table key.
    assert_eq!(
        pks(&response.items.unwrap()),
        ["p5", "p1", "p3", "p6", "p2"]
    );
}

/// DynamoDB refuses a PartiQL `SELECT` on an index with more than one
/// partition key or sort key attribute, whichever kind of statement it is, but
/// not one on the table itself or on its other indexes (captured eu-west-2 and
/// us-east-1, 2026-10-09).
const MULTI_ATTRIBUTE_REFUSAL: &str = "PartiQL statements cannot be executed on tables with \
multi-attribute HASH keys or multi-attribute RANGE keys.";

/// A table with a GSI of each multi-attribute shape, a single-attribute GSI and
/// an LSI, holding one item that every index carries.
fn mixed_indexes_table(db: &Database) {
    let gsi = |name: &str, hash: &[&str], range: &[&str]| {
        let mut schema: Vec<serde_json::Value> = hash
            .iter()
            .map(|a| serde_json::json!({"AttributeName": a, "KeyType": "HASH"}))
            .collect();
        schema.extend(
            range
                .iter()
                .map(|a| serde_json::json!({"AttributeName": a, "KeyType": "RANGE"})),
        );
        serde_json::json!({
            "IndexName": name,
            "KeySchema": schema,
            "Projection": {"ProjectionType": "ALL"}
        })
    };
    let attrs: Vec<serde_json::Value> = ["id", "sk", "g", "h1", "h2", "r1", "r2", "l"]
        .iter()
        .map(|a| serde_json::json!({"AttributeName": a, "AttributeType": "S"}))
        .collect();
    db.create_table(json(serde_json::json!({
        "TableName": "Mixed",
        "BillingMode": "PAY_PER_REQUEST",
        "AttributeDefinitions": attrs,
        "KeySchema": [
            {"AttributeName": "id", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"}
        ],
        "GlobalSecondaryIndexes": [
            gsi("single", &["g"], &["r1"]),
            gsi("hh_rr", &["h1", "h2"], &["r1", "r2"]),
            gsi("hh_r", &["h1", "h2"], &["r1"]),
            gsi("h_rr", &["h1"], &["r1", "r2"]),
            gsi("hh_only", &["h1", "h2"], &[])
        ],
        "LocalSecondaryIndexes": [{
            "IndexName": "local",
            "KeySchema": [
                {"AttributeName": "id", "KeyType": "HASH"},
                {"AttributeName": "l", "KeyType": "RANGE"}
            ],
            "Projection": {"ProjectionType": "ALL"}
        }]
    })))
    .unwrap();
    db.put_item(json(serde_json::json!({
        "TableName": "Mixed",
        "Item": {
            "id": {"S": "i1"}, "sk": {"S": "s"}, "g": {"S": "g1"}, "h1": {"S": "a"},
            "h2": {"S": "x"}, "r1": {"S": "m"}, "r2": {"S": "n"}, "l": {"S": "l1"}
        }
    })))
    .unwrap();
}

fn statement(db: &Database, request: serde_json::Value) -> Result<usize, String> {
    db.execute_statement(json(request))
        .map(|response| response.items.map_or(0, |items| items.len()))
        .map_err(|e| e.to_string())
}

#[test]
fn a_partiql_select_on_a_multi_attribute_index_is_refused_and_nothing_else_is() {
    let db = Database::memory().unwrap();
    mixed_indexes_table(&db);
    for index in ["hh_rr", "hh_r", "h_rr", "hh_only"] {
        assert_eq!(
            statement(
                &db,
                serde_json::json!({
                    "Statement": format!("SELECT * FROM \"Mixed\".\"{index}\" WHERE h1 = 'a' AND h2 = 'x'")
                })
            ),
            Err(MULTI_ATTRIBUTE_REFUSAL.to_string()),
            "{index}"
        );
    }
    // The table's other indexes, and the table itself, read and write as usual.
    for (read, rows) in [
        ("SELECT * FROM \"Mixed\".\"single\" WHERE g = 'g1'", 1),
        ("SELECT * FROM \"Mixed\".\"local\" WHERE id = 'i1'", 1),
        ("SELECT * FROM \"Mixed\" WHERE id = 'i1'", 1),
        ("SELECT * FROM \"Mixed\"", 1),
        (
            "SELECT * FROM \"Mixed\".\"single\" WHERE g = 'g1' ORDER BY g",
            1,
        ),
        ("INSERT INTO \"Mixed\" VALUE {'id': 'n1', 'sk': 's'}", 0),
        ("UPDATE \"Mixed\" SET a = 1 WHERE id = 'i1' AND sk = 's'", 0),
        ("DELETE FROM \"Mixed\" WHERE id = 'n1' AND sk = 's'", 0),
    ] {
        assert_eq!(
            statement(&db, serde_json::json!({"Statement": read})),
            Ok(rows),
            "{read}"
        );
    }
}

#[test]
fn the_multi_attribute_refusal_comes_after_parsing_and_before_every_other_check() {
    let db = Database::memory().unwrap();
    mixed_indexes_table(&db);
    let q = "\"Mixed\".\"hh_rr\"";
    // A literal is checked first, and a missing index or table, or a write
    // naming an index, keeps its own answer.
    for (request, expected) in [
        (
            serde_json::json!({"Statement": format!("SELECT * FROM {q} WHERE h1 = 1e99999999999")}),
            "Statement wasn't well formed, can't be processed: ",
        ),
        (
            serde_json::json!({"Statement": "SELECT * FROM \"Mixed\".\"nosuch\" WHERE h1 = 'a'"}),
            "The table does not have the specified index",
        ),
        (
            serde_json::json!({"Statement": "SELECT * FROM \"Mixed\".\"HH_RR\" WHERE h1 = 'a'"}),
            "The table does not have the specified index",
        ),
        (
            serde_json::json!({"Statement": "SELECT * FROM \"Absent\".\"hh_rr\" WHERE h1 = 'a'"}),
            "Requested resource not found",
        ),
        (
            serde_json::json!({"Statement": format!("UPDATE {q} SET a = 1 WHERE id = 'i1'")}),
            "This operation is not supported on an index",
        ),
        (
            serde_json::json!({"Statement": format!("DELETE FROM {q} WHERE id = 'i1'")}),
            "This operation is not supported on an index",
        ),
    ] {
        assert_eq!(
            statement(&db, request.clone()),
            Err(expected.to_string()),
            "{request}"
        );
    }
    // Everything else about the read waits for the refusal, ORDER BY included.
    for request in [
        serde_json::json!({"Statement": format!("SELECT * FROM {q} WHERE h1 = 5")}),
        serde_json::json!({"Statement": format!("SELECT * FROM {q} WHERE h1 = {{'a': 1}}")}),
        serde_json::json!({"Statement": format!("SELECT * FROM {q} WHERE id = 'i1'")}),
        serde_json::json!({"Statement": format!("SELECT * FROM {q}")}),
        serde_json::json!({"Statement": format!("SELECT COUNT(*) FROM {q}")}),
        serde_json::json!({"Statement": format!("SELECT * FROM {q} WHERE begins_with(h1, 5)")}),
        serde_json::json!({
            "Statement": format!("SELECT * FROM {q} WHERE h1 = 'a'"),
            "ConsistentRead": true
        }),
        serde_json::json!({
            "Statement": format!("SELECT * FROM {q} WHERE h1 = 'a'"),
            "NextToken": "garbage"
        }),
        serde_json::json!({"Statement": format!("SELECT * FROM {q} WHERE h1 = 'a'"), "Limit": 1}),
        serde_json::json!({
            "Statement": format!("SELECT * FROM {q} WHERE h1 = 'a' AND h2 = 'x' ORDER BY r1 DESC")
        }),
        serde_json::json!({
            "Statement": format!("SELECT * FROM {q} WHERE h1 = 'a' AND h2 IN ['x', 'y'] ORDER BY r1")
        }),
        serde_json::json!({"Statement": format!("SELECT * FROM {q} WHERE h1 = 'a' ORDER BY extra")}),
        serde_json::json!({"Statement": format!("SELECT * FROM {q} ORDER BY r1")}),
    ] {
        assert_eq!(
            statement(&db, request.clone()),
            Err(MULTI_ATTRIBUTE_REFUSAL.to_string()),
            "{request}"
        );
    }
    // A transaction refuses any index read before it looks at the index.
    let err = db
        .execute_transaction(json(serde_json::json!({
            "TransactStatements": [{"Statement": format!("SELECT * FROM {q} WHERE h1 = 'a' AND h2 = 'x'")}]
        })))
        .unwrap_err()
        .to_string();
    assert_eq!(
        err,
        "Validation failed in TransactStatements[0]: Reads on indices are not supported within transactions."
    );
}

#[test]
fn a_batch_select_on_a_multi_attribute_index_fails_only_that_statement() {
    let db = Database::memory().unwrap();
    mixed_indexes_table(&db);
    let response = db
        .batch_execute_statement(json(serde_json::json!({
            "Statements": [
                {"Statement": "SELECT * FROM \"Mixed\".\"single\" WHERE g = 'g1' AND r1 = 'm'"},
                {"Statement": "SELECT * FROM \"Mixed\".\"hh_rr\" WHERE h1 = 'a' AND h2 = 'x' AND r1 = 'm'"},
                {"Statement": "SELECT * FROM \"Mixed\" WHERE id = 'i1' AND sk = 's'"}
            ]
        })))
        .unwrap();
    let outcomes: Vec<Result<bool, (String, String)>> = response
        .responses
        .iter()
        .map(|r| match &r.error {
            Some(e) => Err((e.code.clone(), e.message.clone())),
            None => Ok(r.item.is_some()),
        })
        .collect();
    assert_eq!(
        outcomes,
        [
            Err((
                "ValidationError".to_string(),
                "Select statements within BatchExecuteStatement must specify the primary key \
                 in the where clause."
                    .to_string()
            )),
            Err((
                "ValidationError".to_string(),
                MULTI_ATTRIBUTE_REFUSAL.to_string()
            )),
            Ok(true),
        ]
    );
}

#[test]
fn a_key_condition_value_of_a_type_the_key_cannot_hold_fails_the_schema_check() {
    // Captured eu-west-2 and us-east-1, 2026-10-09: an equality with a map
    // (shallow or 31 levels deep), list, BOOL, NULL, any set, or the wrong
    // scalar is refused as a schema mismatch, on a table, a single-attribute
    // GSI and a multi-attribute GSI alike. An ordering comparison or
    // begins_with with a type no key could hold stays an operand error.
    let db = Database::memory().unwrap();
    db.create_table(json(serde_json::json!({
        "TableName": "WrongTypes",
        "BillingMode": "PAY_PER_REQUEST",
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "N"},
            {"AttributeName": "gk", "AttributeType": "S"},
            {"AttributeName": "gs", "AttributeType": "N"},
            {"AttributeName": "h1", "AttributeType": "S"},
            {"AttributeName": "h2", "AttributeType": "N"},
            {"AttributeName": "r1", "AttributeType": "S"},
            {"AttributeName": "r2", "AttributeType": "B"}
        ],
        "KeySchema": [
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"}
        ],
        "GlobalSecondaryIndexes": [
            {
                "IndexName": "single",
                "KeySchema": [
                    {"AttributeName": "gk", "KeyType": "HASH"},
                    {"AttributeName": "gs", "KeyType": "RANGE"}
                ],
                "Projection": {"ProjectionType": "ALL"}
            },
            {
                "IndexName": "multi",
                "KeySchema": [
                    {"AttributeName": "h1", "KeyType": "HASH"},
                    {"AttributeName": "h2", "KeyType": "HASH"},
                    {"AttributeName": "r1", "KeyType": "RANGE"},
                    {"AttributeName": "r2", "KeyType": "RANGE"}
                ],
                "Projection": {"ProjectionType": "ALL"}
            }
        ]
    })))
    .unwrap();
    let mut deep = serde_json::json!({"S": "leaf"});
    for _ in 0..31 {
        deep = serde_json::json!({"M": {"n": deep}});
    }
    let not_a_string = [
        serde_json::json!({"M": {"a": {"S": "x"}}}),
        deep,
        serde_json::json!({"L": [{"S": "x"}]}),
        serde_json::json!({"BOOL": true}),
        serde_json::json!({"NULL": true}),
        serde_json::json!({"SS": ["x"]}),
        serde_json::json!({"NS": ["1"]}),
        serde_json::json!({"BS": ["AQ=="]}),
        serde_json::json!({"N": "1"}),
        serde_json::json!({"B": "AQ=="}),
    ];
    let run = |index: Option<&str>, key_condition: &str, values: serde_json::Value| {
        let mut request = serde_json::json!({
            "TableName": "WrongTypes",
            "KeyConditionExpression": key_condition,
            "ExpressionAttributeValues": values
        });
        if let Some(index) = index {
            request["IndexName"] = serde_json::json!(index);
        }
        match db.query(json(request)) {
            Ok(r) => format!("accepted, {} items", r.count),
            Err(e) => e.to_string(),
        }
    };
    const SCHEMA: &str = "One or more parameter values were invalid: Condition parameter type does not match schema type";
    let (ok_s, ok_n) = (
        serde_json::json!({"S": "ok"}),
        serde_json::json!({"N": "1"}),
    );
    let mut checked = 0;
    for v in &not_a_string {
        for (index, key_condition, values) in [
            (None, "pk = :v", serde_json::json!({":v": v})),
            (Some("single"), "gk = :v", serde_json::json!({":v": v})),
            (
                Some("multi"),
                "h1 = :v AND h2 = :n",
                serde_json::json!({":v": v, ":n": ok_n}),
            ),
            (
                Some("multi"),
                "h1 = :s AND h2 = :n AND r1 = :v",
                serde_json::json!({":s": ok_s, ":n": ok_n, ":v": v}),
            ),
        ] {
            let got = run(index, key_condition, values);
            assert!(got.contains(SCHEMA), "{key_condition} with {v}: got {got}");
            checked += 1;
        }
        // The number sort keys take the same values in an equality, bar N.
        if v.get("N").is_none() {
            for (index, key_condition, values) in [
                (
                    None,
                    "pk = :s AND sk = :v",
                    serde_json::json!({":s": ok_s, ":v": v}),
                ),
                (
                    Some("single"),
                    "gk = :s AND gs = :v",
                    serde_json::json!({":s": ok_s, ":v": v}),
                ),
                (
                    Some("multi"),
                    "h1 = :s AND h2 = :v",
                    serde_json::json!({":s": ok_s, ":v": v}),
                ),
            ] {
                let got = run(index, key_condition, values);
                assert!(got.contains(SCHEMA), "{key_condition} with {v}: got {got}");
                checked += 1;
            }
        }
    }
    assert_eq!(checked, 10 * 4 + 9 * 3);

    // An ordering comparison keeps the operand error for a type no key holds,
    // and gives the schema mismatch for a key type that isn't this key's.
    let got = run(
        None,
        "pk = :s AND sk > :v",
        serde_json::json!({":s": ok_s, ":v": {"M": {"a": {"S": "x"}}}}),
    );
    assert!(
        got.contains("Incorrect operand type for operator or function; operator or function: >, operand type: M"),
        "got {got}"
    );
    let got = run(
        None,
        "pk = :s AND sk > :v",
        serde_json::json!({":s": ok_s, ":v": {"S": "x"}}),
    );
    assert!(got.contains(SCHEMA), "got {got}");
    let got = run(
        Some("multi"),
        "h1 = :s AND h2 = :n AND r1 = :s AND begins_with(r2, :v)",
        serde_json::json!({":s": ok_s, ":n": ok_n, ":v": {"S": "x"}}),
    );
    assert!(got.contains(SCHEMA), "got {got}");
}
