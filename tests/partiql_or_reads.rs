//! Regressions for #229 and #271–#273, captured in eu-west-2 and us-east-1
//! on 10 and 23 October and 3 November 2026 (see the corresponding GitHub
//! issues).
use dynoxide::Database;
use dynoxide::actions::execute_statement::{ExecuteStatementRequest, ExecuteStatementResponse};
use serde_json::json;

fn setup(projection: &str) -> Database {
    let db = Database::memory().unwrap();
    db.create_table(
        serde_json::from_value(json!({
            "TableName": "OrReads", "BillingMode": "PAY_PER_REQUEST",
            "AttributeDefinitions": [
                {"AttributeName":"pk","AttributeType":"S"},
                {"AttributeName":"sk","AttributeType":"S"},
                {"AttributeName":"g","AttributeType":"S"}
            ],
            "KeySchema": [{"AttributeName":"pk","KeyType":"HASH"},
                          {"AttributeName":"sk","KeyType":"RANGE"}],
            "GlobalSecondaryIndexes": [{"IndexName":"g-keys",
                "KeySchema":[{"AttributeName":"g","KeyType":"HASH"},
                             {"AttributeName":"sk","KeyType":"RANGE"}],
                "Projection":{"ProjectionType":projection}}]
        }))
        .unwrap(),
    )
    .unwrap();
    for pk in ["a", "k", "z"] {
        for (sk, x) in [("row-1", 1), ("row-2", 2), ("row-3", 3)] {
            db.put_item(
                serde_json::from_value(json!({"TableName":"OrReads", "Item":{
                    "pk":{"S":pk},"sk":{"S":sk},"g":{"S":pk},"x":{"N":x.to_string()},
                    "payload":{"S":"x".repeat(3000)}
                }}))
                .unwrap(),
            )
            .unwrap();
        }
    }
    db
}

fn read(
    db: &Database,
    sql: &str,
    limit: Option<usize>,
    token: Option<String>,
    consistent: bool,
) -> ExecuteStatementResponse {
    db.execute_statement(ExecuteStatementRequest {
        statement: sql.into(),
        limit,
        next_token: token,
        consistent_read: Some(consistent),
        return_consumed_capacity: Some("TOTAL".into()),
        ..Default::default()
    })
    .unwrap()
}

#[test]
fn an_or_filter_reads_only_the_shared_partition() {
    let db = setup("ALL");
    for (source, key, consistent, capacity) in [
        ("\"OrReads\"", "pk", true, 3.0),
        ("\"OrReads\".\"g-keys\"", "g", false, 1.5),
    ] {
        let sql = format!("SELECT * FROM {source} WHERE {key} = 'k' AND (x = 1 OR x = 2)");
        let page = read(&db, &sql, None, None, consistent);
        assert_eq!(page.items.unwrap().len(), 2);
        assert_eq!(page.consumed_capacity.unwrap().capacity_units, capacity);
        let page = read(&db, &sql, Some(1), None, consistent);
        assert_eq!(page.items.unwrap().len(), 1, "first page reads k, not a");
    }
    let page = read(
        &db,
        "SELECT * FROM \"OrReads\" WHERE pk = 'k' OR x = 1",
        None,
        None,
        true,
    );
    assert_eq!(page.items.unwrap().len(), 5);
    assert_eq!(page.consumed_capacity.unwrap().capacity_units, 7.0);
}

#[test]
fn an_or_on_unprojected_attributes_is_refused_when_the_partition_is_shared() {
    let db = setup("KEYS_ONLY");
    let err = db
        .execute_statement(ExecuteStatementRequest {
            statement: "SELECT * FROM \"OrReads\".\"g-keys\" WHERE g = 'k' AND (x = 1 OR x = 2)"
                .into(),
            ..Default::default()
        })
        .unwrap_err()
        .to_string();
    assert_eq!(
        err,
        "One or more parameter values were invalid: Secondary index g-keys does not project one or more filter attributes: [x]"
    );
    // So is one shared by every branch through a multi-value IN, as the same
    // IN beside a single filter is (captured eu-west-2 and us-east-1,
    // 2026-11-03).
    for filter in ["(x = 1 OR x = 2)", "x = 1"] {
        let err = db
            .execute_statement(ExecuteStatementRequest {
                statement: format!(
                    "SELECT * FROM \"OrReads\".\"g-keys\" WHERE g IN ['k', 'a'] AND {filter}"
                ),
                ..Default::default()
            })
            .unwrap_err()
            .to_string();
        assert_eq!(
            err,
            "One or more parameter values were invalid: Secondary index g-keys does not project one or more filter attributes: [x]",
            "{filter}"
        );
    }
    // A different partition in each branch retains the previously captured scan behaviour.
    // The unprojected `x` matches nothing on the index row, so only partition
    // `z` comes back, at the cost of scanning the whole index.
    let page = read(
        &db,
        "SELECT * FROM \"OrReads\".\"g-keys\" WHERE (g = 'k' AND x = 1) OR g = 'z'",
        None,
        None,
        false,
    );
    let items = page.items.unwrap();
    assert_eq!(items.len(), 3);
    assert!(
        items
            .iter()
            .all(|item| item["g"] == dynoxide::types::AttributeValue::S("z".into()))
    );
    let scan = read(
        &db,
        "SELECT * FROM \"OrReads\".\"g-keys\"",
        None,
        None,
        false,
    );
    assert_eq!(
        page.consumed_capacity.unwrap().capacity_units,
        scan.consumed_capacity.unwrap().capacity_units
    );
}

#[test]
fn ordering_a_shared_partition_does_not_require_ordering_the_hash_key() {
    let db = setup("ALL");
    let page = read(
        &db,
        "SELECT * FROM \"OrReads\" WHERE pk = 'k' AND (x = 1 OR x = 2) ORDER BY sk DESC",
        None,
        None,
        true,
    );
    let items = page.items.unwrap();
    assert_eq!(items.len(), 2);
    assert_eq!(
        items[0]["sk"],
        dynoxide::types::AttributeValue::S("row-2".into())
    );
}

#[test]
fn sort_key_equalities_read_only_their_union() {
    let db = setup("ALL");
    for clause in [
        "pk = 'k' AND (sk = 'row-1' OR sk = 'row-2')",
        "(pk = 'k' AND sk = 'row-1') OR (pk = 'k' AND sk = 'row-2')",
    ] {
        let sql = format!("SELECT * FROM \"OrReads\" WHERE {clause}");
        let page = read(&db, &sql, None, None, true);
        assert_eq!(page.items.unwrap().len(), 2);
        assert_eq!(page.consumed_capacity.unwrap().capacity_units, 2.0);
        // Eventually consistent, the captured read: 1.0, as DynamoDB charges.
        let page = read(&db, &sql, None, None, false);
        assert_eq!(page.consumed_capacity.unwrap().capacity_units, 1.0);
        // DynamoDB returns the two rows over two pages, the second without a
        // token, since the read has passed the last requested key.
        let page = read(&db, &sql, Some(1), None, true);
        assert_eq!(page.items.unwrap().len(), 1);
        assert!(page.next_token.is_some());
        let page = read(&db, &sql, Some(1), page.next_token, true);
        assert_eq!(page.items.unwrap().len(), 1);
        assert!(page.next_token.is_none(), "{clause}");
        // A limit that covers every key needs one page.
        let page = read(&db, &sql, Some(2), None, true);
        assert_eq!(page.items.unwrap().len(), 2);
        assert!(page.next_token.is_none(), "{clause}");
    }
    // A requested key the read hasn't passed keeps the token, even when no
    // item has it: row-9 sorts after row-2.
    let sql = "SELECT * FROM \"OrReads\" WHERE pk = 'k' AND (sk = 'row-2' OR sk = 'row-9')";
    let page = read(&db, sql, Some(1), None, true);
    assert_eq!(page.items.unwrap().len(), 1);
    assert!(page.next_token.is_some());
    // A filter that isn't a fixed set of keys keeps the token at the limit.
    let sql = "SELECT * FROM \"OrReads\" WHERE pk = 'k' AND (x = 1 OR x = 2)";
    let page = read(&db, sql, Some(2), None, true);
    assert!(page.next_token.is_some());
}

/// Three rows: one passes every pair, one fails only the last pair, and one
/// fails the first. Distinct attributes make every term observable.
fn distinct_filter(db: &Database, pairs: usize) -> String {
    for (sk, x) in [("row-1", 1), ("row-2", 2), ("row-3", 3)] {
        let mut item = json!({
            "pk":{"S":"k"}, "sk":{"S":sk}, "g":{"S":"k"},
            "x":{"N":x.to_string()}, "payload":{"S":"x".repeat(3000)}
        });
        for i in 0..pairs {
            let value = if (sk == "row-2" && i == pairs - 1) || (sk == "row-3" && i == 0) {
                3
            } else {
                1
            };
            item[format!("f{i}")] = json!({"N":value.to_string()});
        }
        db.put_item(serde_json::from_value(json!({"TableName":"OrReads", "Item":item})).unwrap())
            .unwrap();
    }
    (0..pairs)
        .map(|i| format!("(f{i} = 1 OR f{i} = 2)"))
        .collect::<Vec<_>>()
        .join(" AND ")
}

#[test]
fn many_or_groups_evaluate_as_a_tree_and_keep_the_partition_read() {
    // #225: DynamoDB runs 9, 16 and 40 pairs without expanding 2^n groups.
    let db = setup("ALL");
    for pairs in [9, 16, 40] {
        let filter = distinct_filter(&db, pairs);
        let sql = format!("SELECT * FROM \"OrReads\" WHERE pk = 'k' AND {filter}");
        let page = read(&db, &sql, None, None, true);
        let items = page.items.unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(
            items[0]["sk"],
            dynoxide::types::AttributeValue::S("row-1".into())
        );
        assert_eq!(page.consumed_capacity.unwrap().capacity_units, 3.0);
        let page = read(&db, &sql, Some(1), None, true);
        assert_eq!(page.items.unwrap().len(), 1);
        // Later pages evaluate the remaining rows but do not match them.
        let page = read(&db, &sql, Some(1), page.next_token, true);
        assert!(page.items.unwrap().is_empty());
        let page = read(&db, &sql, Some(1), page.next_token, true);
        assert!(page.items.unwrap().is_empty());
        let scan_capacity = read(&db, "SELECT * FROM \"OrReads\"", None, None, true)
            .consumed_capacity
            .unwrap()
            .capacity_units;
        let sql = format!("SELECT * FROM \"OrReads\" WHERE {filter}");
        let page = read(&db, &sql, None, None, true);
        let items = page.items.unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(
            items[0]["pk"],
            dynoxide::types::AttributeValue::S("k".into())
        );
        assert_eq!(
            items[0]["sk"],
            dynoxide::types::AttributeValue::S("row-1".into())
        );
        assert_eq!(
            page.consumed_capacity.unwrap().capacity_units,
            scan_capacity
        );
    }
}

#[test]
fn a_wide_filter_keeps_operand_and_index_projection_checks() {
    let filter = vec!["(x = 1 OR x = 2)"; 40].join(" AND ");
    let db = setup("KEYS_ONLY");
    let err = db
        .execute_statement(ExecuteStatementRequest {
            statement: format!("SELECT * FROM \"OrReads\".\"g-keys\" WHERE g = 'k' AND {filter}"),
            ..Default::default()
        })
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("does not project one or more filter attributes: [x]"),
        "{err}"
    );
    let err = db
        .execute_statement(ExecuteStatementRequest {
            statement: format!(
                "SELECT * FROM \"OrReads\" WHERE pk = 'k' AND {filter} AND begins_with(x, 1)"
            ),
            ..Default::default()
        })
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("operator or function: begins_with, operand type: N"),
        "{err}"
    );
}

#[test]
fn a_wide_write_filter_evaluates_every_branch_before_writing() {
    let db = setup("ALL");
    let filter = distinct_filter(&db, 40);
    for (sk, original_x) in [("row-2", "2"), ("row-3", "3")] {
        for action in ["UPDATE \"OrReads\" SET x = 9", "DELETE FROM \"OrReads\""] {
            let err = db
                .execute_statement(ExecuteStatementRequest {
                    statement: format!("{action} WHERE pk = 'k' AND sk = '{sk}' AND {filter}"),
                    ..Default::default()
                })
                .unwrap_err();
            assert!(
                matches!(
                    err,
                    dynoxide::errors::DynoxideError::ConditionalCheckFailedException(..)
                ),
                "{err}"
            );
            let page = read(
                &db,
                &format!("SELECT * FROM \"OrReads\" WHERE pk = 'k' AND sk = '{sk}'"),
                None,
                None,
                true,
            );
            assert_eq!(
                page.items.unwrap()[0]["x"],
                dynoxide::types::AttributeValue::N(original_x.into())
            );
        }
    }
    // The row satisfying every pair can still be updated and deleted.
    for action in ["UPDATE \"OrReads\" SET x = 9", "DELETE FROM \"OrReads\""] {
        db.execute_statement(ExecuteStatementRequest {
            statement: format!("{action} WHERE pk = 'k' AND sk = 'row-1' AND {filter}"),
            ..Default::default()
        })
        .unwrap();
    }
    let page = read(
        &db,
        "SELECT * FROM \"OrReads\" WHERE pk = 'k' AND sk = 'row-1'",
        None,
        None,
        true,
    );
    assert!(page.items.unwrap().is_empty());
}

#[test]
fn wide_filters_preserve_key_type_and_overlap_rejections() {
    let db = setup("ALL");
    let padding = ["(x = 1 OR x = 2)"; 9].join(" AND ");
    for (key_clause, expected) in [
        (
            "pk = 'k' AND (sk = true OR sk = 'row-1')",
            "Key value must be of type S, N, or B. Key name: sk, Key type: BOOL",
        ),
        (
            "pk = 'k' AND (sk = 'row-1' OR sk = 'row-1')",
            "Overlapping conditions with range keys are not supported in where clause",
        ),
        (
            "pk = 'k' AND (sk = 'row-1' OR x = 1)",
            "Overlapping conditions with range keys are not supported in where clause",
        ),
    ] {
        for suffix in [String::new(), format!(" AND {padding}")] {
            let err = db
                .execute_statement(ExecuteStatementRequest {
                    statement: format!("SELECT * FROM \"OrReads\" WHERE {key_clause}{suffix}"),
                    ..Default::default()
                })
                .unwrap_err()
                .to_string();
            assert_eq!(err, expected);
        }
    }
    // A leading scan branch still stops key-type checks in statement order.
    read(
        &db,
        &format!("SELECT * FROM \"OrReads\" WHERE (x = 1 OR pk = true) AND {padding}"),
        None,
        None,
        true,
    );
}

#[test]
fn equivalent_partition_parameters_use_the_same_keyed_read() {
    let db = setup("ALL");
    for (source, key, consistent, capacity) in [
        ("\"OrReads\"", "pk", true, 2.0),
        ("\"OrReads\".\"g-keys\"", "g", false, 1.0),
    ] {
        let sql = format!(
            "SELECT * FROM {source} WHERE ({key} = ? AND sk = 'row-1') OR ({key} = ? AND sk = 'row-2')"
        );
        let request = |limit, token| ExecuteStatementRequest {
            statement: sql.clone(),
            parameters: Some(vec![
                dynoxide::types::AttributeValue::S("k".into()),
                dynoxide::types::AttributeValue::S("k".into()),
            ]),
            limit,
            next_token: token,
            consistent_read: Some(consistent),
            return_consumed_capacity: Some("TOTAL".into()),
            ..Default::default()
        };
        let page = db.execute_statement(request(None, None)).unwrap();
        assert_eq!(page.items.unwrap().len(), 2);
        assert_eq!(page.consumed_capacity.unwrap().capacity_units, capacity);
        let page = db.execute_statement(request(Some(1), None)).unwrap();
        assert_eq!(
            page.items.unwrap()[0]["sk"],
            dynoxide::types::AttributeValue::S("row-1".into())
        );
        let page = db
            .execute_statement(request(Some(1), page.next_token))
            .unwrap();
        assert_eq!(
            page.items.unwrap()[0]["sk"],
            dynoxide::types::AttributeValue::S("row-2".into())
        );
        // The read has passed the last requested key, with nothing after it
        // (#271).
        assert!(page.next_token.is_none(), "{source}");
    }
    let db = setup("KEYS_ONLY");
    let err = db
        .execute_statement(ExecuteStatementRequest {
            statement:
                "SELECT * FROM \"OrReads\".\"g-keys\" WHERE (g = ? AND sk = 'row-1' AND x = 1) OR (g = ? AND sk = 'row-2' AND x = 2)"
                    .into(),
            parameters: Some(vec![dynoxide::types::AttributeValue::S("k".into()); 2]),
            ..Default::default()
        })
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("does not project one or more filter attributes: [x]"),
        "{err}"
    );
}

#[test]
fn equivalent_numeric_partition_spellings_share_the_read_plan() {
    let db = Database::memory().unwrap();
    db.create_table(serde_json::from_value(json!({
        "TableName":"NumericOrReads", "BillingMode":"PAY_PER_REQUEST",
        "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"N"},{"AttributeName":"sk","AttributeType":"S"}],
        "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},{"AttributeName":"sk","KeyType":"RANGE"}]
    })).unwrap()).unwrap();
    for pk in ["0", "1", "2"] {
        for sk in ["row-1", "row-2"] {
            db.put_item(
                serde_json::from_value(json!({"TableName":"NumericOrReads", "Item":{
                    "pk":{"N":pk},"sk":{"S":sk},"payload":{"S":"x".repeat(3000)}
                }}))
                .unwrap(),
            )
            .unwrap();
        }
    }
    for clause in [
        "(pk = ? AND sk = 'row-1') OR (pk = ? AND sk = 'row-2')",
        "pk = ? AND pk = ? AND (sk = 'row-1' OR sk = 'row-2')",
        "(pk = ? AND sk = 'row-1') OR (pk IN [?] AND sk = 'row-2')",
    ] {
        let page = db
            .execute_statement(ExecuteStatementRequest {
                statement: format!("SELECT * FROM \"NumericOrReads\" WHERE {clause}"),
                parameters: Some(vec![
                    dynoxide::types::AttributeValue::N("1".into()),
                    dynoxide::types::AttributeValue::N("1.0".into()),
                ]),
                limit: Some(1),
                consistent_read: Some(true),
                ..Default::default()
            })
            .unwrap();
        let items = page.items.unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(
            items[0]["pk"],
            dynoxide::types::AttributeValue::N("1".into())
        );
        assert_eq!(
            items[0]["sk"],
            dynoxide::types::AttributeValue::S("row-1".into())
        );
    }
}

/// Every page of a statement, as (the rows' `pk/sk`, whether it carried a
/// token, what it cost).
fn walk(
    db: &Database,
    sql: &str,
    limit: Option<usize>,
    consistent: bool,
) -> Vec<(Vec<String>, bool, f64)> {
    let text = |value: &dynoxide::types::AttributeValue| match value {
        dynoxide::types::AttributeValue::S(s) => s.clone(),
        other => panic!("{other:?}"),
    };
    let mut pages = Vec::new();
    let mut token = None;
    loop {
        let page = read(db, sql, limit, token, consistent);
        let rows = page
            .items
            .unwrap()
            .iter()
            .map(|item| format!("{}/{}", text(&item["pk"]), text(&item["sk"])))
            .collect();
        pages.push((
            rows,
            page.next_token.is_some(),
            page.consumed_capacity.unwrap().capacity_units,
        ));
        token = page.next_token;
        if token.is_none() || pages.len() == 8 {
            return pages;
        }
    }
}

fn page(rows: &[&str], token: bool, cost: f64) -> (Vec<String>, bool, f64) {
    (rows.iter().map(|r| r.to_string()).collect(), token, cost)
}

#[test]
fn a_union_of_sort_key_branches_ends_after_its_last_row() {
    // #271, captured eu-west-2 and us-east-1 on 2026-11-03 and, five times in
    // each region, on 2026-11-04: under `Limit: 1` each of these comes back
    // over two pages, the second with no token.
    let db = setup("ALL");
    for (sql, first, second) in [
        (
            "SELECT * FROM \"OrReads\".\"g-keys\" WHERE g = 'k' AND (sk = 'row-1' OR sk = 'row-2')",
            "k/row-1",
            "k/row-2",
        ),
        (
            "SELECT * FROM \"OrReads\" WHERE pk = 'k' AND (sk = 'row-1' OR sk = 'row-2') ORDER BY sk DESC",
            "k/row-2",
            "k/row-1",
        ),
        (
            "SELECT * FROM \"OrReads\" WHERE pk = 'k' AND (sk = 'row-1' OR sk = 'row-3')",
            "k/row-1",
            "k/row-3",
        ),
        (
            "SELECT * FROM \"OrReads\" WHERE pk = 'k' AND (sk = 'row-1' OR sk > 'row-2') ORDER BY sk DESC",
            "k/row-3",
            "k/row-1",
        ),
    ] {
        assert_eq!(
            walk(&db, sql, Some(1), false),
            [page(&[first], true, 0.5), page(&[second], false, 0.5)],
            "{sql}"
        );
    }
    // Three keys under `Limit: 2` take two pages.
    assert_eq!(
        walk(
            &db,
            "SELECT * FROM \"OrReads\" WHERE pk = 'k' AND (sk = 'row-1' OR sk = 'row-2' OR sk = 'row-3')",
            Some(2),
            false
        ),
        [
            page(&["k/row-1", "k/row-2"], true, 1.0),
            page(&["k/row-3"], false, 0.5)
        ]
    );
    // A filter that isn't a union of sort-key branches keeps its token at the
    // limit, and the next page finds nothing.
    assert_eq!(
        walk(
            &db,
            "SELECT * FROM \"OrReads\" WHERE pk = 'k' AND (x = 1 OR x = 2)",
            Some(2),
            false
        ),
        [
            page(&["k/row-1", "k/row-2"], true, 1.0),
            page(&[], false, 0.5)
        ]
    );
}

#[test]
fn an_index_sort_key_shared_by_two_items_keeps_its_token() {
    // Captured us-east-1, 2026-11-03: the read doesn't end on the first of two
    // items under one index key. eu-west-2 returned the same three rows with a
    // fourth, empty page after them.
    let db = setup("ALL");
    for (pk, sk) in [("tie-a", "row-2"), ("tie-b", "row-2"), ("tie-c", "row-1")] {
        db.put_item(
            serde_json::from_value(json!({"TableName":"OrReads", "Item":{
                "pk":{"S":pk},"sk":{"S":sk},"g":{"S":"tie"},"x":{"N":"1"},
                "payload":{"S":"x".repeat(3000)}
            }}))
            .unwrap(),
        )
        .unwrap();
    }
    let sql =
        "SELECT * FROM \"OrReads\".\"g-keys\" WHERE g = 'tie' AND (sk = 'row-1' OR sk = 'row-2')";
    assert_eq!(
        walk(&db, sql, Some(1), false),
        [
            page(&["tie-c/row-1"], true, 0.5),
            page(&["tie-a/row-2"], true, 0.5),
            page(&["tie-b/row-2"], false, 0.5),
        ]
    );
    assert_eq!(
        walk(&db, sql, Some(2), false),
        [
            page(&["tie-c/row-1", "tie-a/row-2"], true, 1.0),
            page(&["tie-b/row-2"], false, 0.5),
        ]
    );
}

#[test]
fn each_requested_key_is_charged_whether_or_not_it_exists() {
    // Captured eu-west-2 and us-east-1, 2026-11-03. Each stored row weighs
    // about 3 KB, half a unit eventually consistent, and a missing key costs
    // what a row does.
    let db = setup("ALL");
    let table = |clause: &str| format!("SELECT * FROM \"OrReads\" WHERE pk = 'k' AND {clause}");
    let cases = [
        (
            table("(sk = 'row-0' OR sk = 'row-2')"),
            Some(1),
            false,
            vec![page(&["k/row-2"], false, 1.0)],
        ),
        (
            table("(sk = 'row-0' OR sk = 'row-2')"),
            None,
            true,
            vec![page(&["k/row-2"], false, 2.0)],
        ),
        (
            table("(sk = 'row-0' OR sk = 'row-9')"),
            None,
            false,
            vec![page(&[], false, 1.0)],
        ),
        (
            table("(sk = 'row-0' OR sk = 'row-00' OR sk = 'row-000' OR sk = 'row-2')"),
            None,
            false,
            vec![page(&["k/row-2"], false, 2.0)],
        ),
        (
            table("(sk = 'row-1' OR sk = 'row-15' OR sk = 'row-2')"),
            Some(1),
            false,
            vec![
                page(&["k/row-1"], true, 0.5),
                page(&["k/row-2"], false, 1.0),
            ],
        ),
        (
            table("(sk = 'row-2' OR sk = 'row-9')"),
            Some(1),
            false,
            vec![page(&["k/row-2"], true, 0.5), page(&[], false, 0.5)],
        ),
        (
            "SELECT * FROM \"OrReads\".\"g-keys\" WHERE g = 'k' AND (sk = 'row-0' OR sk = 'row-2')"
                .to_string(),
            Some(1),
            false,
            vec![page(&["k/row-2"], false, 1.0)],
        ),
    ];
    for (sql, limit, consistent, expected) in cases {
        assert_eq!(walk(&db, &sql, limit, consistent), expected, "{sql}");
    }
}

#[test]
fn sort_key_range_unions_read_only_their_ranges() {
    // Captured eu-west-2 and us-east-1, 2026-11-03: each reads rows 1 and 3
    // and costs what those two rows cost, not the partition's three.
    let db = setup("ALL");
    for sql in [
        "SELECT * FROM \"OrReads\" WHERE pk = 'k' AND (sk < 'row-2' OR sk > 'row-2')",
        "SELECT * FROM \"OrReads\".\"g-keys\" WHERE g = 'k' AND (sk < 'row-2' OR sk BETWEEN 'row-3' AND 'row-4')",
        "SELECT * FROM \"OrReads\" WHERE pk = 'k' AND (begins_with(sk, 'row-1') OR begins_with(sk, 'row-3'))",
        "SELECT * FROM \"OrReads\" WHERE pk = 'k' AND (sk = 'row-1' OR sk > 'row-2')",
    ] {
        assert_eq!(
            walk(&db, sql, None, false),
            [page(&["k/row-1", "k/row-3"], false, 1.0)],
            "{sql}"
        );
    }
    // Ranges that meet, and a branch that doesn't bound the sort key, are
    // refused.
    for clause in [
        "(sk <= 'row-2' OR sk >= 'row-2')",
        "(sk = 'row-1' OR x = 3)",
    ] {
        let err = db
            .execute_statement(ExecuteStatementRequest {
                statement: format!("SELECT * FROM \"OrReads\" WHERE pk = 'k' AND {clause}"),
                ..Default::default()
            })
            .unwrap_err()
            .to_string();
        assert_eq!(
            err, "Overlapping conditions with range keys are not supported in where clause",
            "{clause}"
        );
    }
}

#[test]
fn sort_key_equalities_beside_a_wide_filter_read_only_their_keys() {
    // #225 with #271: too wide to expand, the clause still reads the two keys
    // (captured eu-west-2 and us-east-1, 2026-11-03: 1.0, no items).
    let db = setup("ALL");
    let filter = (0..9)
        .map(|i| format!("(x = {i} OR x = {})", i + 100))
        .collect::<Vec<_>>()
        .join(" AND ");
    let sql = format!(
        "SELECT * FROM \"OrReads\" WHERE pk = 'k' AND (sk = 'row-1' OR sk = 'row-2') AND {filter}"
    );
    assert_eq!(walk(&db, &sql, None, false), [page(&[], false, 1.0)]);
}

#[test]
fn a_string_partition_key_given_one_number_twice_is_refused() {
    // Captured eu-west-2 and us-east-1, 2026-11-03: `1` and `1.0` are one
    // condition, so the key is checked for its type, where a number key reads
    // it (see `equivalent_numeric_partition_spellings_share_the_read_plan`).
    let db = setup("ALL");
    let err = db
        .execute_statement(ExecuteStatementRequest {
            statement: "SELECT * FROM \"OrReads\" WHERE pk = 1 AND pk = 1.0".into(),
            ..Default::default()
        })
        .unwrap_err()
        .to_string();
    assert_eq!(
        err,
        "Key attribute's data type should match its data type in table's schema: Key pk"
    );
}

#[test]
fn number_sort_key_unions_end_after_their_last_row() {
    // Captured eu-west-2 and us-east-1, 2026-11-03: stored number keys order
    // by value, negative and decimal ones included.
    let db = Database::memory().unwrap();
    db.create_table(serde_json::from_value(json!({
        "TableName":"NumberSk", "BillingMode":"PAY_PER_REQUEST",
        "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"sk","AttributeType":"N"}],
        "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},{"AttributeName":"sk","KeyType":"RANGE"}]
    })).unwrap()).unwrap();
    for sk in ["-1", "1.5", "2", "10"] {
        db.put_item(
            serde_json::from_value(json!({"TableName":"NumberSk", "Item":{
                "pk":{"S":"k"},"sk":{"N":sk},"payload":{"S":"x".repeat(3000)}
            }}))
            .unwrap(),
        )
        .unwrap();
    }
    for (clause, first, second) in [
        ("(sk = 2 OR sk = 10)", "2", "10"),
        ("(sk = -1 OR sk = 1.5)", "-1", "1.5"),
    ] {
        let sql = format!("SELECT * FROM \"NumberSk\" WHERE pk = 'k' AND {clause}");
        let mut seen = Vec::new();
        let mut token = None;
        loop {
            let page = read(&db, &sql, Some(1), token, false);
            let rows: Vec<_> = page
                .items
                .unwrap()
                .iter()
                .map(|item| match &item["sk"] {
                    dynoxide::types::AttributeValue::N(n) => n.clone(),
                    other => panic!("{other:?}"),
                })
                .collect();
            seen.push((rows, page.next_token.is_some()));
            token = page.next_token;
            if token.is_none() || seen.len() == 8 {
                break;
            }
        }
        assert_eq!(
            seen,
            [
                (vec![first.to_string()], true),
                (vec![second.to_string()], false)
            ],
            "{clause}"
        );
    }
}

/// Small items in partition `k`, with an LSI on `l` where `row-2` and `row-3`
/// share the key `b`.
fn small_items() -> Database {
    let db = Database::memory().unwrap();
    db.create_table(
        serde_json::from_value(json!({
            "TableName": "Small", "BillingMode": "PAY_PER_REQUEST",
            "AttributeDefinitions": [
                {"AttributeName":"pk","AttributeType":"S"},
                {"AttributeName":"sk","AttributeType":"S"},
                {"AttributeName":"l","AttributeType":"S"}
            ],
            "KeySchema": [{"AttributeName":"pk","KeyType":"HASH"},
                          {"AttributeName":"sk","KeyType":"RANGE"}],
            "LocalSecondaryIndexes": [{"IndexName":"l-all",
                "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                             {"AttributeName":"l","KeyType":"RANGE"}],
                "Projection":{"ProjectionType":"ALL"}}]
        }))
        .unwrap(),
    )
    .unwrap();
    for (sk, l) in [("row-1", "a"), ("row-2", "b"), ("row-3", "b")] {
        db.put_item(
            serde_json::from_value(json!({"TableName":"Small", "Item":{
                "pk":{"S":"k"},"sk":{"S":sk},"l":{"S":l}
            }}))
            .unwrap(),
        )
        .unwrap();
    }
    db
}

#[test]
fn sort_key_branches_are_charged_one_read_each() {
    // Captured eu-west-2 and us-east-1, 2026-11-04, ConsistentRead: each
    // branch rounds its own rows up, a branch that found nothing costs a unit,
    // and the same rows read as one range cost one.
    let db = small_items();
    for (clause, rows, cost) in [
        (
            "(sk = 'row-1' OR sk = 'row-2')",
            &["k/row-1", "k/row-2"][..],
            2.0,
        ),
        (
            "(sk = 'row-1' OR sk = 'row-2' OR sk = 'row-3')",
            &["k/row-1", "k/row-2", "k/row-3"],
            3.0,
        ),
        ("(sk = 'row-0' OR sk = 'row-2')", &["k/row-2"], 2.0),
        ("(sk < 'row-0' OR sk > 'row-2')", &["k/row-3"], 2.0),
        (
            "(sk < 'row-2' OR sk > 'row-2')",
            &["k/row-1", "k/row-3"],
            2.0,
        ),
        (
            "(sk = 'row-1' OR sk > 'row-2')",
            &["k/row-1", "k/row-3"],
            2.0,
        ),
        (
            "((sk = 'row-0' AND sk = 'row-0') OR sk = 'row-2')",
            &["k/row-2"],
            2.0,
        ),
        ("sk > 'row-0'", &["k/row-1", "k/row-2", "k/row-3"], 1.0),
        // An empty branch under begins_with, BETWEEN and a descending read
        // costs a unit too (captured 2026-11-04).
        (
            "(begins_with(sk, 'row-1') OR begins_with(sk, 'zz'))",
            &["k/row-1"],
            2.0,
        ),
        (
            "(sk BETWEEN 'row-0' AND 'row-1' OR sk BETWEEN 'x' AND 'y')",
            &["k/row-1"],
            2.0,
        ),
        (
            "(sk < 'row-0' OR sk > 'row-2') ORDER BY sk DESC",
            &["k/row-3"],
            2.0,
        ),
    ] {
        let sql = format!("SELECT * FROM \"Small\" WHERE pk = 'k' AND {clause}");
        assert_eq!(
            walk(&db, &sql, None, true),
            [page(rows, false, cost)],
            "{clause}"
        );
    }
    let page_of = |sql: &str| walk(&db, sql, None, true);
    assert_eq!(
        page_of("SELECT * FROM \"Small\" WHERE pk = 'k'"),
        [page(&["k/row-1", "k/row-2", "k/row-3"], false, 1.0)]
    );
    // Eventually consistent, two keys cost a unit.
    assert_eq!(
        walk(
            &db,
            "SELECT * FROM \"Small\" WHERE pk = 'k' AND (sk = 'row-1' OR sk = 'row-2')",
            None,
            false
        ),
        [page(&["k/row-1", "k/row-2"], false, 1.0)]
    );
}

#[test]
fn a_page_ending_inside_a_range_keeps_its_token() {
    // DynamoDB keeps the token on most walks of these and drops it on some,
    // the same query either way: 33 of 50 walks kept it across eu-west-2 and
    // us-east-1, captured five times in each region on 2026-11-04. Dynoxide
    // keeps it every time, and the next page finds nothing.
    let db = small_items();
    let small = |clause: &str| format!("SELECT * FROM \"Small\" WHERE pk = 'k' AND {clause}");
    for (clause, first, second) in [
        ("sk > 'row-1'", "k/row-2", "k/row-3"),
        (
            "sk > 'row-1' AND (l = 'b' OR l = 'z')",
            "k/row-2",
            "k/row-3",
        ),
        ("(sk < 'row-2' OR sk > 'row-2')", "k/row-1", "k/row-3"),
        ("(sk = 'row-1' OR sk > 'row-2')", "k/row-1", "k/row-3"),
        (
            "(sk < 'row-2' OR sk > 'row-2') ORDER BY sk DESC",
            "k/row-3",
            "k/row-1",
        ),
    ] {
        assert_eq!(
            walk(&db, &small(clause), Some(1), false),
            [
                page(&[first], true, 0.5),
                page(&[second], true, 0.5),
                page(&[], false, 0.5)
            ],
            "{clause}"
        );
    }
    // Beside a filter too wide to expand as well.
    let wide: String = (0..8)
        .map(|i| format!(" AND (f{i} IS MISSING OR f{i} = 2)"))
        .collect();
    assert_eq!(
        walk(
            &db,
            &small(&format!("sk > 'row-1' AND (l = 'b' OR l = 'z'){wide}")),
            Some(1),
            false
        ),
        [
            page(&["k/row-2"], true, 0.5),
            page(&["k/row-3"], true, 0.5),
            page(&[], false, 0.5)
        ]
    );
}

#[test]
fn a_branch_the_page_has_not_reached_holds_the_token() {
    // Captured eu-west-2 and us-east-1, 2026-11-04, five times in each region:
    // an empty range after the page's last row keeps the token for an empty
    // page, as a missing key does.
    let db = small_items();
    let small = |clause: &str| format!("SELECT * FROM \"Small\" WHERE pk = 'k' AND {clause}");
    assert_eq!(
        walk(
            &db,
            &small("(sk BETWEEN 'row-0' AND 'row-1' OR sk BETWEEN 'x' AND 'y')"),
            Some(1),
            false
        ),
        [page(&["k/row-1"], true, 0.5), page(&[], false, 0.5)]
    );
}

#[test]
fn an_lsi_key_set_charges_and_pages_like_the_table() {
    // Captured eu-west-2 and us-east-1, 2026-11-04.
    let db = small_items();
    let lsi =
        |clause: &str| format!("SELECT * FROM \"Small\".\"l-all\" WHERE pk = 'k' AND {clause}");
    // A missing key holds the token until the read passes it, and costs a
    // unit there.
    assert_eq!(
        walk(&db, &lsi("(l = 'z' OR l = 'a')"), Some(1), false),
        [page(&["k/row-1"], true, 0.5), page(&[], false, 0.5)]
    );
    assert_eq!(
        walk(&db, &lsi("(l = 'a' OR l = 'z')"), None, true),
        [page(&["k/row-1"], false, 2.0)]
    );
    // Two found keys cost a unit each, three items between them.
    let pages = walk(&db, &lsi("(l = 'a' OR l = 'b')"), None, true);
    assert_eq!(pages.len(), 1);
    let mut rows = pages[0].0.clone();
    rows.sort();
    assert_eq!(rows, ["k/row-1", "k/row-2", "k/row-3"]);
    assert_eq!((pages[0].1, pages[0].2), (false, 2.0));
    // Two items under one LSI key both come back, one a page, and the
    // continuation outlasts the first. AWS returned them in the other order,
    // and for the two-key set added an empty page after them.
    for clause in ["l = 'b'", "(l = 'a' OR l = 'b')"] {
        let pages = walk(&db, &lsi(clause), Some(1), false);
        let mut rows: Vec<String> = pages.iter().flat_map(|(rows, _, _)| rows.clone()).collect();
        rows.sort();
        let mut expected = vec!["k/row-2".to_string(), "k/row-3".to_string()];
        if clause.contains("'a'") {
            expected.insert(0, "k/row-1".to_string());
        }
        assert_eq!(rows, expected, "{clause}");
        assert!(
            pages
                .iter()
                .all(|(rows, _, cost)| rows.len() <= 1 && *cost == 0.5)
        );
        assert!(pages[..pages.len() - 1].iter().all(|(_, token, _)| *token));
        assert!(!pages.last().unwrap().1, "{clause} ended without a token");
        assert!(pages.len() <= expected.len() + 1, "{clause}");
    }
}

#[test]
fn a_descending_key_set_charges_a_missing_key_where_it_is_reached() {
    // Captured eu-west-2 and us-east-1, 2026-11-03, under `Limit: 1`.
    let db = setup("ALL");
    let desc = |keys: &str| {
        format!("SELECT * FROM \"OrReads\" WHERE pk = 'k' AND ({keys}) ORDER BY sk DESC")
    };
    // A missing key between two found ones costs a unit on the page that
    // reaches it.
    assert_eq!(
        walk(
            &db,
            &desc("sk = 'row-1' OR sk = 'row-15' OR sk = 'row-2'"),
            Some(1),
            false
        ),
        [
            page(&["k/row-2"], true, 0.5),
            page(&["k/row-1"], false, 1.0)
        ]
    );
    // A missing last key keeps the token for an empty page.
    assert_eq!(
        walk(&db, &desc("sk = 'row-0' OR sk = 'row-2'"), Some(1), false),
        [page(&["k/row-2"], true, 0.5), page(&[], false, 0.5)]
    );
    // A missing first key is charged on the one page.
    assert_eq!(
        walk(&db, &desc("sk = 'row-2' OR sk = 'row-9'"), Some(1), false),
        [page(&["k/row-2"], false, 1.0)]
    );
}

#[test]
fn a_limit_past_the_integer_range_reads_as_usual() {
    // DynamoDB accepts a Limit of 2147483648 (captured eu-west-2 and
    // us-east-1, 2026-11-04). One past 2^63 - 1 used to overflow the read.
    let db = setup("ALL");
    for limit in [2_147_483_648, i64::MAX as usize, usize::MAX] {
        for (clause, rows) in [
            ("", &["k/row-1", "k/row-2", "k/row-3"][..]),
            (
                " AND (sk = 'row-1' OR sk = 'row-3')",
                &["k/row-1", "k/row-3"],
            ),
            (" AND sk = 'row-2'", &["k/row-2"]),
        ] {
            let sql = format!("SELECT * FROM \"OrReads\" WHERE pk = 'k'{clause}");
            let pages = walk(&db, &sql, Some(limit), false);
            assert_eq!(pages.len(), 1, "{limit} {clause}");
            assert_eq!(pages[0].0, rows, "{limit} {clause}");
            assert!(!pages[0].1, "{limit} {clause}");
        }
    }
}
