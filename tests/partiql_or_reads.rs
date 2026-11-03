//! Regressions for #229 and #271–#273, captured in eu-west-2 and us-east-1
//! on 10 and 23 October 2026 (see the corresponding GitHub issues).
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
    // A different partition in each branch retains the previously captured scan behavior.
    read(
        &db,
        "SELECT * FROM \"OrReads\".\"g-keys\" WHERE (g = 'k' AND x = 1) OR g = 'z'",
        None,
        None,
        false,
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
        let page = read(&db, &sql, Some(1), None, true);
        assert_eq!(page.items.unwrap().len(), 1);
        let page = read(&db, &sql, Some(1), page.next_token, true);
        assert_eq!(page.items.unwrap().len(), 1);
        let page = read(&db, &sql, Some(1), page.next_token, true);
        assert!(page.items.unwrap().is_empty());
        assert!(page.next_token.is_none());
    }
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
        let page = db
            .execute_statement(request(Some(1), page.next_token))
            .unwrap();
        assert!(page.items.unwrap().is_empty());
        assert!(page.next_token.is_none());
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
