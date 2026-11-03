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

#[test]
fn many_or_groups_evaluate_as_a_tree_and_keep_the_partition_read() {
    // #225: DynamoDB runs 9, 16 and 40 pairs without expanding 2^n groups.
    let db = setup("ALL");
    for pairs in [9, 16, 40] {
        let filter = vec!["(x = 1 OR x = 2)"; pairs].join(" AND ");
        let sql = format!("SELECT * FROM \"OrReads\" WHERE pk = 'k' AND {filter}");
        let page = read(&db, &sql, None, None, true);
        assert_eq!(page.items.unwrap().len(), 2);
        assert_eq!(page.consumed_capacity.unwrap().capacity_units, 3.0);
        let page = read(&db, &sql, Some(1), None, true);
        assert_eq!(page.items.unwrap().len(), 1);
        let sql = format!("SELECT * FROM \"OrReads\" WHERE {filter}");
        let page = read(&db, &sql, None, None, true);
        assert_eq!(page.items.unwrap().len(), 6);
        assert_eq!(page.consumed_capacity.unwrap().capacity_units, 7.0);
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
    let filter = vec!["(x = 1 OR x = 2)"; 40].join(" AND ");
    let db = setup("ALL");
    let err = db
        .execute_statement(ExecuteStatementRequest {
            statement: format!(
                "UPDATE \"OrReads\" SET x = 9 WHERE pk = 'k' AND sk = 'row-3' AND {filter}"
            ),
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
        "SELECT * FROM \"OrReads\" WHERE pk = 'k' AND sk = 'row-3'",
        None,
        None,
        true,
    );
    assert_eq!(
        page.items.unwrap()[0]["x"],
        dynoxide::types::AttributeValue::N("3".into())
    );
}
