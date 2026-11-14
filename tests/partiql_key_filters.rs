//! A PartiQL SELECT that reads one partition refuses a negated condition on a
//! key attribute inside an OR with a branch naming no key attribute, as
//! DynamoDB does, and runs a test of whether a key is there as a filter.
//! Captured against eu-west-2 and us-east-1 on 5 and 6 November 2026.
use dynoxide::Database;
use dynoxide::actions::execute_statement::ExecuteStatementRequest;
use serde_json::json;

fn setup() -> Database {
    let db = Database::memory().unwrap();
    db.create_table(
        serde_json::from_value(json!({
            "TableName": "KeyFilters", "BillingMode": "PAY_PER_REQUEST",
            "AttributeDefinitions": [
                {"AttributeName":"pk","AttributeType":"S"},
                {"AttributeName":"sk","AttributeType":"S"},
                {"AttributeName":"l","AttributeType":"S"},
                {"AttributeName":"g","AttributeType":"S"}
            ],
            "KeySchema": [{"AttributeName":"pk","KeyType":"HASH"},
                          {"AttributeName":"sk","KeyType":"RANGE"}],
            "LocalSecondaryIndexes": [{"IndexName":"l-all",
                "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                             {"AttributeName":"l","KeyType":"RANGE"}],
                "Projection":{"ProjectionType":"ALL"}}],
            "GlobalSecondaryIndexes": [{"IndexName":"g-all",
                "KeySchema":[{"AttributeName":"g","KeyType":"HASH"},
                             {"AttributeName":"l","KeyType":"RANGE"}],
                "Projection":{"ProjectionType":"ALL"}}]
        }))
        .unwrap(),
    )
    .unwrap();
    for (sk, x, l) in [
        ("row-1", 1, "a"),
        ("row-2", 2, "b"),
        ("row-3", 3, "b"),
        ("row-4", 4, "c"),
    ] {
        db.put_item(
            serde_json::from_value(json!({"TableName":"KeyFilters", "Item":{
                "pk":{"S":"k"},"sk":{"S":sk},"x":{"N":x.to_string()},
                "l":{"S":l},"g":{"S":"k"}
            }}))
            .unwrap(),
        )
        .unwrap();
    }
    db
}

fn run(db: &Database, sql: &str) -> Result<usize, String> {
    db.execute_statement(ExecuteStatementRequest {
        statement: sql.replace("\"T\"", "\"KeyFilters\""),
        ..Default::default()
    })
    .map(|r| r.items.unwrap_or_default().len())
    .map_err(|e| format!("{}: {e}", e.error_type().rsplit('#').next().unwrap()))
}

fn refusal(attribute: &str) -> String {
    format!(
        "ValidationException: Filter Expression can only contain non-primary key \
         attributes: Primary key attribute: {attribute}"
    )
}

fn pairs(n: usize) -> String {
    vec!["(NOT sk = 's0' OR x = 0)"; n].join(" AND ")
}

#[test]
fn a_negated_sort_key_in_an_or_with_a_non_key_branch_is_refused() {
    let db = setup();
    for n in [1, 2, 9] {
        let sql = format!("SELECT * FROM \"T\" WHERE pk = 'k' AND {}", pairs(n));
        assert_eq!(run(&db, &sql), Err(refusal("sk")), "{n} pairs");
    }
    for clause in [
        "(x = 0 OR NOT sk = 's0')",
        "NOT (sk = 's0' OR x = 0)",
        "(NOT begins_with(sk, 's') OR x = 0)",
        "(NOT sk BETWEEN 'a' AND 'b' OR x = 0)",
    ] {
        let sql = format!("SELECT * FROM \"T\" WHERE pk = 'k' AND {clause}");
        assert_eq!(run(&db, &sql), Err(refusal("sk")), "{clause}");
    }
}

/// Any written `NOT` counts, including the ones spelt into a form and a second
/// `NOT` that cancels the first, and so does a `NOT` over an AND, which De
/// Morgan makes an OR.
#[test]
fn every_written_not_on_the_sort_key_is_refused() {
    let db = setup();
    for clause in [
        "(sk NOT IN ['s0', 's1'] OR x = 0)",
        "(sk NOT BETWEEN 'a' AND 'b' OR x = 0)",
        "(NOT contains(sk, 's') OR x = 0)",
        "(NOT attribute_exists(sk) OR x = 0)",
        "(sk IS NOT MISSING OR x = 0)",
        "(sk IS NOT NULL OR x = 0)",
        "(sk IS NOT STRING OR x = 0)",
        "NOT (NOT (sk = 's0' OR x = 0))",
        "NOT (sk = 's0' AND x = 0)",
    ] {
        let sql = format!("SELECT * FROM \"T\" WHERE pk = 'k' AND {clause}");
        assert_eq!(run(&db, &sql), Err(refusal("sk")), "{clause}");
    }
    assert_eq!(
        run(
            &db,
            "SELECT * FROM \"T\".\"l-all\" WHERE pk = 'k' AND (l NOT IN ['a'] OR x = 0)"
        ),
        Err(refusal("l"))
    );
}

#[test]
fn a_key_of_the_wrong_type_is_reported_before_the_refusal() {
    let db = setup();
    assert_eq!(
        run(
            &db,
            "SELECT * FROM \"T\" WHERE pk = true AND (NOT sk = 's0' OR x = 0)"
        ),
        Err(
            "ValidationException: Key value must be of type S, N, or B. \
             Key name: pk, Key type: BOOL"
                .to_string()
        )
    );
}

/// `attribute_exists`, `attribute_not_exists` and `IS MISSING` on a key don't
/// narrow the read, so beside a non-key branch they filter rather than meet
/// as key ranges. `begins_with` does narrow it, and is still refused for
/// overlapping.
#[test]
fn a_test_of_whether_a_key_is_there_filters_beside_a_non_key_branch() {
    let db = setup();
    for (source, clause, rows) in [
        (
            "\"T\"",
            "pk = 'k' AND (attribute_not_exists(sk) OR x = 1)",
            1,
        ),
        (
            "\"T\"",
            "pk = 'k' AND (attribute_not_exists(sk) OR x = 0)",
            0,
        ),
        ("\"T\"", "pk = 'k' AND (attribute_exists(sk) OR x = 0)", 4),
        ("\"T\"", "pk = 'k' AND (sk IS MISSING OR x = 1)", 1),
        (
            "\"T\"",
            "pk = 'k' AND (attribute_not_exists(pk) OR x = 1)",
            1,
        ),
        (
            "\"T\"",
            "pk = 'k' AND (attribute_not_exists(sk) OR attribute_not_exists(x))",
            0,
        ),
        (
            "\"T\"",
            "pk = 'k' AND (attribute_type(sk, 'S') OR x = 0)",
            4,
        ),
        ("\"T\"", "pk = 'k' AND (sk IS STRING OR x = 0)", 4),
        (
            "\"T\".\"l-all\"",
            "pk = 'k' AND (attribute_not_exists(l) OR x = 1)",
            1,
        ),
        (
            "\"T\".\"g-all\"",
            "g = 'k' AND (attribute_not_exists(l) OR x = 1)",
            1,
        ),
    ] {
        let sql = format!("SELECT * FROM {source} WHERE {clause}");
        assert_eq!(run(&db, &sql), Ok(rows), "{source} {clause}");
    }
    assert_eq!(
        run(
            &db,
            "SELECT * FROM \"T\" WHERE pk = 'k' AND (begins_with(sk, 'row') OR x = 0)"
        ),
        Err(
            "ValidationException: Overlapping conditions with range keys are not \
             supported in where clause"
                .to_string()
        )
    );
}

/// Under `Limit: 1` the filter reads one row a page and ends on an empty page
/// with no token, the matching row on the first.
#[test]
fn a_key_test_beside_a_non_key_branch_pages_through_the_partition() {
    let db = setup();
    let mut pages = Vec::new();
    let mut next_token = None;
    loop {
        let response = db
            .execute_statement(ExecuteStatementRequest {
                statement: "SELECT * FROM \"KeyFilters\" WHERE pk = 'k' \
                            AND (attribute_not_exists(sk) OR x = 1)"
                    .to_string(),
                limit: Some(1),
                next_token: next_token.take(),
                ..Default::default()
            })
            .unwrap();
        let rows = response.items.unwrap_or_default().len();
        let more = response.next_token.is_some();
        pages.push((rows, more));
        if !more || pages.len() > 8 {
            break;
        }
        next_token = response.next_token;
    }
    assert_eq!(
        pages,
        [(1, true), (0, true), (0, true), (0, true), (0, false)]
    );
}

#[test]
fn a_negated_partition_key_in_an_or_with_a_non_key_branch_is_refused() {
    let db = setup();
    assert_eq!(
        run(
            &db,
            "SELECT * FROM \"T\" WHERE pk = 'k' AND (NOT pk = 'z' OR x = 0)"
        ),
        Err(refusal("pk"))
    );
}

#[test]
fn an_index_refuses_its_own_negated_sort_key_and_not_the_tables() {
    let db = setup();
    for (source, key) in [("\"T\".\"l-all\"", "pk"), ("\"T\".\"g-all\"", "g")] {
        let sql = format!("SELECT * FROM {source} WHERE {key} = 'k' AND (NOT l = 'a' OR x = 0)");
        assert_eq!(run(&db, &sql), Err(refusal("l")), "{source}");
        let sql = format!("SELECT * FROM {source} WHERE {key} = 'k' AND (NOT sk = 's0' OR x = 0)");
        assert_eq!(run(&db, &sql), Ok(4), "{source}");
    }
}

#[test]
fn shapes_outside_the_rule_still_run() {
    let db = setup();
    for (clause, rows) in [
        ("pk = 'k' AND (sk <> 's0' OR x = 0)", 4),
        ("pk = 'k' AND NOT sk = 's0'", 4),
        ("pk = 'k' AND (NOT sk = 's0' AND x = 0)", 0),
        (
            "(pk = 'k' AND NOT sk = 's0') OR (pk = 'k' AND sk = 'row-2')",
            4,
        ),
        ("pk = 'k' AND (NOT sk = 's0' OR sk = 'row-2')", 4),
        ("NOT sk = 's0' OR x = 0", 4),
    ] {
        let sql = format!("SELECT * FROM \"T\" WHERE {clause}");
        assert_eq!(run(&db, &sql), Ok(rows), "{clause}");
    }
}

#[test]
fn a_missing_table_is_reported_before_the_refusal() {
    let db = setup();
    let err = run(
        &db,
        "SELECT * FROM \"Missing\" WHERE pk = 'k' AND (NOT sk = 's0' OR x = 0)",
    )
    .unwrap_err();
    assert!(err.starts_with("ResourceNotFoundException:"), "{err}");
}

/// Each page of a walk at `limit`: its rows and whether it carried a token.
fn pages(db: &Database, sql: &str, limit: usize) -> Vec<(usize, bool)> {
    let mut pages = Vec::new();
    let mut next_token = None;
    loop {
        let response = db
            .execute_statement(ExecuteStatementRequest {
                statement: sql.replace("\"T\"", "\"KeyFilters\""),
                limit: Some(limit),
                next_token: next_token.take(),
                ..Default::default()
            })
            .unwrap();
        let more = response.next_token.is_some();
        pages.push((response.items.unwrap_or_default().len(), more));
        if !more || pages.len() > 8 {
            return pages;
        }
        next_token = response.next_token;
    }
}

/// A read by partition key with no filter hands back no token on the page
/// holding its last row, on the table and on both kinds of index, whether the
/// index's sort key ties across rows or not. Captured on DynamoDB in eu-west-2
/// and us-east-1 on 6 and 13 November 2026 over a partition of four rows.
#[test]
fn a_read_by_partition_key_ends_on_its_last_row() {
    let db = setup();
    for sql in [
        "SELECT * FROM \"T\" WHERE pk = 'k'",
        "SELECT * FROM \"T\".\"l-all\" WHERE pk = 'k'",
        "SELECT * FROM \"T\".\"g-all\" WHERE g = 'k'",
    ] {
        assert_eq!(
            pages(&db, sql, 1),
            [(1, true), (1, true), (1, true), (1, false)],
            "{sql} at Limit 1"
        );
        assert_eq!(pages(&db, sql, 4), [(4, false)], "{sql} at Limit 4");
    }
}

/// Read in descending order, the walk keeps its trailing page.
#[test]
fn a_descending_read_keeps_its_trailing_page() {
    let db = setup();
    assert_eq!(
        pages(
            &db,
            "SELECT * FROM \"T\" WHERE pk = 'k' ORDER BY sk DESC",
            4
        ),
        [(4, true), (0, false)]
    );
}

/// `begins_with` is not consistent on DynamoDB: over four rows it ended on the
/// last row at `Limit` 1 on every walk, but at `Limit` 4 kept a trailing empty
/// page on 9 walks out of 10. Dynoxide keeps the trailing page for it, on the
/// table and on both kinds of index, the indexes here over two rows sharing
/// their sort key.
#[test]
fn begins_with_keeps_its_trailing_page() {
    let db = setup();
    let sql = "SELECT * FROM \"T\" WHERE pk = 'k' AND begins_with(sk, 'row-')";
    assert_eq!(
        pages(&db, sql, 1),
        [(1, true), (1, true), (1, true), (1, true), (0, false)]
    );
    assert_eq!(pages(&db, sql, 4), [(4, true), (0, false)]);
    for sql in [
        "SELECT * FROM \"T\".\"l-all\" WHERE pk = 'k' AND begins_with(l, 'b')",
        "SELECT * FROM \"T\".\"g-all\" WHERE g = 'k' AND begins_with(l, 'b')",
    ] {
        assert_eq!(
            pages(&db, sql, 1),
            [(1, true), (1, true), (0, false)],
            "{sql} at Limit 1"
        );
        assert_eq!(
            pages(&db, sql, 2),
            [(2, true), (0, false)],
            "{sql} at Limit 2"
        );
    }
}

/// More reads by partition key that end on their last row: the key written
/// as a one-value `IN` or with the value first, an ascending `ORDER BY`, a
/// bound beyond the last row, and `>=` on either kind of index. Captured on
/// DynamoDB in eu-west-2 and us-east-1 on 14 November 2026.
#[test]
fn more_reads_by_partition_key_end_on_their_last_row() {
    let db = setup();
    for sql in [
        "SELECT * FROM \"T\" WHERE pk IN ['k']",
        "SELECT * FROM \"T\" WHERE 'k' = pk",
        "SELECT * FROM \"T\" WHERE pk = 'k' ORDER BY sk",
        "SELECT * FROM \"T\" WHERE pk = 'k' ORDER BY sk ASC",
        "SELECT * FROM \"T\" WHERE pk = 'k' AND sk <= 'row-9'",
        "SELECT * FROM \"T\" WHERE pk = 'k' AND sk BETWEEN 'row-1' AND 'row-9'",
        "SELECT * FROM \"T\".\"l-all\" WHERE pk = 'k' AND l >= 'a'",
        "SELECT * FROM \"T\".\"g-all\" WHERE g = 'k' AND l >= 'a'",
    ] {
        assert_eq!(
            pages(&db, sql, 1),
            [(1, true), (1, true), (1, true), (1, false)],
            "{sql} at Limit 1"
        );
        assert_eq!(pages(&db, sql, 4), [(4, false)], "{sql} at Limit 4");
    }
}

/// DynamoDB reads a clause written with `NOT NOT` around the key as a scan,
/// and ends the walk on an empty page (captured on DynamoDB in eu-west-2 and
/// us-east-1 on 14 November 2026). Dynoxide reads it by key, and pages it as
/// it did before it ended a read on its last row: `NOT NOT pk = 'k'` keeps the
/// trailing page, and with `sk <= 'row-4'` beside it the page ending on that
/// closed bound carries no token, as a sort key range ending on its bound
/// always has here.
#[test]
fn a_key_written_under_not_pages_as_before() {
    let db = setup();
    let sql = "SELECT * FROM \"T\" WHERE NOT NOT pk = 'k'";
    assert_eq!(
        pages(&db, sql, 1),
        [(1, true), (1, true), (1, true), (1, true), (0, false)]
    );
    assert_eq!(pages(&db, sql, 4), [(4, true), (0, false)]);
    let sql = "SELECT * FROM \"T\" WHERE NOT NOT (pk = 'k' AND sk <= 'row-4')";
    assert_eq!(
        pages(&db, sql, 1),
        [(1, true), (1, true), (1, true), (1, false)]
    );
    assert_eq!(pages(&db, sql, 4), [(4, false)]);
}
