//! A PartiQL SELECT that reads one partition refuses a negated condition on a
//! key attribute inside an OR with a branch naming no key attribute, as
//! DynamoDB does. Captured against eu-west-2 and us-east-1 on 5 November 2026.
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
