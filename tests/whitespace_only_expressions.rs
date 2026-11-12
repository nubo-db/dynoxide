//! An expression made only of whitespace is refused as a syntax error at its
//! end, with the whitespace as sent for `near`:
//! `Invalid UpdateExpression: Syntax error; token: "<EOF>", near: "   "`.
//! PutItem, UpdateItem and DeleteItem send it in the request-validation
//! envelope, the reads and transactions bare, and nothing is written.
//!
//! Captured against eu-west-2 and us-east-1 on 10 October 2026, the
//! transaction cases also against ap-southeast-2 and eu-central-1; every
//! region agreed. Dynoxide 1.2.1 answered a whitespace-only UpdateExpression
//! with 200 and wrote the item. The cases marked inferred take the same rule
//! to the other expressions and surfaces that read them.

use dynoxide::Database;
use dynoxide::DynoxideError;
use serde_json::{Value, json};

const BLANK: &str = "   ";
const ENVELOPE: &str = "1 validation error detected: ";

fn syntax_error(kind: &str, near: &str) -> String {
    format!(r#"Invalid {kind}Expression: Syntax error; token: "<EOF>", near: "{near}""#)
}

fn request<T: serde::de::DeserializeOwned>(value: Value) -> T {
    serde_json::from_value(value).unwrap()
}

/// Table `Tbl` keyed on `pk` and `sk`, holding `{pk: e, sk: 1, x: 1}`, as the
/// capture set it up.
fn db() -> Database {
    let db = Database::memory().unwrap();
    db.create_table(request(json!({
        "TableName": "Tbl",
        "KeySchema": [
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"}
        ],
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"}
        ],
        "BillingMode": "PAY_PER_REQUEST"
    })))
    .unwrap();
    db.put_item(request(json!({
        "TableName": "Tbl",
        "Item": {"pk": {"S": "e"}, "sk": {"S": "1"}, "x": {"N": "1"}}
    })))
    .unwrap();
    db
}

/// Every item in `Tbl`, as JSON, in key order.
fn contents(db: &Database) -> Value {
    let mut items: Vec<Value> = db
        .scan(request(json!({"TableName": "Tbl"})))
        .unwrap()
        .items
        .unwrap_or_default()
        .into_iter()
        .map(|item| serde_json::to_value(item).unwrap())
        .collect();
    items.sort_by_key(|item| item.to_string());
    Value::Array(items)
}

/// The table as the capture read it back afterwards: unchanged.
fn untouched() -> Value {
    json!([{"pk": {"S": "e"}, "sk": {"S": "1"}, "x": {"N": "1"}}])
}

fn refusal<T: std::fmt::Debug>(result: Result<T, DynoxideError>, case: &str) -> String {
    match result {
        Ok(response) => panic!("{case} was accepted: {response:?}"),
        Err(err) => {
            assert_eq!(
                err.error_type(),
                "com.amazon.coral.validate#ValidationException",
                "{case}: {err:?}"
            );
            err.to_string()
        }
    }
}

fn key() -> Value {
    json!({"pk": {"S": "e"}, "sk": {"S": "1"}})
}

#[test]
fn a_whitespace_update_expression_is_refused_in_the_envelope_and_writes_nothing() {
    let db = db();
    let new_key = json!({"pk": {"S": "new"}, "sk": {"S": "1"}});
    let cases = [
        ("existing item", key(), BLANK),
        ("missing item", new_key.clone(), BLANK),
        ("tab and newline", new_key, "\t\n"),
    ];
    for (case, key, expression) in cases {
        let got = refusal(
            db.update_item(request(json!({
                "TableName": "Tbl", "Key": key, "UpdateExpression": expression
            }))),
            case,
        );
        assert_eq!(
            got,
            format!("{ENVELOPE}{}", syntax_error("Update", expression)),
            "{case}"
        );
    }
    assert_eq!(contents(&db), untouched());
}

#[test]
fn a_whitespace_condition_is_refused_in_the_envelope_on_every_single_item_write() {
    let db = db();
    let want = format!("{ENVELOPE}{}", syntax_error("Condition", BLANK));
    let update = db.update_item(request(json!({
        "TableName": "Tbl", "Key": key(),
        "UpdateExpression": "SET x = :v",
        "ConditionExpression": BLANK,
        "ExpressionAttributeValues": {":v": {"N": "2"}}
    })));
    assert_eq!(refusal(update, "UpdateItem"), want);
    let put = db.put_item(request(json!({
        "TableName": "Tbl",
        "Item": {"pk": {"S": "e"}, "sk": {"S": "1"}, "x": {"N": "9"}},
        "ConditionExpression": BLANK
    })));
    assert_eq!(refusal(put, "PutItem"), want);
    let delete = db.delete_item(request(json!({
        "TableName": "Tbl", "Key": key(), "ConditionExpression": BLANK
    })));
    assert_eq!(refusal(delete, "DeleteItem"), want);
    assert_eq!(contents(&db), untouched());
}

#[test]
fn a_whitespace_read_expression_is_refused_bare() {
    let db = db();
    let query = db.query(request(json!({
        "TableName": "Tbl", "KeyConditionExpression": BLANK
    })));
    assert_eq!(refusal(query, "Query"), syntax_error("KeyCondition", BLANK));
    let scan = db.scan(request(
        json!({"TableName": "Tbl", "FilterExpression": BLANK}),
    ));
    assert_eq!(refusal(scan, "Scan"), syntax_error("Filter", BLANK));
    let get = db.get_item(request(json!({
        "TableName": "Tbl", "Key": key(), "ProjectionExpression": BLANK
    })));
    assert_eq!(refusal(get, "GetItem"), syntax_error("Projection", BLANK));
}

#[test]
fn a_whitespace_transaction_expression_is_refused_bare_and_writes_nothing() {
    let db = db();
    let cases = [
        (
            "Update",
            json!({"Update": {"TableName": "Tbl", "Key": key(), "UpdateExpression": BLANK}}),
            syntax_error("Update", BLANK),
        ),
        (
            "Put",
            json!({"Put": {
                "TableName": "Tbl",
                "Item": {"pk": {"S": "e"}, "sk": {"S": "1"}, "x": {"N": "8"}},
                "ConditionExpression": BLANK
            }}),
            syntax_error("Condition", BLANK),
        ),
        (
            "ConditionCheck",
            json!({"ConditionCheck": {
                "TableName": "Tbl", "Key": key(), "ConditionExpression": BLANK
            }}),
            syntax_error("Condition", BLANK),
        ),
    ];
    for (case, action, want) in cases {
        let got = refusal(
            db.transact_write_items(request(json!({"TransactItems": [action]}))),
            case,
        );
        assert_eq!(got, want, "{case}");
    }
    assert_eq!(contents(&db), untouched());
}

/// Inferred: the other expressions and surfaces that read them, with no
/// capture of their own, take the captured rule, bare as the reads and
/// transactions answer.
#[test]
fn every_other_surface_refuses_a_whitespace_expression() {
    let db = db();
    let query_filter = db.query(request(json!({
        "TableName": "Tbl",
        "KeyConditionExpression": "pk = :p",
        "FilterExpression": BLANK,
        "ExpressionAttributeValues": {":p": {"S": "e"}}
    })));
    assert_eq!(
        refusal(query_filter, "Query FilterExpression"),
        syntax_error("Filter", BLANK)
    );
    let query_projection = db.query(request(json!({
        "TableName": "Tbl",
        "KeyConditionExpression": "pk = :p",
        "ProjectionExpression": BLANK,
        "ExpressionAttributeValues": {":p": {"S": "e"}}
    })));
    assert_eq!(
        refusal(query_projection, "Query ProjectionExpression"),
        syntax_error("Projection", BLANK)
    );
    let scan_projection = db.scan(request(json!({
        "TableName": "Tbl", "ProjectionExpression": BLANK
    })));
    assert_eq!(
        refusal(scan_projection, "Scan ProjectionExpression"),
        syntax_error("Projection", BLANK)
    );
    let batch_get = db.batch_get_item(request(json!({
        "RequestItems": {"Tbl": {"Keys": [key()], "ProjectionExpression": BLANK}}
    })));
    assert_eq!(
        refusal(batch_get, "BatchGetItem"),
        syntax_error("Projection", BLANK)
    );
    let transact_get = db.transact_get_items(request(json!({
        "TransactItems": [{"Get": {
            "TableName": "Tbl", "Key": key(), "ProjectionExpression": BLANK
        }}]
    })));
    assert_eq!(
        refusal(transact_get, "TransactGetItems"),
        syntax_error("Projection", BLANK)
    );
    let transaction_cases = [
        (
            "Update ConditionExpression",
            json!({"Update": {
                "TableName": "Tbl", "Key": key(),
                "UpdateExpression": "SET x = :v",
                "ConditionExpression": BLANK,
                "ExpressionAttributeValues": {":v": {"N": "2"}}
            }}),
        ),
        (
            "Delete ConditionExpression",
            json!({"Delete": {"TableName": "Tbl", "Key": key(), "ConditionExpression": BLANK}}),
        ),
    ];
    for (case, action) in transaction_cases {
        let got = refusal(
            db.transact_write_items(request(json!({"TransactItems": [action]}))),
            case,
        );
        assert_eq!(got, syntax_error("Condition", BLANK), "{case}");
    }
    assert_eq!(contents(&db), untouched());
}

/// An empty UpdateExpression in a transaction is refused up front, bare,
/// even against a table that doesn't exist (captured eu-west-2 and us-east-1,
/// 9 October 2026), where it used to write an item holding only its key. An
/// empty ProjectionExpression in a TransactGetItems is refused as GetItem
/// refuses one.
#[test]
fn an_empty_expression_in_a_transaction_is_refused_up_front() {
    let db = db();
    for table in ["Tbl", "Absent"] {
        let got = refusal(
            db.transact_write_items(request(json!({"TransactItems": [{"Update": {
                "TableName": table,
                "Key": {"pk": {"S": "new"}, "sk": {"S": "1"}},
                "UpdateExpression": ""
            }}]}))),
            table,
        );
        assert_eq!(
            got,
            "Invalid UpdateExpression: The expression can not be empty;"
        );
    }
    assert_eq!(contents(&db), untouched());
    let transact_get = db.transact_get_items(request(json!({
        "TransactItems": [{"Get": {
            "TableName": "Tbl", "Key": key(), "ProjectionExpression": ""
        }}]
    })));
    assert_eq!(
        refusal(transact_get, "TransactGetItems"),
        "Invalid ProjectionExpression: The expression can not be empty;"
    );
}

/// A TransactGetItems ProjectionExpression is checked before any table is
/// looked up: an empty or whitespace-only one on a Get against a table that
/// doesn't exist is refused as a ValidationException rather than reported as
/// the missing table (captured eu-west-2, 10 October 2026).
#[test]
fn a_transaction_projection_is_refused_before_the_table_is_looked_up() {
    let db = db();
    for (projection, expected) in [
        (
            "",
            "Invalid ProjectionExpression: The expression can not be empty;".to_string(),
        ),
        (BLANK, syntax_error("Projection", BLANK)),
    ] {
        let got = db.transact_get_items(request(json!({
            "TransactItems": [{"Get": {
                "TableName": "Absent", "Key": key(), "ProjectionExpression": projection
            }}]
        })));
        assert_eq!(
            refusal(got, "TransactGetItems on a missing table"),
            expected
        );
    }
}

/// Between two Gets, the earlier Get's name error wins whichever kind it is:
/// an unused name on the first beats an undefined name on the second, as it
/// did when these checks ran after the read (order uncaptured). The first Get
/// has a projection, as a Get without one has its names ignored (captured
/// eu-west-2 and us-east-1, 2026-11-12).
#[test]
fn the_first_gets_name_error_wins_in_a_transaction() {
    let db = db();
    let got = db.transact_get_items(request(json!({
        "TransactItems": [
            {"Get": {
                "TableName": "Tbl", "Key": key(),
                "ProjectionExpression": "pk",
                "ExpressionAttributeNames": {"#u": "x"}
            }},
            {"Get": {
                "TableName": "Tbl", "Key": {"pk": {"S": "e"}, "sk": {"S": "2"}},
                "ProjectionExpression": "#m"
            }}
        ]
    })));
    assert_eq!(
        refusal(got, "two Gets with name errors"),
        "Value provided in ExpressionAttributeNames unused in expressions: keys: {#u}"
    );
}
