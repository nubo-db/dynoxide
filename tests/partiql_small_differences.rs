//! Four small differences between a PartiQL statement here and on DynamoDB:
//! an UPDATE of a key attribute through a path, `ConsistentRead` written as a
//! string, the ORDER BY refusal for a quoted name, and a condition function
//! used as an operand.
//!
//! Each `const` holds requests sent to eu-west-2 on 11 November 2026, in the
//! order they were sent, with the answer DynamoDB gave each one, against an
//! on-demand table keyed `pk` (S) and `sk` (S). They are data captured from
//! DynamoDB, not written by hand, except where a row says it is inferred. A
//! test replays its rows in order against a new server and compares each
//! answer with DynamoDB's as JSON, so key order makes no difference.

#![cfg(feature = "http-server")]

use dynoxide::Database;
use dynoxide::actions::create_table::CreateTableRequest;

/// An operation, its request body and DynamoDB's answer.
type Row = (&'static str, &'static str, &'static str);

const TABLE: &str = "_capture_20261111_230_t";

async fn start_server() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let db = Database::memory().unwrap();
    db.create_table(
        serde_json::from_value::<CreateTableRequest>(serde_json::json!({
            "TableName": TABLE,
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
        .unwrap(),
    )
    .unwrap();
    tokio::spawn(async move {
        dynoxide::server::serve_on(listener, db).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    url
}

async fn answer(url: &str, operation: &str, body: &str) -> serde_json::Value {
    reqwest::Client::new()
        .post(url)
        .header("x-amz-target", format!("DynamoDB_20120810.{operation}"))
        .header("content-type", "application/x-amz-json-1.0")
        .header(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=fakekey/20260101/eu-west-2/dynamodb/aws4_request, \
             SignedHeaders=host;x-amz-date;x-amz-target, Signature=fakesig",
        )
        .header("x-amz-date", "20260101T000000Z")
        .body(body.to_string())
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

/// Send every row in order to a new server, and fail listing each answer that
/// differs from DynamoDB's.
async fn replay(rows: &[Row]) {
    let url = start_server().await;
    let mut wrong = Vec::new();
    for &(operation, body, expected) in rows {
        let expected: serde_json::Value = serde_json::from_str(expected).unwrap();
        let got = answer(&url, operation, body).await;
        if got != expected {
            wrong.push(format!(
                "{operation} {body}\n  expected {expected}\n  got      {got}"
            ));
        }
    }
    assert!(
        wrong.is_empty(),
        "{} of {} answers differ:\n{}",
        wrong.len(),
        rows.len(),
        wrong.join("\n")
    );
}

/// A SET or REMOVE whose path starts at a key attribute is refused as an
/// update of that key, at any depth, quoted or not, whether or not the item
/// exists, and leaves the item as it was. `SET m.pk = 2` is no key update.
#[tokio::test]
async fn an_update_through_a_key_path_is_refused_as_a_key_update() {
    replay(KEY_PATHS).await;
}

/// With both keys named the sort key is reported, in either order. The key
/// refusal loses to a WHERE clause without the whole key and to a parameter
/// count that doesn't match, and BatchExecuteStatement and ExecuteTransaction
/// report it against the statement.
#[tokio::test]
async fn the_sort_key_is_reported_when_both_keys_are_updated() {
    replay(KEY_PATHS_BOTH_KEYS).await;
    replay(KEY_NAMES_BOTH_KEYS_INFERRED).await;
}

/// Inferred from the captured rows that name both keys through a path, in
/// either order: the captured whole-name rows write `sk` first.
const KEY_NAMES_BOTH_KEYS_INFERRED: &[Row] = &[
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_t\" SET pk = 'z' SET sk = 'z' WHERE pk='zz' AND sk='zz'"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"One or more parameter values were invalid: Cannot update attribute sk. This attribute is part of the key"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_t\" REMOVE pk REMOVE sk WHERE pk='zz' AND sk='zz'"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"One or more parameter values were invalid: Cannot update attribute sk. This attribute is part of the key"}"#,
    ),
];

/// Round one, the UPDATE rows: each refused update is followed by the item
/// read back.
const KEY_PATHS: &[Row] = &[
    (
        "PutItem",
        r#"{"TableName":"_capture_20261111_230_t","Item":{"pk":{"S":"a"},"sk":{"S":"b"},"m":{"M":{"x":{"N":"1"},"pk":{"N":"1"}}},"l":{"L":[{"N":"1"},{"N":"2"}]},"n":{"N":"5"},"a.b":{"N":"1"},"a b":{"N":"1"},"s":{"S":"hello"},"bt":{"BOOL":true},"bf":{"BOOL":false}}}"#,
        r#"{}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_t\" SET pk.x = 1 WHERE pk='a' AND sk='b'"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"One or more parameter values were invalid: Cannot update attribute pk. This attribute is part of the key"}"#,
    ),
    (
        "GetItem",
        r#"{"TableName":"_capture_20261111_230_t","Key":{"pk":{"S":"a"},"sk":{"S":"b"}},"ConsistentRead":true}"#,
        r#"{"Item":{"s":{"S":"hello"},"bt":{"BOOL":true},"a b":{"N":"1"},"bf":{"BOOL":false},"a.b":{"N":"1"},"l":{"L":[{"N":"1"},{"N":"2"}]},"m":{"M":{"pk":{"N":"1"},"x":{"N":"1"}}},"pk":{"S":"a"},"n":{"N":"5"},"sk":{"S":"b"}}}"#,
    ),
    (
        "PutItem",
        r#"{"TableName":"_capture_20261111_230_t","Item":{"pk":{"S":"a"},"sk":{"S":"b"},"m":{"M":{"x":{"N":"1"},"pk":{"N":"1"}}},"l":{"L":[{"N":"1"},{"N":"2"}]},"n":{"N":"5"},"a.b":{"N":"1"},"a b":{"N":"1"},"s":{"S":"hello"},"bt":{"BOOL":true},"bf":{"BOOL":false}}}"#,
        r#"{}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_t\" REMOVE pk.x WHERE pk='a' AND sk='b'"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"One or more parameter values were invalid: Cannot update attribute pk. This attribute is part of the key"}"#,
    ),
    (
        "GetItem",
        r#"{"TableName":"_capture_20261111_230_t","Key":{"pk":{"S":"a"},"sk":{"S":"b"}},"ConsistentRead":true}"#,
        r#"{"Item":{"s":{"S":"hello"},"bt":{"BOOL":true},"a b":{"N":"1"},"bf":{"BOOL":false},"a.b":{"N":"1"},"l":{"L":[{"N":"1"},{"N":"2"}]},"m":{"M":{"pk":{"N":"1"},"x":{"N":"1"}}},"pk":{"S":"a"},"n":{"N":"5"},"sk":{"S":"b"}}}"#,
    ),
    (
        "PutItem",
        r#"{"TableName":"_capture_20261111_230_t","Item":{"pk":{"S":"a"},"sk":{"S":"b"},"m":{"M":{"x":{"N":"1"},"pk":{"N":"1"}}},"l":{"L":[{"N":"1"},{"N":"2"}]},"n":{"N":"5"},"a.b":{"N":"1"},"a b":{"N":"1"},"s":{"S":"hello"},"bt":{"BOOL":true},"bf":{"BOOL":false}}}"#,
        r#"{}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_t\" SET pk[0] = 1 WHERE pk='a' AND sk='b'"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"One or more parameter values were invalid: Cannot update attribute pk. This attribute is part of the key"}"#,
    ),
    (
        "GetItem",
        r#"{"TableName":"_capture_20261111_230_t","Key":{"pk":{"S":"a"},"sk":{"S":"b"}},"ConsistentRead":true}"#,
        r#"{"Item":{"s":{"S":"hello"},"bt":{"BOOL":true},"a b":{"N":"1"},"bf":{"BOOL":false},"a.b":{"N":"1"},"l":{"L":[{"N":"1"},{"N":"2"}]},"m":{"M":{"pk":{"N":"1"},"x":{"N":"1"}}},"pk":{"S":"a"},"n":{"N":"5"},"sk":{"S":"b"}}}"#,
    ),
    (
        "PutItem",
        r#"{"TableName":"_capture_20261111_230_t","Item":{"pk":{"S":"a"},"sk":{"S":"b"},"m":{"M":{"x":{"N":"1"},"pk":{"N":"1"}}},"l":{"L":[{"N":"1"},{"N":"2"}]},"n":{"N":"5"},"a.b":{"N":"1"},"a b":{"N":"1"},"s":{"S":"hello"},"bt":{"BOOL":true},"bf":{"BOOL":false}}}"#,
        r#"{}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_t\" REMOVE pk[0] WHERE pk='a' AND sk='b'"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"One or more parameter values were invalid: Cannot update attribute pk. This attribute is part of the key"}"#,
    ),
    (
        "GetItem",
        r#"{"TableName":"_capture_20261111_230_t","Key":{"pk":{"S":"a"},"sk":{"S":"b"}},"ConsistentRead":true}"#,
        r#"{"Item":{"s":{"S":"hello"},"bt":{"BOOL":true},"a b":{"N":"1"},"bf":{"BOOL":false},"a.b":{"N":"1"},"l":{"L":[{"N":"1"},{"N":"2"}]},"m":{"M":{"pk":{"N":"1"},"x":{"N":"1"}}},"pk":{"S":"a"},"n":{"N":"5"},"sk":{"S":"b"}}}"#,
    ),
    (
        "PutItem",
        r#"{"TableName":"_capture_20261111_230_t","Item":{"pk":{"S":"a"},"sk":{"S":"b"},"m":{"M":{"x":{"N":"1"},"pk":{"N":"1"}}},"l":{"L":[{"N":"1"},{"N":"2"}]},"n":{"N":"5"},"a.b":{"N":"1"},"a b":{"N":"1"},"s":{"S":"hello"},"bt":{"BOOL":true},"bf":{"BOOL":false}}}"#,
        r#"{}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_t\" SET sk.x = 1 WHERE pk='a' AND sk='b'"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"One or more parameter values were invalid: Cannot update attribute sk. This attribute is part of the key"}"#,
    ),
    (
        "GetItem",
        r#"{"TableName":"_capture_20261111_230_t","Key":{"pk":{"S":"a"},"sk":{"S":"b"}},"ConsistentRead":true}"#,
        r#"{"Item":{"s":{"S":"hello"},"bt":{"BOOL":true},"a b":{"N":"1"},"bf":{"BOOL":false},"a.b":{"N":"1"},"l":{"L":[{"N":"1"},{"N":"2"}]},"m":{"M":{"pk":{"N":"1"},"x":{"N":"1"}}},"pk":{"S":"a"},"n":{"N":"5"},"sk":{"S":"b"}}}"#,
    ),
    (
        "PutItem",
        r#"{"TableName":"_capture_20261111_230_t","Item":{"pk":{"S":"a"},"sk":{"S":"b"},"m":{"M":{"x":{"N":"1"},"pk":{"N":"1"}}},"l":{"L":[{"N":"1"},{"N":"2"}]},"n":{"N":"5"},"a.b":{"N":"1"},"a b":{"N":"1"},"s":{"S":"hello"},"bt":{"BOOL":true},"bf":{"BOOL":false}}}"#,
        r#"{}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_t\" REMOVE sk.x WHERE pk='a' AND sk='b'"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"One or more parameter values were invalid: Cannot update attribute sk. This attribute is part of the key"}"#,
    ),
    (
        "GetItem",
        r#"{"TableName":"_capture_20261111_230_t","Key":{"pk":{"S":"a"},"sk":{"S":"b"}},"ConsistentRead":true}"#,
        r#"{"Item":{"s":{"S":"hello"},"bt":{"BOOL":true},"a b":{"N":"1"},"bf":{"BOOL":false},"a.b":{"N":"1"},"l":{"L":[{"N":"1"},{"N":"2"}]},"m":{"M":{"pk":{"N":"1"},"x":{"N":"1"}}},"pk":{"S":"a"},"n":{"N":"5"},"sk":{"S":"b"}}}"#,
    ),
    (
        "PutItem",
        r#"{"TableName":"_capture_20261111_230_t","Item":{"pk":{"S":"a"},"sk":{"S":"b"},"m":{"M":{"x":{"N":"1"},"pk":{"N":"1"}}},"l":{"L":[{"N":"1"},{"N":"2"}]},"n":{"N":"5"},"a.b":{"N":"1"},"a b":{"N":"1"},"s":{"S":"hello"},"bt":{"BOOL":true},"bf":{"BOOL":false}}}"#,
        r#"{}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_t\" REMOVE sk[0] WHERE pk='a' AND sk='b'"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"One or more parameter values were invalid: Cannot update attribute sk. This attribute is part of the key"}"#,
    ),
    (
        "GetItem",
        r#"{"TableName":"_capture_20261111_230_t","Key":{"pk":{"S":"a"},"sk":{"S":"b"}},"ConsistentRead":true}"#,
        r#"{"Item":{"s":{"S":"hello"},"bt":{"BOOL":true},"a b":{"N":"1"},"bf":{"BOOL":false},"a.b":{"N":"1"},"l":{"L":[{"N":"1"},{"N":"2"}]},"m":{"M":{"pk":{"N":"1"},"x":{"N":"1"}}},"pk":{"S":"a"},"n":{"N":"5"},"sk":{"S":"b"}}}"#,
    ),
    (
        "PutItem",
        r#"{"TableName":"_capture_20261111_230_t","Item":{"pk":{"S":"a"},"sk":{"S":"b"},"m":{"M":{"x":{"N":"1"},"pk":{"N":"1"}}},"l":{"L":[{"N":"1"},{"N":"2"}]},"n":{"N":"5"},"a.b":{"N":"1"},"a b":{"N":"1"},"s":{"S":"hello"},"bt":{"BOOL":true},"bf":{"BOOL":false}}}"#,
        r#"{}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_t\" SET \"pk\".x = 1 WHERE pk='a' AND sk='b'"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"One or more parameter values were invalid: Cannot update attribute pk. This attribute is part of the key"}"#,
    ),
    (
        "GetItem",
        r#"{"TableName":"_capture_20261111_230_t","Key":{"pk":{"S":"a"},"sk":{"S":"b"}},"ConsistentRead":true}"#,
        r#"{"Item":{"s":{"S":"hello"},"bt":{"BOOL":true},"a b":{"N":"1"},"bf":{"BOOL":false},"a.b":{"N":"1"},"l":{"L":[{"N":"1"},{"N":"2"}]},"m":{"M":{"pk":{"N":"1"},"x":{"N":"1"}}},"pk":{"S":"a"},"n":{"N":"5"},"sk":{"S":"b"}}}"#,
    ),
    (
        "PutItem",
        r#"{"TableName":"_capture_20261111_230_t","Item":{"pk":{"S":"a"},"sk":{"S":"b"},"m":{"M":{"x":{"N":"1"},"pk":{"N":"1"}}},"l":{"L":[{"N":"1"},{"N":"2"}]},"n":{"N":"5"},"a.b":{"N":"1"},"a b":{"N":"1"},"s":{"S":"hello"},"bt":{"BOOL":true},"bf":{"BOOL":false}}}"#,
        r#"{}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_t\" REMOVE \"pk\".\"x\" WHERE pk='a' AND sk='b'"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"One or more parameter values were invalid: Cannot update attribute pk. This attribute is part of the key"}"#,
    ),
    (
        "GetItem",
        r#"{"TableName":"_capture_20261111_230_t","Key":{"pk":{"S":"a"},"sk":{"S":"b"}},"ConsistentRead":true}"#,
        r#"{"Item":{"s":{"S":"hello"},"bt":{"BOOL":true},"a b":{"N":"1"},"bf":{"BOOL":false},"a.b":{"N":"1"},"l":{"L":[{"N":"1"},{"N":"2"}]},"m":{"M":{"pk":{"N":"1"},"x":{"N":"1"}}},"pk":{"S":"a"},"n":{"N":"5"},"sk":{"S":"b"}}}"#,
    ),
    (
        "PutItem",
        r#"{"TableName":"_capture_20261111_230_t","Item":{"pk":{"S":"a"},"sk":{"S":"b"},"m":{"M":{"x":{"N":"1"},"pk":{"N":"1"}}},"l":{"L":[{"N":"1"},{"N":"2"}]},"n":{"N":"5"},"a.b":{"N":"1"},"a b":{"N":"1"},"s":{"S":"hello"},"bt":{"BOOL":true},"bf":{"BOOL":false}}}"#,
        r#"{}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_t\" SET pk = 'z' WHERE pk='a' AND sk='b'"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"One or more parameter values were invalid: Cannot update attribute pk. This attribute is part of the key"}"#,
    ),
    (
        "GetItem",
        r#"{"TableName":"_capture_20261111_230_t","Key":{"pk":{"S":"a"},"sk":{"S":"b"}},"ConsistentRead":true}"#,
        r#"{"Item":{"s":{"S":"hello"},"bt":{"BOOL":true},"a b":{"N":"1"},"bf":{"BOOL":false},"a.b":{"N":"1"},"l":{"L":[{"N":"1"},{"N":"2"}]},"m":{"M":{"pk":{"N":"1"},"x":{"N":"1"}}},"pk":{"S":"a"},"n":{"N":"5"},"sk":{"S":"b"}}}"#,
    ),
    (
        "PutItem",
        r#"{"TableName":"_capture_20261111_230_t","Item":{"pk":{"S":"a"},"sk":{"S":"b"},"m":{"M":{"x":{"N":"1"},"pk":{"N":"1"}}},"l":{"L":[{"N":"1"},{"N":"2"}]},"n":{"N":"5"},"a.b":{"N":"1"},"a b":{"N":"1"},"s":{"S":"hello"},"bt":{"BOOL":true},"bf":{"BOOL":false}}}"#,
        r#"{}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_t\" REMOVE pk WHERE pk='a' AND sk='b'"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"One or more parameter values were invalid: Cannot update attribute pk. This attribute is part of the key"}"#,
    ),
    (
        "GetItem",
        r#"{"TableName":"_capture_20261111_230_t","Key":{"pk":{"S":"a"},"sk":{"S":"b"}},"ConsistentRead":true}"#,
        r#"{"Item":{"s":{"S":"hello"},"bt":{"BOOL":true},"a b":{"N":"1"},"bf":{"BOOL":false},"a.b":{"N":"1"},"l":{"L":[{"N":"1"},{"N":"2"}]},"m":{"M":{"pk":{"N":"1"},"x":{"N":"1"}}},"pk":{"S":"a"},"n":{"N":"5"},"sk":{"S":"b"}}}"#,
    ),
    (
        "PutItem",
        r#"{"TableName":"_capture_20261111_230_t","Item":{"pk":{"S":"a"},"sk":{"S":"b"},"m":{"M":{"x":{"N":"1"},"pk":{"N":"1"}}},"l":{"L":[{"N":"1"},{"N":"2"}]},"n":{"N":"5"},"a.b":{"N":"1"},"a b":{"N":"1"},"s":{"S":"hello"},"bt":{"BOOL":true},"bf":{"BOOL":false}}}"#,
        r#"{}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_t\" SET sk = 'z' WHERE pk='a' AND sk='b'"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"One or more parameter values were invalid: Cannot update attribute sk. This attribute is part of the key"}"#,
    ),
    (
        "GetItem",
        r#"{"TableName":"_capture_20261111_230_t","Key":{"pk":{"S":"a"},"sk":{"S":"b"}},"ConsistentRead":true}"#,
        r#"{"Item":{"s":{"S":"hello"},"bt":{"BOOL":true},"a b":{"N":"1"},"bf":{"BOOL":false},"a.b":{"N":"1"},"l":{"L":[{"N":"1"},{"N":"2"}]},"m":{"M":{"pk":{"N":"1"},"x":{"N":"1"}}},"pk":{"S":"a"},"n":{"N":"5"},"sk":{"S":"b"}}}"#,
    ),
    (
        "PutItem",
        r#"{"TableName":"_capture_20261111_230_t","Item":{"pk":{"S":"a"},"sk":{"S":"b"},"m":{"M":{"x":{"N":"1"},"pk":{"N":"1"}}},"l":{"L":[{"N":"1"},{"N":"2"}]},"n":{"N":"5"},"a.b":{"N":"1"},"a b":{"N":"1"},"s":{"S":"hello"},"bt":{"BOOL":true},"bf":{"BOOL":false}}}"#,
        r#"{}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_t\" REMOVE sk WHERE pk='a' AND sk='b'"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"One or more parameter values were invalid: Cannot update attribute sk. This attribute is part of the key"}"#,
    ),
    (
        "GetItem",
        r#"{"TableName":"_capture_20261111_230_t","Key":{"pk":{"S":"a"},"sk":{"S":"b"}},"ConsistentRead":true}"#,
        r#"{"Item":{"s":{"S":"hello"},"bt":{"BOOL":true},"a b":{"N":"1"},"bf":{"BOOL":false},"a.b":{"N":"1"},"l":{"L":[{"N":"1"},{"N":"2"}]},"m":{"M":{"pk":{"N":"1"},"x":{"N":"1"}}},"pk":{"S":"a"},"n":{"N":"5"},"sk":{"S":"b"}}}"#,
    ),
    (
        "PutItem",
        r#"{"TableName":"_capture_20261111_230_t","Item":{"pk":{"S":"a"},"sk":{"S":"b"},"m":{"M":{"x":{"N":"1"},"pk":{"N":"1"}}},"l":{"L":[{"N":"1"},{"N":"2"}]},"n":{"N":"5"},"a.b":{"N":"1"},"a b":{"N":"1"},"s":{"S":"hello"},"bt":{"BOOL":true},"bf":{"BOOL":false}}}"#,
        r#"{}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_t\" SET m.pk = 2 WHERE pk='a' AND sk='b'"}"#,
        r#"{"Items":[]}"#,
    ),
    (
        "GetItem",
        r#"{"TableName":"_capture_20261111_230_t","Key":{"pk":{"S":"a"},"sk":{"S":"b"}},"ConsistentRead":true}"#,
        r#"{"Item":{"a b":{"N":"1"},"a.b":{"N":"1"},"bf":{"BOOL":false},"bt":{"BOOL":true},"l":{"L":[{"N":"1"},{"N":"2"}]},"m":{"M":{"pk":{"N":"2"},"x":{"N":"1"}}},"n":{"N":"5"},"pk":{"S":"a"},"s":{"S":"hello"},"sk":{"S":"b"}}}"#,
    ),
    (
        "PutItem",
        r#"{"TableName":"_capture_20261111_230_t","Item":{"pk":{"S":"a"},"sk":{"S":"b"},"m":{"M":{"x":{"N":"1"},"pk":{"N":"1"}}},"l":{"L":[{"N":"1"},{"N":"2"}]},"n":{"N":"5"},"a.b":{"N":"1"},"a b":{"N":"1"},"s":{"S":"hello"},"bt":{"BOOL":true},"bf":{"BOOL":false}}}"#,
        r#"{}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_t\" REMOVE m.pk WHERE pk='a' AND sk='b'"}"#,
        r#"{"Items":[]}"#,
    ),
    (
        "GetItem",
        r#"{"TableName":"_capture_20261111_230_t","Key":{"pk":{"S":"a"},"sk":{"S":"b"}},"ConsistentRead":true}"#,
        r#"{"Item":{"a b":{"N":"1"},"a.b":{"N":"1"},"bf":{"BOOL":false},"bt":{"BOOL":true},"l":{"L":[{"N":"1"},{"N":"2"}]},"m":{"M":{"x":{"N":"1"}}},"n":{"N":"5"},"pk":{"S":"a"},"s":{"S":"hello"},"sk":{"S":"b"}}}"#,
    ),
    (
        "PutItem",
        r#"{"TableName":"_capture_20261111_230_t","Item":{"pk":{"S":"a"},"sk":{"S":"b"},"m":{"M":{"x":{"N":"1"},"pk":{"N":"1"}}},"l":{"L":[{"N":"1"},{"N":"2"}]},"n":{"N":"5"},"a.b":{"N":"1"},"a b":{"N":"1"},"s":{"S":"hello"},"bt":{"BOOL":true},"bf":{"BOOL":false}}}"#,
        r#"{}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_t\" SET n = 1 SET pk.x = 1 WHERE pk='a' AND sk='b'"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"One or more parameter values were invalid: Cannot update attribute pk. This attribute is part of the key"}"#,
    ),
    (
        "GetItem",
        r#"{"TableName":"_capture_20261111_230_t","Key":{"pk":{"S":"a"},"sk":{"S":"b"}},"ConsistentRead":true}"#,
        r#"{"Item":{"a b":{"N":"1"},"a.b":{"N":"1"},"bf":{"BOOL":false},"bt":{"BOOL":true},"l":{"L":[{"N":"1"},{"N":"2"}]},"m":{"M":{"pk":{"N":"1"},"x":{"N":"1"}}},"n":{"N":"5"},"pk":{"S":"a"},"s":{"S":"hello"},"sk":{"S":"b"}}}"#,
    ),
    (
        "PutItem",
        r#"{"TableName":"_capture_20261111_230_t","Item":{"pk":{"S":"a"},"sk":{"S":"b"},"m":{"M":{"x":{"N":"1"},"pk":{"N":"1"}}},"l":{"L":[{"N":"1"},{"N":"2"}]},"n":{"N":"5"},"a.b":{"N":"1"},"a b":{"N":"1"},"s":{"S":"hello"},"bt":{"BOOL":true},"bf":{"BOOL":false}}}"#,
        r#"{}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_t\" SET pk.x = 1 SET n = 1 WHERE pk='a' AND sk='b'"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"One or more parameter values were invalid: Cannot update attribute pk. This attribute is part of the key"}"#,
    ),
    (
        "GetItem",
        r#"{"TableName":"_capture_20261111_230_t","Key":{"pk":{"S":"a"},"sk":{"S":"b"}},"ConsistentRead":true}"#,
        r#"{"Item":{"a b":{"N":"1"},"a.b":{"N":"1"},"bf":{"BOOL":false},"bt":{"BOOL":true},"l":{"L":[{"N":"1"},{"N":"2"}]},"m":{"M":{"pk":{"N":"1"},"x":{"N":"1"}}},"n":{"N":"5"},"pk":{"S":"a"},"s":{"S":"hello"},"sk":{"S":"b"}}}"#,
    ),
    (
        "PutItem",
        r#"{"TableName":"_capture_20261111_230_t","Item":{"pk":{"S":"a"},"sk":{"S":"b"},"m":{"M":{"x":{"N":"1"},"pk":{"N":"1"}}},"l":{"L":[{"N":"1"},{"N":"2"}]},"n":{"N":"5"},"a.b":{"N":"1"},"a b":{"N":"1"},"s":{"S":"hello"},"bt":{"BOOL":true},"bf":{"BOOL":false}}}"#,
        r#"{}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_t\" SET n = 1 REMOVE pk.x WHERE pk='a' AND sk='b'"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"One or more parameter values were invalid: Cannot update attribute pk. This attribute is part of the key"}"#,
    ),
    (
        "GetItem",
        r#"{"TableName":"_capture_20261111_230_t","Key":{"pk":{"S":"a"},"sk":{"S":"b"}},"ConsistentRead":true}"#,
        r#"{"Item":{"a b":{"N":"1"},"a.b":{"N":"1"},"bf":{"BOOL":false},"bt":{"BOOL":true},"l":{"L":[{"N":"1"},{"N":"2"}]},"m":{"M":{"pk":{"N":"1"},"x":{"N":"1"}}},"n":{"N":"5"},"pk":{"S":"a"},"s":{"S":"hello"},"sk":{"S":"b"}}}"#,
    ),
    (
        "PutItem",
        r#"{"TableName":"_capture_20261111_230_t","Item":{"pk":{"S":"a"},"sk":{"S":"b"},"m":{"M":{"x":{"N":"1"},"pk":{"N":"1"}}},"l":{"L":[{"N":"1"},{"N":"2"}]},"n":{"N":"5"},"a.b":{"N":"1"},"a b":{"N":"1"},"s":{"S":"hello"},"bt":{"BOOL":true},"bf":{"BOOL":false}}}"#,
        r#"{}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_t\" SET pk.x.y = 1 WHERE pk='a' AND sk='b'"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"One or more parameter values were invalid: Cannot update attribute pk. This attribute is part of the key"}"#,
    ),
    (
        "GetItem",
        r#"{"TableName":"_capture_20261111_230_t","Key":{"pk":{"S":"a"},"sk":{"S":"b"}},"ConsistentRead":true}"#,
        r#"{"Item":{"a b":{"N":"1"},"a.b":{"N":"1"},"bf":{"BOOL":false},"bt":{"BOOL":true},"l":{"L":[{"N":"1"},{"N":"2"}]},"m":{"M":{"pk":{"N":"1"},"x":{"N":"1"}}},"n":{"N":"5"},"pk":{"S":"a"},"s":{"S":"hello"},"sk":{"S":"b"}}}"#,
    ),
    (
        "PutItem",
        r#"{"TableName":"_capture_20261111_230_t","Item":{"pk":{"S":"a"},"sk":{"S":"b"},"m":{"M":{"x":{"N":"1"},"pk":{"N":"1"}}},"l":{"L":[{"N":"1"},{"N":"2"}]},"n":{"N":"5"},"a.b":{"N":"1"},"a b":{"N":"1"},"s":{"S":"hello"},"bt":{"BOOL":true},"bf":{"BOOL":false}}}"#,
        r#"{}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_t\" SET nope.x = 1 SET pk.x = 1 WHERE pk='a' AND sk='b'"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"One or more parameter values were invalid: Cannot update attribute pk. This attribute is part of the key"}"#,
    ),
    (
        "GetItem",
        r#"{"TableName":"_capture_20261111_230_t","Key":{"pk":{"S":"a"},"sk":{"S":"b"}},"ConsistentRead":true}"#,
        r#"{"Item":{"a b":{"N":"1"},"a.b":{"N":"1"},"bf":{"BOOL":false},"bt":{"BOOL":true},"l":{"L":[{"N":"1"},{"N":"2"}]},"m":{"M":{"pk":{"N":"1"},"x":{"N":"1"}}},"n":{"N":"5"},"pk":{"S":"a"},"s":{"S":"hello"},"sk":{"S":"b"}}}"#,
    ),
    (
        "PutItem",
        r#"{"TableName":"_capture_20261111_230_t","Item":{"pk":{"S":"a"},"sk":{"S":"b"},"m":{"M":{"x":{"N":"1"},"pk":{"N":"1"}}},"l":{"L":[{"N":"1"},{"N":"2"}]},"n":{"N":"5"},"a.b":{"N":"1"},"a b":{"N":"1"},"s":{"S":"hello"},"bt":{"BOOL":true},"bf":{"BOOL":false}}}"#,
        r#"{}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_t\" SET pk.x = nope WHERE pk='a' AND sk='b'"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"One or more parameter values were invalid: Cannot update attribute pk. This attribute is part of the key"}"#,
    ),
    (
        "GetItem",
        r#"{"TableName":"_capture_20261111_230_t","Key":{"pk":{"S":"a"},"sk":{"S":"b"}},"ConsistentRead":true}"#,
        r#"{"Item":{"a b":{"N":"1"},"a.b":{"N":"1"},"bf":{"BOOL":false},"bt":{"BOOL":true},"l":{"L":[{"N":"1"},{"N":"2"}]},"m":{"M":{"pk":{"N":"1"},"x":{"N":"1"}}},"n":{"N":"5"},"pk":{"S":"a"},"s":{"S":"hello"},"sk":{"S":"b"}}}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_t\" SET pk.x = 1 WHERE pk='zz' AND sk='zz'"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"One or more parameter values were invalid: Cannot update attribute pk. This attribute is part of the key"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_t\" REMOVE pk.x WHERE pk='zz' AND sk='zz'"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"One or more parameter values were invalid: Cannot update attribute pk. This attribute is part of the key"}"#,
    ),
];

/// Round three, the rows naming both keys and the refusals that come first.
const KEY_PATHS_BOTH_KEYS: &[Row] = &[
    (
        "PutItem",
        r#"{"TableName":"_capture_20261111_230_t","Item":{"pk":{"S":"a"},"sk":{"S":"b"},"m":{"M":{"x":{"N":"1"},"pk":{"N":"1"}}},"l":{"L":[{"N":"1"},{"BOOL":true}]},"n":{"N":"5"},"s":{"S":"hello"},"bt":{"BOOL":true},"bf":{"BOOL":false}}}"#,
        r#"{}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_t\" SET pk.x = 1 SET sk.x = 1 WHERE pk='zz' AND sk='zz'"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"One or more parameter values were invalid: Cannot update attribute sk. This attribute is part of the key"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_t\" SET sk.x = 1 SET pk.x = 1 WHERE pk='zz' AND sk='zz'"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"One or more parameter values were invalid: Cannot update attribute sk. This attribute is part of the key"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_t\" REMOVE sk.x SET pk.x = 1 WHERE pk='zz' AND sk='zz'"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"One or more parameter values were invalid: Cannot update attribute sk. This attribute is part of the key"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_t\" SET sk = 'z' SET pk = 'z' WHERE pk='zz' AND sk='zz'"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"One or more parameter values were invalid: Cannot update attribute sk. This attribute is part of the key"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_t\" REMOVE sk REMOVE pk WHERE pk='zz' AND sk='zz'"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"One or more parameter values were invalid: Cannot update attribute sk. This attribute is part of the key"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_t\" SET pk = 'z' WHERE pk='zz' AND sk='zz'"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"One or more parameter values were invalid: Cannot update attribute pk. This attribute is part of the key"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_t\" REMOVE sk WHERE pk='zz' AND sk='zz'"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"One or more parameter values were invalid: Cannot update attribute sk. This attribute is part of the key"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_t\" SET pk.x = 1 WHERE pk='a'"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Where clause does not contain a mandatory equality on all key attributes"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_t\" SET pk.x = 1"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Where clause does not contain a mandatory equality on all key attributes"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_t\" SET pk.x = ? WHERE pk='a' AND sk='b'"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Number of parameters in request and statement don't match."}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements": [{"Statement": "UPDATE \"_capture_20261111_230_t\" SET pk.x = 1 WHERE pk='a' AND sk='b'"}]}"#,
        r#"{"Responses":[{"Error":{"Code":"ValidationError","Message":"One or more parameter values were invalid: Cannot update attribute pk. This attribute is part of the key"},"TableName":"_capture_20261111_230_t"}]}"#,
    ),
    (
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "UPDATE \"_capture_20261111_230_t\" SET pk.x = 1 WHERE pk='a' AND sk='b'"}]}"#,
        r#"{"__type":"com.amazonaws.dynamodb.v20120810#TransactionCanceledException","CancellationReasons":[{"Code":"ValidationError","Message":"One or more parameter values were invalid: Cannot update attribute pk. This attribute is part of the key"}],"Message":"Transaction cancelled, please refer cancellation reasons for specific reasons [ValidationError]"}"#,
    ),
];

/// `ConsistentRead` on ExecuteStatement and on a BatchExecuteStatement member
/// is read as GetItem reads it: the strings DynamoDB takes for true and false
/// read, `null` is absent, and anything else is refused in DynamoDB's words
/// for what was found. A list is refused with no message, where GetItem names
/// the type. A string that reads is no fault, so a `Limit` of 0 beside it is
/// what's reported, and a write statement takes it too.
#[tokio::test]
async fn consistent_read_given_as_a_string_is_read_as_get_item_reads_it() {
    replay(CONSISTENT_READ).await;
    replay(CONSISTENT_READ_MORE).await;
}

/// Round one, the `ConsistentRead` rows, after the item they read is put.
const CONSISTENT_READ: &[Row] = &[
    (
        "PutItem",
        r#"{"TableName":"_capture_20261111_230_t","Item":{"pk":{"S":"a"},"sk":{"S":"b"},"m":{"M":{"x":{"N":"1"},"pk":{"N":"1"}}},"l":{"L":[{"N":"1"},{"N":"2"}]},"n":{"N":"5"},"a.b":{"N":"1"},"a b":{"N":"1"},"s":{"S":"hello"},"bt":{"BOOL":true},"bf":{"BOOL":false}}}"#,
        r#"{}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement":"SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'","ConsistentRead":"true"}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements":[{"Statement":"SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'","ConsistentRead":"true"}]}"#,
        r#"{"Responses":[{"Item":{"pk":{"S":"a"}},"TableName":"_capture_20261111_230_t"}]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement":"SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'","ConsistentRead":"yes"}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements":[{"Statement":"SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'","ConsistentRead":"yes"}]}"#,
        r#"{"Responses":[{"Item":{"pk":{"S":"a"}},"TableName":"_capture_20261111_230_t"}]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement":"SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'","ConsistentRead":"false"}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements":[{"Statement":"SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'","ConsistentRead":"false"}]}"#,
        r#"{"Responses":[{"Item":{"pk":{"S":"a"}},"TableName":"_capture_20261111_230_t"}]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement":"SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'","ConsistentRead":"no"}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements":[{"Statement":"SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'","ConsistentRead":"no"}]}"#,
        r#"{"Responses":[{"Item":{"pk":{"S":"a"}},"TableName":"_capture_20261111_230_t"}]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement":"SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'","ConsistentRead":"1"}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements":[{"Statement":"SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'","ConsistentRead":"1"}]}"#,
        r#"{"Responses":[{"Item":{"pk":{"S":"a"}},"TableName":"_capture_20261111_230_t"}]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement":"SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'","ConsistentRead":"0"}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements":[{"Statement":"SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'","ConsistentRead":"0"}]}"#,
        r#"{"Responses":[{"Item":{"pk":{"S":"a"}},"TableName":"_capture_20261111_230_t"}]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement":"SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'","ConsistentRead":"t"}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements":[{"Statement":"SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'","ConsistentRead":"t"}]}"#,
        r#"{"Responses":[{"Item":{"pk":{"S":"a"}},"TableName":"_capture_20261111_230_t"}]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement":"SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'","ConsistentRead":"TRUE"}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements":[{"Statement":"SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'","ConsistentRead":"TRUE"}]}"#,
        r#"{"Responses":[{"Item":{"pk":{"S":"a"}},"TableName":"_capture_20261111_230_t"}]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement":"SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'","ConsistentRead":"x"}"#,
        r#"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected token received from parser"}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements":[{"Statement":"SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'","ConsistentRead":"x"}]}"#,
        r#"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected token received from parser"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement":"SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'","ConsistentRead":""}"#,
        r#"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected token received from parser"}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements":[{"Statement":"SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'","ConsistentRead":""}]}"#,
        r#"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected token received from parser"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement":"SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'","ConsistentRead":1}"#,
        r#"{"__type":"com.amazon.coral.service#SerializationException","Message":"NUMBER_VALUE cannot be converted to Boolean"}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements":[{"Statement":"SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'","ConsistentRead":1}]}"#,
        r#"{"__type":"com.amazon.coral.service#SerializationException","Message":"NUMBER_VALUE cannot be converted to Boolean"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement":"SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'","ConsistentRead":0}"#,
        r#"{"__type":"com.amazon.coral.service#SerializationException","Message":"NUMBER_VALUE cannot be converted to Boolean"}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements":[{"Statement":"SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'","ConsistentRead":0}]}"#,
        r#"{"__type":"com.amazon.coral.service#SerializationException","Message":"NUMBER_VALUE cannot be converted to Boolean"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement":"SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'","ConsistentRead":1.5}"#,
        r#"{"__type":"com.amazon.coral.service#SerializationException","Message":"DECIMAL_VALUE cannot be converted to Boolean"}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements":[{"Statement":"SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'","ConsistentRead":1.5}]}"#,
        r#"{"__type":"com.amazon.coral.service#SerializationException","Message":"DECIMAL_VALUE cannot be converted to Boolean"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement":"SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'","ConsistentRead":null}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements":[{"Statement":"SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'","ConsistentRead":null}]}"#,
        r#"{"Responses":[{"Item":{"pk":{"S":"a"}},"TableName":"_capture_20261111_230_t"}]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement":"SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'","ConsistentRead":[]}"#,
        r#"{"__type":"com.amazon.coral.service#SerializationException"}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements":[{"Statement":"SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'","ConsistentRead":[]}]}"#,
        r#"{"__type":"com.amazon.coral.service#SerializationException"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement":"SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'","ConsistentRead":{}}"#,
        r#"{"__type":"com.amazon.coral.service#SerializationException","Message":"Start of structure or map found where not expected"}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements":[{"Statement":"SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'","ConsistentRead":{}}]}"#,
        r#"{"__type":"com.amazon.coral.service#SerializationException","Message":"Start of structure or map found where not expected"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement":"SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'","ConsistentRead":true}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements":[{"Statement":"SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'","ConsistentRead":true}]}"#,
        r#"{"Responses":[{"Item":{"pk":{"S":"a"}},"TableName":"_capture_20261111_230_t"}]}"#,
    ),
];

/// Round two, the `ConsistentRead` rows, after the item they read is put.
const CONSISTENT_READ_MORE: &[Row] = &[
    (
        "PutItem",
        r#"{"TableName":"_capture_20261111_230_t","Item":{"pk":{"S":"a"},"sk":{"S":"b"},"m":{"M":{"x":{"N":"1"},"pk":{"N":"1"}}},"l":{"L":[{"N":"1"},{"N":"2"}]},"n":{"N":"5"},"a.b":{"N":"1"},"a b":{"N":"1"},"s":{"S":"hello"},"bt":{"BOOL":true},"bf":{"BOOL":false}}}"#,
        r#"{}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement":"SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'","ConsistentRead":"y"}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement":"SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'","ConsistentRead":"n"}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement":"SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'","ConsistentRead":"f"}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement":"SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'","ConsistentRead":"Yes"}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement":"SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'","ConsistentRead":"on"}"#,
        r#"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected token received from parser"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement":"SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'","ConsistentRead":" true"}"#,
        r#"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected token received from parser"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement":"SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'","ConsistentRead":true}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement":"SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'","ConsistentRead":false}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement":"SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'","ConsistentRead":"x","Limit":0}"#,
        r#"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected token received from parser"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement":"SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'","ConsistentRead":"true","Limit":0}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value '0' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1"}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements":[{"Statement":"SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'","ConsistentRead":"true"},{"Statement":"SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'","ConsistentRead":"x"}]}"#,
        r#"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected token received from parser"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement":"UPDATE \"_capture_20261111_230_t\" SET z = 2 WHERE pk='a' AND sk='b'","ConsistentRead":"true"}"#,
        r#"{"Items":[]}"#,
    ),
];

/// An ORDER BY term that isn't a key is refused naming the attribute, so a
/// quoted name is printed without its quotes.
#[tokio::test]
async fn the_order_by_refusal_names_a_quoted_attribute_without_its_quotes() {
    replay(ORDER_BY).await;
    replay(ORDER_BY_MORE).await;
}

/// Round one, the ORDER BY rows, after the item they read is put.
const ORDER_BY: &[Row] = &[
    (
        "PutItem",
        r#"{"TableName":"_capture_20261111_230_t","Item":{"pk":{"S":"a"},"sk":{"S":"b"},"m":{"M":{"x":{"N":"1"},"pk":{"N":"1"}}},"l":{"L":[{"N":"1"},{"N":"2"}]},"n":{"N":"5"},"a.b":{"N":"1"},"a b":{"N":"1"},"s":{"S":"hello"},"bt":{"BOOL":true},"bf":{"BOOL":false}}}"#,
        r#"{}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' ORDER BY \"a.b\""}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Attribute a.b in ORDER BY clause must be part of the primary key"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' ORDER BY a.b"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Argument to ORDER BY must be of type VariableReference. Type: PATH at 1:64:1"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' ORDER BY \"n\""}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Attribute n in ORDER BY clause must be part of the primary key"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' ORDER BY n"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Attribute n in ORDER BY clause must be part of the primary key"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' ORDER BY m.x"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Argument to ORDER BY must be of type VariableReference. Type: PATH at 1:64:1"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' ORDER BY \"m\".\"x\""}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Argument to ORDER BY must be of type VariableReference. Type: PATH at 1:64:3"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' ORDER BY l[0]"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Argument to ORDER BY must be of type VariableReference. Type: PATH at 1:64:1"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' ORDER BY \"a b\""}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Attribute a b in ORDER BY clause must be part of the primary key"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' ORDER BY \"pk\""}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' ORDER BY \"sk\" DESC"}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' ORDER BY sk"}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' ORDER BY \"m\".x"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Argument to ORDER BY must be of type VariableReference. Type: PATH at 1:64:3"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' ORDER BY m.\"x\""}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Argument to ORDER BY must be of type VariableReference. Type: PATH at 1:64:1"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' ORDER BY \"a.b\".c"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Argument to ORDER BY must be of type VariableReference. Type: PATH at 1:64:5"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' ORDER BY nope"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Attribute nope in ORDER BY clause must be part of the primary key"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' ORDER BY \"no pe\""}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Attribute no pe in ORDER BY clause must be part of the primary key"}"#,
    ),
];

/// Round two, the ORDER BY rows.
const ORDER_BY_MORE: &[Row] = &[
    (
        "PutItem",
        r#"{"TableName":"_capture_20261111_230_t","Item":{"pk":{"S":"a"},"sk":{"S":"b"},"m":{"M":{"x":{"N":"1"},"pk":{"N":"1"}}},"l":{"L":[{"N":"1"},{"N":"2"}]},"n":{"N":"5"},"a.b":{"N":"1"},"a b":{"N":"1"},"s":{"S":"hello"},"bt":{"BOOL":true},"bf":{"BOOL":false}}}"#,
        r#"{}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' ORDER BY \"a.b\", sk"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Attribute a.b in ORDER BY clause must be part of the primary key"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' ORDER BY sk, \"a.b\""}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Attribute a.b in ORDER BY clause must be part of the primary key"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' ORDER BY \"a.b\" DESC"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Attribute a.b in ORDER BY clause must be part of the primary key"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' ORDER BY \"PK\""}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Attribute PK in ORDER BY clause must be part of the primary key"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' ORDER BY pk.x"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Argument to ORDER BY must be of type VariableReference. Type: PATH at 1:64:2"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' ORDER BY \"n\" ASC, \"s\""}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Attribute n in ORDER BY clause must be part of the primary key"}"#,
    ),
];

/// A call to `attribute_exists`, `attribute_not_exists` or `attribute_type`
/// is read wherever an operand is, and is a boolean there whatever it would
/// return: a BETWEEN bound or subject is refused as `true` is, and
/// `attribute_type(attribute_exists(n), 'BOOL')` holds.
#[tokio::test]
async fn a_condition_function_call_is_a_boolean_operand() {
    replay(CALLS).await;
}

/// A call inside `size`, `attribute_type`, `attribute_exists` or a BETWEEN
/// answers as a boolean literal or parameter written there does, the missing
/// item, UPDATE and DELETE included. A refusal from inside a call comes ahead
/// of the operator it stands under, a wrong number of operands among them.
#[tokio::test]
async fn a_call_answers_as_a_boolean_written_in_its_place() {
    replay(CALLS_MORE).await;
    replay(CALLS_INFERRED).await;
}

/// Inferred from the captured rule that a refusal from inside a call comes
/// ahead of the operator it stands under: a call's wrong number of operands
/// in the bounds of a call's BETWEEN.
const CALLS_INFERRED: &[Row] = &[(
    "ExecuteStatement",
    r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_exists(n) BETWEEN attribute_exists(n, n) AND 5"}"#,
    r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect number of operands for operator or function; operator or function: attribute_exists, number of operands: 2"}"#,
)];

/// `size` refuses a number, a boolean or a null, as a literal, a parameter,
/// a call or `size` itself, whatever the item holds, and the first refusal
/// written is the one reported. It comes ahead of an UPDATE's key refusal.
#[tokio::test]
async fn size_refuses_a_number_a_boolean_or_a_null() {
    replay(CALLS_TYPES).await;
}

/// The captured rows left out of the replays above. `begins_with` and
/// `contains` take only a path and a plain value here, so these answer
/// differently: on DynamoDB either can stand as an operand, take a call as an
/// argument or a first operand that isn't a path, and `begins_with` refuses a
/// first operand by its type. That is one difference with one cause, larger
/// than these, and is left for its own fix.
#[test]
fn only_begins_with_and_contains_rows_are_left_out() {
    for (_, body, _) in LEFT_OUT {
        assert!(
            body.contains("begins_with(") || body.contains("contains("),
            "{body}"
        );
    }
    assert_eq!(LEFT_OUT.len(), 35);
}

/// Round one, the condition rows, after the item they read is put.
const CALLS: &[Row] = &[
    (
        "PutItem",
        r#"{"TableName":"_capture_20261111_230_t","Item":{"pk":{"S":"a"},"sk":{"S":"b"},"m":{"M":{"x":{"N":"1"},"pk":{"N":"1"}}},"l":{"L":[{"N":"1"},{"N":"2"}]},"n":{"N":"5"},"a.b":{"N":"1"},"a b":{"N":"1"},"s":{"S":"hello"},"bt":{"BOOL":true},"bf":{"BOOL":false}}}"#,
        r#"{}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n BETWEEN attribute_exists(n) AND 5"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: BETWEEN, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n BETWEEN 1 AND attribute_exists(n)"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: BETWEEN, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND bt BETWEEN attribute_exists(n) AND attribute_exists(n)"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: BETWEEN, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND bt BETWEEN attribute_not_exists(n) AND attribute_exists(n)"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: BETWEEN, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n BETWEEN attribute_type(n,'N') AND 9"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: BETWEEN, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n BETWEEN size(s) AND 9"}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n BETWEEN 1 AND size(s)"}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(attribute_exists(n)) > 0"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(size(s)) > 0"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: N"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_type(attribute_exists(n),'BOOL')"}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_type(n, attribute_exists(n))"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: attribute_type, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_type(size(s),'N')"}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_exists(attribute_exists(n))"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Operator or function requires a document path; operator or function: attribute_exists"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND bt = attribute_exists(n)"}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND bt IN [attribute_exists(n)]"}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_exists(n) BETWEEN bf AND bt"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: BETWEEN, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_exists(n) BETWEEN attribute_exists(n) AND attribute_exists(n)"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: BETWEEN, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND bt BETWEEN bf AND bt"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"The first operand must be distinct from the remaining operands for this operator or function; operator: BETWEEN, first operand: [bt]"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND 5 BETWEEN attribute_exists(n) AND 9"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: BETWEEN, operand type: BOOL"}"#,
    ),
];

/// Round two, every row before the `ConsistentRead` rows.
const CALLS_MORE: &[Row] = &[
    (
        "PutItem",
        r#"{"TableName":"_capture_20261111_230_t","Item":{"pk":{"S":"a"},"sk":{"S":"b"},"m":{"M":{"x":{"N":"1"},"pk":{"N":"1"}}},"l":{"L":[{"N":"1"},{"N":"2"}]},"n":{"N":"5"},"a.b":{"N":"1"},"a b":{"N":"1"},"s":{"S":"hello"},"bt":{"BOOL":true},"bf":{"BOOL":false}}}"#,
        r#"{}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n BETWEEN true AND 5"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: BETWEEN, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n BETWEEN 1 AND true"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: BETWEEN, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND bt BETWEEN true AND true"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: BETWEEN, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND 5 BETWEEN true AND 9"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: BETWEEN, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n BETWEEN bt AND 9"}"#,
        r#"{"Items":[]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n BETWEEN 1 AND bt"}"#,
        r#"{"Items":[]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(true) > 0"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(bt) > 0"}"#,
        r#"{"Items":[]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(5) > 0"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: N"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(n) > 0"}"#,
        r#"{"Items":[]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_type(true,'BOOL')"}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_type(bt,'BOOL')"}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_type(n, true)"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: attribute_type, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_type(5,'N')"}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND contains(s, true)"}"#,
        r#"{"Items":[]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND contains(true, 'x')"}"#,
        r#"{"Items":[]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND contains(s, bt)"}"#,
        r#"{"Items":[]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND begins_with(s, true)"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: begins_with, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND begins_with(bt,'x')"}"#,
        r#"{"Items":[]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND begins_with(s, bt)"}"#,
        r#"{"Items":[]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_exists(true)"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Operator or function requires a document path; operator or function: attribute_exists"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_exists(size(s))"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Operator or function requires a document path; operator or function: attribute_exists"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_not_exists(attribute_exists(n))"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Operator or function requires a document path; operator or function: attribute_not_exists"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(attribute_type(n,'N')) > 0"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(attribute_not_exists(n)) > 0"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_type(attribute_not_exists(n),'BOOL')"}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_type(attribute_exists(n),'S')"}"#,
        r#"{"Items":[]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_type(attribute_exists(nope),'BOOL')"}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n BETWEEN attribute_exists(nope) AND 5"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: BETWEEN, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n BETWEEN size(attribute_exists(n)) AND 5"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(attribute_exists(n), 1) > 0"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect number of operands for operator or function; operator or function: size, number of operands: 2"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n BETWEEN attribute_exists(n, n) AND 5"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect number of operands for operator or function; operator or function: attribute_exists, number of operands: 2"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND nope BETWEEN attribute_exists(n) AND 5"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: BETWEEN, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n BETWEEN attribute_exists(n) AND nope"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: BETWEEN, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_exists(n) = true"}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_exists(n) < true"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: <, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n < attribute_exists(n)"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: <, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n IN [1, attribute_exists(n)]"}"#,
        r#"{"Items":[]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND NOT n BETWEEN attribute_exists(n) AND 5"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: BETWEEN, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n BETWEEN ? AND 5", "Parameters": [{"BOOL": true}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: BETWEEN, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(?) > 0", "Parameters": [{"BOOL": true}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND begins_with(s, ?)", "Parameters": [{"BOOL": true}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: begins_with, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND contains(s, ?)", "Parameters": [{"BOOL": true}]}"#,
        r#"{"Items":[]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='zz' AND sk='zz' AND n BETWEEN attribute_exists(n) AND 5"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: BETWEEN, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='zz' AND sk='zz' AND size(attribute_exists(n)) > 0"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_t\" SET z = 1 WHERE pk='a' AND sk='b' AND n BETWEEN attribute_exists(n) AND 5"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: BETWEEN, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_t\" SET z = 1 WHERE pk='a' AND sk='b' AND attribute_type(attribute_exists(n),'BOOL')"}"#,
        r#"{"Items":[]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "DELETE FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(attribute_exists(n)) > 0"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "GetItem",
        r#"{"TableName":"_capture_20261111_230_t","Key":{"pk":{"S":"a"},"sk":{"S":"b"}},"ConsistentRead":true,"ProjectionExpression":"z"}"#,
        r#"{"Item":{"z":{"N":"1"}}}"#,
    ),
];

/// Round three, every row before the key-update rows, and the key update
/// whose WHERE clause holds `size(true)`.
const CALLS_TYPES: &[Row] = &[
    (
        "PutItem",
        r#"{"TableName":"_capture_20261111_230_t","Item":{"pk":{"S":"a"},"sk":{"S":"b"},"m":{"M":{"x":{"N":"1"},"pk":{"N":"1"}}},"l":{"L":[{"N":"1"},{"BOOL":true}]},"n":{"N":"5"},"s":{"S":"hello"},"bt":{"BOOL":true},"bf":{"BOOL":false}}}"#,
        r#"{}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size('abc') > 0"}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size('abc') = 3"}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(null) > 0"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: NULL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(MISSING) > 0"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"MISSING must be compared using IS operator"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size([1,2]) > 0"}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size({'a':1}) > 0"}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(<<1,2>>) > 0"}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND contains(5,'x')"}"#,
        r#"{"Items":[]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND contains(l, true)"}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_type('abc','S')"}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_type(size(s),'S')"}"#,
        r#"{"Items":[]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_type(n, size(s))"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: attribute_type, operand type: N"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_type(n, 5)"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: attribute_type, operand type: N"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(s) BETWEEN 1 AND 9"}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(attribute_exists(n)) BETWEEN 1 AND 9"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_exists(n) IN [true]"}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(s) IN [5]"}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n IN [size(s)]"}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n IN [size(attribute_exists(n))]"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_exists(n) AND size(attribute_exists(n)) > 0"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(attribute_exists(n)) > 0 OR attribute_exists(n)"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(true) > 0 AND begins_with(true,'x')"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(?) > 0", "Parameters": [{"S": "abc"}]}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(?) > 0", "Parameters": [{"N": "5"}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: N"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(?) > 0", "Parameters": [{"B": "QUJD"}]}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND begins_with(?, 'a')", "Parameters": [{"B": "QUJD"}]}"#,
        r#"{"Items":[]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(?) > 0", "Parameters": [{"L": [{"N": "1"}]}]}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(?) > 0", "Parameters": [{"M": {"a": {"N": "1"}}}]}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(?) > 0", "Parameters": [{"SS": ["a"]}]}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(?) > 0", "Parameters": [{"NULL": true}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: NULL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(?) > 0", "Parameters": [{"BOOL": false}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_t\" SET pk.x = 1 WHERE pk='a' AND sk='b' AND size(true) > 0"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
];

/// Rows left out of the replays, by the rule on `only_begins_with_and_contains_rows_are_left_out`.
const LEFT_OUT: &[Row] = &[
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n BETWEEN begins_with(s,'h') AND 9"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: BETWEEN, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n BETWEEN contains(s,'h') AND 9"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: BETWEEN, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(begins_with(s,'h')) > 0"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND contains(s, attribute_exists(n))"}"#,
        r#"{"Items":[]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND contains(attribute_exists(n), 'x')"}"#,
        r#"{"Items":[]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND begins_with(attribute_exists(n), 'x')"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: begins_with, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND begins_with(s, attribute_exists(n))"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: begins_with, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND begins_with(true, 'x')"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: begins_with, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(contains(s,'h')) > 0"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_type(begins_with(s,'h'),'BOOL')"}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND contains(attribute_exists(n), true)"}"#,
        r#"{"Items":[]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND contains(l, attribute_exists(n))"}"#,
        r#"{"Items":[]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND contains(s, size(s))"}"#,
        r#"{"Items":[]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n BETWEEN attribute_exists(n) AND begins_with(s,1)"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: begins_with, operand type: N"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n BETWEEN begins_with(s,1) AND 5"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: begins_with, operand type: N"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(begins_with(s,1)) > 0"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: begins_with, operand type: N"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='zz' AND sk='zz' AND begins_with(s, attribute_exists(n))"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: begins_with, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND begins_with('abc','a')"}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND begins_with(5,'a')"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: begins_with, operand type: N"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND begins_with(null,'a')"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: begins_with, operand type: NULL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND begins_with([1],'a')"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: begins_with, operand type: L"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND contains('abc','a')"}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND contains([1,true], attribute_exists(n))"}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND contains(l, attribute_exists(n))"}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND begins_with(true,'x') AND size(true) > 0"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: begins_with, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n BETWEEN true AND begins_with(s,1)"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: begins_with, operand type: N"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n BETWEEN begins_with(s,1) AND size(true)"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: begins_with, operand type: N"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n BETWEEN size(true) AND begins_with(s,1)"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND begins_with(?, 'a')", "Parameters": [{"S": "abc"}]}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND begins_with(?, 'a')", "Parameters": [{"N": "5"}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: begins_with, operand type: N"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND begins_with(?, 'a')", "Parameters": [{"L": [{"N": "1"}]}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: begins_with, operand type: L"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND begins_with(?, 'a')", "Parameters": [{"M": {"a": {"N": "1"}}}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: begins_with, operand type: M"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND begins_with(?, 'a')", "Parameters": [{"SS": ["a"]}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: begins_with, operand type: SS"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND begins_with(?, 'a')", "Parameters": [{"NULL": true}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: begins_with, operand type: NULL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND begins_with(?, 'a')", "Parameters": [{"BOOL": false}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: begins_with, operand type: BOOL"}"#,
    ),
];
