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
