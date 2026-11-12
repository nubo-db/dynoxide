//! Small differences between a PartiQL statement here and on DynamoDB: an
//! UPDATE of a key attribute through a path, `ConsistentRead` written as a
//! string, the ORDER BY refusal for a quoted name, a condition function used
//! as an operand, and the order of the statement's own checks.
//!
//! Each `const` holds requests sent to DynamoDB on 11 November 2026, in the
//! order they were sent, with the answer DynamoDB gave each one, against an
//! on-demand table keyed `pk` (S) and `sk` (S), the indexed table rounds
//! nine to eleven name, or a table that doesn't exist. They are data captured
//! from DynamoDB, not written by hand.
//! Rounds one to four were captured in eu-west-2 and us-east-1, which gave
//! the same answers; rounds five to thirteen in eu-west-2. A test replays
//! its rows in order against a new server and compares each answer with
//! DynamoDB's as JSON, so key order makes no difference, along with the HTTP
//! status: 400 for an error and 200 otherwise.

#![cfg(feature = "http-server")]

use dynoxide::Database;
use dynoxide::actions::create_table::CreateTableRequest;

/// An operation, its request body and DynamoDB's answer.
type Row = (&'static str, &'static str, &'static str);

/// A row left out of the replays: an operation, its request body, DynamoDB's
/// answer and the answer it gets here today.
type LeftOut = (&'static str, &'static str, &'static str, &'static str);

/// The name of the table rounds one to eight run against.
const TABLE_NAME: &str = "_capture_20261111_230_t";

/// The table rounds one to eight run against.
const TABLE: &str = r#"{"TableName": "_capture_20261111_230_t", "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}], "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}, {"AttributeName": "sk", "AttributeType": "S"}], "BillingMode": "PAY_PER_REQUEST"}"#;

/// The table round nine runs against, as it was created on DynamoDB: a key of
/// `id` and `sk`, an index `single` and an index `hh_rr` keyed on two
/// attributes each side.
const INDEXED_TABLE: &str = r#"{"TableName": "_capture_20261111_230_m", "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}, {"AttributeName": "sk", "AttributeType": "S"}, {"AttributeName": "g", "AttributeType": "S"}, {"AttributeName": "h1", "AttributeType": "S"}, {"AttributeName": "h2", "AttributeType": "S"}, {"AttributeName": "r1", "AttributeType": "S"}, {"AttributeName": "r2", "AttributeType": "S"}], "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}], "BillingMode": "PAY_PER_REQUEST", "GlobalSecondaryIndexes": [{"IndexName": "single", "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}, {"AttributeName": "r1", "KeyType": "RANGE"}], "Projection": {"ProjectionType": "ALL"}}, {"IndexName": "hh_rr", "KeySchema": [{"AttributeName": "h1", "KeyType": "HASH"}, {"AttributeName": "h2", "KeyType": "HASH"}, {"AttributeName": "r1", "KeyType": "RANGE"}, {"AttributeName": "r2", "KeyType": "RANGE"}], "Projection": {"ProjectionType": "ALL"}}]}"#;

async fn start_server() -> String {
    start_server_with(TABLE).await
}

async fn start_server_with(table: &str) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let db = Database::memory().unwrap();
    db.create_table(serde_json::from_str::<CreateTableRequest>(table).unwrap())
        .unwrap();
    tokio::spawn(async move {
        dynoxide::server::serve_on(listener, db).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    url
}

/// The HTTP status and the body of the answer to a request.
async fn answer(url: &str, operation: &str, body: &str) -> (u16, serde_json::Value) {
    let response = reqwest::Client::new()
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
        .unwrap();
    let status = response.status().as_u16();
    (status, response.json().await.unwrap())
}

/// The status DynamoDB answers with: 400 for an error, 200 otherwise.
fn status_of(expected: &serde_json::Value) -> u16 {
    if expected.get("__type").is_some() {
        400
    } else {
        200
    }
}

/// Send every row in order to a new server, and fail listing each answer that
/// differs from DynamoDB's.
async fn replay(rows: &[Row]) {
    replay_on(TABLE, rows).await;
}

/// [`replay`] against a server holding `table`.
async fn replay_on(table: &str, rows: &[Row]) {
    let url = start_server_with(table).await;
    let mut wrong = Vec::new();
    for &(operation, body, expected) in rows {
        let expected: serde_json::Value = serde_json::from_str(expected).unwrap();
        let expected_status = status_of(&expected);
        let (status, got) = answer(&url, operation, body).await;
        if got != expected || status != expected_status {
            wrong.push(format!(
                "{operation} {body}\n  expected {expected_status} {expected}\n  got      {status} {got}"
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

/// With both keys named the sort key is reported, in either order, through a
/// path or by name (round six has the second). The key
/// refusal loses to a WHERE clause without the whole key and to a parameter
/// count that doesn't match, and BatchExecuteStatement and ExecuteTransaction
/// report it against the statement.
#[tokio::test]
async fn the_sort_key_is_reported_when_both_keys_are_updated() {
    replay(KEY_PATHS_BOTH_KEYS).await;
}

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
/// for what was found. DynamoDB refuses a list here with no message, empty or
/// not (captured eu-west-2 and us-east-1, 2026-11-11); the shared reader's
/// different answer for the other operations is not changed here. A string
/// that reads is no fault, so a `Limit` of 0 beside it is what's reported,
/// and a write statement takes it too.
#[tokio::test]
async fn consistent_read_given_as_a_string_is_read_as_get_item_reads_it() {
    replay(CONSISTENT_READ).await;
    replay(CONSISTENT_READ_MORE).await;
    replay(CONSISTENT_READ_LISTS).await;
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

/// Round four, `ConsistentRead` given a list, against a table that doesn't
/// exist: the request is refused before the table is looked for.
const CONSISTENT_READ_LISTS: &[Row] = &[
    (
        "ExecuteStatement",
        r#"{"Statement":"SELECT * FROM \"_capture_20261111_230_none\" WHERE pk = 'a'","ConsistentRead":[]}"#,
        r#"{"__type":"com.amazon.coral.service#SerializationException"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement":"SELECT * FROM \"_capture_20261111_230_none\" WHERE pk = 'a'","ConsistentRead":[true]}"#,
        r#"{"__type":"com.amazon.coral.service#SerializationException"}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements":[{"Statement":"SELECT * FROM \"_capture_20261111_230_none\" WHERE pk = 'a'","ConsistentRead":[]}]}"#,
        r#"{"__type":"com.amazon.coral.service#SerializationException"}"#,
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
}

/// `size` refuses a number, a boolean or a null, as a literal, a parameter,
/// a call or `size` itself, whatever the item holds, and the first refusal
/// written is the one reported. It comes ahead of an UPDATE's key refusal.
#[tokio::test]
async fn size_refuses_a_number_a_boolean_or_a_null() {
    replay(CALLS_TYPES).await;
}

/// One pass decides which refusal is reported: the operands of a call or of
/// `size` first, then the operator they stand under, and conditions in the
/// order written. A wrong number of operands is noted as the statement is
/// read and comes first. Both keys named by name report the sort key, and a
/// batch naming one item twice is refused as holding duplicate keys.
#[tokio::test]
async fn a_refusal_inside_an_operand_comes_before_its_operator() {
    replay(ROUND_SIX).await;
}

/// The statement checks are one pass over the WHERE clause, and they are the
/// statement's own validation. In BatchExecuteStatement a refusal is that
/// member's error: the other members run, and the member takes no part in
/// the duplicate, the mixed read and write, or the whole-key checks. In
/// ExecuteTransaction it is `Validation failed in TransactStatements[i]`,
/// ahead of the same-item check, never a cancellation reason. On one
/// statement the first fault written is reported, an operand's ahead of the
/// operator it stands under, whether it is a type, a count, an operand that
/// isn't distinct or an IN with too many.
#[tokio::test]
async fn the_operand_checks_are_the_statement_s_own_validation() {
    replay(ROUND_SEVEN).await;
}

/// Inside one condition the operands are checked in the order written, each
/// operand's own operands first, and then the operator, through NOT, OR and
/// brackets. The parameter count comes first; the table, the key, ORDER BY,
/// a key update and RETURNING all come after.
#[tokio::test]
async fn operand_faults_are_reported_in_the_order_written() {
    replay(ROUND_EIGHT).await;
}

/// A write naming an index is refused first, and a read of an index keyed on
/// more than one attribute a side, outside a transaction, before the
/// operands are checked; a read of another index, or one that doesn't exist,
/// after. A refused `set_add` comes after the operands and the parameter
/// count. An UPDATE sent with a NextToken runs.
#[tokio::test]
async fn the_index_and_set_clause_checks_sit_around_the_operand_checks() {
    replay_on(INDEXED_TABLE, ROUND_NINE).await;
}

/// A write naming an index and a read of a multi-attribute index beside other
/// members of a batch, writes naming an index in a transaction, a size's
/// arithmetic refused ahead of its operands, the parameter count ahead of a
/// call's operand, and a DELETE and an INSERT sent with a NextToken, which
/// run as an UPDATE does.
#[tokio::test]
async fn index_members_arithmetic_and_write_next_tokens_answer_as_dynamodb_does() {
    replay_on(INDEXED_TABLE, ROUND_TEN).await;
}

/// A write naming an index is refused ahead of the parameter count and its
/// operands on every surface. A read of a multi-attribute index is refused
/// ahead of the parameter count, a COUNT projection and its operands on
/// ExecuteStatement and in a batch; in a transaction those come first.
#[tokio::test]
async fn index_refusals_rank_against_the_statement_checks() {
    replay_on(INDEXED_TABLE, ROUND_ELEVEN).await;
}

/// A SELECT on an index keyed on two attributes a side is refused, on
/// ExecuteStatement and as a batch member.
#[tokio::test]
async fn a_read_of_a_multi_attribute_index_is_refused() {
    let url = start_server_with(INDEXED_TABLE).await;
    let refusal = "PartiQL statements cannot be executed on tables with multi-attribute HASH \
                   keys or multi-attribute RANGE keys.";
    let select = r#"SELECT * FROM "_capture_20261111_230_m"."hh_rr" WHERE h1 = 'a' AND h2 = 'x'"#;
    let (status, got) = answer(
        &url,
        "ExecuteStatement",
        &serde_json::json!({ "Statement": select }).to_string(),
    )
    .await;
    assert_eq!(
        (status, &got["message"]),
        (400, &serde_json::json!(refusal))
    );
    let (status, got) = answer(
        &url,
        "BatchExecuteStatement",
        &serde_json::json!({ "Statements": [{ "Statement": select }] }).to_string(),
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(
        got["Responses"][0]["Error"],
        serde_json::json!({ "Code": "ValidationError", "Message": refusal })
    );
}

/// A size given two operands is refused for its count standing bare, beside
/// another condition and under NOT, and for the `+` in arithmetic; under IS
/// it is refused for the IS, on every surface. These checks come before the
/// table is looked for.
#[tokio::test]
async fn a_size_with_two_operands_is_refused_where_it_stands() {
    replay(ROUND_TWELVE).await;
}

/// An empty NextToken is refused as a member on any statement, and a
/// non-empty one on a write is ignored. IS on any size is refused for the IS,
/// and arithmetic is refused for its operator wherever it is written,
/// bracketed or not and on a path or a value.
#[tokio::test]
async fn empty_next_tokens_is_on_a_size_and_arithmetic_answer_as_dynamodb_does() {
    replay(ROUND_THIRTEEN).await;
}

/// A batch member refused for its WHERE clause writes nothing, and the
/// member beside it writes as it would alone, whether the refusal is a size's
/// operand count or arithmetic.
#[tokio::test]
async fn a_refused_batch_member_writes_nothing() {
    let url = start_server().await;
    let z = |sk: &str| {
        let url = url.clone();
        let key = serde_json::json!({
            "TableName": TABLE_NAME,
            "Key": {"pk": {"S": "a"}, "sk": {"S": sk}},
            "ConsistentRead": true
        });
        async move { answer(&url, "GetItem", &key.to_string()).await.1["Item"]["z"].clone() }
    };
    for (refused, message) in [
        (
            "size(s, s) AND n = 1",
            "Incorrect number of operands for operator or function; operator or function: \
             size, number of operands: 2",
        ),
        (
            "n + 1 > 0",
            "Unsupported operator in Condition Expression. Operator: +",
        ),
    ] {
        for sk in ["b", "c"] {
            let item = serde_json::json!({
                "TableName": TABLE_NAME,
                "Item": {"pk": {"S": "a"}, "sk": {"S": sk}, "n": {"N": "1"}, "s": {"S": "x"}}
            });
            assert_eq!(answer(&url, "PutItem", &item.to_string()).await.0, 200);
        }
        let batch = serde_json::json!({ "Statements": [
            {"Statement": format!(
                r#"UPDATE "{TABLE_NAME}" SET z = 1 WHERE pk='a' AND sk='b' AND {refused}"#
            )},
            {"Statement": format!(r#"UPDATE "{TABLE_NAME}" SET z = 2 WHERE pk='a' AND sk='c'"#)},
        ]});
        let (status, got) = answer(&url, "BatchExecuteStatement", &batch.to_string()).await;
        assert_eq!(status, 200, "{refused}");
        assert_eq!(
            got["Responses"][0]["Error"],
            serde_json::json!({"Code": "ValidationError", "Message": message}),
            "{refused}"
        );
        assert!(
            got["Responses"][1].get("Error").is_none(),
            "{refused}: {got}"
        );
        assert_eq!(z("b").await, serde_json::Value::Null, "{refused}");
        assert_eq!(z("c").await, serde_json::json!({"N": "2"}), "{refused}");
    }
}

/// A write sent with a NextToken taken from a real page of a SELECT runs once,
/// as it would without the token.
#[tokio::test]
async fn a_write_with_a_next_token_runs_once() {
    let url = start_server().await;
    for sk in ["b", "c", "d"] {
        let item = serde_json::json!({
            "TableName": TABLE_NAME,
            "Item": {"pk": {"S": "a"}, "sk": {"S": sk}, "c": {"N": "0"}}
        });
        assert_eq!(answer(&url, "PutItem", &item.to_string()).await.0, 200);
    }
    let page = serde_json::json!({
        "Statement": format!(r#"SELECT * FROM "{TABLE_NAME}" WHERE pk='a'"#),
        "Limit": 1
    });
    let (status, got) = answer(&url, "ExecuteStatement", &page.to_string()).await;
    assert_eq!(status, 200);
    let token = got["NextToken"]
        .as_str()
        .expect("a first page carries a token");
    let write = serde_json::json!({
        "Statement": format!(r#"UPDATE "{TABLE_NAME}" SET c = c + 1 WHERE pk='a' AND sk='c'"#),
        "NextToken": token
    });
    let (status, got) = answer(&url, "ExecuteStatement", &write.to_string()).await;
    assert_eq!((status, got.get("NextToken")), (200, None), "{got}");
    let key = serde_json::json!({
        "TableName": TABLE_NAME,
        "Key": {"pk": {"S": "a"}, "sk": {"S": "c"}},
        "ConsistentRead": true
    });
    let (_, got) = answer(&url, "GetItem", &key.to_string()).await;
    assert_eq!(got["Item"]["c"], serde_json::json!({"N": "1"}));
}

/// Two more rows are left out, for causes of their own. `SET z = size(true)`
/// doesn't parse here and does on DynamoDB. `attribute_type(n, s)` names the
/// types it would take in the order of a Java hash set, which differs from
/// host to host: the one held here was eu-west-2's on 9 October 2026, and
/// eu-west-2 gave another on 11 November, so the types must be the same ones
/// in any order. Each still gives the answer it gives today.
#[tokio::test]
async fn two_rows_with_causes_of_their_own_are_left_out() {
    let url = start_server().await;
    let (status, _) = answer(&url, ROUND_EIGHT[0].0, ROUND_EIGHT[0].1).await;
    assert_eq!(status, 200);
    let mut wrong = Vec::new();
    for row in LEFT_OUT_OTHER {
        wrong.extend(left_out_answer(&url, row).await);
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
    let types = |answer: &str| {
        let message = serde_json::from_str::<serde_json::Value>(answer).unwrap()["message"]
            .as_str()
            .unwrap()
            .to_string();
        let (head, set) = message.split_once('{').unwrap();
        let mut set: Vec<String> = set
            .trim_end_matches('}')
            .split(',')
            .map(String::from)
            .collect();
        set.sort();
        (head.to_string(), set)
    };
    let (_, _, dynamodb, today) = LEFT_OUT_OTHER[1];
    assert_eq!(types(today), types(dynamodb));
}

/// The captured rows left out of the replays above. `begins_with` and
/// `contains` take only a path and a plain value here, so these answer
/// differently: on DynamoDB either can stand as an operand, take a call as an
/// argument or a first operand that isn't a path, and `begins_with` refuses a
/// first operand by its type. That is one difference with one cause, larger
/// than these, and is left for its own fix. Each is still sent, after the item
/// its round put, and must give the answer it gives today, which differs from
/// DynamoDB's.
#[tokio::test]
async fn only_begins_with_and_contains_rows_are_left_out() {
    let rounds = [
        (CALLS[0], LEFT_OUT_ROUND_ONE),
        (CALLS_MORE[0], LEFT_OUT_ROUND_TWO),
        (CALLS_TYPES[0], LEFT_OUT_ROUND_THREE),
    ];
    let mut count = 0;
    let mut wrong = Vec::new();
    for (put, rows) in rounds {
        let url = start_server().await;
        let (status, _) = answer(&url, put.0, put.1).await;
        assert_eq!(status, 200, "{}", put.1);
        for row in rows {
            count += 1;
            assert!(
                row.1.contains("begins_with(") || row.1.contains("contains("),
                "{}",
                row.1
            );
            wrong.extend(left_out_answer(&url, row).await);
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
    assert_eq!(count, 35);
}

/// Why a left-out row's answer is wrong, if it is: it now matches DynamoDB's,
/// and belongs in the replays, or it has changed from the answer it gave.
async fn left_out_answer(
    url: &str,
    &(operation, body, dynamodb, today): &LeftOut,
) -> Option<String> {
    let dynamodb: serde_json::Value = serde_json::from_str(dynamodb).unwrap();
    let today: serde_json::Value = serde_json::from_str(today).unwrap();
    let (status, got) = answer(url, operation, body).await;
    if got == dynamodb && status == status_of(&dynamodb) {
        Some(format!(
            "{body}\n  now matches DynamoDB: move it into the replays"
        ))
    } else if got != today || status != status_of(&today) {
        Some(format!(
            "{body}\n  answered {status} {got}\n  where it answered {} {today}",
            status_of(&today)
        ))
    } else {
        None
    }
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

/// Round one, the rows left out of `CALLS`.
const LEFT_OUT_ROUND_ONE: &[LeftOut] = &[
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n BETWEEN begins_with(s,'h') AND 9"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: BETWEEN, operand type: BOOL"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Statement wasn't well formed, can't be processed: Expected AND keyword"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n BETWEEN contains(s,'h') AND 9"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: BETWEEN, operand type: BOOL"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Statement wasn't well formed, can't be processed: Expected AND keyword"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(begins_with(s,'h')) > 0"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Statement wasn't well formed, can't be processed: Expected ')' but got '('"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND contains(s, attribute_exists(n))"}"#,
        r#"{"Items":[]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Statement wasn't well formed, can't be processed: Expected ')' but got '('"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND contains(attribute_exists(n), 'x')"}"#,
        r#"{"Items":[]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Statement wasn't well formed, can't be processed: Expected ',' but got '('"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND begins_with(attribute_exists(n), 'x')"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: begins_with, operand type: BOOL"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Statement wasn't well formed, can't be processed: Expected ',' but got '('"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND begins_with(s, attribute_exists(n))"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: begins_with, operand type: BOOL"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Statement wasn't well formed, can't be processed: Expected ')' but got '('"}"#,
    ),
];

/// Round two, the rows left out of `CALLS_MORE`.
const LEFT_OUT_ROUND_TWO: &[LeftOut] = &[
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND begins_with(true, 'x')"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: begins_with, operand type: BOOL"}"#,
        r#"{"Items":[]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(contains(s,'h')) > 0"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Statement wasn't well formed, can't be processed: Expected ')' but got '('"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_type(begins_with(s,'h'),'BOOL')"}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Statement wasn't well formed, can't be processed: Expected ')' but got '('"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND contains(attribute_exists(n), true)"}"#,
        r#"{"Items":[]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Statement wasn't well formed, can't be processed: Expected ',' but got '('"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND contains(l, attribute_exists(n))"}"#,
        r#"{"Items":[]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Statement wasn't well formed, can't be processed: Expected ')' but got '('"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND contains(s, size(s))"}"#,
        r#"{"Items":[]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Statement wasn't well formed, can't be processed: Expected ')' but got '('"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n BETWEEN attribute_exists(n) AND begins_with(s,1)"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: begins_with, operand type: N"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Statement wasn't well formed, can't be processed: Unexpected token after expression"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n BETWEEN begins_with(s,1) AND 5"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: begins_with, operand type: N"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Statement wasn't well formed, can't be processed: Expected AND keyword"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(begins_with(s,1)) > 0"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: begins_with, operand type: N"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Statement wasn't well formed, can't be processed: Expected ')' but got '('"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='zz' AND sk='zz' AND begins_with(s, attribute_exists(n))"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: begins_with, operand type: BOOL"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Statement wasn't well formed, can't be processed: Expected ')' but got '('"}"#,
    ),
];

/// Round three, the rows left out of `CALLS_TYPES`.
const LEFT_OUT_ROUND_THREE: &[LeftOut] = &[
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND begins_with('abc','a')"}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
        r#"{"Items":[]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND begins_with(5,'a')"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: begins_with, operand type: N"}"#,
        r#"{"Items":[]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND begins_with(null,'a')"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: begins_with, operand type: NULL"}"#,
        r#"{"Items":[]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND begins_with([1],'a')"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: begins_with, operand type: L"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Statement wasn't well formed, can't be processed: Expected ',' but got '1'"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND contains('abc','a')"}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
        r#"{"Items":[]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND contains([1,true], attribute_exists(n))"}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Statement wasn't well formed, can't be processed: Expected ',' but got '1'"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND contains(l, attribute_exists(n))"}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Statement wasn't well formed, can't be processed: Expected ')' but got '('"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND begins_with(true,'x') AND size(true) > 0"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: begins_with, operand type: BOOL"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n BETWEEN true AND begins_with(s,1)"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: begins_with, operand type: N"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Statement wasn't well formed, can't be processed: Unexpected token after expression"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n BETWEEN begins_with(s,1) AND size(true)"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: begins_with, operand type: N"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Statement wasn't well formed, can't be processed: Expected AND keyword"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n BETWEEN size(true) AND begins_with(s,1)"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Statement wasn't well formed, can't be processed: Unexpected token after expression"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND begins_with(?, 'a')", "Parameters": [{"S": "abc"}]}"#,
        r#"{"Items":[{"pk":{"S":"a"}}]}"#,
        r#"{"Items":[]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND begins_with(?, 'a')", "Parameters": [{"N": "5"}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: begins_with, operand type: N"}"#,
        r#"{"Items":[]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND begins_with(?, 'a')", "Parameters": [{"L": [{"N": "1"}]}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: begins_with, operand type: L"}"#,
        r#"{"Items":[]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND begins_with(?, 'a')", "Parameters": [{"M": {"a": {"N": "1"}}}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: begins_with, operand type: M"}"#,
        r#"{"Items":[]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND begins_with(?, 'a')", "Parameters": [{"SS": ["a"]}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: begins_with, operand type: SS"}"#,
        r#"{"Items":[]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND begins_with(?, 'a')", "Parameters": [{"NULL": true}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: begins_with, operand type: NULL"}"#,
        r#"{"Items":[]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND begins_with(?, 'a')", "Parameters": [{"BOOL": false}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: begins_with, operand type: BOOL"}"#,
        r#"{"Items":[]}"#,
    ),
];

/// Round six, captured in eu-west-2: refusal order, both keys named by name,
/// and a batch naming one item twice.
const ROUND_SIX: &[Row] = &[
    (
        "PutItem",
        r#"{"TableName":"_capture_20261111_230_t","Item":{"pk":{"S":"a"},"sk":{"S":"b"},"n":{"N":"5"},"s":{"S":"hello"},"bt":{"BOOL":true}}}"#,
        r#"{}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_exists(n) BETWEEN size(true) AND 5"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_exists(n) BETWEEN 1 AND size(true)"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(true) > 0 AND attribute_type(n, attribute_exists(n))"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_type(n, attribute_exists(n)) AND size(true) > 0"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: attribute_type, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_type(n, size(true))"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_type(n, size(s))"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: attribute_type, operand type: N"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_type(size(true), 'N')"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n BETWEEN true AND 5 AND size(true) > 0"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: BETWEEN, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(true) > 0 AND n BETWEEN true AND 5"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_exists(n) BETWEEN attribute_exists(n, n) AND 5"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect number of operands for operator or function; operator or function: attribute_exists, number of operands: 2"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_exists(n) < size(true)"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(true) < attribute_exists(n)"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_exists(n) BETWEEN 1 AND 5"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: BETWEEN, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(attribute_type(n, ?)) > 0", "Parameters": [{"S": "N"}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n BETWEEN attribute_type(n, ?) AND 5", "Parameters": [{"S": "N"}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: BETWEEN, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(size(true)) > 0"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n IN [size(true), attribute_exists(n)]"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND NOT size(true) > 0"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(true) > 0 OR n = 5"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n = 5 OR size(true) > 0"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
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
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_t\" SET \"pk.x\" = 1 WHERE pk='zz' AND sk='zz'"}"#,
        r#"{"__type":"com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException","message":"The conditional request failed"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_t\" SET pkx = 1 WHERE pk='zz' AND sk='zz'"}"#,
        r#"{"__type":"com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException","message":"The conditional request failed"}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements":[{"Statement":"SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'","ConsistentRead":"yes"},{"Statement":"SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'","ConsistentRead":"yes"}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Provided list of item keys contains duplicates"}"#,
    ),
];

/// Round seven, captured in eu-west-2: batches, transactions, and the order
/// against an operand that isn't distinct, the IN limit and operand counts.
/// Row 52 is in `LEFT_OUT_OTHER`.
const ROUND_SEVEN: &[Row] = &[
    (
        "PutItem",
        r#"{"TableName":"_capture_20261111_230_t","Item":{"pk":{"S":"a"},"sk":{"S":"b"},"n":{"N":"5"},"s":{"S":"hello"},"bt":{"BOOL":true}}}"#,
        r#"{}"#,
    ),
    (
        "PutItem",
        r#"{"TableName":"_capture_20261111_230_t","Item":{"pk":{"S":"a"},"sk":{"S":"c"},"n":{"N":"5"},"s":{"S":"hello"},"bt":{"BOOL":true}}}"#,
        r#"{}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements": [{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(true) > 0"}]}"#,
        r#"{"Responses":[{"Error":{"Code":"ValidationError","Message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}}]}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements": [{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(true) > 0"}, {"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='c'"}]}"#,
        r#"{"Responses":[{"Error":{"Code":"ValidationError","Message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}},{"Item":{"pk":{"S":"a"}},"TableName":"_capture_20261111_230_t"}]}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements": [{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(true) > 0"}, {"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'"}]}"#,
        r#"{"Responses":[{"Error":{"Code":"ValidationError","Message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}},{"Item":{"pk":{"S":"a"}},"TableName":"_capture_20261111_230_t"}]}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements": [{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(true) > 0"}, {"Statement": "UPDATE \"_capture_20261111_230_t\" SET z = 1 WHERE pk='a' AND sk='c'"}]}"#,
        r#"{"Responses":[{"Error":{"Code":"ValidationError","Message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}},{"TableName":"_capture_20261111_230_t"}]}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements": [{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_exists(n) BETWEEN 1 AND 5"}, {"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'"}]}"#,
        r#"{"Responses":[{"Error":{"Code":"ValidationError","Message":"Incorrect operand type for operator or function; operator or function: BETWEEN, operand type: BOOL"}},{"Item":{"pk":{"S":"a"}},"TableName":"_capture_20261111_230_t"}]}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements": [{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_type(n, 5)"}, {"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'"}]}"#,
        r#"{"Responses":[{"Error":{"Code":"ValidationError","Message":"Incorrect operand type for operator or function; operator or function: attribute_type, operand type: N"}},{"Item":{"pk":{"S":"a"}},"TableName":"_capture_20261111_230_t"}]}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements": [{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n < true"}, {"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'"}]}"#,
        r#"{"Responses":[{"Error":{"Code":"ValidationError","Message":"Incorrect operand type for operator or function; operator or function: <, operand type: BOOL"}},{"Item":{"pk":{"S":"a"}},"TableName":"_capture_20261111_230_t"}]}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements": [{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_exists(n, n)"}, {"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'"}]}"#,
        r#"{"Responses":[{"Error":{"Code":"ValidationError","Message":"Incorrect number of operands for operator or function; operator or function: attribute_exists, number of operands: 2"}},{"Item":{"pk":{"S":"a"}},"TableName":"_capture_20261111_230_t"}]}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements": [{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n = n"}, {"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'"}]}"#,
        r#"{"Responses":[{"Error":{"Code":"ValidationError","Message":"The first operand must be distinct from the remaining operands for this operator or function; operator: =, first operand: [n]"}},{"Item":{"pk":{"S":"a"}},"TableName":"_capture_20261111_230_t"}]}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements": [{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_exists(n) < 1"}, {"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'"}]}"#,
        r#"{"Responses":[{"Error":{"Code":"ValidationError","Message":"Incorrect operand type for operator or function; operator or function: <, operand type: BOOL"}},{"Item":{"pk":{"S":"a"}},"TableName":"_capture_20261111_230_t"}]}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements": [{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(?) > 0", "Parameters": [{"BOOL": true}]}, {"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'"}]}"#,
        r#"{"Responses":[{"Error":{"Code":"ValidationError","Message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}},{"Item":{"pk":{"S":"a"}},"TableName":"_capture_20261111_230_t"}]}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements": [{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(?) > 0", "Parameters": [{"BOOL": true}]}]}"#,
        r#"{"Responses":[{"Error":{"Code":"ValidationError","Message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}}]}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements": [{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n < ?", "Parameters": [{"BOOL": true}]}, {"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'"}]}"#,
        r#"{"Responses":[{"Error":{"Code":"ValidationError","Message":"Incorrect operand type for operator or function; operator or function: <, operand type: BOOL"}},{"Item":{"pk":{"S":"a"}},"TableName":"_capture_20261111_230_t"}]}"#,
    ),
    (
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "UPDATE \"_capture_20261111_230_t\" SET z = 1 WHERE pk='a' AND sk='b' AND size(true) > 0"}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Validation failed in TransactStatements[0]: Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "UPDATE \"_capture_20261111_230_t\" SET z = 1 WHERE pk='a' AND sk='b'"}, {"Statement": "UPDATE \"_capture_20261111_230_t\" SET z = 1 WHERE pk='a' AND sk='c' AND size(true) > 0"}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Validation failed in TransactStatements[1]: Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "UPDATE \"_capture_20261111_230_t\" SET z = 1 WHERE pk='a' AND sk='b' AND size(true) > 0"}, {"Statement": "UPDATE \"_capture_20261111_230_t\" SET z = 1 WHERE pk='a' AND sk='b'"}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Validation failed in TransactStatements[0]: Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "UPDATE \"_capture_20261111_230_t\" SET z = 1 WHERE pk='a' AND sk='b' AND attribute_exists(n) BETWEEN 1 AND 5"}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Validation failed in TransactStatements[0]: Incorrect operand type for operator or function; operator or function: BETWEEN, operand type: BOOL"}"#,
    ),
    (
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "UPDATE \"_capture_20261111_230_t\" SET z = 1 WHERE pk='a' AND sk='b' AND attribute_exists(n) BETWEEN 1 AND 5"}, {"Statement": "UPDATE \"_capture_20261111_230_t\" SET z = 1 WHERE pk='a' AND sk='b'"}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Validation failed in TransactStatements[0]: Incorrect operand type for operator or function; operator or function: BETWEEN, operand type: BOOL"}"#,
    ),
    (
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "UPDATE \"_capture_20261111_230_t\" SET z = 1 WHERE pk='a' AND sk='b' AND attribute_type(n, 5)"}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Validation failed in TransactStatements[0]: Incorrect operand type for operator or function; operator or function: attribute_type, operand type: N"}"#,
    ),
    (
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "UPDATE \"_capture_20261111_230_t\" SET z = 1 WHERE pk='a' AND sk='b' AND attribute_type(n, 5)"}, {"Statement": "UPDATE \"_capture_20261111_230_t\" SET z = 1 WHERE pk='a' AND sk='b'"}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Validation failed in TransactStatements[0]: Incorrect operand type for operator or function; operator or function: attribute_type, operand type: N"}"#,
    ),
    (
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "UPDATE \"_capture_20261111_230_t\" SET z = 1 WHERE pk='a' AND sk='b' AND n < true"}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Validation failed in TransactStatements[0]: Incorrect operand type for operator or function; operator or function: <, operand type: BOOL"}"#,
    ),
    (
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "UPDATE \"_capture_20261111_230_t\" SET z = 1 WHERE pk='a' AND sk='b' AND n < true"}, {"Statement": "UPDATE \"_capture_20261111_230_t\" SET z = 1 WHERE pk='a' AND sk='b'"}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Validation failed in TransactStatements[0]: Incorrect operand type for operator or function; operator or function: <, operand type: BOOL"}"#,
    ),
    (
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "UPDATE \"_capture_20261111_230_t\" SET z = 1 WHERE pk='a' AND sk='b' AND attribute_exists(n, n)"}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Validation failed in TransactStatements[0]: Incorrect number of operands for operator or function; operator or function: attribute_exists, number of operands: 2"}"#,
    ),
    (
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "UPDATE \"_capture_20261111_230_t\" SET z = 1 WHERE pk='a' AND sk='b' AND attribute_exists(n, n)"}, {"Statement": "UPDATE \"_capture_20261111_230_t\" SET z = 1 WHERE pk='a' AND sk='b'"}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Validation failed in TransactStatements[0]: Incorrect number of operands for operator or function; operator or function: attribute_exists, number of operands: 2"}"#,
    ),
    (
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "UPDATE \"_capture_20261111_230_t\" SET z = 1 WHERE pk='a' AND sk='b' AND n = n"}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Validation failed in TransactStatements[0]: The first operand must be distinct from the remaining operands for this operator or function; operator: =, first operand: [n]"}"#,
    ),
    (
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "UPDATE \"_capture_20261111_230_t\" SET z = 1 WHERE pk='a' AND sk='b' AND n = n"}, {"Statement": "UPDATE \"_capture_20261111_230_t\" SET z = 1 WHERE pk='a' AND sk='b'"}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Validation failed in TransactStatements[0]: The first operand must be distinct from the remaining operands for this operator or function; operator: =, first operand: [n]"}"#,
    ),
    (
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "UPDATE \"_capture_20261111_230_t\" SET z = 1 WHERE pk='a' AND sk='b' AND attribute_exists(n) < 1"}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Validation failed in TransactStatements[0]: Incorrect operand type for operator or function; operator or function: <, operand type: BOOL"}"#,
    ),
    (
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "UPDATE \"_capture_20261111_230_t\" SET z = 1 WHERE pk='a' AND sk='b' AND attribute_exists(n) < 1"}, {"Statement": "UPDATE \"_capture_20261111_230_t\" SET z = 1 WHERE pk='a' AND sk='b'"}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Validation failed in TransactStatements[0]: Incorrect operand type for operator or function; operator or function: <, operand type: BOOL"}"#,
    ),
    (
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "UPDATE \"_capture_20261111_230_t\" SET z = 1 WHERE pk='a' AND sk='b' AND size(?) > 0", "Parameters": [{"BOOL": true}]}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Validation failed in TransactStatements[0]: Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "UPDATE \"_capture_20261111_230_t\" SET z = 1 WHERE pk='a' AND sk='b' AND size(?) > 0", "Parameters": [{"BOOL": true}]}, {"Statement": "UPDATE \"_capture_20261111_230_t\" SET z = 1 WHERE pk='a' AND sk='b'"}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Validation failed in TransactStatements[0]: Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "UPDATE \"_capture_20261111_230_t\" SET z = 1 WHERE pk='a' AND sk='b' AND n < ?", "Parameters": [{"BOOL": true}]}, {"Statement": "UPDATE \"_capture_20261111_230_t\" SET z = 1 WHERE pk='a' AND sk='b'"}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Validation failed in TransactStatements[0]: Incorrect operand type for operator or function; operator or function: <, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(true) > 0 AND n = n"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n = n AND size(true) > 0"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"The first operand must be distinct from the remaining operands for this operator or function; operator: =, first operand: [n]"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_exists(n) < 1 AND n = n"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: <, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n = n AND attribute_exists(n) < 1"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"The first operand must be distinct from the remaining operands for this operator or function; operator: =, first operand: [n]"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_type(n, 5) AND n = n"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: attribute_type, operand type: N"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n = n AND attribute_type(n, 5)"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"The first operand must be distinct from the remaining operands for this operator or function; operator: =, first operand: [n]"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(?) > 0 AND n = n", "Parameters": [{"BOOL": true}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n = n AND size(?) > 0", "Parameters": [{"BOOL": true}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"The first operand must be distinct from the remaining operands for this operator or function; operator: =, first operand: [n]"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_exists(n) BETWEEN 1 AND 5 AND n = n"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: BETWEEN, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n = n AND attribute_exists(n) BETWEEN 1 AND 5"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"The first operand must be distinct from the remaining operands for this operator or function; operator: =, first operand: [n]"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(true) > 0 AND n IN [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100]"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n IN [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100] AND size(true) > 0"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"The IN operator is provided with too many operands; number of operands: 101"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n < ? AND n = n", "Parameters": [{"BOOL": true}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: <, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n = n AND n < ?", "Parameters": [{"BOOL": true}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"The first operand must be distinct from the remaining operands for this operator or function; operator: =, first operand: [n]"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_type(n, ?) AND n = n", "Parameters": [{"N": "5"}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: attribute_type, operand type: N"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n = n AND attribute_type(n, ?)", "Parameters": [{"N": "5"}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"The first operand must be distinct from the remaining operands for this operator or function; operator: =, first operand: [n]"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_type(n, 'ZZ') AND size(true) > 0"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Invalid attribute type name found; type: ZZ, valid types: {N,BS,L,B,NULL,M,S,SS,NS,BOOL}"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(true) > 0 AND attribute_type(n, 'ZZ')"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(true) > 0 AND attribute_type(n, s)"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n < true AND attribute_type(n, 5)"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: <, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_type(n, 5) AND n < true"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: attribute_type, operand type: N"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(?) > 0 AND attribute_exists(n, n)", "Parameters": [{"BOOL": true}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_exists(n, n) AND size(?) > 0", "Parameters": [{"BOOL": true}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect number of operands for operator or function; operator or function: attribute_exists, number of operands: 2"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(true) > 0 AND attribute_exists(n, n)"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_exists(n, n) AND size(true) > 0"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect number of operands for operator or function; operator or function: attribute_exists, number of operands: 2"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND ? < attribute_exists(n)", "Parameters": [{"NULL": true}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: <, operand type: NULL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_type(n, 5) BETWEEN 1 AND 5"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: attribute_type, operand type: N"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_type(n, 'ZZ')"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Invalid attribute type name found; type: ZZ, valid types: {N,BS,L,B,NULL,M,S,SS,NS,BOOL}"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_type(n, ?) BETWEEN 1 AND 5", "Parameters": [{"S": "Q"}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Invalid attribute type name found; type: Q, valid types: {N,BS,L,B,NULL,M,S,SS,NS,BOOL}"}"#,
    ),
];

/// Round eight, captured in eu-west-2: the order inside one condition, NOT,
/// OR and brackets, the parameter count, a missing table or key, and
/// RETURNING. Row 41 is in `LEFT_OUT_OTHER`.
const ROUND_EIGHT: &[Row] = &[
    (
        "PutItem",
        r#"{"TableName":"_capture_20261111_230_t","Item":{"pk":{"S":"a"},"sk":{"S":"b"},"n":{"N":"5"},"s":{"S":"hello"},"bt":{"BOOL":true}}}"#,
        r#"{}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n BETWEEN size(true) AND attribute_exists(n, n)"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n BETWEEN attribute_exists(n, n) AND size(true)"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect number of operands for operator or function; operator or function: attribute_exists, number of operands: 2"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(true) = attribute_exists(n, n)"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_exists(n, n) = size(true)"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect number of operands for operator or function; operator or function: attribute_exists, number of operands: 2"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n IN [size(true), attribute_exists(n, n)]"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n IN [attribute_exists(n, n), size(true)]"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect number of operands for operator or function; operator or function: attribute_exists, number of operands: 2"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(true) BETWEEN n AND n"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n BETWEEN n AND size(true)"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n BETWEEN size(true) AND n"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(true) < size(true)"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n < n"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"The first operand must be distinct from the remaining operands for this operator or function; operator: <, first operand: [n]"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND true < true"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: <, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_exists(n) < attribute_exists(n)"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: <, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_type(n, n)"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"The first operand must be distinct from the remaining operands for this operator or function; operator: attribute_type, first operand: [n]"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_type(size(true), size(true))"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_exists(n) NOT BETWEEN 1 AND 5"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: BETWEEN, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND NOT attribute_exists(n) BETWEEN 1 AND 5"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: BETWEEN, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND NOT (size(true) > 0)"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND NOT n = n"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"The first operand must be distinct from the remaining operands for this operator or function; operator: =, first operand: [n]"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND (size(true) > 0 OR n = n)"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND (n = n OR size(true) > 0)"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"The first operand must be distinct from the remaining operands for this operator or function; operator: =, first operand: [n]"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(true) > 0 AND (n = n OR n < true)"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND NULL < attribute_exists(n)"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: <, operand type: NULL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_exists(n) < NULL"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: <, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(s) < true AND s IN [s]"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: <, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND s IN [s] AND size(s) < true"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"The first operand must be distinct from the remaining operands for this operator or function; operator: IN, first operand: [s]"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(true) > 0 AND n = ?"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Number of parameters in request and statement don't match."}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n = ? AND size(true) > 0"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Number of parameters in request and statement don't match."}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n = n AND s = ?"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Number of parameters in request and statement don't match."}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND s = ? AND n = n"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Number of parameters in request and statement don't match."}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(?) > 0 AND s = ?", "Parameters": [{"BOOL": true}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Number of parameters in request and statement don't match."}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND n < true AND s = ?"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Number of parameters in request and statement don't match."}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(true) > 0 AND s = ?", "Parameters": [{"S": "a"}, {"S": "b"}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE size(true) > 0"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk = 'a' AND size(true) > 0 ORDER BY n"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_missing\" WHERE pk='a' AND size(true) > 0"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_missing\" WHERE pk='a' AND n = n"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"The first operand must be distinct from the remaining operands for this operator or function; operator: =, first operand: [n]"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_t\" SET pk = 'z' WHERE pk='a' AND sk='b' AND size(true) > 0"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_t\" SET z = 1 WHERE pk='a' AND size(true) > 0"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "DELETE FROM \"_capture_20261111_230_t\" WHERE pk='a' AND size(true) > 0"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements": [{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND size(true) > 0 AND s = ?"}, {"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b'"}]}"#,
        r#"{"Responses":[{"Error":{"Code":"ValidationError","Message":"Number of parameters in request and statement don't match."}},{"Item":{"pk":{"S":"a"}},"TableName":"_capture_20261111_230_t"}]}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements": [{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE size(true) > 0"}]}"#,
        r#"{"Responses":[{"Error":{"Code":"ValidationError","Message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}}]}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements": [{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND size(true) > 0"}]}"#,
        r#"{"Responses":[{"Error":{"Code":"ValidationError","Message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}}]}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements": [{"Statement": "SELECT pk FROM \"_capture_20261111_230_missing\" WHERE pk='a' AND sk='b' AND size(true) > 0"}]}"#,
        r#"{"Responses":[{"Error":{"Code":"ValidationError","Message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}}]}"#,
    ),
    (
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "UPDATE \"_capture_20261111_230_t\" SET z = 1 WHERE pk='a' AND sk='b' AND size(true) > 0 AND s = ?"}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Validation failed in TransactStatements[0]: Number of parameters in request and statement don't match."}"#,
    ),
    (
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "UPDATE \"_capture_20261111_230_missing\" SET z = 1 WHERE pk='a' AND sk='b' AND size(true) > 0"}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Validation failed in TransactStatements[0]: Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "UPDATE \"_capture_20261111_230_t\" SET z = 1 WHERE pk='a' AND sk='b' AND size(true) > 0 RETURNING ALL OLD *"}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Validation failed in TransactStatements[0]: Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
];

/// Round nine, captured in eu-west-2 against `INDEXED_TABLE`: index reads and
/// writes, index reads in a transaction, `set_add` refusals and a NextToken
/// on an UPDATE.
const ROUND_NINE: &[Row] = &[
    (
        "PutItem",
        r#"{"TableName": "_capture_20261111_230_m", "Item": {"id": {"S": "i1"}, "sk": {"S": "s"}, "g": {"S": "g1"}, "h1": {"S": "a"}, "h2": {"S": "x"}, "r1": {"S": "m"}, "r2": {"S": "n"}, "n": {"N": "5"}, "ss": {"SS": ["a"]}}}"#,
        r#"{}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT * FROM \"_capture_20261111_230_m\".\"hh_rr\" WHERE h1 = 'a' AND h2 = 'x'"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"PartiQL statements cannot be executed on tables with multi-attribute HASH keys or multi-attribute RANGE keys."}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT * FROM \"_capture_20261111_230_m\".\"hh_rr\" WHERE h1 = h1"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"PartiQL statements cannot be executed on tables with multi-attribute HASH keys or multi-attribute RANGE keys."}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT * FROM \"_capture_20261111_230_m\".\"hh_rr\" WHERE h1 < true"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"PartiQL statements cannot be executed on tables with multi-attribute HASH keys or multi-attribute RANGE keys."}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT * FROM \"_capture_20261111_230_m\".\"hh_rr\" WHERE size(true) > 0"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"PartiQL statements cannot be executed on tables with multi-attribute HASH keys or multi-attribute RANGE keys."}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT * FROM \"_capture_20261111_230_m\".\"hh_rr\" WHERE attribute_exists(h1, h1)"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"PartiQL statements cannot be executed on tables with multi-attribute HASH keys or multi-attribute RANGE keys."}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT * FROM \"_capture_20261111_230_m\".\"hh_rr\" WHERE begins_with(h1, 5)"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"PartiQL statements cannot be executed on tables with multi-attribute HASH keys or multi-attribute RANGE keys."}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT * FROM \"_capture_20261111_230_m\".\"single\" WHERE g = 'g1' AND h1 = h1"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"The first operand must be distinct from the remaining operands for this operator or function; operator: =, first operand: [h1]"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT * FROM \"_capture_20261111_230_m\".\"single\" WHERE g = 'g1' AND h1 < true"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: <, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT * FROM \"_capture_20261111_230_m\".\"single\" WHERE g = 'g1' AND size(true) > 0"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT * FROM \"_capture_20261111_230_m\".\"single\" WHERE g = 'g1' AND attribute_exists(h1, h1)"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect number of operands for operator or function; operator or function: attribute_exists, number of operands: 2"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT * FROM \"_capture_20261111_230_m\".\"nosuch\" WHERE h1 = h1"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"The first operand must be distinct from the remaining operands for this operator or function; operator: =, first operand: [h1]"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT * FROM \"_capture_20261111_230_m\".\"nosuch\" WHERE h1 < true"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: <, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT * FROM \"_capture_20261111_230_m\".\"nosuch\" WHERE size(true) > 0"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_m\".\"hh_rr\" SET a = 1 WHERE id = 'i1' AND sk = 's' AND h1 = h1"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"This operation is not supported on an index"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "DELETE FROM \"_capture_20261111_230_m\".\"hh_rr\" WHERE id = 'i1' AND sk = 's' AND h1 = h1"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"This operation is not supported on an index"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_m\".\"hh_rr\" SET a = 1 WHERE id = 'i1' AND sk = 's' AND h1 < true"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"This operation is not supported on an index"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "DELETE FROM \"_capture_20261111_230_m\".\"hh_rr\" WHERE id = 'i1' AND sk = 's' AND h1 < true"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"This operation is not supported on an index"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_m\".\"hh_rr\" SET a = 1 WHERE id = 'i1' AND sk = 's' AND size(true) > 0"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"This operation is not supported on an index"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "DELETE FROM \"_capture_20261111_230_m\".\"hh_rr\" WHERE id = 'i1' AND sk = 's' AND size(true) > 0"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"This operation is not supported on an index"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_m\" SET a = 1 WHERE id = 'i1' AND sk = 's' AND size(true) > 0", "NextToken": "abc"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_m\" SET a = 1 WHERE id = 'i1' AND sk = 's' AND n = n", "NextToken": "abc"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"The first operand must be distinct from the remaining operands for this operator or function; operator: =, first operand: [n]"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_m\" SET a = 1 WHERE id = 'i1' AND sk = 's'", "NextToken": "abc"}"#,
        r#"{"Items":[]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT * FROM \"_capture_20261111_230_m\" WHERE id = 'i1' AND sk = 's' AND size(true) > 0", "NextToken": "abc"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT * FROM \"_capture_20261111_230_m\" WHERE id = 'i1' AND sk = 's' AND n = n", "NextToken": "abc"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"The first operand must be distinct from the remaining operands for this operator or function; operator: =, first operand: [n]"}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements": [{"Statement": "SELECT * FROM \"_capture_20261111_230_m\".\"hh_rr\" WHERE h1 = h1"}]}"#,
        r#"{"Responses":[{"Error":{"Code":"ValidationError","Message":"PartiQL statements cannot be executed on tables with multi-attribute HASH keys or multi-attribute RANGE keys."}}]}"#,
    ),
    (
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "SELECT * FROM \"_capture_20261111_230_m\".\"single\" WHERE g = 'g1' AND h1 = h1"}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Validation failed in TransactStatements[0]: The first operand must be distinct from the remaining operands for this operator or function; operator: =, first operand: [h1]"}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements": [{"Statement": "SELECT * FROM \"_capture_20261111_230_m\".\"hh_rr\" WHERE h1 < true"}]}"#,
        r#"{"Responses":[{"Error":{"Code":"ValidationError","Message":"PartiQL statements cannot be executed on tables with multi-attribute HASH keys or multi-attribute RANGE keys."}}]}"#,
    ),
    (
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "SELECT * FROM \"_capture_20261111_230_m\".\"single\" WHERE g = 'g1' AND h1 < true"}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Validation failed in TransactStatements[0]: Incorrect operand type for operator or function; operator or function: <, operand type: BOOL"}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements": [{"Statement": "SELECT * FROM \"_capture_20261111_230_m\".\"hh_rr\" WHERE size(true) > 0"}]}"#,
        r#"{"Responses":[{"Error":{"Code":"ValidationError","Message":"PartiQL statements cannot be executed on tables with multi-attribute HASH keys or multi-attribute RANGE keys."}}]}"#,
    ),
    (
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "SELECT * FROM \"_capture_20261111_230_m\".\"single\" WHERE g = 'g1' AND size(true) > 0"}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Validation failed in TransactStatements[0]: Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements": [{"Statement": "SELECT * FROM \"_capture_20261111_230_m\".\"hh_rr\" WHERE attribute_exists(h1, h1)"}]}"#,
        r#"{"Responses":[{"Error":{"Code":"ValidationError","Message":"PartiQL statements cannot be executed on tables with multi-attribute HASH keys or multi-attribute RANGE keys."}}]}"#,
    ),
    (
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "SELECT * FROM \"_capture_20261111_230_m\".\"single\" WHERE g = 'g1' AND attribute_exists(h1, h1)"}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Validation failed in TransactStatements[0]: Incorrect number of operands for operator or function; operator or function: attribute_exists, number of operands: 2"}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements": [{"Statement": "SELECT * FROM \"_capture_20261111_230_m\".\"hh_rr\" WHERE h1 = 'a' AND h2 = 'x'"}]}"#,
        r#"{"Responses":[{"Error":{"Code":"ValidationError","Message":"PartiQL statements cannot be executed on tables with multi-attribute HASH keys or multi-attribute RANGE keys."}}]}"#,
    ),
    (
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "SELECT * FROM \"_capture_20261111_230_m\".\"single\" WHERE g = 'g1'"}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Validation failed in TransactStatements[0]: Reads on indices are not supported within transactions."}"#,
    ),
    (
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "SELECT * FROM \"_capture_20261111_230_m\".\"hh_rr\" WHERE h1 = 'a' AND h2 = 'x'"}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Validation failed in TransactStatements[0]: Reads on indices are not supported within transactions."}"#,
    ),
    (
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "SELECT * FROM \"_capture_20261111_230_m\".\"hh_rr\" WHERE size(true) > 0"}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Validation failed in TransactStatements[0]: Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_m\" SET t = set_add(ss, <<'x'>>) WHERE id = 'i1' AND sk = 's'"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"The first argument to SET_ADD must equal the assignment value at 1:42:7"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_m\" SET t = set_add(ss, <<'x'>>) WHERE id = 'i1' AND sk = 's' AND size(true) > 0"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_m\" SET t = set_add(ss, <<'x'>>) WHERE id = 'i1' AND sk = 's' AND n = n"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"The first operand must be distinct from the remaining operands for this operator or function; operator: =, first operand: [n]"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_m\" SET t = set_add(ss, <<'x'>>) WHERE id = 'i1' AND sk = 's' AND n < true"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: <, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_m\" SET t = set_add(ss, <<'x'>>) WHERE id = 'i1' AND sk = 's' AND g = ?"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Number of parameters in request and statement don't match."}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_m\" SET ss = set_add(ss, 5) WHERE id = 'i1' AND sk = 's'"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"The second argument to SET_ADD must be a value with type SET at 1:51:2"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_m\" SET ss = set_add(ss, 5) WHERE id = 'i1' AND sk = 's' AND size(true) > 0"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_m\" SET ss = set_add(ss, 5) WHERE id = 'i1' AND sk = 's' AND n = n"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"The first operand must be distinct from the remaining operands for this operator or function; operator: =, first operand: [n]"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_m\" SET ss = set_add(ss, 5) WHERE id = 'i1' AND sk = 's' AND n < true"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: <, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_m\" SET ss = set_add(ss, 5) WHERE id = 'i1' AND sk = 's' AND g = ?"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Number of parameters in request and statement don't match."}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_m\" SET ss = set_add(ss) WHERE id = 'i1' AND sk = 's'"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"SET_ADD must have exactly two arguments at 1:43:7"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_m\" SET ss = set_add(ss) WHERE id = 'i1' AND sk = 's' AND size(true) > 0"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_m\" SET ss = set_add(ss) WHERE id = 'i1' AND sk = 's' AND n = n"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"The first operand must be distinct from the remaining operands for this operator or function; operator: =, first operand: [n]"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_m\" SET ss = set_add(ss) WHERE id = 'i1' AND sk = 's' AND n < true"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: <, operand type: BOOL"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_m\" SET ss = set_add(ss) WHERE id = 'i1' AND sk = 's' AND g = ?"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Number of parameters in request and statement don't match."}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_absent\" SET t = set_add(ss, <<'x'>>) WHERE id = 'i1' AND sk = 's'"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"The first argument to SET_ADD must equal the assignment value at 1:47:7"}"#,
    ),
    (
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "UPDATE \"_capture_20261111_230_m\" SET t = set_add(ss, <<'x'>>) WHERE id = 'i1' AND sk = 's' AND size(true) > 0"}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Validation failed in TransactStatements[0]: Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements": [{"Statement": "UPDATE \"_capture_20261111_230_m\" SET t = set_add(ss, <<'x'>>) WHERE id = 'i1' AND sk = 's' AND size(true) > 0"}]}"#,
        r#"{"Responses":[{"Error":{"Code":"ValidationError","Message":"Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}}]}"#,
    ),
];

/// Round eight's row 41 and round seven's row 52, left out for causes of
/// their own.
const LEFT_OUT_OTHER: &[LeftOut] = &[
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_t\" SET z = size(true) WHERE pk='a' AND sk='b' AND n = n"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"The first operand must be distinct from the remaining operands for this operator or function; operator: =, first operand: [n]"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Statement wasn't well formed, can't be processed: Unexpected token after expression"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT pk FROM \"_capture_20261111_230_t\" WHERE pk='a' AND sk='b' AND attribute_type(n, s) AND size(true) > 0"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: attribute_type, operand type: {B,FS,DOUBLESET,NULL,BOOL,DECIMAL,S,NS,INTSET,SS,HDS,N,DECIMALSET,M,L,HD,FLOAT,DOUBLE,BS,DICT,INT}"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: attribute_type, operand type: {S,HDS,N,FS,BS,NULL,DOUBLE,NS,DECIMAL,SS,DECIMALSET,BOOL,HD,DICT,INTSET,M,L,B,FLOAT,INT,DOUBLESET}"}"#,
    ),
];

/// Round ten, captured in eu-west-2 against `INDEXED_TABLE`.
const ROUND_TEN: &[Row] = &[
    (
        "PutItem",
        r#"{"TableName": "_capture_20261111_230_m", "Item": {"id": {"S": "i1"}, "sk": {"S": "s"}, "g": {"S": "g1"}, "h1": {"S": "a"}, "h2": {"S": "x"}, "r1": {"S": "m"}, "r2": {"S": "n"}, "n": {"N": "5"}, "s": {"S": "hello"}}}"#,
        r#"{}"#,
    ),
    (
        "PutItem",
        r#"{"TableName": "_capture_20261111_230_m", "Item": {"id": {"S": "i2"}, "sk": {"S": "s"}, "g": {"S": "g2"}, "r1": {"S": "m"}}}"#,
        r#"{}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements": [{"Statement": "UPDATE \"_capture_20261111_230_m\".\"single\" SET a = 1 WHERE id = 'i1' AND sk = 's'"}]}"#,
        r#"{"Responses":[{"Error":{"Code":"ValidationError","Message":"This operation is not supported on an index"}}]}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements": [{"Statement": "UPDATE \"_capture_20261111_230_m\".\"single\" SET a = 1 WHERE id = 'i1' AND sk = 's'"}, {"Statement": "UPDATE \"_capture_20261111_230_m\" SET a = 1 WHERE id = 'i1' AND sk = 's'"}]}"#,
        r#"{"Responses":[{"Error":{"Code":"ValidationError","Message":"This operation is not supported on an index"}},{"TableName":"_capture_20261111_230_m"}]}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements": [{"Statement": "UPDATE \"_capture_20261111_230_m\".\"single\" SET a = 1 WHERE id = 'i1' AND sk = 's'"}, {"Statement": "SELECT * FROM \"_capture_20261111_230_m\" WHERE id = 'i2' AND sk = 's'"}]}"#,
        r#"{"Responses":[{"Error":{"Code":"ValidationError","Message":"This operation is not supported on an index"}},{"Item":{"r1":{"S":"m"},"sk":{"S":"s"},"id":{"S":"i2"},"g":{"S":"g2"}},"TableName":"_capture_20261111_230_m"}]}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements": [{"Statement": "DELETE FROM \"_capture_20261111_230_m\".\"single\" WHERE id = 'i1' AND sk = 's'"}, {"Statement": "DELETE FROM \"_capture_20261111_230_m\" WHERE id = 'i1' AND sk = 's'"}]}"#,
        r#"{"Responses":[{"Error":{"Code":"ValidationError","Message":"This operation is not supported on an index"}},{"TableName":"_capture_20261111_230_m"}]}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements": [{"Statement": "UPDATE \"_capture_20261111_230_m\".\"single\" SET a = 1 WHERE id = 'i1' AND sk = 's'"}, {"Statement": "UPDATE \"_capture_20261111_230_m\" SET a = 1 WHERE id = 'i2' AND sk = 's'"}]}"#,
        r#"{"Responses":[{"Error":{"Code":"ValidationError","Message":"This operation is not supported on an index"}},{"TableName":"_capture_20261111_230_m"}]}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements": [{"Statement": "SELECT * FROM \"_capture_20261111_230_m\".\"hh_rr\" WHERE h1 = 'a' AND h2 = 'x'"}, {"Statement": "UPDATE \"_capture_20261111_230_m\" SET a = 1 WHERE id = 'i2' AND sk = 's'"}]}"#,
        r#"{"Responses":[{"Error":{"Code":"ValidationError","Message":"PartiQL statements cannot be executed on tables with multi-attribute HASH keys or multi-attribute RANGE keys."}},{"TableName":"_capture_20261111_230_m"}]}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements": [{"Statement": "SELECT * FROM \"_capture_20261111_230_m\".\"hh_rr\" WHERE h1 = 'a' AND h2 = 'x'"}, {"Statement": "SELECT * FROM \"_capture_20261111_230_m\" WHERE id = 'i2' AND sk = 's'"}]}"#,
        r#"{"Responses":[{"Error":{"Code":"ValidationError","Message":"PartiQL statements cannot be executed on tables with multi-attribute HASH keys or multi-attribute RANGE keys."}},{"Item":{"a":{"N":"1"},"r1":{"S":"m"},"sk":{"S":"s"},"id":{"S":"i2"},"g":{"S":"g2"}},"TableName":"_capture_20261111_230_m"}]}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements": [{"Statement": "SELECT * FROM \"_capture_20261111_230_m\".\"single\" WHERE g = 'g1'"}, {"Statement": "SELECT * FROM \"_capture_20261111_230_m\" WHERE id = 'i2' AND sk = 's'"}]}"#,
        r#"{"Responses":[{"Error":{"Code":"ValidationError","Message":"Select statements within BatchExecuteStatement must specify the primary key in the where clause."}},{"Item":{"a":{"N":"1"},"r1":{"S":"m"},"sk":{"S":"s"},"id":{"S":"i2"},"g":{"S":"g2"}},"TableName":"_capture_20261111_230_m"}]}"#,
    ),
    (
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "UPDATE \"_capture_20261111_230_m\".\"single\" SET a = 1 WHERE id = 'i1' AND sk = 's'"}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Validation failed in TransactStatements[0]: This operation is not supported on an index"}"#,
    ),
    (
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "UPDATE \"_capture_20261111_230_m\".\"single\" SET a = 1 WHERE id = 'i1' AND sk = 's'"}, {"Statement": "UPDATE \"_capture_20261111_230_m\" SET a = 1 WHERE id = 'i1' AND sk = 's'"}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Validation failed in TransactStatements[0]: This operation is not supported on an index"}"#,
    ),
    (
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "UPDATE \"_capture_20261111_230_m\" SET a = 1 WHERE id = 'i2' AND sk = 's'"}, {"Statement": "UPDATE \"_capture_20261111_230_m\".\"single\" SET a = 1 WHERE id = 'i1' AND sk = 's'"}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Validation failed in TransactStatements[1]: This operation is not supported on an index"}"#,
    ),
    (
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "DELETE FROM \"_capture_20261111_230_m\".\"single\" WHERE id = 'i1' AND sk = 's'"}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Validation failed in TransactStatements[0]: This operation is not supported on an index"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT * FROM \"_capture_20261111_230_m\" WHERE id = 'i1' AND sk = 's' AND attribute_exists(?)"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Number of parameters in request and statement don't match."}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT * FROM \"_capture_20261111_230_m\" WHERE id = 'i1' AND sk = 's' AND size(s) + 1 > size(true)"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Unsupported operator in Condition Expression. Operator: +"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT * FROM \"_capture_20261111_230_m\" WHERE id = 'i1' AND sk = 's' AND size(true) + 1 > size(s)"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Unsupported operator in Condition Expression. Operator: +"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "DELETE FROM \"_capture_20261111_230_m\" WHERE id = 'zz' AND sk = 'zz'", "NextToken": "abc"}"#,
        r#"{"Items":[]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "INSERT INTO \"_capture_20261111_230_m\" VALUE {'id': 'zz', 'sk': 'zz'}", "NextToken": "abc"}"#,
        r#"{"Items":[]}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "DELETE FROM \"_capture_20261111_230_m\" WHERE id = 'zz' AND sk = 'zz'"}"#,
        r#"{"Items":[]}"#,
    ),
];

/// Round eleven, captured in eu-west-2 against `INDEXED_TABLE`: the index
/// refusals against the parameter count, a COUNT projection and an operand.
const ROUND_ELEVEN: &[Row] = &[
    (
        "PutItem",
        r#"{"TableName": "_capture_20261111_230_m", "Item": {"id": {"S": "i1"}, "sk": {"S": "s"}, "g": {"S": "g1"}, "h1": {"S": "a"}, "h2": {"S": "x"}, "r1": {"S": "m"}, "r2": {"S": "n"}, "n": {"N": "5"}, "s": {"S": "hello"}}}"#,
        r#"{}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_m\".\"single\" SET a = ? WHERE id = 'i1' AND sk = 's'"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"This operation is not supported on an index"}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements": [{"Statement": "UPDATE \"_capture_20261111_230_m\".\"single\" SET a = ? WHERE id = 'i1' AND sk = 's'"}]}"#,
        r#"{"Responses":[{"Error":{"Code":"ValidationError","Message":"This operation is not supported on an index"}}]}"#,
    ),
    (
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "UPDATE \"_capture_20261111_230_m\".\"single\" SET a = ? WHERE id = 'i1' AND sk = 's'"}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Validation failed in TransactStatements[0]: This operation is not supported on an index"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "DELETE FROM \"_capture_20261111_230_m\".\"single\" WHERE id = ? AND sk = 's'"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"This operation is not supported on an index"}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements": [{"Statement": "DELETE FROM \"_capture_20261111_230_m\".\"single\" WHERE id = ? AND sk = 's'"}]}"#,
        r#"{"Responses":[{"Error":{"Code":"ValidationError","Message":"This operation is not supported on an index"}}]}"#,
    ),
    (
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "DELETE FROM \"_capture_20261111_230_m\".\"single\" WHERE id = ? AND sk = 's'"}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Validation failed in TransactStatements[0]: This operation is not supported on an index"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT * FROM \"_capture_20261111_230_m\".\"hh_rr\" WHERE h1 = ? AND h2 = 'x'"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"PartiQL statements cannot be executed on tables with multi-attribute HASH keys or multi-attribute RANGE keys."}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements": [{"Statement": "SELECT * FROM \"_capture_20261111_230_m\".\"hh_rr\" WHERE h1 = ? AND h2 = 'x'"}]}"#,
        r#"{"Responses":[{"Error":{"Code":"ValidationError","Message":"PartiQL statements cannot be executed on tables with multi-attribute HASH keys or multi-attribute RANGE keys."}}]}"#,
    ),
    (
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "SELECT * FROM \"_capture_20261111_230_m\".\"hh_rr\" WHERE h1 = ? AND h2 = 'x'"}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Validation failed in TransactStatements[0]: Number of parameters in request and statement don't match."}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT COUNT(*) FROM \"_capture_20261111_230_m\".\"hh_rr\" WHERE h1 = 'a' AND h2 = 'x'"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"PartiQL statements cannot be executed on tables with multi-attribute HASH keys or multi-attribute RANGE keys."}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements": [{"Statement": "SELECT COUNT(*) FROM \"_capture_20261111_230_m\".\"hh_rr\" WHERE h1 = 'a' AND h2 = 'x'"}]}"#,
        r#"{"Responses":[{"Error":{"Code":"ValidationError","Message":"PartiQL statements cannot be executed on tables with multi-attribute HASH keys or multi-attribute RANGE keys."}}]}"#,
    ),
    (
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "SELECT COUNT(*) FROM \"_capture_20261111_230_m\".\"hh_rr\" WHERE h1 = 'a' AND h2 = 'x'"}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Validation failed in TransactStatements[0]: Unexpected path component at 1:8:5"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT COUNT(*) FROM \"_capture_20261111_230_m\" WHERE id = 'i1' AND sk = 's'"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Unexpected path component at 1:8:5"}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements": [{"Statement": "SELECT COUNT(*) FROM \"_capture_20261111_230_m\" WHERE id = 'i1' AND sk = 's'"}]}"#,
        r#"{"Responses":[{"Error":{"Code":"ValidationError","Message":"Unexpected path component at 1:8:5"}}]}"#,
    ),
    (
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "SELECT COUNT(*) FROM \"_capture_20261111_230_m\" WHERE id = 'i1' AND sk = 's'"}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Validation failed in TransactStatements[0]: Unexpected path component at 1:8:5"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_m\".\"single\" SET a = 1 WHERE id = 'i1' AND sk = 's' AND size(true) > 1"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"This operation is not supported on an index"}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements": [{"Statement": "UPDATE \"_capture_20261111_230_m\".\"single\" SET a = 1 WHERE id = 'i1' AND sk = 's' AND size(true) > 1"}]}"#,
        r#"{"Responses":[{"Error":{"Code":"ValidationError","Message":"This operation is not supported on an index"}}]}"#,
    ),
    (
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "UPDATE \"_capture_20261111_230_m\".\"single\" SET a = 1 WHERE id = 'i1' AND sk = 's' AND size(true) > 1"}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Validation failed in TransactStatements[0]: This operation is not supported on an index"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT * FROM \"_capture_20261111_230_m\".\"hh_rr\" WHERE h1 = 'a' AND size(true) > 1"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"PartiQL statements cannot be executed on tables with multi-attribute HASH keys or multi-attribute RANGE keys."}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements": [{"Statement": "SELECT * FROM \"_capture_20261111_230_m\".\"hh_rr\" WHERE h1 = 'a' AND size(true) > 1"}]}"#,
        r#"{"Responses":[{"Error":{"Code":"ValidationError","Message":"PartiQL statements cannot be executed on tables with multi-attribute HASH keys or multi-attribute RANGE keys."}}]}"#,
    ),
    (
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "SELECT * FROM \"_capture_20261111_230_m\".\"hh_rr\" WHERE h1 = 'a' AND size(true) > 1"}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Validation failed in TransactStatements[0]: Incorrect operand type for operator or function; operator or function: size, operand type: BOOL"}"#,
    ),
];

/// Round twelve, captured in eu-west-2 against a table that doesn't exist.
const ROUND_TWELVE: &[Row] = &[
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT * FROM \"_capture_20261111_230_none\" WHERE id = 'i1' AND size(s, s)"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect number of operands for operator or function; operator or function: size, number of operands: 2"}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements": [{"Statement": "SELECT * FROM \"_capture_20261111_230_none\" WHERE id = 'i1' AND size(s, s)"}]}"#,
        r#"{"Responses":[{"Error":{"Code":"ValidationError","Message":"Incorrect number of operands for operator or function; operator or function: size, number of operands: 2"}}]}"#,
    ),
    (
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "SELECT * FROM \"_capture_20261111_230_none\" WHERE id = 'i1' AND size(s, s)"}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Validation failed in TransactStatements[0]: Incorrect number of operands for operator or function; operator or function: size, number of operands: 2"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT * FROM \"_capture_20261111_230_none\" WHERE id = 'i1' AND size(s, s) AND n = 1"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect number of operands for operator or function; operator or function: size, number of operands: 2"}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements": [{"Statement": "SELECT * FROM \"_capture_20261111_230_none\" WHERE id = 'i1' AND size(s, s) AND n = 1"}]}"#,
        r#"{"Responses":[{"Error":{"Code":"ValidationError","Message":"Incorrect number of operands for operator or function; operator or function: size, number of operands: 2"}}]}"#,
    ),
    (
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "SELECT * FROM \"_capture_20261111_230_none\" WHERE id = 'i1' AND size(s, s) AND n = 1"}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Validation failed in TransactStatements[0]: Incorrect number of operands for operator or function; operator or function: size, number of operands: 2"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT * FROM \"_capture_20261111_230_none\" WHERE id = 'i1' AND size(s, s) IS NULL"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"The left hand side argument to IS must be a Variable Reference, Literal, or Path"}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements": [{"Statement": "SELECT * FROM \"_capture_20261111_230_none\" WHERE id = 'i1' AND size(s, s) IS NULL"}]}"#,
        r#"{"Responses":[{"Error":{"Code":"ValidationError","Message":"The left hand side argument to IS must be a Variable Reference, Literal, or Path"}}]}"#,
    ),
    (
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "SELECT * FROM \"_capture_20261111_230_none\" WHERE id = 'i1' AND size(s, s) IS NULL"}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Validation failed in TransactStatements[0]: The left hand side argument to IS must be a Variable Reference, Literal, or Path"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT * FROM \"_capture_20261111_230_none\" WHERE id = 'i1' AND NOT size(s, s)"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect number of operands for operator or function; operator or function: size, number of operands: 2"}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements": [{"Statement": "SELECT * FROM \"_capture_20261111_230_none\" WHERE id = 'i1' AND NOT size(s, s)"}]}"#,
        r#"{"Responses":[{"Error":{"Code":"ValidationError","Message":"Incorrect number of operands for operator or function; operator or function: size, number of operands: 2"}}]}"#,
    ),
    (
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "SELECT * FROM \"_capture_20261111_230_none\" WHERE id = 'i1' AND NOT size(s, s)"}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Validation failed in TransactStatements[0]: Incorrect number of operands for operator or function; operator or function: size, number of operands: 2"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT * FROM \"_capture_20261111_230_none\" WHERE id = 'i1' AND size(s) + size(s, s) > 0"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Unsupported operator in Condition Expression. Operator: +"}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements": [{"Statement": "SELECT * FROM \"_capture_20261111_230_none\" WHERE id = 'i1' AND size(s) + size(s, s) > 0"}]}"#,
        r#"{"Responses":[{"Error":{"Code":"ValidationError","Message":"Unsupported operator in Condition Expression. Operator: +"}}]}"#,
    ),
    (
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "SELECT * FROM \"_capture_20261111_230_none\" WHERE id = 'i1' AND size(s) + size(s, s) > 0"}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Validation failed in TransactStatements[0]: Unsupported operator in Condition Expression. Operator: +"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT * FROM \"_capture_20261111_230_none\" WHERE id = 'i1' AND size(s, s) + 1 > 0"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Unsupported operator in Condition Expression. Operator: +"}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements": [{"Statement": "SELECT * FROM \"_capture_20261111_230_none\" WHERE id = 'i1' AND size(s, s) + 1 > 0"}]}"#,
        r#"{"Responses":[{"Error":{"Code":"ValidationError","Message":"Unsupported operator in Condition Expression. Operator: +"}}]}"#,
    ),
    (
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "SELECT * FROM \"_capture_20261111_230_none\" WHERE id = 'i1' AND size(s, s) + 1 > 0"}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Validation failed in TransactStatements[0]: Unsupported operator in Condition Expression. Operator: +"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT * FROM \"_capture_20261111_230_none\" WHERE id = 'i1' AND size(s)"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: AND, operand type: N"}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements": [{"Statement": "SELECT * FROM \"_capture_20261111_230_none\" WHERE id = 'i1' AND size(s)"}]}"#,
        r#"{"Responses":[{"Error":{"Code":"ValidationError","Message":"Incorrect operand type for operator or function; operator or function: AND, operand type: N"}}]}"#,
    ),
    (
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "SELECT * FROM \"_capture_20261111_230_none\" WHERE id = 'i1' AND size(s)"}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Validation failed in TransactStatements[0]: Incorrect operand type for operator or function; operator or function: AND, operand type: N"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT * FROM \"_capture_20261111_230_none\" WHERE id = 'i1' AND size(s) AND n = 1"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Incorrect operand type for operator or function; operator or function: AND, operand type: N"}"#,
    ),
    (
        "BatchExecuteStatement",
        r#"{"Statements": [{"Statement": "SELECT * FROM \"_capture_20261111_230_none\" WHERE id = 'i1' AND size(s) AND n = 1"}]}"#,
        r#"{"Responses":[{"Error":{"Code":"ValidationError","Message":"Incorrect operand type for operator or function; operator or function: AND, operand type: N"}}]}"#,
    ),
    (
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "SELECT * FROM \"_capture_20261111_230_none\" WHERE id = 'i1' AND size(s) AND n = 1"}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Validation failed in TransactStatements[0]: Incorrect operand type for operator or function; operator or function: AND, operand type: N"}"#,
    ),
];

/// Round thirteen, captured in eu-west-2 against a table that doesn't exist.
const ROUND_THIRTEEN: &[Row] = &[
    (
        "ExecuteStatement",
        r#"{"Statement": "DELETE FROM \"_capture_20261111_230_none\" WHERE id = 'zz' AND sk = 'zz'", "NextToken": ""}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value '' at 'nextToken' failed to satisfy constraint: Member must have length greater than or equal to 1"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT * FROM \"_capture_20261111_230_none\" WHERE id = 'zz'", "NextToken": ""}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value '' at 'nextToken' failed to satisfy constraint: Member must have length greater than or equal to 1"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "UPDATE \"_capture_20261111_230_none\" SET a = 1 WHERE id = 'zz' AND sk = 'zz'", "NextToken": ""}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value '' at 'nextToken' failed to satisfy constraint: Member must have length greater than or equal to 1"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "DELETE FROM \"_capture_20261111_230_none\" WHERE id = 'zz' AND sk = 'zz'", "NextToken": "abc"}"#,
        r#"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT * FROM \"_capture_20261111_230_none\" WHERE id = 'zz'", "NextToken": "abc"}"#,
        r#"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT * FROM \"_capture_20261111_230_none\" WHERE id = 'i1' AND 1 + size(s, s) > 0"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Unsupported operator in Condition Expression. Operator: +"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT * FROM \"_capture_20261111_230_none\" WHERE id = 'i1' AND (size(s, s) + 1) > 0"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Unsupported operator in Condition Expression. Operator: +"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT * FROM \"_capture_20261111_230_none\" WHERE id = 'i1' AND 1 + size(s) > 0"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Unsupported operator in Condition Expression. Operator: +"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT * FROM \"_capture_20261111_230_none\" WHERE id = 'i1' AND (size(s) + 1) > 0"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Unsupported operator in Condition Expression. Operator: +"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT * FROM \"_capture_20261111_230_none\" WHERE id = 'i1' AND n + 1 > 0"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Unsupported operator in Condition Expression. Operator: +"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT * FROM \"_capture_20261111_230_none\" WHERE id = 'i1' AND size(s, s) IS MISSING"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"The left hand side argument to IS must be a Variable Reference, Literal, or Path"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT * FROM \"_capture_20261111_230_none\" WHERE id = 'i1' AND size(s) IS NULL"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"The left hand side argument to IS must be a Variable Reference, Literal, or Path"}"#,
    ),
    (
        "ExecuteStatement",
        r#"{"Statement": "SELECT * FROM \"_capture_20261111_230_none\" WHERE id = 'i1' AND size(true) IS NULL"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"The left hand side argument to IS must be a Variable Reference, Literal, or Path"}"#,
    ),
];
