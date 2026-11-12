//! A request DynamoDB refuses with a constraint message echoes the member it
//! refused, in the Java model's `toString` form, which names all sixteen of a
//! value's fields and so runs to about twenty times the request's own size.
//! Dynoxide bounds the echo: a rejected request near the body limit must not
//! build an error message hundreds of megabytes long. The bound is Dynoxide's
//! own, well above any echo of an ordinary request; DynamoDB's behaviour for
//! echoes this size has not been captured.

use dynoxide::Database;
use dynoxide::actions::batch_execute_statement::{
    BatchExecuteStatementRequest, BatchStatementRequest,
};
use dynoxide::actions::batch_get_item::{BatchGetItemRequest, KeysAndAttributes};
use dynoxide::actions::batch_write_item::{BatchWriteItemRequest, PutRequest, WriteRequest};
use dynoxide::actions::execute_transaction::{ExecuteTransactionRequest, ParameterizedStatement};
use dynoxide::actions::transact_get_items::{
    TransactGet, TransactGetItem, TransactGetItemsRequest,
};
use dynoxide::actions::transact_write_items::{
    TransactPut, TransactWriteItem, TransactWriteItemsRequest,
};
use dynoxide::errors::DynoxideError;
use dynoxide::types::AttributeValue;
use std::collections::HashMap;

/// A little over the bound, so a test can tell a capped echo from an uncapped
/// one: the uncapped echoes below run to megabytes or tens of megabytes.
const BOUNDED: usize = 1_200_000;

/// The bound itself (Dynoxide's `ECHO_LIMIT`, 1 MiB). A cut echo runs to it and
/// then ends `...`, so the message is longer than this.
const LIMIT: usize = 1024 * 1024;

/// A capped echo reached the limit and was cut there: the message is longer
/// than the limit, shorter than [`BOUNDED`], and the echo ends `...` just
/// before the member it names.
fn assert_cut(message: &str) {
    assert!(
        message.len() > LIMIT,
        "the echo was cut short: {} bytes",
        message.len()
    );
    assert!(
        message.len() < BOUNDED,
        "the echo ran to {} bytes",
        message.len()
    );
    assert!(
        message.contains("...' at '"),
        "the cut echo doesn't end with ...: {}",
        &message[message.len().saturating_sub(200)..]
    );
}

fn message(err: DynoxideError) -> String {
    match err {
        DynoxideError::ValidationException(m) => m,
        other => panic!("expected a ValidationException, got {other:?}"),
    }
}

/// BatchWriteItem echoes the whole RequestItems map when a table name breaks
/// its constraints. One item of 200,000 small attributes is about 3 MB of
/// request and about 40 MB of echo.
#[test]
fn a_batch_write_echo_is_bounded() {
    let db = Database::memory().unwrap();
    let item: HashMap<String, AttributeValue> = (0..200_000)
        .map(|i| (format!("a{i}"), AttributeValue::N("1".to_string())))
        .collect();
    let mut request_items = HashMap::new();
    request_items.insert(
        "a!".to_string(),
        vec![WriteRequest {
            put_request: Some(PutRequest { item }),
            delete_request: None,
        }],
    );
    let err = db
        .batch_write_item(BatchWriteItemRequest {
            request_items,
            ..Default::default()
        })
        .unwrap_err();
    let message = message(err);
    assert!(
        message.starts_with("1 validation error detected: Value '{a!=[WriteRequest(putRequest="),
        "unexpected message start: {}",
        &message[..message.len().min(200)]
    );
    assert_cut(&message);
}

/// BatchExecuteStatement echoes the statement list when it holds more than 25.
/// Twenty-six statements of 2,000 parameters each are about 1.5 MB of request
/// and about 10 MB of echo.
#[test]
fn a_batch_statement_echo_is_bounded() {
    let db = Database::memory().unwrap();
    let statements: Vec<BatchStatementRequest> = (0..26)
        .map(|_| {
            let mut statement = BatchStatementRequest::default();
            statement.statement = "SELECT * FROM \"T\" WHERE pk = ?".to_string();
            statement.parameters = Some(
                (0..2_000)
                    .map(|_| AttributeValue::N("1".to_string()))
                    .collect(),
            );
            statement
        })
        .collect();
    let err = db
        .batch_execute_statement(BatchExecuteStatementRequest {
            statements,
            ..Default::default()
        })
        .unwrap_err();
    let message = message(err);
    assert!(
        message.starts_with("1 validation error detected: Value '[BatchStatementRequest("),
        "unexpected message start: {}",
        &message[..message.len().min(200)]
    );
    assert_cut(&message);
}

/// ExecuteTransaction echoes its statements when there are more than 100.
/// 101 statements of 2,000 number parameters each are about 1.5 MB of request
/// and about 34 MB of echo.
#[test]
fn a_transaction_statement_echo_is_bounded() {
    let db = Database::memory().unwrap();
    let statements: Vec<ParameterizedStatement> = (0..101)
        .map(|_| ParameterizedStatement {
            statement: "SELECT * FROM \"T\" WHERE pk = ?".to_string(),
            parameters: Some(
                (0..2_000)
                    .map(|_| AttributeValue::N("1".to_string()))
                    .collect(),
            ),
            ..Default::default()
        })
        .collect();
    let err = db
        .execute_transaction(ExecuteTransactionRequest {
            transact_statements: statements,
            ..Default::default()
        })
        .unwrap_err();
    let message = message(err);
    assert!(
        message.starts_with("1 validation error detected: Value '[ParameterizedStatement("),
        "unexpected message start: {}",
        &message[..message.len().min(200)]
    );
    assert_cut(&message);
}

/// BatchGetItem echoes its RequestItems map as JSON when a table name breaks
/// its constraints. That echo is no larger than the request, but it is cut at
/// the same bound: 100 keys of 20,000-character strings are about 2 MB.
#[test]
fn a_batch_get_echo_is_bounded() {
    let db = Database::memory().unwrap();
    let keys: Vec<HashMap<String, AttributeValue>> = (0..100)
        .map(|i| {
            HashMap::from([(
                "pk".to_string(),
                AttributeValue::S(format!("{i}{}", "x".repeat(20_000))),
            )])
        })
        .collect();
    let mut request_items = HashMap::new();
    request_items.insert(
        "a!".to_string(),
        KeysAndAttributes {
            keys,
            ..Default::default()
        },
    );
    let err = db
        .batch_get_item(BatchGetItemRequest {
            request_items,
            ..Default::default()
        })
        .unwrap_err();
    let message = message(err);
    assert!(
        message.starts_with("1 validation error detected: Value '{\"a!\":"),
        "unexpected message start: {}",
        &message[..message.len().min(200)]
    );
    assert_cut(&message);
}

/// A key 20,000 characters long, distinct for each `i`.
fn long_key(i: usize) -> HashMap<String, AttributeValue> {
    HashMap::from([(
        "pk".to_string(),
        AttributeValue::S(format!("{i}{}", "x".repeat(20_000))),
    )])
}

/// TransactWriteItems refuses more than 100 actions without listing them, in
/// the older form (captured eu-west-2 and us-east-1, 2026-11-11), so however
/// large the request the message stays short. A hundred and one puts of a
/// 20,000-character attribute are about 2 MB of request.
#[test]
fn a_transaction_write_over_100_actions_is_not_listed() {
    let db = Database::memory().unwrap();
    let transact_items = (0..101)
        .map(|i| TransactWriteItem {
            put: Some(TransactPut {
                table_name: "T".to_string(),
                item: long_key(i),
                ..Default::default()
            }),
            ..Default::default()
        })
        .collect();
    let err = db
        .transact_write_items(TransactWriteItemsRequest {
            transact_items,
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(
        message(err),
        "1 validation error detected: Value at 'TransactItems' failed to satisfy constraint: \
         Member must have length less than or equal to 100"
    );
}

/// TransactGetItems refuses more than 100 gets without listing them unless
/// the request holds a null name, and then lists them in the same form
/// (captured eu-west-2 and us-east-1, 2026-11-12). Asked for over HTTP, where
/// a null name can be sent, the listing is bounded.
#[test]
fn a_transaction_get_listing_is_bounded() {
    let db = Database::memory().unwrap();
    let transact_items = (0..101)
        .map(|i| TransactGetItem {
            get: TransactGet {
                table_name: "T".to_string(),
                key: long_key(i),
                ..Default::default()
            },
        })
        .collect();
    let err = db
        .transact_get_items(TransactGetItemsRequest {
            transact_items,
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(
        message(err),
        "1 validation error detected: Value at 'TransactItems' failed to satisfy constraint: \
         Member must have length less than or equal to 100"
    );

    let gets: Vec<serde_json::Value> = (0..101)
        .map(|i| {
            let mut get = serde_json::json!({"TableName": "T", "Key": long_key(i)});
            if i == 0 {
                get["ExpressionAttributeNames"] = serde_json::json!({"#x": null});
            }
            serde_json::json!({"Get": get})
        })
        .collect();
    let message = tokio::runtime::Runtime::new().unwrap().block_on(async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(dynoxide::server::serve_on(listener, db));
        let body: serde_json::Value = reqwest::Client::new()
            .post(url)
            .header("x-amz-target", "DynamoDB_20120810.TransactGetItems")
            .header("content-type", "application/x-amz-json-1.0")
            .header(
                "authorization",
                "AWS4-HMAC-SHA256 Credential=fakekey/20260101/us-east-1/dynamodb/aws4_request, \
                 SignedHeaders=host;x-amz-date;x-amz-target, Signature=fakesig",
            )
            .header("x-amz-date", "20260101T000000Z")
            .body(serde_json::json!({"TransactItems": gets}).to_string())
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        body["message"].as_str().unwrap().to_string()
    });
    assert!(
        message.starts_with("1 validation error detected: Value '[TransactGetItem("),
        "unexpected message start: {}",
        &message[..message.len().min(200)]
    );
    assert_cut(&message);
}
