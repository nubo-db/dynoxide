//! A BatchGetItem or BatchWriteItem whose `RequestItems` is missing, null or
//! malformed: a table's entry with no `Keys`, a write request with no `Item`
//! or `Key`, a null where a table, a key or a write request belongs, a member
//! of the wrong JSON type, and a write request holding both a put and a
//! delete; and how those faults sit beside the request's other faults.
//!
//! The rows are the requests sent to DynamoDB in eu-west-2 and us-east-1, in
//! the order they were sent, with the answers DynamoDB gave, against tables
//! that don't exist: a first set on 11 and 12 November 2026 and a second on
//! 13 November 2026. They are data captured from DynamoDB, not written by
//! hand. Where the regions gave different answers both are kept, and Dynoxide
//! follows eu-west-2. Answers are compared as parsed JSON, since DynamoDB and
//! Dynoxide order members differently, along with the HTTP status.

#![cfg(feature = "http-server")]

use dynoxide::Database;
use serde_json::Value;

/// A captured row: the operation, the request body, eu-west-2's answer, and
/// us-east-1's where it differs.
struct Row {
    operation: &'static str,
    body: &'static str,
    answer: &'static str,
    us_east_1: Option<&'static str>,
}

/// A row left out by name: the operation and body it is found by, the answer
/// it gets here today and the reason. It is still sent, must still differ
/// from DynamoDB's answer and must give the answer recorded here.
struct LeftOut {
    operation: &'static str,
    body: &'static str,
    answer: &'static str,
    reason: &'static str,
}

/// Why a row whose answer echoes attribute values is left out: eu-west-2
/// printed each one as an object hash in that answer, and Dynoxide prints the
/// Java model's fields, as us-east-1 did. The regions swapped forms between
/// rows of the same set.
const HASHED_ECHO: &str = "eu-west-2 echoed each attribute value as an object hash";

async fn start_server() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let db = Database::memory().unwrap();
    tokio::spawn(async move {
        dynoxide::server::serve_on(listener, db).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    url
}

/// The HTTP status and the body of the answer to a request.
async fn send(url: &str, operation: &str, body: &str) -> (u16, Value) {
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

/// The status DynamoDB sends with an answer: 400 for an error, 200 otherwise.
fn status_of(answer: &Value) -> u16 {
    if answer.get("__type").is_some() {
        400
    } else {
        200
    }
}

/// Send every row, in order, to one server, and list each answer that differs
/// from eu-west-2's. A row left out by name must keep the answer recorded for
/// it. Returns how many rows were sent, compared and left out.
async fn replay(rows: &[Row], left_out_rows: &[LeftOut]) -> (usize, usize, usize) {
    let url = start_server().await;
    let mut wrong = Vec::new();
    let (mut sent, mut compared, mut left_out) = (0, 0, 0);
    for row in rows {
        let expected: Value = serde_json::from_str(row.answer).unwrap();
        let other: Option<Value> = row.us_east_1.map(|a| serde_json::from_str(a).unwrap());
        if let Some(other) = &other {
            assert_ne!(&expected, other, "the regions' answers must differ");
        }
        let (status, got) = send(&url, row.operation, row.body).await;
        sent += 1;
        if let Some(l) = left_out_rows
            .iter()
            .find(|l| l.operation == row.operation && l.body == row.body)
        {
            left_out += 1;
            let pinned: Value = serde_json::from_str(l.answer).unwrap();
            if got == expected {
                wrong.push(format!(
                    "{} {} now answers as DynamoDB did: drop it from the rows left out",
                    row.operation, row.body
                ));
            } else if status != status_of(&pinned) || got != pinned {
                wrong.push(format!(
                    "{} {} (left out: {})\n  expected {} {pinned}\n  got      {status} {got}",
                    row.operation,
                    row.body,
                    l.reason,
                    status_of(&pinned)
                ));
            }
            continue;
        }
        compared += 1;
        if status != status_of(&expected) || got != expected {
            let region = match &other {
                Some(other) if *other == got => " (us-east-1's answer)",
                _ => "",
            };
            wrong.push(format!(
                "{} {}\n  expected {} {expected}\n  got      {status} {got}{region}",
                row.operation,
                row.body,
                status_of(&expected)
            ));
        }
    }
    println!("sent {sent} rows, compared {compared}, left out {left_out}");
    assert!(
        wrong.is_empty(),
        "{} of {sent} rows differ:\n{}",
        wrong.len(),
        wrong.join("\n")
    );
    (sent, compared, left_out)
}

/// Each row left out names a row that is sent.
fn assert_left_out_rows_are_sent(rows: &[Row], left_out_rows: &[LeftOut]) {
    for l in left_out_rows {
        assert!(
            rows.iter()
                .any(|row| row.operation == l.operation && row.body == l.body),
            "{} {} is left out but not sent",
            l.operation,
            l.body
        );
    }
}

/// The first set, captured on 11 and 12 November 2026: each malformed shape
/// on its own, or beside one other fault.
#[tokio::test]
async fn malformed_request_items_answer_as_dynamodb_did() {
    assert_eq!(replay(ROWS, LEFT_OUT).await, (50, 50, 0));
}

/// The second set, captured on 13 November 2026: how those faults sit beside
/// a bad `ReturnConsumedCapacity`, a table name that breaks its constraints,
/// an empty or overlong list and each other.
#[tokio::test]
async fn faults_beside_other_faults_answer_as_dynamodb_did() {
    assert_eq!(replay(SECOND_ROWS, SECOND_LEFT_OUT).await, (31, 30, 1));
}

#[test]
fn every_row_left_out_is_sent() {
    assert_left_out_rows_are_sent(ROWS, LEFT_OUT);
    assert_left_out_rows_are_sent(SECOND_ROWS, SECOND_LEFT_OUT);
}

const ROWS: &[Row] = &[
    Row {
        operation: "BatchGetItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": {}}}"##,
        answer: r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value null at 'requestItems._capture_20261111_237_none.member.keys' failed to satisfy constraint: Member must not be null"}"##,
        us_east_1: None,
    },
    Row {
        operation: "BatchGetItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": {"Keys": "x"}}}"##,
        answer: r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected field type"}"##,
        us_east_1: None,
    },
    Row {
        operation: "BatchGetItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": {"Keys": 5}}}"##,
        answer: r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected field type"}"##,
        us_east_1: None,
    },
    Row {
        operation: "BatchGetItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": {"Keys": {}}}}"##,
        answer: r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Start of structure or map found where not expected"}"##,
        us_east_1: None,
    },
    Row {
        operation: "BatchGetItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": {"Keys": null}}}"##,
        answer: r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value null at 'requestItems._capture_20261111_237_none.member.keys' failed to satisfy constraint: Member must not be null"}"##,
        us_east_1: None,
    },
    Row {
        operation: "BatchGetItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": {"Keys": []}}}"##,
        answer: r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'RequestItems._capture_20261111_237_none.member.Keys' failed to satisfy constraint: Member must have length greater than or equal to 1"}"##,
        us_east_1: None,
    },
    Row {
        operation: "BatchGetItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": {"Keys": ["x"]}}}"##,
        answer: r##"{"__type":"com.amazon.coral.service#SerializationException"}"##,
        us_east_1: None,
    },
    Row {
        operation: "BatchGetItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": {"Keys": [5]}}}"##,
        answer: r##"{"__type":"com.amazon.coral.service#SerializationException"}"##,
        us_east_1: None,
    },
    Row {
        operation: "BatchGetItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": {"Keys": [null]}}}"##,
        answer: r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The list of keys in RequestItems for BatchGetItem may not contain null or empty values: _capture_20261111_237_none has one or more empty keys"}"##,
        us_east_1: None,
    },
    Row {
        operation: "BatchGetItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": {"Keys": [{"pk": {"S": "a"}}], "ProjectionExpression": 5}}}"##,
        answer: r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"NUMBER_VALUE cannot be converted to String"}"##,
        us_east_1: None,
    },
    Row {
        operation: "BatchGetItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": {"Keys": [{"pk": {"S": "a"}}], "ExpressionAttributeNames": "x"}}}"##,
        answer: r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected field type"}"##,
        us_east_1: None,
    },
    Row {
        operation: "BatchGetItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": {"Keys": [{"pk": {"S": "a"}}], "ExpressionAttributeNames": {"#a": 5}}}}"##,
        answer: r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"NUMBER_VALUE cannot be converted to String"}"##,
        us_east_1: None,
    },
    Row {
        operation: "BatchGetItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": {"Keys": [{"pk": {"S": "a"}}], "AttributesToGet": "x"}}}"##,
        answer: r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected field type"}"##,
        us_east_1: None,
    },
    Row {
        operation: "BatchGetItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": "x"}}"##,
        answer: r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected value type in payload"}"##,
        us_east_1: None,
    },
    Row {
        operation: "BatchGetItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": 5}}"##,
        answer: r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected value type in payload"}"##,
        us_east_1: None,
    },
    Row {
        operation: "BatchGetItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": []}}"##,
        answer: r##"{"__type":"com.amazon.coral.service#SerializationException"}"##,
        us_east_1: None,
    },
    Row {
        operation: "BatchGetItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": null}}"##,
        answer: r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The list of keys in RequestItems for BatchGetItem is required: _capture_20261111_237_none has empty list"}"##,
        us_east_1: None,
    },
    Row {
        operation: "BatchGetItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": {"keys": [{"pk": {"S": "a"}}]}}}"##,
        answer: r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value null at 'requestItems._capture_20261111_237_none.member.keys' failed to satisfy constraint: Member must not be null"}"##,
        us_east_1: None,
    },
    Row {
        operation: "BatchGetItem",
        body: r##"{"RequestItems": "x"}"##,
        answer: r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected field type"}"##,
        us_east_1: None,
    },
    Row {
        operation: "BatchGetItem",
        body: r##"{"RequestItems": []}"##,
        answer: r##"{"__type":"com.amazon.coral.service#SerializationException"}"##,
        us_east_1: None,
    },
    Row {
        operation: "BatchGetItem",
        body: r##"{"RequestItems": null}"##,
        answer: r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The requestItems parameter is required for BatchGetItem"}"##,
        us_east_1: None,
    },
    Row {
        operation: "BatchGetItem",
        body: r##"{"RequestItems": {}}"##,
        answer: r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'RequestItems' failed to satisfy constraint: Member must have length greater than or equal to 1"}"##,
        us_east_1: None,
    },
    Row {
        operation: "BatchGetItem",
        body: r##"{}"##,
        answer: r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The requestItems parameter is required for BatchGetItem"}"##,
        us_east_1: None,
    },
    Row {
        operation: "BatchGetItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": {}}, "ReturnConsumedCapacity": "BOGUS"}"##,
        answer: r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value null at 'requestItems._capture_20261111_237_none.member.keys' failed to satisfy constraint: Member must not be null; Value 'BOGUS' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"}"##,
        us_east_1: None,
    },
    Row {
        operation: "BatchGetItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": {"Keys": "x"}}, "ReturnConsumedCapacity": "BOGUS"}"##,
        answer: r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected field type"}"##,
        us_east_1: None,
    },
    Row {
        operation: "BatchGetItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": {}, "_capture_20261111_237_other": {}}}"##,
        answer: r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value null at 'requestItems._capture_20261111_237_none.member.keys' failed to satisfy constraint: Member must not be null; Value null at 'requestItems._capture_20261111_237_other.member.keys' failed to satisfy constraint: Member must not be null"}"##,
        us_east_1: None,
    },
    Row {
        operation: "BatchWriteItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": [{"PutRequest": {}}]}}"##,
        answer: r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value null at 'requestItems._capture_20261111_237_none.member.1.member.putRequest.item' failed to satisfy constraint: Member must not be null"}"##,
        us_east_1: None,
    },
    Row {
        operation: "BatchWriteItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": [{"DeleteRequest": {}}]}}"##,
        answer: r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value null at 'requestItems._capture_20261111_237_none.member.1.member.deleteRequest.key' failed to satisfy constraint: Member must not be null"}"##,
        us_east_1: None,
    },
    Row {
        operation: "BatchWriteItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": [{"PutRequest": {"Item": null}}]}}"##,
        answer: r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value null at 'requestItems._capture_20261111_237_none.member.1.member.putRequest.item' failed to satisfy constraint: Member must not be null"}"##,
        us_east_1: None,
    },
    Row {
        operation: "BatchWriteItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": [{"DeleteRequest": {"Key": null}}]}}"##,
        answer: r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value null at 'requestItems._capture_20261111_237_none.member.1.member.deleteRequest.key' failed to satisfy constraint: Member must not be null"}"##,
        us_east_1: None,
    },
    Row {
        operation: "BatchWriteItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": [{"PutRequest": "x"}]}}"##,
        answer: r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected field type"}"##,
        us_east_1: None,
    },
    Row {
        operation: "BatchWriteItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": [{"DeleteRequest": 5}]}}"##,
        answer: r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected field type"}"##,
        us_east_1: None,
    },
    Row {
        operation: "BatchWriteItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": [{}]}}"##,
        answer: r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes"}"##,
        us_east_1: None,
    },
    Row {
        operation: "BatchWriteItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": ["x"]}}"##,
        answer: r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected value type in payload"}"##,
        us_east_1: None,
    },
    Row {
        operation: "BatchWriteItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": [5]}}"##,
        answer: r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected value type in payload"}"##,
        us_east_1: None,
    },
    Row {
        operation: "BatchWriteItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": [null]}}"##,
        answer: r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The batch write request list for a table cannot be null or empty"}"##,
        us_east_1: None,
    },
    Row {
        operation: "BatchWriteItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": []}}"##,
        answer: r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'RequestItems._capture_20261111_237_none.member' failed to satisfy constraint: Member must have length greater than or equal to 1"}"##,
        us_east_1: Some(
            r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value '{_capture_20261111_237_none=[]}' at 'requestItems' failed to satisfy constraint: Map value must satisfy constraint: [Member must have length less than or equal to 25, Member must have length greater than or equal to 1]"}"##,
        ),
    },
    Row {
        operation: "BatchWriteItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": "x"}}"##,
        answer: r##"{"__type":"com.amazon.coral.service#SerializationException"}"##,
        us_east_1: None,
    },
    Row {
        operation: "BatchWriteItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": 5}}"##,
        answer: r##"{"__type":"com.amazon.coral.service#SerializationException"}"##,
        us_east_1: None,
    },
    Row {
        operation: "BatchWriteItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": {}}}"##,
        answer: r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Start of structure or map found where not expected"}"##,
        us_east_1: None,
    },
    Row {
        operation: "BatchWriteItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": null}}"##,
        answer: r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The batch write request list for a table cannot be null or empty: _capture_20261111_237_none"}"##,
        us_east_1: None,
    },
    Row {
        operation: "BatchWriteItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": [{"PutRequest": {"Item": "x"}}]}}"##,
        answer: r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected field type"}"##,
        us_east_1: None,
    },
    Row {
        operation: "BatchWriteItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": [{"DeleteRequest": {"Key": []}}]}}"##,
        answer: r##"{"__type":"com.amazon.coral.service#SerializationException"}"##,
        us_east_1: None,
    },
    Row {
        operation: "BatchWriteItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": [{"PutRequest": {"Item": {"pk": {"S": "a"}}}, "DeleteRequest": {"Key": {"pk": {"S": "a"}}}}]}}"##,
        answer: r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes"}"##,
        us_east_1: None,
    },
    Row {
        operation: "BatchWriteItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": [{"putRequest": {"Item": {"pk": {"S": "a"}}}}]}}"##,
        answer: r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes"}"##,
        us_east_1: None,
    },
    Row {
        operation: "BatchWriteItem",
        body: r##"{}"##,
        answer: r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The requestItems parameter is required for BatchWriteItem"}"##,
        us_east_1: None,
    },
    Row {
        operation: "BatchWriteItem",
        body: r##"{"RequestItems": {}}"##,
        answer: r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'RequestItems' failed to satisfy constraint: Member must have length greater than or equal to 1"}"##,
        us_east_1: Some(
            r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The requestItems parameter is required for BatchWriteItem"}"##,
        ),
    },
    Row {
        operation: "BatchWriteItem",
        body: r##"{"RequestItems": "x"}"##,
        answer: r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected field type"}"##,
        us_east_1: None,
    },
    Row {
        operation: "BatchWriteItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": [{"PutRequest": {}}]}, "ReturnConsumedCapacity": "BOGUS"}"##,
        answer: r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value null at 'requestItems._capture_20261111_237_none.member.1.member.putRequest.item' failed to satisfy constraint: Member must not be null; Value 'BOGUS' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"}"##,
        us_east_1: None,
    },
    Row {
        operation: "BatchWriteItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": [{"PutRequest": {}}]}, "ReturnItemCollectionMetrics": "BOGUS"}"##,
        answer: r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value null at 'requestItems._capture_20261111_237_none.member.1.member.putRequest.item' failed to satisfy constraint: Member must not be null; Value 'BOGUS' at 'returnItemCollectionMetrics' failed to satisfy constraint: Member must satisfy enum value set: [SIZE, NONE]"}"##,
        us_east_1: None,
    },
];

const LEFT_OUT: &[LeftOut] = &[];

const SECOND_ROWS: &[Row] = &[
    Row {
        operation: "BatchWriteItem",
        body: r#"{"RequestItems":{"_capture_20261113_sweep_none":[]},"ReturnConsumedCapacity":"BOGUS"}"#,
        answer: r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'RequestItems._capture_20261113_sweep_none.member' failed to satisfy constraint: Member must have length greater than or equal to 1"}"#,
        us_east_1: Some(
            r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value '{_capture_20261113_sweep_none=[]}' at 'requestItems' failed to satisfy constraint: Map value must satisfy constraint: [Member must have length less than or equal to 25, Member must have length greater than or equal to 1]; Value 'BOGUS' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"}"#,
        ),
    },
    Row {
        operation: "BatchWriteItem",
        body: r#"{"RequestItems":{"_capture_20261113_sweep_none":[]},"ReturnItemCollectionMetrics":"BOGUS"}"#,
        answer: r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'RequestItems._capture_20261113_sweep_none.member' failed to satisfy constraint: Member must have length greater than or equal to 1"}"#,
        us_east_1: Some(
            r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value '{_capture_20261113_sweep_none=[]}' at 'requestItems' failed to satisfy constraint: Map value must satisfy constraint: [Member must have length less than or equal to 25, Member must have length greater than or equal to 1]; Value 'BOGUS' at 'returnItemCollectionMetrics' failed to satisfy constraint: Member must satisfy enum value set: [SIZE, NONE]"}"#,
        ),
    },
    Row {
        operation: "BatchWriteItem",
        body: r#"{"RequestItems":{"bad!name":[]}}"#,
        answer: r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'RequestItems.bad!name.member' failed to satisfy constraint: Member must have length greater than or equal to 1"}"#,
        us_east_1: Some(
            r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value '{bad!name=[]}' at 'requestItems' failed to satisfy constraint: Map value must satisfy constraint: [Member must have length less than or equal to 25, Member must have length greater than or equal to 1]"}"#,
        ),
    },
    Row {
        operation: "BatchWriteItem",
        body: r#"{"RequestItems":{"_capture_20261113_sweep_none":[],"_capture_20261113_sweep_other":[]}}"#,
        answer: r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value at 'RequestItems._capture_20261113_sweep_other.member' failed to satisfy constraint: Member must have length greater than or equal to 1; Value at 'RequestItems._capture_20261113_sweep_none.member' failed to satisfy constraint: Member must have length greater than or equal to 1"}"#,
        us_east_1: Some(
            r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value '{_capture_20261113_sweep_none=[], _capture_20261113_sweep_other=[]}' at 'requestItems' failed to satisfy constraint: Map value must satisfy constraint: [Member must have length less than or equal to 25, Member must have length greater than or equal to 1]"}"#,
        ),
    },
    Row {
        operation: "BatchWriteItem",
        body: r#"{"RequestItems":{"_capture_20261113_sweep_none":[],"_capture_20261113_sweep_other":[{"PutRequest":{"Item":{"pk":{"S":"k0"}}}},{"PutRequest":{"Item":{"pk":{"S":"k1"}}}},{"PutRequest":{"Item":{"pk":{"S":"k2"}}}},{"PutRequest":{"Item":{"pk":{"S":"k3"}}}},{"PutRequest":{"Item":{"pk":{"S":"k4"}}}},{"PutRequest":{"Item":{"pk":{"S":"k5"}}}},{"PutRequest":{"Item":{"pk":{"S":"k6"}}}},{"PutRequest":{"Item":{"pk":{"S":"k7"}}}},{"PutRequest":{"Item":{"pk":{"S":"k8"}}}},{"PutRequest":{"Item":{"pk":{"S":"k9"}}}},{"PutRequest":{"Item":{"pk":{"S":"k10"}}}},{"PutRequest":{"Item":{"pk":{"S":"k11"}}}},{"PutRequest":{"Item":{"pk":{"S":"k12"}}}},{"PutRequest":{"Item":{"pk":{"S":"k13"}}}},{"PutRequest":{"Item":{"pk":{"S":"k14"}}}},{"PutRequest":{"Item":{"pk":{"S":"k15"}}}},{"PutRequest":{"Item":{"pk":{"S":"k16"}}}},{"PutRequest":{"Item":{"pk":{"S":"k17"}}}},{"PutRequest":{"Item":{"pk":{"S":"k18"}}}},{"PutRequest":{"Item":{"pk":{"S":"k19"}}}},{"PutRequest":{"Item":{"pk":{"S":"k20"}}}},{"PutRequest":{"Item":{"pk":{"S":"k21"}}}},{"PutRequest":{"Item":{"pk":{"S":"k22"}}}},{"PutRequest":{"Item":{"pk":{"S":"k23"}}}},{"PutRequest":{"Item":{"pk":{"S":"k24"}}}},{"PutRequest":{"Item":{"pk":{"S":"k25"}}}}]}}"#,
        answer: r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value at 'RequestItems._capture_20261113_sweep_other.member' failed to satisfy constraint: Member must have length less than or equal to 25; Value at 'RequestItems._capture_20261113_sweep_none.member' failed to satisfy constraint: Member must have length greater than or equal to 1"}"#,
        us_east_1: Some(
            r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value '{_capture_20261113_sweep_none=[], _capture_20261113_sweep_other=[WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k0, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k1, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k2, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k3, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k4, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k5, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k6, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k7, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k8, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k9, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k10, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k11, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k12, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k13, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k14, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k15, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k16, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k17, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k18, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k19, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k20, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k21, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k22, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k23, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k24, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k25, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null)]}' at 'requestItems' failed to satisfy constraint: Map value must satisfy constraint: [Member must have length less than or equal to 25, Member must have length greater than or equal to 1]"}"#,
        ),
    },
    Row {
        operation: "BatchWriteItem",
        body: r#"{"RequestItems":{"_capture_20261113_sweep_none":[{"PutRequest":{"Item":{"pk":{"S":"k0"}}}},{"PutRequest":{"Item":{"pk":{"S":"k1"}}}},{"PutRequest":{"Item":{"pk":{"S":"k2"}}}},{"PutRequest":{"Item":{"pk":{"S":"k3"}}}},{"PutRequest":{"Item":{"pk":{"S":"k4"}}}},{"PutRequest":{"Item":{"pk":{"S":"k5"}}}},{"PutRequest":{"Item":{"pk":{"S":"k6"}}}},{"PutRequest":{"Item":{"pk":{"S":"k7"}}}},{"PutRequest":{"Item":{"pk":{"S":"k8"}}}},{"PutRequest":{"Item":{"pk":{"S":"k9"}}}},{"PutRequest":{"Item":{"pk":{"S":"k10"}}}},{"PutRequest":{"Item":{"pk":{"S":"k11"}}}},{"PutRequest":{"Item":{"pk":{"S":"k12"}}}},{"PutRequest":{"Item":{"pk":{"S":"k13"}}}},{"PutRequest":{"Item":{"pk":{"S":"k14"}}}},{"PutRequest":{"Item":{"pk":{"S":"k15"}}}},{"PutRequest":{"Item":{"pk":{"S":"k16"}}}},{"PutRequest":{"Item":{"pk":{"S":"k17"}}}},{"PutRequest":{"Item":{"pk":{"S":"k18"}}}},{"PutRequest":{"Item":{"pk":{"S":"k19"}}}},{"PutRequest":{"Item":{"pk":{"S":"k20"}}}},{"PutRequest":{"Item":{"pk":{"S":"k21"}}}},{"PutRequest":{"Item":{"pk":{"S":"k22"}}}},{"PutRequest":{"Item":{"pk":{"S":"k23"}}}},{"PutRequest":{"Item":{"pk":{"S":"k24"}}}},{"PutRequest":{"Item":{"pk":{"S":"k25"}}}}]}}"#,
        answer: r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'RequestItems._capture_20261113_sweep_none.member' failed to satisfy constraint: Member must have length less than or equal to 25"}"#,
        us_east_1: Some(
            r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value '{_capture_20261113_sweep_none=[WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k0, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k1, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k2, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k3, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k4, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k5, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k6, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k7, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k8, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k9, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k10, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k11, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k12, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k13, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k14, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k15, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k16, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k17, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k18, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k19, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k20, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k21, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k22, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k23, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k24, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k25, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null)]}' at 'requestItems' failed to satisfy constraint: Map value must satisfy constraint: [Member must have length less than or equal to 25, Member must have length greater than or equal to 1]"}"#,
        ),
    },
    Row {
        operation: "BatchWriteItem",
        body: r#"{"RequestItems":{"_capture_20261113_sweep_none":[{"PutRequest":{"Item":{"pk":{"S":"k0"}}}},{"PutRequest":{"Item":{"pk":{"S":"k1"}}}},{"PutRequest":{"Item":{"pk":{"S":"k2"}}}},{"PutRequest":{"Item":{"pk":{"S":"k3"}}}},{"PutRequest":{"Item":{"pk":{"S":"k4"}}}},{"PutRequest":{"Item":{"pk":{"S":"k5"}}}},{"PutRequest":{"Item":{"pk":{"S":"k6"}}}},{"PutRequest":{"Item":{"pk":{"S":"k7"}}}},{"PutRequest":{"Item":{"pk":{"S":"k8"}}}},{"PutRequest":{"Item":{"pk":{"S":"k9"}}}},{"PutRequest":{"Item":{"pk":{"S":"k10"}}}},{"PutRequest":{"Item":{"pk":{"S":"k11"}}}},{"PutRequest":{"Item":{"pk":{"S":"k12"}}}},{"PutRequest":{"Item":{"pk":{"S":"k13"}}}},{"PutRequest":{"Item":{"pk":{"S":"k14"}}}},{"PutRequest":{"Item":{"pk":{"S":"k15"}}}},{"PutRequest":{"Item":{"pk":{"S":"k16"}}}},{"PutRequest":{"Item":{"pk":{"S":"k17"}}}},{"PutRequest":{"Item":{"pk":{"S":"k18"}}}},{"PutRequest":{"Item":{"pk":{"S":"k19"}}}},{"PutRequest":{"Item":{"pk":{"S":"k20"}}}},{"PutRequest":{"Item":{"pk":{"S":"k21"}}}},{"PutRequest":{"Item":{"pk":{"S":"k22"}}}},{"PutRequest":{"Item":{"pk":{"S":"k23"}}}},{"PutRequest":{"Item":{"pk":{"S":"k24"}}}},{"PutRequest":{"Item":{"pk":{"S":"k25"}}}}]},"ReturnConsumedCapacity":"BOGUS"}"#,
        answer: r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'RequestItems._capture_20261113_sweep_none.member' failed to satisfy constraint: Member must have length less than or equal to 25"}"#,
        us_east_1: Some(
            r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value '{_capture_20261113_sweep_none=[WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k0, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k1, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k2, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k3, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k4, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k5, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k6, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k7, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k8, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k9, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k10, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k11, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k12, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k13, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k14, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k15, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k16, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k17, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k18, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k19, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k20, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k21, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k22, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k23, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k24, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k25, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null)]}' at 'requestItems' failed to satisfy constraint: Map value must satisfy constraint: [Member must have length less than or equal to 25, Member must have length greater than or equal to 1]; Value 'BOGUS' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"}"#,
        ),
    },
    Row {
        operation: "BatchWriteItem",
        body: r#"{"RequestItems":{"bad!name":[{"PutRequest":{"Item":{"pk":{"S":"k1"}}}}]}}"#,
        answer: r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value '{\"bad!name\":[{\"PutRequest\":{\"Item\":{\"pk\":{\"S\":\"k1\"}}}}]}' at 'requestItems' failed to satisfy constraint: Map keys must satisfy constraint: [Member must have length less than or equal to 255, Member must have length greater than or equal to 3, Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+]"}"#,
        us_east_1: Some(
            r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value '{bad!name=[WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k1, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null)]}' at 'requestItems' failed to satisfy constraint: Map keys must satisfy constraint: [Member must have length less than or equal to 255, Member must have length greater than or equal to 3, Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+]"}"#,
        ),
    },
    Row {
        operation: "BatchWriteItem",
        body: r#"{"RequestItems":{"bad!name":[{"PutRequest":{"Item":{"pk":{"S":"k1"}}}}]},"ReturnConsumedCapacity":"BOGUS"}"#,
        answer: r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value '{\"bad!name\":[{\"PutRequest\":{\"Item\":{\"pk\":{\"S\":\"k1\"}}}}]}' at 'requestItems' failed to satisfy constraint: Map keys must satisfy constraint: [Member must have length less than or equal to 255, Member must have length greater than or equal to 3, Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+]; Value 'BOGUS' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"}"#,
        us_east_1: Some(
            r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value 'BOGUS' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"}"#,
        ),
    },
    Row {
        operation: "BatchGetItem",
        body: r#"{"ReturnConsumedCapacity":"BOGUS"}"#,
        answer: r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"The requestItems parameter is required for BatchGetItem"}"#,
        us_east_1: None,
    },
    Row {
        operation: "BatchGetItem",
        body: r#"{"RequestItems":null,"ReturnConsumedCapacity":"BOGUS"}"#,
        answer: r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"The requestItems parameter is required for BatchGetItem"}"#,
        us_east_1: None,
    },
    Row {
        operation: "BatchGetItem",
        body: r#"{"RequestItems":{"_capture_20261113_sweep_none":null},"ReturnConsumedCapacity":"BOGUS"}"#,
        answer: r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value 'BOGUS' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"}"#,
        us_east_1: None,
    },
    Row {
        operation: "BatchWriteItem",
        body: r#"{"ReturnConsumedCapacity":"BOGUS"}"#,
        answer: r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"The requestItems parameter is required for BatchWriteItem"}"#,
        us_east_1: None,
    },
    Row {
        operation: "BatchWriteItem",
        body: r#"{"RequestItems":null,"ReturnConsumedCapacity":"BOGUS"}"#,
        answer: r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"The requestItems parameter is required for BatchWriteItem"}"#,
        us_east_1: None,
    },
    Row {
        operation: "BatchWriteItem",
        body: r#"{"RequestItems":{"_capture_20261113_sweep_none":null},"ReturnConsumedCapacity":"BOGUS"}"#,
        answer: r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value 'BOGUS' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"}"#,
        us_east_1: None,
    },
    Row {
        operation: "BatchGetItem",
        body: r#"{"RequestItems":{"_capture_20261113_sweep_none":{"Keys":[null]}},"ReturnConsumedCapacity":"BOGUS"}"#,
        answer: r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value 'BOGUS' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"}"#,
        us_east_1: None,
    },
    Row {
        operation: "BatchWriteItem",
        body: r#"{"RequestItems":{"_capture_20261113_sweep_none":[null]},"ReturnConsumedCapacity":"BOGUS"}"#,
        answer: r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value 'BOGUS' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"}"#,
        us_east_1: None,
    },
    Row {
        operation: "BatchGetItem",
        body: r#"{"RequestItems":{"_capture_20261113_sweep_none":{},"_capture_20261113_sweep_other":{"Keys":[]}}}"#,
        answer: r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value null at 'requestItems._capture_20261113_sweep_none.member.keys' failed to satisfy constraint: Member must not be null; Value '[]' at 'requestItems._capture_20261113_sweep_other.member.keys' failed to satisfy constraint: Member must have length greater than or equal to 1"}"#,
        us_east_1: None,
    },
    Row {
        operation: "BatchGetItem",
        body: r#"{"RequestItems":{"_capture_20261113_sweep_other":{"Keys":[]},"_capture_20261113_sweep_none":{}}}"#,
        answer: r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value '[]' at 'requestItems._capture_20261113_sweep_other.member.keys' failed to satisfy constraint: Member must have length greater than or equal to 1; Value null at 'requestItems._capture_20261113_sweep_none.member.keys' failed to satisfy constraint: Member must not be null"}"#,
        us_east_1: None,
    },
    Row {
        operation: "BatchGetItem",
        body: r#"{"RequestItems":{"_capture_20261113_sweep_none":{},"_capture_20261113_sweep_other":{"Keys":[{"pk":{"S":"k0"}},{"pk":{"S":"k1"}},{"pk":{"S":"k2"}},{"pk":{"S":"k3"}},{"pk":{"S":"k4"}},{"pk":{"S":"k5"}},{"pk":{"S":"k6"}},{"pk":{"S":"k7"}},{"pk":{"S":"k8"}},{"pk":{"S":"k9"}},{"pk":{"S":"k10"}},{"pk":{"S":"k11"}},{"pk":{"S":"k12"}},{"pk":{"S":"k13"}},{"pk":{"S":"k14"}},{"pk":{"S":"k15"}},{"pk":{"S":"k16"}},{"pk":{"S":"k17"}},{"pk":{"S":"k18"}},{"pk":{"S":"k19"}},{"pk":{"S":"k20"}},{"pk":{"S":"k21"}},{"pk":{"S":"k22"}},{"pk":{"S":"k23"}},{"pk":{"S":"k24"}},{"pk":{"S":"k25"}},{"pk":{"S":"k26"}},{"pk":{"S":"k27"}},{"pk":{"S":"k28"}},{"pk":{"S":"k29"}},{"pk":{"S":"k30"}},{"pk":{"S":"k31"}},{"pk":{"S":"k32"}},{"pk":{"S":"k33"}},{"pk":{"S":"k34"}},{"pk":{"S":"k35"}},{"pk":{"S":"k36"}},{"pk":{"S":"k37"}},{"pk":{"S":"k38"}},{"pk":{"S":"k39"}},{"pk":{"S":"k40"}},{"pk":{"S":"k41"}},{"pk":{"S":"k42"}},{"pk":{"S":"k43"}},{"pk":{"S":"k44"}},{"pk":{"S":"k45"}},{"pk":{"S":"k46"}},{"pk":{"S":"k47"}},{"pk":{"S":"k48"}},{"pk":{"S":"k49"}},{"pk":{"S":"k50"}},{"pk":{"S":"k51"}},{"pk":{"S":"k52"}},{"pk":{"S":"k53"}},{"pk":{"S":"k54"}},{"pk":{"S":"k55"}},{"pk":{"S":"k56"}},{"pk":{"S":"k57"}},{"pk":{"S":"k58"}},{"pk":{"S":"k59"}},{"pk":{"S":"k60"}},{"pk":{"S":"k61"}},{"pk":{"S":"k62"}},{"pk":{"S":"k63"}},{"pk":{"S":"k64"}},{"pk":{"S":"k65"}},{"pk":{"S":"k66"}},{"pk":{"S":"k67"}},{"pk":{"S":"k68"}},{"pk":{"S":"k69"}},{"pk":{"S":"k70"}},{"pk":{"S":"k71"}},{"pk":{"S":"k72"}},{"pk":{"S":"k73"}},{"pk":{"S":"k74"}},{"pk":{"S":"k75"}},{"pk":{"S":"k76"}},{"pk":{"S":"k77"}},{"pk":{"S":"k78"}},{"pk":{"S":"k79"}},{"pk":{"S":"k80"}},{"pk":{"S":"k81"}},{"pk":{"S":"k82"}},{"pk":{"S":"k83"}},{"pk":{"S":"k84"}},{"pk":{"S":"k85"}},{"pk":{"S":"k86"}},{"pk":{"S":"k87"}},{"pk":{"S":"k88"}},{"pk":{"S":"k89"}},{"pk":{"S":"k90"}},{"pk":{"S":"k91"}},{"pk":{"S":"k92"}},{"pk":{"S":"k93"}},{"pk":{"S":"k94"}},{"pk":{"S":"k95"}},{"pk":{"S":"k96"}},{"pk":{"S":"k97"}},{"pk":{"S":"k98"}},{"pk":{"S":"k99"}},{"pk":{"S":"k100"}}]}}}"#,
        answer: r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value null at 'requestItems._capture_20261113_sweep_none.member.keys' failed to satisfy constraint: Member must not be null; Value '[{pk=com.amazonaws.dynamodb.v20120810.AttributeValue@9a88c60e}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@9a88c9cf}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@9a88cd90}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@9a88d151}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@9a88d512}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@9a88d8d3}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@9a88dc94}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@9a88e055}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@9a88e416}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@9a88e7d7}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a0523b73}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a0523f34}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a05242f5}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a05246b6}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a0524a77}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a0524e38}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a05251f9}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a05255ba}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a052597b}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a0525d3c}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a052afd2}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a052b393}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a052b754}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a052bb15}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a052bed6}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a052c297}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a052c658}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a052ca19}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a052cdda}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a052d19b}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a0532431}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a05327f2}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a0532bb3}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a0532f74}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a0533335}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a05336f6}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a0533ab7}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a0533e78}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a0534239}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a05345fa}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a0539890}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a0539c51}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a053a012}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a053a3d3}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a053a794}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a053ab55}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a053af16}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a053b2d7}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a053b698}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a053ba59}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a0540cef}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a05410b0}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a0541471}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a0541832}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a0541bf3}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a0541fb4}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a0542375}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a0542736}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a0542af7}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a0542eb8}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a054814e}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a054850f}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a05488d0}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a0548c91}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a0549052}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a0549413}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a05497d4}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a0549b95}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a0549f56}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a054a317}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a054f5ad}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a054f96e}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a054fd2f}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a05500f0}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a05504b1}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a0550872}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a0550c33}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a0550ff4}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a05513b5}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a0551776}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a0556a0c}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a0556dcd}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a055718e}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a055754f}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a0557910}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a0557cd1}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a0558092}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a0558453}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a0558814}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a0558bd5}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a055de6b}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a055e22c}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a055e5ed}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a055e9ae}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a055ed6f}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a055f130}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a055f4f1}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a055f8b2}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a055fc73}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a0560034}, {pk=com.amazonaws.dynamodb.v20120810.AttributeValue@53b6fe4f}]' at 'requestItems._capture_20261113_sweep_other.member.keys' failed to satisfy constraint: Member must have length less than or equal to 100"}"#,
        us_east_1: Some(
            r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value null at 'requestItems._capture_20261113_sweep_none.member.keys' failed to satisfy constraint: Member must not be null; Value '[{pk=AttributeValue(s=k0, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k1, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k2, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k3, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k4, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k5, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k6, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k7, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k8, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k9, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k10, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k11, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k12, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k13, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k14, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k15, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k16, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k17, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k18, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k19, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k20, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k21, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k22, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k23, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k24, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k25, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k26, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k27, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k28, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k29, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k30, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k31, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k32, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k33, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k34, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k35, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k36, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k37, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k38, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k39, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k40, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k41, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k42, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k43, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k44, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k45, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k46, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k47, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k48, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k49, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k50, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k51, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k52, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k53, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k54, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k55, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k56, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k57, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k58, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k59, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k60, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k61, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k62, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k63, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k64, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k65, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k66, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k67, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k68, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k69, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k70, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k71, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k72, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k73, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k74, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k75, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k76, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k77, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k78, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k79, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k80, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k81, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k82, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k83, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k84, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k85, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k86, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k87, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k88, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k89, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k90, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k91, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k92, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k93, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k94, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k95, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k96, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k97, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k98, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k99, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k100, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}]' at 'requestItems._capture_20261113_sweep_other.member.keys' failed to satisfy constraint: Member must have length less than or equal to 100"}"#,
        ),
    },
    Row {
        operation: "BatchGetItem",
        body: r#"{"RequestItems":{"_capture_20261113_sweep_none":{},"bad!name":{"Keys":[{"pk":{"S":"k1"}}]}}}"#,
        answer: r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value null at 'requestItems._capture_20261113_sweep_none.member.keys' failed to satisfy constraint: Member must not be null"}"#,
        us_east_1: None,
    },
    Row {
        operation: "BatchGetItem",
        body: r#"{"RequestItems":{"_capture_20261113_sweep_z":{},"_capture_20261113_sweep_m":{},"_capture_20261113_sweep_a":{}}}"#,
        answer: r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"3 validation errors detected: Value null at 'requestItems._capture_20261113_sweep_m.member.keys' failed to satisfy constraint: Member must not be null; Value null at 'requestItems._capture_20261113_sweep_a.member.keys' failed to satisfy constraint: Member must not be null; Value null at 'requestItems._capture_20261113_sweep_z.member.keys' failed to satisfy constraint: Member must not be null"}"#,
        us_east_1: None,
    },
    Row {
        operation: "BatchGetItem",
        body: r#"{"RequestItems":{"_capture_20261113_sweep_none":{"Keys":[{}]}}}"#,
        answer: r#"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found"}"#,
        us_east_1: None,
    },
    Row {
        operation: "BatchGetItem",
        body: r#"{"RequestItems":{"_capture_20261113_sweep_none":{"Keys":[{}]}},"ReturnConsumedCapacity":"BOGUS"}"#,
        answer: r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value 'BOGUS' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"}"#,
        us_east_1: None,
    },
    Row {
        operation: "BatchWriteItem",
        body: r#"{"RequestItems":{"_capture_20261113_sweep_none":[{"PutRequest":{}}],"_capture_20261113_sweep_other":[{"PutRequest":{"Item":{"pk":{"S":"k0"}}}},{"PutRequest":{"Item":{"pk":{"S":"k1"}}}},{"PutRequest":{"Item":{"pk":{"S":"k2"}}}},{"PutRequest":{"Item":{"pk":{"S":"k3"}}}},{"PutRequest":{"Item":{"pk":{"S":"k4"}}}},{"PutRequest":{"Item":{"pk":{"S":"k5"}}}},{"PutRequest":{"Item":{"pk":{"S":"k6"}}}},{"PutRequest":{"Item":{"pk":{"S":"k7"}}}},{"PutRequest":{"Item":{"pk":{"S":"k8"}}}},{"PutRequest":{"Item":{"pk":{"S":"k9"}}}},{"PutRequest":{"Item":{"pk":{"S":"k10"}}}},{"PutRequest":{"Item":{"pk":{"S":"k11"}}}},{"PutRequest":{"Item":{"pk":{"S":"k12"}}}},{"PutRequest":{"Item":{"pk":{"S":"k13"}}}},{"PutRequest":{"Item":{"pk":{"S":"k14"}}}},{"PutRequest":{"Item":{"pk":{"S":"k15"}}}},{"PutRequest":{"Item":{"pk":{"S":"k16"}}}},{"PutRequest":{"Item":{"pk":{"S":"k17"}}}},{"PutRequest":{"Item":{"pk":{"S":"k18"}}}},{"PutRequest":{"Item":{"pk":{"S":"k19"}}}},{"PutRequest":{"Item":{"pk":{"S":"k20"}}}},{"PutRequest":{"Item":{"pk":{"S":"k21"}}}},{"PutRequest":{"Item":{"pk":{"S":"k22"}}}},{"PutRequest":{"Item":{"pk":{"S":"k23"}}}},{"PutRequest":{"Item":{"pk":{"S":"k24"}}}},{"PutRequest":{"Item":{"pk":{"S":"k25"}}}}]}}"#,
        answer: r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value '{_capture_20261113_sweep_none=[WriteRequest(putRequest=PutRequest(item=null, workloadProfileName=null), deleteRequest=null)], _capture_20261113_sweep_other=[WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k0, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k1, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k2, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k3, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k4, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k5, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k6, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k7, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k8, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k9, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k10, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k11, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k12, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k13, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k14, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k15, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k16, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k17, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k18, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k19, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k20, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k21, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k22, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k23, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k24, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=AttributeValue(s=k25, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, workloadProfileName=null), deleteRequest=null)]}' at 'requestItems' failed to satisfy constraint: Map value must satisfy constraint: [Member must have length less than or equal to 25, Member must have length greater than or equal to 1]; Value null at 'requestItems._capture_20261113_sweep_none.member.1.member.putRequest.item' failed to satisfy constraint: Member must not be null"}"#,
        us_east_1: Some(
            r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value '{_capture_20261113_sweep_none=[WriteRequest(putRequest=PutRequest(item=null, workloadProfileName=null), deleteRequest=null)], _capture_20261113_sweep_other=[WriteRequest(putRequest=PutRequest(item={pk=com.amazonaws.dynamodb.v20120810.AttributeValue@9a88c60e}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=com.amazonaws.dynamodb.v20120810.AttributeValue@9a88c9cf}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=com.amazonaws.dynamodb.v20120810.AttributeValue@9a88cd90}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=com.amazonaws.dynamodb.v20120810.AttributeValue@9a88d151}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=com.amazonaws.dynamodb.v20120810.AttributeValue@9a88d512}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=com.amazonaws.dynamodb.v20120810.AttributeValue@9a88d8d3}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=com.amazonaws.dynamodb.v20120810.AttributeValue@9a88dc94}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=com.amazonaws.dynamodb.v20120810.AttributeValue@9a88e055}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=com.amazonaws.dynamodb.v20120810.AttributeValue@9a88e416}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=com.amazonaws.dynamodb.v20120810.AttributeValue@9a88e7d7}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a0523b73}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a0523f34}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a05242f5}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a05246b6}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a0524a77}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a0524e38}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a05251f9}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a05255ba}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a052597b}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a0525d3c}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a052afd2}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a052b393}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a052b754}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a052bb15}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a052bed6}, workloadProfileName=null), deleteRequest=null), WriteRequest(putRequest=PutRequest(item={pk=com.amazonaws.dynamodb.v20120810.AttributeValue@a052c297}, workloadProfileName=null), deleteRequest=null)]}' at 'requestItems' failed to satisfy constraint: Map value must satisfy constraint: [Member must have length less than or equal to 25, Member must have length greater than or equal to 1]; Value null at 'requestItems._capture_20261113_sweep_none.member.1.member.putRequest.item' failed to satisfy constraint: Member must not be null"}"#,
        ),
    },
    Row {
        operation: "BatchWriteItem",
        body: r#"{"RequestItems":{"_capture_20261113_sweep_none":[{"PutRequest":{}}],"bad!name":[{"PutRequest":{"Item":{"pk":{"S":"k1"}}}}]}}"#,
        answer: r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value null at 'requestItems._capture_20261113_sweep_none.member.1.member.putRequest.item' failed to satisfy constraint: Member must not be null"}"#,
        us_east_1: None,
    },
    Row {
        operation: "BatchWriteItem",
        body: r#"{"RequestItems":{"_capture_20261113_sweep_none":[{"PutRequest":{"Item":{"pk":{"S":"a"}}},"DeleteRequest":{}}]}}"#,
        answer: r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value null at 'requestItems._capture_20261113_sweep_none.member.1.member.deleteRequest.key' failed to satisfy constraint: Member must not be null"}"#,
        us_east_1: None,
    },
    Row {
        operation: "BatchWriteItem",
        body: r#"{"RequestItems":{"_capture_20261113_sweep_none":[{"PutRequest":{"Item":{"pk":{"S":"a"}}},"DeleteRequest":null}]}}"#,
        answer: r#"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found"}"#,
        us_east_1: None,
    },
    Row {
        operation: "BatchWriteItem",
        body: r#"{"RequestItems":{"_capture_20261113_sweep_none":[{"PutRequest":{},"DeleteRequest":{"Key":{"pk":{"S":"a"}}}}]}}"#,
        answer: r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value null at 'requestItems._capture_20261113_sweep_none.member.1.member.putRequest.item' failed to satisfy constraint: Member must not be null"}"#,
        us_east_1: None,
    },
    Row {
        operation: "BatchWriteItem",
        body: r#"{"RequestItems":{"_capture_20261113_sweep_none":[{"PutRequest":{"Item":{"pk":{"S":"a"}}},"DeleteRequest":{"Key":{"pk":{"S":"a"}}}}]},"ReturnConsumedCapacity":"BOGUS"}"#,
        answer: r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value 'BOGUS' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"}"#,
        us_east_1: None,
    },
    Row {
        operation: "BatchWriteItem",
        body: r#"{"RequestItems":{"_capture_20261113_sweep_none":[{}]},"ReturnConsumedCapacity":"BOGUS"}"#,
        answer: r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value 'BOGUS' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"}"#,
        us_east_1: None,
    },
];

const SECOND_LEFT_OUT: &[LeftOut] = &[LeftOut {
    operation: "BatchGetItem",
    body: r#"{"RequestItems":{"_capture_20261113_sweep_none":{},"_capture_20261113_sweep_other":{"Keys":[{"pk":{"S":"k0"}},{"pk":{"S":"k1"}},{"pk":{"S":"k2"}},{"pk":{"S":"k3"}},{"pk":{"S":"k4"}},{"pk":{"S":"k5"}},{"pk":{"S":"k6"}},{"pk":{"S":"k7"}},{"pk":{"S":"k8"}},{"pk":{"S":"k9"}},{"pk":{"S":"k10"}},{"pk":{"S":"k11"}},{"pk":{"S":"k12"}},{"pk":{"S":"k13"}},{"pk":{"S":"k14"}},{"pk":{"S":"k15"}},{"pk":{"S":"k16"}},{"pk":{"S":"k17"}},{"pk":{"S":"k18"}},{"pk":{"S":"k19"}},{"pk":{"S":"k20"}},{"pk":{"S":"k21"}},{"pk":{"S":"k22"}},{"pk":{"S":"k23"}},{"pk":{"S":"k24"}},{"pk":{"S":"k25"}},{"pk":{"S":"k26"}},{"pk":{"S":"k27"}},{"pk":{"S":"k28"}},{"pk":{"S":"k29"}},{"pk":{"S":"k30"}},{"pk":{"S":"k31"}},{"pk":{"S":"k32"}},{"pk":{"S":"k33"}},{"pk":{"S":"k34"}},{"pk":{"S":"k35"}},{"pk":{"S":"k36"}},{"pk":{"S":"k37"}},{"pk":{"S":"k38"}},{"pk":{"S":"k39"}},{"pk":{"S":"k40"}},{"pk":{"S":"k41"}},{"pk":{"S":"k42"}},{"pk":{"S":"k43"}},{"pk":{"S":"k44"}},{"pk":{"S":"k45"}},{"pk":{"S":"k46"}},{"pk":{"S":"k47"}},{"pk":{"S":"k48"}},{"pk":{"S":"k49"}},{"pk":{"S":"k50"}},{"pk":{"S":"k51"}},{"pk":{"S":"k52"}},{"pk":{"S":"k53"}},{"pk":{"S":"k54"}},{"pk":{"S":"k55"}},{"pk":{"S":"k56"}},{"pk":{"S":"k57"}},{"pk":{"S":"k58"}},{"pk":{"S":"k59"}},{"pk":{"S":"k60"}},{"pk":{"S":"k61"}},{"pk":{"S":"k62"}},{"pk":{"S":"k63"}},{"pk":{"S":"k64"}},{"pk":{"S":"k65"}},{"pk":{"S":"k66"}},{"pk":{"S":"k67"}},{"pk":{"S":"k68"}},{"pk":{"S":"k69"}},{"pk":{"S":"k70"}},{"pk":{"S":"k71"}},{"pk":{"S":"k72"}},{"pk":{"S":"k73"}},{"pk":{"S":"k74"}},{"pk":{"S":"k75"}},{"pk":{"S":"k76"}},{"pk":{"S":"k77"}},{"pk":{"S":"k78"}},{"pk":{"S":"k79"}},{"pk":{"S":"k80"}},{"pk":{"S":"k81"}},{"pk":{"S":"k82"}},{"pk":{"S":"k83"}},{"pk":{"S":"k84"}},{"pk":{"S":"k85"}},{"pk":{"S":"k86"}},{"pk":{"S":"k87"}},{"pk":{"S":"k88"}},{"pk":{"S":"k89"}},{"pk":{"S":"k90"}},{"pk":{"S":"k91"}},{"pk":{"S":"k92"}},{"pk":{"S":"k93"}},{"pk":{"S":"k94"}},{"pk":{"S":"k95"}},{"pk":{"S":"k96"}},{"pk":{"S":"k97"}},{"pk":{"S":"k98"}},{"pk":{"S":"k99"}},{"pk":{"S":"k100"}}]}}}"#,
    answer: r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value null at 'requestItems._capture_20261113_sweep_none.member.keys' failed to satisfy constraint: Member must not be null; Value '[{pk=AttributeValue(s=k0, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k1, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k2, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k3, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k4, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k5, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k6, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k7, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k8, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k9, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k10, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k11, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k12, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k13, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k14, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k15, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k16, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k17, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k18, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k19, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k20, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k21, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k22, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k23, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k24, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k25, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k26, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k27, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k28, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k29, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k30, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k31, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k32, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k33, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k34, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k35, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k36, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k37, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k38, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k39, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k40, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k41, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k42, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k43, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k44, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k45, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k46, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k47, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k48, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k49, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k50, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k51, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k52, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k53, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k54, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k55, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k56, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k57, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k58, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k59, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k60, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k61, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k62, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k63, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k64, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k65, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k66, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k67, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k68, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k69, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k70, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k71, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k72, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k73, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k74, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k75, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k76, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k77, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k78, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k79, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k80, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k81, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k82, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k83, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k84, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k85, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k86, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k87, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k88, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k89, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k90, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k91, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k92, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k93, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k94, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k95, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k96, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k97, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k98, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k99, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}, {pk=AttributeValue(s=k100, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}]' at 'requestItems._capture_20261113_sweep_other.member.keys' failed to satisfy constraint: Member must have length less than or equal to 100"}"#,
    reason: HASHED_ECHO,
}];
