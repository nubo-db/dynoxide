//! A BatchGetItem or BatchWriteItem whose `RequestItems` is missing, null or
//! malformed: a table's entry with no `Keys`, a write request with no `Item`
//! or `Key`, a null where a table, a key or a write request belongs, a member
//! of the wrong JSON type, and a write request holding both a put and a
//! delete.
//!
//! The rows are the requests sent to DynamoDB in eu-west-2 and us-east-1 on
//! 11 and 12 November 2026, in the order they were sent, with the answers
//! DynamoDB gave, against a table that doesn't exist. They are data captured
//! from DynamoDB, not written by hand. The regions gave the same answer to
//! every row but two, both BatchWriteItem; both answers are kept there, and
//! Dynoxide follows eu-west-2. Answers are compared as parsed JSON, since
//! DynamoDB and Dynoxide order members differently, along with the HTTP
//! status.

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

/// How many rows are sent, compared with DynamoDB's answers and left out.
const COUNTS: (usize, usize, usize) = (50, 25, 25);

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

/// Every row is sent, in order, to one server, and each answer that differs
/// from eu-west-2's is listed. A row left out by name must keep the answer
/// recorded for it.
#[tokio::test]
async fn malformed_request_items_answer_as_dynamodb_did() {
    let url = start_server().await;
    let mut wrong = Vec::new();
    let (mut sent, mut compared, mut left_out) = (0, 0, 0);
    for row in ROWS {
        let expected: Value = serde_json::from_str(row.answer).unwrap();
        let other: Option<Value> = row.us_east_1.map(|a| serde_json::from_str(a).unwrap());
        if let Some(other) = &other {
            assert_ne!(&expected, other, "the regions' answers must differ");
        }
        let (status, got) = send(&url, row.operation, row.body).await;
        sent += 1;
        if let Some(l) = LEFT_OUT
            .iter()
            .find(|l| l.operation == row.operation && l.body == row.body)
        {
            left_out += 1;
            let pinned: Value = serde_json::from_str(l.answer).unwrap();
            if got == expected {
                wrong.push(format!(
                    "{} {} now answers as DynamoDB did: drop it from LEFT_OUT",
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
    assert_eq!((sent, compared, left_out), COUNTS);
}

/// Each row left out names a row that is sent.
#[test]
fn every_row_left_out_is_sent() {
    for l in LEFT_OUT {
        assert!(
            ROWS.iter()
                .any(|row| row.operation == l.operation && row.body == l.body),
            "{} {} is left out but not sent",
            l.operation,
            l.body
        );
    }
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

const LEFT_OUT: &[LeftOut] = &[
    LeftOut {
        operation: "BatchGetItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": {}}}"##,
        answer: r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"missing field `Keys` at line 1 column 50"}"##,
        reason: "a missing or null required member is answered in the decoder's own words",
    },
    LeftOut {
        operation: "BatchGetItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": {"Keys": null}}}"##,
        answer: r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"invalid type: null, expected a sequence at line 1 column 61"}"##,
        reason: "a missing or null required member is answered in the decoder's own words",
    },
    LeftOut {
        operation: "BatchGetItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": {"Keys": ["x"]}}}"##,
        answer: r##"{"Message":"class sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl cannot be cast to class java.lang.Class (sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl and java.lang.Class are in module java.base of loader 'bootstrap')","__type":"com.amazon.coral.service#SerializationException"}"##,
        reason: "a member of the wrong JSON type in a list or a table entry is answered with a class-cast message",
    },
    LeftOut {
        operation: "BatchGetItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": {"Keys": [5]}}}"##,
        answer: r##"{"Message":"class sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl cannot be cast to class java.lang.Class (sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl and java.lang.Class are in module java.base of loader 'bootstrap')","__type":"com.amazon.coral.service#SerializationException"}"##,
        reason: "a member of the wrong JSON type in a list or a table entry is answered with a class-cast message",
    },
    LeftOut {
        operation: "BatchGetItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": {"Keys": [null]}}}"##,
        answer: r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"invalid type: null, expected a map at line 1 column 62"}"##,
        reason: "a null table entry or a null in a list is answered in the decoder's own words",
    },
    LeftOut {
        operation: "BatchGetItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": null}}"##,
        answer: r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"invalid type: null, expected struct KeysAndAttributes at line 1 column 52"}"##,
        reason: "a null table entry or a null in a list is answered in the decoder's own words",
    },
    LeftOut {
        operation: "BatchGetItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": {"keys": [{"pk": {"S": "a"}}]}}}"##,
        answer: r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"missing field `Keys` at line 1 column 78"}"##,
        reason: "a missing or null required member is answered in the decoder's own words",
    },
    LeftOut {
        operation: "BatchGetItem",
        body: r##"{"RequestItems": null}"##,
        answer: r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"invalid type: null, expected a map at line 1 column 21"}"##,
        reason: "a missing or null RequestItems is answered in the decoder's own words",
    },
    LeftOut {
        operation: "BatchGetItem",
        body: r##"{}"##,
        answer: r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"missing field `RequestItems` at line 1 column 2"}"##,
        reason: "a missing or null RequestItems is answered in the decoder's own words",
    },
    LeftOut {
        operation: "BatchGetItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": {}}, "ReturnConsumedCapacity": "BOGUS"}"##,
        answer: r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"missing field `Keys` at line 1 column 50"}"##,
        reason: "a missing or null required member is answered in the decoder's own words",
    },
    LeftOut {
        operation: "BatchGetItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": {}, "_capture_20261111_237_other": {}}}"##,
        answer: r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"missing field `Keys` at line 1 column 50"}"##,
        reason: "a missing or null required member is answered in the decoder's own words",
    },
    LeftOut {
        operation: "BatchWriteItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": [{"PutRequest": {}}]}}"##,
        answer: r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"missing field `Item` at line 1 column 66"}"##,
        reason: "a missing or null required member is answered in the decoder's own words",
    },
    LeftOut {
        operation: "BatchWriteItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": [{"DeleteRequest": {}}]}}"##,
        answer: r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"missing field `Key` at line 1 column 69"}"##,
        reason: "a missing or null required member is answered in the decoder's own words",
    },
    LeftOut {
        operation: "BatchWriteItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": [{"PutRequest": {"Item": null}}]}}"##,
        answer: r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"invalid type: null, expected a map at line 1 column 77"}"##,
        reason: "a missing or null required member is answered in the decoder's own words",
    },
    LeftOut {
        operation: "BatchWriteItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": [{"DeleteRequest": {"Key": null}}]}}"##,
        answer: r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"invalid type: null, expected a map at line 1 column 79"}"##,
        reason: "a missing or null required member is answered in the decoder's own words",
    },
    LeftOut {
        operation: "BatchWriteItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": [null]}}"##,
        answer: r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"invalid type: null, expected struct WriteRequest at line 1 column 53"}"##,
        reason: "a null table entry or a null in a list is answered in the decoder's own words",
    },
    LeftOut {
        operation: "BatchWriteItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": []}}"##,
        answer: r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value '{_capture_20261111_237_none=[]}' at 'requestItems' failed to satisfy constraint: Map value must satisfy constraint: [Member must have length less than or equal to 25, Member must have length greater than or equal to 1]"}"##,
        reason: "the empty list's place among other faults is not yet captured",
    },
    LeftOut {
        operation: "BatchWriteItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": "x"}}"##,
        answer: r##"{"Message":"class sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl cannot be cast to class java.lang.Class (sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl and java.lang.Class are in module java.base of loader 'bootstrap')","__type":"com.amazon.coral.service#SerializationException"}"##,
        reason: "a member of the wrong JSON type in a list or a table entry is answered with a class-cast message",
    },
    LeftOut {
        operation: "BatchWriteItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": 5}}"##,
        answer: r##"{"Message":"class sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl cannot be cast to class java.lang.Class (sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl and java.lang.Class are in module java.base of loader 'bootstrap')","__type":"com.amazon.coral.service#SerializationException"}"##,
        reason: "a member of the wrong JSON type in a list or a table entry is answered with a class-cast message",
    },
    LeftOut {
        operation: "BatchWriteItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": {}}}"##,
        answer: r##"{"Message":"class sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl cannot be cast to class java.lang.Class (sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl and java.lang.Class are in module java.base of loader 'bootstrap')","__type":"com.amazon.coral.service#SerializationException"}"##,
        reason: "a member of the wrong JSON type in a list or a table entry is answered with a class-cast message",
    },
    LeftOut {
        operation: "BatchWriteItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": null}}"##,
        answer: r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"invalid type: null, expected a sequence at line 1 column 52"}"##,
        reason: "a null table entry or a null in a list is answered in the decoder's own words",
    },
    LeftOut {
        operation: "BatchWriteItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": [{"PutRequest": {"Item": {"pk": {"S": "a"}}}, "DeleteRequest": {"Key": {"pk": {"S": "a"}}}}]}}"##,
        answer: r##"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found"}"##,
        reason: "a write request holding both a put and a delete is run",
    },
    LeftOut {
        operation: "BatchWriteItem",
        body: r##"{}"##,
        answer: r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"missing field `RequestItems` at line 1 column 2"}"##,
        reason: "a missing or null RequestItems is answered in the decoder's own words",
    },
    LeftOut {
        operation: "BatchWriteItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": [{"PutRequest": {}}]}, "ReturnConsumedCapacity": "BOGUS"}"##,
        answer: r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"missing field `Item` at line 1 column 66"}"##,
        reason: "a missing or null required member is answered in the decoder's own words",
    },
    LeftOut {
        operation: "BatchWriteItem",
        body: r##"{"RequestItems": {"_capture_20261111_237_none": [{"PutRequest": {}}]}, "ReturnItemCollectionMetrics": "BOGUS"}"##,
        answer: r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"missing field `Item` at line 1 column 66"}"##,
        reason: "a missing or null required member is answered in the decoder's own words",
    },
];
