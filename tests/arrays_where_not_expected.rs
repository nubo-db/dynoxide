//! A JSON array given where DynamoDB doesn't expect one.
//!
//! DynamoDB refuses a request body that is a JSON array, and an array given
//! for a scalar, a map or a structure inside a request, with a
//! `SerializationException` that has no message. A body that is a string, a
//! number or a boolean is refused as `Unexpected value type in payload`, and
//! `null` as `Request payload does not contain the input model`. A few forms
//! are read by position: BatchGetItem's body as `[RequestItems]` or
//! `[RequestItems, ReturnConsumedCapacity]`, and on Query and Scan a
//! `Condition` as `[AttributeValueList, ComparisonOperator]`.
//!
//! The rows below are the requests sent to eu-west-2 and us-east-1 on 11 and
//! 12 November 2026 and the answers DynamoDB gave. They are data captured from
//! DynamoDB, not written by hand. Where the regions differ both answers are
//! kept and the row names the one Dynoxide gives: BatchWriteItem reads its body
//! as `[RequestItems]`, `[RequestItems, ReturnConsumedCapacity]` or
//! `[RequestItems, ReturnConsumedCapacity, ReturnItemCollectionMetrics]`, a
//! `PutRequest` as `[Item]` and a `DeleteRequest` as `[Key]` in eu-west-2 and
//! refuses them in us-east-1, and Dynoxide reads them as eu-west-2 does. The
//! one other row where the regions differ is an operator outside the enum,
//! which the regions word differently whatever form the condition takes, and
//! Dynoxide gives it in us-east-1's words. Every request in the third round
//! runs, in order, against a table holding one item; every other request names
//! a table that doesn't exist. Answers are compared as parsed JSON, since
//! DynamoDB and Dynoxide order members differently.

#![cfg(feature = "http-server")]

use dynoxide::Database;

/// The region whose answer Dynoxide gives where the two differ.
#[derive(Clone, Copy, Debug)]
enum Region {
    UsEast1,
    EuWest2,
}

/// DynamoDB's answer to a row: the same in both regions, or one per region.
enum Answer {
    Same(&'static str),
    Regions {
        eu_west_2: &'static str,
        us_east_1: &'static str,
        follows: Region,
    },
}

use Answer::{Regions, Same};

impl Answer {
    /// The answer Dynoxide must give, and the one it must not where the
    /// regions differ.
    fn expected(&self) -> (serde_json::Value, Option<serde_json::Value>) {
        let parse = |s: &str| serde_json::from_str::<serde_json::Value>(s).unwrap();
        match *self {
            Same(answer) => (parse(answer), None),
            Regions {
                eu_west_2,
                us_east_1,
                follows,
            } => {
                let (kept, other) = match follows {
                    Region::UsEast1 => (us_east_1, eu_west_2),
                    Region::EuWest2 => (eu_west_2, us_east_1),
                };
                assert_ne!(
                    parse(kept),
                    parse(other),
                    "the regions' answers must differ"
                );
                (parse(kept), Some(parse(other)))
            }
        }
    }
}

/// A captured row: its number in the round, the operation, the request body,
/// and DynamoDB's answer.
type Row = (usize, &'static str, &'static str, Answer);

/// Rows left out by name, with Dynoxide's present answer and the reason. Each
/// is still sent, must still differ from DynamoDB's answer, and must give the
/// answer recorded here.
const LEFT_OUT: &[(&str, usize, &str, &str)] = &[
    (
        "second round",
        3,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"missing field `Keys` at line 1 column 90"}"#,
        "an array holding an object with a RequestItems member is read by position as a \
         RequestItems map naming a table called RequestItems, whose entry has no Keys",
    ),
    (
        "seventh round",
        21,
        r#"{"__type":"com.amazon.coral.service#UnknownOperationException"}"#,
        "UpdateContinuousBackups is not implemented",
    ),
    (
        "seventh round",
        27,
        r#"{"__type":"com.amazon.coral.service#SerializationException"}"#,
        "a PutRequest given as [Item, \"x\"] is read with the second element ignored; one \
         row is not enough to know the rule for elements past the first",
    ),
];

const ARR_TABLE: &str = "_capture_20261112_arr";

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

async fn send(url: &str, operation: &str, body: &str) -> (u16, serde_json::Value) {
    let resp = reqwest::Client::new()
        .post(url)
        .header("x-amz-target", format!("DynamoDB_20120810.{operation}"))
        .header("content-type", "application/x-amz-json-1.0")
        .header(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=fakekey/20260101/us-east-1/dynamodb/aws4_request, \
             SignedHeaders=host;x-amz-date;x-amz-target, Signature=fakesig",
        )
        .header("x-amz-date", "20260101T000000Z")
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    let status = resp.status().as_u16();
    (status, resp.json().await.unwrap())
}

/// The status DynamoDB sends with an answer: 400 for an error, 200 otherwise.
fn status_of(answer: &serde_json::Value) -> u16 {
    if answer.get("__type").is_some() {
        400
    } else {
        200
    }
}

/// Send every row of a round in order and return how many were compared.
async fn assert_rows(url: &str, round: &str, rows: &[Row]) -> usize {
    let mut wrong = Vec::new();
    let mut compared = 0;
    let mut left_out = 0;
    for (number, operation, body, answer) in rows {
        let (expected, other_region) = answer.expected();
        let (status, got) = send(url, operation, body).await;
        compared += 1;
        if let Some(&(_, _, present, reason)) = LEFT_OUT
            .iter()
            .find(|&&(r, n, _, _)| r == round && n == *number)
        {
            left_out += 1;
            let present: serde_json::Value = serde_json::from_str(present).unwrap();
            if got == expected {
                wrong.push(format!(
                    "{round} row {number} now answers as DynamoDB did: drop it from LEFT_OUT"
                ));
            } else if status != status_of(&present) || got != present {
                wrong.push(format!(
                    "{round} row {number} (left out: {reason}): {operation} {body}\n  \
                     expected {} {present}\n  got      {status} {got}",
                    status_of(&present)
                ));
            }
            continue;
        }
        if status != status_of(&expected) || got != expected {
            let region = match other_region {
                Some(ref other) if *other == got => " (the other region's answer)",
                _ => "",
            };
            wrong.push(format!(
                "{round} row {number}: {operation} {body}\n  expected {} {expected}\n  \
                 got      {status} {got}{region}",
                status_of(&expected)
            ));
        }
    }
    assert_eq!(compared, rows.len());
    assert_eq!(
        left_out,
        LEFT_OUT.iter().filter(|&&(r, _, _, _)| r == round).count(),
        "every row left out of the {round} must still be sent"
    );
    println!("{round}: compared {compared} rows, {left_out} left out by name");
    assert!(
        wrong.is_empty(),
        "{} of {compared} rows in the {round} differ:\n{}",
        wrong.len(),
        wrong.join("\n")
    );
    compared
}

#[tokio::test]
async fn the_first_round_answers_as_dynamodb_did() {
    let url = start_server().await;
    assert_eq!(assert_rows(&url, "first round", FIRST_ROUND).await, 32);
}

#[tokio::test]
async fn the_second_round_answers_as_dynamodb_did() {
    let url = start_server().await;
    assert_eq!(assert_rows(&url, "second round", SECOND_ROUND).await, 42);
}

/// The third round runs in file order against a table holding
/// `{pk: a, x: 1}`, because its BatchWriteItem rows would change it and its
/// last row reads it back.
#[tokio::test]
async fn the_third_round_answers_as_dynamodb_did() {
    let url = start_server().await;
    let (status, got) = send(
        &url,
        "CreateTable",
        &format!(
            r#"{{"TableName":"{ARR_TABLE}","AttributeDefinitions":[{{"AttributeName":"pk","AttributeType":"S"}}],"KeySchema":[{{"AttributeName":"pk","KeyType":"HASH"}}],"BillingMode":"PAY_PER_REQUEST"}}"#
        ),
    )
    .await;
    assert_eq!(status, 200, "{got}");
    let (status, got) = send(
        &url,
        "PutItem",
        &format!(r#"{{"TableName":"{ARR_TABLE}","Item":{{"pk":{{"S":"a"}},"x":{{"N":"1"}}}}}}"#),
    )
    .await;
    assert_eq!(status, 200, "{got}");
    assert_eq!(assert_rows(&url, "third round", THIRD_ROUND).await, 18);
}

#[tokio::test]
async fn the_fourth_round_answers_as_dynamodb_did() {
    let url = start_server().await;
    assert_eq!(assert_rows(&url, "fourth round", FOURTH_ROUND).await, 42);
}

#[tokio::test]
async fn the_fifth_round_answers_as_dynamodb_did() {
    let url = start_server().await;
    assert_eq!(assert_rows(&url, "fifth round", FIFTH_ROUND).await, 22);
}

#[tokio::test]
async fn the_sixth_round_answers_as_dynamodb_did() {
    let url = start_server().await;
    assert_eq!(assert_rows(&url, "sixth round", SIXTH_ROUND).await, 12);
}

#[tokio::test]
async fn the_seventh_round_answers_as_dynamodb_did() {
    let url = start_server().await;
    assert_eq!(assert_rows(&url, "seventh round", SEVENTH_ROUND).await, 31);
}

/// SearchVectors is not in the captures and keeps its own reader, which
/// answers a body that isn't an object in its own words. Its present answer
/// to an array body is pinned so a change to it is a decision, not an accident.
#[tokio::test]
async fn search_vectors_keeps_its_answer_to_an_array_body() {
    let url = start_server().await;
    let (status, got) = send(&url, "SearchVectors", r#"["t"]"#).await;
    assert_eq!(status, 400);
    assert_eq!(
        got,
        serde_json::json!({
            "__type": "com.amazon.coral.service#SerializationException",
            "message": "Start of structure or map found where not expected"
        })
    );
}

const FIRST_ROUND: &[Row] = &[
    (
        1,
        "PutItem",
        r#"["_capture_20261111_317_none", {"pk": {"S": "a"}}]"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        2,
        "PutItem",
        r#"[]"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        3,
        "PutItem",
        r#"["x"]"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        4,
        "PutItem",
        r#""x""#,
        Same(
            r#"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected value type in payload"}"#,
        ),
    ),
    (
        5,
        "PutItem",
        r#"5"#,
        Same(
            r#"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected value type in payload"}"#,
        ),
    ),
    (
        6,
        "PutItem",
        r#"null"#,
        Same(
            r#"{"__type":"com.amazon.coral.service#SerializationException","Message":"Request payload does not contain the input model"}"#,
        ),
    ),
    (
        7,
        "PutItem",
        r#"true"#,
        Same(
            r#"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected value type in payload"}"#,
        ),
    ),
    (
        8,
        "PutItem",
        r#"[{"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}}]"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        9,
        "GetItem",
        r#"["_capture_20261111_317_none", {"pk": {"S": "a"}}]"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        10,
        "GetItem",
        r#"[]"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        11,
        "DeleteItem",
        r#"["_capture_20261111_317_none", {"pk": {"S": "a"}}]"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        12,
        "UpdateItem",
        r#"["_capture_20261111_317_none", {"pk": {"S": "a"}}]"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        13,
        "Query",
        r#"["_capture_20261111_317_none"]"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        14,
        "Query",
        r#"[]"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        15,
        "Scan",
        r#"["_capture_20261111_317_none"]"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        16,
        "Scan",
        r#"[]"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        17,
        "Scan",
        r#""x""#,
        Same(
            r#"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected value type in payload"}"#,
        ),
    ),
    (
        18,
        "TransactWriteItems",
        r#"[[{"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}}}]]"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        19,
        "TransactWriteItems",
        r#"[]"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        20,
        "TransactGetItems",
        r#"[[{"Get": {"TableName": "_capture_20261111_317_none", "Key": {"pk": {"S": "a"}}}}]]"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        21,
        "BatchGetItem",
        r#"[{"_capture_20261111_317_none": {"Keys": [{"pk": {"S": "a"}}]}}]"#,
        Same(
            r#"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found"}"#,
        ),
    ),
    (
        22,
        "BatchWriteItem",
        r#"[{"_capture_20261111_317_none": [{"PutRequest": {"Item": {"pk": {"S": "a"}}}}]}]"#,
        Regions {
            eu_west_2: r#"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found"}"#,
            us_east_1: r#"{"__type":"com.amazon.coral.service#SerializationException"}"#,
            follows: Region::EuWest2,
        },
    ),
    (
        23,
        "ExecuteStatement",
        r#"["SELECT * FROM \"_capture_20261111_317_none\""]"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        24,
        "DescribeTable",
        r#"["_capture_20261111_317_none"]"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        25,
        "DescribeTable",
        r#"[]"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        26,
        "ListTables",
        r#"[]"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        27,
        "ListTables",
        r#"[1]"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        28,
        "ListTables",
        r#""x""#,
        Same(
            r#"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected value type in payload"}"#,
        ),
    ),
    (
        29,
        "CreateTable",
        r#"[]"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        30,
        "PutItem",
        r#""#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        31,
        "ListTables",
        r#""#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        32,
        "PutItem",
        r#"{"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
];

const SECOND_ROUND: &[Row] = &[
    (
        1,
        "BatchGetItem",
        r#"[]"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        2,
        "BatchGetItem",
        r#"[{}]"#,
        Same(
            r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'RequestItems' failed to satisfy constraint: Member must have length greater than or equal to 1"}"#,
        ),
    ),
    (
        3,
        "BatchGetItem",
        r#"[{"RequestItems":{"_capture_20261111_317_none":{"Keys":[{"pk":{"S":"a"}}]}}}]"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        4,
        "BatchGetItem",
        r#"[{"_capture_20261111_317_none":{"Keys":[{"pk":{"S":"a"}}]}},"TOTAL"]"#,
        Same(
            r#"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found"}"#,
        ),
    ),
    (
        5,
        "BatchGetItem",
        r#"[{"_capture_20261111_317_none":{"Keys":[{"pk":{"S":"a"}}]}},"BOGUS"]"#,
        Same(
            r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value 'BOGUS' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"}"#,
        ),
    ),
    (
        6,
        "BatchGetItem",
        r#"["x"]"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        7,
        "BatchWriteItem",
        r#"[]"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        8,
        "BatchWriteItem",
        r#"[{}]"#,
        Regions {
            eu_west_2: r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'RequestItems' failed to satisfy constraint: Member must have length greater than or equal to 1"}"#,
            us_east_1: r#"{"__type":"com.amazon.coral.service#SerializationException"}"#,
            follows: Region::EuWest2,
        },
    ),
    (
        9,
        "BatchWriteItem",
        r#"[{"_capture_20261111_317_none":[{"PutRequest":{"Item":{"pk":{"S":"a"}}}}]},"TOTAL"]"#,
        Regions {
            eu_west_2: r#"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found"}"#,
            us_east_1: r#"{"__type":"com.amazon.coral.service#SerializationException"}"#,
            follows: Region::EuWest2,
        },
    ),
    (
        10,
        "BatchWriteItem",
        r#"[{"_capture_20261111_317_none":[{"PutRequest":{"Item":{"pk":{"S":"a"}}}}]}]"#,
        Regions {
            eu_west_2: r#"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found"}"#,
            us_east_1: r#"{"__type":"com.amazon.coral.service#SerializationException"}"#,
            follows: Region::EuWest2,
        },
    ),
    (
        11,
        "BatchWriteItem",
        r#"[{"_capture_20261111_317_none":[{"PutRequest":{"Item":{"pk":{"S":"a"}}}}]}]"#,
        Regions {
            eu_west_2: r#"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found"}"#,
            us_east_1: r#"{"__type":"com.amazon.coral.service#SerializationException"}"#,
            follows: Region::EuWest2,
        },
    ),
    (
        12,
        "TransactGetItems",
        r#"[{}]"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        13,
        "ListTables",
        r#"[{}]"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        14,
        "DescribeTable",
        r#"[{}]"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        15,
        "BatchExecuteStatement",
        r#"[[{"Statement":"SELECT * FROM \"_capture_20261111_317_none\""}]]"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        16,
        "ExecuteTransaction",
        r#"[[{"Statement":"SELECT * FROM \"_capture_20261111_317_none\" WHERE pk='a'"}]]"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        17,
        "CreateTable",
        r#"{"TableName":"_capture_20261111_317_nonex","AttributeDefinitions":[["pk","S"]],"KeySchema":[["pk","HASH"]],"BillingMode":"PAY_PER_REQUEST"}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        18,
        "CreateTable",
        r#"{"TableName":"_capture_20261111_317_nonex","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],"ProvisionedThroughput":[1,1]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        19,
        "CreateTable",
        r#"{"TableName":"_capture_20261111_317_nonex","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],"BillingMode":"PAY_PER_REQUEST","StreamSpecification":[true,"NEW_IMAGE"]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        20,
        "CreateTable",
        r#"{"TableName":"_capture_20261111_317_nonex","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],"BillingMode":"PAY_PER_REQUEST","SSESpecification":[true]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        21,
        "CreateTable",
        r#"{"TableName":"_capture_20261111_317_nonex","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],"BillingMode":"PAY_PER_REQUEST","Tags":[["k","v"]]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        22,
        "CreateTable",
        r#"{"TableName":"_capture_20261111_317_nonex","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],"BillingMode":"PAY_PER_REQUEST","GlobalSecondaryIndexes":[["g",[["pk","HASH"]],["ALL"]]]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        23,
        "TransactWriteItems",
        r#"{"TransactItems":[{"Put":["_capture_20261111_317_none",{"pk":{"S":"a"}}]}]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        24,
        "TransactWriteItems",
        r#"{"TransactItems":[[{"TableName":"_capture_20261111_317_none","Item":{"pk":{"S":"a"}}}]]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        25,
        "TransactGetItems",
        r#"{"TransactItems":[{"Get":[{"pk":{"S":"a"}},"_capture_20261111_317_none"]}]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        26,
        "BatchGetItem",
        r#"{"RequestItems":{"_capture_20261111_317_none":[[{"pk":{"S":"a"}}]]}}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        27,
        "BatchWriteItem",
        r#"{"RequestItems":{"_capture_20261111_317_none":[[{"Item":{"pk":{"S":"a"}}}]]}}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        28,
        "BatchWriteItem",
        r#"{"RequestItems":{"_capture_20261111_317_none":[{"PutRequest":[{"pk":{"S":"a"}}]}]}}"#,
        Regions {
            eu_west_2: r#"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found"}"#,
            us_east_1: r#"{"__type":"com.amazon.coral.service#SerializationException"}"#,
            follows: Region::EuWest2,
        },
    ),
    (
        29,
        "BatchWriteItem",
        r#"{"RequestItems":{"_capture_20261111_317_none":[{"DeleteRequest":[{"pk":{"S":"a"}}]}]}}"#,
        Regions {
            eu_west_2: r#"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found"}"#,
            us_east_1: r#"{"__type":"com.amazon.coral.service#SerializationException"}"#,
            follows: Region::EuWest2,
        },
    ),
    (
        30,
        "PutItem",
        r#"{"TableName":"_capture_20261111_317_none","Item":{"pk":["a"]}}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        31,
        "PutItem",
        r#"{"TableName":"_capture_20261111_317_none","Item":{"pk":{"S":"a"}},"Expected":{"pk":[true]}}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        32,
        "Query",
        r#"{"TableName":"_capture_20261111_317_none","KeyConditions":{"pk":[[{"S":"a"}],"EQ"]}}"#,
        Same(
            r#"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found"}"#,
        ),
    ),
    (
        33,
        "Scan",
        r#"{"TableName":"_capture_20261111_317_none","ScanFilter":{"pk":[[{"S":"a"}],"EQ"]}}"#,
        Same(
            r#"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found"}"#,
        ),
    ),
    (
        34,
        "UpdateItem",
        r#"{"TableName":"_capture_20261111_317_none","Key":{"pk":{"S":"a"}},"AttributeUpdates":{"x":[{"S":"a"},"PUT"]}}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        35,
        "BatchExecuteStatement",
        r#"{"Statements":[["SELECT * FROM \"_capture_20261111_317_none\""]]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        36,
        "ExecuteTransaction",
        r#"{"TransactStatements":[["SELECT * FROM \"_capture_20261111_317_none\" WHERE pk='a'"]]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        37,
        "UpdateTable",
        r#"{"TableName":"_capture_20261111_317_none","ProvisionedThroughput":[1,1]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        38,
        "UpdateTable",
        r#"{"TableName":"_capture_20261111_317_none","GlobalSecondaryIndexUpdates":[[{"IndexName":"g"}]]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        39,
        "UpdateTimeToLive",
        r#"{"TableName":"_capture_20261111_317_none","TimeToLiveSpecification":[true,"ttl"]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        40,
        "Bogus",
        r#"[]"#,
        Same(r#"{"__type":"com.amazon.coral.service#UnknownOperationException"}"#),
    ),
    (
        41,
        "Bogus",
        r#""x""#,
        Same(r#"{"__type":"com.amazon.coral.service#UnknownOperationException"}"#),
    ),
    (
        42,
        "Bogus",
        r#"null"#,
        Same(r#"{"__type":"com.amazon.coral.service#UnknownOperationException"}"#),
    ),
];

const THIRD_ROUND: &[Row] = &[
    (
        1,
        "Query",
        r#"{"TableName":"_capture_20261112_arr","KeyConditions":{"pk":[[{"S":"a"}],"EQ"]}}"#,
        Same(r#"{"Count":1,"Items":[{"pk":{"S":"a"},"x":{"N":"1"}}],"ScannedCount":1}"#),
    ),
    (
        2,
        "Query",
        r#"{"TableName":"_capture_20261112_arr","KeyConditions":{"pk":["EQ",[{"S":"a"}]]}}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        3,
        "Query",
        r#"{"TableName":"_capture_20261112_arr","KeyConditions":{"pk":[]}}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        4,
        "Query",
        r#"{"TableName":"_capture_20261112_arr","KeyConditions":{"pk":[[{"S":"a"}]]}}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        5,
        "Query",
        r#"{"TableName":"_capture_20261112_arr","KeyConditions":{"pk":[[{"S":"a"}],"EQ","extra"]}}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        6,
        "Query",
        r#"{"TableName":"_capture_20261112_arr","KeyConditions":{"pk":{"AttributeValueList":[{"S":"a"}],"ComparisonOperator":"EQ"}},"QueryFilter":{"x":[[{"N":"1"}],"EQ"]}}"#,
        Same(r#"{"Count":1,"Items":[{"pk":{"S":"a"},"x":{"N":"1"}}],"ScannedCount":1}"#),
    ),
    (
        7,
        "Query",
        r#"{"TableName":"_capture_20261112_arr","KeyConditions":{"pk":{"AttributeValueList":[{"S":"a"}],"ComparisonOperator":"EQ"}},"QueryFilter":{"x":[[{"N":"2"}],"EQ"]}}"#,
        Same(r#"{"Count":0,"Items":[],"ScannedCount":1}"#),
    ),
    (
        8,
        "Scan",
        r#"{"TableName":"_capture_20261112_arr","ScanFilter":{"x":[[{"N":"1"}],"EQ"]}}"#,
        Same(r#"{"Count":1,"Items":[{"pk":{"S":"a"},"x":{"N":"1"}}],"ScannedCount":1}"#),
    ),
    (
        9,
        "Scan",
        r#"{"TableName":"_capture_20261112_arr","ScanFilter":{"x":[[{"N":"2"}],"EQ"]}}"#,
        Same(r#"{"Count":0,"Items":[],"ScannedCount":1}"#),
    ),
    (
        10,
        "Scan",
        r#"{"TableName":"_capture_20261112_arr","ScanFilter":{"x":[]}}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        11,
        "BatchGetItem",
        r#"[{"_capture_20261112_arr":{"Keys":[{"pk":{"S":"a"}}]}},"TOTAL"]"#,
        Same(
            r#"{"ConsumedCapacity":[{"CapacityUnits":0.5,"TableName":"_capture_20261112_arr"}],"Responses":{"_capture_20261112_arr":[{"pk":{"S":"a"},"x":{"N":"1"}}]},"UnprocessedKeys":{}}"#,
        ),
    ),
    (
        12,
        "BatchGetItem",
        r#"[{"_capture_20261112_arr":{"Keys":[{"pk":{"S":"a"}}]}},"TOTAL","extra"]"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        13,
        "BatchGetItem",
        r#"[{"_capture_20261112_arr":[[{"pk":{"S":"a"}}]]}]"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        14,
        "BatchGetItem",
        r#"{"RequestItems":{"_capture_20261112_arr":[[{"pk":{"S":"a"}}]]}}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        15,
        "BatchWriteItem",
        r#"[{"_capture_20261112_arr":[{"PutRequest":{"Item":{"pk":{"S":"b"}}}}]}]"#,
        Regions {
            eu_west_2: r#"{"UnprocessedItems":{}}"#,
            us_east_1: r#"{"__type":"com.amazon.coral.service#SerializationException"}"#,
            follows: Region::EuWest2,
        },
    ),
    (
        16,
        "BatchWriteItem",
        r#"{"RequestItems":{"_capture_20261112_arr":[{"PutRequest":[{"pk":{"S":"c"}}]}]}}"#,
        Regions {
            eu_west_2: r#"{"UnprocessedItems":{}}"#,
            us_east_1: r#"{"__type":"com.amazon.coral.service#SerializationException"}"#,
            follows: Region::EuWest2,
        },
    ),
    (
        17,
        "BatchWriteItem",
        r#"{"RequestItems":{"_capture_20261112_arr":[{"DeleteRequest":[{"pk":{"S":"a"}}]}]}}"#,
        Regions {
            eu_west_2: r#"{"UnprocessedItems":{}}"#,
            us_east_1: r#"{"__type":"com.amazon.coral.service#SerializationException"}"#,
            follows: Region::EuWest2,
        },
    ),
    (
        18,
        "Scan",
        r#"{"TableName":"_capture_20261112_arr"}"#,
        Regions {
            eu_west_2: r#"{"Count":2,"Items":[{"pk":{"S":"b"}},{"pk":{"S":"c"}}],"ScannedCount":2}"#,
            us_east_1: r#"{"Count":1,"Items":[{"pk":{"S":"a"},"x":{"N":"1"}}],"ScannedCount":1}"#,
            follows: Region::EuWest2,
        },
    ),
];

const FOURTH_ROUND: &[Row] = &[
    (
        1,
        "PutItem",
        r#"{"TableName":["_capture_20261111_317_none"],"Item":{"pk":{"S":"a"}}}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        2,
        "GetItem",
        r#"{"TableName":["_capture_20261111_317_none"],"Key":{"pk":{"S":"a"}}}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        3,
        "DeleteItem",
        r#"{"TableName":["_capture_20261111_317_none"],"Key":{"pk":{"S":"a"}}}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        4,
        "UpdateItem",
        r#"{"TableName":["_capture_20261111_317_none"],"Key":{"pk":{"S":"a"}}}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        5,
        "Query",
        r#"{"TableName":["_capture_20261111_317_none"]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        6,
        "Scan",
        r#"{"TableName":["_capture_20261111_317_none"]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        7,
        "DescribeTable",
        r#"{"TableName":["_capture_20261111_317_none"]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        8,
        "DeleteTable",
        r#"{"TableName":["_capture_20261111_317_none"]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        9,
        "CreateTable",
        r#"{"TableName":["_capture_20261111_317_nonex"],"AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],"BillingMode":"PAY_PER_REQUEST"}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        10,
        "CreateTable",
        r#"{"TableName":"_capture_20261111_317_nonex","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],"BillingMode":["PAY_PER_REQUEST"]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        11,
        "CreateTable",
        r#"{"TableName":"_capture_20261111_317_nonex","AttributeDefinitions":[{"AttributeName":["pk"],"AttributeType":"S"}],"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],"BillingMode":"PAY_PER_REQUEST"}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        12,
        "CreateTable",
        r#"{"TableName":"_capture_20261111_317_nonex","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],"ProvisionedThroughput":{"ReadCapacityUnits":[1],"WriteCapacityUnits":1}}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        13,
        "CreateTable",
        r#"{"TableName":"_capture_20261111_317_nonex","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],"BillingMode":"PAY_PER_REQUEST","StreamSpecification":{"StreamEnabled":[true]}}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        14,
        "ListTables",
        r#"{"Limit":[1]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        15,
        "ListTables",
        r#"{"ExclusiveStartTableName":["x"]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        16,
        "Query",
        r#"{"TableName":"_capture_20261111_317_none","Limit":[1]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        17,
        "Query",
        r#"{"TableName":"_capture_20261111_317_none","Select":["COUNT"]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        18,
        "Query",
        r#"{"TableName":"_capture_20261111_317_none","KeyConditionExpression":["pk = :a"]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        19,
        "Query",
        r#"{"TableName":"_capture_20261111_317_none","ScanIndexForward":[true]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        20,
        "Scan",
        r#"{"TableName":"_capture_20261111_317_none","Limit":[1]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        21,
        "Scan",
        r#"{"TableName":"_capture_20261111_317_none","Segment":[0],"TotalSegments":1}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        22,
        "Scan",
        r#"{"TableName":"_capture_20261111_317_none","FilterExpression":["a = :a"]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        23,
        "PutItem",
        r#"{"TableName":"_capture_20261111_317_none","Item":{"pk":{"S":"a"}},"ReturnValues":["NONE"]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        24,
        "PutItem",
        r#"{"TableName":"_capture_20261111_317_none","Item":{"pk":{"S":"a"}},"ConditionExpression":["a = :a"]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        25,
        "PutItem",
        r##"{"TableName":"_capture_20261111_317_none","Item":{"pk":{"S":"a"}},"ExpressionAttributeNames":{"#a":["x"]}}"##,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        26,
        "GetItem",
        r#"{"TableName":"_capture_20261111_317_none","Key":{"pk":{"S":"a"}},"ProjectionExpression":["a"]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        27,
        "GetItem",
        r#"{"TableName":"_capture_20261111_317_none","Key":{"pk":{"S":"a"}},"AttributesToGet":[["a"]]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        28,
        "UpdateItem",
        r#"{"TableName":"_capture_20261111_317_none","Key":{"pk":{"S":"a"}},"UpdateExpression":["SET a = :a"]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        29,
        "BatchGetItem",
        r#"{"RequestItems":{"_capture_20261111_317_none":{"Keys":[{"pk":{"S":"a"}}],"ConsistentRead":[true]}}}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        30,
        "BatchGetItem",
        r#"{"RequestItems":{"_capture_20261111_317_none":{"Keys":[{"pk":{"S":"a"}}]}},"ReturnConsumedCapacity":["TOTAL"]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        31,
        "BatchWriteItem",
        r#"{"RequestItems":{"_capture_20261111_317_none":[{"PutRequest":{"Item":{"pk":{"S":"a"}}}}]},"ReturnConsumedCapacity":["TOTAL"]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        32,
        "TransactWriteItems",
        r#"{"TransactItems":[{"Put":{"TableName":["_capture_20261111_317_none"],"Item":{"pk":{"S":"a"}}}}]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        33,
        "TransactWriteItems",
        r#"{"TransactItems":[{"Put":{"TableName":"_capture_20261111_317_none","Item":{"pk":{"S":"a"}}}}],"ClientRequestToken":["x"]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        34,
        "TransactGetItems",
        r#"{"TransactItems":[{"Get":{"TableName":["_capture_20261111_317_none"],"Key":{"pk":{"S":"a"}}}}]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        35,
        "ExecuteStatement",
        r#"{"Statement":["SELECT * FROM \"_capture_20261111_317_none\""]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        36,
        "ExecuteStatement",
        r#"{"Statement":"SELECT * FROM \"_capture_20261111_317_none\"","Limit":[1]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        37,
        "BatchExecuteStatement",
        r#"{"Statements":[{"Statement":["SELECT * FROM \"_capture_20261111_317_none\""]}]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        38,
        "UpdateTimeToLive",
        r#"{"TableName":"_capture_20261111_317_none","TimeToLiveSpecification":{"Enabled":[true],"AttributeName":"ttl"}}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        39,
        "PutItem",
        r#"{"TableName":"_capture_20261111_317_none","Item":{"pk":{"S":["a"]}}}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        40,
        "PutItem",
        r#"{"TableName":"_capture_20261111_317_none","Item":{"pk":{"N":["1"]}}}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        41,
        "PutItem",
        r#"{"TableName":"_capture_20261111_317_none","Item":{"pk":{"S":"a"},"b":{"BOOL":[true]}}}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        42,
        "Query",
        r#"{"TableName":"_capture_20261111_317_none","KeyConditions":{"pk":{"AttributeValueList":[{"S":"a"}],"ComparisonOperator":["EQ"]}}}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
];

const FIFTH_ROUND: &[Row] = &[
    (
        1,
        "PutItem",
        r#"{"TableName":"_capture_20261111_317_none","Item":[]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        2,
        "PutItem",
        r#"{"TableName":"_capture_20261111_317_none","Item":[{"pk":{"S":"a"}}]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        3,
        "GetItem",
        r#"{"TableName":"_capture_20261111_317_none","Key":[]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        4,
        "DeleteItem",
        r#"{"TableName":"_capture_20261111_317_none","Key":[{"pk":{"S":"a"}}]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        5,
        "UpdateItem",
        r#"{"TableName":"_capture_20261111_317_none","Key":[]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        6,
        "PutItem",
        r#"{"TableName":"_capture_20261111_317_none","Item":{"pk":{"S":"a"}},"ExpressionAttributeValues":[]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        7,
        "PutItem",
        r#"{"TableName":"_capture_20261111_317_none","Item":{"pk":{"S":"a"}},"ExpressionAttributeNames":[]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        8,
        "PutItem",
        r#"{"TableName":"_capture_20261111_317_none","Item":{"pk":{"S":"a"}},"Expected":[]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        9,
        "Query",
        r#"{"TableName":"_capture_20261111_317_none","KeyConditions":[]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        10,
        "Query",
        r#"{"TableName":"_capture_20261111_317_none","ExpressionAttributeValues":[]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        11,
        "Query",
        r#"{"TableName":"_capture_20261111_317_none","ExclusiveStartKey":[]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        12,
        "Scan",
        r#"{"TableName":"_capture_20261111_317_none","ScanFilter":[]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        13,
        "Scan",
        r#"{"TableName":"_capture_20261111_317_none","ExpressionAttributeNames":[]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        14,
        "UpdateItem",
        r#"{"TableName":"_capture_20261111_317_none","Key":{"pk":{"S":"a"}},"AttributeUpdates":[]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        15,
        "BatchGetItem",
        r#"{"RequestItems":[]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        16,
        "BatchWriteItem",
        r#"{"RequestItems":[]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        17,
        "BatchGetItem",
        r#"{"RequestItems":{"_capture_20261111_317_none":{"Keys":[[]]}}}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        18,
        "BatchGetItem",
        r#"{"RequestItems":{"_capture_20261111_317_none":{"Keys":[{"pk":{"S":"a"}}],"ExpressionAttributeNames":[]}}}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        19,
        "BatchWriteItem",
        r#"{"RequestItems":{"_capture_20261111_317_none":[{"PutRequest":{"Item":[]}}]}}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        20,
        "TransactWriteItems",
        r#"{"TransactItems":[{"Put":{"TableName":"_capture_20261111_317_none","Item":[]}}]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        21,
        "TransactGetItems",
        r#"{"TransactItems":[{"Get":{"TableName":"_capture_20261111_317_none","Key":[]}}]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        22,
        "PutItem",
        r#"{"TableName":"_capture_20261111_317_none","Item":{"pk":{"S":"a"},"m":{"M":[]}}}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
];

const SIXTH_ROUND: &[Row] = &[
    (
        1,
        "Query",
        r#"{"TableName":"_capture_20261111_317_none","KeyConditions":{"pk":[[{"S":"a"}],5]}}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        2,
        "Query",
        r#"{"TableName":"_capture_20261111_317_none","KeyConditions":{"pk":["x","EQ"]}}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        3,
        "Query",
        r#"{"TableName":"_capture_20261111_317_none","KeyConditions":{"pk":[[{"S":"a"}],"BOGUS"]}}"#,
        Regions {
            eu_west_2: r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Value 'BOGUS' at 'keyConditions.pk.member.comparisonOperator' failed to satisfy constraint: Member must satisfy enum value set: [IN, NULL, BETWEEN, LT, NOT_CONTAINS, EQ, GT, NOT_NULL, NE, LE, BEGINS_WITH, GE, CONTAINS]"}"#,
            us_east_1: r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"One or more parameter values were invalid: Invalid comparison operator"}"#,
            // The object form's wording for an operator outside the enum,
            // which Dynoxide gives in us-east-1's words.
            follows: Region::UsEast1,
        },
    ),
    (
        4,
        "Query",
        r#"{"TableName":"_capture_20261111_317_none","KeyConditions":{"pk":[[{"S":"a"}],null]}}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        5,
        "Query",
        r#"{"TableName":"_capture_20261111_317_none","KeyConditions":{"pk":[null,"EQ"]}}"#,
        Same(
            r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"One or more parameter values were invalid: Invalid number of argument(s) for the EQ ComparisonOperator"}"#,
        ),
    ),
    (
        6,
        "Scan",
        r#"{"TableName":"_capture_20261111_317_none","ScanFilter":{"x":[[{"S":"a"}],{}]}}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        7,
        "Query",
        r#"{"TableName":"_capture_20261111_317_none","KeyConditions":{"pk":{"AttributeValueList":[{"S":"a"}],"ComparisonOperator":5}}}"#,
        Same(
            r#"{"__type":"com.amazon.coral.service#SerializationException","Message":"NUMBER_VALUE cannot be converted to String"}"#,
        ),
    ),
    (
        8,
        "Query",
        r#"{"TableName":"_capture_20261111_317_none","KeyConditions":{"pk":{"AttributeValueList":"x","ComparisonOperator":"EQ"}}}"#,
        Same(
            r#"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected field type"}"#,
        ),
    ),
    (
        9,
        "BatchGetItem",
        r#"[{"_capture_20261111_317_none":{"Keys":[{"pk":{"S":"a"}}]}},5]"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        10,
        "BatchGetItem",
        r#"[{"_capture_20261111_317_none":{"Keys":[{"pk":{"S":"a"}}]}},null]"#,
        Same(
            r#"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found"}"#,
        ),
    ),
    (
        11,
        "BatchGetItem",
        r#"[null]"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        12,
        "BatchGetItem",
        r#"[null,"TOTAL"]"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
];
const SEVENTH_ROUND: &[Row] = &[
    (
        1,
        "TransactGetItems",
        r#"{"TransactItems":[[]]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        2,
        "TransactGetItems",
        r#"{"TransactItems":[{"Get":[]}]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        3,
        "TransactGetItems",
        r#"{"TransactItems":[[],{"Get":{"TableName":"_capture_20261111_317_none","Key":{"pk":{"S":"a"}}}}]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        4,
        "TransactWriteItems",
        r#"{"TransactItems":[[]]}"#,
        Same(
            r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value '' at 'transactItems' failed to satisfy constraint: TransactWriteRequest should contain Delete or Put or Update or ConditionCheck"}"#,
        ),
    ),
    (
        5,
        "UpdateTable",
        r#"{"TableName":"_capture_20261111_317_none","StreamSpecification":[true,"NEW_IMAGE"]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        6,
        "UpdateTable",
        r#"{"TableName":"_capture_20261111_317_none","SSESpecification":[true]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        7,
        "UpdateTable",
        r#"{"TableName":"_capture_20261111_317_none","SSESpecification":[true,"KMS","k"]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        8,
        "UpdateTable",
        r#"{"TableName":"_capture_20261111_317_none","AttributeDefinitions":[["pk","S"]]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        9,
        "UpdateTable",
        r#"{"TableName":"_capture_20261111_317_none","GlobalSecondaryIndexUpdates":[{"Create":["g",[{"AttributeName":"pk","KeyType":"HASH"}],{"ProjectionType":"ALL"}]}]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        10,
        "UpdateTable",
        r#"{"TableName":"_capture_20261111_317_none","GlobalSecondaryIndexUpdates":[{"Delete":["g"]}]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        11,
        "UpdateTable",
        r#"{"TableName":"_capture_20261111_317_none","GlobalSecondaryIndexUpdates":[{"Update":["g",{"ReadCapacityUnits":1,"WriteCapacityUnits":1}]}]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        12,
        "UpdateTable",
        r#"{"TableName":"_capture_20261111_317_none","OnDemandThroughput":[1,1]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        13,
        "UpdateTable",
        r#"{"TableName":"_capture_20261111_317_none","ReplicaUpdates":[[{"RegionName":"eu-west-1"}]]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        14,
        "CreateTable",
        r#"{"TableName":"_capture_20261111_317_nonex","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],"BillingMode":"PAY_PER_REQUEST","GlobalSecondaryIndexes":[{"IndexName":"g","KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],"Projection":["ALL"]}]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        15,
        "CreateTable",
        r#"{"TableName":"_capture_20261111_317_nonex","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],"BillingMode":"PAY_PER_REQUEST","GlobalSecondaryIndexes":[{"IndexName":"g","KeySchema":[["pk","HASH"]],"Projection":{"ProjectionType":"ALL"}}]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        16,
        "CreateTable",
        r#"{"TableName":"_capture_20261111_317_nonex","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],"BillingMode":"PAY_PER_REQUEST","LocalSecondaryIndexes":[["l",[{"AttributeName":"pk","KeyType":"HASH"}],{"ProjectionType":"ALL"}]]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        17,
        "CreateTable",
        r#"{"TableName":"_capture_20261111_317_nonex","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],"BillingMode":"PAY_PER_REQUEST","OnDemandThroughput":[1,1]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        18,
        "CreateTable",
        r#"{"TableName":"_capture_20261111_317_nonex","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],"BillingMode":"PAY_PER_REQUEST","StreamSpecification":[true]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        19,
        "CreateTable",
        r#"{"TableName":"_capture_20261111_317_nonex","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],"BillingMode":"PAY_PER_REQUEST","SSESpecification":[true,"KMS","k"]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        20,
        "TagResource",
        r#"{"ResourceArn":"arn:aws:dynamodb:eu-west-2:000000000000:table/_capture_20261111_317_none","Tags":[["k","v"]]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        21,
        "UpdateContinuousBackups",
        r#"{"TableName":"_capture_20261111_317_none","PointInTimeRecoverySpecification":[true]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        22,
        "ExecuteStatement",
        r#"{"Statement":"SELECT * FROM \"_capture_20261111_317_none\" WHERE pk = ?","Parameters":[["a"]]}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        23,
        "BatchWriteItem",
        r#"[{"_capture_20261111_317_none":[{"PutRequest":{"Item":{"pk":{"S":"a"}}}}]},"TOTAL","SIZE"]"#,
        Same(
            r#"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found"}"#,
        ),
    ),
    (
        24,
        "BatchWriteItem",
        r#"[{"_capture_20261111_317_none":[{"PutRequest":{"Item":{"pk":{"S":"a"}}}}]},"TOTAL","SIZE","x"]"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        25,
        "BatchWriteItem",
        r#"[{"_capture_20261111_317_none":[{"PutRequest":{"Item":{"pk":{"S":"a"}}}}]},"TOTAL","BOGUS"]"#,
        Same(
            r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value 'BOGUS' at 'returnItemCollectionMetrics' failed to satisfy constraint: Member must satisfy enum value set: [SIZE, NONE]"}"#,
        ),
    ),
    (
        26,
        "BatchWriteItem",
        r#"[{"_capture_20261111_317_none":[{"PutRequest":{"Item":{"pk":{"S":"a"}}}}]},"BOGUS"]"#,
        Same(
            r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value 'BOGUS' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"}"#,
        ),
    ),
    (
        27,
        "BatchWriteItem",
        r#"{"RequestItems":{"_capture_20261111_317_none":[{"PutRequest":[{"pk":{"S":"a"}},"x"]}]}}"#,
        Same(
            r#"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found"}"#,
        ),
    ),
    (
        28,
        "BatchWriteItem",
        r#"{"RequestItems":{"_capture_20261111_317_none":[{"PutRequest":[]}]}}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        29,
        "BatchWriteItem",
        r#"{"RequestItems":{"_capture_20261111_317_none":[{"DeleteRequest":[null]}]}}"#,
        Same(r#"{"__type":"com.amazon.coral.service#SerializationException"}"#),
    ),
    (
        30,
        "BatchWriteItem",
        r#"{"RequestItems":{}}"#,
        Same(
            r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'RequestItems' failed to satisfy constraint: Member must have length greater than or equal to 1"}"#,
        ),
    ),
    (
        31,
        "BatchGetItem",
        r#"{"RequestItems":{}}"#,
        Same(
            r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'RequestItems' failed to satisfy constraint: Member must have length greater than or equal to 1"}"#,
        ),
    ),
];
