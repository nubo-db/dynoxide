//! `ClientRequestToken` on TransactWriteItems and ExecuteTransaction: its
//! length at both ends, and where its refusal stands among the request's other
//! member errors.
//!
//! TransactWriteItems checks its members in stages and answers with the first
//! that finds anything. A required member left null comes first, listed in the
//! newer form with the token after it (`Value '' at 'clientRequestToken'`).
//! Then the token's length and `TransactItems`' length, in the older form with
//! no value echoed (`Value at 'ClientRequestToken'`), reported alone. Then an
//! item naming no action, or else the table names, with the request's enums;
//! then an item naming two actions; then a value refused in an item; then an
//! action's `ReturnValuesOnConditionCheckFailure`. ExecuteTransaction lists the token
//! first among its member errors, always in the newer form. A token of a
//! single space or 36 characters is accepted.
//!
//! The rows below are the requests sent to eu-west-2 and us-east-1 on
//! 11 November 2026 (the seventh round to eu-west-1 and us-west-2 as well) and
//! the answers DynamoDB gave, which the regions agreed on row for row. They are data captured from DynamoDB, not written by hand.
//! Every request names a table that doesn't exist. `PUTS_101` stands for a
//! list of 101 copies of the Put in row 1, which is what those rows sent.

#![cfg(feature = "http-server")]

use dynoxide::Database;

/// A captured row: its number in the round, the operation, the request body,
/// and DynamoDB's answer, every one of them refused with HTTP 400.
type Row = (usize, &'static str, &'static str, &'static str);

const PUT: &str =
    r#"{"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}}}"#;

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

/// Rows left out because they turn on a different gap: DynamoDB answers a
/// list given where a string belongs with a `SerializationException` that has
/// no message, for every member and not only this one. Each is still sent: it
/// must still differ from DynamoDB's answer, and give the answer recorded
/// here, which is Dynoxide's answer today.
const LIST_FOR_A_STRING: &[(&str, usize, &str)] = &[
    (
        "first round",
        9,
        r#"{"Message":"Unrecognized collection type class java.lang.String","__type":"com.amazon.coral.service#SerializationException"}"#,
    ),
    (
        "first round",
        20,
        r#"{"Message":"Unrecognized collection type class java.lang.String","__type":"com.amazon.coral.service#SerializationException"}"#,
    ),
];

async fn assert_rows(url: &str, round: &str, rows: &[Row]) {
    let puts: String = format!("[{}]", vec![PUT; 101].join(", "));
    let mut wrong = Vec::new();
    let mut compared = 0;
    let mut left_out = 0;
    for &(number, operation, body, answer) in rows {
        let body = body.replace("PUTS_101", &puts);
        let expected: serde_json::Value = serde_json::from_str(answer).unwrap();
        let (status, got) = send(url, operation, &body).await;
        compared += 1;
        if let Some(&(_, _, today)) = LIST_FOR_A_STRING
            .iter()
            .find(|&&(r, n, _)| r == round && n == number)
        {
            left_out += 1;
            let today: serde_json::Value = serde_json::from_str(today).unwrap();
            if got == expected {
                wrong.push(format!(
                    "{round} row {number} now answers as DynamoDB did: drop it from \
                     LIST_FOR_A_STRING"
                ));
            } else if status != 400 || got != today {
                wrong.push(format!(
                    "{round} row {number}: {operation}\n  expected 400 {today}\n  got      {status} {got}"
                ));
            }
            continue;
        }
        if status != 400 || got != expected {
            let shown = if body.len() > 300 {
                &body[..300]
            } else {
                &body
            };
            wrong.push(format!(
                "{round} row {number}: {operation} {shown}\n  expected 400 {expected}\n  got      {status} {got}"
            ));
        }
    }
    assert_eq!(compared, rows.len());
    assert_eq!(
        left_out,
        LIST_FOR_A_STRING
            .iter()
            .filter(|&&(r, _, _)| r == round)
            .count(),
        "every row left out of {round} must still be sent"
    );
    assert!(
        wrong.is_empty(),
        "{} of {compared} rows in the {round} differ:\n{}",
        wrong.len(),
        wrong.join("\n")
    );
}

#[tokio::test]
async fn the_first_round_answers_as_dynamodb_did() {
    let url = start_server().await;
    assert_rows(&url, "first round", FIRST_ROUND).await;
}

#[tokio::test]
async fn the_second_round_answers_as_dynamodb_did() {
    let url = start_server().await;
    assert_rows(&url, "second round", SECOND_ROUND).await;
}

#[tokio::test]
async fn the_third_round_answers_as_dynamodb_did() {
    let url = start_server().await;
    assert_rows(&url, "third round", THIRD_ROUND).await;
}

#[tokio::test]
async fn the_fourth_round_answers_as_dynamodb_did() {
    let url = start_server().await;
    assert_rows(&url, "fourth round", FOURTH_ROUND).await;
}

#[tokio::test]
async fn the_fifth_round_answers_as_dynamodb_did() {
    let url = start_server().await;
    assert_rows(&url, "fifth round", FIFTH_ROUND).await;
}

#[tokio::test]
async fn the_sixth_round_answers_as_dynamodb_did() {
    let url = start_server().await;
    assert_rows(&url, "sixth round", SIXTH_ROUND).await;
}

#[tokio::test]
async fn the_seventh_round_answers_as_dynamodb_did() {
    let url = start_server().await;
    assert_rows(&url, "seventh round", SEVENTH_ROUND).await;
}

/// A number refused in an item waits for the stages checked as the request is
/// read: an empty token beside it and a bad `ReturnConsumedCapacity` is
/// reported alone, in the older form (fifth round row 14).
#[tokio::test]
async fn a_refused_number_waits_for_the_token_and_the_enums() {
    let url = start_server().await;
    let (status, got) = send(
        &url,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"N": "abc"}}}}], "ClientRequestToken": "", "ReturnConsumedCapacity": "BAD"}"#,
    )
    .await;
    assert_eq!(status, 400);
    assert_eq!(
        got,
        serde_json::json!({
            "__type": "com.amazon.coral.validate#ValidationException",
            "message": "1 validation error detected: Value at 'ClientRequestToken' failed to \
                        satisfy constraint: Member must have length greater than or equal to 1"
        })
    );
}

/// The token at its lengths and beside the request's other members, with the
/// answers to ExecuteTransaction and TransactGetItems for comparison.
const FIRST_ROUND: &[Row] = &[
    (
        1,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}}}], "ClientRequestToken": ""}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'ClientRequestToken' failed to satisfy constraint: Member must have length greater than or equal to 1"}"#,
    ),
    (
        2,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}}}], "ClientRequestToken": "", "ReturnConsumedCapacity": "BAD"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'ClientRequestToken' failed to satisfy constraint: Member must have length greater than or equal to 1"}"#,
    ),
    (
        3,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}}}], "ClientRequestToken": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'ClientRequestToken' failed to satisfy constraint: Member must have length less than or equal to 36"}"#,
    ),
    (
        4,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}}}], "ClientRequestToken": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", "ReturnConsumedCapacity": "BAD"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'ClientRequestToken' failed to satisfy constraint: Member must have length less than or equal to 36"}"#,
    ),
    (
        5,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}}}], "ClientRequestToken": null}"#,
        r#"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found"}"#,
    ),
    (
        6,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}}}], "ClientRequestToken": 5}"#,
        r#"{"__type":"com.amazon.coral.service#SerializationException","Message":"NUMBER_VALUE cannot be converted to String"}"#,
    ),
    (
        7,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}}}], "ClientRequestToken": " "}"#,
        r#"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found"}"#,
    ),
    (
        8,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}}}], "ClientRequestToken": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}"#,
        r#"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found"}"#,
    ),
    (
        9,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}}}], "ClientRequestToken": []}"#,
        r#"{"__type":"com.amazon.coral.service#SerializationException"}"#,
    ),
    (
        10,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}}}], "ClientRequestToken": {}}"#,
        r#"{"__type":"com.amazon.coral.service#SerializationException","Message":"Start of structure or map found where not expected"}"#,
    ),
    (
        11,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}}}], "ClientRequestToken": true}"#,
        r#"{"__type":"com.amazon.coral.service#SerializationException","Message":"TRUE_VALUE cannot be converted to String"}"#,
    ),
    (
        12,
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "SELECT * FROM \"_capture_20261111_317_none\" WHERE pk = 'a'"}], "ClientRequestToken": ""}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value '' at 'clientRequestToken' failed to satisfy constraint: Member must have length greater than or equal to 1"}"#,
    ),
    (
        13,
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "SELECT * FROM \"_capture_20261111_317_none\" WHERE pk = 'a'"}], "ClientRequestToken": "", "ReturnConsumedCapacity": "BAD"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value '' at 'clientRequestToken' failed to satisfy constraint: Member must have length greater than or equal to 1; Value 'BAD' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"}"#,
    ),
    (
        14,
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "SELECT * FROM \"_capture_20261111_317_none\" WHERE pk = 'a'"}], "ClientRequestToken": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx' at 'clientRequestToken' failed to satisfy constraint: Member must have length less than or equal to 36"}"#,
    ),
    (
        15,
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "SELECT * FROM \"_capture_20261111_317_none\" WHERE pk = 'a'"}], "ClientRequestToken": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", "ReturnConsumedCapacity": "BAD"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx' at 'clientRequestToken' failed to satisfy constraint: Member must have length less than or equal to 36; Value 'BAD' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"}"#,
    ),
    (
        16,
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "SELECT * FROM \"_capture_20261111_317_none\" WHERE pk = 'a'"}], "ClientRequestToken": null}"#,
        r#"{"__type":"com.amazonaws.dynamodb.v20120810#TransactionCanceledException","CancellationReasons":[{"Code":"ResourceNotFound","Message":"Requested resource not found"}],"Message":"Transaction cancelled, please refer cancellation reasons for specific reasons [ResourceNotFound]"}"#,
    ),
    (
        17,
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "SELECT * FROM \"_capture_20261111_317_none\" WHERE pk = 'a'"}], "ClientRequestToken": 5}"#,
        r#"{"__type":"com.amazon.coral.service#SerializationException","Message":"NUMBER_VALUE cannot be converted to String"}"#,
    ),
    (
        18,
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "SELECT * FROM \"_capture_20261111_317_none\" WHERE pk = 'a'"}], "ClientRequestToken": " "}"#,
        r#"{"__type":"com.amazonaws.dynamodb.v20120810#TransactionCanceledException","CancellationReasons":[{"Code":"ResourceNotFound","Message":"Requested resource not found"}],"Message":"Transaction cancelled, please refer cancellation reasons for specific reasons [ResourceNotFound]"}"#,
    ),
    (
        19,
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "SELECT * FROM \"_capture_20261111_317_none\" WHERE pk = 'a'"}], "ClientRequestToken": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}"#,
        r#"{"__type":"com.amazonaws.dynamodb.v20120810#TransactionCanceledException","CancellationReasons":[{"Code":"ResourceNotFound","Message":"Requested resource not found"}],"Message":"Transaction cancelled, please refer cancellation reasons for specific reasons [ResourceNotFound]"}"#,
    ),
    (
        20,
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "SELECT * FROM \"_capture_20261111_317_none\" WHERE pk = 'a'"}], "ClientRequestToken": []}"#,
        r#"{"__type":"com.amazon.coral.service#SerializationException"}"#,
    ),
    (
        21,
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "SELECT * FROM \"_capture_20261111_317_none\" WHERE pk = 'a'"}], "ClientRequestToken": {}}"#,
        r#"{"__type":"com.amazon.coral.service#SerializationException","Message":"Start of structure or map found where not expected"}"#,
    ),
    (
        22,
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "SELECT * FROM \"_capture_20261111_317_none\" WHERE pk = 'a'"}], "ClientRequestToken": true}"#,
        r#"{"__type":"com.amazon.coral.service#SerializationException","Message":"TRUE_VALUE cannot be converted to String"}"#,
    ),
    (
        23,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}}}], "ClientRequestToken": "", "ReturnItemCollectionMetrics": "BAD"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'ClientRequestToken' failed to satisfy constraint: Member must have length greater than or equal to 1"}"#,
    ),
    (
        24,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}}}], "ClientRequestToken": "", "ReturnConsumedCapacity": "BAD", "ReturnItemCollectionMetrics": "BAD"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'ClientRequestToken' failed to satisfy constraint: Member must have length greater than or equal to 1"}"#,
    ),
    (
        25,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}}}], "ClientRequestToken": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", "ReturnItemCollectionMetrics": "BAD"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'ClientRequestToken' failed to satisfy constraint: Member must have length less than or equal to 36"}"#,
    ),
    (
        26,
        "TransactWriteItems",
        r#"{"TransactItems": [], "ClientRequestToken": ""}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value at 'ClientRequestToken' failed to satisfy constraint: Member must have length greater than or equal to 1; Value at 'TransactItems' failed to satisfy constraint: Member must have length greater than or equal to 1"}"#,
    ),
    (
        27,
        "TransactWriteItems",
        r#"{"ClientRequestToken": ""}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value null at 'transactItems' failed to satisfy constraint: Member must not be null; Value '' at 'clientRequestToken' failed to satisfy constraint: Member must have length greater than or equal to 1"}"#,
    ),
    (
        28,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"N": "abc"}}}}], "ClientRequestToken": ""}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'ClientRequestToken' failed to satisfy constraint: Member must have length greater than or equal to 1"}"#,
    ),
    (
        29,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"N": 1}}}}], "ClientRequestToken": ""}"#,
        r#"{"__type":"com.amazon.coral.service#SerializationException","Message":"NUMBER_VALUE cannot be converted to String"}"#,
    ),
    (
        30,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"Item": {"pk": {"S": "a"}}}}], "ClientRequestToken": ""}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value null at 'transactItems.1.member.put.tableName' failed to satisfy constraint: Member must not be null; Value '' at 'clientRequestToken' failed to satisfy constraint: Member must have length greater than or equal to 1"}"#,
    ),
    (
        31,
        "TransactWriteItems",
        r#"{"TransactItems": PUTS_101, "ClientRequestToken": ""}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value at 'ClientRequestToken' failed to satisfy constraint: Member must have length greater than or equal to 1; Value at 'TransactItems' failed to satisfy constraint: Member must have length less than or equal to 100"}"#,
    ),
    (
        32,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}}}, {"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}}}], "ClientRequestToken": ""}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'ClientRequestToken' failed to satisfy constraint: Member must have length greater than or equal to 1"}"#,
    ),
    (
        33,
        "TransactWriteItems",
        r#"{"TransactItems": [{}], "ClientRequestToken": ""}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'ClientRequestToken' failed to satisfy constraint: Member must have length greater than or equal to 1"}"#,
    ),
    (
        34,
        "ExecuteTransaction",
        r#"{"TransactStatements": [], "ClientRequestToken": ""}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value '' at 'clientRequestToken' failed to satisfy constraint: Member must have length greater than or equal to 1; Value '[]' at 'transactStatements' failed to satisfy constraint: Member must have length greater than or equal to 1"}"#,
    ),
    (
        35,
        "ExecuteTransaction",
        r#"{"ClientRequestToken": ""}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value '' at 'clientRequestToken' failed to satisfy constraint: Member must have length greater than or equal to 1; Value null at 'transactStatements' failed to satisfy constraint: Member must not be null"}"#,
    ),
    (
        36,
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": ""}], "ClientRequestToken": ""}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value '' at 'clientRequestToken' failed to satisfy constraint: Member must have length greater than or equal to 1; Value '' at 'transactStatements.1.member.statement' failed to satisfy constraint: Member must have length greater than or equal to 1"}"#,
    ),
    (
        37,
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "SELECT * FROM \"_capture_20261111_317_none\" WHERE pk = ?", "Parameters": [{"N": "abc"}]}], "ClientRequestToken": ""}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value '' at 'clientRequestToken' failed to satisfy constraint: Member must have length greater than or equal to 1"}"#,
    ),
    (
        38,
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "SELECT * FROM \"_capture_20261111_317_none\" WHERE pk = ?", "Parameters": [{"N": 1}]}], "ClientRequestToken": ""}"#,
        r#"{"__type":"com.amazon.coral.service#SerializationException","Message":"NUMBER_VALUE cannot be converted to String"}"#,
    ),
    (
        39,
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "SELECT * FROM \"_capture_20261111_317_none\" WHERE pk = ?", "Parameters": []}], "ClientRequestToken": ""}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value '' at 'clientRequestToken' failed to satisfy constraint: Member must have length greater than or equal to 1; Value '[]' at 'transactStatements.1.member.parameters' failed to satisfy constraint: Member must have length greater than or equal to 1"}"#,
    ),
    (
        40,
        "ExecuteTransaction",
        r#"{"TransactStatements": [{"Statement": "garbage"}], "ClientRequestToken": ""}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value '' at 'clientRequestToken' failed to satisfy constraint: Member must have length greater than or equal to 1"}"#,
    ),
    (
        41,
        "TransactGetItems",
        r#"{"TransactItems": [{"Get": {"TableName": "_capture_20261111_317_none", "Key": {"pk": {"S": "a"}}}}], "ClientRequestToken": ""}"#,
        r#"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found"}"#,
    ),
];

/// What each stage reports when another stage also has something to report:
/// the `TransactItems` length on its own, a member left null, and an empty
/// action.
const SECOND_ROUND: &[Row] = &[
    (
        1,
        "TransactWriteItems",
        r#"{"TransactItems": []}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'TransactItems' failed to satisfy constraint: Member must have length greater than or equal to 1"}"#,
    ),
    (
        2,
        "TransactWriteItems",
        r#"{"TransactItems": PUTS_101}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'TransactItems' failed to satisfy constraint: Member must have length less than or equal to 100"}"#,
    ),
    (
        3,
        "TransactWriteItems",
        r#"{"TransactItems": [], "ReturnConsumedCapacity": "BAD"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'TransactItems' failed to satisfy constraint: Member must have length greater than or equal to 1"}"#,
    ),
    (
        4,
        "TransactWriteItems",
        r#"{"TransactItems": PUTS_101, "ReturnConsumedCapacity": "BAD"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'TransactItems' failed to satisfy constraint: Member must have length less than or equal to 100"}"#,
    ),
    (
        5,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}, "ReturnValuesOnConditionCheckFailure": "BAD"}}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value 'BAD' at 'transactItems.1.member.put.returnValuesOnConditionCheckFailure' failed to satisfy constraint: Member must satisfy enum value set: [ALL_OLD, NONE]"}"#,
    ),
    (
        6,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}, "ReturnValuesOnConditionCheckFailure": "BAD"}}], "ClientRequestToken": ""}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'ClientRequestToken' failed to satisfy constraint: Member must have length greater than or equal to 1"}"#,
    ),
    (
        7,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}, "ReturnValuesOnConditionCheckFailure": "BAD"}}], "ClientRequestToken": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'ClientRequestToken' failed to satisfy constraint: Member must have length less than or equal to 36"}"#,
    ),
    (
        8,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}, "ReturnValuesOnConditionCheckFailure": "BAD"}}], "ClientRequestToken": "", "ReturnConsumedCapacity": "BAD", "ReturnItemCollectionMetrics": "BAD"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'ClientRequestToken' failed to satisfy constraint: Member must have length greater than or equal to 1"}"#,
    ),
    (
        9,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}, "ReturnValuesOnConditionCheckFailure": "BAD"}}], "ClientRequestToken": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", "ReturnConsumedCapacity": "BAD", "ReturnItemCollectionMetrics": "BAD"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'ClientRequestToken' failed to satisfy constraint: Member must have length less than or equal to 36"}"#,
    ),
    (
        10,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}, "ReturnValuesOnConditionCheckFailure": "BAD"}}], "ReturnConsumedCapacity": "BAD", "ReturnItemCollectionMetrics": "BAD"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value 'BAD' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]; Value 'BAD' at 'returnItemCollectionMetrics' failed to satisfy constraint: Member must satisfy enum value set: [SIZE, NONE]"}"#,
    ),
    (
        11,
        "TransactWriteItems",
        r#"{"ClientRequestToken": "", "ReturnConsumedCapacity": "BAD"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"3 validation errors detected: Value null at 'transactItems' failed to satisfy constraint: Member must not be null; Value '' at 'clientRequestToken' failed to satisfy constraint: Member must have length greater than or equal to 1; Value 'BAD' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"}"#,
    ),
    (
        12,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"Item": {"pk": {"S": "a"}}}}], "ClientRequestToken": "", "ReturnConsumedCapacity": "BAD"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"3 validation errors detected: Value null at 'transactItems.1.member.put.tableName' failed to satisfy constraint: Member must not be null; Value '' at 'clientRequestToken' failed to satisfy constraint: Member must have length greater than or equal to 1; Value 'BAD' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"}"#,
    ),
    (
        13,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "_capture_20261111_317_none"}}], "ClientRequestToken": ""}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value null at 'transactItems.1.member.put.item' failed to satisfy constraint: Member must not be null; Value '' at 'clientRequestToken' failed to satisfy constraint: Member must have length greater than or equal to 1"}"#,
    ),
    (
        14,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Update": {"TableName": "_capture_20261111_317_none", "UpdateExpression": "SET a = :v"}}], "ClientRequestToken": ""}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value null at 'transactItems.1.member.update.key' failed to satisfy constraint: Member must not be null; Value '' at 'clientRequestToken' failed to satisfy constraint: Member must have length greater than or equal to 1"}"#,
    ),
    (
        15,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Delete": {"TableName": "_capture_20261111_317_none"}}], "ClientRequestToken": ""}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value null at 'transactItems.1.member.delete.key' failed to satisfy constraint: Member must not be null; Value '' at 'clientRequestToken' failed to satisfy constraint: Member must have length greater than or equal to 1"}"#,
    ),
    (
        16,
        "TransactWriteItems",
        r#"{"TransactItems": [{"ConditionCheck": {"TableName": "_capture_20261111_317_none", "Key": {"pk": {"S": "a"}}}}], "ClientRequestToken": ""}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value null at 'transactItems.1.member.conditionCheck.conditionExpression' failed to satisfy constraint: Member must not be null; Value '' at 'clientRequestToken' failed to satisfy constraint: Member must have length greater than or equal to 1"}"#,
    ),
    (
        17,
        "TransactWriteItems",
        r#"{"TransactItems": [], "ClientRequestToken": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value at 'ClientRequestToken' failed to satisfy constraint: Member must have length less than or equal to 36; Value at 'TransactItems' failed to satisfy constraint: Member must have length greater than or equal to 1"}"#,
    ),
    (
        18,
        "TransactWriteItems",
        r#"{"ClientRequestToken": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value null at 'transactItems' failed to satisfy constraint: Member must not be null; Value 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx' at 'clientRequestToken' failed to satisfy constraint: Member must have length less than or equal to 36"}"#,
    ),
    (
        19,
        "TransactWriteItems",
        r#"{"TransactItems": [{}], "ClientRequestToken": "", "ReturnConsumedCapacity": "BAD"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'ClientRequestToken' failed to satisfy constraint: Member must have length greater than or equal to 1"}"#,
    ),
    (
        20,
        "TransactWriteItems",
        r#"{"TransactItems": [{}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value '' at 'transactItems' failed to satisfy constraint: TransactWriteRequest should contain Delete or Put or Update or ConditionCheck"}"#,
    ),
    (
        21,
        "TransactWriteItems",
        r#"{"TransactItems": [{}], "ReturnConsumedCapacity": "BAD"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value '' at 'transactItems' failed to satisfy constraint: TransactWriteRequest should contain Delete or Put or Update or ConditionCheck; Value 'BAD' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"}"#,
    ),
    (
        22,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"Item": {"pk": {"S": "a"}}}}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value null at 'transactItems.1.member.put.tableName' failed to satisfy constraint: Member must not be null"}"#,
    ),
    (
        23,
        "TransactWriteItems",
        r#"{}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value null at 'transactItems' failed to satisfy constraint: Member must not be null"}"#,
    ),
    (
        24,
        "ExecuteTransaction",
        r#"{"ClientRequestToken": "", "ReturnConsumedCapacity": "BAD"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"3 validation errors detected: Value '' at 'clientRequestToken' failed to satisfy constraint: Member must have length greater than or equal to 1; Value null at 'transactStatements' failed to satisfy constraint: Member must not be null; Value 'BAD' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"}"#,
    ),
    (
        25,
        "ExecuteTransaction",
        r#"{"TransactStatements": [], "ClientRequestToken": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx' at 'clientRequestToken' failed to satisfy constraint: Member must have length less than or equal to 36; Value '[]' at 'transactStatements' failed to satisfy constraint: Member must have length greater than or equal to 1"}"#,
    ),
    (
        26,
        "ExecuteTransaction",
        r#"{"TransactStatements": []}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value '[]' at 'transactStatements' failed to satisfy constraint: Member must have length greater than or equal to 1"}"#,
    ),
    (
        27,
        "ExecuteTransaction",
        r#"{}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value null at 'transactStatements' failed to satisfy constraint: Member must not be null"}"#,
    ),
];

/// Where the stages meet: an empty action against a bad
/// `ReturnValuesOnConditionCheckFailure` and against a value refused in an
/// item, the order of several null members in one action, a missing
/// `UpdateExpression`, and a table name refused beside a bad
/// `ReturnConsumedCapacity`.
const THIRD_ROUND: &[Row] = &[
    (
        1,
        "TransactWriteItems",
        r#"{"TransactItems": [{}, {"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}, "ReturnValuesOnConditionCheckFailure": "BAD"}}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value '' at 'transactItems' failed to satisfy constraint: TransactWriteRequest should contain Delete or Put or Update or ConditionCheck"}"#,
    ),
    (
        2,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}, "ReturnValuesOnConditionCheckFailure": "BAD"}}, {}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value '' at 'transactItems' failed to satisfy constraint: TransactWriteRequest should contain Delete or Put or Update or ConditionCheck"}"#,
    ),
    (
        3,
        "TransactWriteItems",
        r#"{"TransactItems": [{}], "ReturnItemCollectionMetrics": "BAD"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value '' at 'transactItems' failed to satisfy constraint: TransactWriteRequest should contain Delete or Put or Update or ConditionCheck; Value 'BAD' at 'returnItemCollectionMetrics' failed to satisfy constraint: Member must satisfy enum value set: [SIZE, NONE]"}"#,
    ),
    (
        4,
        "TransactWriteItems",
        r#"{"TransactItems": [{}], "ReturnConsumedCapacity": "BAD", "ReturnItemCollectionMetrics": "BAD"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"3 validation errors detected: Value '' at 'transactItems' failed to satisfy constraint: TransactWriteRequest should contain Delete or Put or Update or ConditionCheck; Value 'BAD' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]; Value 'BAD' at 'returnItemCollectionMetrics' failed to satisfy constraint: Member must satisfy enum value set: [SIZE, NONE]"}"#,
    ),
    (
        5,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}, "ReturnValuesOnConditionCheckFailure": "BAD"}}], "ReturnConsumedCapacity": "BAD"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value 'BAD' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"}"#,
    ),
    (
        6,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}, "ReturnValuesOnConditionCheckFailure": "BAD"}}], "ReturnItemCollectionMetrics": "BAD"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value 'BAD' at 'returnItemCollectionMetrics' failed to satisfy constraint: Member must satisfy enum value set: [SIZE, NONE]"}"#,
    ),
    (
        7,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}, "ReturnValuesOnConditionCheckFailure": "BAD"}}, {"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}, "ReturnValuesOnConditionCheckFailure": "BAD"}}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value 'BAD' at 'transactItems.1.member.put.returnValuesOnConditionCheckFailure' failed to satisfy constraint: Member must satisfy enum value set: [ALL_OLD, NONE]"}"#,
    ),
    (
        8,
        "TransactWriteItems",
        r#"{"ReturnItemCollectionMetrics": "BAD"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value null at 'transactItems' failed to satisfy constraint: Member must not be null; Value 'BAD' at 'returnItemCollectionMetrics' failed to satisfy constraint: Member must satisfy enum value set: [SIZE, NONE]"}"#,
    ),
    (
        9,
        "TransactWriteItems",
        r#"{"ReturnConsumedCapacity": "BAD", "ReturnItemCollectionMetrics": "BAD"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"3 validation errors detected: Value null at 'transactItems' failed to satisfy constraint: Member must not be null; Value 'BAD' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]; Value 'BAD' at 'returnItemCollectionMetrics' failed to satisfy constraint: Member must satisfy enum value set: [SIZE, NONE]"}"#,
    ),
    (
        10,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"Item": {"pk": {"S": "a"}}}}], "ReturnItemCollectionMetrics": "BAD"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value null at 'transactItems.1.member.put.tableName' failed to satisfy constraint: Member must not be null; Value 'BAD' at 'returnItemCollectionMetrics' failed to satisfy constraint: Member must satisfy enum value set: [SIZE, NONE]"}"#,
    ),
    (
        11,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"Item": {"pk": {"S": "a"}}, "ReturnValuesOnConditionCheckFailure": "BAD"}}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value 'BAD' at 'transactItems.1.member.put.returnValuesOnConditionCheckFailure' failed to satisfy constraint: Member must satisfy enum value set: [ALL_OLD, NONE]; Value null at 'transactItems.1.member.put.tableName' failed to satisfy constraint: Member must not be null"}"#,
    ),
    (
        12,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {}}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value null at 'transactItems.1.member.put.item' failed to satisfy constraint: Member must not be null; Value null at 'transactItems.1.member.put.tableName' failed to satisfy constraint: Member must not be null"}"#,
    ),
    (
        13,
        "TransactWriteItems",
        r#"{"TransactItems": [{"ConditionCheck": {}}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"3 validation errors detected: Value null at 'transactItems.1.member.conditionCheck.conditionExpression' failed to satisfy constraint: Member must not be null; Value null at 'transactItems.1.member.conditionCheck.key' failed to satisfy constraint: Member must not be null; Value null at 'transactItems.1.member.conditionCheck.tableName' failed to satisfy constraint: Member must not be null"}"#,
    ),
    (
        14,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Update": {}}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"3 validation errors detected: Value null at 'transactItems.1.member.update.updateExpression' failed to satisfy constraint: Member must not be null; Value null at 'transactItems.1.member.update.key' failed to satisfy constraint: Member must not be null; Value null at 'transactItems.1.member.update.tableName' failed to satisfy constraint: Member must not be null"}"#,
    ),
    (
        15,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Delete": {}}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value null at 'transactItems.1.member.delete.key' failed to satisfy constraint: Member must not be null; Value null at 'transactItems.1.member.delete.tableName' failed to satisfy constraint: Member must not be null"}"#,
    ),
    (
        16,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Update": {"TableName": "_capture_20261111_317_none", "Key": {"pk": {"S": "a"}}}}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value null at 'transactItems.1.member.update.updateExpression' failed to satisfy constraint: Member must not be null"}"#,
    ),
    (
        17,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Update": {"TableName": "_capture_20261111_317_none", "Key": {"pk": {"S": "a"}}}}], "ClientRequestToken": ""}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value null at 'transactItems.1.member.update.updateExpression' failed to satisfy constraint: Member must not be null; Value '' at 'clientRequestToken' failed to satisfy constraint: Member must have length greater than or equal to 1"}"#,
    ),
    (
        18,
        "TransactWriteItems",
        r#"{"TransactItems": [{}, {}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value '' at 'transactItems' failed to satisfy constraint: TransactWriteRequest should contain Delete or Put or Update or ConditionCheck"}"#,
    ),
    (
        19,
        "TransactWriteItems",
        r#"{"TransactItems": [{}, {"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}}}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value '' at 'transactItems' failed to satisfy constraint: TransactWriteRequest should contain Delete or Put or Update or ConditionCheck"}"#,
    ),
    (
        20,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {}}, {"Delete": {}}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"4 validation errors detected: Value null at 'transactItems.1.member.put.item' failed to satisfy constraint: Member must not be null; Value null at 'transactItems.1.member.put.tableName' failed to satisfy constraint: Member must not be null; Value null at 'transactItems.2.member.delete.key' failed to satisfy constraint: Member must not be null; Value null at 'transactItems.2.member.delete.tableName' failed to satisfy constraint: Member must not be null"}"#,
    ),
    (
        21,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}}, "Delete": {"TableName": "_capture_20261111_317_none", "Key": {"pk": {"S": "a"}}}}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"TransactItems can only contain one of Check, Put, Update or Delete"}"#,
    ),
    (
        22,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}}, "Delete": {"TableName": "_capture_20261111_317_none", "Key": {"pk": {"S": "a"}}}}], "ClientRequestToken": ""}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'ClientRequestToken' failed to satisfy constraint: Member must have length greater than or equal to 1"}"#,
    ),
    (
        23,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}, "ConditionExpression": "a = :v", "ExpressionAttributeValues": {":v": {"N": "abc"}}}}], "ClientRequestToken": ""}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'ClientRequestToken' failed to satisfy constraint: Member must have length greater than or equal to 1"}"#,
    ),
    (
        24,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}, "ConditionExpression": "a = :v", "ExpressionAttributeValues": {":v": {"N": "abc"}}}}], "ReturnConsumedCapacity": "BAD"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value 'BAD' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"}"#,
    ),
    (
        25,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"N": "abc"}}}}], "ReturnConsumedCapacity": "BAD"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value 'BAD' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"}"#,
    ),
    (
        26,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"N": "abc"}}, "ReturnValuesOnConditionCheckFailure": "BAD"}}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"The parameter cannot be converted to a numeric value: abc"}"#,
    ),
    (
        27,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"N": "abc"}}}}, {}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value '' at 'transactItems' failed to satisfy constraint: TransactWriteRequest should contain Delete or Put or Update or ConditionCheck"}"#,
    ),
    (
        28,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"Item": {"pk": {"N": "abc"}}}}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value null at 'transactItems.1.member.put.tableName' failed to satisfy constraint: Member must not be null"}"#,
    ),
    (
        29,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "x", "Item": {"pk": {"S": "a"}}}}], "ClientRequestToken": ""}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'ClientRequestToken' failed to satisfy constraint: Member must have length greater than or equal to 1"}"#,
    ),
    (
        30,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "x", "Item": {"pk": {"S": "a"}}}}], "ReturnConsumedCapacity": "BAD"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value 'x' at 'transactItems.1.member.put.tableName' failed to satisfy constraint: Member must have length greater than or equal to 3; Value 'BAD' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"}"#,
    ),
];

/// Where a refused `ReturnValuesOnConditionCheckFailure` stands among an
/// action's null members, and what an item naming no action leaves out.
const FOURTH_ROUND: &[Row] = &[
    (
        1,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"Item": {"pk": {"S": "a"}}, "ReturnValuesOnConditionCheckFailure": "BAD"}}, {"Put": {"Item": {"pk": {"S": "a"}}, "ReturnValuesOnConditionCheckFailure": "BAD"}}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"4 validation errors detected: Value 'BAD' at 'transactItems.1.member.put.returnValuesOnConditionCheckFailure' failed to satisfy constraint: Member must satisfy enum value set: [ALL_OLD, NONE]; Value null at 'transactItems.1.member.put.tableName' failed to satisfy constraint: Member must not be null; Value 'BAD' at 'transactItems.2.member.put.returnValuesOnConditionCheckFailure' failed to satisfy constraint: Member must satisfy enum value set: [ALL_OLD, NONE]; Value null at 'transactItems.2.member.put.tableName' failed to satisfy constraint: Member must not be null"}"#,
    ),
    (
        2,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}, "ReturnValuesOnConditionCheckFailure": "BAD"}}, {"Put": {"Item": {"pk": {"S": "a"}}}}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value 'BAD' at 'transactItems.1.member.put.returnValuesOnConditionCheckFailure' failed to satisfy constraint: Member must satisfy enum value set: [ALL_OLD, NONE]; Value null at 'transactItems.2.member.put.tableName' failed to satisfy constraint: Member must not be null"}"#,
    ),
    (
        3,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"ReturnValuesOnConditionCheckFailure": "BAD"}}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"3 validation errors detected: Value null at 'transactItems.1.member.put.item' failed to satisfy constraint: Member must not be null; Value 'BAD' at 'transactItems.1.member.put.returnValuesOnConditionCheckFailure' failed to satisfy constraint: Member must satisfy enum value set: [ALL_OLD, NONE]; Value null at 'transactItems.1.member.put.tableName' failed to satisfy constraint: Member must not be null"}"#,
    ),
    (
        4,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Update": {"ReturnValuesOnConditionCheckFailure": "BAD"}}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"4 validation errors detected: Value 'BAD' at 'transactItems.1.member.update.returnValuesOnConditionCheckFailure' failed to satisfy constraint: Member must satisfy enum value set: [ALL_OLD, NONE]; Value null at 'transactItems.1.member.update.updateExpression' failed to satisfy constraint: Member must not be null; Value null at 'transactItems.1.member.update.key' failed to satisfy constraint: Member must not be null; Value null at 'transactItems.1.member.update.tableName' failed to satisfy constraint: Member must not be null"}"#,
    ),
    (
        5,
        "TransactWriteItems",
        r#"{"TransactItems": [{"ConditionCheck": {"ReturnValuesOnConditionCheckFailure": "BAD"}}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"4 validation errors detected: Value null at 'transactItems.1.member.conditionCheck.conditionExpression' failed to satisfy constraint: Member must not be null; Value 'BAD' at 'transactItems.1.member.conditionCheck.returnValuesOnConditionCheckFailure' failed to satisfy constraint: Member must satisfy enum value set: [ALL_OLD, NONE]; Value null at 'transactItems.1.member.conditionCheck.key' failed to satisfy constraint: Member must not be null; Value null at 'transactItems.1.member.conditionCheck.tableName' failed to satisfy constraint: Member must not be null"}"#,
    ),
    (
        6,
        "TransactWriteItems",
        r#"{"TransactItems": [{}, {"Put": {"TableName": "x", "Item": {"pk": {"S": "a"}}}}], "ReturnConsumedCapacity": "BAD"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value '' at 'transactItems' failed to satisfy constraint: TransactWriteRequest should contain Delete or Put or Update or ConditionCheck; Value 'BAD' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"}"#,
    ),
    (
        7,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "x", "Item": {"pk": {"S": "a"}}}}, {}], "ReturnConsumedCapacity": "BAD"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value '' at 'transactItems' failed to satisfy constraint: TransactWriteRequest should contain Delete or Put or Update or ConditionCheck; Value 'BAD' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"}"#,
    ),
    (
        8,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "bad!name", "Item": {"pk": {"S": "a"}}}}], "ReturnConsumedCapacity": "BAD"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value 'bad!name' at 'transactItems.1.member.put.tableName' failed to satisfy constraint: Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+; Value 'BAD' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"}"#,
    ),
    (
        9,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "bad!name", "Item": {"pk": {"S": "a"}}}}], "ReturnItemCollectionMetrics": "BAD"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value 'bad!name' at 'transactItems.1.member.put.tableName' failed to satisfy constraint: Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+; Value 'BAD' at 'returnItemCollectionMetrics' failed to satisfy constraint: Member must satisfy enum value set: [SIZE, NONE]"}"#,
    ),
    (
        10,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "x", "Item": {"pk": {"S": "a"}}}}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value 'x' at 'transactItems.1.member.put.tableName' failed to satisfy constraint: Member must have length greater than or equal to 3"}"#,
    ),
];

/// A value refused in an item or key beside one refused in the expression
/// values, and an action or item given as a list or null.
const FIFTH_ROUND: &[Row] = &[
    (
        1,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"N": "abc"}}, "ConditionExpression": "a = :v", "ExpressionAttributeValues": {":v": {"N": "xyz"}}}}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"The parameter cannot be converted to a numeric value: abc"}"#,
    ),
    (
        2,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "_capture_20261111_317_none", "ConditionExpression": "a = :v", "ExpressionAttributeValues": {":v": {"N": "xyz"}}, "Item": {"pk": {"N": "abc"}}}}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"The parameter cannot be converted to a numeric value: abc"}"#,
    ),
    (
        3,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Update": {"TableName": "_capture_20261111_317_none", "Key": {"pk": {"N": "abc"}}, "UpdateExpression": "SET a = :v", "ExpressionAttributeValues": {":v": {"N": "xyz"}}}}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"The parameter cannot be converted to a numeric value: abc"}"#,
    ),
    (
        4,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Delete": {"TableName": "_capture_20261111_317_none", "Key": {"pk": {"N": "abc"}}, "ConditionExpression": "a = :v", "ExpressionAttributeValues": {":v": {"N": "xyz"}}}}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"The parameter cannot be converted to a numeric value: abc"}"#,
    ),
    (
        5,
        "TransactWriteItems",
        r#"{"TransactItems": [{"ConditionCheck": {"TableName": "_capture_20261111_317_none", "Key": {"pk": {"N": "abc"}}, "ConditionExpression": "a = :v", "ExpressionAttributeValues": {":v": {"N": "xyz"}}}}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"The parameter cannot be converted to a numeric value: abc"}"#,
    ),
    (
        6,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}}}, {"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"N": "abc"}}}}, {"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"N": "xyz"}}}}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"The parameter cannot be converted to a numeric value: abc"}"#,
    ),
    (
        7,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": []}]}"#,
        r#"{"__type":"com.amazon.coral.service#SerializationException"}"#,
    ),
    (
        8,
        "TransactWriteItems",
        r#"{"TransactItems": [[]]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value '' at 'transactItems' failed to satisfy constraint: TransactWriteRequest should contain Delete or Put or Update or ConditionCheck"}"#,
    ),
    (
        9,
        "TransactWriteItems",
        r#"{"TransactItems": [null]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"TransactItems can't be null"}"#,
    ),
    (
        10,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": null}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value '' at 'transactItems' failed to satisfy constraint: TransactWriteRequest should contain Delete or Put or Update or ConditionCheck"}"#,
    ),
    (
        11,
        "TransactWriteItems",
        r#"{"TransactItems": [{}, {"Put": {"Item": {"pk": {"S": "a"}}}}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value null at 'transactItems.2.member.put.tableName' failed to satisfy constraint: Member must not be null"}"#,
    ),
    (
        12,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"Item": {"pk": {"S": "a"}}}}, {}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value null at 'transactItems.1.member.put.tableName' failed to satisfy constraint: Member must not be null"}"#,
    ),
    (
        13,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"N": "abc"}}}}], "ClientRequestToken": ""}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'ClientRequestToken' failed to satisfy constraint: Member must have length greater than or equal to 1"}"#,
    ),
    (
        14,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"N": "abc"}}}}], "ClientRequestToken": "", "ReturnConsumedCapacity": "BAD"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'ClientRequestToken' failed to satisfy constraint: Member must have length greater than or equal to 1"}"#,
    ),
];

/// A Delete's mode among its null members, values refused in two actions, and
/// an item or action given as null, a list or a scalar.
const SIXTH_ROUND: &[Row] = &[
    (
        1,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Delete": {"ReturnValuesOnConditionCheckFailure": "BAD"}}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"3 validation errors detected: Value 'BAD' at 'transactItems.1.member.delete.returnValuesOnConditionCheckFailure' failed to satisfy constraint: Member must satisfy enum value set: [ALL_OLD, NONE]; Value null at 'transactItems.1.member.delete.key' failed to satisfy constraint: Member must not be null; Value null at 'transactItems.1.member.delete.tableName' failed to satisfy constraint: Member must not be null"}"#,
    ),
    (
        2,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}, "ConditionExpression": "a = :v", "ExpressionAttributeValues": {":v": {"N": "xyz"}}}}, {"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"N": "abc"}}}}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"The parameter cannot be converted to a numeric value: xyz"}"#,
    ),
    (
        3,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"N": "abc"}}}}, {"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}, "ConditionExpression": "a = :v", "ExpressionAttributeValues": {":v": {"N": "xyz"}}}}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"The parameter cannot be converted to a numeric value: abc"}"#,
    ),
    (
        4,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": ["x"]}]}"#,
        r#"{"__type":"com.amazon.coral.service#SerializationException"}"#,
    ),
    (
        5,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": "x"}]}"#,
        r#"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected field type"}"#,
    ),
    (
        6,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": 5}]}"#,
        r#"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected field type"}"#,
    ),
    (
        7,
        "TransactWriteItems",
        r#"{"TransactItems": [null, {"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": 5}}}}]}"#,
        r#"{"__type":"com.amazon.coral.service#SerializationException","Message":"NUMBER_VALUE cannot be converted to String"}"#,
    ),
    (
        8,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": 5}}}}, null]}"#,
        r#"{"__type":"com.amazon.coral.service#SerializationException","Message":"NUMBER_VALUE cannot be converted to String"}"#,
    ),
    (
        9,
        "TransactWriteItems",
        r#"{"TransactItems": [null], "ClientRequestToken": ""}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value '' at 'clientRequestToken' failed to satisfy constraint: Member must have length greater than or equal to 1"}"#,
    ),
    (
        10,
        "TransactWriteItems",
        r#"{"TransactItems": [null, {}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"TransactItems can't be null"}"#,
    ),
    (
        11,
        "TransactWriteItems",
        r#"{"TransactItems": ["x"]}"#,
        r#"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected value type in payload"}"#,
    ),
    (
        12,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}, "ConditionExpression": "a = :a AND b = :b", "ExpressionAttributeValues": {":a": {"N": "abc"}, ":b": {"S": 5}}}}]}"#,
        r#"{"__type":"com.amazon.coral.service#SerializationException","Message":"NUMBER_VALUE cannot be converted to String"}"#,
    ),
];

/// Values read on the current layer, a null or scalar item beside the enums,
/// and where an item naming two actions, a table name and a refused value meet.
/// Captured in eu-west-2, us-east-1, eu-west-1 and us-west-2, which agreed on
/// every row.
const SEVENTH_ROUND: &[Row] = &[
    (
        1,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}, "ConditionExpression": "a = :v", "ExpressionAttributeValues": {":v": {"N": "xyz"}}}}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"The parameter cannot be converted to a numeric value: xyz"}"#,
    ),
    (
        2,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Update": {"TableName": "_capture_20261111_317_none", "Key": {"pk": {"S": "a"}}, "UpdateExpression": "SET a = :v", "ExpressionAttributeValues": {":v": {"N": "1e2147483647"}}}}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Number overflow. Attempting to store a number with magnitude larger than supported range"}"#,
    ),
    (
        3,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Update": {"TableName": "_capture_20261111_317_none", "Key": {"pk": {"S": "a"}}, "UpdateExpression": "SET a = :v", "ExpressionAttributeValues": {":v": {"N": "xyz"}}}}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"The parameter cannot be converted to a numeric value: xyz"}"#,
    ),
    (
        4,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Delete": {"TableName": "_capture_20261111_317_none", "Key": {"pk": {"S": "a"}}, "ConditionExpression": "a = :v", "ExpressionAttributeValues": {":v": {"N": "xyz"}}}}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"The parameter cannot be converted to a numeric value: xyz"}"#,
    ),
    (
        5,
        "TransactWriteItems",
        r#"{"TransactItems": [{"ConditionCheck": {"TableName": "_capture_20261111_317_none", "Key": {"pk": {"S": "a"}}, "ConditionExpression": "a = :v", "ExpressionAttributeValues": {":v": {"N": "xyz"}}}}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"The parameter cannot be converted to a numeric value: xyz"}"#,
    ),
    (
        6,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"N": "1e2147483647"}}}}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Number overflow. Attempting to store a number with magnitude larger than supported range"}"#,
    ),
    (
        7,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}, "ConditionExpression": "a = :v", "ExpressionAttributeValues": {":v": {"N": ""}}}}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"The parameter cannot be converted to a numeric value: "}"#,
    ),
    (
        8,
        "TransactWriteItems",
        r#"{"TransactItems": [null], "ReturnConsumedCapacity": "BAD"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value 'BAD' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"}"#,
    ),
    (
        9,
        "TransactWriteItems",
        r#"{"TransactItems": [5]}"#,
        r#"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected value type in payload"}"#,
    ),
    (
        10,
        "TransactWriteItems",
        r#"{"TransactItems": [true]}"#,
        r#"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected value type in payload"}"#,
    ),
    (
        11,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}, "ReturnValuesOnConditionCheckFailure": "BAD"}, "Delete": {"TableName": "_capture_20261111_317_none", "Key": {"pk": {"S": "a"}}}}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"TransactItems can only contain one of Check, Put, Update or Delete"}"#,
    ),
    (
        12,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}}, "Delete": {"TableName": "_capture_20261111_317_none", "Key": {"pk": {"S": "a"}}}}, {"Put": {"TableName": "x", "Item": {"pk": {"S": "a"}}}}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value 'x' at 'transactItems.2.member.put.tableName' failed to satisfy constraint: Member must have length greater than or equal to 3"}"#,
    ),
    (
        13,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"N": "abc"}}}}, {"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"N": "xyz"}}}}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"The parameter cannot be converted to a numeric value: abc"}"#,
    ),
    (
        14,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "x", "Item": {"pk": {"N": "abc"}}}}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value 'x' at 'transactItems.1.member.put.tableName' failed to satisfy constraint: Member must have length greater than or equal to 3"}"#,
    ),
    (
        15,
        "TransactWriteItems",
        r#"{"TransactItems": [{"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"N": "abc"}}}, "Delete": {"TableName": "_capture_20261111_317_none", "Key": {"pk": {"S": "a"}}}}]}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"TransactItems can only contain one of Check, Put, Update or Delete"}"#,
    ),
];
