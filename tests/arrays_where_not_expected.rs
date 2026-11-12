//! A JSON array given where DynamoDB doesn't expect one.
//!
//! DynamoDB refuses a request body that is a JSON array, and an array given
//! for a scalar, a map, a structure or an attribute value inside a request,
//! with a `SerializationException` that has no message. A body that is a
//! string, a number or a boolean is refused as `Unexpected value type in
//! payload`, and `null` as `Request payload does not contain the input model`.
//! A few forms are read by position: a BatchGetItem or BatchWriteItem body,
//! a BatchWriteItem `PutRequest` or `DeleteRequest`, and on Query and Scan a
//! `Condition`. A positional form too deep for DynamoDB's parser is refused
//! with no message, where the object form gets the nesting error.
//!
//! The rows are the requests sent to eu-west-2 and us-east-1 on 11 and 12
//! November 2026 and the answers DynamoDB gave, kept in
//! `fixtures/arrays_where_not_expected.json`, which the wasm engine's tests
//! read too. Where the regions differ both answers are kept and the row names
//! the one Dynoxide gives: the regions differed on BatchWriteItem's positional
//! forms, and Dynoxide follows eu-west-2. The one other row where they differ
//! is an operator outside the enum, which the regions word differently
//! whatever form the condition takes, and Dynoxide gives it in us-east-1's
//! words. Every request in the third round runs, in order, against a table
//! holding one item; every other request names a table that doesn't exist.
//! Answers are compared as parsed JSON, since DynamoDB and Dynoxide order
//! members differently.

#![cfg(feature = "http-server")]

use dynoxide::Database;
use serde_json::Value;

const FIXTURE: &str = include_str!("fixtures/arrays_where_not_expected.json");

/// A captured row: its number in the round, the operation, the request body,
/// the answer Dynoxide must give, and where the regions differ the other
/// region's answer, which it must not.
struct Row {
    number: usize,
    operation: String,
    body: String,
    expected: Value,
    other_region: Option<Value>,
}

/// A row left out by name: Dynoxide's present answer and the reason. Each is
/// still sent, must still differ from DynamoDB's answer, and must give the
/// answer recorded here.
struct LeftOut {
    round: String,
    row: usize,
    answer: Value,
    reason: String,
}

struct Fixture {
    rounds: Vec<(String, Option<String>, Vec<Row>)>,
    left_out: Vec<LeftOut>,
}

fn fixture() -> Fixture {
    let doc: Value = serde_json::from_str(FIXTURE).unwrap();
    let rounds = doc["rounds"]
        .as_array()
        .unwrap()
        .iter()
        .map(|round| {
            let rows = round["rows"]
                .as_array()
                .unwrap()
                .iter()
                .map(|row| {
                    let (expected, other_region) = match row.get("answer") {
                        Some(answer) => (answer.clone(), None),
                        None => {
                            let (kept, other) = match row["follows"].as_str().unwrap() {
                                "eu-west-2" => ("eu_west_2", "us_east_1"),
                                "us-east-1" => ("us_east_1", "eu_west_2"),
                                follows => panic!("no region {follows}"),
                            };
                            assert_ne!(row[kept], row[other], "the regions' answers must differ");
                            (row[kept].clone(), Some(row[other].clone()))
                        }
                    };
                    Row {
                        number: row["row"].as_u64().unwrap() as usize,
                        operation: row["operation"].as_str().unwrap().to_string(),
                        body: row["body"].as_str().unwrap().to_string(),
                        expected,
                        other_region,
                    }
                })
                .collect();
            (
                round["name"].as_str().unwrap().to_string(),
                round["table"].as_str().map(str::to_string),
                rows,
            )
        })
        .collect();
    let left_out = doc["left_out"]
        .as_array()
        .unwrap()
        .iter()
        .map(|l| LeftOut {
            round: l["round"].as_str().unwrap().to_string(),
            row: l["row"].as_u64().unwrap() as usize,
            answer: l["answer"].clone(),
            reason: l["reason"].as_str().unwrap().to_string(),
        })
        .collect();
    Fixture { rounds, left_out }
}

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
fn status_of(answer: &Value) -> u16 {
    if answer.get("__type").is_some() {
        400
    } else {
        200
    }
}

/// Send every row of a round in order, on a server of its own, and return how
/// many were compared. A round that names a table runs against it holding
/// `{pk: a, x: 1}`, because its BatchWriteItem rows would change it and its
/// last row reads it back.
async fn assert_round(round: &str) -> usize {
    let fixture = fixture();
    let (_, table, rows) = fixture
        .rounds
        .iter()
        .find(|(name, _, _)| name == round)
        .unwrap_or_else(|| panic!("no {round} in the fixture"));
    let url = start_server().await;
    if let Some(table) = table {
        let (status, got) = send(
            &url,
            "CreateTable",
            &format!(
                r#"{{"TableName":"{table}","AttributeDefinitions":[{{"AttributeName":"pk","AttributeType":"S"}}],"KeySchema":[{{"AttributeName":"pk","KeyType":"HASH"}}],"BillingMode":"PAY_PER_REQUEST"}}"#
            ),
        )
        .await;
        assert_eq!(status, 200, "{got}");
        let (status, got) = send(
            &url,
            "PutItem",
            &format!(r#"{{"TableName":"{table}","Item":{{"pk":{{"S":"a"}},"x":{{"N":"1"}}}}}}"#),
        )
        .await;
        assert_eq!(status, 200, "{got}");
    }
    let mut wrong = Vec::new();
    let mut compared = 0;
    let mut left_out = 0;
    for row in rows {
        let Row {
            number,
            operation,
            body,
            expected,
            other_region,
        } = row;
        let (status, got) = send(&url, operation, body).await;
        compared += 1;
        if let Some(l) = fixture
            .left_out
            .iter()
            .find(|l| l.round == round && l.row == *number)
        {
            left_out += 1;
            if got == *expected {
                wrong.push(format!(
                    "{round} row {number} now answers as DynamoDB did: drop it from left_out"
                ));
            } else if status != status_of(&l.answer) || got != l.answer {
                wrong.push(format!(
                    "{round} row {number} (left out: {}): {operation} {body}\n  \
                     expected {} {}\n  got      {status} {got}",
                    l.reason,
                    status_of(&l.answer),
                    l.answer
                ));
            }
            continue;
        }
        if status != status_of(expected) || got != *expected {
            let region = match other_region {
                Some(other) if *other == got => " (the other region's answer)",
                _ => "",
            };
            wrong.push(format!(
                "{round} row {number}: {operation} {body}\n  expected {} {expected}\n  \
                 got      {status} {got}{region}",
                status_of(expected)
            ));
        }
    }
    assert_eq!(
        compared,
        rows.len(),
        "every row of the {round} in the fixture must be sent"
    );
    assert_eq!(
        left_out,
        fixture.left_out.iter().filter(|l| l.round == round).count(),
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

/// Every round in the fixture has a test below, so a round added to it is not
/// silently skipped.
#[test]
fn every_round_in_the_fixture_is_tested() {
    let names: Vec<String> = fixture()
        .rounds
        .into_iter()
        .map(|(name, _, _)| name)
        .collect();
    assert_eq!(names, ROUNDS);
    let rows: usize = fixture().rounds.iter().map(|(_, _, rows)| rows.len()).sum();
    println!("{} rounds, {rows} rows", names.len());
    assert_eq!(rows, 288, "rows added to or removed from the fixture");
}

const ROUNDS: [&str; 10] = [
    "first round",
    "second round",
    "third round",
    "fourth round",
    "fifth round",
    "sixth round",
    "seventh round",
    "eighth round",
    "ninth round",
    "tenth round",
];

#[tokio::test]
async fn the_first_round_answers_as_dynamodb_did() {
    assert_round(ROUNDS[0]).await;
}

#[tokio::test]
async fn the_second_round_answers_as_dynamodb_did() {
    assert_round(ROUNDS[1]).await;
}

#[tokio::test]
async fn the_third_round_answers_as_dynamodb_did() {
    assert_round(ROUNDS[2]).await;
}

#[tokio::test]
async fn the_fourth_round_answers_as_dynamodb_did() {
    assert_round(ROUNDS[3]).await;
}

#[tokio::test]
async fn the_fifth_round_answers_as_dynamodb_did() {
    assert_round(ROUNDS[4]).await;
}

#[tokio::test]
async fn the_sixth_round_answers_as_dynamodb_did() {
    assert_round(ROUNDS[5]).await;
}

#[tokio::test]
async fn the_seventh_round_answers_as_dynamodb_did() {
    assert_round(ROUNDS[6]).await;
}

/// Values nested past DynamoDB's limit inside each form, positional ones
/// included, at 40 and 140 levels.
#[tokio::test]
async fn the_eighth_round_answers_as_dynamodb_did() {
    assert_round(ROUNDS[7]).await;
}

/// An attribute value given as an array, in every place one is read.
#[tokio::test]
async fn the_ninth_round_answers_as_dynamodb_did() {
    assert_round(ROUNDS[8]).await;
}

/// A list nested 45 to 135 deep in a BatchGetItem body given by position, a
/// `Condition` given by position and the BatchGetItem object form: the
/// positional forms are refused with no message from 61 and 62 levels, the
/// object form gets the nesting error at every depth.
#[tokio::test]
async fn the_tenth_round_answers_as_dynamodb_did() {
    assert_round(ROUNDS[9]).await;
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
