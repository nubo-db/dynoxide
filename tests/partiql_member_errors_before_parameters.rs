//! On ExecuteStatement and ExecuteTransaction, DynamoDB reads the whole
//! request, then checks its members, then walks the PartiQL parameters. So a
//! member error (`Limit`, a mode, the statement, the token) comes back ahead
//! of any parameter it would refuse, such as a number it can't read or a value
//! with no type, while a parameter of the wrong JSON type fails the request
//! ahead of both, even after a parameter refused for its value.
//!
//! Every expectation was captured against eu-west-2 on 10 November 2026. No
//! table is needed: each request is refused before its table is looked up.

#![cfg(feature = "http-server")]

use dynoxide::Database;
use serde_json::Value;

const VALIDATION: &str = "com.amazon.coral.validate#ValidationException";
const SERIALIZATION: &str = "com.amazon.coral.service#SerializationException";

const LIMIT_0: &str = "1 validation error detected: Value '0' at 'limit' failed to satisfy \
                       constraint: Member must have value greater than or equal to 1";
const ABC: &str = "The parameter cannot be converted to a numeric value: abc";
const EMPTY: &str =
    "Supplied AttributeValue is empty, must contain exactly one of the supported datatypes";
const NUMBER_TO_STRING: &str = "NUMBER_VALUE cannot be converted to String";
const NOT_AN_OBJECT: &str = "Unexpected value type in payload";
const BAD_CAPACITY: &str = "1 validation error detected: Value 'BAD' at 'returnConsumedCapacity' \
                            failed to satisfy constraint: Member must satisfy enum value set: \
                            [INDEXES, TOTAL, NONE]";

async fn start() -> String {
    let db = Database::memory().unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        dynoxide::server::serve_on(listener, db).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    format!("http://{addr}")
}

/// Send a body as written, since member order and malformed values matter.
async fn send(url: &str, operation: &str, body: &str) -> (u16, Value) {
    let resp = reqwest::Client::new()
        .post(url)
        .header("x-amz-target", format!("DynamoDB_20120810.{operation}"))
        .header("content-type", "application/x-amz-json-1.0")
        .header(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=fakekey/20260101/us-east-1/dynamodb/aws4_request, SignedHeaders=host;x-amz-date;x-amz-target, Signature=fakesig",
        )
        .header("x-amz-date", "20260101T000000Z")
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    let status = resp.status().as_u16();
    let text = resp.text().await.unwrap();
    (status, serde_json::from_str(&text).unwrap_or(Value::Null))
}

const SELECT: &str = r#""Statement":"SELECT * FROM \"Missing\" WHERE pk = ?""#;

/// An ExecuteStatement body with these parameters and other members.
fn statement(parameters: &str, members: &str) -> String {
    format!(r#"{{{SELECT},"Parameters":[{parameters}]{members}}}"#)
}

/// An ExecuteTransaction body with one statement per parameter list.
fn transaction(parameter_lists: &[&str], members: &str) -> String {
    let statements: Vec<String> = parameter_lists
        .iter()
        .map(|p| format!(r#"{{{SELECT},"Parameters":[{p}]}}"#))
        .collect();
    format!(
        r#"{{"TransactStatements":[{}]{members}}}"#,
        statements.join(",")
    )
}

/// Each case answers with the given error type and message.
async fn expect(url: &str, cases: &[(&str, String, &str, &str)]) {
    let mut wrong = Vec::new();
    for (operation, body, kind, message) in cases {
        let (status, v) = send(url, operation, body).await;
        let got_message = v["message"]
            .as_str()
            .or(v["Message"].as_str())
            .unwrap_or_default();
        if status != 400 || v["__type"] != *kind || got_message != *message {
            wrong.push(format!(
                "{operation} {body}\n  expected {kind}: {message}\n  got {status} {v}"
            ));
        }
    }
    assert!(
        wrong.is_empty(),
        "{} of {} cases wrong:\n{}",
        wrong.len(),
        cases.len(),
        wrong.join("\n")
    );
}

#[tokio::test]
async fn execute_statement_member_errors_beat_a_bad_number() {
    let url = start().await;
    let bad = r#"{"N":"abc"}"#;
    let cases = [
        (
            "ExecuteStatement",
            statement(bad, r#","Limit":0"#),
            VALIDATION,
            LIMIT_0,
        ),
        (
            "ExecuteStatement",
            statement(bad, r#","Limit":-5"#),
            VALIDATION,
            "1 validation error detected: Value '-5' at 'limit' failed to satisfy constraint: \
             Member must have value greater than or equal to 1",
        ),
        (
            "ExecuteStatement",
            statement(bad, r#","ReturnValuesOnConditionCheckFailure":"BAD""#),
            VALIDATION,
            "1 validation error detected: Value 'BAD' at 'returnValuesOnConditionCheckFailure' \
             failed to satisfy constraint: Member must satisfy enum value set: [ALL_OLD, NONE]",
        ),
        (
            "ExecuteStatement",
            statement(bad, r#","ReturnConsumedCapacity":"BAD""#),
            VALIDATION,
            BAD_CAPACITY,
        ),
        (
            "ExecuteStatement",
            format!(r#"{{"Statement":"","Parameters":[{bad}]}}"#),
            VALIDATION,
            "1 validation error detected: Value '' at 'statement' failed to satisfy constraint: \
             Member must have length greater than or equal to 1",
        ),
        (
            "ExecuteStatement",
            statement(bad, r#","Limit":0,"ReturnConsumedCapacity":"BAD""#),
            VALIDATION,
            "2 validation errors detected: Value '0' at 'limit' failed to satisfy constraint: \
             Member must have value greater than or equal to 1; Value 'BAD' at \
             'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum \
             value set: [INDEXES, TOTAL, NONE]",
        ),
    ];
    expect(&url, &cases).await;
}

#[tokio::test]
async fn limit_beats_every_kind_of_refused_parameter() {
    let url = start().await;
    let refused = [
        "{}",
        r#"{"S":"a","N":"1"}"#,
        r#"{"NULL":false}"#,
        r#"{"NULL":"false"}"#,
        r#"{"NULL":false,"S":"a"}"#,
        r#"{"X":"a"}"#,
        r#"{"S":null}"#,
        "null",
        r#"{"NS":["abc"]}"#,
        r#"{"M":{"a":{"N":"abc"}}}"#,
        r#"{"L":[{"N":"abc"}]}"#,
        r#"{"N":"1e400"}"#,
        r#"{"N":""}"#,
        r#"{},{"N":"abc"}"#,
        r#"{"N":"abc"},{}"#,
        r#"{"N":"1"},{"N":"abc"},{"SS":["a","a"]}"#,
    ];
    let cases: Vec<_> = refused
        .iter()
        .map(|p| {
            (
                "ExecuteStatement",
                statement(p, r#","Limit":0"#),
                VALIDATION,
                LIMIT_0,
            )
        })
        .collect();
    expect(&url, &cases).await;
}

#[tokio::test]
async fn execute_transaction_member_errors_beat_a_refused_parameter() {
    let url = start().await;
    let bad = r#"{"N":"abc"}"#;
    let select = SELECT;
    let long_token = "x".repeat(37);
    let token_too_long = format!(
        "1 validation error detected: Value '{long_token}' at 'clientRequestToken' failed to \
         satisfy constraint: Member must have length less than or equal to 36"
    );
    let cases = [
        (
            "ExecuteTransaction",
            transaction(&[bad], r#","ReturnConsumedCapacity":"BAD""#),
            VALIDATION,
            BAD_CAPACITY.to_string(),
        ),
        (
            "ExecuteTransaction",
            transaction(&[bad], &format!(r#","ClientRequestToken":"{long_token}""#)),
            VALIDATION,
            token_too_long.clone(),
        ),
        (
            "ExecuteTransaction",
            transaction(&["{}"], &format!(r#","ClientRequestToken":"{long_token}""#)),
            VALIDATION,
            token_too_long.clone(),
        ),
        (
            "ExecuteTransaction",
            format!(r#"{{"TransactStatements":[{{"Statement":"","Parameters":[{bad}]}}]}}"#),
            VALIDATION,
            "1 validation error detected: Value '' at 'transactStatements.1.member.statement' \
             failed to satisfy constraint: Member must have length greater than or equal to 1"
                .to_string(),
        ),
        (
            "ExecuteTransaction",
            format!(
                r#"{{"TransactStatements":[{{{select},"Parameters":[{bad}],"ReturnValuesOnConditionCheckFailure":"BAD"}}]}}"#
            ),
            VALIDATION,
            "1 validation error detected: Value 'BAD' at \
             'transactStatements.1.member.returnValuesOnConditionCheckFailure' failed to \
             satisfy constraint: Member must satisfy enum value set: [ALL_OLD, NONE]"
                .to_string(),
        ),
        (
            "ExecuteTransaction",
            format!(
                r#"{{"TransactStatements":[{{{select},"Parameters":[{bad}]}},{{{select},"Parameters":[]}}]}}"#
            ),
            VALIDATION,
            "1 validation error detected: Value '[]' at 'transactStatements.2.member.parameters' \
             failed to satisfy constraint: Member must have length greater than or equal to 1"
                .to_string(),
        ),
    ];
    let cases: Vec<_> = cases
        .iter()
        .map(|(o, b, k, m)| (*o, b.clone(), *k, m.as_str()))
        .collect();
    expect(&url, &cases).await;
}

#[tokio::test]
async fn a_parameter_of_the_wrong_json_type_fails_first() {
    let url = start().await;
    let cases = [
        (
            "ExecuteStatement",
            statement(r#"{"N":"abc"},{"N":1}"#, ""),
            SERIALIZATION,
            NUMBER_TO_STRING,
        ),
        (
            "ExecuteStatement",
            statement(r#"{"N":"abc"},{"N":1}"#, r#","Limit":0"#),
            SERIALIZATION,
            NUMBER_TO_STRING,
        ),
        (
            "ExecuteStatement",
            statement(r#"{"N":"abc"},{"S":1}"#, r#","Limit":0"#),
            SERIALIZATION,
            NUMBER_TO_STRING,
        ),
        (
            "ExecuteStatement",
            statement(r#"{"N":1}"#, r#","Limit":0"#),
            SERIALIZATION,
            NUMBER_TO_STRING,
        ),
        (
            "ExecuteStatement",
            statement(r#""str""#, ""),
            SERIALIZATION,
            NOT_AN_OBJECT,
        ),
        (
            "ExecuteStatement",
            statement(r#""str""#, r#","Limit":0"#),
            SERIALIZATION,
            NOT_AN_OBJECT,
        ),
        (
            "ExecuteTransaction",
            transaction(&[r#"{"N":"abc"}"#, r#"{"S":1}"#], ""),
            SERIALIZATION,
            NUMBER_TO_STRING,
        ),
        (
            "ExecuteTransaction",
            transaction(&[r#"{"S":1}"#], r#","ReturnConsumedCapacity":"BAD""#),
            SERIALIZATION,
            NUMBER_TO_STRING,
        ),
    ];
    expect(&url, &cases).await;
}

#[tokio::test]
async fn with_no_member_error_the_first_refused_parameter_is_reported() {
    // These answer the same on `main`.
    let url = start().await;
    let cases = [
        (
            "ExecuteStatement",
            statement(r#"{"N":"abc"}"#, ""),
            VALIDATION,
            ABC,
        ),
        (
            "ExecuteStatement",
            statement(r#"{},{"N":"abc"}"#, ""),
            VALIDATION,
            EMPTY,
        ),
        (
            "ExecuteStatement",
            statement(r#"{"N":"abc"},{}"#, ""),
            VALIDATION,
            ABC,
        ),
        // `NextToken` isn't a member check: the number still wins.
        (
            "ExecuteStatement",
            statement(r#"{"N":"abc"}"#, r#","NextToken":"garbage""#),
            VALIDATION,
            ABC,
        ),
        (
            "ExecuteTransaction",
            transaction(&[r#"{"N":"abc"}"#], ""),
            VALIDATION,
            ABC,
        ),
        (
            "ExecuteTransaction",
            transaction(&[r#"{"N":"abc"}"#, "{}"], ""),
            VALIDATION,
            ABC,
        ),
        (
            "ExecuteTransaction",
            transaction(&["{}", r#"{"N":"abc"}"#], ""),
            VALIDATION,
            EMPTY,
        ),
    ];
    expect(&url, &cases).await;
}
