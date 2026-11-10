//! A PartiQL statement whose parameters hold both a value nested too deep and
//! a number DynamoDB can't read is refused for whichever DynamoDB's walk meets
//! first: list elements in written order, a map's members in the order a Java
//! `HashMap` sized for them iterates.
//!
//! Captured against DynamoDB in eu-west-2 and us-east-1 on 2026-10-09, at 40
//! levels (inside what serde_json parses) and 100 (past it), on
//! ExecuteStatement, ExecuteTransaction and BatchExecuteStatement, and the map
//! order in eu-west-2 on 2026-11-09.
//! ExecuteTransaction reads every statement's parameters as one list, and a
//! bad `Limit` comes ahead of both faults (captured eu-west-2, 2026-11-09).
//! Dynoxide still reads a bad number ahead of `Limit`, so only the order with
//! the deep value first is pinned against `Limit` here. The bodies
//! are written by hand, because a `serde_json::Value` sorts a map's members
//! and would lose the order these tests are about.

#![cfg(feature = "http-server")]

use dynoxide::Database;
use serde_json::Value;

const NEST: &str = "Nesting Levels have exceeded supported limits: Attributes in the item have nested levels beyond supported limit";
const ABC: &str = "The parameter cannot be converted to a numeric value: abc";
const BAD: &str = r#"{"N":"abc"}"#;

const PAIR: &str = r#"SELECT * FROM \"Deep\" WHERE pk = 'a' AND x = ? AND y = ?"#;
const SINGLE: &str = r#"SELECT * FROM \"Deep\" WHERE pk = 'a' AND x = ?"#;
const UPDATE: &str = r#"UPDATE \"Deep\" SET x = ? SET y = ? WHERE pk = 'a'"#;

async fn start() -> String {
    let db = Database::memory().unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        dynoxide::server::serve_on(listener, db).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let url = format!("http://{addr}");
    let (status, body) = send(
        &url,
        "CreateTable",
        r#"{"TableName":"Deep","KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],"AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"BillingMode":"PAY_PER_REQUEST"}"#.to_string(),
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");
    url
}

async fn send(url: &str, operation: &str, body: String) -> (u16, Value) {
    let resp = reqwest::Client::new()
        .post(url)
        .header("x-amz-target", format!("DynamoDB_20120810.{operation}"))
        .header("content-type", "application/x-amz-json-1.0")
        .header(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=fakekey/20260101/us-east-1/dynamodb/aws4_request, SignedHeaders=host;x-amz-date;x-amz-target, Signature=fakesig",
        )
        .header("x-amz-date", "20260101T000000Z")
        .body(body)
        .send()
        .await
        .unwrap();
    let status = resp.status().as_u16();
    let text = resp.text().await.unwrap();
    (status, serde_json::from_str(&text).unwrap_or(Value::Null))
}

/// A leaf wrapped in `depth` single-member maps.
fn deep(depth: usize, leaf: &str) -> String {
    format!(
        "{}{leaf}{}",
        "{\"M\":{\"n\":".repeat(depth),
        "}}".repeat(depth)
    )
}

fn leaf_deep(depth: usize) -> String {
    deep(depth, r#"{"S":"leaf"}"#)
}

/// The message of a top-level ValidationException.
fn refusal(status: u16, body: &Value) -> String {
    assert_eq!(status, 400, "expected a 400, got {status}: {body}");
    assert_eq!(
        body["__type"], "com.amazon.coral.validate#ValidationException",
        "{body}"
    );
    body["message"].as_str().unwrap_or_default().to_string()
}

async fn execute_statement(url: &str, statement: &str, parameters: &str) -> String {
    let (status, body) = send(
        url,
        "ExecuteStatement",
        format!(r#"{{"Statement":"{statement}","Parameters":[{parameters}]}}"#),
    )
    .await;
    refusal(status, &body)
}

/// The orders from the captures, as (statement, parameters, answer).
fn the_orders(depth: usize) -> Vec<(&'static str, String, &'static str)> {
    let d = leaf_deep(depth);
    vec![
        (PAIR, format!("{d},{BAD}"), NEST),
        (PAIR, format!("{BAD},{d}"), ABC),
        (SINGLE, format!(r#"{{"M":{{"d":{d},"bad":{BAD}}}}}"#), NEST),
        (SINGLE, format!(r#"{{"M":{{"bad":{BAD},"d":{d}}}}}"#), ABC),
        (SINGLE, deep(depth, BAD), NEST),
        (PAIR, format!(r#"{d},{{"N":"1e126"}}"#), NEST),
        // `p` is met ahead of `a`, and `d` ahead of `b`, wherever written.
        (SINGLE, format!(r#"{{"M":{{"a":{d},"p":{BAD}}}}}"#), ABC),
        (SINGLE, format!(r#"{{"M":{{"p":{BAD},"a":{d}}}}}"#), ABC),
        (SINGLE, format!(r#"{{"M":{{"b":{BAD},"d":{d}}}}}"#), NEST),
        (SINGLE, format!(r#"{{"M":{{"d":{d},"b":{BAD}}}}}"#), NEST),
    ]
}

#[tokio::test]
async fn execute_statement_reports_the_first_fault_in_walk_order() {
    let url = start().await;
    for depth in [40, 100] {
        for (statement, parameters, answer) in the_orders(depth) {
            assert_eq!(
                execute_statement(&url, statement, &parameters).await,
                answer,
                "depth {depth}: {}",
                &parameters[..parameters.len().min(80)]
            );
        }
    }
}

#[tokio::test]
async fn execute_transaction_reports_the_first_fault_in_walk_order() {
    let url = start().await;
    for depth in [40, 100] {
        let d = leaf_deep(depth);
        for (parameters, answer) in [(format!("{d},{BAD}"), NEST), (format!("{BAD},{d}"), ABC)] {
            let (status, body) = send(
                &url,
                "ExecuteTransaction",
                format!(
                    r#"{{"TransactStatements":[{{"Statement":"{UPDATE}","Parameters":[{parameters}]}}]}}"#
                ),
            )
            .await;
            assert_eq!(refusal(status, &body), answer, "depth {depth}");
        }
    }
}

#[tokio::test]
async fn batch_execute_statement_reports_the_first_fault_per_statement() {
    let url = start().await;
    let (status, body) = send(
        &url,
        "PutItem",
        r#"{"TableName":"Deep","Item":{"pk":{"S":"a"}}}"#.to_string(),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let runs = r#"{"Statement":"SELECT * FROM \"Deep\" WHERE pk = ?","Parameters":[{"S":"a"}]}"#;
    for depth in [40, 100] {
        let d = leaf_deep(depth);
        let (status, body) = send(
            &url,
            "BatchExecuteStatement",
            format!(
                r#"{{"Statements":[{{"Statement":"{PAIR}","Parameters":[{d},{BAD}]}},{runs},{{"Statement":"{PAIR}","Parameters":[{BAD},{d}]}}]}}"#
            ),
        )
        .await;
        assert_eq!(status, 200, "depth {depth}: {body}");
        let responses = body["Responses"].as_array().expect("Responses");
        assert_eq!(responses[0]["Error"]["Code"], "ValidationError", "{body}");
        assert_eq!(
            responses[0]["Error"]["Message"], NEST,
            "depth {depth}: {body}"
        );
        assert_eq!(responses[1]["Item"]["pk"]["S"], "a", "{body}");
        assert_eq!(responses[2]["Error"]["Code"], "ValidationError", "{body}");
        assert_eq!(
            responses[2]["Error"]["Message"], ABC,
            "depth {depth}: {body}"
        );
    }
}

#[tokio::test]
async fn a_transaction_reads_all_its_parameters_as_one_list() {
    let url = start().await;
    let single =
        |parameters: &str| format!(r#"{{"Statement":"{SINGLE}","Parameters":[{parameters}]}}"#);
    for depth in [40, 100] {
        let d = leaf_deep(depth);
        for (first, second, answer) in [
            (single(&d), single(r#"{"N":"xyz"}"#), NEST.to_string()),
            (
                single(r#"{"N":"xyz"}"#),
                single(&d),
                "The parameter cannot be converted to a numeric value: xyz".to_string(),
            ),
        ] {
            let (status, body) = send(
                &url,
                "ExecuteTransaction",
                format!(r#"{{"TransactStatements":[{first},{second}]}}"#),
            )
            .await;
            assert_eq!(refusal(status, &body), answer, "depth {depth}");
        }
    }
}

#[tokio::test]
async fn a_bad_limit_comes_ahead_of_both_faults() {
    let url = start().await;
    for depth in [40, 100] {
        let (status, body) = send(
            &url,
            "ExecuteStatement",
            format!(
                r#"{{"Statement":"{PAIR}","Parameters":[{},{BAD}],"Limit":0}}"#,
                leaf_deep(depth)
            ),
        )
        .await;
        assert_eq!(refusal(status, &body), LIMIT, "depth {depth}");
    }
}

#[tokio::test]
async fn a_statement_with_one_kind_of_fault_answers_as_before() {
    // With one kind of fault nothing is rewritten, so each answer is what it
    // was before the order was read from the text.
    let url = start().await;
    for depth in [40, 100] {
        let (status, body) = send(
            &url,
            "ExecuteStatement",
            format!(
                r#"{{"Statement":"{PAIR}","Parameters":[{},{{"S":"x"}}],"Limit":0}}"#,
                leaf_deep(depth)
            ),
        )
        .await;
        assert_eq!(refusal(status, &body), LIMIT, "depth {depth}");
        assert_eq!(
            execute_statement(&url, PAIR, &format!(r#"{},{{"S":"x"}}"#, leaf_deep(depth))).await,
            NEST
        );
    }
    for parameters in [format!(r#"{BAD},{{"S":"x"}}"#), format!("{BAD},{BAD}")] {
        assert_eq!(execute_statement(&url, PAIR, &parameters).await, ABC);
    }
}

#[tokio::test]
async fn a_malformed_body_past_the_parser_depth_stays_a_bare_serialization_exception() {
    // The rewrite would make this body shallow enough for serde to name the
    // missing comma; it is answered as the malformed body it is.
    let url = start().await;
    let (status, body) = send(
        &url,
        "ExecuteStatement",
        format!(
            r#"{{"Statement":"{PAIR}","Parameters":[{BAD},{}] "Limit":5}}"#,
            leaf_deep(100)
        ),
    )
    .await;
    assert_eq!(status, 400, "{body}");
    assert_eq!(
        body,
        serde_json::json!({"__type": "com.amazon.coral.service#SerializationException"})
    );
}

const LIMIT: &str = "1 validation error detected: Value '0' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1";

#[tokio::test]
async fn a_parameter_value_with_two_members_is_answered_and_the_server_stays_up() {
    // A value holding both `N` and `M` is malformed; the walk leaves it to the
    // decoder rather than swapping one value inside another.
    let url = start().await;
    let parameters = format!(r#"{},{{"N":"abc","M":{{"a":{BAD}}}}}"#, leaf_deep(40));
    let (status, body) = send(
        &url,
        "ExecuteStatement",
        format!(r#"{{"Statement":"{PAIR}","Parameters":[{parameters}]}}"#),
    )
    .await;
    assert_eq!(status, 400, "{body}");
    let (status, body) = send(&url, "ListTables", "{}".to_string()).await;
    assert_eq!(status, 200, "{body}");
}

#[tokio::test]
async fn a_map_member_with_no_name_is_answered_and_the_server_stays_up() {
    let url = start().await;
    for depth in [40, 100] {
        let parameters = format!(r#"{},{{"M":{{{BAD}}}}}"#, leaf_deep(depth));
        let (status, body) = send(
            &url,
            "ExecuteStatement",
            format!(r#"{{"Statement":"{PAIR}","Parameters":[{parameters}]}}"#),
        )
        .await;
        assert_eq!(status, 400, "depth {depth}: {body}");
    }
    let (status, body) = send(&url, "ListTables", "{}".to_string()).await;
    assert_eq!(status, 200, "{body}");
}
