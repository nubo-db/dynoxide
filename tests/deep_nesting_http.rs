//! A request carrying an attribute value nested too deep for the JSON parser
//! still reaches validation.
//!
//! serde_json refuses a document nested more than 127 levels, which a value
//! reaches at about 61 attribute levels (each map or list level costs two
//! JSON levels, plus the request's own wrappers). DynamoDB reads such a body
//! and answers with its nesting `ValidationException`, so these requests used
//! to come back as a bare `SerializationException` where DynamoDB validates.
//!
//! Every expectation below was captured against real DynamoDB in us-east-1 and
//! eu-west-2 on 2026-10-09, and both regions agree on each one. Past the depth
//! where Dynoxide's parser gives up, DynamoDB has moved to its second nesting
//! check on every surface: the item forms and keys answer the bare message, an
//! `ExpressionAttributeValues` entry is named, and an `ExclusiveStartKey` takes
//! the starting-key prefix. DynamoDB's own parser gives up somewhere between
//! about 2,000 and 5,000 levels (it varies by run and region) and answers a
//! bare `SerializationException`.

#![cfg(feature = "http-server")]

use dynoxide::Database;
use serde_json::Value;

const NEST: &str = "Nesting Levels have exceeded supported limits: Attributes in the item have nested levels beyond supported limit";

fn nest_for_key(key: &str) -> String {
    format!("ExpressionAttributeValues contains invalid value: {NEST} for key {key}")
}

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

/// Send a raw body, since the deep values here can't round-trip through
/// `serde_json::Value` on the client side either.
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

/// A leaf wrapped in `depth` single-member maps: `{"M":{"n":...}}`.
fn deep_map(depth: usize) -> String {
    format!(
        "{}{{\"S\":\"leaf\"}}{}",
        "{\"M\":{\"n\":".repeat(depth),
        "}}".repeat(depth)
    )
}

/// The same depth built from single-element lists: `{"L":[...]}`.
fn deep_list(depth: usize) -> String {
    format!(
        "{}{{\"S\":\"leaf\"}}{}",
        "{\"L\":[".repeat(depth),
        "]}".repeat(depth)
    )
}

async fn create_table(url: &str, name: &str) {
    let (status, body) = send(
        url,
        "CreateTable",
        format!(
            r#"{{"TableName":"{name}","KeySchema":[{{"AttributeName":"pk","KeyType":"HASH"}}],"AttributeDefinitions":[{{"AttributeName":"pk","AttributeType":"S"}}],"BillingMode":"PAY_PER_REQUEST"}}"#
        ),
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");
}

async fn put_base_item(url: &str) {
    let (status, body) = send(
        url,
        "PutItem",
        r#"{"TableName":"Deep","Item":{"pk":{"S":"base"},"data":{"S":"x"}}}"#.to_string(),
    )
    .await;
    assert_eq!(status, 200, "PutItem failed: {body}");
}

fn assert_validation(status: u16, body: &Value, message: &str) {
    assert_eq!(status, 400, "expected a 400, got {status}: {body}");
    assert_eq!(
        body["__type"], "com.amazon.coral.validate#ValidationException",
        "expected a ValidationException: {body}"
    );
    assert_eq!(body["message"], message, "wrong message: {body}");
}

fn assert_bare_serialization(status: u16, body: &Value) {
    assert_eq!(status, 400, "expected a 400, got {status}: {body}");
    assert_eq!(
        body,
        &serde_json::json!({"__type": "com.amazon.coral.service#SerializationException"}),
        "expected a bare SerializationException"
    );
}

async fn item_exists(url: &str, pk: &str) -> bool {
    let (status, body) = send(
        url,
        "GetItem",
        format!(r#"{{"TableName":"Deep","Key":{{"pk":{{"S":"{pk}"}}}}}}"#),
    )
    .await;
    assert_eq!(status, 200, "GetItem failed: {body}");
    body.get("Item").is_some()
}

// ---- The item forms answer the bare message ----------------------------------

#[tokio::test]
async fn a_put_item_nested_63_levels_is_refused_with_the_bare_nesting_message() {
    let url = start().await;
    create_table(&url, "Deep").await;
    for value in [deep_map(63), deep_list(63)] {
        let (status, body) = send(
            &url,
            "PutItem",
            format!(r#"{{"TableName":"Deep","Item":{{"pk":{{"S":"p"}},"data":{value}}}}}"#),
        )
        .await;
        assert_validation(status, &body, NEST);
    }
    assert!(
        !item_exists(&url, "p").await,
        "the refused item was written"
    );
}

#[tokio::test]
async fn a_value_nested_2000_levels_is_still_read_and_refused() {
    let url = start().await;
    create_table(&url, "Deep").await;
    let (status, body) = send(
        &url,
        "PutItem",
        format!(
            r#"{{"TableName":"Deep","Item":{{"pk":{{"S":"p"}},"data":{}}}}}"#,
            deep_map(2000)
        ),
    )
    .await;
    assert_validation(status, &body, NEST);
}

#[tokio::test]
async fn a_value_nested_past_2000_levels_is_a_bare_serialization_exception() {
    let url = start().await;
    create_table(&url, "Deep").await;
    for value in [deep_map(2001), deep_list(5000)] {
        let (status, body) = send(
            &url,
            "PutItem",
            format!(r#"{{"TableName":"Deep","Item":{{"pk":{{"S":"p"}},"data":{value}}}}}"#),
        )
        .await;
        assert_bare_serialization(status, &body);
    }
}

#[tokio::test]
async fn a_batch_write_item_nested_61_levels_is_refused_with_the_bare_nesting_message() {
    let url = start().await;
    create_table(&url, "Deep").await;
    let (status, body) = send(
        &url,
        "BatchWriteItem",
        format!(
            r#"{{"RequestItems":{{"Deep":[{{"PutRequest":{{"Item":{{"pk":{{"S":"p"}},"data":{}}}}}}}]}}}}"#,
            deep_map(61)
        ),
    )
    .await;
    assert_validation(status, &body, NEST);
}

#[tokio::test]
async fn a_transact_put_item_nested_61_levels_is_refused_and_nothing_is_written() {
    let url = start().await;
    create_table(&url, "Deep").await;
    let (status, body) = send(
        &url,
        "TransactWriteItems",
        format!(
            r#"{{"TransactItems":[{{"Put":{{"TableName":"Deep","Item":{{"pk":{{"S":"ok"}}}}}}}},{{"Put":{{"TableName":"Deep","Item":{{"pk":{{"S":"p"}},"data":{}}}}}}}]}}"#,
            deep_map(61)
        ),
    )
    .await;
    assert_validation(status, &body, NEST);
    assert!(!item_exists(&url, "ok").await, "the valid Put was written");
}

#[tokio::test]
async fn keys_and_legacy_values_nested_100_levels_answer_the_bare_message() {
    let url = start().await;
    create_table(&url, "Deep").await;
    let v = deep_map(100);
    let cases = [
        (
            "GetItem",
            format!(r#"{{"TableName":"Deep","Key":{{"pk":{v}}}}}"#),
        ),
        (
            "DeleteItem",
            format!(r#"{{"TableName":"Deep","Key":{{"pk":{v}}}}}"#),
        ),
        (
            "BatchGetItem",
            format!(r#"{{"RequestItems":{{"Deep":{{"Keys":[{{"pk":{v}}}]}}}}}}"#),
        ),
        (
            "BatchWriteItem",
            format!(
                r#"{{"RequestItems":{{"Deep":[{{"DeleteRequest":{{"Key":{{"pk":{v}}}}}}}]}}}}"#
            ),
        ),
        (
            "TransactWriteItems",
            format!(
                r#"{{"TransactItems":[{{"Delete":{{"TableName":"Deep","Key":{{"pk":{v}}}}}}}]}}"#
            ),
        ),
        (
            "PutItem",
            format!(
                r#"{{"TableName":"Deep","Item":{{"pk":{{"S":"a"}}}},"Expected":{{"data":{{"Value":{v}}}}}}}"#
            ),
        ),
        (
            "UpdateItem",
            format!(
                r#"{{"TableName":"Deep","Key":{{"pk":{{"S":"a"}}}},"AttributeUpdates":{{"data":{{"Value":{v},"Action":"PUT"}}}}}}"#
            ),
        ),
        (
            "Scan",
            format!(
                r#"{{"TableName":"Deep","ScanFilter":{{"data":{{"ComparisonOperator":"EQ","AttributeValueList":[{v}]}}}}}}"#
            ),
        ),
        (
            "ExecuteStatement",
            format!(r#"{{"Statement":"SELECT * FROM \"Deep\" WHERE pk = ?","Parameters":[{v}]}}"#),
        ),
    ];
    for (operation, body) in cases {
        let (status, answer) = send(&url, operation, body).await;
        assert_eq!(status, 400, "{operation}: {answer}");
        assert_eq!(answer["message"], NEST, "{operation}: {answer}");
    }
}

// ---- ExpressionAttributeValues entries are named ------------------------------

#[tokio::test]
async fn an_expression_value_nested_63_levels_is_refused_naming_its_key() {
    let url = start().await;
    create_table(&url, "Deep").await;
    put_base_item(&url).await;
    for v in [deep_map(63), deep_list(63)] {
        let cases = [
            (
                "UpdateItem",
                format!(
                    r##"{{"TableName":"Deep","Key":{{"pk":{{"S":"p"}}}},"UpdateExpression":"SET #d = :deep","ExpressionAttributeNames":{{"#d":"data"}},"ExpressionAttributeValues":{{":deep":{v}}}}}"##
                ),
            ),
            (
                "UpdateItem",
                format!(
                    r##"{{"TableName":"Deep","Key":{{"pk":{{"S":"base"}}}},"UpdateExpression":"SET touched = :t","ConditionExpression":"#d = :deep","ExpressionAttributeNames":{{"#d":"data"}},"ExpressionAttributeValues":{{":t":{{"S":"y"}},":deep":{v}}}}}"##
                ),
            ),
            (
                "PutItem",
                format!(
                    r##"{{"TableName":"Deep","Item":{{"pk":{{"S":"base"}},"data":{{"S":"x"}}}},"ConditionExpression":"#d = :deep","ExpressionAttributeNames":{{"#d":"data"}},"ExpressionAttributeValues":{{":deep":{v}}}}}"##
                ),
            ),
            (
                "DeleteItem",
                format!(
                    r##"{{"TableName":"Deep","Key":{{"pk":{{"S":"absent"}}}},"ConditionExpression":"#d = :deep","ExpressionAttributeNames":{{"#d":"data"}},"ExpressionAttributeValues":{{":deep":{v}}}}}"##
                ),
            ),
            (
                "Scan",
                format!(
                    r##"{{"TableName":"Deep","FilterExpression":"#d = :deep","ExpressionAttributeNames":{{"#d":"data"}},"ExpressionAttributeValues":{{":deep":{v}}}}}"##
                ),
            ),
            (
                "Query",
                format!(
                    r##"{{"TableName":"Deep","KeyConditionExpression":"pk = :pk","FilterExpression":"#d = :deep","ExpressionAttributeNames":{{"#d":"data"}},"ExpressionAttributeValues":{{":pk":{{"S":"base"}},":deep":{v}}}}}"##
                ),
            ),
        ];
        for (operation, body) in cases {
            let (status, answer) = send(&url, operation, body).await;
            assert_eq!(status, 400, "{operation}: {answer}");
            assert_validation(status, &answer, &nest_for_key(":deep"));
        }
    }
}

#[tokio::test]
async fn a_transaction_expression_value_nested_61_levels_is_refused_naming_its_key() {
    let url = start().await;
    create_table(&url, "Deep").await;
    put_base_item(&url).await;
    for v in [deep_map(61), deep_list(61)] {
        for action in [
            format!(
                r##"{{"ConditionCheck":{{"TableName":"Deep","Key":{{"pk":{{"S":"base"}}}},"ConditionExpression":"#d = :deep","ExpressionAttributeNames":{{"#d":"data"}},"ExpressionAttributeValues":{{":deep":{v}}}}}}}"##
            ),
            format!(
                r##"{{"Update":{{"TableName":"Deep","Key":{{"pk":{{"S":"u"}}}},"UpdateExpression":"SET #d = :deep","ExpressionAttributeNames":{{"#d":"data"}},"ExpressionAttributeValues":{{":deep":{v}}}}}}}"##
            ),
        ] {
            let (status, answer) = send(
                &url,
                "TransactWriteItems",
                format!(r#"{{"TransactItems":[{action}]}}"#),
            )
            .await;
            assert_validation(status, &answer, &nest_for_key(":deep"));
        }
    }
}

#[tokio::test]
async fn an_exclusive_start_key_nested_63_levels_takes_the_starting_key_prefix() {
    let url = start().await;
    create_table(&url, "Deep").await;
    let (status, body) = send(
        &url,
        "Scan",
        format!(
            r#"{{"TableName":"Deep","ExclusiveStartKey":{{"pk":{}}}}}"#,
            deep_map(63)
        ),
    )
    .await;
    assert_validation(
        status,
        &body,
        &format!("The provided starting key is invalid: {NEST}"),
    );
}

// ---- Which deep value is reported, and what comes first -----------------------

#[tokio::test]
async fn the_first_deep_value_in_the_request_is_the_one_reported() {
    let url = start().await;
    create_table(&url, "Deep").await;
    put_base_item(&url).await;
    let v = deep_map(100);

    let (status, body) = send(
        &url,
        "UpdateItem",
        format!(
            r#"{{"TableName":"Deep","Key":{{"pk":{{"S":"a"}}}},"UpdateExpression":"SET x = :zz, y = :aa","ExpressionAttributeValues":{{":zz":{v},":aa":{v}}}}}"#
        ),
    )
    .await;
    assert_validation(status, &body, &nest_for_key(":zz"));

    let check = format!(
        r##"{{"ConditionCheck":{{"TableName":"Deep","Key":{{"pk":{{"S":"base"}}}},"ConditionExpression":"#d = :deep","ExpressionAttributeNames":{{"#d":"data"}},"ExpressionAttributeValues":{{":deep":{v}}}}}}}"##
    );
    let put = format!(r#"{{"Put":{{"TableName":"Deep","Item":{{"pk":{{"S":"b"}},"data":{v}}}}}}}"#);
    let (status, body) = send(
        &url,
        "TransactWriteItems",
        format!(r#"{{"TransactItems":[{check},{put}]}}"#),
    )
    .await;
    assert_validation(status, &body, &nest_for_key(":deep"));
    let (status, body) = send(
        &url,
        "TransactWriteItems",
        format!(r#"{{"TransactItems":[{put},{check}]}}"#),
    )
    .await;
    assert_validation(status, &body, NEST);
}

#[tokio::test]
async fn a_deep_value_is_reported_ahead_of_a_missing_table_a_syntax_error_or_an_unused_value() {
    let url = start().await;
    create_table(&url, "Deep").await;
    let v = deep_map(100);

    let (status, body) = send(
        &url,
        "PutItem",
        format!(r#"{{"TableName":"Missing","Item":{{"pk":{{"S":"a"}},"data":{v}}}}}"#),
    )
    .await;
    assert_validation(status, &body, NEST);

    let (status, body) = send(
        &url,
        "PutItem",
        format!(r#"{{"TableName":"Deep","Item":{{"other":{{"S":"a"}},"data":{v}}}}}"#),
    )
    .await;
    assert_validation(status, &body, NEST);

    let (status, body) = send(
        &url,
        "UpdateItem",
        format!(
            r#"{{"TableName":"Deep","Key":{{"pk":{{"S":"a"}}}},"UpdateExpression":"SET = :deep","ExpressionAttributeValues":{{":deep":{v}}}}}"#
        ),
    )
    .await;
    assert_validation(status, &body, &nest_for_key(":deep"));

    let (status, body) = send(
        &url,
        "UpdateItem",
        format!(
            r#"{{"TableName":"Deep","Key":{{"pk":{{"S":"a"}}}},"UpdateExpression":"SET x = :x","ExpressionAttributeValues":{{":x":{{"S":"1"}},":deep":{v}}}}}"#
        ),
    )
    .await;
    assert_validation(status, &body, &nest_for_key(":deep"));

    let (status, body) = send(
        &url,
        "TransactWriteItems",
        format!(
            r#"{{"TransactItems":[{{"Put":{{"TableName":"Missing","Item":{{"pk":{{"S":"a"}}}}}}}},{{"Put":{{"TableName":"Deep","Item":{{"pk":{{"S":"b"}},"data":{v}}}}}}}]}}"#
        ),
    )
    .await;
    assert_validation(status, &body, NEST);
}

#[tokio::test]
async fn a_member_constraint_is_reported_ahead_of_a_deep_value() {
    let url = start().await;
    create_table(&url, "Deep").await;
    let v = deep_map(100);

    let (status, body) = send(
        &url,
        "PutItem",
        format!(r#"{{"TableName":"bad!name","Item":{{"pk":{{"S":"a"}},"data":{v}}}}}"#),
    )
    .await;
    assert_validation(
        status,
        &body,
        "1 validation error detected: Value 'bad!name' at 'tableName' failed to satisfy constraint: Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+",
    );

    let (status, body) = send(
        &url,
        "PutItem",
        format!(
            r#"{{"TableName":"Deep","Item":{{"pk":{{"S":"a"}},"data":{v}}},"ReturnValues":"BOGUS"}}"#
        ),
    )
    .await;
    assert_validation(
        status,
        &body,
        "1 validation error detected: Value 'BOGUS' at 'returnValues' failed to satisfy constraint: Member must satisfy enum value set: [ALL_NEW, UPDATED_OLD, ALL_OLD, NONE, UPDATED_NEW]",
    );

    let (status, body) = send(
        &url,
        "UpdateItem",
        format!(
            r##"{{"TableName":"bad!name","Key":{{"pk":{{"S":"a"}}}},"UpdateExpression":"SET #d = :deep","ExpressionAttributeNames":{{"#d":"data"}},"ExpressionAttributeValues":{{":deep":{v}}}}}"##
        ),
    )
    .await;
    assert_validation(
        status,
        &body,
        "1 validation error detected: Value 'bad!name' at 'tableName' failed to satisfy constraint: Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+",
    );

    let (status, body) = send(
        &url,
        "Scan",
        format!(
            r##"{{"TableName":"Deep","Limit":0,"FilterExpression":"#d = :deep","ExpressionAttributeNames":{{"#d":"data"}},"ExpressionAttributeValues":{{":deep":{v}}}}}"##
        ),
    )
    .await;
    assert_validation(
        status,
        &body,
        "1 validation error detected: Value at 'Limit' failed to satisfy constraint: Member must have value greater than or equal to 1",
    );
}

// BatchWriteItem and TransactWriteItems check their members after decoding;
// those checks still come ahead of a deep value. Captured eu-west-2 and
// us-east-1, 2026-10-09, with a value nested 100 levels.
#[tokio::test]
async fn batch_and_transaction_member_errors_are_reported_ahead_of_a_deep_value() {
    let url = start().await;
    create_table(&url, "Deep").await;
    let v = deep_map(100);
    let rcc = "1 validation error detected: Value 'BOGUS' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]";

    let (status, body) = send(
        &url,
        "BatchWriteItem",
        format!(
            r#"{{"RequestItems":{{"Deep":[{{"PutRequest":{{"Item":{{"pk":{{"S":"a"}},"data":{v}}}}}}}]}},"ReturnConsumedCapacity":"BOGUS"}}"#
        ),
    )
    .await;
    assert_validation(status, &body, rcc);

    let (status, body) = send(
        &url,
        "TransactWriteItems",
        format!(
            r#"{{"TransactItems":[{{"Put":{{"TableName":"Deep","Item":{{"pk":{{"S":"b"}},"data":{v}}}}}}}],"ReturnConsumedCapacity":"BOGUS"}}"#
        ),
    )
    .await;
    assert_validation(status, &body, rcc);

    let (status, body) = send(
        &url,
        "TransactWriteItems",
        format!(
            r#"{{"TransactItems":[{{"Put":{{"TableName":"bad!name","Item":{{"pk":{{"S":"b"}},"data":{v}}}}}}}]}}"#
        ),
    )
    .await;
    assert_validation(
        status,
        &body,
        "1 validation error detected: Value 'bad!name' at 'transactItems.1.member.put.tableName' failed to satisfy constraint: Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+",
    );

    let (status, body) = send(
        &url,
        "BatchWriteItem",
        format!(
            r#"{{"RequestItems":{{"bad!name":[{{"PutRequest":{{"Item":{{"pk":{{"S":"a"}},"data":{v}}}}}}}]}}}}"#
        ),
    )
    .await;
    assert_eq!(status, 400, "expected a 400, got {status}: {body}");
    let message = body["message"].as_str().unwrap_or_default();
    assert!(
        message.starts_with("1 validation error detected: Value '{bad!name=[WriteRequest(")
            && message.ends_with(
                "' at 'requestItems' failed to satisfy constraint: Map keys must satisfy constraint: [Member must have length less than or equal to 255, Member must have length greater than or equal to 3, Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+]"
            ),
        "expected the RequestItems map-key constraint, not the nesting error: {body}"
    );
}

// ---- BatchExecuteStatement answers per statement ------------------------------

#[tokio::test]
async fn a_batch_statement_parameter_nested_63_levels_fails_that_statement_only() {
    let url = start().await;
    create_table(&url, "Deep").await;
    put_base_item(&url).await;
    let (status, body) = send(
        &url,
        "BatchExecuteStatement",
        format!(
            r#"{{"Statements":[{{"Statement":"SELECT * FROM \"Deep\" WHERE pk = ?","Parameters":[{{"S":"base"}}]}},{{"Statement":"SELECT * FROM \"Deep\" WHERE pk = ?","Parameters":[{}]}}]}}"#,
            deep_map(63)
        ),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let responses = body["Responses"].as_array().expect("Responses");
    assert_eq!(responses[0]["Item"]["pk"]["S"], "base", "{body}");
    assert_eq!(responses[1]["Error"]["Code"], "ValidationError", "{body}");
    assert_eq!(responses[1]["Error"]["Message"], NEST, "{body}");
}

/// Send a BatchExecuteStatement and return its status and body.
async fn batch(url: &str, statements: &[String]) -> (u16, Value) {
    send(
        url,
        "BatchExecuteStatement",
        format!(r#"{{"Statements":[{}]}}"#, statements.join(",")),
    )
    .await
}

#[tokio::test]
async fn a_statement_refused_for_its_own_parameter_drops_out_of_the_batch_checks() {
    // Captured eu-west-2 and us-east-1, 2026-10-09: a statement whose
    // parameter is nested 63 levels fails on its own and takes no part in the
    // mixed read/write or duplicate checks, so the batch returns 200 and runs
    // the rest. Without such a statement the checks refuse as before. An
    // empty attribute name in a parameter drops out the same way.
    let url = start().await;
    create_table(&url, "Deep").await;
    put_base_item(&url).await;
    let (status, body) = send(
        &url,
        "PutItem",
        r#"{"TableName":"Deep","Item":{"pk":{"S":"b"},"x":{"S":"old"}}}"#.to_string(),
    )
    .await;
    assert_eq!(status, 200, "{body}");

    let select = statement("SELECT * FROM \\\"Deep\\\" WHERE pk = ?", r#"{"S":"base"}"#);
    let deep_update = statement(
        "UPDATE \\\"Deep\\\" SET x = ? WHERE pk = 'b'",
        &deep_map(63),
    );
    let plain_update = statement(
        "UPDATE \\\"Deep\\\" SET x = ? WHERE pk = 'b'",
        r#"{"S":"v"}"#,
    );
    let deep_select = format!(
        r#"{{"Statement":"SELECT * FROM \"Deep\" WHERE pk = 'base' AND x = ?","Parameters":[{}]}}"#,
        deep_map(63)
    );
    let empty_name_insert = statement(
        "INSERT INTO \\\"Deep\\\" VALUE {'pk': 'e', 'm': ?}",
        r#"{"M":{"":{"S":"x"}}}"#,
    );

    let outcome = |body: &Value| -> Vec<String> {
        body["Responses"]
            .as_array()
            .expect("Responses")
            .iter()
            .map(|r| match r["Error"]["Message"].as_str() {
                Some(m) => format!("error: {m}"),
                None => "ran".to_string(),
            })
            .collect()
    };
    let nest = format!("error: {NEST}");
    for (statements, expected) in [
        (
            vec![select.clone(), deep_update.clone()],
            vec!["ran".to_string(), nest.clone()],
        ),
        (
            vec![deep_update.clone(), select.clone()],
            vec![nest.clone(), "ran".to_string()],
        ),
        (
            vec![deep_select.clone(), plain_update.clone()],
            vec![nest.clone(), "ran".to_string()],
        ),
        (
            vec![plain_update.clone(), deep_update.clone()],
            vec!["ran".to_string(), nest.clone()],
        ),
        (
            vec![deep_update.clone(), plain_update.clone()],
            vec![nest.clone(), "ran".to_string()],
        ),
        (
            vec![select.clone(), empty_name_insert.clone()],
            vec!["ran".to_string(), "error: Empty attribute name".to_string()],
        ),
    ] {
        let (status, body) = batch(&url, &statements).await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(outcome(&body), expected, "{body}");
    }

    // Without a statement refused on its own, the batch-wide checks still fire.
    let (status, body) = batch(&url, &[select.clone(), plain_update.clone()]).await;
    assert_validation(
        status,
        &body,
        "Read and write requests together in the same batch is not supported.",
    );
    let (status, body) = batch(&url, &[plain_update.clone(), plain_update.clone()]).await;
    assert_validation(
        status,
        &body,
        "Provided list of item keys contains duplicates",
    );
}

// ---- PartiQL parameters are checked before the statement runs -----------------
//
// From 33 levels a parameter is refused before its statement is parsed or its
// table looked up: ExecuteStatement and ExecuteTransaction answer at the top
// level (a transaction is not cancelled, and nothing in it is written), and
// BatchExecuteStatement fails that statement alone. A member constraint such
// as `Limit` still comes first. At 32 levels the statement runs. Captured in
// us-east-1 and eu-west-2, 2026-10-09.

fn statement(sql: &str, parameter: &str) -> String {
    format!(r#"{{"Statement":"{sql}","Parameters":[{parameter}]}}"#)
}

#[tokio::test]
async fn a_statement_parameter_nested_33_levels_is_refused_before_the_statement_runs() {
    let url = start().await;
    create_table(&url, "Deep").await;
    put_base_item(&url).await;
    for depth in [33, 40, 61] {
        let v = deep_map(depth);
        for sql in [
            r#"SELECT * FROM \"Deep\" WHERE pk = ?"#,
            r#"SELECT * FROM \"Deep\" WHERE pk = 'base' AND data = ?"#,
            r#"SELEC * FROM \"Deep\" WHERE pk = ?"#,
            r#"SELECT * FROM \"Missing\" WHERE pk = ?"#,
            r#"INSERT INTO \"Deep\" VALUE {'pk': 'ins', 'data': ?}"#,
        ] {
            let (status, body) = send(&url, "ExecuteStatement", statement(sql, &v)).await;
            assert_eq!(status, 400, "{depth} {sql}: {body}");
            assert_validation(status, &body, NEST);
        }
    }
    assert!(
        !item_exists(&url, "ins").await,
        "the refused INSERT was written"
    );

    // A member constraint is still reported first.
    let (status, body) = send(
        &url,
        "ExecuteStatement",
        format!(
            r#"{{"Statement":"SELECT * FROM \"Deep\" WHERE pk = ?","Parameters":[{}],"Limit":0}}"#,
            deep_map(40)
        ),
    )
    .await;
    assert_validation(
        status,
        &body,
        "1 validation error detected: Value '0' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1",
    );
}

#[tokio::test]
async fn a_transaction_statement_parameter_nested_33_levels_fails_the_transaction_up_front() {
    let url = start().await;
    create_table(&url, "Deep").await;
    put_base_item(&url).await;
    for depth in [40, 100] {
        let (status, body) = send(
            &url,
            "ExecuteTransaction",
            format!(
                r#"{{"TransactStatements":[{{"Statement":"INSERT INTO \"Deep\" VALUE {{'pk': 'etx'}}"}},{}]}}"#,
                statement(r#"INSERT INTO \"Deep\" VALUE {'pk': 'etx2', 'data': ?}"#, &deep_map(depth))
            ),
        )
        .await;
        assert_validation(status, &body, NEST);
    }
    assert!(
        !item_exists(&url, "etx").await,
        "a statement ahead of the refused one was written"
    );

    // An earlier statement's own failure still comes first.
    let (status, body) = send(
        &url,
        "ExecuteTransaction",
        format!(
            r#"{{"TransactStatements":[{{"Statement":"INSRT INTO \"Deep\" VALUE {{'pk': 'x'}}"}},{}]}}"#,
            statement(r#"INSERT INTO \"Deep\" VALUE {'pk': 'etx3', 'data': ?}"#, &deep_map(40))
        ),
    )
    .await;
    assert_eq!(status, 400, "{body}");
    assert_ne!(
        body["message"], NEST,
        "the later statement's parameter was reported first: {body}"
    );
}

#[tokio::test]
async fn a_batch_statement_parameter_nested_33_levels_fails_that_statement_whatever_else_is_wrong()
{
    let url = start().await;
    create_table(&url, "Deep").await;
    put_base_item(&url).await;
    let v = deep_map(40);
    for sql in [
        r#"SELECT * FROM \"Deep\" WHERE pk = ?"#,
        r#"SELECT * FROM \"Deep\" WHERE pk = 'base' AND data = ?"#,
        r#"SELEC * FROM \"Deep\" WHERE pk = ?"#,
        r#"SELECT * FROM \"Missing\" WHERE pk = ?"#,
    ] {
        let (status, body) = send(
            &url,
            "BatchExecuteStatement",
            format!(r#"{{"Statements":[{}]}}"#, statement(sql, &v)),
        )
        .await;
        assert_eq!(status, 200, "{sql}: {body}");
        assert_eq!(
            body["Responses"][0]["Error"]["Code"], "ValidationError",
            "{sql}: {body}"
        );
        assert_eq!(
            body["Responses"][0]["Error"]["Message"], NEST,
            "{sql}: {body}"
        );
    }
}

#[tokio::test]
async fn a_statement_parameter_nested_32_levels_still_runs() {
    let url = start().await;
    create_table(&url, "Deep").await;
    put_base_item(&url).await;
    let (status, body) = send(
        &url,
        "ExecuteStatement",
        statement(
            r#"SELECT * FROM \"Deep\" WHERE pk = 'base' AND data = ?"#,
            &deep_map(32),
        ),
    )
    .await;
    assert_eq!(status, 200, "{body}");
}

// ---- Keys, starting keys and legacy parameters from 32 levels -----------------
//
// Below the depth where the parser gives up, these positions still get the
// nesting check, from the same 32 levels an item does. A key is refused before
// it is matched against the schema, so a map nested 32 levels never reaches
// "The provided key element does not match the schema". The form depends on
// the operation: bare on GetItem, BatchGetItem, BatchWriteItem's DeleteRequest,
// TransactGetItems and a transaction's Delete (at the top level, not as a
// cancellation), and inside the validation envelope on DeleteItem's and
// UpdateItem's Key, PutItem's Expected and UpdateItem's AttributeUpdates.
// Captured in us-east-1 and eu-west-2, 2026-10-09, at 31, 32, 33, 60, 61 and 62
// levels; the regions agree unless a test says otherwise.

fn enveloped() -> String {
    format!("1 validation error detected: {NEST}")
}

#[tokio::test]
async fn a_key_nested_32_levels_is_refused_before_it_is_matched_to_the_schema() {
    let url = start().await;
    create_table(&url, "Deep").await;
    for depth in [32, 40, 61, 62] {
        let v = deep_map(depth);
        let bare = [
            (
                "GetItem",
                format!(r#"{{"TableName":"Deep","Key":{{"pk":{v}}}}}"#),
            ),
            (
                "BatchGetItem",
                format!(r#"{{"RequestItems":{{"Deep":{{"Keys":[{{"pk":{v}}}]}}}}}}"#),
            ),
            (
                "BatchWriteItem",
                format!(
                    r#"{{"RequestItems":{{"Deep":[{{"DeleteRequest":{{"Key":{{"pk":{v}}}}}}}]}}}}"#
                ),
            ),
            (
                "TransactGetItems",
                format!(
                    r#"{{"TransactItems":[{{"Get":{{"TableName":"Deep","Key":{{"pk":{v}}}}}}}]}}"#
                ),
            ),
            (
                "TransactWriteItems",
                format!(
                    r#"{{"TransactItems":[{{"Delete":{{"TableName":"Deep","Key":{{"pk":{v}}}}}}}]}}"#
                ),
            ),
        ];
        for (operation, body) in bare {
            let (status, answer) = send(&url, operation, body).await;
            assert_eq!(status, 400, "{operation} at {depth}: {answer}");
            assert_validation(status, &answer, NEST);
        }
        let enveloped_cases = [
            (
                "DeleteItem",
                format!(r#"{{"TableName":"Deep","Key":{{"pk":{v}}}}}"#),
            ),
            (
                "UpdateItem",
                format!(
                    r#"{{"TableName":"Deep","Key":{{"pk":{v}}},"UpdateExpression":"SET a = :a","ExpressionAttributeValues":{{":a":{{"S":"x"}}}}}}"#
                ),
            ),
        ];
        for (operation, body) in enveloped_cases {
            let (status, answer) = send(&url, operation, body).await;
            assert_eq!(status, 400, "{operation} at {depth}: {answer}");
            assert_validation(status, &answer, &enveloped());
        }
    }
}

#[tokio::test]
async fn a_transaction_key_nested_32_levels_fails_the_request_and_writes_nothing() {
    let url = start().await;
    create_table(&url, "Deep").await;
    let (status, body) = send(
        &url,
        "TransactWriteItems",
        format!(
            r#"{{"TransactItems":[{{"Put":{{"TableName":"Deep","Item":{{"pk":{{"S":"ok"}}}}}}}},{{"Delete":{{"TableName":"Deep","Key":{{"pk":{}}}}}}}]}}"#,
            deep_map(40)
        ),
    )
    .await;
    assert_validation(status, &body, NEST);
    assert!(!item_exists(&url, "ok").await, "the valid Put was written");
}

#[tokio::test]
async fn a_legacy_expected_or_attribute_update_nested_32_levels_is_refused_in_the_envelope() {
    let url = start().await;
    create_table(&url, "Deep").await;
    put_base_item(&url).await;
    for depth in [32, 61] {
        let v = deep_map(depth);
        let cases = [
            (
                "PutItem",
                format!(
                    r#"{{"TableName":"Deep","Item":{{"pk":{{"S":"base"}}}},"Expected":{{"data":{{"Value":{v}}}}}}}"#
                ),
            ),
            (
                "PutItem",
                format!(
                    r#"{{"TableName":"Deep","Item":{{"pk":{{"S":"base"}}}},"Expected":{{"data":{{"ComparisonOperator":"EQ","AttributeValueList":[{v}]}}}}}}"#
                ),
            ),
            (
                "UpdateItem",
                format!(
                    r#"{{"TableName":"Deep","Key":{{"pk":{{"S":"base"}}}},"AttributeUpdates":{{"data":{{"Value":{v},"Action":"PUT"}}}}}}"#
                ),
            ),
        ];
        for (operation, body) in cases {
            let (status, answer) = send(&url, operation, body).await;
            assert_eq!(status, 400, "{operation} at {depth}: {answer}");
            assert_validation(status, &answer, &enveloped());
        }
    }
    // Nothing was overwritten.
    let (_, item) = send(
        &url,
        "GetItem",
        r#"{"TableName":"Deep","Key":{"pk":{"S":"base"}}}"#.to_string(),
    )
    .await;
    assert_eq!(item["Item"]["data"]["S"], "x", "{item}");
}

#[tokio::test]
async fn a_scan_filter_value_nested_33_levels_is_refused_bare() {
    let url = start().await;
    create_table(&url, "Deep").await;
    let scan = |depth| {
        format!(
            r#"{{"TableName":"Deep","ScanFilter":{{"data":{{"ComparisonOperator":"EQ","AttributeValueList":[{}]}}}}}}"#,
            deep_map(depth)
        )
    };
    // Seven of ten regions sampled run a 32-level ScanFilter value and refuse
    // one nested 33; eu-west-2, eu-central-1 and ap-southeast-2 refuse from 32.
    // This follows the majority.
    let (status, body) = send(&url, "Scan", scan(32)).await;
    assert_eq!(status, 200, "{body}");
    for depth in [33, 61] {
        let (status, body) = send(&url, "Scan", scan(depth)).await;
        assert_validation(status, &body, NEST);
    }
}

#[tokio::test]
async fn a_scan_start_key_nested_32_levels_takes_the_starting_key_prefix() {
    let url = start().await;
    create_table(&url, "Deep").await;
    // Seven of ten regions sampled prefix the message from 32 levels, as every
    // region does from 63; eu-west-2, eu-central-1 and ap-southeast-2 answer
    // it bare up to 62. This follows the majority.
    for depth in [32, 61] {
        let (status, body) = send(
            &url,
            "Scan",
            format!(
                r#"{{"TableName":"Deep","ExclusiveStartKey":{{"pk":{}}}}}"#,
                deep_map(depth)
            ),
        )
        .await;
        assert_validation(
            status,
            &body,
            &format!("The provided starting key is invalid: {NEST}"),
        );
    }
}

#[tokio::test]
async fn query_start_keys_and_legacy_conditions_nested_32_levels_are_refused_bare() {
    let url = start().await;
    create_table(&url, "Deep").await;
    for depth in [32, 61] {
        let v = deep_map(depth);
        let cases = [
            // Bare below 63 levels in all ten regions sampled, unlike Scan's.
            format!(
                r#"{{"TableName":"Deep","KeyConditionExpression":"pk = :pk","ExpressionAttributeValues":{{":pk":{{"S":"a"}}}},"ExclusiveStartKey":{{"pk":{v}}}}}"#
            ),
            format!(
                r#"{{"TableName":"Deep","KeyConditions":{{"pk":{{"ComparisonOperator":"EQ","AttributeValueList":[{{"S":"a"}}]}}}},"QueryFilter":{{"data":{{"ComparisonOperator":"EQ","AttributeValueList":[{v}]}}}}}}"#
            ),
            format!(
                r#"{{"TableName":"Deep","KeyConditions":{{"pk":{{"ComparisonOperator":"EQ","AttributeValueList":[{v}]}}}}}}"#
            ),
        ];
        for body in cases {
            let (status, answer) = send(&url, "Query", body).await;
            assert_eq!(status, 400, "{depth}: {answer}");
            assert_validation(status, &answer, NEST);
        }
    }
}

#[tokio::test]
async fn a_delete_or_update_expected_value_nested_32_levels_is_refused_in_the_envelope() {
    let url = start().await;
    create_table(&url, "Deep").await;
    put_base_item(&url).await;
    for depth in [32, 61] {
        let v = deep_map(depth);
        let cases = [
            (
                "DeleteItem",
                format!(
                    r#"{{"TableName":"Deep","Key":{{"pk":{{"S":"base"}}}},"Expected":{{"data":{{"Value":{v}}}}}}}"#
                ),
            ),
            (
                "UpdateItem",
                format!(
                    r#"{{"TableName":"Deep","Key":{{"pk":{{"S":"base"}}}},"AttributeUpdates":{{"x":{{"Value":{{"S":"1"}},"Action":"PUT"}}}},"Expected":{{"data":{{"Value":{v}}}}}}}"#
                ),
            ),
        ];
        for (operation, body) in cases {
            let (status, answer) = send(&url, operation, body).await;
            assert_eq!(status, 400, "{operation} at {depth}: {answer}");
            assert_validation(status, &answer, &enveloped());
        }
    }
    assert!(
        item_exists(&url, "base").await,
        "the refused DeleteItem ran"
    );
}

#[tokio::test]
async fn a_transaction_condition_check_or_update_key_nested_32_levels_fails_the_request() {
    let url = start().await;
    create_table(&url, "Deep").await;
    for depth in [32, 61] {
        let v = deep_map(depth);
        for action in [
            format!(
                r#"{{"ConditionCheck":{{"TableName":"Deep","Key":{{"pk":{v}}},"ConditionExpression":"attribute_exists(pk)"}}}}"#
            ),
            format!(
                r#"{{"Update":{{"TableName":"Deep","Key":{{"pk":{v}}},"UpdateExpression":"SET a = :a","ExpressionAttributeValues":{{":a":{{"S":"x"}}}}}}}}"#
            ),
        ] {
            let (status, body) = send(
                &url,
                "TransactWriteItems",
                format!(r#"{{"TransactItems":[{action}]}}"#),
            )
            .await;
            assert_validation(status, &body, NEST);
        }
    }
}

#[tokio::test]
async fn a_nested_key_or_legacy_value_is_reported_ahead_of_a_missing_table() {
    let url = start().await;
    let v = deep_map(40);
    let bare = [
        (
            "GetItem",
            format!(r#"{{"TableName":"Missing","Key":{{"pk":{v}}}}}"#),
        ),
        (
            "TransactGetItems",
            format!(
                r#"{{"TransactItems":[{{"Get":{{"TableName":"Missing","Key":{{"pk":{v}}}}}}}]}}"#
            ),
        ),
        (
            "TransactWriteItems",
            format!(
                r#"{{"TransactItems":[{{"Delete":{{"TableName":"Missing","Key":{{"pk":{v}}}}}}}]}}"#
            ),
        ),
        (
            "Scan",
            format!(
                r#"{{"TableName":"Missing","ScanFilter":{{"data":{{"ComparisonOperator":"EQ","AttributeValueList":[{v}]}}}}}}"#
            ),
        ),
    ];
    for (operation, body) in bare {
        let (status, answer) = send(&url, operation, body).await;
        assert_eq!(status, 400, "{operation}: {answer}");
        assert_validation(status, &answer, NEST);
    }
    let enveloped_cases = [
        (
            "DeleteItem",
            format!(r#"{{"TableName":"Missing","Key":{{"pk":{v}}}}}"#),
        ),
        (
            "UpdateItem",
            format!(
                r#"{{"TableName":"Missing","Key":{{"pk":{{"S":"a"}}}},"AttributeUpdates":{{"data":{{"Value":{v},"Action":"PUT"}}}}}}"#
            ),
        ),
        (
            "PutItem",
            format!(
                r#"{{"TableName":"Missing","Item":{{"pk":{{"S":"a"}}}},"Expected":{{"data":{{"Value":{v}}}}}}}"#
            ),
        ),
    ];
    for (operation, body) in enveloped_cases {
        let (status, answer) = send(&url, operation, body).await;
        assert_eq!(status, 400, "{operation}: {answer}");
        assert_validation(status, &answer, &enveloped());
    }
    let (status, answer) = send(
        &url,
        "Scan",
        format!(r#"{{"TableName":"Missing","ExclusiveStartKey":{{"pk":{v}}}}}"#),
    )
    .await;
    assert_validation(
        status,
        &answer,
        &format!("The provided starting key is invalid: {NEST}"),
    );
}

#[tokio::test]
async fn values_nested_31_levels_in_these_positions_are_not_refused_for_nesting() {
    let url = start().await;
    create_table(&url, "Deep").await;
    put_base_item(&url).await;
    let v = deep_map(31);
    let (status, body) = send(
        &url,
        "GetItem",
        format!(r#"{{"TableName":"Deep","Key":{{"pk":{v}}}}}"#),
    )
    .await;
    assert_validation(
        status,
        &body,
        "The provided key element does not match the schema",
    );

    let (status, body) = send(
        &url,
        "UpdateItem",
        format!(
            r#"{{"TableName":"Deep","Key":{{"pk":{{"S":"u31"}}}},"AttributeUpdates":{{"data":{{"Value":{v},"Action":"PUT"}}}}}}"#
        ),
    )
    .await;
    assert_eq!(status, 200, "{body}");

    let (status, body) = send(
        &url,
        "Scan",
        format!(
            r#"{{"TableName":"Deep","ScanFilter":{{"data":{{"ComparisonOperator":"EQ","AttributeValueList":[{v}]}}}}}}"#
        ),
    )
    .await;
    assert_eq!(status, 200, "{body}");
}

/// Send a raw body, returning `None` when the server doesn't answer at all.
async fn try_send(url: &str, operation: &str, body: String) -> Option<(u16, Value)> {
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
        .ok()?;
    let status = resp.status().as_u16();
    let text = resp.text().await.ok()?;
    Some((status, serde_json::from_str(&text).unwrap_or(Value::Null)))
}

/// Members of an attribute value other than `M` and `L` hold no attribute
/// values, whatever they are called, so `{"S":{"Item":{...}}}` deep inside a
/// value is part of that value. The request gets the nesting refusal and the
/// server answers the next one.
#[tokio::test]
async fn request_shaped_members_inside_a_deep_value_are_part_of_it() {
    let url = start().await;
    create_table(&url, "Deep").await;
    put_base_item(&url).await;

    let inner = format!(r#"{{"Item":{{"y":{}}}}}"#, deep_map(40));
    let in_maps = format!(
        "{}{{\"S\":{inner}}}{}",
        "{\"M\":{\"n\":".repeat(70),
        "}}".repeat(70)
    );
    let in_lists = format!(
        "{}{{\"S\":{inner}}}{}",
        "{\"L\":[".repeat(70),
        "]}".repeat(70)
    );
    for value in [in_maps, in_lists] {
        let body = format!(r#"{{"TableName":"Deep","Item":{{"pk":{{"S":"p"}},"data":{value}}}}}"#);
        let (status, answer) = try_send(&url, "PutItem", body)
            .await
            .expect("the server answers the request");
        assert_validation(status, &answer, NEST);
        assert!(
            item_exists(&url, "base").await,
            "the server answers the next request"
        );
    }
}

/// A PartiQL parameter at 32 levels runs even when another statement in the
/// same batch nests deep enough that the body has to be walked: its condition
/// is evaluated, where the 63-level one is refused before it runs.
#[tokio::test]
async fn a_batch_statement_parameter_at_32_levels_runs_beside_a_63_level_one() {
    let url = start().await;
    create_table(&url, "Deep").await;
    put_base_item(&url).await;
    let (status, body) = send(
        &url,
        "BatchExecuteStatement",
        format!(
            r#"{{"Statements":[{},{}]}}"#,
            statement(
                r#"DELETE FROM \"Deep\" WHERE pk = 'base' AND data = ?"#,
                &deep_map(32)
            ),
            statement(
                r#"DELETE FROM \"Deep\" WHERE pk = 'other' AND data = ?"#,
                &deep_map(63)
            ),
        ),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let responses = body["Responses"].as_array().expect("Responses");
    // The first ran: its condition was evaluated, and failed.
    assert_eq!(
        responses[0]["Error"]["Code"], "ConditionalCheckFailed",
        "{body}"
    );
    assert_eq!(responses[1]["Error"]["Code"], "ValidationError", "{body}");
    assert_eq!(responses[1]["Error"]["Message"], NEST, "{body}");
    assert!(item_exists(&url, "base").await);
}

// #262, captured eu-west-2 and us-east-1 on 9 October 2026.
#[tokio::test]
async fn older_member_errors_win_over_deep_values() {
    let url = start().await;
    let rv = "1 validation error detected: Value 'BOGUS' at 'returnValues' failed to satisfy constraint: Member must satisfy enum value set: [ALL_NEW, UPDATED_OLD, ALL_OLD, NONE, UPDATED_NEW]";
    let xor = "1 validation error detected: Value 'XOR' at 'conditionalOperator' failed to satisfy constraint: Member must satisfy enum value set: [OR, AND]";
    // Shallower requests keep the newer wording, so the rewording must not
    // reach them: the generic ReturnValues set at 40 levels, [ALL, OR] at 33
    // and 40.
    let generic_rv = "1 validation error detected: Failed to satisfy constraint: Member must satisfy enum value set: [ALL_OLD, UPDATED_OLD, ALL_NEW, UPDATE_NEW, NONE]";
    let newer_xor = "1 validation error detected: Failed to satisfy constraint: Member must satisfy enum value set: [ALL, OR]";
    for depth in [33, 40, 63, 100] {
        let v = deep_map(depth);
        let (rv, xor) = if depth < 63 {
            (generic_rv, newer_xor)
        } else {
            (rv, xor)
        };
        // DeleteItem's shallow wording at 33 levels was captured on
        // 2026-11-03.
        let delete_members = [
            format!(r#""Key":{{"pk":{v}}}"#),
            format!(
                r#""Key":{{"pk":{{"S":"a"}}}},"ExpressionAttributeValues":{{":v":{v}}},"ConditionExpression":"pk = :v""#
            ),
            format!(r#""Key":{{"pk":{{"S":"a"}}}},"Expected":{{"data":{{"Value":{v}}}}}"#),
        ];
        for member in delete_members {
            let (status, body) = send(
                &url,
                "DeleteItem",
                format!(r#"{{"TableName":"Deep",{member},"ReturnValues":"BOGUS"}}"#),
            )
            .await;
            assert_validation(status, &body, rv);
        }
        for (operation, member) in [
            ("PutItem", r#""Item":{"pk":{"S":"a"}}"#),
            ("UpdateItem", r#""Key":{"pk":{"S":"a"}}"#),
        ] {
            let (status, body) = send(&url, operation, format!(r#"{{"TableName":"Deep",{member},"Expected":{{"data":{{"Value":{v}}},"other":{{"Value":{{"S":"a"}}}}}},"ConditionalOperator":"XOR"}}"#)).await;
            assert_validation(status, &body, xor);
        }
    }
    // Several members wrong at 100 levels: each reported, in the older
    // layer's order for the operation (captured eu-west-2 and us-east-1,
    // 2026-11-03 and 2026-11-04).
    let v = deep_map(100);
    let rv = "Value 'BOGUS' at 'returnValues' failed to satisfy constraint: Member must satisfy enum value set: [ALL_NEW, UPDATED_OLD, ALL_OLD, NONE, UPDATED_NEW]";
    let xor = "Value 'XOR' at 'conditionalOperator' failed to satisfy constraint: Member must satisfy enum value set: [OR, AND]";
    let rcc = "Value 'BOGUS' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]";
    let ricm = "Value 'BOGUS' at 'returnItemCollectionMetrics' failed to satisfy constraint: Member must satisfy enum value set: [SIZE, NONE]";
    let put = r#""Item":{"pk":{"S":"a"}}"#;
    let key = r#""Key":{"pk":{"S":"a"}}"#;
    let xor_rv = r#""ConditionalOperator":"XOR","ReturnValues":"BOGUS""#;
    for (operation, member, extra, expected) in [
        ("PutItem", put, xor_rv, vec![rv, xor]),
        ("UpdateItem", key, xor_rv, vec![rv, xor]),
        ("DeleteItem", key, xor_rv, vec![rv, xor]),
        (
            "PutItem",
            put,
            r#""ConditionalOperator":"XOR","ReturnValues":"BOGUS","ReturnConsumedCapacity":"BOGUS""#,
            vec![rv, xor, rcc],
        ),
        (
            "UpdateItem",
            key,
            r#""ConditionalOperator":"XOR","ReturnValues":"BOGUS","ReturnConsumedCapacity":"BOGUS""#,
            vec![rcc, rv, xor],
        ),
        (
            "PutItem",
            put,
            r#""ConditionalOperator":"XOR","ReturnValues":"BOGUS","ReturnItemCollectionMetrics":"BOGUS""#,
            vec![rv, xor, ricm],
        ),
        (
            "PutItem",
            put,
            r#""ReturnValues":"BOGUS","ReturnConsumedCapacity":"BOGUS""#,
            vec![rv, rcc],
        ),
        (
            "DeleteItem",
            key,
            r#""ConditionalOperator":"XOR","ReturnValues":"BOGUS","ReturnConsumedCapacity":"BOGUS""#,
            vec![rv, xor, rcc],
        ),
        (
            "DeleteItem",
            key,
            r#""ConditionalOperator":"XOR","ReturnValues":"BOGUS","ReturnItemCollectionMetrics":"BOGUS""#,
            vec![rv, xor, ricm],
        ),
        (
            "UpdateItem",
            key,
            r#""ConditionalOperator":"XOR","ReturnValues":"BOGUS","ReturnItemCollectionMetrics":"BOGUS""#,
            vec![rv, xor, ricm],
        ),
        (
            "PutItem",
            put,
            r#""ConditionalOperator":"XOR","ReturnValues":"NONE","ReturnConsumedCapacity":"BOGUS""#,
            vec![xor, rcc],
        ),
        (
            "DeleteItem",
            key,
            r#""ConditionalOperator":"XOR","ReturnValues":"ALL_NEW""#,
            vec![xor],
        ),
        (
            "UpdateItem",
            key,
            r#""ConditionalOperator":"XOR","ReturnValues":"BOGUS","ReturnConsumedCapacity":"BOGUS","ReturnItemCollectionMetrics":"BOGUS""#,
            vec![rcc, rv, xor, ricm],
        ),
        (
            "DeleteItem",
            key,
            r#""ConditionalOperator":"XOR","ReturnValues":"BOGUS","ReturnConsumedCapacity":"BOGUS","ReturnItemCollectionMetrics":"BOGUS""#,
            vec![rv, xor, rcc, ricm],
        ),
    ] {
        let (status, body) = send(&url, operation, format!(r#"{{"TableName":"Deep",{member},"Expected":{{"data":{{"Value":{v}}},"other":{{"Value":{{"S":"a"}}}}}},{extra}}}"#)).await;
        let message = match expected.as_slice() {
            [single] => format!("1 validation error detected: {single}"),
            _ => format!(
                "{} validation errors detected: {}",
                expected.len(),
                expected.join("; ")
            ),
        };
        assert_validation(status, &body, &message);
    }
    // Every member wrong, with and without the key or item, and a member the
    // older layer names beside the ones above (captured eu-west-2 and
    // us-east-1, 2026-11-04).
    let rvoccf = "Value 'BOGUS' at 'returnValuesOnConditionCheckFailure' failed to satisfy constraint: Member must satisfy enum value set: [ALL_OLD, NONE]";
    let no_key = "Value null at 'key' failed to satisfy constraint: Member must not be null";
    let no_item = "Value null at 'item' failed to satisfy constraint: Member must not be null";
    let all = r#""ConditionalOperator":"XOR","ReturnValues":"BOGUS","ReturnConsumedCapacity":"BOGUS","ReturnItemCollectionMetrics":"BOGUS","ReturnValuesOnConditionCheckFailure":"BOGUS""#;
    let expected_deep =
        format!(r#""Expected":{{"data":{{"Value":{v}}},"other":{{"Value":{{"S":"a"}}}}}}"#);
    for (operation, members, expected) in [
        (
            "UpdateItem",
            r#""ConditionalOperator":"XOR","ReturnValues":"BOGUS""#.to_string(),
            vec![rv, xor, no_key],
        ),
        (
            "DeleteItem",
            format!(
                r#"{key},"ConditionalOperator":"XOR","ReturnValues":"BOGUS","ReturnValuesOnConditionCheckFailure":"BOGUS""#
            ),
            vec![rv, xor, rvoccf],
        ),
        (
            "UpdateItem",
            all.to_string(),
            vec![rcc, rv, xor, rvoccf, ricm, no_key],
        ),
        (
            "UpdateItem",
            format!("{key},{all}"),
            vec![rcc, rv, xor, rvoccf, ricm],
        ),
        (
            "PutItem",
            format!("{put},{all}"),
            vec![rv, xor, rvoccf, rcc, ricm],
        ),
        (
            "PutItem",
            all.to_string(),
            vec![rv, xor, no_item, rvoccf, rcc, ricm],
        ),
        (
            "DeleteItem",
            format!("{key},{all}"),
            vec![rv, xor, rvoccf, rcc, ricm],
        ),
        (
            "DeleteItem",
            all.to_string(),
            vec![rv, xor, rvoccf, rcc, ricm, no_key],
        ),
    ] {
        let (status, body) = send(
            &url,
            operation,
            format!(r#"{{"TableName":"Deep",{expected_deep},{members}}}"#),
        )
        .await;
        let message = format!(
            "{} validation errors detected: {}",
            expected.len(),
            expected.join("; ")
        );
        assert_validation(status, &body, &message);
    }
}

#[tokio::test]
async fn missing_update_key_joins_older_member_errors() {
    let url = start().await;
    for (body, expected) in [
        (
            r#"{"TableName":""}"#,
            "2 validation errors detected: Value '' at 'tableName' failed to satisfy constraint: Member must have length greater than or equal to 1; Value null at 'key' failed to satisfy constraint: Member must not be null",
        ),
        (
            r#"{"TableName":"","ReturnConsumedCapacity":"BOGUS","ReturnValues":"BOGUS"}"#,
            "4 validation errors detected: Value 'BOGUS' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]; Value '' at 'tableName' failed to satisfy constraint: Member must have length greater than or equal to 1; Value 'BOGUS' at 'returnValues' failed to satisfy constraint: Member must satisfy enum value set: [ALL_NEW, UPDATED_OLD, ALL_OLD, NONE, UPDATED_NEW]; Value null at 'key' failed to satisfy constraint: Member must not be null",
        ),
        (
            r#"{"TableName":"Deep","Expected":{"a":{"Exists":false},"b":{"Exists":false}},"ConditionalOperator":"XOR"}"#,
            "2 validation errors detected: Value 'XOR' at 'conditionalOperator' failed to satisfy constraint: Member must satisfy enum value set: [OR, AND]; Value null at 'key' failed to satisfy constraint: Member must not be null",
        ),
    ] {
        let (status, body) = send(&url, "UpdateItem", body.into()).await;
        assert_validation(status, &body, expected);
    }
    let (status, body) = send(&url, "Query", r#"{"TableName":"Deep","Select":"BOGUS","ConditionalOperator":"XOR","QueryFilter":{"a":{"ComparisonOperator":"NULL"},"b":{"ComparisonOperator":"NULL"}}}"#.into()).await;
    assert_validation(
        status,
        &body,
        "Failed to satisfy constraint: Member must satisfy enum value set: [ALL, OR]",
    );
}
