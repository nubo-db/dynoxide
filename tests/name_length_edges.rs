//! Name checks next to the captured length units, which no captured row
//! decides: an empty index name, a table name on a request built in code, and
//! a bad `IndexName` beside another member fault.

#![cfg(feature = "http-server")]

use dynoxide::actions::delete_table::DeleteTableRequest;
use dynoxide::actions::describe_table::DescribeTableRequest;
use dynoxide::actions::describe_time_to_live::DescribeTimeToLiveRequest;
use dynoxide::actions::update_table::UpdateTableRequest;
use dynoxide::actions::update_time_to_live::UpdateTimeToLiveRequest;
use dynoxide::{Database, DynoxideError};
use serde_json::json;

async fn start_server() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let db = Database::memory().unwrap();
    db.create_table(
        serde_json::from_value(json!({
            "TableName": "Existing",
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "BillingMode": "PAY_PER_REQUEST"
        }))
        .unwrap(),
    )
    .unwrap();
    tokio::spawn(async move {
        dynoxide::server::serve_on(listener, db).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    url
}

/// The HTTP status, the error type after its `#` and the message of the
/// answer to a request.
async fn answer(url: &str, operation: &str, body: serde_json::Value) -> (u16, String, String) {
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
    let body: serde_json::Value = response.json().await.unwrap();
    let kind = body["__type"].as_str().unwrap_or_default();
    (
        status,
        kind.rsplit('#').next().unwrap().to_string(),
        body["message"].as_str().unwrap_or_default().to_string(),
    )
}

/// The error type of a refused request's validation.
const VALIDATION: &str = "ValidationException";

fn short_index_name(path: &str) -> String {
    format!(
        "1 validation error detected: Value '' at '{path}' failed to satisfy constraint: \
         Member must have length greater than or equal to 3"
    )
}

/// An empty index name gets the length fault alone, on every index position
/// of CreateTable and UpdateTable, as CreateTable's and UpdateTable's vector
/// create always gave it. DynamoDB's answer for one is uncaptured.
#[tokio::test]
async fn an_empty_index_name_gets_the_length_fault_alone() {
    let url = start_server().await;
    let key = json!([{"AttributeName": "g", "KeyType": "HASH"}]);
    let definitions = json!([
        {"AttributeName": "pk", "AttributeType": "S"},
        {"AttributeName": "g", "AttributeType": "S"}
    ]);
    let vector = json!({
        "IndexName": "",
        "VectorAttribute": {"AttributeName": "embedding"},
        "Dimensions": 3,
        "DistanceFunction": "COSINE",
        "Projection": {"ProjectionType": "ALL"}
    });
    let cases = [
        (
            "CreateTable",
            json!({
                "TableName": "Fresh",
                "AttributeDefinitions": definitions,
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "BillingMode": "PAY_PER_REQUEST",
                "GlobalSecondaryIndexes": [{"IndexName": "", "KeySchema": key,
                    "Projection": {"ProjectionType": "ALL"}}]
            }),
            "globalSecondaryIndexes.1.member.indexName",
        ),
        (
            "CreateTable",
            json!({
                "TableName": "Fresh",
                "AttributeDefinitions": definitions,
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "g", "KeyType": "RANGE"}],
                "BillingMode": "PAY_PER_REQUEST",
                "LocalSecondaryIndexes": [{"IndexName": "", "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "g", "KeyType": "RANGE"}],
                    "Projection": {"ProjectionType": "ALL"}}]
            }),
            "localSecondaryIndexes.1.member.indexName",
        ),
        (
            "CreateTable",
            json!({
                "TableName": "Fresh",
                "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "BillingMode": "PAY_PER_REQUEST",
                "VectorIndexes": [vector]
            }),
            "vectorIndexes.1.member.indexName",
        ),
        (
            "UpdateTable",
            json!({"TableName": "Existing", "VectorIndexUpdates": [{"Create": vector}]}),
            "vectorIndexUpdates.1.member.create.indexName",
        ),
        (
            "UpdateTable",
            json!({
                "TableName": "Existing",
                "AttributeDefinitions": definitions,
                "GlobalSecondaryIndexUpdates": [{"Create": {"IndexName": "", "KeySchema": key,
                    "Projection": {"ProjectionType": "ALL"}}}]
            }),
            "globalSecondaryIndexUpdates.1.member.create.indexName",
        ),
        (
            "UpdateTable",
            json!({
                "TableName": "Existing",
                "GlobalSecondaryIndexUpdates": [{"Delete": {"IndexName": ""}}]
            }),
            "globalSecondaryIndexUpdates.1.member.delete.indexName",
        ),
    ];
    let mut wrong = Vec::new();
    for (operation, body, path) in cases {
        let got = answer(&url, operation, body).await;
        if got != (400, VALIDATION.to_string(), short_index_name(path)) {
            wrong.push(format!("{path}: {got:?}"));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

fn update_table(name: &str) -> UpdateTableRequest {
    serde_json::from_value(json!({"TableName": "placeholder", "BillingMode": "PAY_PER_REQUEST"}))
        .map(|request: UpdateTableRequest| UpdateTableRequest {
            table_name: name.to_string(),
            ..request
        })
        .unwrap()
}

/// The message of a ValidationException, failing on any other error.
fn validation<T: std::fmt::Debug>(result: dynoxide::Result<T>) -> String {
    match result.unwrap_err() {
        DynoxideError::ValidationException(message) => message,
        other => panic!("not a ValidationException: {other:?}"),
    }
}

fn too_short(name: &str) -> String {
    format!(
        "1 validation error detected: Value '{name}' at 'tableName' \
         failed to satisfy constraint: Member must have length greater than or equal to 3"
    )
}

fn too_long(name: &str) -> String {
    format!(
        "1 validation error detected: Value '{name}' at 'tableName' \
         failed to satisfy constraint: Member must have length less than or equal to 255"
    )
}

/// UpdateTable built in code checks its table name as a body's is checked,
/// before anything else about the request.
#[test]
fn update_table_built_in_code_checks_the_table_name() {
    let db = Database::memory().unwrap();
    let err = validation(db.update_table(update_table("ab")));
    assert_eq!(err, too_short("ab"));
    let long = "a".repeat(256);
    let err = validation(db.update_table(update_table(&long)));
    assert_eq!(err, too_long(&long));
    // The table name comes ahead of the request's other checks: a BillingMode
    // outside its enum is not reached.
    let request = UpdateTableRequest {
        billing_mode: Some("BOGUS".to_string()),
        ..update_table("ab")
    };
    assert_eq!(validation(db.update_table(request)), too_short("ab"));
}

/// The other table operations built in code refuse a two-character name as a
/// ValidationException too.
#[test]
fn table_operations_built_in_code_refuse_a_short_table_name() {
    let db = Database::memory().unwrap();
    let name = || "ab".to_string();
    let describe = db.describe_table(DescribeTableRequest { table_name: name() });
    assert_eq!(validation(describe), too_short("ab"));
    let delete = db.delete_table(DeleteTableRequest { table_name: name() });
    assert_eq!(validation(delete), too_short("ab"));
    let describe_ttl = db.describe_time_to_live(DescribeTimeToLiveRequest { table_name: name() });
    assert_eq!(validation(describe_ttl), too_short("ab"));
    let update_ttl: UpdateTimeToLiveRequest = serde_json::from_value(json!({
        "TableName": "ab",
        "TimeToLiveSpecification": {"Enabled": true, "AttributeName": "ttl"}
    }))
    .unwrap();
    assert_eq!(
        validation(db.update_time_to_live(update_ttl)),
        too_short("ab")
    );
}

/// A bad `IndexName` on Query and Scan is reported with the table name's
/// faults, ahead of a `Select` outside its enum, which waits for a later
/// stage. Pinned as Dynoxide answers today; the order is uncaptured.
#[tokio::test]
async fn a_bad_index_name_comes_ahead_of_a_bad_select() {
    let url = start_server().await;
    let short = "1 validation error detected: Value at 'IndexName' failed to satisfy \
                 constraint: Member must have length greater than or equal to 3"
        .to_string();
    let query = json!({
        "TableName": "Existing",
        "IndexName": "ab",
        "Select": "BOGUS",
        "KeyConditionExpression": "pk = :a",
        "ExpressionAttributeValues": {":a": {"S": "a"}}
    });
    assert_eq!(
        answer(&url, "Query", query).await,
        (400, VALIDATION.to_string(), short.clone())
    );
    let scan = json!({"TableName": "Existing", "IndexName": "ab", "Select": "BOGUS"});
    assert_eq!(
        answer(&url, "Scan", scan).await,
        (400, VALIDATION.to_string(), short)
    );
}
