//! HTTP server integration tests.
//! These tests require the `http-server` feature.

#![cfg(feature = "http-server")]

use dynoxide::Database;
use serde_json::json;

/// Start a test server on a random port, returning the base URL.
async fn start_test_server() -> (String, tokio::task::JoinHandle<()>) {
    let db = Database::memory().unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let url = format!("http://{addr}");

    let handle = tokio::spawn(async move {
        dynoxide::server::serve_on(listener, db).await;
    });

    // Give the server a moment to start
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    (url, handle)
}

fn client() -> reqwest::Client {
    reqwest::Client::new()
}

async fn dynamo_request(
    base_url: &str,
    operation: &str,
    body: serde_json::Value,
) -> reqwest::Response {
    client()
        .post(base_url)
        .header("x-amz-target", format!("DynamoDB_20120810.{operation}"))
        .header("content-type", "application/x-amz-json-1.0")
        .header(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=fakekey/20260101/us-east-1/dynamodb/aws4_request, SignedHeaders=host;x-amz-date;x-amz-target, Signature=fakesig",
        )
        .header("x-amz-date", "20260101T000000Z")
        .json(&body)
        .send()
        .await
        .unwrap()
}

#[tokio::test]
async fn test_create_table_via_http() {
    let (url, _handle) = start_test_server().await;

    let resp = dynamo_request(
        &url,
        "CreateTable",
        json!({
            "TableName": "TestTable",
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}]
        }),
    )
    .await;

    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["TableDescription"]["TableName"], "TestTable");
    // CreateTable may return CREATING or ACTIVE depending on configuration
    let status = body["TableDescription"]["TableStatus"].as_str().unwrap();
    assert!(
        status == "ACTIVE" || status == "CREATING",
        "Expected ACTIVE or CREATING, got {status}"
    );
    assert_eq!(
        body["TableDescription"]["TableArn"],
        "arn:aws:dynamodb:dynoxide:000000000000:table/TestTable"
    );
}

#[tokio::test]
async fn test_put_get_roundtrip() {
    let (url, _handle) = start_test_server().await;

    // Create table
    dynamo_request(
        &url,
        "CreateTable",
        json!({
            "TableName": "Users",
            "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}]
        }),
    )
    .await;

    // PutItem
    let resp = dynamo_request(
        &url,
        "PutItem",
        json!({
            "TableName": "Users",
            "Item": {
                "id": {"S": "user1"},
                "name": {"S": "Alice"},
                "age": {"N": "30"}
            }
        }),
    )
    .await;
    assert_eq!(resp.status(), 200);

    // GetItem
    let resp = dynamo_request(
        &url,
        "GetItem",
        json!({
            "TableName": "Users",
            "Key": {"id": {"S": "user1"}}
        }),
    )
    .await;
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["Item"]["id"]["S"], "user1");
    assert_eq!(body["Item"]["name"]["S"], "Alice");
    assert_eq!(body["Item"]["age"]["N"], "30");
}

#[tokio::test]
async fn test_error_response_format() {
    let (url, _handle) = start_test_server().await;

    // Get from nonexistent table
    let resp = dynamo_request(
        &url,
        "GetItem",
        json!({
            "TableName": "NonExistent",
            "Key": {"pk": {"S": "val"}}
        }),
    )
    .await;

    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        body["__type"]
            .as_str()
            .unwrap()
            .contains("ResourceNotFoundException")
    );
    assert!(
        body["message"]
            .as_str()
            .unwrap()
            .contains("Requested resource not found")
    );
}

#[tokio::test]
async fn test_unknown_operation() {
    let (url, _handle) = start_test_server().await;

    let resp = dynamo_request(&url, "FakeOperation", json!({})).await;

    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    // DynamoDB returns UnknownOperationException with just __type, no message
    assert!(
        body["__type"]
            .as_str()
            .unwrap()
            .contains("UnknownOperationException")
    );
}

#[tokio::test]
async fn test_missing_target_header() {
    let (url, _handle) = start_test_server().await;

    let resp = client()
        .post(&url)
        .header("content-type", "application/x-amz-json-1.0")
        .body("{}")
        .send()
        .await
        .unwrap();

    // DynamoDB returns UnknownOperationException when no target header is present
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        body["__type"]
            .as_str()
            .unwrap()
            .contains("UnknownOperationException")
    );
}

#[tokio::test]
async fn test_malformed_json() {
    let (url, _handle) = start_test_server().await;

    let resp = client()
        .post(&url)
        .header("x-amz-target", "DynamoDB_20120810.CreateTable")
        .header("content-type", "application/x-amz-json-1.0")
        .body("not valid json{{{")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        body["__type"].as_str().unwrap(),
        "com.amazon.coral.service#SerializationException"
    );
}

#[tokio::test]
async fn test_content_type_header() {
    let (url, _handle) = start_test_server().await;

    let resp = dynamo_request(&url, "ListTables", json!({})).await;

    assert_eq!(resp.status(), 200);
    let ct = resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap();
    assert_eq!(ct, "application/x-amz-json-1.0");
}

#[tokio::test]
async fn test_cors_preflight() {
    let (url, _handle) = start_test_server().await;

    let resp = client()
        .request(reqwest::Method::OPTIONS, &url)
        .header("origin", "http://localhost:3000")
        .header("access-control-request-method", "POST")
        .header(
            "access-control-request-headers",
            "x-amz-target,content-type",
        )
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    assert!(resp.headers().contains_key("access-control-allow-origin"));
}

#[tokio::test]
async fn test_server_version_headers_on_success() {
    let (url, _handle) = start_test_server().await;

    let resp = dynamo_request(&url, "ListTables", json!({})).await;

    assert_eq!(resp.status(), 200);
    let server = resp.headers().get("server").unwrap().to_str().unwrap();
    assert!(
        server.starts_with("Dynoxide/"),
        "Server header should start with 'Dynoxide/', got: {server}"
    );
    assert!(resp.headers().contains_key("x-dynoxide-version"));
}

#[tokio::test]
async fn test_server_version_headers_on_error() {
    let (url, _handle) = start_test_server().await;

    let resp = dynamo_request(
        &url,
        "GetItem",
        json!({
            "TableName": "NonExistent",
            "Key": {"pk": {"S": "val"}}
        }),
    )
    .await;

    assert_eq!(resp.status(), 400);
    let server = resp.headers().get("server").unwrap().to_str().unwrap();
    assert!(server.starts_with("Dynoxide/"));
    assert!(resp.headers().contains_key("x-dynoxide-version"));
}

#[tokio::test]
async fn test_server_version_headers_on_preflight() {
    let (url, _handle) = start_test_server().await;

    let resp = client()
        .request(reqwest::Method::OPTIONS, &url)
        .header("origin", "http://localhost:3000")
        .header("access-control-request-method", "POST")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let server = resp.headers().get("server").unwrap().to_str().unwrap();
    assert!(server.starts_with("Dynoxide/"));
    assert!(resp.headers().contains_key("x-dynoxide-version"));
}

#[tokio::test]
async fn test_version_header_values_match() {
    let (url, _handle) = start_test_server().await;

    let resp = dynamo_request(&url, "ListTables", json!({})).await;

    let server = resp.headers().get("server").unwrap().to_str().unwrap();
    let version = resp
        .headers()
        .get("x-dynoxide-version")
        .unwrap()
        .to_str()
        .unwrap();

    // Server header format: Dynoxide/{version}
    let server_version = server.strip_prefix("Dynoxide/").unwrap();
    assert_eq!(server_version, version);
    // Sanity check: version looks like a semver
    assert!(
        version.contains('.'),
        "Version should be semver-like, got: {version}"
    );
}

#[tokio::test]
async fn test_query_via_http() {
    let (url, _handle) = start_test_server().await;

    // Create table
    dynamo_request(
        &url,
        "CreateTable",
        json!({
            "TableName": "Events",
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"}
            ],
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"}
            ]
        }),
    )
    .await;

    // Put items
    for i in 1..=3 {
        dynamo_request(
            &url,
            "PutItem",
            json!({
                "TableName": "Events",
                "Item": {
                    "pk": {"S": "user1"},
                    "sk": {"S": format!("event{i}")},
                    "data": {"S": format!("data{i}")}
                }
            }),
        )
        .await;
    }

    // Query
    let resp = dynamo_request(
        &url,
        "Query",
        json!({
            "TableName": "Events",
            "KeyConditionExpression": "pk = :pk",
            "ExpressionAttributeValues": {":pk": {"S": "user1"}}
        }),
    )
    .await;
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["Count"], 3);
}
