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

// ---------------------------------------------------------------------------
// {"NULL": false} enveloping on the HTTP surface
// ---------------------------------------------------------------------------

/// The enveloped rejection PutItem and UpdateItem return for {"NULL": false}
/// in any position (item body, Key, ExpressionAttributeValues), captured
/// against real DynamoDB (eu-west-2).
const NULL_FALSE_ENVELOPED: &str = "1 validation error detected: \
     One or more parameter values were invalid: \
     Null attribute value types must have the value of true";

/// The bare rejection every other operation returns for {"NULL": false}.
const NULL_FALSE_BARE: &str = "One or more parameter values were invalid: \
     Null attribute value types must have the value of true";

/// Create a hash-only table for the validation tests below.
async fn create_items_table(url: &str) {
    let resp = dynamo_request(
        url,
        "CreateTable",
        json!({
            "TableName": "Items",
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}]
        }),
    )
    .await;
    assert_eq!(resp.status(), 200);
}

/// Assert a 400 ValidationException with an exact message, and that no
/// internal marker or serde position suffix leaked into the body.
async fn assert_validation_error(resp: reqwest::Response, expected_message: &str) {
    assert_eq!(resp.status(), 400);
    let text = resp.text().await.unwrap();
    assert!(
        !text.contains("VALIDATION") && !text.contains(" at line "),
        "internal marker or serde position leaked: {text}"
    );
    let body: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert!(
        body["__type"]
            .as_str()
            .unwrap()
            .ends_with("ValidationException"),
        "unexpected __type: {}",
        body["__type"]
    );
    assert_eq!(body["message"].as_str().unwrap(), expected_message);
}

#[tokio::test]
async fn test_put_item_null_false_in_item_enveloped() {
    let (url, _handle) = start_test_server().await;
    create_items_table(&url).await;

    let resp = dynamo_request(
        &url,
        "PutItem",
        json!({
            "TableName": "Items",
            "Item": {"pk": {"S": "k1"}, "flag": {"NULL": false}}
        }),
    )
    .await;
    assert_validation_error(resp, NULL_FALSE_ENVELOPED).await;
}

#[tokio::test]
async fn test_put_item_null_false_in_eav_enveloped() {
    let (url, _handle) = start_test_server().await;
    create_items_table(&url).await;

    let resp = dynamo_request(
        &url,
        "PutItem",
        json!({
            "TableName": "Items",
            "Item": {"pk": {"S": "k1"}},
            "ConditionExpression": "flag = :n",
            "ExpressionAttributeValues": {":n": {"NULL": false}}
        }),
    )
    .await;
    assert_validation_error(resp, NULL_FALSE_ENVELOPED).await;
}

#[tokio::test]
async fn test_update_item_null_false_in_eav_enveloped() {
    let (url, _handle) = start_test_server().await;
    create_items_table(&url).await;

    let resp = dynamo_request(
        &url,
        "UpdateItem",
        json!({
            "TableName": "Items",
            "Key": {"pk": {"S": "k1"}},
            "UpdateExpression": "SET flag = :n",
            "ExpressionAttributeValues": {":n": {"NULL": false}}
        }),
    )
    .await;
    assert_validation_error(resp, NULL_FALSE_ENVELOPED).await;
}

#[tokio::test]
async fn test_update_item_null_false_in_key_enveloped() {
    let (url, _handle) = start_test_server().await;
    create_items_table(&url).await;

    let resp = dynamo_request(
        &url,
        "UpdateItem",
        json!({
            "TableName": "Items",
            "Key": {"pk": {"NULL": false}},
            "UpdateExpression": "SET flag = :n",
            "ExpressionAttributeValues": {":n": {"S": "v"}}
        }),
    )
    .await;
    assert_validation_error(resp, NULL_FALSE_ENVELOPED).await;
}

#[tokio::test]
async fn test_get_item_null_false_in_key_bare() {
    let (url, _handle) = start_test_server().await;
    create_items_table(&url).await;

    let resp = dynamo_request(
        &url,
        "GetItem",
        json!({
            "TableName": "Items",
            "Key": {"pk": {"NULL": false}}
        }),
    )
    .await;
    assert_validation_error(resp, NULL_FALSE_BARE).await;
}

#[tokio::test]
async fn test_put_item_empty_attribute_value_bare() {
    let (url, _handle) = start_test_server().await;
    create_items_table(&url).await;

    let resp = dynamo_request(
        &url,
        "PutItem",
        json!({
            "TableName": "Items",
            "Item": {"pk": {"S": "k1"}, "data": {}}
        }),
    )
    .await;
    assert_validation_error(
        resp,
        "Supplied AttributeValue is empty, must contain exactly one of the supported datatypes",
    )
    .await;
}

#[tokio::test]
async fn test_put_item_multi_datatype_attribute_value_bare() {
    let (url, _handle) = start_test_server().await;
    create_items_table(&url).await;

    let resp = dynamo_request(
        &url,
        "PutItem",
        json!({
            "TableName": "Items",
            "Item": {"pk": {"S": "k1"}, "data": {"S": "v", "N": "1"}}
        }),
    )
    .await;
    assert_validation_error(
        resp,
        "Supplied AttributeValue has more than one datatypes set, \
         must contain exactly one of the supported datatypes",
    )
    .await;
}

/// The enveloped rejection UpdateItem returns for a duplicate string set in
/// ExpressionAttributeValues.
const DUPLICATE_SET_IN_EAV_ENVELOPED: &str = "1 validation error detected: \
     ExpressionAttributeValues contains invalid value: \
     One or more parameter values were invalid: \
     Input collection [a, b, a] contains duplicates. for key :t";

/// The bare rejection UpdateItem returns for a SET on a key attribute,
/// captured against real DynamoDB (eu-west-2).
const CANNOT_UPDATE_KEY_BARE: &str = "One or more parameter values were invalid: \
     Cannot update attribute pk. This attribute is part of the key";

#[tokio::test]
async fn test_update_item_duplicate_set_in_eav_enveloped() {
    let (url, _handle) = start_test_server().await;
    create_items_table(&url).await;

    let resp = dynamo_request(
        &url,
        "UpdateItem",
        json!({
            "TableName": "Items",
            "Key": {"pk": {"S": "k1"}},
            "UpdateExpression": "SET tags = :t",
            "ExpressionAttributeValues": {":t": {"SS": ["a", "b", "a"]}}
        }),
    )
    .await;
    assert_validation_error(resp, DUPLICATE_SET_IN_EAV_ENVELOPED).await;
}

#[tokio::test]
async fn test_update_item_cannot_update_key_bare() {
    let (url, _handle) = start_test_server().await;
    create_items_table(&url).await;

    let resp = dynamo_request(
        &url,
        "UpdateItem",
        json!({
            "TableName": "Items",
            "Key": {"pk": {"S": "k1"}},
            "UpdateExpression": "SET pk = :v",
            "ExpressionAttributeValues": {":v": {"S": "k2"}}
        }),
    )
    .await;
    assert_validation_error(resp, CANNOT_UPDATE_KEY_BARE).await;
}

// ---------------------------------------------------------------------------
// PutItem/UpdateItem validation-error envelope matrix
//
// Real DynamoDB (eu-west-2) wraps request-validation errors in a
// "1 validation error detected: " prefix and leaves data-plane, structural,
// and limit errors bare. The split is per family, not per message prefix.
// These tests pin the complete family matrix on the HTTP surface. Where
// dynoxide's inner wording deliberately differs from AWS's, the current
// dynoxide wording is asserted; the envelope split is what matters here.
// ---------------------------------------------------------------------------

const ENVELOPE: &str = "1 validation error detected: ";

/// Create a composite-key table with a numeric-keyed GSI, for the index-key
/// validation tests below.
async fn create_indexed_table(url: &str) {
    let resp = dynamo_request(
        url,
        "CreateTable",
        json!({
            "TableName": "Indexed",
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"}
            ],
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
                {"AttributeName": "gpk", "AttributeType": "N"}
            ],
            "GlobalSecondaryIndexes": [{
                "IndexName": "gsi1",
                "KeySchema": [{"AttributeName": "gpk", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"}
            }],
            "BillingMode": "PAY_PER_REQUEST"
        }),
    )
    .await;
    assert_eq!(resp.status(), 200);
}

// ---- Enveloped families (request validation) ----

#[tokio::test]
async fn test_put_item_empty_string_set_enveloped() {
    let (url, _handle) = start_test_server().await;
    create_items_table(&url).await;

    let resp = dynamo_request(
        &url,
        "PutItem",
        json!({
            "TableName": "Items",
            "Item": {"pk": {"S": "k1"}, "ss": {"SS": []}}
        }),
    )
    .await;
    // The doubled space in "set  may" matches AWS verbatim.
    assert_validation_error(
        resp,
        "1 validation error detected: One or more parameter values were invalid: \
         An string set  may not be empty",
    )
    .await;
}

#[tokio::test]
async fn test_put_item_empty_number_set_enveloped() {
    let (url, _handle) = start_test_server().await;
    create_items_table(&url).await;

    let resp = dynamo_request(
        &url,
        "PutItem",
        json!({
            "TableName": "Items",
            "Item": {"pk": {"S": "k1"}, "ns": {"NS": []}}
        }),
    )
    .await;
    assert_validation_error(
        resp,
        "1 validation error detected: One or more parameter values were invalid: \
         An number set  may not be empty",
    )
    .await;
}

#[tokio::test]
async fn test_put_item_empty_binary_set_enveloped() {
    let (url, _handle) = start_test_server().await;
    create_items_table(&url).await;

    let resp = dynamo_request(
        &url,
        "PutItem",
        json!({
            "TableName": "Items",
            "Item": {"pk": {"S": "k1"}, "bs": {"BS": []}}
        }),
    )
    .await;
    assert_validation_error(
        resp,
        "1 validation error detected: One or more parameter values were invalid: \
         Binary sets should not be empty",
    )
    .await;
}

#[tokio::test]
async fn test_put_item_duplicate_string_set_in_item_enveloped() {
    let (url, _handle) = start_test_server().await;
    create_items_table(&url).await;

    let resp = dynamo_request(
        &url,
        "PutItem",
        json!({
            "TableName": "Items",
            "Item": {"pk": {"S": "k1"}, "tags": {"SS": ["x", "x"]}}
        }),
    )
    .await;
    assert_validation_error(
        resp,
        "1 validation error detected: One or more parameter values were invalid: \
         Input collection [x, x] contains duplicates.",
    )
    .await;
}

#[tokio::test]
async fn test_put_item_condition_expression_syntax_error_enveloped() {
    let (url, _handle) = start_test_server().await;
    create_items_table(&url).await;

    let resp = dynamo_request(
        &url,
        "PutItem",
        json!({
            "TableName": "Items",
            "Item": {"pk": {"S": "k1"}},
            "ConditionExpression": "foo =="
        }),
    )
    .await;
    assert_validation_error(
        resp,
        "1 validation error detected: Invalid ConditionExpression: Expected operand, got =",
    )
    .await;
}

#[tokio::test]
async fn test_put_item_condition_expression_oversize_enveloped() {
    let (url, _handle) = start_test_server().await;
    create_items_table(&url).await;

    let resp = dynamo_request(
        &url,
        "PutItem",
        json!({
            "TableName": "Items",
            "Item": {"pk": {"S": "k1"}},
            "ConditionExpression": "a".repeat(4200)
        }),
    )
    .await;
    assert_validation_error(
        resp,
        "1 validation error detected: Invalid ConditionExpression: \
         Expression size has exceeded the maximum allowed size; expression size: 4200",
    )
    .await;
}

#[tokio::test]
async fn test_update_item_update_expression_syntax_error_enveloped() {
    let (url, _handle) = start_test_server().await;
    create_items_table(&url).await;

    let resp = dynamo_request(
        &url,
        "UpdateItem",
        json!({
            "TableName": "Items",
            "Key": {"pk": {"S": "k1"}},
            "UpdateExpression": "SET"
        }),
    )
    .await;
    assert_validation_error(
        resp,
        "1 validation error detected: Invalid UpdateExpression: Syntax error; \
         Expected attribute name, got end of expression",
    )
    .await;
}

#[tokio::test]
async fn test_update_item_update_expression_oversize_enveloped() {
    let (url, _handle) = start_test_server().await;
    create_items_table(&url).await;

    let resp = dynamo_request(
        &url,
        "UpdateItem",
        json!({
            "TableName": "Items",
            "Key": {"pk": {"S": "k1"}},
            "UpdateExpression": "a".repeat(4200)
        }),
    )
    .await;
    assert_validation_error(
        resp,
        "1 validation error detected: Invalid UpdateExpression: \
         Expression size has exceeded the maximum allowed size; expression size: 4200",
    )
    .await;
}

#[tokio::test]
async fn test_put_item_return_values_all_new_enveloped() {
    let (url, _handle) = start_test_server().await;
    create_items_table(&url).await;

    let resp = dynamo_request(
        &url,
        "PutItem",
        json!({
            "TableName": "Items",
            "Item": {"pk": {"S": "k1"}},
            "ReturnValues": "ALL_NEW"
        }),
    )
    .await;
    assert_validation_error(
        resp,
        "1 validation error detected: ReturnValues can only be ALL_OLD or NONE",
    )
    .await;
}

#[tokio::test]
async fn test_update_item_return_values_invalid_enveloped() {
    let (url, _handle) = start_test_server().await;
    create_items_table(&url).await;

    let resp = dynamo_request(
        &url,
        "UpdateItem",
        json!({
            "TableName": "Items",
            "Key": {"pk": {"S": "k1"}},
            "UpdateExpression": "SET a = :v",
            "ExpressionAttributeValues": {":v": {"S": "v"}},
            "ReturnValues": "BOGUS"
        }),
    )
    .await;
    assert_validation_error(
        resp,
        "1 validation error detected: Value 'BOGUS' at 'returnValues' failed to satisfy \
         constraint: Member must satisfy enum value set: \
         [ALL_NEW, UPDATED_OLD, ALL_OLD, NONE, UPDATED_NEW]",
    )
    .await;
}

#[tokio::test]
async fn test_put_item_eav_without_expression_enveloped() {
    let (url, _handle) = start_test_server().await;
    create_items_table(&url).await;

    let resp = dynamo_request(
        &url,
        "PutItem",
        json!({
            "TableName": "Items",
            "Item": {"pk": {"S": "k1"}},
            "ExpressionAttributeValues": {":v": {"S": "v"}}
        }),
    )
    .await;
    assert_validation_error(
        resp,
        "1 validation error detected: ExpressionAttributeValues can only be specified \
         when using expressions: ConditionExpression is null",
    )
    .await;
}

#[tokio::test]
async fn test_put_item_redundant_parentheses_enveloped() {
    let (url, _handle) = start_test_server().await;
    create_items_table(&url).await;

    let resp = dynamo_request(
        &url,
        "PutItem",
        json!({
            "TableName": "Items",
            "Item": {"pk": {"S": "k1"}},
            "ConditionExpression": "((attribute_exists(a)))"
        }),
    )
    .await;
    assert_validation_error(
        resp,
        "1 validation error detected: Invalid ConditionExpression: \
         The expression has redundant parentheses;",
    )
    .await;
}

#[tokio::test]
async fn test_put_item_contains_distinct_operand_enveloped() {
    let (url, _handle) = start_test_server().await;
    create_items_table(&url).await;

    let resp = dynamo_request(
        &url,
        "PutItem",
        json!({
            "TableName": "Items",
            "Item": {"pk": {"S": "k1"}},
            "ConditionExpression": "contains(a, a)"
        }),
    )
    .await;
    assert_validation_error(
        resp,
        "1 validation error detected: Invalid ConditionExpression: The first operand must \
         be distinct from the remaining operands for this operator or function; \
         operator: contains, first operand: [a]",
    )
    .await;
}

#[tokio::test]
async fn test_put_item_expected_and_condition_expression_enveloped() {
    let (url, _handle) = start_test_server().await;
    create_items_table(&url).await;

    let resp = dynamo_request(
        &url,
        "PutItem",
        json!({
            "TableName": "Items",
            "Item": {"pk": {"S": "k1"}},
            "ConditionExpression": "attribute_exists(a)",
            "Expected": {"a": {"Exists": false}}
        }),
    )
    .await;
    assert_validation_error(
        resp,
        "1 validation error detected: Can not use both expression and non-expression \
         parameters in the same request: Non-expression parameters: {Expected} \
         Expression parameters: {ConditionExpression}",
    )
    .await;
}

// ---- Bare families (data-plane, structural, limit) ----

#[tokio::test]
async fn test_put_item_key_type_mismatch_bare() {
    let (url, _handle) = start_test_server().await;
    create_items_table(&url).await;

    let resp = dynamo_request(
        &url,
        "PutItem",
        json!({
            "TableName": "Items",
            "Item": {"pk": {"N": "1"}}
        }),
    )
    .await;
    assert_validation_error(
        resp,
        "One or more parameter values were invalid: \
         Type mismatch for key pk expected: S actual: N",
    )
    .await;
}

#[tokio::test]
async fn test_put_item_index_key_type_mismatch_bare() {
    let (url, _handle) = start_test_server().await;
    create_indexed_table(&url).await;

    let resp = dynamo_request(
        &url,
        "PutItem",
        json!({
            "TableName": "Indexed",
            "Item": {"pk": {"S": "p1"}, "sk": {"S": "s1"}, "gpk": {"S": "notnum"}}
        }),
    )
    .await;
    assert_validation_error(
        resp,
        "One or more parameter values were invalid: \
         Type mismatch for Index Key gpk Expected: N Actual: S IndexName: gsi1",
    )
    .await;
}

#[tokio::test]
async fn test_update_item_document_path_invalid_bare() {
    let (url, _handle) = start_test_server().await;
    create_items_table(&url).await;

    // Seed an item whose m.a is a scalar, so a deeper path is invalid.
    let resp = dynamo_request(
        &url,
        "PutItem",
        json!({
            "TableName": "Items",
            "Item": {"pk": {"S": "k1"}, "m": {"M": {"a": {"S": "v"}}}}
        }),
    )
    .await;
    assert_eq!(resp.status(), 200);

    let resp = dynamo_request(
        &url,
        "UpdateItem",
        json!({
            "TableName": "Items",
            "Key": {"pk": {"S": "k1"}},
            "UpdateExpression": "SET m.a.b.c = :v",
            "ExpressionAttributeValues": {":v": {"S": "v"}}
        }),
    )
    .await;
    assert_validation_error(
        resp,
        "The document path provided in the update expression is invalid for update",
    )
    .await;
}

#[tokio::test]
async fn test_update_item_nonexistent_attribute_reference_bare() {
    let (url, _handle) = start_test_server().await;
    create_items_table(&url).await;

    let resp = dynamo_request(
        &url,
        "PutItem",
        json!({
            "TableName": "Items",
            "Item": {"pk": {"S": "k1"}}
        }),
    )
    .await;
    assert_eq!(resp.status(), 200);

    let resp = dynamo_request(
        &url,
        "UpdateItem",
        json!({
            "TableName": "Items",
            "Key": {"pk": {"S": "k1"}},
            "UpdateExpression": "SET a = nosuch"
        }),
    )
    .await;
    assert_validation_error(
        resp,
        "The provided expression refers to an attribute that does not exist in the item",
    )
    .await;
}

#[tokio::test]
async fn test_put_item_empty_string_key_bare() {
    let (url, _handle) = start_test_server().await;
    create_items_table(&url).await;

    let resp = dynamo_request(
        &url,
        "PutItem",
        json!({
            "TableName": "Items",
            "Item": {"pk": {"S": ""}}
        }),
    )
    .await;
    assert_validation_error(
        resp,
        "One or more parameter values are not valid. The AttributeValue for a key \
         attribute cannot contain an empty string value. Key: pk",
    )
    .await;
}

#[tokio::test]
async fn test_put_item_size_exceeded_bare() {
    let (url, _handle) = start_test_server().await;
    create_items_table(&url).await;

    let resp = dynamo_request(
        &url,
        "PutItem",
        json!({
            "TableName": "Items",
            "Item": {"pk": {"S": "k1"}, "big": {"S": "a".repeat(410_000)}}
        }),
    )
    .await;
    assert_validation_error(resp, "Item size has exceeded the maximum allowed size").await;
}

// ---- Envelope guards ----

#[tokio::test]
async fn test_put_item_duplicate_set_matching_bare_wording_still_enveloped() {
    let (url, _handle) = start_test_server().await;
    create_items_table(&url).await;

    // The duplicated value deliberately mirrors the wording of a bare-family
    // message; classification must key on the error family, not the text.
    let resp = dynamo_request(
        &url,
        "PutItem",
        json!({
            "TableName": "Items",
            "Item": {"pk": {"S": "k1"}, "tags": {"SS": [
                "This attribute is part of the key",
                "This attribute is part of the key"
            ]}}
        }),
    )
    .await;
    assert_validation_error(
        resp,
        "1 validation error detected: One or more parameter values were invalid: \
         Input collection [This attribute is part of the key, \
         This attribute is part of the key] contains duplicates.",
    )
    .await;
}

#[tokio::test]
async fn test_put_item_constraint_error_carries_single_envelope() {
    let (url, _handle) = start_test_server().await;

    // A table name over 255 characters fails a deserialiser constraint that
    // is already enveloped; dispatch must not wrap it a second time.
    let long_name = "x".repeat(300);
    let resp = dynamo_request(
        &url,
        "PutItem",
        json!({
            "TableName": long_name,
            "Item": {"pk": {"S": "k1"}}
        }),
    )
    .await;
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    let message = body["message"].as_str().unwrap();
    let Some(rest) = message.strip_prefix(ENVELOPE) else {
        panic!("expected an envelope, got: {message}");
    };
    assert!(
        !rest.contains(ENVELOPE),
        "envelope applied twice: {message}"
    );
    assert_eq!(
        rest,
        format!(
            "Value '{long_name}' at 'tableName' failed to satisfy constraint: \
             Member must have length less than or equal to 255"
        )
    );
}

#[tokio::test]
async fn test_get_item_projection_expression_oversize_bare() {
    let (url, _handle) = start_test_server().await;
    create_items_table(&url).await;

    let resp = dynamo_request(
        &url,
        "GetItem",
        json!({
            "TableName": "Items",
            "Key": {"pk": {"S": "k1"}},
            "ProjectionExpression": "a".repeat(4200)
        }),
    )
    .await;
    assert_validation_error(
        resp,
        "Invalid ProjectionExpression: \
         Expression size has exceeded the maximum allowed size; expression size: 4200",
    )
    .await;
}

#[tokio::test]
async fn test_query_key_condition_expression_oversize_bare() {
    let (url, _handle) = start_test_server().await;
    create_items_table(&url).await;

    let resp = dynamo_request(
        &url,
        "Query",
        json!({
            "TableName": "Items",
            "KeyConditionExpression": "a".repeat(4200)
        }),
    )
    .await;
    assert_validation_error(
        resp,
        "Invalid KeyConditionExpression: \
         Expression size has exceeded the maximum allowed size; expression size: 4200",
    )
    .await;
}

#[tokio::test]
async fn test_query_filter_expression_oversize_bare() {
    let (url, _handle) = start_test_server().await;
    create_items_table(&url).await;

    let resp = dynamo_request(
        &url,
        "Query",
        json!({
            "TableName": "Items",
            "KeyConditionExpression": "pk = :p",
            "FilterExpression": "a".repeat(4200),
            "ExpressionAttributeValues": {":p": {"S": "k1"}}
        }),
    )
    .await;
    assert_validation_error(
        resp,
        "Invalid FilterExpression: \
         Expression size has exceeded the maximum allowed size; expression size: 4200",
    )
    .await;
}

#[tokio::test]
async fn test_scan_filter_expression_oversize_bare() {
    let (url, _handle) = start_test_server().await;
    create_items_table(&url).await;

    let resp = dynamo_request(
        &url,
        "Scan",
        json!({
            "TableName": "Items",
            "FilterExpression": "a".repeat(4200)
        }),
    )
    .await;
    assert_validation_error(
        resp,
        "Invalid FilterExpression: \
         Expression size has exceeded the maximum allowed size; expression size: 4200",
    )
    .await;
}
