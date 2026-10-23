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
    assert_version_headers(&resp);
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
    assert_version_headers(&resp);
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
    assert_version_headers(&resp);
}

/// Both version headers carry the product version, exactly.
///
/// Asserting the value rather than the shape is the point: these headers used
/// to report the crate version, and two identically wrong values passed the
/// old "they agree and contain a dot" check.
fn assert_version_headers(resp: &reqwest::Response) {
    let expected = dynoxide::PRODUCT_VERSION;
    let server = resp.headers().get("server").unwrap().to_str().unwrap();
    assert_eq!(
        server,
        format!("Dynoxide/{expected}"),
        "Server header should carry the product version"
    );
    let version = resp
        .headers()
        .get("x-dynoxide-version")
        .unwrap()
        .to_str()
        .unwrap();
    assert_eq!(
        version, expected,
        "x-dynoxide-version should carry the product version"
    );
}

#[tokio::test]
async fn test_product_version_is_the_checked_in_version_file() {
    // Catches build.rs drifting from the file, and catches anyone rewiring
    // the product version back to the crate version.
    let file = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/VERSION")).unwrap();
    assert_eq!(dynoxide::PRODUCT_VERSION, file.trim());
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
// UpdateItem refuses it in the current layer's words, with no
// ExpressionAttributeValues wrapper (eu-west-2, us-east-1, eu-west-1 and us-west-2, 2026-10-09).
const DUPLICATE_SET_IN_EAV_ENVELOPED: &str = "1 validation error detected: \
     One or more parameter values were invalid: \
     Input collection [\"a\", \"b\", \"a\"] contains duplicates.";

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
         Input collection [\"x\", \"x\"] contains duplicates.",
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
        r#"1 validation error detected: Invalid ConditionExpression: Syntax error; token: "=", near: "==""#,
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
         Expression size has exceeded the maximum allowed size;",
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
        r#"1 validation error detected: Invalid UpdateExpression: Syntax error; token: "<EOF>", near: "SET""#,
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
         Expression size has exceeded the maximum allowed size;",
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
        // The validation framework's generic form, in 31 of 33 regions
        // (captured 2026-10-08): no value, no member, DynamoDB's own order.
        "1 validation error detected: Failed to satisfy constraint: Member must satisfy enum \
         value set: [ALL_OLD, UPDATED_OLD, ALL_NEW, UPDATE_NEW, NONE]",
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
         when using expressions",
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
         Input collection [\"This attribute is part of the key\", \
         \"This attribute is part of the key\"] contains duplicates.",
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
         Expression size has exceeded the maximum allowed size;",
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
         Expression size has exceeded the maximum allowed size;",
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
         Expression size has exceeded the maximum allowed size;",
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

#[tokio::test]
async fn test_search_vectors_http_bytes_match_the_in_process_facade() {
    // One database serves both surfaces so the same request must produce
    // identical bytes: the server serialises the same response type the
    // facade returns.
    let db = Database::memory().unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server_db = db.clone();
    let _handle = tokio::spawn(async move {
        dynoxide::server::serve_on(listener, server_db).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    dynamo_request(
        &url,
        "CreateTable",
        json!({
            "TableName": "VecHttp",
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "BillingMode": "PAY_PER_REQUEST",
            "VectorIndexes": [{
                "IndexName": "cosine",
                "VectorAttribute": {"AttributeName": "embedding"},
                "Dimensions": 3,
                "DistanceFunction": "COSINE",
                "Projection": {"ProjectionType": "ALL"}
            }]
        }),
    )
    .await;
    for (pk, v) in [("a", ["1", "0", "0"]), ("b", ["0", "1", "0"])] {
        let resp = dynamo_request(
            &url,
            "PutItem",
            json!({
                "TableName": "VecHttp",
                "Item": {
                    "pk": {"S": pk},
                    "embedding": {"L": v.map(|n| json!({"N": n})).to_vec()}
                }
            }),
        )
        .await;
        assert_eq!(resp.status(), 200);
    }

    let request_body = json!({
        "TableName": "VecHttp",
        "IndexName": "cosine",
        "SearchVector": [{"N": "1"}, {"N": "0"}, {"N": "0"}],
        "TopK": 10
    });

    let http_resp = dynamo_request(&url, "SearchVectors", request_body.clone()).await;
    assert_eq!(http_resp.status(), 200);
    let http_body = http_resp.text().await.unwrap();

    let facade_req: dynoxide::actions::search_vectors::SearchVectorsRequest =
        serde_json::from_value(request_body).unwrap();
    let facade_resp = db.search_vectors(facade_req).unwrap();
    let facade_body = serde_json::to_string(&facade_resp).unwrap();

    assert_eq!(http_body, facade_body);
    // Ranked best-first with the score serialised as a JSON double, and the
    // vector attribute excluded by default.
    assert!(
        http_body
            .starts_with("{\"SearchResults\":[{\"Item\":{\"pk\":{\"S\":\"a\"}},\"Score\":0.0}")
    );
    assert!(!http_body.contains("embedding"));
}

#[tokio::test]
async fn test_search_vectors_probe_shape_on_a_nonexistent_table() {
    let (url, _handle) = start_test_server().await;

    // The conformance suite's data-plane probe sends SearchVectors at a table
    // that cannot exist and expects a real ResourceNotFoundException, not an
    // unknown-operation fault.
    let resp = dynamo_request(
        &url,
        "SearchVectors",
        json!({
            "TableName": "_conformance_no_such_table_probe",
            "IndexName": "no-such-index",
            "SearchVector": [{"N": "1"}, {"N": "0"}, {"N": "0"}],
            "TopK": 1
        }),
    )
    .await;
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        body["__type"],
        "com.amazonaws.dynamodb.v20120810#ResourceNotFoundException"
    );
    assert_eq!(body["message"], "Requested resource not found");
}

#[tokio::test]
async fn test_search_vectors_top_k_zero_is_enveloped_over_the_wire() {
    let (url, _handle) = start_test_server().await;

    let resp = dynamo_request(
        &url,
        "SearchVectors",
        json!({
            "TableName": "AnyTable",
            "IndexName": "vix",
            "SearchVector": [{"N": "1"}],
            "TopK": 0
        }),
    )
    .await;
    assert_validation_error(
        resp,
        // The newer validation layer's wording (captured 2026-10-09).
        "1 validation error detected: Value at 'TopK' failed to satisfy constraint: \
         Member must have value greater than or equal to 1",
    )
    .await;
}

#[tokio::test]
async fn test_search_vectors_serialization_shapes_over_the_wire() {
    let (url, _handle) = start_test_server().await;

    // SearchVector must be a bare array of attribute values, not an
    // L-wrapped attribute value. The search endpoint quotes its deserialiser,
    // with a lowercase `message` (captured eu-west-2 and us-east-1, 23 October
    // 2026).
    let resp = dynamo_request(
        &url,
        "SearchVectors",
        json!({
            "TableName": "AnyTable", "IndexName": "vix",
            "SearchVector": {"L": [{"N": "1"}]}, "TopK": 1
        }),
    )
    .await;
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        body["__type"]
            .as_str()
            .unwrap()
            .ends_with("SerializationException"),
        "unexpected __type: {}",
        body["__type"]
    );
    let message = body["message"].as_str().unwrap();
    assert!(
        message.starts_with("invalid type: map, expected a sequence at line 1 column "),
        "unexpected message: {message}"
    );

    // A Create or Delete inside VectorIndexUpdates names the vector action,
    // not the GSI one. The two share a field name and the class the message
    // reports is decided by the parent, so resolving on the field name alone
    // told a caller their vector update was a global secondary index action.
    for (field, class) in [
        ("Create", "CreateVectorIndexAction"),
        ("Delete", "DeleteVectorIndexAction"),
    ] {
        let resp = dynamo_request(
            &url,
            "UpdateTable",
            json!({
                "TableName": "AnyTable",
                "VectorIndexUpdates": [{ field: [] }]
            }),
        )
        .await;
        assert_eq!(resp.status(), 400);
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(
            body["Message"],
            format!("Unrecognized collection type class com.amazonaws.dynamodb.v20120810.{class}"),
            "field {field}"
        );
    }

    // A scalar in the same position answers as it does under the GSI family:
    // only the collection class differs by parent, not the scalar wording.
    let resp = dynamo_request(
        &url,
        "UpdateTable",
        json!({"TableName": "AnyTable", "VectorIndexUpdates": [{"Create": 5}]}),
    )
    .await;
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["Message"], "Unexpected field type");

    // TopK as a string is a serialisation-layer type mismatch, quoted the way
    // a fractional TopK is.
    let resp = dynamo_request(
        &url,
        "SearchVectors",
        json!({
            "TableName": "AnyTable", "IndexName": "vix",
            "SearchVector": [{"N": "1"}], "TopK": "10"
        }),
    )
    .await;
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        body["__type"]
            .as_str()
            .unwrap()
            .ends_with("SerializationException"),
        "unexpected __type: {}",
        body["__type"]
    );
    let message = body["message"].as_str().unwrap();
    assert!(
        message.starts_with(r#"invalid type: string "10", expected i32 at line 1 column "#),
        "unexpected message: {message}"
    );
}

#[tokio::test]
async fn test_search_vectors_fractional_top_k_is_a_serialization_exception() {
    let (url, _handle) = start_test_server().await;

    // Captured from real DynamoDB (eu-west-2 and us-east-1, 2026-08-13):
    // a fractional TopK leaks the raw serde-style rejection, position suffix
    // included. The column is request-dependent, so only the shape up to the
    // column number is pinned here.
    let resp = dynamo_request(
        &url,
        "SearchVectors",
        json!({
            "TableName": "AnyTable", "IndexName": "vix",
            "SearchVector": [{"N": "1"}], "TopK": 3.5
        }),
    )
    .await;
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        body["__type"]
            .as_str()
            .unwrap()
            .ends_with("SerializationException"),
        "unexpected __type: {}",
        body["__type"]
    );
    let message = body["message"].as_str().unwrap();
    assert!(
        message.starts_with("invalid type: floating point `3.5`, expected i32 at line 1 column"),
        "unexpected message: {message}"
    );
}

/// Send a SearchVectors body and return the error's short type name and
/// message, asserting the status is 400.
async fn search_vectors_error(url: &str, body: serde_json::Value) -> (String, String) {
    let resp = dynamo_request(url, "SearchVectors", body).await;
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    let kind = body["__type"].as_str().unwrap();
    let kind = kind.rsplit('#').next().unwrap().to_string();
    let message = body["Message"].as_str().or(body["message"].as_str());
    (kind, message.unwrap_or_default().to_string())
}

/// Whether `message` is `prefix` followed by serde's position suffix,
/// " at line 1 column N". The column depends on the body's layout.
fn is_at_position(message: &str, prefix: &str) -> bool {
    message
        .strip_prefix(prefix)
        .and_then(|rest| rest.strip_prefix(" at line 1 column "))
        .is_some_and(|column| !column.is_empty() && column.bytes().all(|b| b.is_ascii_digit()))
}

#[tokio::test]
async fn test_search_vectors_explicit_null_member_is_a_serialization_exception() {
    let (url, _handle) = start_test_server().await;

    // SearchVectors reads its body with a strict deserialiser, so an explicit
    // null for a required member is a SerializationException quoting it,
    // position included, ahead of any member check. Captured from real
    // DynamoDB (eu-west-2 and us-east-1, 9 October 2026, again 23 October).
    let valid = json!({
        "TableName": "AnyTable", "IndexName": "vix",
        "SearchVector": [{"N": "1"}], "TopK": 1
    });
    for (member, expected) in [
        ("IndexName", "a string"),
        ("TableName", "a string"),
        ("SearchVector", "a sequence"),
        ("TopK", "i32"),
    ] {
        let mut body = valid.clone();
        body[member] = serde_json::Value::Null;
        let (kind, message) = search_vectors_error(&url, body).await;
        assert_eq!(kind, "SerializationException", "{member}: {message}");
        assert!(
            is_at_position(
                &message,
                &format!("invalid type: null, expected {expected}")
            ),
            "{member}: unexpected message: {message}"
        );
    }

    // A null name is refused as it is read, before a too-short IndexName.
    let (kind, message) = search_vectors_error(
        &url,
        json!({
            "TableName": null, "IndexName": "ab",
            "SearchVector": [{"N": "1"}], "TopK": 1
        }),
    )
    .await;
    assert_eq!(kind, "SerializationException");
    assert!(
        is_at_position(&message, "invalid type: null, expected a string"),
        "unexpected message: {message}"
    );
}

#[tokio::test]
async fn test_search_vectors_null_optional_member_reads_as_absent() {
    let (url, _handle) = start_test_server().await;

    // A null optional member is read as absent, so the request goes on to the
    // table lookup (captured, eu-west-2 and us-east-1, 23 October 2026).
    for member in [
        "SearchConditionExpression",
        "ProjectionExpression",
        "ExpressionAttributeNames",
        "ExpressionAttributeValues",
        "ReturnConsumedCapacity",
    ] {
        let mut body = json!({
            "TableName": "AnyTable", "IndexName": "vix",
            "SearchVector": [{"N": "1"}], "TopK": 1
        });
        body[member] = serde_json::Value::Null;
        let (kind, _) = search_vectors_error(&url, body).await;
        assert_eq!(kind, "ResourceNotFoundException", "{member}");
    }
}

#[tokio::test]
async fn test_search_vectors_missing_member_names_the_first_in_dynamodb_order() {
    let (url, _handle) = start_test_server().await;

    // A missing TableName is serde's missing-field message like the other
    // three required members, ahead of a too-short IndexName. With more than
    // one missing, DynamoDB names the first of IndexName, SearchVector,
    // TableName, TopK (captured, eu-west-2 and us-east-1, 23 October 2026).
    let valid = json!({
        "TableName": "AnyTable", "IndexName": "vix",
        "SearchVector": [{"N": "1"}], "TopK": 1
    });
    let without = |members: &[&str], short_index: bool| {
        let mut body = valid.clone();
        let object = body.as_object_mut().unwrap();
        for member in members {
            object.remove(*member);
        }
        if short_index {
            object.insert("IndexName".to_string(), json!("ab"));
        }
        body
    };
    for (missing, short_index, named) in [
        (&["TableName"][..], false, "TableName"),
        (&["TableName"][..], true, "TableName"),
        (&["TableName", "SearchVector"][..], false, "SearchVector"),
        (&["TableName", "TopK"][..], false, "TableName"),
        (&["IndexName", "SearchVector"][..], false, "IndexName"),
        (&["SearchVector", "TopK"][..], false, "SearchVector"),
        (
            &["TableName", "IndexName", "SearchVector", "TopK"][..],
            false,
            "IndexName",
        ),
    ] {
        let (kind, message) = search_vectors_error(&url, without(missing, short_index)).await;
        assert_eq!(kind, "ValidationException", "{missing:?}: {message}");
        assert!(
            is_at_position(&message, &format!("missing field `{named}`")),
            "{missing:?}: unexpected message: {message}"
        );
    }
}

/// Send `body` to SearchVectors exactly as written, so a message's column can
/// be compared with the one DynamoDB gave for the same bytes, and return the
/// status and the response body.
async fn search_vectors_raw(url: &str, body: &str) -> (u16, String) {
    let resp = client()
        .post(url)
        .header("x-amz-target", "DynamoDB_20120810.SearchVectors")
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
    (resp.status().as_u16(), resp.text().await.unwrap())
}

/// A SearchVectors body naming an absent table, with `values` as its
/// ExpressionAttributeValues.
fn search_vectors_with_values(values: &str) -> String {
    format!(
        r#"{{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{{"N":"1"}}],"TopK":1,"ExpressionAttributeValues":{values}}}"#
    )
}

/// A SearchVectors body naming an absent table, with `vector` as its
/// SearchVector.
fn search_vectors_with_vector(vector: &str) -> String {
    format!(
        r#"{{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":{vector},"TopK":1}}"#
    )
}

#[tokio::test]
async fn test_search_vectors_shapeless_value_is_a_serialization_exception_with_no_message() {
    let (url, _handle) = start_test_server().await;

    // SearchVectors reads each attribute value strictly, one member at a
    // time. A value that is null, has no member or has a second one, at the
    // top or inside a list or map, is a SerializationException with no
    // message at all (captured eu-west-2 and us-east-1, 23 October 2026).
    let bodies = [
        search_vectors_with_vector("[null]"),
        search_vectors_with_vector(r#"[{"N":"1"},null]"#),
        search_vectors_with_vector("[{}]"),
        search_vectors_with_vector(r#"[{"S":"x","N":"1"}]"#),
        search_vectors_with_values(r#"{":v":null}"#),
        search_vectors_with_values(r#"{":v":{}}"#),
        search_vectors_with_values(r#"{":a":{"S":"x"},":v":null}"#),
        search_vectors_with_values(r#"{":v":{"S":"x","N":"1"}}"#),
        search_vectors_with_values(r#"{":v":{"S":"a","N":null}}"#),
        search_vectors_with_values(r#"{":v":{"L":[null]}}"#),
        search_vectors_with_values(r#"{":v":{"M":{"a":null}}}"#),
        // Read ahead of a null IndexName later in the body.
        r#"{"TableName":"_capture_20261023_absent_x9","SearchVector":[null],"IndexName":null,"TopK":1}"#
            .to_string(),
    ];
    for body in bodies {
        let (status, response) = search_vectors_raw(&url, &body).await;
        assert_eq!(status, 400, "{body}");
        assert_eq!(
            response, r#"{"__type":"com.amazon.coral.service#SerializationException"}"#,
            "{body}"
        );
    }
}

#[tokio::test]
async fn test_search_vectors_typed_null_quotes_the_deserialiser() {
    let (url, _handle) = start_test_server().await;

    // A null where a member's own value belongs names the type the member
    // takes, at the position DynamoDB gave for the same bytes (captured
    // eu-west-2 and us-east-1, 23 October 2026).
    let cases = [
        (
            search_vectors_with_vector(r#"[{"N":null}]"#),
            "a string at line 1 column 86",
        ),
        (
            search_vectors_with_vector(r#"[{"S":null}]"#),
            "a string at line 1 column 86",
        ),
        (
            search_vectors_with_vector(r#"[{"BOOL":null}]"#),
            "a boolean at line 1 column 89",
        ),
        (
            search_vectors_with_vector(r#"[{"L":null}]"#),
            "a sequence at line 1 column 86",
        ),
        (
            search_vectors_with_vector(r#"[{"NULL":null}]"#),
            "a boolean at line 1 column 89",
        ),
        (
            search_vectors_with_values(r#"{":v":{"N":null}}"#),
            "a string at line 1 column 140",
        ),
        (
            search_vectors_with_values(r#"{":v":{"S":null}}"#),
            "a string at line 1 column 140",
        ),
        (
            search_vectors_with_values(r#"{":v":{"B":null}}"#),
            "a Base64-encoded string at line 1 column 140",
        ),
        (
            search_vectors_with_values(r#"{":v":{"M":null}}"#),
            "a map at line 1 column 140",
        ),
        (
            search_vectors_with_values(r#"{":v":{"SS":null}}"#),
            "a sequence at line 1 column 141",
        ),
        (
            search_vectors_with_values(r#"{":v":{"BOOL":null}}"#),
            "a boolean at line 1 column 143",
        ),
        (
            search_vectors_with_values(r#"{":v":{"NULL":null}}"#),
            "a boolean at line 1 column 143",
        ),
        (
            search_vectors_with_values(r#"{":v":{"SS":[null]}}"#),
            "a string at line 1 column 142",
        ),
        // The first member is read before the second is noticed.
        (
            search_vectors_with_values(r#"{":v":{"N":null,"S":"a"}}"#),
            "a string at line 1 column 140",
        ),
    ];
    for (body, expected) in cases {
        let (status, response) = search_vectors_raw(&url, &body).await;
        assert_eq!(status, 400, "{body}");
        let response: serde_json::Value = serde_json::from_str(&response).unwrap();
        assert_eq!(
            response["__type"], "com.amazon.coral.service#SerializationException",
            "{body}"
        );
        assert_eq!(
            response["message"],
            format!("invalid type: null, expected {expected}"),
            "{body}"
        );
    }
}

#[tokio::test]
async fn test_null_value_on_query_is_still_a_validation_exception() {
    let (url, _handle) = start_test_server().await;

    // Only SearchVectors reads values strictly: Query answers the same shapes
    // with a ValidationException, as DynamoDB does (captured eu-west-2, 23
    // October 2026).
    for values in [
        json!({":v": null}),
        json!({":v": {}}),
        json!({":v": {"S": null}}),
    ] {
        let resp = dynamo_request(
            &url,
            "Query",
            json!({
                "TableName": "AnyTable",
                "KeyConditionExpression": "pk = :v",
                "ExpressionAttributeValues": values
            }),
        )
        .await;
        assert_eq!(resp.status(), 400);
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(
            body["__type"], "com.amazon.coral.validate#ValidationException",
            "{values}"
        );
    }
}

/// Send each captured SearchVectors body and compare the whole response with
/// the bytes DynamoDB's search endpoint returned for it.
async fn assert_search_vectors_answers(url: &str, cases: &[(&str, &str, u16, &str)]) {
    for (name, body, status, expected) in cases {
        let (got_status, got) = search_vectors_raw(url, body).await;
        assert_eq!(got_status, *status, "{name}: {got}");
        assert_eq!(got, *expected, "{name}");
    }
}

#[tokio::test]
async fn test_search_vectors_wrongly_typed_member_quotes_the_deserialiser() {
    let (url, _handle) = start_test_server().await;

    // A member holding the wrong JSON type, at any depth, is a SerializationException
    // quoting the deserialiser, position included. Every response is the body DynamoDB's search
    // endpoint gave for the same request (captured eu-west-2 and us-east-1,
    // 23 October 2026, identical apart from columns).
    assert_search_vectors_answers(
        &url,
        &[
            (
                "svN_int",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":1}],"TopK":1}"#,
                400,
                r#"{"__type":"com.amazon.coral.service#SerializationException","message":"invalid type: integer `1`, expected a string at line 1 column 83"}"#,
            ),
            (
                "svN_float",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":1.5}],"TopK":1}"#,
                400,
                r#"{"__type":"com.amazon.coral.service#SerializationException","message":"invalid type: floating point `1.5`, expected a string at line 1 column 85"}"#,
            ),
            (
                "svN_bool",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":true}],"TopK":1}"#,
                400,
                r#"{"__type":"com.amazon.coral.service#SerializationException","message":"invalid type: boolean `true`, expected a string at line 1 column 86"}"#,
            ),
            (
                "svN_map",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":{}}],"TopK":1}"#,
                400,
                r#"{"__type":"com.amazon.coral.service#SerializationException","message":"invalid type: map, expected a string at line 1 column 82"}"#,
            ),
            (
                "svN_seq",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":[]}],"TopK":1}"#,
                400,
                r#"{"__type":"com.amazon.coral.service#SerializationException","message":"invalid type: sequence, expected a string at line 1 column 82"}"#,
            ),
            (
                "svS_int",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"S":1}],"TopK":1}"#,
                400,
                r#"{"__type":"com.amazon.coral.service#SerializationException","message":"invalid type: integer `1`, expected a string at line 1 column 83"}"#,
            ),
            (
                "svB_int",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"B":1}],"TopK":1}"#,
                400,
                r#"{"__type":"com.amazon.coral.service#SerializationException","message":"invalid type: integer `1`, expected a Base64-encoded string at line 1 column 83"}"#,
            ),
            (
                "svBOOL_str",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"BOOL":"true"}],"TopK":1}"#,
                400,
                r#"{"__type":"com.amazon.coral.service#SerializationException","message":"invalid type: string \"true\", expected a boolean at line 1 column 91"}"#,
            ),
            (
                "svBOOL_int",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"BOOL":1}],"TopK":1}"#,
                400,
                r#"{"__type":"com.amazon.coral.service#SerializationException","message":"invalid type: integer `1`, expected a boolean at line 1 column 86"}"#,
            ),
            (
                "svNULL_str",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"NULL":"true"}],"TopK":1}"#,
                400,
                r#"{"__type":"com.amazon.coral.service#SerializationException","message":"invalid type: string \"true\", expected a boolean at line 1 column 91"}"#,
            ),
            (
                "svL_str",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"L":"a"}],"TopK":1}"#,
                400,
                r#"{"__type":"com.amazon.coral.service#SerializationException","message":"invalid type: string \"a\", expected a sequence at line 1 column 85"}"#,
            ),
            (
                "svL_map",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"L":{}}],"TopK":1}"#,
                400,
                r#"{"__type":"com.amazon.coral.service#SerializationException","message":"invalid type: map, expected a sequence at line 1 column 82"}"#,
            ),
            (
                "svM_str",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"M":"a"}],"TopK":1}"#,
                400,
                r#"{"__type":"com.amazon.coral.service#SerializationException","message":"invalid type: string \"a\", expected a map at line 1 column 85"}"#,
            ),
            (
                "svM_seq",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"M":[]}],"TopK":1}"#,
                400,
                r#"{"__type":"com.amazon.coral.service#SerializationException","message":"invalid type: sequence, expected a map at line 1 column 82"}"#,
            ),
            (
                "svSS_str",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"SS":"a"}],"TopK":1}"#,
                400,
                r#"{"__type":"com.amazon.coral.service#SerializationException","message":"invalid type: string \"a\", expected a sequence at line 1 column 86"}"#,
            ),
            (
                "svSS_intElem",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"SS":[1]}],"TopK":1}"#,
                400,
                r#"{"__type":"com.amazon.coral.service#SerializationException","message":"invalid type: integer `1`, expected a string at line 1 column 85"}"#,
            ),
            (
                "svNS_intElem",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"NS":[1]}],"TopK":1}"#,
                400,
                r#"{"__type":"com.amazon.coral.service#SerializationException","message":"invalid type: integer `1`, expected a string at line 1 column 85"}"#,
            ),
            (
                "svBS_str",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"BS":"a"}],"TopK":1}"#,
                400,
                r#"{"__type":"com.amazon.coral.service#SerializationException","message":"invalid type: string \"a\", expected a sequence at line 1 column 86"}"#,
            ),
            (
                "sv_map",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":{},"TopK":1}"#,
                400,
                r#"{"__type":"com.amazon.coral.service#SerializationException","message":"invalid type: map, expected a sequence at line 1 column 76"}"#,
            ),
            (
                "sv_str",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":"x","TopK":1}"#,
                400,
                r#"{"__type":"com.amazon.coral.service#SerializationException","message":"invalid type: string \"x\", expected a sequence at line 1 column 79"}"#,
            ),
            (
                "sv_int",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":1,"TopK":1}"#,
                400,
                r#"{"__type":"com.amazon.coral.service#SerializationException","message":"invalid type: integer `1`, expected a sequence at line 1 column 77"}"#,
            ),
            (
                "ev_str",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"ExpressionAttributeValues":"x"}"#,
                400,
                r#"{"__type":"com.amazon.coral.service#SerializationException","message":"invalid type: string \"x\", expected a map at line 1 column 128"}"#,
            ),
            (
                "ev_seq",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"ExpressionAttributeValues":[]}"#,
                400,
                r#"{"__type":"com.amazon.coral.service#SerializationException","message":"invalid type: sequence, expected a map at line 1 column 125"}"#,
            ),
            (
                "evN_int",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"ExpressionAttributeValues":{":v":{"N":1}}}"#,
                400,
                r#"{"__type":"com.amazon.coral.service#SerializationException","message":"invalid type: integer `1`, expected a string at line 1 column 137"}"#,
            ),
            (
                "evS_bool",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"ExpressionAttributeValues":{":v":{"S":false}}}"#,
                400,
                r#"{"__type":"com.amazon.coral.service#SerializationException","message":"invalid type: boolean `false`, expected a string at line 1 column 141"}"#,
            ),
            (
                "evM_seq",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"ExpressionAttributeValues":{":v":{"M":[]}}}"#,
                400,
                r#"{"__type":"com.amazon.coral.service#SerializationException","message":"invalid type: sequence, expected a map at line 1 column 136"}"#,
            ),
            (
                "evL_map",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"ExpressionAttributeValues":{":v":{"L":{}}}}"#,
                400,
                r#"{"__type":"com.amazon.coral.service#SerializationException","message":"invalid type: map, expected a sequence at line 1 column 136"}"#,
            ),
            (
                "evBOOL_str",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"ExpressionAttributeValues":{":v":{"BOOL":"true"}}}"#,
                400,
                r#"{"__type":"com.amazon.coral.service#SerializationException","message":"invalid type: string \"true\", expected a boolean at line 1 column 145"}"#,
            ),
            (
                "evNULL_str",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"ExpressionAttributeValues":{":v":{"NULL":"true"}}}"#,
                400,
                r#"{"__type":"com.amazon.coral.service#SerializationException","message":"invalid type: string \"true\", expected a boolean at line 1 column 145"}"#,
            ),
            (
                "tbl_int",
                r#"{"TableName":5,"IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1}"#,
                400,
                r#"{"__type":"com.amazon.coral.service#SerializationException","message":"invalid type: integer `5`, expected a string at line 1 column 14"}"#,
            ),
            (
                "topk_float",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1.5}"#,
                400,
                r#"{"__type":"com.amazon.coral.service#SerializationException","message":"invalid type: floating point `1.5`, expected i32 at line 1 column 98"}"#,
            ),
            (
                "v_eavINT_bad",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"ExpressionAttributeValues":{":v":{"INT":1}}}"#,
                400,
                r#"{"__type":"com.amazon.coral.service#SerializationException","message":"invalid type: integer `1`, expected a string at line 1 column 139"}"#,
            ),
            (
                "v_eavINT_null",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"ExpressionAttributeValues":{":v":{"INT":null}}}"#,
                400,
                r#"{"__type":"com.amazon.coral.service#SerializationException","message":"invalid type: null, expected a string at line 1 column 142"}"#,
            ),
            (
                "v_svINT_null",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"INT":null}],"TopK":1}"#,
                400,
                r#"{"__type":"com.amazon.coral.service#SerializationException","message":"invalid type: null, expected a string at line 1 column 88"}"#,
            ),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_search_vectors_unknown_type_lists_the_variants() {
    let (url, _handle) = start_test_server().await;

    // A type DynamoDB does not know, or a bare string where a value belongs, lists
    // the types it does know; any other value that is not an object has no
    // message, nor does a body that is not JSON. Every response is the body DynamoDB's search
    // endpoint gave for the same request (captured eu-west-2 and us-east-1,
    // 23 October 2026, identical apart from columns).
    assert_search_vectors_answers(
        &url,
        &[
            (
                "svElem_str",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":["1"],"TopK":1}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"unknown variant `1`, expected one of `B`, `BOOL`, `BS`, `DECIMAL`, `DECIMALSET`, `DOUBLE`, `DOUBLESET`, `INT`, `INTSET`, `L`, `M`, `N`, `NS`, `NULL`, `S`, `SS` at line 1 column 80"}"#,
            ),
            (
                "svElem_int",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[1],"TopK":1}"#,
                400,
                r#"{"__type":"com.amazon.coral.service#SerializationException"}"#,
            ),
            (
                "svElem_seq",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[[{"N":"1"}]],"TopK":1}"#,
                400,
                r#"{"__type":"com.amazon.coral.service#SerializationException"}"#,
            ),
            (
                "svElem_bool",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[true],"TopK":1}"#,
                400,
                r#"{"__type":"com.amazon.coral.service#SerializationException"}"#,
            ),
            (
                "evVal_str",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"ExpressionAttributeValues":{":v":"x"}}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"unknown variant `x`, expected one of `B`, `BOOL`, `BS`, `DECIMAL`, `DECIMALSET`, `DOUBLE`, `DOUBLESET`, `INT`, `INTSET`, `L`, `M`, `N`, `NS`, `NULL`, `S`, `SS` at line 1 column 134"}"#,
            ),
            (
                "evVal_int",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"ExpressionAttributeValues":{":v":1}}"#,
                400,
                r#"{"__type":"com.amazon.coral.service#SerializationException"}"#,
            ),
            (
                "evVal_seq",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"ExpressionAttributeValues":{":v":[]}}"#,
                400,
                r#"{"__type":"com.amazon.coral.service#SerializationException"}"#,
            ),
            (
                "svX",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"X":"1"}],"TopK":1}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"unknown variant `X`, expected one of `B`, `BOOL`, `BS`, `DECIMAL`, `DECIMALSET`, `DOUBLE`, `DOUBLESET`, `INT`, `INTSET`, `L`, `M`, `N`, `NS`, `NULL`, `S`, `SS` at line 1 column 81"}"#,
            ),
            (
                "evX",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"ExpressionAttributeValues":{":v":{"X":"1"}}}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"unknown variant `X`, expected one of `B`, `BOOL`, `BS`, `DECIMAL`, `DECIMALSET`, `DOUBLE`, `DOUBLESET`, `INT`, `INTSET`, `L`, `M`, `N`, `NS`, `NULL`, `S`, `SS` at line 1 column 135"}"#,
            ),
            (
                "evLower",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"ExpressionAttributeValues":{":v":{"s":"a"}}}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"unknown variant `s`, expected one of `B`, `BOOL`, `BS`, `DECIMAL`, `DECIMALSET`, `DOUBLE`, `DOUBLESET`, `INT`, `INTSET`, `L`, `M`, `N`, `NS`, `NULL`, `S`, `SS` at line 1 column 135"}"#,
            ),
            (
                "evX_then_S",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"ExpressionAttributeValues":{":v":{"X":"1","S":"a"}}}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"unknown variant `X`, expected one of `B`, `BOOL`, `BS`, `DECIMAL`, `DECIMALSET`, `DOUBLE`, `DOUBLESET`, `INT`, `INTSET`, `L`, `M`, `N`, `NS`, `NULL`, `S`, `SS` at line 1 column 135"}"#,
            ),
            (
                "evS_then_X",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"ExpressionAttributeValues":{":v":{"S":"a","X":"1"}}}"#,
                400,
                r#"{"__type":"com.amazon.coral.service#SerializationException"}"#,
            ),
            (
                "v_eavINT_then_S",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"ExpressionAttributeValues":{":v":{"INT":"1","S":"a"}}}"#,
                400,
                r#"{"__type":"com.amazon.coral.service#SerializationException"}"#,
            ),
            (
                "bad_json",
                r#"{not json"#,
                400,
                r#"{"__type":"com.amazon.coral.service#SerializationException"}"#,
            ),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_search_vectors_expression_value_refusals() {
    let (url, _handle) = start_test_server().await;

    // Each expression value is checked on its own before the expression
    // parameters are: a type DynamoDB keeps for itself, an empty set, a number it
    // cannot read, a NULL that is not true. Every response is the body DynamoDB's search
    // endpoint gave for the same request (captured eu-west-2 and us-east-1,
    // 23 October 2026, identical apart from columns).
    assert_search_vectors_answers(
        &url,
        &[
            (
                "evINT",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"ExpressionAttributeValues":{":v":{"INT":"1"}}}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"This type mapping is not supported"}"#,
            ),
            (
                "evDECIMAL",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"ExpressionAttributeValues":{":v":{"DECIMAL":"1"}}}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"This type mapping is not supported"}"#,
            ),
            (
                "evDOUBLE",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"ExpressionAttributeValues":{":v":{"DOUBLE":"1"}}}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"This type mapping is not supported"}"#,
            ),
            (
                "evINTSET",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"ExpressionAttributeValues":{":v":{"INTSET":["1"]}}}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"This type mapping is not supported"}"#,
            ),
            (
                "v_eavDECIMALSET",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"ExpressionAttributeValues":{":v":{"DECIMALSET":["1"]}}}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"This type mapping is not supported"}"#,
            ),
            (
                "v_eavDOUBLESET",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"ExpressionAttributeValues":{":v":{"DOUBLESET":["1"]}}}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"This type mapping is not supported"}"#,
            ),
            (
                "evSS_empty",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"ExpressionAttributeValues":{":v":{"SS":[]}}}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"One or more parameter values were invalid: An string set  may not be empty"}"#,
            ),
            (
                "evNS_empty",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"ExpressionAttributeValues":{":v":{"NS":[]}}}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"One or more parameter values were invalid: An number set  may not be empty"}"#,
            ),
            (
                "evBS_empty",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"ExpressionAttributeValues":{":v":{"BS":[]}}}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"One or more parameter values were invalid: Binary sets should not be empty"}"#,
            ),
            (
                "evN_abc",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"ExpressionAttributeValues":{":v":{"N":"abc"}}}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"The parameter cannot be converted to a numeric value: abc"}"#,
            ),
            (
                "evNS_abc",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"ExpressionAttributeValues":{":v":{"NS":["abc"]}}}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"The parameter cannot be converted to a numeric value: abc"}"#,
            ),
            (
                "evN_empty",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"ExpressionAttributeValues":{":v":{"N":""}}}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"The parameter cannot be converted to a numeric value: "}"#,
            ),
            (
                "evNULL_false",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"ExpressionAttributeValues":{":v":{"NULL":false}}}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"One or more parameter values were invalid: Null attribute value types must have the value of true"}"#,
            ),
            (
                "v_eavINT_noExpr",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"ExpressionAttributeValues":{":v":{"INT":"1"}}}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"This type mapping is not supported"}"#,
            ),
            (
                "e_eav_noexpr_emptySS_twokeys",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"ExpressionAttributeValues":{":v":{"S":"a"},":w":{"SS":[]}}}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"One or more parameter values were invalid: An string set  may not be empty"}"#,
            ),
            (
                "e_ean_noexpr_eav_emptySS",
                r##"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"ExpressionAttributeNames":{"#a":"a"},"ExpressionAttributeValues":{":v":{"SS":[]}}}"##,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"One or more parameter values were invalid: An string set  may not be empty"}"#,
            ),
            (
                "e_eav_noexpr_emptySS_badkey",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"ExpressionAttributeValues":{"v":{"SS":[]}}}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"One or more parameter values were invalid: An string set  may not be empty"}"#,
            ),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_search_vectors_search_vector_holds_32_bit_floats() {
    let (url, _handle) = start_test_server().await;

    // Every element of the search vector must be a number that reads as a finite
    // 32-bit float, whatever else it is; the check runs before the table is
    // looked up. Every response is the body DynamoDB's search
    // endpoint gave for the same request (captured eu-west-2 and us-east-1,
    // 23 October 2026, identical apart from columns).
    assert_search_vectors_answers(
        &url,
        &[
            (
                "svINT",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"INT":"1"}],"TopK":1}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Search vector contains invalid values. All values in the search vector must be a 32-bit floating-point number attribute"}"#,
            ),
            (
                "svSS_empty",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"SS":[]}],"TopK":1}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Search vector contains invalid values. All values in the search vector must be a 32-bit floating-point number attribute"}"#,
            ),
            (
                "svN_abc",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"abc"}],"TopK":1}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Search vector contains invalid values. All values in the search vector must be a 32-bit floating-point number attribute"}"#,
            ),
            (
                "svN_empty",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":""}],"TopK":1}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Search vector contains invalid values. All values in the search vector must be a 32-bit floating-point number attribute"}"#,
            ),
            (
                "svN_huge",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1e400"}],"TopK":1}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Search vector contains invalid values. All values in the search vector must be a 32-bit floating-point number attribute"}"#,
            ),
            (
                "svN_tiny",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1e-400"}],"TopK":1}"#,
                400,
                r#"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found"}"#,
            ),
            (
                "svN_42digits",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"123456789012345678901234567890123456789012"}],"TopK":1}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Search vector contains invalid values. All values in the search vector must be a 32-bit floating-point number attribute"}"#,
            ),
            (
                "svS",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"S":"a"}],"TopK":1}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Search vector contains invalid values. All values in the search vector must be a 32-bit floating-point number attribute"}"#,
            ),
            (
                "svN_then_S",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"},{"S":"a"}],"TopK":1}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Search vector contains invalid values. All values in the search vector must be a 32-bit floating-point number attribute"}"#,
            ),
            (
                "svN_abc_then_S",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"S":"a"},{"N":"abc"}],"TopK":1}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Search vector contains invalid values. All values in the search vector must be a 32-bit floating-point number attribute"}"#,
            ),
            (
                "v_svLelem",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"L":[{"N":"1"}]}],"TopK":1}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Search vector contains invalid values. All values in the search vector must be a 32-bit floating-point number attribute"}"#,
            ),
            (
                "v_svN_1e39",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1e39"}],"TopK":1}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Search vector contains invalid values. All values in the search vector must be a 32-bit floating-point number attribute"}"#,
            ),
            (
                "v_svN_ws",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":" 1"}],"TopK":1}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Search vector contains invalid values. All values in the search vector must be a 32-bit floating-point number attribute"}"#,
            ),
            (
                "v_svN_hex",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"0x1"}],"TopK":1}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Search vector contains invalid values. All values in the search vector must be a 32-bit floating-point number attribute"}"#,
            ),
            (
                "v_svN_inf",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"Infinity"}],"TopK":1}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Search vector contains invalid values. All values in the search vector must be a 32-bit floating-point number attribute"}"#,
            ),
            (
                "v_svN_nan",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"NaN"}],"TopK":1}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Search vector contains invalid values. All values in the search vector must be a 32-bit floating-point number attribute"}"#,
            ),
            (
                "v_svN_plus",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"+1"}],"TopK":1}"#,
                400,
                r#"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found"}"#,
            ),
            (
                "v_svN_dot",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":".5"}],"TopK":1}"#,
                400,
                r#"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found"}"#,
            ),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_search_vectors_check_order() {
    let (url, _handle) = start_test_server().await;

    // The member constraints, the table name, ReturnConsumedCapacity and the TopK
    // ceiling come first, then the search vector, then the expression values. Every response is the body DynamoDB's search
    // endpoint gave for the same request (captured eu-west-2 and us-east-1,
    // 23 October 2026, identical apart from columns).
    assert_search_vectors_answers(
        &url,
        &[
            (
                "o_idxShort",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"ab","SearchVector":[{"S":"a"}],"TopK":1}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'IndexName' failed to satisfy constraint: Member must have length greater than or equal to 3"}"#,
            ),
            (
                "o_topk0",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"S":"a"}],"TopK":0}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'TopK' failed to satisfy constraint: Member must have value greater than or equal to 1"}"#,
            ),
            (
                "o_topk101",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"S":"a"}],"TopK":101}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Provided TopK value '101' is out of valid range. The value must be between 1 and 100 inclusive"}"#,
            ),
            (
                "o_tblBad",
                r#"{"TableName":"a!","IndexName":"vix","SearchVector":[{"S":"a"}],"TopK":1}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Value 'a!' at 'tableName' failed to satisfy constraint: Member must have length greater than or equal to 3"}"#,
            ),
            (
                "o_tblEmpty",
                r#"{"TableName":"","IndexName":"vix","SearchVector":[{"S":"a"}],"TopK":1}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'TableName' failed to satisfy constraint: Member must have length greater than or equal to 1"}"#,
            ),
            (
                "o_eavNoExpr",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"S":"a"}],"TopK":1,"ExpressionAttributeValues":{":v":{"S":"a"}}}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Search vector contains invalid values. All values in the search vector must be a 32-bit floating-point number attribute"}"#,
            ),
            (
                "o_eavEmpty",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"S":"a"}],"TopK":1,"ExpressionAttributeValues":{}}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Search vector contains invalid values. All values in the search vector must be a 32-bit floating-point number attribute"}"#,
            ),
            (
                "o_eavEmptySS",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"S":"a"}],"TopK":1,"ExpressionAttributeValues":{":v":{"SS":[]}}}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Search vector contains invalid values. All values in the search vector must be a 32-bit floating-point number attribute"}"#,
            ),
            (
                "o_eavBadN",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"S":"a"}],"TopK":1,"ExpressionAttributeValues":{":v":{"N":"abc"}}}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Search vector contains invalid values. All values in the search vector must be a 32-bit floating-point number attribute"}"#,
            ),
            (
                "o_eavINT",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"S":"a"}],"TopK":1,"ExpressionAttributeValues":{":v":{"INT":"1"}}}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Search vector contains invalid values. All values in the search vector must be a 32-bit floating-point number attribute"}"#,
            ),
            (
                "o_scSyntax",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"S":"a"}],"TopK":1,"SearchConditionExpression":"a = = :v","ExpressionAttributeValues":{":v":{"S":"a"}}}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Search vector contains invalid values. All values in the search vector must be a 32-bit floating-point number attribute"}"#,
            ),
            (
                "o_scEmpty",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"S":"a"}],"TopK":1,"SearchConditionExpression":""}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Search vector contains invalid values. All values in the search vector must be a 32-bit floating-point number attribute"}"#,
            ),
            (
                "o_projEmpty",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"S":"a"}],"TopK":1,"ProjectionExpression":""}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Search vector contains invalid values. All values in the search vector must be a 32-bit floating-point number attribute"}"#,
            ),
            (
                "o_dims",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"},{"S":"a"}],"TopK":1}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Search vector contains invalid values. All values in the search vector must be a 32-bit floating-point number attribute"}"#,
            ),
            (
                "v_eavINT_idxShort",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"ab","SearchVector":[{"N":"1"}],"TopK":1,"ExpressionAttributeValues":{":v":{"INT":"1"}}}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'IndexName' failed to satisfy constraint: Member must have length greater than or equal to 3"}"#,
            ),
            (
                "v_eavEmptySS_idxShort",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"ab","SearchVector":[{"N":"1"}],"TopK":1,"ExpressionAttributeValues":{":v":{"SS":[]}}}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'IndexName' failed to satisfy constraint: Member must have length greater than or equal to 3"}"#,
            ),
            (
                "v_eavBadN_idxShort",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"ab","SearchVector":[{"N":"1"}],"TopK":1,"ExpressionAttributeValues":{":v":{"N":"abc"}}}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'IndexName' failed to satisfy constraint: Member must have length greater than or equal to 3"}"#,
            ),
            (
                "v_eavNoExpr_idxShort",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"ab","SearchVector":[{"N":"1"}],"TopK":1,"ExpressionAttributeValues":{":v":{"S":"a"}}}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'IndexName' failed to satisfy constraint: Member must have length greater than or equal to 3"}"#,
            ),
            (
                "v_eavNoExpr_topk101",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":101,"ExpressionAttributeValues":{":v":{"S":"a"}}}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Provided TopK value '101' is out of valid range. The value must be between 1 and 100 inclusive"}"#,
            ),
            (
                "v_eavEmpty_idxShort",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"ab","SearchVector":[{"N":"1"}],"TopK":1,"ExpressionAttributeValues":{}}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'IndexName' failed to satisfy constraint: Member must have length greater than or equal to 3"}"#,
            ),
            (
                "v_eavNULLfalse_idxShort",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"ab","SearchVector":[{"N":"1"}],"TopK":1,"ExpressionAttributeValues":{":v":{"NULL":false}}}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'IndexName' failed to satisfy constraint: Member must have length greater than or equal to 3"}"#,
            ),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_search_vectors_expression_parameters() {
    let (url, _handle) = start_test_server().await;

    // Names need an expression and values need a SearchConditionExpression; an
    // empty map, a malformed key and an unused name or value are refused before
    // the table is looked up, names ahead of values. Every response is the body DynamoDB's search
    // endpoint gave for the same request (captured eu-west-2 and us-east-1,
    // 23 October 2026, identical apart from columns).
    assert_search_vectors_answers(
        &url,
        &[
            (
                "evNULL_true",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"ExpressionAttributeValues":{":v":{"NULL":true}}}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"ExpressionAttributeValues can only be specified when using expressions"}"#,
            ),
            (
                "evL_empty",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"ExpressionAttributeValues":{":v":{"L":[]}}}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"ExpressionAttributeValues can only be specified when using expressions"}"#,
            ),
            (
                "evM_empty",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"ExpressionAttributeValues":{":v":{"M":{}}}}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"ExpressionAttributeValues can only be specified when using expressions"}"#,
            ),
            (
                "ev_noexpr",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"ExpressionAttributeValues":{":v":{"S":"a"}}}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"ExpressionAttributeValues can only be specified when using expressions"}"#,
            ),
            (
                "ev_emptyMap",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"ExpressionAttributeValues":{}}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"ExpressionAttributeValues must not be empty"}"#,
            ),
            (
                "ean_noexpr",
                r##"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"ExpressionAttributeNames":{"#a":"a"}}"##,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"ExpressionAttributeNames can only be specified when using expressions"}"#,
            ),
            (
                "ev_unused",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"SearchConditionExpression":"a = :v","ExpressionAttributeValues":{":v":{"S":"a"},":w":{"S":"b"}}}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Value provided in ExpressionAttributeValues unused in expressions: keys: {:w}"}"#,
            ),
            (
                "ev_projection_only",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"ProjectionExpression":"a","ExpressionAttributeValues":{":v":{"S":"a"}}}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"ExpressionAttributeValues can only be specified when using expressions"}"#,
            ),
            (
                "e_ean_proj",
                r##"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"ProjectionExpression":"#a","ExpressionAttributeNames":{"#a":"a"}}"##,
                400,
                r#"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found"}"#,
            ),
            (
                "e_ean_proj_unused",
                r##"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"ProjectionExpression":"#a","ExpressionAttributeNames":{"#a":"a","#b":"b"}}"##,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Value provided in ExpressionAttributeNames unused in expressions: keys: {#b}"}"#,
            ),
            (
                "e_ean_empty",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"ProjectionExpression":"a","ExpressionAttributeNames":{}}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"ExpressionAttributeNames must not be empty"}"#,
            ),
            (
                "e_ean_empty_noexpr",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"ExpressionAttributeNames":{}}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"ExpressionAttributeNames must not be empty"}"#,
            ),
            (
                "e_ean_badkey",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"ProjectionExpression":"a","ExpressionAttributeNames":{"a":"a"}}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"ExpressionAttributeNames contains invalid key: Syntax error; key: \"a\""}"#,
            ),
            (
                "e_eav_badkey",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"SearchConditionExpression":"a = :v","ExpressionAttributeValues":{"v":{"S":"a"}}}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"ExpressionAttributeValues contains invalid key: Syntax error; key: \"v\""}"#,
            ),
            (
                "e_eav_empty_cond",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"SearchConditionExpression":"a = :v","ExpressionAttributeValues":{}}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"ExpressionAttributeValues must not be empty"}"#,
            ),
            (
                "e_eav_unused_cond",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"SearchConditionExpression":"a = :v","ExpressionAttributeValues":{":v":{"S":"a"},":w":{"S":"b"}}}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Value provided in ExpressionAttributeValues unused in expressions: keys: {:w}"}"#,
            ),
            (
                "e_eav_unused_both",
                r##"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"SearchConditionExpression":"#a = :v","ExpressionAttributeNames":{"#a":"a","#b":"b"},"ExpressionAttributeValues":{":v":{"S":"a"},":w":{"S":"b"}}}"##,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Value provided in ExpressionAttributeNames unused in expressions: keys: {#b}"}"#,
            ),
            (
                "e_ean_unused_cond",
                r##"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"SearchConditionExpression":"#a = :v","ExpressionAttributeNames":{"#a":"a","#b":"b"},"ExpressionAttributeValues":{":v":{"S":"a"}}}"##,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Value provided in ExpressionAttributeNames unused in expressions: keys: {#b}"}"#,
            ),
            (
                "e_eav_undefined",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"SearchConditionExpression":"a = :v"}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Invalid SearchConditionExpression: An expression attribute value used in expression is not defined; attribute value: :v"}"#,
            ),
            (
                "e_eav_noexpr_badkey",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"ExpressionAttributeValues":{"v":{"S":"a"}}}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"ExpressionAttributeValues contains invalid key: Syntax error; key: \"v\""}"#,
            ),
            (
                "e_eav_proj_cond_ok",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"SearchConditionExpression":"a = :v","ProjectionExpression":"a","ExpressionAttributeValues":{":v":{"S":"a"}}}"#,
                400,
                r#"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found"}"#,
            ),
            (
                "v_eanNoExpr_eavNoExpr",
                r##"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"ExpressionAttributeNames":{"#a":"a"},"ExpressionAttributeValues":{":v":{"S":"a"}}}"##,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"ExpressionAttributeNames can only be specified when using expressions"}"#,
            ),
            (
                "rnf",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1}"#,
                400,
                r#"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found"}"#,
            ),
            (
                "idx_short",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"ab","SearchVector":[{"N":"1"}],"TopK":1}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'IndexName' failed to satisfy constraint: Member must have length greater than or equal to 3"}"#,
            ),
            (
                "unknown_member",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"Foo":1}"#,
                400,
                r#"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found"}"#,
            ),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_search_vectors_return_consumed_capacity_is_its_own_check() {
    let (url, _handle) = start_test_server().await;

    // ReturnConsumedCapacity is checked on its own, bare, after the member
    // constraints and the table name and ahead of the TopK ceiling and the
    // search vector. A member constraint beside it is reported alone. Every response is the body DynamoDB's search
    // endpoint gave for the same request (captured eu-west-2 and us-east-1,
    // 23 October 2026, identical apart from columns).
    assert_search_vectors_answers(
        &url,
        &[
            (
                "r_rcc",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"ReturnConsumedCapacity":"BAD"}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Value 'BAD' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"}"#,
            ),
            (
                "r_rcc_lower",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"ReturnConsumedCapacity":"total"}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Value 'total' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"}"#,
            ),
            (
                "r_rcc_badvec",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"S":"a"}],"TopK":1,"ReturnConsumedCapacity":"BAD"}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Value 'BAD' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"}"#,
            ),
            (
                "r_rcc_idxShort",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"ab","SearchVector":[{"N":"1"}],"TopK":1,"ReturnConsumedCapacity":"BAD"}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'IndexName' failed to satisfy constraint: Member must have length greater than or equal to 3"}"#,
            ),
            (
                "r_rcc_topk0",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":0,"ReturnConsumedCapacity":"BAD"}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'TopK' failed to satisfy constraint: Member must have value greater than or equal to 1"}"#,
            ),
            (
                "r_rcc_tblBad",
                r#"{"TableName":"a!","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"ReturnConsumedCapacity":"BAD"}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Value 'a!' at 'tableName' failed to satisfy constraint: Member must have length greater than or equal to 3"}"#,
            ),
            (
                "r_rcc_topk101",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":101,"ReturnConsumedCapacity":"BAD"}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Value 'BAD' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"}"#,
            ),
            (
                "r_rcc_badvec_idxShort",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"ab","SearchVector":[{"S":"a"}],"TopK":1,"ReturnConsumedCapacity":"BAD"}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'IndexName' failed to satisfy constraint: Member must have length greater than or equal to 3"}"#,
            ),
            (
                "r_rcc_emptyVec",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[],"TopK":1,"ReturnConsumedCapacity":"BAD"}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'SearchVector' failed to satisfy constraint: Member must have length greater than or equal to 1"}"#,
            ),
            (
                "r_rcc_eavNoExpr",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"ReturnConsumedCapacity":"BAD","ExpressionAttributeValues":{":v":{"S":"a"}}}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Value 'BAD' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"}"#,
            ),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_search_vectors_top_k_of_the_wrong_type() {
    let (url, _handle) = start_test_server().await;

    // TopK of the wrong JSON type quotes the deserialiser as a
    // SerializationException; an integer beyond i32 is a ValidationException
    // in the same words; a whole number written as a float is read. Every response is the body DynamoDB's search
    // endpoint gave for the same request (captured eu-west-2 and us-east-1,
    // 23 October 2026, identical apart from columns).
    assert_search_vectors_answers(
        &url,
        &[
            (
                "t_str10",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":"10"}"#,
                400,
                r#"{"__type":"com.amazon.coral.service#SerializationException","message":"invalid type: string \"10\", expected i32 at line 1 column 99"}"#,
            ),
            (
                "t_strAbc",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":"abc"}"#,
                400,
                r#"{"__type":"com.amazon.coral.service#SerializationException","message":"invalid type: string \"abc\", expected i32 at line 1 column 100"}"#,
            ),
            (
                "t_str15",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":"1.5"}"#,
                400,
                r#"{"__type":"com.amazon.coral.service#SerializationException","message":"invalid type: string \"1.5\", expected i32 at line 1 column 100"}"#,
            ),
            (
                "t_bool",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":true}"#,
                400,
                r#"{"__type":"com.amazon.coral.service#SerializationException","message":"invalid type: boolean `true`, expected i32 at line 1 column 99"}"#,
            ),
            (
                "t_map",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":{}}"#,
                400,
                r#"{"__type":"com.amazon.coral.service#SerializationException","message":"invalid type: map, expected i32 at line 1 column 95"}"#,
            ),
            (
                "t_seq",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":[]}"#,
                400,
                r#"{"__type":"com.amazon.coral.service#SerializationException","message":"invalid type: sequence, expected i32 at line 1 column 95"}"#,
            ),
            (
                "t_big",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":10000000000}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"invalid value: integer `10000000000`, expected i32 at line 1 column 106"}"#,
            ),
            (
                "t_i32max1",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":2147483648}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"invalid value: integer `2147483648`, expected i32 at line 1 column 105"}"#,
            ),
            (
                "t_neg",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":-1}"#,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'TopK' failed to satisfy constraint: Member must have value greater than or equal to 1"}"#,
            ),
            (
                "t_float_whole",
                r#"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":2}"#,
                400,
                r#"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found"}"#,
            ),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_search_vectors_condition_shape_is_refused_before_unused_names() {
    let (url, _handle) = start_test_server().await;

    // A condition on a nested attribute, or two on one attribute, is refused
    // before an unused name or value is, and before the table is looked up. Every response is the body DynamoDB's search
    // endpoint gave for the same request (captured eu-west-2 and us-east-1,
    // 23 October 2026, identical apart from columns).
    assert_search_vectors_answers(
        &url,
        &[
            (
                "n_nested_unused",
                r##"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"SearchConditionExpression":"#t.deep = :t","ExpressionAttributeNames":{"#t":"tenant","#c":"category"},"ExpressionAttributeValues":{":t":{"S":"t1"},":c":{"S":"c1"}}}"##,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Invalid SearchConditionExpression: SearchConditionExpression cannot have conditions on nested attributes"}"#,
            ),
            (
                "n_nested_only",
                r##"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"SearchConditionExpression":"#t.deep = :t","ExpressionAttributeNames":{"#t":"tenant"},"ExpressionAttributeValues":{":t":{"S":"t1"}}}"##,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Invalid SearchConditionExpression: SearchConditionExpression cannot have conditions on nested attributes"}"#,
            ),
            (
                "n_nested_unusedValue",
                r##"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"SearchConditionExpression":"#t.deep = :t","ExpressionAttributeNames":{"#t":"tenant"},"ExpressionAttributeValues":{":t":{"S":"t1"},":c":{"S":"c1"}}}"##,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Invalid SearchConditionExpression: SearchConditionExpression cannot have conditions on nested attributes"}"#,
            ),
            (
                "n_nested2_unused",
                r##"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"SearchConditionExpression":"#t = :t AND #c.deep = :c","ExpressionAttributeNames":{"#t":"tenant","#c":"category","#z":"z"},"ExpressionAttributeValues":{":t":{"S":"t1"},":c":{"S":"c1"}}}"##,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Invalid SearchConditionExpression: SearchConditionExpression cannot have conditions on nested attributes"}"#,
            ),
            (
                "n_nested_literal_unused",
                r##"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"SearchConditionExpression":"tenant.deep = :t","ExpressionAttributeNames":{"#c":"category"},"ExpressionAttributeValues":{":t":{"S":"t1"}}}"##,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Invalid SearchConditionExpression: SearchConditionExpression cannot have conditions on nested attributes"}"#,
            ),
            (
                "n_index_nested",
                r##"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"SearchConditionExpression":"#t[0] = :t","ExpressionAttributeNames":{"#t":"tenant","#c":"c"},"ExpressionAttributeValues":{":t":{"S":"t1"}}}"##,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Invalid SearchConditionExpression: SearchConditionExpression cannot have conditions on nested attributes"}"#,
            ),
            (
                "n_twice_unused",
                r##"{"TableName":"_capture_20261023_absent_x9","IndexName":"vix","SearchVector":[{"N":"1"}],"TopK":1,"SearchConditionExpression":"#t = :t AND #t = :t","ExpressionAttributeNames":{"#t":"tenant","#c":"c"},"ExpressionAttributeValues":{":t":{"S":"t1"}}}"##,
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Invalid SearchConditionExpression: SearchConditionExpression must only contain one condition per attribute"}"#,
            ),
        ],
    )
    .await;
}
