//! Query's request checks that the Scan captures of 20 October 2026 turned up,
//! request by request, as DynamoDB answers them: `Select` against the
//! projection members and the index, `Limit` read as a 32-bit integer that
//! saturates and drops a fraction, and the starting key on a table with
//! indexes.
//!
//! Each expectation is the answer eu-west-2 gave on 20 October 2026, for a table
//! with a string hash key named `pk` and two global indexes (`gsi`, keys only,
//! on `gk`; `gsi2`, projecting everything, on `gk2`). eu-central-1 and
//! us-east-1 gave the same on every one, so Query is not split between layers
//! here. An empty expectation is a request DynamoDB accepts. The table is data
//! captured from DynamoDB; capture the requests afresh to change it.
//!
//! One request is left out on purpose: a `Limit` that saturates beside
//! `SPECIFIC_ATTRIBUTES` with no projection. DynamoDB answers the whole request
//! on the older layer then, without the envelope; Dynoxide applies the older
//! layer to the member checks only, which the typed SDKs cannot reach.

#![cfg(feature = "http-server")]

use dynoxide::Database;
use dynoxide::actions::create_table::CreateTableRequest;

const TABLE: &str = "QueryMembers";

async fn start_server() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let db = Database::memory().unwrap();
    db.create_table(
        serde_json::from_value::<CreateTableRequest>(serde_json::json!({
            "TableName": TABLE,
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "gk", "AttributeType": "S"},
                {"AttributeName": "gk2", "AttributeType": "S"}
            ],
            "GlobalSecondaryIndexes": [
                {"IndexName": "gsi", "KeySchema": [{"AttributeName": "gk", "KeyType": "HASH"}],
                 "Projection": {"ProjectionType": "KEYS_ONLY"}},
                {"IndexName": "gsi2", "KeySchema": [{"AttributeName": "gk2", "KeyType": "HASH"}],
                 "Projection": {"ProjectionType": "ALL"}}
            ],
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

/// The ValidationException (or SerializationException) message a Query is
/// refused with, or an empty string when it is accepted.
async fn answer(url: &str, body: &serde_json::Value) -> String {
    answer_raw(url, body.to_string()).await
}

/// [`answer`] for a body sent as written, for a number a JSON value can't
/// hold.
async fn answer_raw(url: &str, body: String) -> String {
    let resp = reqwest::Client::new()
        .post(url)
        .header("x-amz-target", "DynamoDB_20120810.Query")
        .header("content-type", "application/x-amz-json-1.0")
        .header(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=fakekey/20260101/us-east-1/dynamodb/aws4_request, \
             SignedHeaders=host;x-amz-date;x-amz-target, Signature=fakesig",
        )
        .header("x-amz-date", "20260101T000000Z")
        .body(body.clone())
        .send()
        .await
        .unwrap();
    if resp.status() == 200 {
        return String::new();
    }
    let text = resp.text().await.unwrap();
    assert!(
        !text.contains("VALIDATION") && !text.contains(" at line "),
        "internal marker or serde position leaked: {text}"
    );
    let parsed: serde_json::Value = serde_json::from_str(&text).unwrap();
    let kind = parsed["__type"].as_str().unwrap();
    assert!(
        kind.ends_with("#ValidationException") || kind.ends_with("#SerializationException"),
        "unexpected __type for {body}: {parsed}"
    );
    parsed["message"]
        .as_str()
        .or_else(|| parsed["Message"].as_str())
        .unwrap()
        .to_string()
}

/// (case, request members, message). A `TableName` of `__absent__` leaves the
/// member out; any other `TableName` replaces the table's.
const CASES: &[(&str, &str, &str)] = &[
    (
        r##"Select COUNT + AttributesToGet"##,
        r##"{"KeyConditions":{"pk":{"ComparisonOperator":"EQ","AttributeValueList":[{"S":"a"}]}},"Select":"COUNT","AttributesToGet":["a"]}"##,
        r##"1 validation error detected: Cannot specify the AttributesToGet when choosing to get only the Count"##,
    ),
    (
        r##"Select ALL_ATTRIBUTES + AttributesToGet"##,
        r##"{"KeyConditions":{"pk":{"ComparisonOperator":"EQ","AttributeValueList":[{"S":"a"}]}},"Select":"ALL_ATTRIBUTES","AttributesToGet":["a"]}"##,
        r##"1 validation error detected: Cannot specify the AttributesToGet when choosing to get ALL_ATTRIBUTES"##,
    ),
    (
        r##"Select SPECIFIC_ATTRIBUTES + AttributesToGet, accepted"##,
        r##"{"KeyConditions":{"pk":{"ComparisonOperator":"EQ","AttributeValueList":[{"S":"a"}]}},"Select":"SPECIFIC_ATTRIBUTES","AttributesToGet":["a"]}"##,
        r##""##,
    ),
    (
        r##"Select COUNT + ProjectionExpression"##,
        r##"{"KeyConditionExpression":"pk = :p","ExpressionAttributeValues":{":p":{"S":"a"}},"Select":"COUNT","ProjectionExpression":"a"}"##,
        r##"1 validation error detected: Cannot specify the ProjectionExpression when choosing to get only the Count"##,
    ),
    (
        r##"Select ALL_ATTRIBUTES + ProjectionExpression"##,
        r##"{"KeyConditionExpression":"pk = :p","ExpressionAttributeValues":{":p":{"S":"a"}},"Select":"ALL_ATTRIBUTES","ProjectionExpression":"a"}"##,
        r##"1 validation error detected: Cannot specify the ProjectionExpression when choosing to get ALL_ATTRIBUTES"##,
    ),
    (
        r##"Select SPECIFIC_ATTRIBUTES, no projection"##,
        r##"{"KeyConditionExpression":"pk = :p","ExpressionAttributeValues":{":p":{"S":"a"}},"Select":"SPECIFIC_ATTRIBUTES"}"##,
        r##"1 validation error detected: Must specify the AttributesToGet or ProjectionExpression when choosing to get SPECIFIC_ATTRIBUTES"##,
    ),
    (
        r##"Select ALL_PROJECTED_ATTRIBUTES, no index"##,
        r##"{"KeyConditionExpression":"pk = :p","ExpressionAttributeValues":{":p":{"S":"a"}},"Select":"ALL_PROJECTED_ATTRIBUTES"}"##,
        r##"1 validation error detected: ALL_PROJECTED_ATTRIBUTES can be used only when Querying using an IndexName"##,
    ),
    (
        r##"gsi: Select ALL_PROJECTED_ATTRIBUTES, accepted"##,
        r##"{"IndexName":"gsi","KeyConditionExpression":"gk = :g","ExpressionAttributeValues":{":g":{"S":"a"}},"Select":"ALL_PROJECTED_ATTRIBUTES"}"##,
        r##""##,
    ),
    (
        r##"gsi: Select ALL_PROJECTED_ATTRIBUTES + AttributesToGet"##,
        r##"{"IndexName":"gsi","KeyConditions":{"gk":{"ComparisonOperator":"EQ","AttributeValueList":[{"S":"a"}]}},"Select":"ALL_PROJECTED_ATTRIBUTES","AttributesToGet":["a"]}"##,
        r##"1 validation error detected: Cannot specify the AttributesToGet when choosing to get ALL_PROJECTED_ATTRIBUTES"##,
    ),
    (
        r##"gsi: Select ALL_PROJECTED_ATTRIBUTES + ProjectionExpression"##,
        r##"{"IndexName":"gsi","KeyConditionExpression":"gk = :g","ExpressionAttributeValues":{":g":{"S":"a"}},"Select":"ALL_PROJECTED_ATTRIBUTES","ProjectionExpression":"a"}"##,
        r##"1 validation error detected: Cannot specify the ProjectionExpression when choosing to get ALL_PROJECTED_ATTRIBUTES"##,
    ),
    (
        r##"gsi: Select ALL_ATTRIBUTES on a keys-only index"##,
        r##"{"IndexName":"gsi","KeyConditionExpression":"gk = :g","ExpressionAttributeValues":{":g":{"S":"a"}},"Select":"ALL_ATTRIBUTES"}"##,
        r##"One or more parameter values were invalid: Select type ALL_ATTRIBUTES is not supported for global secondary index gsi because its projection type is not ALL"##,
    ),
    (
        r##"gsi: Select ALL_ATTRIBUTES on a keys-only index + malformed ExclusiveStartKey"##,
        r##"{"IndexName":"gsi","KeyConditionExpression":"gk = :g","ExpressionAttributeValues":{":g":{"S":"a"}},"Select":"ALL_ATTRIBUTES","ExclusiveStartKey":{"bad":{"S":"p"}}}"##,
        r##"The provided starting key is invalid"##,
    ),
    (
        r##"Select BOGUS"##,
        r##"{"KeyConditionExpression":"pk = :p","ExpressionAttributeValues":{":p":{"S":"a"}},"Select":"BOGUS"}"##,
        r##"1 validation error detected: Value 'BOGUS' at 'select' failed to satisfy constraint: Member must satisfy enum value set: [SPECIFIC_ATTRIBUTES, COUNT, ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES]"##,
    ),
    (
        r##"Select BOGUS + AttributesToGet"##,
        r##"{"KeyConditions":{"pk":{"ComparisonOperator":"EQ","AttributeValueList":[{"S":"a"}]}},"Select":"BOGUS","AttributesToGet":["a"]}"##,
        r##"1 validation error detected: Value 'BOGUS' at 'select' failed to satisfy constraint: Member must satisfy enum value set: [SPECIFIC_ATTRIBUTES, COUNT, ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES]"##,
    ),
    (
        r##"Limit 0"##,
        r##"{"KeyConditionExpression":"pk = :p","ExpressionAttributeValues":{":p":{"S":"a"}},"Limit":0}"##,
        r##"1 validation error detected: Value at 'Limit' failed to satisfy constraint: Member must have value greater than or equal to 1"##,
    ),
    (
        r##"Limit -1"##,
        r##"{"KeyConditionExpression":"pk = :p","ExpressionAttributeValues":{":p":{"S":"a"}},"Limit":-1}"##,
        r##"1 validation error detected: Value at 'Limit' failed to satisfy constraint: Member must have value greater than or equal to 1"##,
    ),
    (
        r##"Limit -2147483649"##,
        r##"{"KeyConditionExpression":"pk = :p","ExpressionAttributeValues":{":p":{"S":"a"}},"Limit":-2147483649}"##,
        r##"1 validation error detected: Value '-2147483648' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1"##,
    ),
    (
        r##"Limit 2147483647, accepted"##,
        r##"{"KeyConditionExpression":"pk = :p","ExpressionAttributeValues":{":p":{"S":"a"}},"Limit":2147483647}"##,
        r##""##,
    ),
    (
        r##"Limit 2147483648"##,
        r##"{"KeyConditionExpression":"pk = :p","ExpressionAttributeValues":{":p":{"S":"a"}},"Limit":2147483648}"##,
        r##""##,
    ),
    (
        r##"Limit 4294967296"##,
        r##"{"KeyConditionExpression":"pk = :p","ExpressionAttributeValues":{":p":{"S":"a"}},"Limit":4294967296}"##,
        r##""##,
    ),
    (
        r##"Limit 9223372036854775808"##,
        r##"{"KeyConditionExpression":"pk = :p","ExpressionAttributeValues":{":p":{"S":"a"}},"Limit":9223372036854776000}"##,
        r##""##,
    ),
    (
        r##"Limit 1.5"##,
        r##"{"KeyConditionExpression":"pk = :p","ExpressionAttributeValues":{":p":{"S":"a"}},"Limit":1.5}"##,
        r##""##,
    ),
    (
        r##"Limit 0.5"##,
        r##"{"KeyConditionExpression":"pk = :p","ExpressionAttributeValues":{":p":{"S":"a"}},"Limit":0.5}"##,
        r##"1 validation error detected: Value '0' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1"##,
    ),
    // Captured eu-west-2 and us-east-1, 6 November 2026: a zero written with
    // a fraction is reported as read.
    (
        r##"Limit 0.0"##,
        r##"{"KeyConditionExpression":"pk = :p","ExpressionAttributeValues":{":p":{"S":"a"}},"Limit":0.0}"##,
        r##"1 validation error detected: Value '0' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1"##,
    ),
    (
        r##"Limit -0.0"##,
        r##"{"KeyConditionExpression":"pk = :p","ExpressionAttributeValues":{":p":{"S":"a"}},"Limit":-0.0}"##,
        r##"1 validation error detected: Value '0' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1"##,
    ),
    (
        r##"Limit as a string"##,
        r##"{"KeyConditionExpression":"pk = :p","ExpressionAttributeValues":{":p":{"S":"a"}},"Limit":"5"}"##,
        r##"STRING_VALUE cannot be converted to Integer"##,
    ),
    (
        r##"Limit 1.5 + Select BOGUS"##,
        r##"{"KeyConditionExpression":"pk = :p","ExpressionAttributeValues":{":p":{"S":"a"}},"Limit":1.5,"Select":"BOGUS"}"##,
        r##"1 validation error detected: Value 'BOGUS' at 'select' failed to satisfy constraint: Member must satisfy enum value set: [SPECIFIC_ATTRIBUTES, COUNT, ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES]"##,
    ),
    (
        r##"Limit 4294967296 + empty TableName"##,
        r##"{"KeyConditionExpression":"pk = :p","ExpressionAttributeValues":{":p":{"S":"a"}},"Limit":4294967296,"TableName":""}"##,
        r##"1 validation error detected: Value '' at 'tableName' failed to satisfy constraint: Member must have length greater than or equal to 1"##,
    ),
    (
        r##"ExclusiveStartKey: table key of the wrong type"##,
        r##"{"KeyConditionExpression":"pk = :p","ExpressionAttributeValues":{":p":{"S":"a"}},"ExclusiveStartKey":{"pk":{"N":"1"}}}"##,
        r##"The provided starting key is invalid"##,
    ),
    (
        r##"ExclusiveStartKey: table key of the wrong type and an index key"##,
        r##"{"KeyConditionExpression":"pk = :p","ExpressionAttributeValues":{":p":{"S":"a"}},"ExclusiveStartKey":{"pk":{"N":"1"},"gk":{"S":"a"}}}"##,
        r##"The provided starting key is invalid"##,
    ),
    (
        r##"ExclusiveStartKey: an index key of the wrong type, no IndexName"##,
        r##"{"KeyConditionExpression":"pk = :p","ExpressionAttributeValues":{":p":{"S":"a"}},"ExclusiveStartKey":{"gk":{"N":"1"}}}"##,
        r##"The provided starting key is invalid"##,
    ),
    (
        r##"ExclusiveStartKey: malformed"##,
        r##"{"KeyConditionExpression":"pk = :p","ExpressionAttributeValues":{":p":{"S":"a"}},"ExclusiveStartKey":{"bad":{"S":"p"}}}"##,
        r##"The provided starting key is invalid"##,
    ),
    (
        r##"ExclusiveStartKey: extra attribute"##,
        r##"{"KeyConditionExpression":"pk = :p","ExpressionAttributeValues":{":p":{"S":"a"}},"ExclusiveStartKey":{"pk":{"S":"a"},"extra":{"S":"b"}}}"##,
        r##"The provided starting key is invalid"##,
    ),
    (
        r##"gsi: ExclusiveStartKey: index key of the wrong type"##,
        r##"{"IndexName":"gsi","KeyConditionExpression":"gk = :g","ExpressionAttributeValues":{":g":{"S":"a"}},"ExclusiveStartKey":{"pk":{"S":"a"},"gk":{"N":"1"}}}"##,
        r##"The provided starting key is invalid"##,
    ),
    (
        r##"gsi: ExclusiveStartKey: table key of the wrong type"##,
        r##"{"IndexName":"gsi","KeyConditionExpression":"gk = :g","ExpressionAttributeValues":{":g":{"S":"a"}},"ExclusiveStartKey":{"pk":{"N":"1"},"gk":{"S":"a"}}}"##,
        r##"The provided starting key is invalid"##,
    ),
    (
        r##"gsi: ExclusiveStartKey: table key only"##,
        r##"{"IndexName":"gsi","KeyConditionExpression":"gk = :g","ExpressionAttributeValues":{":g":{"S":"a"}},"ExclusiveStartKey":{"pk":{"S":"a"}}}"##,
        r##"The provided starting key is invalid"##,
    ),
    (
        r##"gsi: ExclusiveStartKey: malformed"##,
        r##"{"IndexName":"gsi","KeyConditionExpression":"gk = :g","ExpressionAttributeValues":{":g":{"S":"a"}},"ExclusiveStartKey":{"bad":{"S":"p"}}}"##,
        r##"The provided starting key is invalid"##,
    ),
    (
        r##"gsi2: ExclusiveStartKey: table key of the wrong type"##,
        r##"{"KeyConditionExpression":"gk2 = :g","ExpressionAttributeValues":{":g":{"S":"a"}},"IndexName":"gsi2","ExclusiveStartKey":{"pk":{"N":"1"},"gk2":{"S":"a"}}}"##,
        r##"The provided starting key is invalid"##,
    ),
    (
        r##"absent TableName + ReturnConsumedCapacity BOGUS"##,
        r##"{"KeyConditionExpression":"pk = :p","ExpressionAttributeValues":{":p":{"S":"a"}},"TableName":"__absent__","ReturnConsumedCapacity":"BOGUS"}"##,
        r##"2 validation errors detected: Value 'BOGUS' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]; Value null at 'tableName' failed to satisfy constraint: Member must not be null"##,
    ),
    (
        r##"absent TableName + Limit 0 + ReturnConsumedCapacity BOGUS"##,
        r##"{"KeyConditionExpression":"pk = :p","ExpressionAttributeValues":{":p":{"S":"a"}},"TableName":"__absent__","Limit":0,"ReturnConsumedCapacity":"BOGUS"}"##,
        r##"3 validation errors detected: Value 'BOGUS' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]; Value null at 'tableName' failed to satisfy constraint: Member must not be null; Value '0' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1"##,
    ),
    (
        r##"absent TableName + Limit 0 + Select BOGUS + ReturnConsumedCapacity BOGUS"##,
        r##"{"KeyConditionExpression":"pk = :p","ExpressionAttributeValues":{":p":{"S":"a"}},"TableName":"__absent__","Limit":0,"Select":"BOGUS","ReturnConsumedCapacity":"BOGUS"}"##,
        r##"4 validation errors detected: Value 'BOGUS' at 'select' failed to satisfy constraint: Member must satisfy enum value set: [SPECIFIC_ATTRIBUTES, COUNT, ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES]; Value 'BOGUS' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]; Value null at 'tableName' failed to satisfy constraint: Member must not be null; Value '0' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1"##,
    ),
    (
        r##"Limit 1.5 + ReturnConsumedCapacity BOGUS"##,
        r##"{"KeyConditionExpression":"pk = :p","ExpressionAttributeValues":{":p":{"S":"a"}},"Limit":1.5,"ReturnConsumedCapacity":"BOGUS"}"##,
        r##"1 validation error detected: Value 'BOGUS' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"##,
    ),
    (
        r##"Limit 0.5 + ReturnConsumedCapacity BOGUS"##,
        r##"{"KeyConditionExpression":"pk = :p","ExpressionAttributeValues":{":p":{"S":"a"}},"Limit":0.5,"ReturnConsumedCapacity":"BOGUS"}"##,
        r##"2 validation errors detected: Value 'BOGUS' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]; Value '0' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1"##,
    ),
    (
        r##"Limit 0.5 + Select BOGUS + ReturnConsumedCapacity BOGUS"##,
        r##"{"KeyConditionExpression":"pk = :p","ExpressionAttributeValues":{":p":{"S":"a"}},"Limit":0.5,"Select":"BOGUS","ReturnConsumedCapacity":"BOGUS"}"##,
        r##"3 validation errors detected: Value 'BOGUS' at 'select' failed to satisfy constraint: Member must satisfy enum value set: [SPECIFIC_ATTRIBUTES, COUNT, ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES]; Value 'BOGUS' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]; Value '0' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1"##,
    ),
];

#[tokio::test]
async fn each_request_is_answered_as_dynamodb_answers_it() {
    assert_eq!(CASES.len(), 44, "a case was added or lost");
    let url = start_server().await;
    let mut wrong = Vec::new();
    for (case, members, expected) in CASES {
        let mut body = serde_json::json!({"TableName": TABLE});
        for (key, value) in serde_json::from_str::<serde_json::Value>(members)
            .unwrap()
            .as_object()
            .unwrap()
        {
            if key == "TableName" && value == "__absent__" {
                body.as_object_mut().unwrap().remove("TableName");
            } else {
                body[key] = value.clone();
            }
        }
        let got = answer(&url, &body).await;
        if got != *expected {
            wrong.push(format!("{case}\n  want: {expected}\n  got:  {got}"));
        }
    }
    assert!(
        wrong.is_empty(),
        "{} of {} requests answered differently:\n{}",
        wrong.len(),
        CASES.len(),
        wrong.join("\n")
    );
}

/// A `Limit` past f64's range saturates, read as written (captured eu-west-2
/// and us-east-1, 6 November 2026).
#[tokio::test]
async fn a_limit_past_the_range_of_a_double_saturates() {
    let url = start_server().await;
    let body = |limit: &str| {
        format!(
            r#"{{"TableName":"{TABLE}","KeyConditionExpression":"pk = :p","ExpressionAttributeValues":{{":p":{{"S":"a"}}}},"Limit":{limit}}}"#
        )
    };
    assert_eq!(answer_raw(&url, body("1e400")).await, "");
    assert_eq!(
        answer_raw(&url, body("-1e400")).await,
        "1 validation error detected: Value '-2147483648' at 'limit' failed to satisfy \
         constraint: Member must have value greater than or equal to 1"
    );
}

/// A request built in code gets the wire answer for the checks it can express.
#[test]
fn a_request_built_in_code_is_answered_as_the_wire_request_is() {
    use dynoxide::actions::query::QueryRequest;

    let wire = |case: &str| {
        CASES
            .iter()
            .find(|(name, _, _)| *name == case)
            .unwrap_or_else(|| panic!("no case called {case}"))
            .2
    };
    let db = Database::memory().unwrap();
    db.create_table(
        serde_json::from_value::<CreateTableRequest>(serde_json::json!({
            "TableName": TABLE,
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "BillingMode": "PAY_PER_REQUEST"
        }))
        .unwrap(),
    )
    .unwrap();
    let legacy = || {
        Some(
            serde_json::json!({"pk": {"ComparisonOperator": "EQ", "AttributeValueList": [{"S": "a"}]}}),
        )
    };
    let kce = |request: QueryRequest| {
        let mut values = std::collections::HashMap::new();
        values.insert(
            ":p".to_string(),
            dynoxide::types::AttributeValue::S("a".into()),
        );
        QueryRequest {
            table_name: TABLE.into(),
            key_condition_expression: Some("pk = :p".into()),
            expression_attribute_values: Some(values),
            ..request
        }
    };
    let cases: Vec<(&str, QueryRequest)> = vec![
        (
            "Limit 0",
            kce(QueryRequest {
                limit: Some(0),
                ..Default::default()
            }),
        ),
        (
            "Select COUNT + AttributesToGet",
            QueryRequest {
                table_name: TABLE.into(),
                key_conditions: legacy(),
                select: Some("COUNT".into()),
                attributes_to_get: Some(vec!["a".into()]),
                ..Default::default()
            },
        ),
        (
            "Select ALL_ATTRIBUTES + ProjectionExpression",
            kce(QueryRequest {
                select: Some("ALL_ATTRIBUTES".into()),
                projection_expression: Some("a".into()),
                ..Default::default()
            }),
        ),
        (
            "Select ALL_PROJECTED_ATTRIBUTES, no index",
            kce(QueryRequest {
                select: Some("ALL_PROJECTED_ATTRIBUTES".into()),
                ..Default::default()
            }),
        ),
        (
            "Select SPECIFIC_ATTRIBUTES, no projection",
            kce(QueryRequest {
                select: Some("SPECIFIC_ATTRIBUTES".into()),
                ..Default::default()
            }),
        ),
    ];
    let mut wrong = Vec::new();
    for (case, request) in cases {
        let got = db.query(request).unwrap_err().to_string();
        if got != wire(case) {
            wrong.push(format!("{case}\n  want: {}\n  got:  {got}", wire(case)));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
