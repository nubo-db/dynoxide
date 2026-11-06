//! Scan's request-member checks, request by request, as DynamoDB answers them
//! in the newer validation layer: which error is reported, in which wording,
//! and which errors come back together.
//!
//! Each expectation is the answer eu-west-2 gave on 20 October 2026, for a table
//! with a string hash key named `pk` and two global indexes (`gsi`, keys only,
//! on `gk`; `gsi2`, projecting everything, on `gk2`); eu-central-1 gave the same
//! on every one.
//! The layer reached 20 of the 33 observed regions by then, and the code
//! follows the majority. The requests run in ten rounds, each looking at the
//! order between two stages of the check, or at an edge of one, so the table
//! is long on purpose. An empty expectation is a request DynamoDB accepts.
//!
//! The tables are data captured from DynamoDB, not written by hand. When the
//! layer moves again, capture the same requests afresh and replace the tables
//! whole rather than editing single expectations.
//!
//! Requests with a `Limit` that saturates or loses a fraction beside a check
//! made after the members are left out on purpose. DynamoDB puts such a request
//! on the older layer whole and answers every later check in its wording;
//! Dynoxide applies the older layer to the member checks only, which the typed
//! SDKs cannot reach. The same checks with an ordinary `Limit` are pinned.

#![cfg(feature = "http-server")]

use dynoxide::Database;
use dynoxide::actions::create_table::CreateTableRequest;

const TABLE: &str = "ScanMembers";

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

/// The ValidationException (or SerializationException) message a Scan is refused with, or an empty string
/// when it is accepted.
async fn answer(url: &str, body: &serde_json::Value) -> String {
    answer_raw(url, body.to_string()).await
}

/// [`answer`] for a body sent as written, for a number a JSON value can't
/// hold.
async fn answer_raw(url: &str, body: String) -> String {
    let resp = reqwest::Client::new()
        .post(url)
        .header("x-amz-target", "DynamoDB_20120810.Scan")
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
        r##"Segment >= TotalSegments"##,
        r##"{"Segment":5,"TotalSegments":5}"##,
        r##"The Segment parameter is zero-based and must be less than parameter TotalSegments"##,
    ),
    (
        r##"Limit 0"##,
        r##"{"Limit":0}"##,
        r##"1 validation error detected: Value at 'Limit' failed to satisfy constraint: Member must have value greater than or equal to 1"##,
    ),
    (
        r##"negative Segment"##,
        r##"{"Segment":-1,"TotalSegments":4}"##,
        r##"1 validation error detected: Value at 'Segment' failed to satisfy constraint: Member must have value greater than or equal to 0"##,
    ),
    (
        r##"malformed ExclusiveStartKey"##,
        r##"{"ExclusiveStartKey":{"bad":{"S":"p"}}}"##,
        r##"The provided starting key is invalid"##,
    ),
    (
        r##"SPECIFIC_ATTRIBUTES, no projection"##,
        r##"{"Select":"SPECIFIC_ATTRIBUTES"}"##,
        r##"1 validation error detected: Must specify the AttributesToGet or ProjectionExpression when choosing to get SPECIFIC_ATTRIBUTES"##,
    ),
    (
        r##"Segment without TotalSegments (control)"##,
        r##"{"Segment":0}"##,
        r##"The TotalSegments parameter is required but was not present in the request when Segment parameter is present"##,
    ),
    (
        r##"TotalSegments without Segment (control)"##,
        r##"{"TotalSegments":4}"##,
        r##"The Segment parameter is required but was not present in the request when parameter TotalSegments is present"##,
    ),
    (
        r##"Limit 0 + negative Segment"##,
        r##"{"Limit":0,"Segment":-1,"TotalSegments":4}"##,
        r##"2 validation errors detected: Value at 'Limit' failed to satisfy constraint: Member must have value greater than or equal to 1; Value at 'Segment' failed to satisfy constraint: Member must have value greater than or equal to 0"##,
    ),
    (
        r##"Limit 0 + Select BOGUS"##,
        r##"{"Limit":0,"Select":"BOGUS"}"##,
        r##"1 validation error detected: Value at 'Limit' failed to satisfy constraint: Member must have value greater than or equal to 1"##,
    ),
    (
        r##"Limit 0 + ReturnConsumedCapacity BOGUS"##,
        r##"{"Limit":0,"ReturnConsumedCapacity":"BOGUS"}"##,
        r##"1 validation error detected: Value at 'Limit' failed to satisfy constraint: Member must have value greater than or equal to 1"##,
    ),
    (
        r##"Limit 0 + ConditionalOperator BOGUS"##,
        r##"{"Limit":0,"ConditionalOperator":"BOGUS"}"##,
        r##"1 validation error detected: Value at 'Limit' failed to satisfy constraint: Member must have value greater than or equal to 1"##,
    ),
    (
        r##"all member errors at once"##,
        r##"{"Limit":0,"Segment":-1,"TotalSegments":4,"Select":"BOGUS","ReturnConsumedCapacity":"BOGUS","ConditionalOperator":"BOGUS"}"##,
        r##"2 validation errors detected: Value at 'Limit' failed to satisfy constraint: Member must have value greater than or equal to 1; Value at 'Segment' failed to satisfy constraint: Member must have value greater than or equal to 0"##,
    ),
    (
        r##"Limit 0 + empty TableName"##,
        r##"{"Limit":0,"TableName":""}"##,
        r##"2 validation errors detected: Value at 'Limit' failed to satisfy constraint: Member must have value greater than or equal to 1; Value at 'TableName' failed to satisfy constraint: Member must have length greater than or equal to 1"##,
    ),
    (
        r##"Limit 0 + Segment >= TotalSegments"##,
        r##"{"Limit":0,"Segment":5,"TotalSegments":5}"##,
        r##"1 validation error detected: Value at 'Limit' failed to satisfy constraint: Member must have value greater than or equal to 1"##,
    ),
    (
        r##"negative Segment, no TotalSegments"##,
        r##"{"Segment":-1}"##,
        r##"1 validation error detected: Value at 'Segment' failed to satisfy constraint: Member must have value greater than or equal to 0"##,
    ),
    (
        r##"Limit 0 + SPECIFIC_ATTRIBUTES, no projection"##,
        r##"{"Limit":0,"Select":"SPECIFIC_ATTRIBUTES"}"##,
        r##"1 validation error detected: Value at 'Limit' failed to satisfy constraint: Member must have value greater than or equal to 1"##,
    ),
    (
        r##"Limit 0 + malformed ExclusiveStartKey"##,
        r##"{"Limit":0,"ExclusiveStartKey":{"bad":{"S":"p"}}}"##,
        r##"1 validation error detected: Value at 'Limit' failed to satisfy constraint: Member must have value greater than or equal to 1"##,
    ),
    (
        r##"Segment >= TotalSegments + SPECIFIC_ATTRIBUTES"##,
        r##"{"Segment":5,"TotalSegments":5,"Select":"SPECIFIC_ATTRIBUTES"}"##,
        r##"1 validation error detected: Must specify the AttributesToGet or ProjectionExpression when choosing to get SPECIFIC_ATTRIBUTES"##,
    ),
    (
        r##"Segment >= TotalSegments + malformed ExclusiveStartKey"##,
        r##"{"Segment":5,"TotalSegments":5,"ExclusiveStartKey":{"bad":{"S":"p"}}}"##,
        r##"The Segment parameter is zero-based and must be less than parameter TotalSegments"##,
    ),
    (
        r##"SPECIFIC_ATTRIBUTES + malformed ExclusiveStartKey"##,
        r##"{"Select":"SPECIFIC_ATTRIBUTES","ExclusiveStartKey":{"bad":{"S":"p"}}}"##,
        r##"1 validation error detected: Must specify the AttributesToGet or ProjectionExpression when choosing to get SPECIFIC_ATTRIBUTES"##,
    ),
    (
        r##"ExclusiveStartKey right name, wrong type"##,
        r##"{"ExclusiveStartKey":{"pk":{"N":"1"}}}"##,
        r##"One or more parameter values were invalid: Type mismatch for Index Key pk Expected: S Actual: N IndexName: gsi"##,
    ),
    (
        r##"ExclusiveStartKey empty map"##,
        r##"{"ExclusiveStartKey":{}}"##,
        r##"The provided starting key is invalid"##,
    ),
    (
        r##"ExclusiveStartKey with an extra attribute"##,
        r##"{"ExclusiveStartKey":{"pk":{"S":"a"},"extra":{"S":"b"}}}"##,
        r##"The provided starting key is invalid"##,
    ),
    (
        r##"Select BOGUS"##,
        r##"{"Select":"BOGUS"}"##,
        r##"1 validation error detected: Value 'BOGUS' at 'select' failed to satisfy constraint: Member must satisfy enum value set: [SPECIFIC_ATTRIBUTES, COUNT, ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES]"##,
    ),
    (
        r##"ReturnConsumedCapacity BOGUS"##,
        r##"{"ReturnConsumedCapacity":"BOGUS"}"##,
        r##"1 validation error detected: Value 'BOGUS' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"##,
    ),
    (
        r##"ConditionalOperator BOGUS"##,
        r##"{"ConditionalOperator":"BOGUS"}"##,
        r##"ConditionalOperator cannot be used without Filter or Expected"##,
    ),
    (
        r##"Select BOGUS + ReturnConsumedCapacity BOGUS"##,
        r##"{"Select":"BOGUS","ReturnConsumedCapacity":"BOGUS"}"##,
        r##"1 validation error detected: Value 'BOGUS' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"##,
    ),
    (
        r##"Select BOGUS + empty TableName"##,
        r##"{"Select":"BOGUS","TableName":""}"##,
        r##"1 validation error detected: Value at 'TableName' failed to satisfy constraint: Member must have length greater than or equal to 1"##,
    ),
    (
        r##"Select BOGUS + Segment >= TotalSegments"##,
        r##"{"Select":"BOGUS","Segment":5,"TotalSegments":5}"##,
        r##"1 validation error detected: Value 'BOGUS' at 'select' failed to satisfy constraint: Member must satisfy enum value set: [SPECIFIC_ATTRIBUTES, COUNT, ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES]"##,
    ),
    (
        r##"Select BOGUS + malformed ExclusiveStartKey"##,
        r##"{"Select":"BOGUS","ExclusiveStartKey":{"bad":{"S":"p"}}}"##,
        r##"1 validation error detected: Value 'BOGUS' at 'select' failed to satisfy constraint: Member must satisfy enum value set: [SPECIFIC_ATTRIBUTES, COUNT, ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES]"##,
    ),
    (
        r##"ReturnConsumedCapacity BOGUS + SPECIFIC_ATTRIBUTES, no projection"##,
        r##"{"ReturnConsumedCapacity":"BOGUS","Select":"SPECIFIC_ATTRIBUTES"}"##,
        r##"1 validation error detected: Value 'BOGUS' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"##,
    ),
    (
        r##"ReturnConsumedCapacity BOGUS + negative Segment"##,
        r##"{"ReturnConsumedCapacity":"BOGUS","Segment":-1,"TotalSegments":4}"##,
        r##"1 validation error detected: Value at 'Segment' failed to satisfy constraint: Member must have value greater than or equal to 0"##,
    ),
    (
        r##"ConditionalOperator BOGUS + Segment >= TotalSegments"##,
        r##"{"ConditionalOperator":"BOGUS","Segment":5,"TotalSegments":5}"##,
        r##"ConditionalOperator cannot be used without Filter or Expected"##,
    ),
    (
        r##"Select BOGUS + TotalSegments 0"##,
        r##"{"Select":"BOGUS","Segment":0,"TotalSegments":0}"##,
        r##"1 validation error detected: Value at 'TotalSegments' failed to satisfy constraint: Member must have value greater than or equal to 1"##,
    ),
    (
        r##"empty TableName"##,
        r##"{"TableName":""}"##,
        r##"1 validation error detected: Value at 'TableName' failed to satisfy constraint: Member must have length greater than or equal to 1"##,
    ),
    (
        r##"TableName with a bad character, a!"##,
        r##"{"TableName":"a!"}"##,
        r##"1 validation error detected: Value 'a!' at 'tableName' failed to satisfy constraint: Member must have length greater than or equal to 3"##,
    ),
    (
        r##"TableName too short, ab"##,
        r##"{"TableName":"ab"}"##,
        r##"1 validation error detected: Value 'ab' at 'tableName' failed to satisfy constraint: Member must have length greater than or equal to 3"##,
    ),
    (
        r##"TableName too long"##,
        r##"{"TableName":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}"##,
        r##"1 validation error detected: Value 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' at 'tableName' failed to satisfy constraint: Member must have length less than or equal to 255"##,
    ),
    (
        r##"TableName absent"##,
        r##"{"TableName":"__absent__"}"##,
        r##"1 validation error detected: Value null at 'tableName' failed to satisfy constraint: Member must not be null"##,
    ),
    (
        r##"Limit 0 + TableName a!"##,
        r##"{"Limit":0,"TableName":"a!"}"##,
        r##"1 validation error detected: Value at 'Limit' failed to satisfy constraint: Member must have value greater than or equal to 1"##,
    ),
    (
        r##"Limit 0 + TableName ab"##,
        r##"{"Limit":0,"TableName":"ab"}"##,
        r##"1 validation error detected: Value at 'Limit' failed to satisfy constraint: Member must have value greater than or equal to 1"##,
    ),
    (
        r##"Limit 0 + TableName absent"##,
        r##"{"Limit":0,"TableName":"__absent__"}"##,
        r##"2 validation errors detected: Value null at 'tableName' failed to satisfy constraint: Member must not be null; Value '0' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1"##,
    ),
    (
        r##"negative Segment + empty TableName"##,
        r##"{"Segment":-1,"TotalSegments":4,"TableName":""}"##,
        r##"2 validation errors detected: Value at 'Segment' failed to satisfy constraint: Member must have value greater than or equal to 0; Value at 'TableName' failed to satisfy constraint: Member must have length greater than or equal to 1"##,
    ),
    (
        r##"Segment >= TotalSegments + empty TableName"##,
        r##"{"Segment":5,"TotalSegments":5,"TableName":""}"##,
        r##"1 validation error detected: Value at 'TableName' failed to satisfy constraint: Member must have length greater than or equal to 1"##,
    ),
    (
        r##"negative Segment + TotalSegments 0"##,
        r##"{"Segment":-1,"TotalSegments":0}"##,
        r##"2 validation errors detected: Value at 'Segment' failed to satisfy constraint: Member must have value greater than or equal to 0; Value at 'TotalSegments' failed to satisfy constraint: Member must have value greater than or equal to 1"##,
    ),
    (
        r##"Segment 5 + TotalSegments 0"##,
        r##"{"Segment":5,"TotalSegments":0}"##,
        r##"1 validation error detected: Value at 'TotalSegments' failed to satisfy constraint: Member must have value greater than or equal to 1"##,
    ),
    (
        r##"Limit 0 + TotalSegments 0"##,
        r##"{"Limit":0,"Segment":0,"TotalSegments":0}"##,
        r##"2 validation errors detected: Value at 'Limit' failed to satisfy constraint: Member must have value greater than or equal to 1; Value at 'TotalSegments' failed to satisfy constraint: Member must have value greater than or equal to 1"##,
    ),
    (
        r##"TotalSegments 0 without Segment"##,
        r##"{"TotalSegments":0}"##,
        r##"1 validation error detected: Value at 'TotalSegments' failed to satisfy constraint: Member must have value greater than or equal to 1"##,
    ),
    (
        r##"ScanFilter condition with no ComparisonOperator"##,
        r##"{"ScanFilter":{"a":{"AttributeValueList":[{"S":"x"}]}}}"##,
        r##"1 validation error detected: Value null at 'scanFilter.a.member.comparisonOperator' failed to satisfy constraint: Member must not be null"##,
    ),
    (
        r##"Limit 0 + ScanFilter with no ComparisonOperator"##,
        r##"{"Limit":0,"ScanFilter":{"a":{"AttributeValueList":[{"S":"x"}]}}}"##,
        r##"2 validation errors detected: Value null at 'scanFilter.a.member.comparisonOperator' failed to satisfy constraint: Member must not be null; Value '0' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1"##,
    ),
    (
        r##"ScanFilter ComparisonOperator BOGUS"##,
        r##"{"ScanFilter":{"a":{"ComparisonOperator":"BOGUS","AttributeValueList":[{"S":"x"}]}}}"##,
        r##"One or more parameter values were invalid: Invalid comparison operator"##,
    ),
    (
        r##"absent TableName + every member error"##,
        r##"{"TableName":"__absent__","Select":"BOGUS","ReturnConsumedCapacity":"BOGUS","ConditionalOperator":"BOGUS","Segment":-1,"TotalSegments":0,"Limit":0}"##,
        r##"7 validation errors detected: Value 'BOGUS' at 'select' failed to satisfy constraint: Member must satisfy enum value set: [SPECIFIC_ATTRIBUTES, COUNT, ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES]; Value '0' at 'totalSegments' failed to satisfy constraint: Member must have value greater than or equal to 1; Value 'BOGUS' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]; Value null at 'tableName' failed to satisfy constraint: Member must not be null; Value 'BOGUS' at 'conditionalOperator' failed to satisfy constraint: Member must satisfy enum value set: [OR, AND]; Value '-1' at 'segment' failed to satisfy constraint: Member must have value greater than or equal to 0; Value '0' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1"##,
    ),
    (
        r##"absent TableName + Select BOGUS"##,
        r##"{"TableName":"__absent__","Select":"BOGUS"}"##,
        r##"2 validation errors detected: Value 'BOGUS' at 'select' failed to satisfy constraint: Member must satisfy enum value set: [SPECIFIC_ATTRIBUTES, COUNT, ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES]; Value null at 'tableName' failed to satisfy constraint: Member must not be null"##,
    ),
    (
        r##"absent TableName + ReturnConsumedCapacity BOGUS"##,
        r##"{"TableName":"__absent__","ReturnConsumedCapacity":"BOGUS"}"##,
        r##"2 validation errors detected: Value 'BOGUS' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]; Value null at 'tableName' failed to satisfy constraint: Member must not be null"##,
    ),
    (
        r##"absent TableName + ConditionalOperator BOGUS"##,
        r##"{"TableName":"__absent__","ConditionalOperator":"BOGUS"}"##,
        r##"2 validation errors detected: Value null at 'tableName' failed to satisfy constraint: Member must not be null; Value 'BOGUS' at 'conditionalOperator' failed to satisfy constraint: Member must satisfy enum value set: [OR, AND]"##,
    ),
    (
        r##"absent TableName + negative Segment"##,
        r##"{"TableName":"__absent__","Segment":-1,"TotalSegments":4}"##,
        r##"2 validation errors detected: Value null at 'tableName' failed to satisfy constraint: Member must not be null; Value '-1' at 'segment' failed to satisfy constraint: Member must have value greater than or equal to 0"##,
    ),
    (
        r##"absent TableName + TotalSegments 0"##,
        r##"{"TableName":"__absent__","Segment":0,"TotalSegments":0}"##,
        r##"2 validation errors detected: Value '0' at 'totalSegments' failed to satisfy constraint: Member must have value greater than or equal to 1; Value null at 'tableName' failed to satisfy constraint: Member must not be null"##,
    ),
    (
        r##"absent TableName + Segment >= TotalSegments"##,
        r##"{"TableName":"__absent__","Segment":5,"TotalSegments":5}"##,
        r##"1 validation error detected: Value null at 'tableName' failed to satisfy constraint: Member must not be null"##,
    ),
    (
        r##"absent TableName + SPECIFIC_ATTRIBUTES, no projection"##,
        r##"{"TableName":"__absent__","Select":"SPECIFIC_ATTRIBUTES"}"##,
        r##"1 validation error detected: Value null at 'tableName' failed to satisfy constraint: Member must not be null"##,
    ),
    (
        r##"absent TableName + ScanFilter with no ComparisonOperator"##,
        r##"{"TableName":"__absent__","ScanFilter":{"a":{"AttributeValueList":[{"S":"x"}]}}}"##,
        r##"2 validation errors detected: Value null at 'tableName' failed to satisfy constraint: Member must not be null; Value null at 'scanFilter.a.member.comparisonOperator' failed to satisfy constraint: Member must not be null"##,
    ),
    (
        r##"ScanFilter with no ComparisonOperator + every member error"##,
        r##"{"ScanFilter":{"a":{"AttributeValueList":[{"S":"x"}]}},"Select":"BOGUS","ReturnConsumedCapacity":"BOGUS","ConditionalOperator":"BOGUS","Segment":-1,"TotalSegments":0,"Limit":0}"##,
        r##"7 validation errors detected: Value 'BOGUS' at 'select' failed to satisfy constraint: Member must satisfy enum value set: [SPECIFIC_ATTRIBUTES, COUNT, ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES]; Value '0' at 'totalSegments' failed to satisfy constraint: Member must have value greater than or equal to 1; Value 'BOGUS' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]; Value 'BOGUS' at 'conditionalOperator' failed to satisfy constraint: Member must satisfy enum value set: [OR, AND]; Value null at 'scanFilter.a.member.comparisonOperator' failed to satisfy constraint: Member must not be null; Value '-1' at 'segment' failed to satisfy constraint: Member must have value greater than or equal to 0; Value '0' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1"##,
    ),
    (
        r##"ScanFilter with no ComparisonOperator + Select BOGUS"##,
        r##"{"ScanFilter":{"a":{"AttributeValueList":[{"S":"x"}]}},"Select":"BOGUS"}"##,
        r##"2 validation errors detected: Value 'BOGUS' at 'select' failed to satisfy constraint: Member must satisfy enum value set: [SPECIFIC_ATTRIBUTES, COUNT, ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES]; Value null at 'scanFilter.a.member.comparisonOperator' failed to satisfy constraint: Member must not be null"##,
    ),
    (
        r##"ScanFilter with no ComparisonOperator + ReturnConsumedCapacity BOGUS"##,
        r##"{"ScanFilter":{"a":{"AttributeValueList":[{"S":"x"}]}},"ReturnConsumedCapacity":"BOGUS"}"##,
        r##"2 validation errors detected: Value 'BOGUS' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]; Value null at 'scanFilter.a.member.comparisonOperator' failed to satisfy constraint: Member must not be null"##,
    ),
    (
        r##"ScanFilter with no ComparisonOperator + ConditionalOperator BOGUS"##,
        r##"{"ScanFilter":{"a":{"AttributeValueList":[{"S":"x"}]}},"ConditionalOperator":"BOGUS"}"##,
        r##"2 validation errors detected: Value 'BOGUS' at 'conditionalOperator' failed to satisfy constraint: Member must satisfy enum value set: [OR, AND]; Value null at 'scanFilter.a.member.comparisonOperator' failed to satisfy constraint: Member must not be null"##,
    ),
    (
        r##"ScanFilter with no ComparisonOperator + negative Segment"##,
        r##"{"ScanFilter":{"a":{"AttributeValueList":[{"S":"x"}]}},"Segment":-1,"TotalSegments":4}"##,
        r##"2 validation errors detected: Value null at 'scanFilter.a.member.comparisonOperator' failed to satisfy constraint: Member must not be null; Value '-1' at 'segment' failed to satisfy constraint: Member must have value greater than or equal to 0"##,
    ),
    (
        r##"ScanFilter with no ComparisonOperator + TotalSegments 0"##,
        r##"{"ScanFilter":{"a":{"AttributeValueList":[{"S":"x"}]}},"Segment":0,"TotalSegments":0}"##,
        r##"2 validation errors detected: Value '0' at 'totalSegments' failed to satisfy constraint: Member must have value greater than or equal to 1; Value null at 'scanFilter.a.member.comparisonOperator' failed to satisfy constraint: Member must not be null"##,
    ),
    (
        r##"ScanFilter with no ComparisonOperator + empty TableName"##,
        r##"{"ScanFilter":{"a":{"AttributeValueList":[{"S":"x"}]}},"TableName":""}"##,
        r##"2 validation errors detected: Value '' at 'tableName' failed to satisfy constraint: Member must have length greater than or equal to 1; Value null at 'scanFilter.a.member.comparisonOperator' failed to satisfy constraint: Member must not be null"##,
    ),
    (
        r##"Select BOGUS + TableName ab"##,
        r##"{"Select":"BOGUS","TableName":"ab"}"##,
        r##"1 validation error detected: Value 'ab' at 'tableName' failed to satisfy constraint: Member must have length greater than or equal to 3"##,
    ),
    (
        r##"ReturnConsumedCapacity BOGUS + TableName ab"##,
        r##"{"ReturnConsumedCapacity":"BOGUS","TableName":"ab"}"##,
        r##"2 validation errors detected: Value 'ab' at 'tableName' failed to satisfy constraint: Member must have length greater than or equal to 3; Value 'BOGUS' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"##,
    ),
    (
        r##"Segment >= TotalSegments + TableName ab"##,
        r##"{"Segment":5,"TotalSegments":5,"TableName":"ab"}"##,
        r##"1 validation error detected: Value 'ab' at 'tableName' failed to satisfy constraint: Member must have length greater than or equal to 3"##,
    ),
    (
        r##"SPECIFIC_ATTRIBUTES, no projection + TableName ab"##,
        r##"{"Select":"SPECIFIC_ATTRIBUTES","TableName":"ab"}"##,
        r##"1 validation error detected: Value 'ab' at 'tableName' failed to satisfy constraint: Member must have length greater than or equal to 3"##,
    ),
    (
        r##"ConditionalOperator BOGUS + TableName ab"##,
        r##"{"ConditionalOperator":"BOGUS","TableName":"ab"}"##,
        r##"1 validation error detected: Value 'ab' at 'tableName' failed to satisfy constraint: Member must have length greater than or equal to 3"##,
    ),
    (
        r##"negative Segment + TableName ab"##,
        r##"{"Segment":-1,"TotalSegments":4,"TableName":"ab"}"##,
        r##"1 validation error detected: Value at 'Segment' failed to satisfy constraint: Member must have value greater than or equal to 0"##,
    ),
    (
        r##"ConditionalOperator AND, no filter"##,
        r##"{"ConditionalOperator":"AND"}"##,
        r##"ConditionalOperator cannot be used without Filter or Expected"##,
    ),
    (
        r##"ConditionalOperator AND, no filter + Select BOGUS"##,
        r##"{"ConditionalOperator":"AND","Select":"BOGUS"}"##,
        r##"ConditionalOperator cannot be used without Filter or Expected"##,
    ),
    (
        r##"ConditionalOperator AND, no filter + SPECIFIC_ATTRIBUTES, no projection"##,
        r##"{"ConditionalOperator":"AND","Select":"SPECIFIC_ATTRIBUTES"}"##,
        r##"ConditionalOperator cannot be used without Filter or Expected"##,
    ),
    (
        r##"ConditionalOperator AND, no filter + Segment >= TotalSegments"##,
        r##"{"ConditionalOperator":"AND","Segment":5,"TotalSegments":5}"##,
        r##"ConditionalOperator cannot be used without Filter or Expected"##,
    ),
    (
        r##"ConditionalOperator BOGUS with a ScanFilter"##,
        r##"{"ConditionalOperator":"BOGUS","ScanFilter":{"a":{"ComparisonOperator":"EQ","AttributeValueList":[{"S":"x"}]}}}"##,
        r##"ConditionalOperator can only be used when Filter or Expected has two or more elements"##,
    ),
    (
        r##"ConditionalOperator BOGUS with a ScanFilter + Select BOGUS"##,
        r##"{"ConditionalOperator":"BOGUS","ScanFilter":{"a":{"ComparisonOperator":"EQ","AttributeValueList":[{"S":"x"}]}},"Select":"BOGUS"}"##,
        r##"ConditionalOperator can only be used when Filter or Expected has two or more elements"##,
    ),
    (
        r##"ConditionalOperator BOGUS with a ScanFilter + ReturnConsumedCapacity BOGUS"##,
        r##"{"ConditionalOperator":"BOGUS","ScanFilter":{"a":{"ComparisonOperator":"EQ","AttributeValueList":[{"S":"x"}]}},"ReturnConsumedCapacity":"BOGUS"}"##,
        r##"1 validation error detected: Value 'BOGUS' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"##,
    ),
    (
        r##"ConditionalOperator BOGUS with a ScanFilter + SPECIFIC_ATTRIBUTES, no projection"##,
        r##"{"ConditionalOperator":"BOGUS","ScanFilter":{"a":{"ComparisonOperator":"EQ","AttributeValueList":[{"S":"x"}]}},"Select":"SPECIFIC_ATTRIBUTES"}"##,
        r##"ConditionalOperator can only be used when Filter or Expected has two or more elements"##,
    ),
    (
        r##"ConditionalOperator BOGUS with a ScanFilter + Segment >= TotalSegments"##,
        r##"{"ConditionalOperator":"BOGUS","ScanFilter":{"a":{"ComparisonOperator":"EQ","AttributeValueList":[{"S":"x"}]}},"Segment":5,"TotalSegments":5}"##,
        r##"ConditionalOperator can only be used when Filter or Expected has two or more elements"##,
    ),
    (
        r##"ConditionalOperator BOGUS with a ScanFilter + negative Segment"##,
        r##"{"ConditionalOperator":"BOGUS","ScanFilter":{"a":{"ComparisonOperator":"EQ","AttributeValueList":[{"S":"x"}]}},"Segment":-1,"TotalSegments":4}"##,
        r##"1 validation error detected: Value at 'Segment' failed to satisfy constraint: Member must have value greater than or equal to 0"##,
    ),
    (
        r##"ScanFilter ComparisonOperator BOGUS + Limit 0"##,
        r##"{"Limit":0,"ScanFilter":{"a":{"ComparisonOperator":"BOGUS","AttributeValueList":[{"S":"x"}]}}}"##,
        r##"1 validation error detected: Value at 'Limit' failed to satisfy constraint: Member must have value greater than or equal to 1"##,
    ),
    (
        r##"ScanFilter ComparisonOperator BOGUS + Select BOGUS"##,
        r##"{"Select":"BOGUS","ScanFilter":{"a":{"ComparisonOperator":"BOGUS","AttributeValueList":[{"S":"x"}]}}}"##,
        r##"One or more parameter values were invalid: Invalid comparison operator"##,
    ),
    (
        r##"ScanFilter ComparisonOperator BOGUS + Segment >= TotalSegments"##,
        r##"{"Segment":5,"TotalSegments":5,"ScanFilter":{"a":{"ComparisonOperator":"BOGUS","AttributeValueList":[{"S":"x"}]}}}"##,
        r##"One or more parameter values were invalid: Invalid comparison operator"##,
    ),
    (
        r##"ReturnConsumedCapacity BOGUS + Select BOGUS"##,
        r##"{"ReturnConsumedCapacity":"BOGUS","Select":"BOGUS"}"##,
        r##"1 validation error detected: Value 'BOGUS' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"##,
    ),
    (
        r##"ConditionalOperator BOGUS with a two-element ScanFilter"##,
        r##"{"ConditionalOperator":"BOGUS","ScanFilter":{"a":{"ComparisonOperator":"EQ","AttributeValueList":[{"S":"x"}]},"b":{"ComparisonOperator":"EQ","AttributeValueList":[{"S":"y"}]}}}"##,
        r##"Failed to satisfy constraint: Member must satisfy enum value set: [ALL, OR]"##,
    ),
    (
        r##"ConditionalOperator AND with a two-element ScanFilter (valid)"##,
        r##"{"ConditionalOperator":"AND","ScanFilter":{"a":{"ComparisonOperator":"EQ","AttributeValueList":[{"S":"x"}]},"b":{"ComparisonOperator":"EQ","AttributeValueList":[{"S":"y"}]}}}"##,
        r##""##,
    ),
    (
        r##"ConditionalOperator AND with a one-element ScanFilter"##,
        r##"{"ConditionalOperator":"AND","ScanFilter":{"a":{"ComparisonOperator":"EQ","AttributeValueList":[{"S":"x"}]}}}"##,
        r##"ConditionalOperator can only be used when Filter or Expected has two or more elements"##,
    ),
    (
        r##"ConditionalOperator BOGUS with a two-element ScanFilter + Select BOGUS"##,
        r##"{"ConditionalOperator":"BOGUS","ScanFilter":{"a":{"ComparisonOperator":"EQ","AttributeValueList":[{"S":"x"}]},"b":{"ComparisonOperator":"EQ","AttributeValueList":[{"S":"y"}]}},"Select":"BOGUS"}"##,
        r##"Failed to satisfy constraint: Member must satisfy enum value set: [ALL, OR]"##,
    ),
    (
        r##"ConditionalOperator AND + FilterExpression"##,
        r##"{"ConditionalOperator":"AND","FilterExpression":"a = :v","ExpressionAttributeValues":{":v":{"S":"x"}}}"##,
        r##"Can not use both expression and non-expression parameters in the same request: Non-expression parameters: {ConditionalOperator} Expression parameters: {FilterExpression}"##,
    ),
    (
        r##"ScanFilter + FilterExpression"##,
        r##"{"ScanFilter":{"a":{"ComparisonOperator":"EQ","AttributeValueList":[{"S":"x"}]}},"FilterExpression":"a = :v","ExpressionAttributeValues":{":v":{"S":"x"}}}"##,
        r##"Can not use both expression and non-expression parameters in the same request: Non-expression parameters: {ScanFilter} Expression parameters: {FilterExpression}"##,
    ),
    (
        r##"ScanFilter + FilterExpression + Select BOGUS"##,
        r##"{"ScanFilter":{"a":{"ComparisonOperator":"EQ","AttributeValueList":[{"S":"x"}]}},"FilterExpression":"a = :v","ExpressionAttributeValues":{":v":{"S":"x"}},"Select":"BOGUS"}"##,
        r##"Can not use both expression and non-expression parameters in the same request: Non-expression parameters: {ScanFilter} Expression parameters: {FilterExpression}"##,
    ),
    (
        r##"AttributesToGet + ProjectionExpression"##,
        r##"{"AttributesToGet":["a"],"ProjectionExpression":"a"}"##,
        r##"Can not use both expression and non-expression parameters in the same request: Non-expression parameters: {AttributesToGet} Expression parameters: {ProjectionExpression}"##,
    ),
    (
        r##"AttributesToGet + ProjectionExpression + Select BOGUS"##,
        r##"{"AttributesToGet":["a"],"ProjectionExpression":"a","Select":"BOGUS"}"##,
        r##"Can not use both expression and non-expression parameters in the same request: Non-expression parameters: {AttributesToGet} Expression parameters: {ProjectionExpression}"##,
    ),
    (
        r##"AttributesToGet + ProjectionExpression + SPECIFIC_ATTRIBUTES"##,
        r##"{"AttributesToGet":["a"],"ProjectionExpression":"a","Select":"SPECIFIC_ATTRIBUTES"}"##,
        r##"Can not use both expression and non-expression parameters in the same request: Non-expression parameters: {AttributesToGet} Expression parameters: {ProjectionExpression}"##,
    ),
    (
        r##"FilterExpression + ConditionalOperator BOGUS"##,
        r##"{"ConditionalOperator":"BOGUS","FilterExpression":"a = :v","ExpressionAttributeValues":{":v":{"S":"x"}}}"##,
        r##"Can not use both expression and non-expression parameters in the same request: Non-expression parameters: {ConditionalOperator} Expression parameters: {FilterExpression}"##,
    ),
    (
        r##"ScanFilter op BOGUS + ReturnConsumedCapacity BOGUS"##,
        r##"{"ScanFilter":{"a":{"ComparisonOperator":"BOGUS","AttributeValueList":[{"S":"x"}]}},"ReturnConsumedCapacity":"BOGUS"}"##,
        r##"1 validation error detected: Value 'BOGUS' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"##,
    ),
    (
        r##"ScanFilter op BOGUS + TableName ab"##,
        r##"{"ScanFilter":{"a":{"ComparisonOperator":"BOGUS","AttributeValueList":[{"S":"x"}]}},"TableName":"ab"}"##,
        r##"1 validation error detected: Value 'ab' at 'tableName' failed to satisfy constraint: Member must have length greater than or equal to 3"##,
    ),
    (
        r##"ScanFilter op BOGUS + ConditionalOperator AND (one element)"##,
        r##"{"ScanFilter":{"a":{"ComparisonOperator":"BOGUS","AttributeValueList":[{"S":"x"}]}},"ConditionalOperator":"AND"}"##,
        r##"ConditionalOperator can only be used when Filter or Expected has two or more elements"##,
    ),
    (
        r##"ScanFilter op BOGUS + SPECIFIC_ATTRIBUTES, no projection"##,
        r##"{"ScanFilter":{"a":{"ComparisonOperator":"BOGUS","AttributeValueList":[{"S":"x"}]}},"Select":"SPECIFIC_ATTRIBUTES"}"##,
        r##"One or more parameter values were invalid: Invalid comparison operator"##,
    ),
    (
        r##"ScanFilter op BOGUS + negative Segment"##,
        r##"{"ScanFilter":{"a":{"ComparisonOperator":"BOGUS","AttributeValueList":[{"S":"x"}]}},"Segment":-1,"TotalSegments":4}"##,
        r##"1 validation error detected: Value at 'Segment' failed to satisfy constraint: Member must have value greater than or equal to 0"##,
    ),
    (
        r##"ScanFilter with no ComparisonOperator + TableName a!"##,
        r##"{"ScanFilter":{"a":{"AttributeValueList":[{"S":"x"}]}},"TableName":"a!"}"##,
        r##"1 validation error detected: Value null at 'scanFilter.a.member.comparisonOperator' failed to satisfy constraint: Member must not be null"##,
    ),
    (
        r##"ScanFilter with no ComparisonOperator + TableName ab"##,
        r##"{"ScanFilter":{"a":{"AttributeValueList":[{"S":"x"}]}},"TableName":"ab"}"##,
        r##"1 validation error detected: Value null at 'scanFilter.a.member.comparisonOperator' failed to satisfy constraint: Member must not be null"##,
    ),
    (
        r##"TableName abc!"##,
        r##"{"TableName":"abc!"}"##,
        r##"1 validation error detected: Value 'abc!' at 'tableName' failed to satisfy constraint: Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+"##,
    ),
    (
        r##"TableName too long and a bad character"##,
        r##"{"TableName":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa!"}"##,
        r##"1 validation error detected: Value 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa!' at 'tableName' failed to satisfy constraint: Member must have length less than or equal to 255"##,
    ),
    (
        r##"TableName abc! + ReturnConsumedCapacity BOGUS"##,
        r##"{"TableName":"abc!","ReturnConsumedCapacity":"BOGUS"}"##,
        r##"2 validation errors detected: Value 'abc!' at 'tableName' failed to satisfy constraint: Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+; Value 'BOGUS' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"##,
    ),
    (
        r##"TotalSegments 1000001 + negative Segment"##,
        r##"{"Segment":-1,"TotalSegments":1000001}"##,
        r##"2 validation errors detected: Value at 'Segment' failed to satisfy constraint: Member must have value greater than or equal to 0; Value at 'TotalSegments' failed to satisfy constraint: Member must have value less than or equal to 1000000"##,
    ),
    (
        r##"Limit 0 + TotalSegments 1000001"##,
        r##"{"Limit":0,"Segment":0,"TotalSegments":1000001}"##,
        r##"2 validation errors detected: Value at 'Limit' failed to satisfy constraint: Member must have value greater than or equal to 1; Value at 'TotalSegments' failed to satisfy constraint: Member must have value less than or equal to 1000000"##,
    ),
    (
        r##"Segment 0 without TotalSegments + SPECIFIC_ATTRIBUTES, no projection"##,
        r##"{"Segment":0,"Select":"SPECIFIC_ATTRIBUTES"}"##,
        r##"1 validation error detected: Must specify the AttributesToGet or ProjectionExpression when choosing to get SPECIFIC_ATTRIBUTES"##,
    ),
    (
        r##"Segment 0 without TotalSegments + Select BOGUS"##,
        r##"{"Segment":0,"Select":"BOGUS"}"##,
        r##"1 validation error detected: Value 'BOGUS' at 'select' failed to satisfy constraint: Member must satisfy enum value set: [SPECIFIC_ATTRIBUTES, COUNT, ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES]"##,
    ),
    (
        r##"Segment 0 without TotalSegments + ReturnConsumedCapacity BOGUS"##,
        r##"{"Segment":0,"ReturnConsumedCapacity":"BOGUS"}"##,
        r##"1 validation error detected: Value 'BOGUS' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"##,
    ),
    (
        r##"Segment 0 without TotalSegments + malformed ExclusiveStartKey"##,
        r##"{"Segment":0,"ExclusiveStartKey":{"bad":{"S":"p"}}}"##,
        r##"The TotalSegments parameter is required but was not present in the request when Segment parameter is present"##,
    ),
    (
        r##"TotalSegments 4 without Segment + SPECIFIC_ATTRIBUTES, no projection"##,
        r##"{"TotalSegments":4,"Select":"SPECIFIC_ATTRIBUTES"}"##,
        r##"1 validation error detected: Must specify the AttributesToGet or ProjectionExpression when choosing to get SPECIFIC_ATTRIBUTES"##,
    ),
    (
        r##"Select COUNT + ProjectionExpression"##,
        r##"{"Select":"COUNT","ProjectionExpression":"a"}"##,
        r##"1 validation error detected: Cannot specify the ProjectionExpression when choosing to get only the Count"##,
    ),
    (
        r##"Select COUNT + ProjectionExpression + Segment >= TotalSegments"##,
        r##"{"Select":"COUNT","ProjectionExpression":"a","Segment":5,"TotalSegments":5}"##,
        r##"1 validation error detected: Cannot specify the ProjectionExpression when choosing to get only the Count"##,
    ),
    (
        r##"ProjectionExpression + Segment >= TotalSegments"##,
        r##"{"ProjectionExpression":"a","Segment":5,"TotalSegments":5}"##,
        r##"The Segment parameter is zero-based and must be less than parameter TotalSegments"##,
    ),
    (
        r##"invalid FilterExpression syntax"##,
        r##"{"FilterExpression":"a = = :v","ExpressionAttributeValues":{":v":{"S":"x"}}}"##,
        r##"Invalid FilterExpression: Syntax error; token: "=", near: "= = :v""##,
    ),
    (
        r##"invalid FilterExpression syntax + Select BOGUS"##,
        r##"{"FilterExpression":"a = = :v","ExpressionAttributeValues":{":v":{"S":"x"}},"Select":"BOGUS"}"##,
        r##"Invalid FilterExpression: Syntax error; token: "=", near: "= = :v""##,
    ),
    (
        r##"invalid FilterExpression syntax + SPECIFIC_ATTRIBUTES, no projection"##,
        r##"{"FilterExpression":"a = = :v","ExpressionAttributeValues":{":v":{"S":"x"}},"Select":"SPECIFIC_ATTRIBUTES"}"##,
        r##"Invalid FilterExpression: Syntax error; token: "=", near: "= = :v""##,
    ),
    (
        r##"invalid FilterExpression syntax + Segment >= TotalSegments"##,
        r##"{"FilterExpression":"a = = :v","ExpressionAttributeValues":{":v":{"S":"x"}},"Segment":5,"TotalSegments":5}"##,
        r##"Invalid FilterExpression: Syntax error; token: "=", near: "= = :v""##,
    ),
    (
        r##"invalid FilterExpression syntax + Select COUNT + ProjectionExpression"##,
        r##"{"FilterExpression":"a = = :v","ExpressionAttributeValues":{":v":{"S":"x"}},"Select":"COUNT","ProjectionExpression":"a"}"##,
        r##"Invalid FilterExpression: Syntax error; token: "=", near: "= = :v""##,
    ),
    (
        r##"empty ProjectionExpression + SPECIFIC_ATTRIBUTES"##,
        r##"{"ProjectionExpression":"","Select":"SPECIFIC_ATTRIBUTES"}"##,
        r##"Invalid ProjectionExpression: The expression can not be empty;"##,
    ),
    (
        r##"empty ProjectionExpression + Select BOGUS"##,
        r##"{"ProjectionExpression":"","Select":"BOGUS"}"##,
        r##"Invalid ProjectionExpression: The expression can not be empty;"##,
    ),
    (
        r##"empty ProjectionExpression + Segment >= TotalSegments"##,
        r##"{"ProjectionExpression":"","Segment":5,"TotalSegments":5}"##,
        r##"Invalid ProjectionExpression: The expression can not be empty;"##,
    ),
    (
        r##"empty FilterExpression + Select BOGUS"##,
        r##"{"FilterExpression":"","Select":"BOGUS"}"##,
        r##"Invalid FilterExpression: The expression can not be empty;"##,
    ),
    (
        r##"empty FilterExpression + Segment >= TotalSegments"##,
        r##"{"FilterExpression":"","Segment":5,"TotalSegments":5}"##,
        r##"Invalid FilterExpression: The expression can not be empty;"##,
    ),
    (
        r##"undefined name in FilterExpression + Select BOGUS"##,
        r##"{"FilterExpression":"#n = :v","ExpressionAttributeValues":{":v":{"S":"x"}},"Select":"BOGUS"}"##,
        r##"Invalid FilterExpression: An expression attribute name used in the document path is not defined; attribute name: #n"##,
    ),
    (
        r##"undefined name in FilterExpression + Segment >= TotalSegments"##,
        r##"{"FilterExpression":"#n = :v","ExpressionAttributeValues":{":v":{"S":"x"}},"Segment":5,"TotalSegments":5}"##,
        r##"Invalid FilterExpression: An expression attribute name used in the document path is not defined; attribute name: #n"##,
    ),
    (
        r##"duplicate AttributesToGet + Select BOGUS"##,
        r##"{"AttributesToGet":["a","a"],"Select":"BOGUS"}"##,
        r##"One or more parameter values were invalid: Duplicate value in attribute name: a"##,
    ),
    (
        r##"duplicate AttributesToGet + Segment >= TotalSegments"##,
        r##"{"AttributesToGet":["a","a"],"Segment":5,"TotalSegments":5}"##,
        r##"One or more parameter values were invalid: Duplicate value in attribute name: a"##,
    ),
    (
        r##"Select COUNT + AttributesToGet"##,
        r##"{"Select":"COUNT","AttributesToGet":["a"]}"##,
        r##"1 validation error detected: Cannot specify the AttributesToGet when choosing to get only the Count"##,
    ),
    (
        r##"Select ALL_ATTRIBUTES + AttributesToGet"##,
        r##"{"Select":"ALL_ATTRIBUTES","AttributesToGet":["a"]}"##,
        r##"1 validation error detected: Cannot specify the AttributesToGet when choosing to get ALL_ATTRIBUTES"##,
    ),
    (
        r##"ProjectionExpression + Select ALL_ATTRIBUTES"##,
        r##"{"ProjectionExpression":"a","Select":"ALL_ATTRIBUTES"}"##,
        r##"1 validation error detected: Cannot specify the ProjectionExpression when choosing to get ALL_ATTRIBUTES"##,
    ),
    (
        r##"empty AttributesToGet name"##,
        r##"{"AttributesToGet":[""]}"##,
        r##"Empty attribute name"##,
    ),
    (
        r##"empty AttributesToGet name + ConditionalOperator BOGUS, no filter"##,
        r##"{"AttributesToGet":[""],"ConditionalOperator":"BOGUS"}"##,
        r##"Empty attribute name"##,
    ),
    (
        r##"empty AttributesToGet name + ConditionalOperator AND, no filter"##,
        r##"{"AttributesToGet":[""],"ConditionalOperator":"AND"}"##,
        r##"Empty attribute name"##,
    ),
    (
        r##"empty AttributesToGet name + ConditionalOperator BOGUS, two-element ScanFilter"##,
        r##"{"AttributesToGet":[""],"ConditionalOperator":"BOGUS","ScanFilter":{"a":{"ComparisonOperator":"EQ","AttributeValueList":[{"S":"x"}]},"b":{"ComparisonOperator":"EQ","AttributeValueList":[{"S":"y"}]}}}"##,
        r##"Empty attribute name"##,
    ),
    (
        r##"Select ALL_PROJECTED_ATTRIBUTES, no index"##,
        r##"{"Select":"ALL_PROJECTED_ATTRIBUTES"}"##,
        r##"1 validation error detected: ALL_PROJECTED_ATTRIBUTES can be used only when Querying using an IndexName"##,
    ),
    (
        r##"Select ALL_PROJECTED_ATTRIBUTES + AttributesToGet"##,
        r##"{"Select":"ALL_PROJECTED_ATTRIBUTES","AttributesToGet":["a"]}"##,
        r##"1 validation error detected: Cannot specify the AttributesToGet when choosing to get ALL_PROJECTED_ATTRIBUTES"##,
    ),
    (
        r##"Select ALL_PROJECTED_ATTRIBUTES + ProjectionExpression"##,
        r##"{"Select":"ALL_PROJECTED_ATTRIBUTES","ProjectionExpression":"a"}"##,
        r##"1 validation error detected: Cannot specify the ProjectionExpression when choosing to get ALL_PROJECTED_ATTRIBUTES"##,
    ),
    (
        r##"Select ALL_PROJECTED_ATTRIBUTES + Segment >= TotalSegments"##,
        r##"{"Select":"ALL_PROJECTED_ATTRIBUTES","Segment":5,"TotalSegments":5}"##,
        r##"1 validation error detected: ALL_PROJECTED_ATTRIBUTES can be used only when Querying using an IndexName"##,
    ),
    (
        r##"TotalSegments 1, accepted"##,
        r##"{"Segment":0,"TotalSegments":1}"##,
        r##""##,
    ),
    (
        r##"TotalSegments 1000000, accepted"##,
        r##"{"Segment":0,"TotalSegments":1000000}"##,
        r##""##,
    ),
    (
        r##"absent TableName + TotalSegments 1000001"##,
        r##"{"TableName":"__absent__","Segment":0,"TotalSegments":1000001}"##,
        r##"2 validation errors detected: Value '1000001' at 'totalSegments' failed to satisfy constraint: Member must have value less than or equal to 1000000; Value null at 'tableName' failed to satisfy constraint: Member must not be null"##,
    ),
    (
        r##"SPECIFIC_ATTRIBUTES with AttributesToGet, accepted"##,
        r##"{"Select":"SPECIFIC_ATTRIBUTES","AttributesToGet":["a"]}"##,
        r##""##,
    ),
    (
        r##"SPECIFIC_ATTRIBUTES with ProjectionExpression, accepted"##,
        r##"{"Select":"SPECIFIC_ATTRIBUTES","ProjectionExpression":"a"}"##,
        r##""##,
    ),
    (
        r##"Select COUNT, accepted"##,
        r##"{"Select":"COUNT"}"##,
        r##""##,
    ),
    (
        r##"index: no such index"##,
        r##"{"IndexName":"nope"}"##,
        r##"The table does not have the specified index: nope"##,
    ),
    (
        r##"index: ALL_PROJECTED_ATTRIBUTES, accepted"##,
        r##"{"IndexName":"gsi","Select":"ALL_PROJECTED_ATTRIBUTES"}"##,
        r##""##,
    ),
    (
        r##"index: ALL_PROJECTED_ATTRIBUTES + AttributesToGet"##,
        r##"{"IndexName":"gsi","Select":"ALL_PROJECTED_ATTRIBUTES","AttributesToGet":["a"]}"##,
        r##"1 validation error detected: Cannot specify the AttributesToGet when choosing to get ALL_PROJECTED_ATTRIBUTES"##,
    ),
    (
        r##"index: ALL_PROJECTED_ATTRIBUTES + ProjectionExpression"##,
        r##"{"IndexName":"gsi","Select":"ALL_PROJECTED_ATTRIBUTES","ProjectionExpression":"a"}"##,
        r##"1 validation error detected: Cannot specify the ProjectionExpression when choosing to get ALL_PROJECTED_ATTRIBUTES"##,
    ),
    (
        r##"index: ALL_ATTRIBUTES on a keys-only index"##,
        r##"{"IndexName":"gsi","Select":"ALL_ATTRIBUTES"}"##,
        r##"One or more parameter values were invalid: Select type ALL_ATTRIBUTES is not supported for global secondary index gsi because its projection type is not ALL"##,
    ),
    (
        r##"index: ALL_ATTRIBUTES on a keys-only index + malformed ExclusiveStartKey"##,
        r##"{"IndexName":"gsi","Select":"ALL_ATTRIBUTES","ExclusiveStartKey":{"bad":{"S":"p"}}}"##,
        r##"One or more parameter values were invalid: Select type ALL_ATTRIBUTES is not supported for global secondary index gsi because its projection type is not ALL"##,
    ),
    (
        r##"index: SPECIFIC_ATTRIBUTES, no projection"##,
        r##"{"IndexName":"gsi","Select":"SPECIFIC_ATTRIBUTES"}"##,
        r##"1 validation error detected: Must specify the AttributesToGet or ProjectionExpression when choosing to get SPECIFIC_ATTRIBUTES"##,
    ),
    (
        r##"index: Segment >= TotalSegments"##,
        r##"{"IndexName":"gsi","Segment":5,"TotalSegments":5}"##,
        r##"The Segment parameter is zero-based and must be less than parameter TotalSegments"##,
    ),
    (
        r##"index: malformed ExclusiveStartKey"##,
        r##"{"IndexName":"gsi","ExclusiveStartKey":{"bad":{"S":"p"}}}"##,
        r##"The provided starting key is invalid"##,
    ),
    (
        r##"index: ExclusiveStartKey with the table key only"##,
        r##"{"IndexName":"gsi","ExclusiveStartKey":{"pk":{"S":"a"}}}"##,
        r##"The provided starting key is invalid"##,
    ),
    (
        r##"index: ExclusiveStartKey with the index key only"##,
        r##"{"IndexName":"gsi","ExclusiveStartKey":{"gk":{"S":"a"}}}"##,
        r##"The provided starting key is invalid"##,
    ),
    (
        r##"index: ExclusiveStartKey with a wrong-typed index key"##,
        r##"{"IndexName":"gsi","ExclusiveStartKey":{"pk":{"S":"a"},"gk":{"N":"1"}}}"##,
        r##"One or more parameter values were invalid: Type mismatch for Index Key gk Expected: S Actual: N IndexName: gsi"##,
    ),
    (
        r##"index: ExclusiveStartKey with an extra attribute"##,
        r##"{"IndexName":"gsi","ExclusiveStartKey":{"pk":{"S":"a"},"gk":{"S":"b"},"extra":{"S":"c"}}}"##,
        r##"The provided starting key is invalid"##,
    ),
    (
        r##"Segment 4294967296 of 1"##,
        r##"{"Segment":4294967296,"TotalSegments":1}"##,
        r##"1 validation error detected: Value '2147483647' at 'segment' failed to satisfy constraint: Member must have value less than or equal to 999999"##,
    ),
    (
        r##"Segment 2147483648 of 5"##,
        r##"{"Segment":2147483648,"TotalSegments":5}"##,
        r##"1 validation error detected: Value '2147483647' at 'segment' failed to satisfy constraint: Member must have value less than or equal to 999999"##,
    ),
    (
        r##"Segment 2147483647 of 5"##,
        r##"{"Segment":2147483647,"TotalSegments":5}"##,
        r##"1 validation error detected: Value at 'Segment' failed to satisfy constraint: Member must have value less than or equal to 999999"##,
    ),
    (
        r##"Segment -2147483649 of 5"##,
        r##"{"Segment":-2147483649,"TotalSegments":5}"##,
        r##"1 validation error detected: Value '-2147483648' at 'segment' failed to satisfy constraint: Member must have value greater than or equal to 0"##,
    ),
    (
        r##"Segment 1.5 of 5"##,
        r##"{"Segment":1.5,"TotalSegments":5}"##,
        r##""##,
    ),
    (
        r##"TotalSegments 4294967296"##,
        r##"{"Segment":0,"TotalSegments":4294967296}"##,
        r##"1 validation error detected: Value '2147483647' at 'totalSegments' failed to satisfy constraint: Member must have value less than or equal to 1000000"##,
    ),
    (
        r##"TotalSegments 2147483648"##,
        r##"{"Segment":0,"TotalSegments":2147483648}"##,
        r##"1 validation error detected: Value '2147483647' at 'totalSegments' failed to satisfy constraint: Member must have value less than or equal to 1000000"##,
    ),
    (
        r##"TotalSegments -1"##,
        r##"{"Segment":0,"TotalSegments":-1}"##,
        r##"1 validation error detected: Value at 'TotalSegments' failed to satisfy constraint: Member must have value greater than or equal to 1"##,
    ),
    (
        r##"TotalSegments 1.5"##,
        r##"{"Segment":0,"TotalSegments":1.5}"##,
        r##""##,
    ),
    (
        r##"TotalSegments 4294967296 without Segment"##,
        r##"{"TotalSegments":4294967296}"##,
        r##"1 validation error detected: Value '2147483647' at 'totalSegments' failed to satisfy constraint: Member must have value less than or equal to 1000000"##,
    ),
    (
        r##"Limit 9223372036854775808"##,
        r##"{"Limit":9223372036854776000}"##,
        r##""##,
    ),
    (
        r##"Limit 4294967296"##,
        r##"{"Limit":4294967296}"##,
        r##""##,
    ),
    (
        r##"Limit 2147483648"##,
        r##"{"Limit":2147483648}"##,
        r##""##,
    ),
    (
        r##"Limit 2147483647, accepted"##,
        r##"{"Limit":2147483647}"##,
        r##""##,
    ),
    (
        r##"Limit -1"##,
        r##"{"Limit":-1}"##,
        r##"1 validation error detected: Value at 'Limit' failed to satisfy constraint: Member must have value greater than or equal to 1"##,
    ),
    (r##"Limit 1.5"##, r##"{"Limit":1.5}"##, r##""##),
    (
        r##"Limit as a string"##,
        r##"{"Limit":"5"}"##,
        r##"STRING_VALUE cannot be converted to Integer"##,
    ),
    (
        r##"Limit -1 + TotalSegments 4294967296"##,
        r##"{"Limit":-1,"Segment":0,"TotalSegments":4294967296}"##,
        r##"2 validation errors detected: Value '2147483647' at 'totalSegments' failed to satisfy constraint: Member must have value less than or equal to 1000000; Value '-1' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1"##,
    ),
    (
        r##"Limit 4294967296 + negative Segment"##,
        r##"{"Limit":4294967296,"Segment":-1,"TotalSegments":4}"##,
        r##"1 validation error detected: Value '-1' at 'segment' failed to satisfy constraint: Member must have value greater than or equal to 0"##,
    ),
    (
        r##"Limit 4294967296 + Select BOGUS"##,
        r##"{"Limit":4294967296,"Select":"BOGUS"}"##,
        r##"1 validation error detected: Value 'BOGUS' at 'select' failed to satisfy constraint: Member must satisfy enum value set: [SPECIFIC_ATTRIBUTES, COUNT, ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES]"##,
    ),
    (
        r##"Limit 4294967296 + empty TableName"##,
        r##"{"Limit":4294967296,"TableName":""}"##,
        r##"1 validation error detected: Value '' at 'tableName' failed to satisfy constraint: Member must have length greater than or equal to 1"##,
    ),
    (
        r##"Segment 999999 of 1000000, accepted"##,
        r##"{"Segment":999999,"TotalSegments":1000000}"##,
        r##""##,
    ),
    (
        r##"Segment 1000000 of 1000000"##,
        r##"{"Segment":1000000,"TotalSegments":1000000}"##,
        r##"1 validation error detected: Value at 'Segment' failed to satisfy constraint: Member must have value less than or equal to 999999"##,
    ),
    (
        r##"Limit 0.5"##,
        r##"{"Limit":0.5}"##,
        r##"1 validation error detected: Value '0' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1"##,
    ),
    (
        r##"Segment -0.5 of 5, accepted"##,
        r##"{"Segment":-0.5,"TotalSegments":5}"##,
        r##""##,
    ),
    (
        r##"Limit 0 + TotalSegments 2147483648"##,
        r##"{"Limit":0,"Segment":0,"TotalSegments":2147483648}"##,
        r##"2 validation errors detected: Value '2147483647' at 'totalSegments' failed to satisfy constraint: Member must have value less than or equal to 1000000; Value '0' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1"##,
    ),
    (
        r##"TotalSegments 4294967296 + empty TableName"##,
        r##"{"Segment":0,"TotalSegments":4294967296,"TableName":""}"##,
        r##"2 validation errors detected: Value '2147483647' at 'totalSegments' failed to satisfy constraint: Member must have value less than or equal to 1000000; Value '' at 'tableName' failed to satisfy constraint: Member must have length greater than or equal to 1"##,
    ),
    (
        r##"Limit -2147483649"##,
        r##"{"Limit":-2147483649}"##,
        r##"1 validation error detected: Value '-2147483648' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1"##,
    ),
    (
        r##"table key of the wrong type, no IndexName, two indexes"##,
        r##"{"ExclusiveStartKey":{"pk":{"N":"1"}}}"##,
        r##"One or more parameter values were invalid: Type mismatch for Index Key pk Expected: S Actual: N IndexName: gsi"##,
    ),
    (
        r##"gsi: table key of the wrong type"##,
        r##"{"IndexName":"gsi","ExclusiveStartKey":{"pk":{"N":"1"},"gk":{"S":"a"}}}"##,
        r##"One or more parameter values were invalid: Type mismatch for Index Key pk Expected: S Actual: N IndexName: gsi"##,
    ),
    (
        r##"gsi2: table key of the wrong type"##,
        r##"{"IndexName":"gsi2","ExclusiveStartKey":{"pk":{"N":"1"},"gk2":{"S":"a"}}}"##,
        r##"One or more parameter values were invalid: Type mismatch for Index Key pk Expected: S Actual: N IndexName: gsi2"##,
    ),
    (
        r##"gsi2: index key of the wrong type"##,
        r##"{"IndexName":"gsi2","ExclusiveStartKey":{"pk":{"S":"a"},"gk2":{"N":"1"}}}"##,
        r##"One or more parameter values were invalid: Type mismatch for Index Key gk2 Expected: S Actual: N IndexName: gsi2"##,
    ),
    (
        r##"table key of the wrong type and an index key present, no IndexName"##,
        r##"{"ExclusiveStartKey":{"pk":{"N":"1"},"gk":{"S":"a"}}}"##,
        r##"One or more parameter values were invalid: Type mismatch for Index Key pk Expected: S Actual: N IndexName: gsi"##,
    ),
    (
        r##"a non-key attribute of the wrong type, no IndexName"##,
        r##"{"ExclusiveStartKey":{"gk":{"N":"1"}}}"##,
        r##"One or more parameter values were invalid: Type mismatch for Index Key gk Expected: S Actual: N IndexName: gsi"##,
    ),
    (
        r##"r10 plain SPECIFIC_ATTRIBUTES, no projection"##,
        r##"{"Select":"SPECIFIC_ATTRIBUTES"}"##,
        r##"1 validation error detected: Must specify the AttributesToGet or ProjectionExpression when choosing to get SPECIFIC_ATTRIBUTES"##,
    ),
    (
        r##"r10 plain Select COUNT + AttributesToGet"##,
        r##"{"Select":"COUNT","AttributesToGet":["a"]}"##,
        r##"1 validation error detected: Cannot specify the AttributesToGet when choosing to get only the Count"##,
    ),
    (
        r##"r10 plain Select COUNT + ProjectionExpression"##,
        r##"{"Select":"COUNT","ProjectionExpression":"a"}"##,
        r##"1 validation error detected: Cannot specify the ProjectionExpression when choosing to get only the Count"##,
    ),
    (
        r##"r10 plain ALL_PROJECTED_ATTRIBUTES, no index"##,
        r##"{"Select":"ALL_PROJECTED_ATTRIBUTES"}"##,
        r##"1 validation error detected: ALL_PROJECTED_ATTRIBUTES can be used only when Querying using an IndexName"##,
    ),
    (
        r##"r10 plain Segment >= TotalSegments"##,
        r##"{"Segment":5,"TotalSegments":5}"##,
        r##"The Segment parameter is zero-based and must be less than parameter TotalSegments"##,
    ),
    (
        r##"r10 plain Segment without TotalSegments"##,
        r##"{"Segment":0}"##,
        r##"The TotalSegments parameter is required but was not present in the request when Segment parameter is present"##,
    ),
    (
        r##"r10 plain malformed ExclusiveStartKey"##,
        r##"{"ExclusiveStartKey":{"bad":{"S":"p"}}}"##,
        r##"The provided starting key is invalid"##,
    ),
    (
        r##"r10 plain ExclusiveStartKey of the wrong type, indexed table"##,
        r##"{"ExclusiveStartKey":{"pk":{"N":"1"}}}"##,
        r##"One or more parameter values were invalid: Type mismatch for Index Key pk Expected: S Actual: N IndexName: gsi"##,
    ),
    (
        r##"r10 plain gsi: index key of the wrong type"##,
        r##"{"IndexName":"gsi","ExclusiveStartKey":{"pk":{"S":"a"},"gk":{"N":"1"}}}"##,
        r##"One or more parameter values were invalid: Type mismatch for Index Key gk Expected: S Actual: N IndexName: gsi"##,
    ),
    (
        r##"r10 plain gsi: ALL_ATTRIBUTES on a keys-only index + malformed ExclusiveStartKey"##,
        r##"{"IndexName":"gsi","Select":"ALL_ATTRIBUTES","ExclusiveStartKey":{"bad":{"S":"p"}}}"##,
        r##"One or more parameter values were invalid: Select type ALL_ATTRIBUTES is not supported for global secondary index gsi because its projection type is not ALL"##,
    ),
    (
        r##"r10 plain ConditionalOperator AND, no filter"##,
        r##"{"ConditionalOperator":"AND"}"##,
        r##"ConditionalOperator cannot be used without Filter or Expected"##,
    ),
    (
        r##"r10 plain ConditionalOperator BOGUS, two-element ScanFilter"##,
        r##"{"ConditionalOperator":"BOGUS","ScanFilter":{"a":{"ComparisonOperator":"EQ","AttributeValueList":[{"S":"x"}]},"b":{"ComparisonOperator":"EQ","AttributeValueList":[{"S":"y"}]}}}"##,
        r##"Failed to satisfy constraint: Member must satisfy enum value set: [ALL, OR]"##,
    ),
    (
        r##"r10 plain ScanFilter operator BOGUS"##,
        r##"{"ScanFilter":{"a":{"ComparisonOperator":"BOGUS","AttributeValueList":[{"S":"x"}]}}}"##,
        r##"One or more parameter values were invalid: Invalid comparison operator"##,
    ),
    (
        r##"r10 plain empty AttributesToGet name"##,
        r##"{"AttributesToGet":[""]}"##,
        r##"Empty attribute name"##,
    ),
    (
        r##"r10 plain empty name in ExclusiveStartKey"##,
        r##"{"ExclusiveStartKey":{"":{"S":"a"}}}"##,
        r##"Empty attribute name"##,
    ),
    (
        r##"r10 plain empty name in an expression value"##,
        r##"{"FilterExpression":"a = :v","ExpressionAttributeValues":{":v":{"M":{"":{"S":"x"}}}}}"##,
        r##"Empty attribute name"##,
    ),
    (
        r##"r10 plain expression and non-expression mixed"##,
        r##"{"ScanFilter":{"a":{"ComparisonOperator":"EQ","AttributeValueList":[{"S":"x"}]}},"FilterExpression":"a = :v","ExpressionAttributeValues":{":v":{"S":"x"}}}"##,
        r##"Can not use both expression and non-expression parameters in the same request: Non-expression parameters: {ScanFilter} Expression parameters: {FilterExpression}"##,
    ),
    (
        r##"r10 plain empty number in an expression value"##,
        r##"{"FilterExpression":"a = :v","ExpressionAttributeValues":{":v":{"N":""}}}"##,
        r##"The parameter cannot be converted to a numeric value: "##,
    ),
    (
        r##"r10 plain repeated string set member in an expression value"##,
        r##"{"FilterExpression":"a = :v","ExpressionAttributeValues":{":v":{"SS":["a","a"]}}}"##,
        r##"One or more parameter values were invalid: Input collection ["a", "a"] contains duplicates."##,
    ),
    (
        r##"r10 plain ScanFilter empty number"##,
        r##"{"ScanFilter":{"a":{"ComparisonOperator":"EQ","AttributeValueList":[{"N":""}]}}}"##,
        r##"The parameter cannot be converted to a numeric value: "##,
    ),
    (
        r##"r10 plain TableName ab"##,
        r##"{"TableName":"ab"}"##,
        r##"1 validation error detected: Value 'ab' at 'tableName' failed to satisfy constraint: Member must have length greater than or equal to 3"##,
    ),
    (
        r##"r10 plain ReturnConsumedCapacity BOGUS"##,
        r##"{"ReturnConsumedCapacity":"BOGUS"}"##,
        r##"1 validation error detected: Value 'BOGUS' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"##,
    ),
    (
        r##"r10 plain TableName ab + ReturnConsumedCapacity BOGUS"##,
        r##"{"TableName":"ab","ReturnConsumedCapacity":"BOGUS"}"##,
        r##"2 validation errors detected: Value 'ab' at 'tableName' failed to satisfy constraint: Member must have length greater than or equal to 3; Value 'BOGUS' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"##,
    ),
    (
        r##"r10 plain invalid FilterExpression syntax"##,
        r##"{"FilterExpression":"a = = :v","ExpressionAttributeValues":{":v":{"S":"x"}}}"##,
        r##"Invalid FilterExpression: Syntax error; token: "=", near: "= = :v""##,
    ),
    (
        r##"r10 plain no such index"##,
        r##"{"IndexName":"nope"}"##,
        r##"The table does not have the specified index: nope"##,
    ),
    (
        r##"TotalSegments 0 with Segment 0"##,
        r##"{"Segment":0,"TotalSegments":0}"##,
        r##"1 validation error detected: Value at 'TotalSegments' failed to satisfy constraint: Member must have value greater than or equal to 1"##,
    ),
    (
        r##"TotalSegments 1000001 with Segment 0"##,
        r##"{"Segment":0,"TotalSegments":1000001}"##,
        r##"1 validation error detected: Value at 'TotalSegments' failed to satisfy constraint: Member must have value less than or equal to 1000000"##,
    ),
];

#[tokio::test]
async fn each_request_is_answered_as_the_newer_layer_answers_it() {
    assert_eq!(CASES.len(), 223, "a case was added or lost");
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

/// Scan's integer members written as numbers a JSON value can't hold, or as
/// a zero with a fraction, sent as written. Each saturates or truncates and is
/// reported as read (captured eu-west-2 and us-east-1, 6 November 2026, where
/// both regions gave the same answers).
#[tokio::test]
async fn integer_members_past_the_range_of_a_double_saturate() {
    let url = start_server().await;
    let below_one = |value: &str, member: &str| {
        format!(
            "1 validation error detected: Value '{value}' at '{member}' failed to satisfy \
             constraint: Member must have value greater than or equal to 1"
        )
    };
    for (members, expected) in [
        (r#""Limit":1e400"#, String::new()),
        (r#""Limit":-1e400"#, below_one("-2147483648", "limit")),
        (r#""Limit":0.0"#, below_one("0", "limit")),
        (r#""Limit":-0.0"#, below_one("0", "limit")),
        (
            r#""Segment":0,"TotalSegments":1e400"#,
            "1 validation error detected: Value '2147483647' at 'totalSegments' failed to \
             satisfy constraint: Member must have value less than or equal to 1000000"
                .to_string(),
        ),
        (
            r#""Segment":0,"TotalSegments":-1e400"#,
            below_one("-2147483648", "totalSegments"),
        ),
        (
            r#""Segment":1e400,"TotalSegments":2"#,
            "1 validation error detected: Value '2147483647' at 'segment' failed to satisfy \
             constraint: Member must have value less than or equal to 999999"
                .to_string(),
        ),
        (
            r#""Segment":-1e400,"TotalSegments":2"#,
            "1 validation error detected: Value '-2147483648' at 'segment' failed to satisfy \
             constraint: Member must have value greater than or equal to 0"
                .to_string(),
        ),
    ] {
        let body = format!(r#"{{"TableName":"{TABLE}",{members}}}"#);
        assert_eq!(answer_raw(&url, body).await, expected, "{members}");
    }
}

/// A number past a double's range in a member that isn't a number is named by
/// how it is written, whatever its size: a decimal with a point or an
/// exponent, as `1e300` is, and a whole number without (captured eu-west-2
/// and us-east-1, 6 November 2026, where both regions gave the same answers).
#[tokio::test]
async fn numbers_past_the_range_of_a_double_are_named_as_written_elsewhere() {
    let url = start_server().await;
    let to_string = "DECIMAL_VALUE cannot be converted to String";
    let digits = format!("1{}", "0".repeat(400));
    for (body, expected) in [
        (
            format!(r#"{{"TableName":{digits}}}"#),
            "NUMBER_VALUE cannot be converted to String",
        ),
        (
            format!(r#"{{"TableName":-{digits}}}"#),
            "NUMBER_VALUE cannot be converted to String",
        ),
        (format!(r#"{{"TableName":{digits}.0}}"#), to_string),
        (
            format!(r#"{{"TableName":"{TABLE}","Select":{digits}}}"#),
            "NUMBER_VALUE cannot be converted to String",
        ),
        (
            format!(r#"{{"TableName":"{TABLE}","ConsistentRead":{digits}}}"#),
            "NUMBER_VALUE cannot be converted to Boolean",
        ),
        (r#"{"TableName":1e400}"#.to_string(), to_string),
        (r#"{"TableName":-1e400}"#.to_string(), to_string),
        (r#"{"TableName":1e300}"#.to_string(), to_string),
        (
            format!(r#"{{"TableName":"{TABLE}","ConsistentRead":1e400}}"#),
            "DECIMAL_VALUE cannot be converted to Boolean",
        ),
        (
            format!(r#"{{"TableName":"{TABLE}","Select":1e400}}"#),
            to_string,
        ),
    ] {
        assert_eq!(answer_raw(&url, body.clone()).await, expected, "{body}");
    }
}

/// Scan reads its attribute values on the newer layer as well: a number that
/// does not parse is refused with the value and nothing around it, a repeated
/// set member is quoted, and an expression value is not wrapped in
/// `ExpressionAttributeValues contains invalid value`. An empty attribute name
/// in a value or a starting key is refused bare. eu-west-2, 20 October 2026;
/// 19 of the 33 observed regions answered this way for the numbers and sets, and
/// 20 for the empty names.
const VALUE_CASES: &[(&str, &str, &str)] = &[
    (
        r##"exprEmptyNumber"##,
        r##"{"FilterExpression":"a = :v","ExpressionAttributeValues":{":v":{"N":""}}}"##,
        r##"The parameter cannot be converted to a numeric value: "##,
    ),
    (
        r##"exprBadNumber"##,
        r##"{"FilterExpression":"a = :v","ExpressionAttributeValues":{":v":{"N":"abc"}}}"##,
        r##"The parameter cannot be converted to a numeric value: abc"##,
    ),
    (
        r##"exprDuplicateStringSet"##,
        r##"{"FilterExpression":"a = :v","ExpressionAttributeValues":{":v":{"SS":["a","a"]}}}"##,
        r##"One or more parameter values were invalid: Input collection ["a", "a"] contains duplicates."##,
    ),
    (
        r##"exprDuplicateNumberSet"##,
        r##"{"FilterExpression":"a = :v","ExpressionAttributeValues":{":v":{"NS":["1","1"]}}}"##,
        r##"One or more parameter values were invalid: Input collection contains duplicates."##,
    ),
    (
        r##"legacyEmptyNumber"##,
        r##"{"ScanFilter":{"a":{"ComparisonOperator":"EQ","AttributeValueList":[{"N":""}]}}}"##,
        r##"The parameter cannot be converted to a numeric value: "##,
    ),
    (
        r##"legacyBadNumber"##,
        r##"{"ScanFilter":{"a":{"ComparisonOperator":"EQ","AttributeValueList":[{"N":"abc"}]}}}"##,
        r##"The parameter cannot be converted to a numeric value: abc"##,
    ),
    (
        r##"legacyDuplicateStringSet"##,
        r##"{"ScanFilter":{"a":{"ComparisonOperator":"EQ","AttributeValueList":[{"SS":["a","a"]}]}}}"##,
        r##"One or more parameter values were invalid: Input collection ["a", "a"] contains duplicates."##,
    ),
    (
        r##"emptyNameInExpressionValue"##,
        r##"{"FilterExpression":"a = :v","ExpressionAttributeValues":{":v":{"M":{"":{"S":"x"}}}}}"##,
        r##"Empty attribute name"##,
    ),
    (
        r##"emptyNameInStartKey"##,
        r##"{"ExclusiveStartKey":{"":{"S":"a"}}}"##,
        r##"Empty attribute name"##,
    ),
    (
        r##"emptyNameInStartKeyValue"##,
        r##"{"ExclusiveStartKey":{"pk":{"M":{"":{"S":"a"}}}}}"##,
        r##"Empty attribute name"##,
    ),
];

#[tokio::test]
async fn attribute_values_are_read_as_the_newer_layer_reads_them() {
    assert_eq!(VALUE_CASES.len(), 10, "a case was added or lost");
    let url = start_server().await;
    let mut wrong = Vec::new();
    for (case, members, expected) in VALUE_CASES {
        let mut body = serde_json::json!({"TableName": TABLE});
        for (key, value) in serde_json::from_str::<serde_json::Value>(members)
            .unwrap()
            .as_object()
            .unwrap()
        {
            body[key] = value.clone();
        }
        let got = answer(&url, &body).await;
        if got != *expected {
            wrong.push(format!("{case}\n  want: {expected}\n  got:  {got}"));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// The wire table's answer for `case`.
fn wire_answer(case: &str) -> &'static str {
    CASES
        .iter()
        .find(|(name, _, _)| *name == case)
        .unwrap_or_else(|| panic!("no case called {case}"))
        .2
}

/// A request built in code skips the body reader, so `scan` has to give the
/// answers the wire path gives: the same message for the same mistake.
#[test]
fn a_request_built_in_code_is_answered_as_the_wire_request_is() {
    use dynoxide::actions::scan::ScanRequest;

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
    let on = |request: ScanRequest| ScanRequest {
        table_name: TABLE.to_string(),
        ..request
    };
    let cases: Vec<(&str, ScanRequest)> = vec![
        (
            "Limit 0",
            on(ScanRequest {
                limit: Some(0),
                ..Default::default()
            }),
        ),
        (
            "TotalSegments 0 with Segment 0",
            on(ScanRequest {
                segment: Some(0),
                total_segments: Some(0),
                ..Default::default()
            }),
        ),
        (
            "TotalSegments 1000001 with Segment 0",
            on(ScanRequest {
                segment: Some(0),
                total_segments: Some(1_000_001),
                ..Default::default()
            }),
        ),
        (
            "Segment 1000000 of 1000000",
            on(ScanRequest {
                segment: Some(1_000_000),
                total_segments: Some(1_000_000),
                ..Default::default()
            }),
        ),
        (
            "Segment >= TotalSegments",
            on(ScanRequest {
                segment: Some(5),
                total_segments: Some(5),
                ..Default::default()
            }),
        ),
        (
            "Segment 0 without TotalSegments + Select BOGUS",
            on(ScanRequest {
                segment: Some(0),
                select: Some("BOGUS".into()),
                ..Default::default()
            }),
        ),
        (
            "empty TableName",
            ScanRequest {
                table_name: String::new(),
                ..Default::default()
            },
        ),
        (
            "TableName with a bad character, a!",
            ScanRequest {
                table_name: "a!".into(),
                ..Default::default()
            },
        ),
        (
            "TableName too short, ab",
            ScanRequest {
                table_name: "ab".into(),
                ..Default::default()
            },
        ),
        (
            "Select BOGUS",
            on(ScanRequest {
                select: Some("BOGUS".into()),
                ..Default::default()
            }),
        ),
        (
            "ReturnConsumedCapacity BOGUS",
            on(ScanRequest {
                return_consumed_capacity: Some("BOGUS".into()),
                ..Default::default()
            }),
        ),
        (
            "SPECIFIC_ATTRIBUTES, no projection",
            on(ScanRequest {
                select: Some("SPECIFIC_ATTRIBUTES".into()),
                ..Default::default()
            }),
        ),
        (
            "Select COUNT + AttributesToGet",
            on(ScanRequest {
                select: Some("COUNT".into()),
                attributes_to_get: Some(vec!["a".into()]),
                ..Default::default()
            }),
        ),
        (
            "ConditionalOperator AND, no filter",
            on(ScanRequest {
                conditional_operator: Some("AND".into()),
                ..Default::default()
            }),
        ),
    ];
    let mut wrong = Vec::new();
    for (case, request) in cases {
        let got = db.scan(request).unwrap_err().to_string();
        if got != wire_answer(case) {
            wrong.push(format!(
                "{case}\n  want: {}\n  got:  {got}",
                wire_answer(case)
            ));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
