//! DynamoDB's request parser takes any run of digits, `.`, `e`, `E`, `+` and
//! `-` starting with a digit or `-` as a number token, and reads its text only
//! when the member it sits in is read. A token JSON doesn't allow (`1.5.5`,
//! `1e`, `01`, `-`, `1e400.5`) is accepted in a member DynamoDB doesn't read,
//! named `DECIMAL_VALUE` or `NUMBER_VALUE` by how it is written in a member of
//! another type, and read or refused as `Bad number format` by a looser rule
//! than JSON's in an integer member.
//!
//! Every expectation in [`CAPTURED`] is DynamoDB's answer to the same body,
//! captured against eu-west-2 on 10 and 11 November 2026, with the table name
//! changed. No table exists, so a request that gets past its members is
//! answered `ResourceNotFoundException`.

#![cfg(feature = "http-server")]

use dynoxide::Database;
use serde_json::Value;

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

/// Send a body as written, since a malformed token is not JSON.
async fn send(url: &str, operation: &str, body: &str) -> (u16, String) {
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
    (status, resp.text().await.unwrap())
}

/// Operation, body as sent, and DynamoDB's error, or `""` where it accepted
/// the request.
const CAPTURED: &[(&str, &str, &str)] = &[
    (
        "UpdateTable",
        r##"{"TableName":"no_such_table","ProvisionedThroughput":{"ReadCapacityUnits":1.5.5,"WriteCapacityUnits":1}}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Bad number format"}"##,
    ),
    (
        "UpdateTable",
        r##"{"TableName":"no_such_table","ProvisionedThroughput":{"ReadCapacityUnits":1e,"WriteCapacityUnits":1}}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Bad number format"}"##,
    ),
    (
        "UpdateTable",
        r##"{"TableName":"no_such_table","ProvisionedThroughput":{"ReadCapacityUnits":01,"WriteCapacityUnits":1}}"##,
        r##"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found: Table: no_such_table not found"}"##,
    ),
    (
        "UpdateTable",
        r##"{"TableName":"no_such_table","ProvisionedThroughput":{"ReadCapacityUnits":-,"WriteCapacityUnits":1}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value '0' at 'provisionedThroughput.readCapacityUnits' failed to satisfy constraint: Member must have value greater than or equal to 1"}"##,
    ),
    (
        "UpdateTable",
        r##"{"TableName":"no_such_table","ProvisionedThroughput":{"ReadCapacityUnits":1e400.5,"WriteCapacityUnits":1}}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Bad number format"}"##,
    ),
    (
        "UpdateTable",
        r##"{"TableName":"no_such_table","ProvisionedThroughput":{"ReadCapacityUnits":5,"WriteCapacityUnits":1}}"##,
        r##"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found: Table: no_such_table not found"}"##,
    ),
    (
        "Scan",
        r##"{"TableName":"no_such_table","Segment":1.5.5,"TotalSegments":2}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Bad number format"}"##,
    ),
    (
        "Scan",
        r##"{"TableName":"no_such_table","Segment":1e,"TotalSegments":2}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Bad number format"}"##,
    ),
    (
        "Scan",
        r##"{"TableName":"no_such_table","Segment":01,"TotalSegments":2}"##,
        r##"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found"}"##,
    ),
    (
        "Scan",
        r##"{"TableName":"no_such_table","Segment":-,"TotalSegments":2}"##,
        r##"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found"}"##,
    ),
    (
        "Scan",
        r##"{"TableName":"no_such_table","Segment":1e400.5,"TotalSegments":2}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Bad number format"}"##,
    ),
    (
        "Scan",
        r##"{"TableName":"no_such_table","Segment":5,"TotalSegments":2}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The Segment parameter is zero-based and must be less than parameter TotalSegments"}"##,
    ),
    (
        "DescribeTable",
        r##"{"TableName":1.5.5}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"DECIMAL_VALUE cannot be converted to String"}"##,
    ),
    (
        "DescribeTable",
        r##"{"TableName":1e}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"DECIMAL_VALUE cannot be converted to String"}"##,
    ),
    (
        "DescribeTable",
        r##"{"TableName":01}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"NUMBER_VALUE cannot be converted to String"}"##,
    ),
    (
        "DescribeTable",
        r##"{"TableName":-}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"NUMBER_VALUE cannot be converted to String"}"##,
    ),
    (
        "DescribeTable",
        r##"{"TableName":1e400.5}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"DECIMAL_VALUE cannot be converted to String"}"##,
    ),
    (
        "DescribeTable",
        r##"{"TableName":5}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"NUMBER_VALUE cannot be converted to String"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":{"S":"a"}},"ConsistentRead":1.5.5}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"DECIMAL_VALUE cannot be converted to Boolean"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":{"S":"a"}},"ConsistentRead":1e}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"DECIMAL_VALUE cannot be converted to Boolean"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":{"S":"a"}},"ConsistentRead":01}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"NUMBER_VALUE cannot be converted to Boolean"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":{"S":"a"}},"ConsistentRead":-}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"NUMBER_VALUE cannot be converted to Boolean"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":{"S":"a"}},"ConsistentRead":1e400.5}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"DECIMAL_VALUE cannot be converted to Boolean"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":{"S":"a"}},"ConsistentRead":5}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"NUMBER_VALUE cannot be converted to Boolean"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":{"S":"a"}},"AttributesToGet":1.5.5}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected field type"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":{"S":"a"}},"AttributesToGet":1e}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected field type"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":{"S":"a"}},"AttributesToGet":01}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected field type"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":{"S":"a"}},"AttributesToGet":-}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected field type"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":{"S":"a"}},"AttributesToGet":1e400.5}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected field type"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":{"S":"a"}},"AttributesToGet":5}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected field type"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":{"S":"a"}},"AttributesToGet":[1.5.5]}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"DECIMAL_VALUE cannot be converted to String"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":{"S":"a"}},"AttributesToGet":[1e]}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"DECIMAL_VALUE cannot be converted to String"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":{"S":"a"}},"AttributesToGet":[01]}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"NUMBER_VALUE cannot be converted to String"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":{"S":"a"}},"AttributesToGet":[-]}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"NUMBER_VALUE cannot be converted to String"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":{"S":"a"}},"AttributesToGet":[1e400.5]}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"DECIMAL_VALUE cannot be converted to String"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":{"S":"a"}},"AttributesToGet":[5]}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"NUMBER_VALUE cannot be converted to String"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":1.5.5}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected field type"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":1e}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected field type"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":01}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected field type"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":-}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected field type"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":1e400.5}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected field type"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":5}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected field type"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":{"S":"a"}},"ExpressionAttributeNames":{"#a":1.5.5}}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"DECIMAL_VALUE cannot be converted to String"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":{"S":"a"}},"ExpressionAttributeNames":{"#a":1e}}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"DECIMAL_VALUE cannot be converted to String"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":{"S":"a"}},"ExpressionAttributeNames":{"#a":01}}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"NUMBER_VALUE cannot be converted to String"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":{"S":"a"}},"ExpressionAttributeNames":{"#a":-}}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"NUMBER_VALUE cannot be converted to String"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":{"S":"a"}},"ExpressionAttributeNames":{"#a":1e400.5}}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"DECIMAL_VALUE cannot be converted to String"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":{"S":"a"}},"ExpressionAttributeNames":{"#a":5}}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"NUMBER_VALUE cannot be converted to String"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":{"N":1.5.5}}}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"DECIMAL_VALUE cannot be converted to String"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":{"N":1e}}}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"DECIMAL_VALUE cannot be converted to String"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":{"N":01}}}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"NUMBER_VALUE cannot be converted to String"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":{"N":-}}}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"NUMBER_VALUE cannot be converted to String"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":{"N":1e400.5}}}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"DECIMAL_VALUE cannot be converted to String"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":{"N":5}}}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"NUMBER_VALUE cannot be converted to String"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":{"S":1.5.5}}}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"DECIMAL_VALUE cannot be converted to String"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":{"S":1e}}}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"DECIMAL_VALUE cannot be converted to String"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":{"S":01}}}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"NUMBER_VALUE cannot be converted to String"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":{"S":-}}}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"NUMBER_VALUE cannot be converted to String"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":{"S":1e400.5}}}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"DECIMAL_VALUE cannot be converted to String"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":{"S":5}}}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"NUMBER_VALUE cannot be converted to String"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":1.5.5}}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected value type in payload"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":1e}}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected value type in payload"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":01}}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected value type in payload"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":-}}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected value type in payload"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":1e400.5}}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected value type in payload"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":5}}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected value type in payload"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":{"S":"a","Foo":1.5.5}}}"##,
        r##"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":{"S":"a","Foo":1e}}}"##,
        r##"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":{"S":"a","Foo":01}}}"##,
        r##"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":{"S":"a","Foo":-}}}"##,
        r##"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":{"S":"a","Foo":1e400.5}}}"##,
        r##"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":{"S":"a","Foo":5}}}"##,
        r##"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":{"S":"a"}},"ReturnConsumedCapacity":1.5.5}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"DECIMAL_VALUE cannot be converted to String"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":{"S":"a"}},"ReturnConsumedCapacity":1e}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"DECIMAL_VALUE cannot be converted to String"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":{"S":"a"}},"ReturnConsumedCapacity":01}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"NUMBER_VALUE cannot be converted to String"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":{"S":"a"}},"ReturnConsumedCapacity":-}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"NUMBER_VALUE cannot be converted to String"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":{"S":"a"}},"ReturnConsumedCapacity":1e400.5}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"DECIMAL_VALUE cannot be converted to String"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":{"S":"a"}},"ReturnConsumedCapacity":5}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"NUMBER_VALUE cannot be converted to String"}"##,
    ),
    (
        "ExecuteStatement",
        r##"{"Statement":"SELECT * FROM \"no_such_table\" WHERE pk = ?","Parameters":[{"N":1.5.5}]}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"DECIMAL_VALUE cannot be converted to String"}"##,
    ),
    (
        "ExecuteStatement",
        r##"{"Statement":"SELECT * FROM \"no_such_table\" WHERE pk = ?","Parameters":[{"N":1e}]}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"DECIMAL_VALUE cannot be converted to String"}"##,
    ),
    (
        "ExecuteStatement",
        r##"{"Statement":"SELECT * FROM \"no_such_table\" WHERE pk = ?","Parameters":[{"N":01}]}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"NUMBER_VALUE cannot be converted to String"}"##,
    ),
    (
        "ExecuteStatement",
        r##"{"Statement":"SELECT * FROM \"no_such_table\" WHERE pk = ?","Parameters":[{"N":-}]}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"NUMBER_VALUE cannot be converted to String"}"##,
    ),
    (
        "ExecuteStatement",
        r##"{"Statement":"SELECT * FROM \"no_such_table\" WHERE pk = ?","Parameters":[{"N":1e400.5}]}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"DECIMAL_VALUE cannot be converted to String"}"##,
    ),
    (
        "ExecuteStatement",
        r##"{"Statement":"SELECT * FROM \"no_such_table\" WHERE pk = ?","Parameters":[{"N":5}]}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"NUMBER_VALUE cannot be converted to String"}"##,
    ),
    (
        "ExecuteStatement",
        r##"{"Statement":"SELECT * FROM \"no_such_table\" WHERE pk = ?","Parameters":[1.5.5]}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected value type in payload"}"##,
    ),
    (
        "ExecuteStatement",
        r##"{"Statement":"SELECT * FROM \"no_such_table\" WHERE pk = ?","Parameters":[1e]}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected value type in payload"}"##,
    ),
    (
        "ExecuteStatement",
        r##"{"Statement":"SELECT * FROM \"no_such_table\" WHERE pk = ?","Parameters":[01]}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected value type in payload"}"##,
    ),
    (
        "ExecuteStatement",
        r##"{"Statement":"SELECT * FROM \"no_such_table\" WHERE pk = ?","Parameters":[-]}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected value type in payload"}"##,
    ),
    (
        "ExecuteStatement",
        r##"{"Statement":"SELECT * FROM \"no_such_table\" WHERE pk = ?","Parameters":[1e400.5]}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected value type in payload"}"##,
    ),
    (
        "ExecuteStatement",
        r##"{"Statement":"SELECT * FROM \"no_such_table\" WHERE pk = ?","Parameters":[5]}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected value type in payload"}"##,
    ),
    (
        "ExecuteStatement",
        r##"{"Statement":"SELECT * FROM \"no_such_table\"","Limit":1.5.5}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Bad number format"}"##,
    ),
    (
        "ExecuteStatement",
        r##"{"Statement":"SELECT * FROM \"no_such_table\"","Limit":1e}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Bad number format"}"##,
    ),
    (
        "ExecuteStatement",
        r##"{"Statement":"SELECT * FROM \"no_such_table\"","Limit":01}"##,
        r##"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found"}"##,
    ),
    (
        "ExecuteStatement",
        r##"{"Statement":"SELECT * FROM \"no_such_table\"","Limit":-}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value '0' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1"}"##,
    ),
    (
        "ExecuteStatement",
        r##"{"Statement":"SELECT * FROM \"no_such_table\"","Limit":1e400.5}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Bad number format"}"##,
    ),
    (
        "ExecuteStatement",
        r##"{"Statement":"SELECT * FROM \"no_such_table\"","Limit":5}"##,
        r##"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found"}"##,
    ),
    (
        "PutItem",
        r##"{"TableName":"no_such_table","Item":{"pk":{"L":[1.5.5]}}}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected value type in payload"}"##,
    ),
    (
        "PutItem",
        r##"{"TableName":"no_such_table","Item":{"pk":{"L":[1e]}}}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected value type in payload"}"##,
    ),
    (
        "PutItem",
        r##"{"TableName":"no_such_table","Item":{"pk":{"L":[01]}}}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected value type in payload"}"##,
    ),
    (
        "PutItem",
        r##"{"TableName":"no_such_table","Item":{"pk":{"L":[-]}}}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected value type in payload"}"##,
    ),
    (
        "PutItem",
        r##"{"TableName":"no_such_table","Item":{"pk":{"L":[1e400.5]}}}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected value type in payload"}"##,
    ),
    (
        "PutItem",
        r##"{"TableName":"no_such_table","Item":{"pk":{"L":[5]}}}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Unexpected value type in payload"}"##,
    ),
    (
        "PutItem",
        r##"{"TableName":"no_such_table","Item":{"pk":{"NS":[1.5.5]}}}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"DECIMAL_VALUE cannot be converted to String"}"##,
    ),
    (
        "PutItem",
        r##"{"TableName":"no_such_table","Item":{"pk":{"NS":[1e]}}}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"DECIMAL_VALUE cannot be converted to String"}"##,
    ),
    (
        "PutItem",
        r##"{"TableName":"no_such_table","Item":{"pk":{"NS":[01]}}}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"NUMBER_VALUE cannot be converted to String"}"##,
    ),
    (
        "PutItem",
        r##"{"TableName":"no_such_table","Item":{"pk":{"NS":[-]}}}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"NUMBER_VALUE cannot be converted to String"}"##,
    ),
    (
        "PutItem",
        r##"{"TableName":"no_such_table","Item":{"pk":{"NS":[1e400.5]}}}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"DECIMAL_VALUE cannot be converted to String"}"##,
    ),
    (
        "PutItem",
        r##"{"TableName":"no_such_table","Item":{"pk":{"NS":[5]}}}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"NUMBER_VALUE cannot be converted to String"}"##,
    ),
    (
        "Query",
        r##"{"TableName":"no_such_table","KeyConditionExpression":"pk = :v","ExpressionAttributeValues":{":v":{"S":"a"}},"Limit":01}"##,
        r##"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found"}"##,
    ),
    (
        "Scan",
        r##"{"TableName":"no_such_table","Limit":01}"##,
        r##"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found"}"##,
    ),
    ("ListTables", r##"{"Limit":01}"##, r##""##),
    (
        "UpdateTable",
        r##"{"TableName":"no_such_table","OnDemandThroughput":{"MaxReadRequestUnits":01}}"##,
        r##"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found: Table: no_such_table not found"}"##,
    ),
    (
        "Query",
        r##"{"TableName":"no_such_table","KeyConditionExpression":"pk = :v","ExpressionAttributeValues":{":v":{"S":"a"}},"Limit":1.}"##,
        r##"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found"}"##,
    ),
    (
        "Scan",
        r##"{"TableName":"no_such_table","Limit":1.}"##,
        r##"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found"}"##,
    ),
    ("ListTables", r##"{"Limit":1.}"##, r##""##),
    (
        "UpdateTable",
        r##"{"TableName":"no_such_table","OnDemandThroughput":{"MaxReadRequestUnits":1.}}"##,
        r##"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found: Table: no_such_table not found"}"##,
    ),
    (
        "Query",
        r##"{"TableName":"no_such_table","KeyConditionExpression":"pk = :v","ExpressionAttributeValues":{":v":{"S":"a"}},"Limit":-.5}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value '0' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1"}"##,
    ),
    (
        "Scan",
        r##"{"TableName":"no_such_table","Limit":-.5}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value '0' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1"}"##,
    ),
    (
        "ListTables",
        r##"{"Limit":-.5}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value '0' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1"}"##,
    ),
    (
        "UpdateTable",
        r##"{"TableName":"no_such_table","OnDemandThroughput":{"MaxReadRequestUnits":-.5}}"##,
        r##"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found: Table: no_such_table not found"}"##,
    ),
    (
        "Query",
        r##"{"TableName":"no_such_table","KeyConditionExpression":"pk = :v","ExpressionAttributeValues":{":v":{"S":"a"}},"Limit":-}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value '0' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1"}"##,
    ),
    (
        "Scan",
        r##"{"TableName":"no_such_table","Limit":-}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value '0' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1"}"##,
    ),
    (
        "ListTables",
        r##"{"Limit":-}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value '0' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1"}"##,
    ),
    (
        "UpdateTable",
        r##"{"TableName":"no_such_table","OnDemandThroughput":{"MaxReadRequestUnits":-}}"##,
        r##"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found: Table: no_such_table not found"}"##,
    ),
    (
        "Query",
        r##"{"TableName":"no_such_table","KeyConditionExpression":"pk = :v","ExpressionAttributeValues":{":v":{"S":"a"}},"Limit":00}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value '0' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1"}"##,
    ),
    (
        "Scan",
        r##"{"TableName":"no_such_table","Limit":00}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value '0' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1"}"##,
    ),
    (
        "ListTables",
        r##"{"Limit":00}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value '0' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1"}"##,
    ),
    (
        "UpdateTable",
        r##"{"TableName":"no_such_table","OnDemandThroughput":{"MaxReadRequestUnits":00}}"##,
        r##"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found: Table: no_such_table not found"}"##,
    ),
    (
        "Query",
        r##"{"TableName":"no_such_table","KeyConditionExpression":"pk = :v","ExpressionAttributeValues":{":v":{"S":"a"}},"Limit":1.e5}"##,
        r##"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found"}"##,
    ),
    (
        "Scan",
        r##"{"TableName":"no_such_table","Limit":1.e5}"##,
        r##"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found"}"##,
    ),
    (
        "ListTables",
        r##"{"Limit":1.e5}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value '100000' at 'limit' failed to satisfy constraint: Member must have value less than or equal to 100"}"##,
    ),
    (
        "UpdateTable",
        r##"{"TableName":"no_such_table","OnDemandThroughput":{"MaxReadRequestUnits":1.e5}}"##,
        r##"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found: Table: no_such_table not found"}"##,
    ),
    (
        "Query",
        r##"{"TableName":"no_such_table","KeyConditionExpression":"pk = :v","ExpressionAttributeValues":{":v":{"S":"a"}},"Limit":1e400.5}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Bad number format"}"##,
    ),
    (
        "Scan",
        r##"{"TableName":"no_such_table","Limit":1e400.5}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Bad number format"}"##,
    ),
    (
        "ListTables",
        r##"{"Limit":1e400.5}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Bad number format"}"##,
    ),
    (
        "UpdateTable",
        r##"{"TableName":"no_such_table","OnDemandThroughput":{"MaxReadRequestUnits":1e400.5}}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Bad number format"}"##,
    ),
    (
        "Query",
        r##"{"TableName":"no_such_table","KeyConditionExpression":"pk = :v","ExpressionAttributeValues":{":v":{"S":"a"}},"Limit":1e}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Bad number format"}"##,
    ),
    (
        "Scan",
        r##"{"TableName":"no_such_table","Limit":1e}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Bad number format"}"##,
    ),
    (
        "ListTables",
        r##"{"Limit":1e}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Bad number format"}"##,
    ),
    (
        "UpdateTable",
        r##"{"TableName":"no_such_table","OnDemandThroughput":{"MaxReadRequestUnits":1e}}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Bad number format"}"##,
    ),
    (
        "Query",
        r##"{"TableName":"no_such_table","KeyConditionExpression":"pk = :v","ExpressionAttributeValues":{":v":{"S":"a"}},"Limit":0.5}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value '0' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1"}"##,
    ),
    (
        "Scan",
        r##"{"TableName":"no_such_table","Limit":0.5}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value '0' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1"}"##,
    ),
    (
        "ListTables",
        r##"{"Limit":0.5}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value '0' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1"}"##,
    ),
    (
        "UpdateTable",
        r##"{"TableName":"no_such_table","OnDemandThroughput":{"MaxReadRequestUnits":0.5}}"##,
        r##"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found: Table: no_such_table not found"}"##,
    ),
    (
        "DescribeTable",
        r##"{"TableName":1-2}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"NUMBER_VALUE cannot be converted to String"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":{"S":"a"}},"ConsistentRead":1-2}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"NUMBER_VALUE cannot be converted to Boolean"}"##,
    ),
    (
        "DescribeTable",
        r##"{"TableName":--1}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"NUMBER_VALUE cannot be converted to String"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":{"S":"a"}},"ConsistentRead":--1}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"NUMBER_VALUE cannot be converted to Boolean"}"##,
    ),
    (
        "DescribeTable",
        r##"{"TableName":1e+}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"DECIMAL_VALUE cannot be converted to String"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":{"S":"a"}},"ConsistentRead":1e+}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"DECIMAL_VALUE cannot be converted to Boolean"}"##,
    ),
    (
        "DescribeTable",
        r##"{"TableName":1..}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"DECIMAL_VALUE cannot be converted to String"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":{"S":"a"}},"ConsistentRead":1..}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"DECIMAL_VALUE cannot be converted to Boolean"}"##,
    ),
    (
        "DescribeTable",
        r##"{"TableName":1--}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"NUMBER_VALUE cannot be converted to String"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName":"no_such_table","Key":{"pk":{"S":"a"}},"ConsistentRead":1--}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"NUMBER_VALUE cannot be converted to Boolean"}"##,
    ),
    (
        "Scan",
        r##"{"TableName":"no_such_table","Limit":1.5.5,"ConsistentRead":5}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Bad number format"}"##,
    ),
    (
        "Scan",
        r##"{"TableName":"no_such_table","ConsistentRead":5,"Limit":1.5.5}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"NUMBER_VALUE cannot be converted to Boolean"}"##,
    ),
    (
        "Scan",
        r##"{"TableName":"no_such_table","Limit":1.5.5,"Select":"BAD"}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Bad number format"}"##,
    ),
    (
        "Scan",
        r##"{"TableName":5,"Limit":1.5.5}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"NUMBER_VALUE cannot be converted to String"}"##,
    ),
    (
        "Scan",
        r##"{"TableName":"no_such_table","Limit":1.5.5,"Segment":1e}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Bad number format"}"##,
    ),
    (
        "Scan",
        r##"{"TableName":"no_such_table","Limit":-,"ConsistentRead":5}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"NUMBER_VALUE cannot be converted to Boolean"}"##,
    ),
    (
        "ListTables",
        r##"{"Limit":-e5}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Bad number format"}"##,
    ),
    (
        "ListTables",
        r##"{"Limit":-.}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Bad number format"}"##,
    ),
    (
        "ListTables",
        r##"{"Limit":-.e5}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Bad number format"}"##,
    ),
    (
        "ListTables",
        r##"{"Limit":-.e}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Bad number format"}"##,
    ),
    (
        "ListTables",
        r##"{"Limit":-e}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Bad number format"}"##,
    ),
    (
        "ListTables",
        r##"{"Limit":0.5}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value '0' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1"}"##,
    ),
    (
        "ListTables",
        r##"{"Limit":-.5}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value '0' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1"}"##,
    ),
    (
        "ListTables",
        r##"{"Limit":1.e5}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value '100000' at 'limit' failed to satisfy constraint: Member must have value less than or equal to 100"}"##,
    ),
    (
        "ListTables",
        r##"{"Limit":0.9}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value '0' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1"}"##,
    ),
    (
        "UpdateTable",
        r##"{"TableName":"no_such_table","ProvisionedThroughput":{"ReadCapacityUnits":0.5,"WriteCapacityUnits":1}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value '0' at 'provisionedThroughput.readCapacityUnits' failed to satisfy constraint: Member must have value greater than or equal to 1"}"##,
    ),
    (
        "CreateTable",
        r##"{"TableName":"no_such_table","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"ProvisionedThroughput":{"ReadCapacityUnits":0.5,"WriteCapacityUnits":1}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value '0' at 'provisionedThroughput.readCapacityUnits' failed to satisfy constraint: Member must have value greater than or equal to 1"}"##,
    ),
    (
        "UpdateTable",
        r##"{"TableName":"no_such_table","ProvisionedThroughput":{"ReadCapacityUnits":-.5,"WriteCapacityUnits":1}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value '0' at 'provisionedThroughput.readCapacityUnits' failed to satisfy constraint: Member must have value greater than or equal to 1"}"##,
    ),
    (
        "CreateTable",
        r##"{"TableName":"no_such_table","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"ProvisionedThroughput":{"ReadCapacityUnits":-.5,"WriteCapacityUnits":1}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value '0' at 'provisionedThroughput.readCapacityUnits' failed to satisfy constraint: Member must have value greater than or equal to 1"}"##,
    ),
    (
        "UpdateTable",
        r##"{"TableName":"no_such_table","ProvisionedThroughput":{"ReadCapacityUnits":1.e5,"WriteCapacityUnits":1}}"##,
        r##"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found: Table: no_such_table not found"}"##,
    ),
    (
        "CreateTable",
        r##"{"TableName":"no_such_table","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"ProvisionedThroughput":{"ReadCapacityUnits":1.e5,"WriteCapacityUnits":1}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value null at 'keySchema' failed to satisfy constraint: Member must not be null"}"##,
    ),
    (
        "UpdateTable",
        r##"{"TableName":"no_such_table","ProvisionedThroughput":{"ReadCapacityUnits":1.5,"WriteCapacityUnits":1}}"##,
        r##"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found: Table: no_such_table not found"}"##,
    ),
    (
        "CreateTable",
        r##"{"TableName":"no_such_table","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"ProvisionedThroughput":{"ReadCapacityUnits":1.5,"WriteCapacityUnits":1}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value null at 'keySchema' failed to satisfy constraint: Member must not be null"}"##,
    ),
    (
        "UpdateTable",
        r##"{"TableName":"no_such_table","ProvisionedThroughput":{"ReadCapacityUnits":0.9,"WriteCapacityUnits":1}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value '0' at 'provisionedThroughput.readCapacityUnits' failed to satisfy constraint: Member must have value greater than or equal to 1"}"##,
    ),
    (
        "CreateTable",
        r##"{"TableName":"no_such_table","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"ProvisionedThroughput":{"ReadCapacityUnits":0.9,"WriteCapacityUnits":1}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value '0' at 'provisionedThroughput.readCapacityUnits' failed to satisfy constraint: Member must have value greater than or equal to 1"}"##,
    ),
    (
        "UpdateTable",
        r##"{"TableName":"no_such_table","ProvisionedThroughput":{"ReadCapacityUnits":-e5,"WriteCapacityUnits":1}}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Bad number format"}"##,
    ),
    (
        "CreateTable",
        r##"{"TableName":"no_such_table","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"ProvisionedThroughput":{"ReadCapacityUnits":-e5,"WriteCapacityUnits":1}}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Bad number format"}"##,
    ),
    (
        "CreateTable",
        r##"{"TableName":"no_such_table","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"BillingMode":"PAY_PER_REQUEST","OnDemandThroughput":{"MaxReadRequestUnits":1.5.5}}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"Bad number format"}"##,
    ),
    (
        "CreateTable",
        r##"{"TableName":"no_such_table","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"BillingMode":"PAY_PER_REQUEST","OnDemandThroughput":{"MaxReadRequestUnits":01}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value null at 'keySchema' failed to satisfy constraint: Member must not be null"}"##,
    ),
    (
        "CreateTable",
        r##"{"TableName":"no_such_table","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"BillingMode":"PAY_PER_REQUEST","OnDemandThroughput":{"MaxReadRequestUnits":0.5}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value null at 'keySchema' failed to satisfy constraint: Member must not be null"}"##,
    ),
    (
        "CreateTable",
        r##"{"TableName":"no_such_table","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"BillingMode":"PAY_PER_REQUEST","OnDemandThroughput":{"MaxReadRequestUnits":1e400}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value null at 'keySchema' failed to satisfy constraint: Member must not be null"}"##,
    ),
    (
        "CreateTable",
        r##"{"TableName":"no_such_table","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"ProvisionedThroughput":{"ReadCapacityUnits":0,"WriteCapacityUnits":1}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value '0' at 'provisionedThroughput.readCapacityUnits' failed to satisfy constraint: Member must have value greater than or equal to 1"}"##,
    ),
    (
        "CreateTable",
        r##"{"TableName":"no_such_table","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],"ProvisionedThroughput":{"ReadCapacityUnits":0,"WriteCapacityUnits":0}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value '0' at 'provisionedThroughput.writeCapacityUnits' failed to satisfy constraint: Member must have value greater than or equal to 1; Value '0' at 'provisionedThroughput.readCapacityUnits' failed to satisfy constraint: Member must have value greater than or equal to 1"}"##,
    ),
    (
        "CreateTable",
        r##"{"TableName":"no_such_table","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"ProvisionedThroughput":{"ReadCapacityUnits":-1,"WriteCapacityUnits":1}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value '-1' at 'provisionedThroughput.readCapacityUnits' failed to satisfy constraint: Member must have value greater than or equal to 1"}"##,
    ),
    (
        "CreateTable",
        r##"{"TableName":"no_such_table","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],"ProvisionedThroughput":{"ReadCapacityUnits":-1,"WriteCapacityUnits":0}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value '0' at 'provisionedThroughput.writeCapacityUnits' failed to satisfy constraint: Member must have value greater than or equal to 1; Value '-1' at 'provisionedThroughput.readCapacityUnits' failed to satisfy constraint: Member must have value greater than or equal to 1"}"##,
    ),
    (
        "CreateTable",
        r##"{"TableName":"no_such_table","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"ProvisionedThroughput":{"ReadCapacityUnits":0.5,"WriteCapacityUnits":1}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value '0' at 'provisionedThroughput.readCapacityUnits' failed to satisfy constraint: Member must have value greater than or equal to 1"}"##,
    ),
    (
        "CreateTable",
        r##"{"TableName":"no_such_table","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],"ProvisionedThroughput":{"ReadCapacityUnits":0.5,"WriteCapacityUnits":0}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value '0' at 'provisionedThroughput.writeCapacityUnits' failed to satisfy constraint: Member must have value greater than or equal to 1; Value '0' at 'provisionedThroughput.readCapacityUnits' failed to satisfy constraint: Member must have value greater than or equal to 1"}"##,
    ),
    (
        "CreateTable",
        r##"{"TableName":"no_such_table","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"ProvisionedThroughput":{"ReadCapacityUnits":1.5,"WriteCapacityUnits":1}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value null at 'keySchema' failed to satisfy constraint: Member must not be null"}"##,
    ),
    (
        "CreateTable",
        r##"{"TableName":"no_such_table","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],"ProvisionedThroughput":{"ReadCapacityUnits":1.5,"WriteCapacityUnits":0}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value '0' at 'provisionedThroughput.writeCapacityUnits' failed to satisfy constraint: Member must have value greater than or equal to 1"}"##,
    ),
    (
        "CreateTable",
        r##"{"TableName":"no_such_table","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"ProvisionedThroughput":{"ReadCapacityUnits":01,"WriteCapacityUnits":1}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value null at 'keySchema' failed to satisfy constraint: Member must not be null"}"##,
    ),
    (
        "CreateTable",
        r##"{"TableName":"no_such_table","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],"ProvisionedThroughput":{"ReadCapacityUnits":01,"WriteCapacityUnits":0}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value '0' at 'provisionedThroughput.writeCapacityUnits' failed to satisfy constraint: Member must have value greater than or equal to 1"}"##,
    ),
];

/// Bodies whose answer depends on which of two faults DynamoDB meets first in
/// the body as written. Dynoxide checks every member's JSON type before
/// reading any member's value, so it meets the boolean first (#302).
const WRITTEN_ORDER: &[&str] =
    &[r##"{"TableName":"no_such_table","Limit":1.5.5,"ConsistentRead":5}"##];

/// CreateTable bodies with no `KeySchema` and a throughput below 1, which
/// DynamoDB answers with the throughput error alone, whether the unit is
/// written `0`, `-1` or `0.5`. Dynoxide lists the missing key schema too.
const KEY_SCHEMA_BESIDE_THROUGHPUT: &[&str] = &[
    r##"{"TableName":"no_such_table","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"ProvisionedThroughput":{"ReadCapacityUnits":0,"WriteCapacityUnits":1}}"##,
    r##"{"TableName":"no_such_table","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"ProvisionedThroughput":{"ReadCapacityUnits":-1,"WriteCapacityUnits":1}}"##,
    r##"{"TableName":"no_such_table","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"ProvisionedThroughput":{"ReadCapacityUnits":0.5,"WriteCapacityUnits":1}}"##,
    r##"{"TableName":"no_such_table","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"ProvisionedThroughput":{"ReadCapacityUnits":-.5,"WriteCapacityUnits":1}}"##,
    r##"{"TableName":"no_such_table","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"ProvisionedThroughput":{"ReadCapacityUnits":0.9,"WriteCapacityUnits":1}}"##,
];

#[tokio::test]
async fn every_captured_body_is_answered_as_dynamodb_answers_it() {
    let url = start().await;
    let mut wrong = Vec::new();
    let mut skipped = 0;
    for (operation, body, expected) in CAPTURED {
        if WRITTEN_ORDER.contains(body) || KEY_SCHEMA_BESIDE_THROUGHPUT.contains(body) {
            skipped += 1;
            continue;
        }
        let (status, got) = send(&url, operation, body).await;
        let matches = if expected.is_empty() {
            status == 200
        } else {
            serde_json::from_str::<Value>(&got).ok()
                == Some(serde_json::from_str::<Value>(expected).unwrap())
        };
        if !matches {
            wrong.push(format!(
                "{operation} {body}\n  want {expected}\n  got  {status} {got}"
            ));
        }
    }
    assert_eq!(
        (CAPTURED.len(), skipped),
        // The `0.5` body of the second list was captured twice.
        (
            195,
            WRITTEN_ORDER.len() + KEY_SCHEMA_BESIDE_THROUGHPUT.len() + 1
        ),
        "every captured row is checked or named"
    );
    assert!(
        wrong.is_empty(),
        "{} of {} answered differently:\n{}",
        wrong.len(),
        CAPTURED.len(),
        wrong.join("\n")
    );
}

const BARE: &str = r#"{"__type":"com.amazon.coral.service#SerializationException"}"#;

/// Tokens DynamoDB doesn't take as a number at all, starting with `.` or `+`
/// or running into a letter, fail the whole request as they did before
/// (captured eu-west-2, 9 November 2026).
#[tokio::test]
async fn tokens_dynamodb_refuses_outright_fail_the_whole_request() {
    let url = start().await;
    let mut checked = 0;
    for token in [".5", "+1", "0x10", "1.5x", "-.5x", "+1.5.5", ".1.5"] {
        for body in [
            format!(r#"{{"Foo":{token}}}"#),
            format!(r#"{{"Limit":{token}}}"#),
        ] {
            let (status, got) = send(&url, "ListTables", &body).await;
            assert_eq!((status, got.as_str()), (400, BARE), "{body}");
            checked += 1;
        }
    }
    assert_eq!(checked, 14);
}

/// A number JSON allows reads as it did before wherever the integer rule
/// doesn't change it, however large: in a member nothing reads it is
/// accepted, and in `TopK` it is a bare SerializationException. In ListTables
/// `Limit` it now saturates as Query's `Limit` does, which is what DynamoDB
/// answers (captured eu-west-2, 9 November 2026).
#[tokio::test]
async fn numbers_past_f64s_range_read_as_before() {
    let url = start().await;
    for number in ["1e400", "-1e400"] {
        let (status, got) = send(&url, "ListTables", &format!(r#"{{"Foo":{number}}}"#)).await;
        assert_eq!(status, 200, "{number}: {got}");
        let body = format!(
            r#"{{"TableName":"no_such_table","IndexName":"i","SearchVector":[{{"N":"1"}}],"TopK":{number}}}"#
        );
        let (status, got) = send(&url, "SearchVectors", &body).await;
        assert_eq!((status, got.as_str()), (400, BARE), "{body}");
    }
    let (_, got) = send(&url, "ListTables", r#"{"Limit":1e400}"#).await;
    assert_eq!(
        serde_json::from_str::<Value>(&got).unwrap()["message"],
        "1 validation error detected: Value '2147483647' at 'limit' failed to satisfy \
         constraint: Member must have value less than or equal to 100"
    );
    let (_, got) = send(&url, "ListTables", r#"{"Limit":-1e400}"#).await;
    assert_eq!(
        serde_json::from_str::<Value>(&got).unwrap()["message"],
        "1 validation error detected: Value '-2147483648' at 'limit' failed to satisfy \
         constraint: Member must have value greater than or equal to 1"
    );
}

/// A number member DynamoDB's integer rule hasn't been captured for answers a
/// malformed token as it did before, a bare SerializationException, and no
/// answer quotes the number that stood in for it.
#[tokio::test]
async fn members_outside_the_integer_rule_refuse_a_malformed_token() {
    let url = start().await;
    let mut checked = 0;
    for token in ["1.5.5", "01", "-", "1e"] {
        let cases = [
            (
                "SearchVectors",
                format!(
                    r#"{{"TableName":"no_such_table","IndexName":"i","SearchVector":[{{"N":"1"}}],"TopK":{token}}}"#
                ),
            ),
            (
                "SearchVectors",
                format!(
                    r#"{{"TableName":"no_such_table","IndexName":"i","SearchVector":[{{"N":"1"}}],"TopK":3,"ExpressionAttributeValues":{{":v":{{"S":{token}}}}}}}"#
                ),
            ),
            (
                "CreateTable",
                format!(
                    r#"{{"TableName":"vectors","AttributeDefinitions":[{{"AttributeName":"pk","AttributeType":"S"}}],"KeySchema":[{{"AttributeName":"pk","KeyType":"HASH"}}],"BillingMode":"PAY_PER_REQUEST","VectorIndexes":[{{"IndexName":"v","VectorAttribute":{{"AttributeName":"vec"}},"Dimensions":{token},"DistanceFunction":"COSINE","Projection":{{"ProjectionType":"ALL"}}}}]}}"#
                ),
            ),
        ];
        for (operation, body) in cases {
            let (status, got) = send(&url, operation, &body).await;
            assert_eq!((status, got.as_str()), (400, BARE), "{operation} {body}");
            checked += 1;
        }
        let body = format!(r#"{{"ShardIterator":"x","Limit":{token}}}"#);
        let (status, got) = send_streams(&url, "GetRecords", &body).await;
        assert_eq!((status, got.as_str()), (400, BARE), "{body}");
        checked += 1;
    }
    assert_eq!(checked, 16);
}

async fn send_streams(url: &str, operation: &str, body: &str) -> (u16, String) {
    let resp = reqwest::Client::new()
        .post(url)
        .header("x-amz-target", format!("DynamoDBStreams_20120810.{operation}"))
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
    (status, resp.text().await.unwrap())
}

/// A body nested deeper than the parser follows is read a second way, and
/// its integer members read a malformed token there too: `-.5` answers as
/// `0.5` does.
#[tokio::test]
async fn a_body_nested_too_deep_reads_a_malformed_token_the_same_way() {
    let url = start().await;
    let deep = format!(
        "{}{{\"S\":\"leaf\"}}{}",
        "{\"M\":{\"n\":".repeat(200),
        "}}".repeat(200)
    );
    let body = |limit: &str| {
        format!(
            r#"{{"TableName":"no_such_table","KeyConditionExpression":"pk = :pk","ExpressionAttributeValues":{{":pk":{{"S":"a"}},":deep":{deep}}},"Limit":{limit}}}"#
        )
    };
    let (status, well_formed) = send(&url, "Query", &body("0.5")).await;
    assert_eq!(status, 400, "{well_formed}");
    assert!(
        well_formed.contains("Value '0' at 'limit'"),
        "{well_formed}"
    );
    let json = |text: &str| serde_json::from_str::<Value>(text).unwrap();
    let (_, malformed) = send(&url, "Query", &body("-.5")).await;
    assert_eq!(json(&malformed), json(&well_formed));
    let (_, refused) = send(&url, "Query", &body("1.5.5")).await;
    assert_eq!(
        json(&refused),
        json(
            r#"{"__type":"com.amazon.coral.service#SerializationException","Message":"Bad number format"}"#
        )
    );
}
