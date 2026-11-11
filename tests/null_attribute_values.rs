//! A JSON `null` where an attribute value belongs.
//!
//! DynamoDB refuses it as `The supplied Item contains a null AttributeValue`,
//! naming the attribute (`: pk is null`) when the null is a member of an item,
//! a key or a nested `M`, and naming nothing when it is a member of an `L`, a
//! PartiQL parameter or a legacy `AttributeValueList` member.
//! `ExpressionAttributeValues` and `ExclusiveStartKey` wrap it in their own
//! words. It is an ordinary value fault, met where DynamoDB's walk over the
//! request meets it: an attribute map's members in the order a Java `HashMap`
//! iterates them, a list's in written order, and an item or key before the
//! request's expression values. A null also puts the request on the older
//! validation layer, so a bad number beside it is refused bare.
//!
//! The rows are the requests sent to eu-west-2 and us-east-1 on 11 November
//! 2026 and the answers DynamoDB gave, which agree in both regions, held as
//! data captured from DynamoDB, not written by hand. Every request names a
//! table that doesn't exist. Answers are compared as parsed JSON, with the
//! HTTP status DynamoDB gives them: 200 for a BatchExecuteStatement, whose
//! statements carry their own errors, and 400 for the rest.

#![cfg(feature = "http-server")]

use dynoxide::Database;

/// A captured row: its number in the set, the operation, the request body and
/// DynamoDB's answer.
type Row = (usize, &'static str, &'static str, &'static str);

/// Rows whose answer turns on a difference that isn't the null's, which
/// Dynoxide doesn't follow yet. Most depend on the order a request's member
/// errors and value faults are reported in: DynamoDB reports a member error (a
/// missing `TableName`, a bad `ReturnValues`) ahead of every value fault but a
/// wrong JSON type, and Dynoxide reports the value fault. An empty value in
/// place of the null gets the same wrong order, so that gap is not the null's.
/// Each is still sent: it must still differ from DynamoDB's answer and give the
/// answer recorded here, which is Dynoxide's answer today.
type Excepted = (&'static str, usize, &'static str, &'static str);

const NOT_FOLLOWED_YET: &[Excepted] = &[
    (
        "first",
        19,
        "a bad ReturnValues beside a null",
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"The supplied Item contains a null AttributeValue: pk is null"}"#,
    ),
    (
        "first",
        20,
        "no TableName beside a null",
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"The supplied Item contains a null AttributeValue: pk is null"}"#,
    ),
    (
        "first",
        60,
        "a statement on a missing table echoes the table name",
        r#"{"Responses":[{"Error":{"Code":"ResourceNotFound","Message":"Requested resource not found"},"TableName":"_capture_20261111_317_none"},{"Error":{"Code":"ValidationError","Message":"The supplied Item contains a null AttributeValue"}}]}"#,
    ),
    (
        "second",
        25,
        "a bad ReturnValues beside an empty value",
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Supplied AttributeValue is empty, must contain exactly one of the supported datatypes"}"#,
    ),
    (
        "second",
        26,
        "no TableName beside an empty value",
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Supplied AttributeValue is empty, must contain exactly one of the supported datatypes"}"#,
    ),
    (
        "second",
        27,
        "a bad ReturnValues beside a bad number",
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: The parameter cannot be converted to a numeric value: abc"}"#,
    ),
    (
        "second",
        28,
        "no TableName beside a bad number",
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: The parameter cannot be converted to a numeric value: abc"}"#,
    ),
    (
        "second",
        31,
        "a bad ReturnValues beside a value with two types",
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes"}"#,
    ),
    (
        "second",
        32,
        "no TableName beside a value with two types",
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes"}"#,
    ),
    (
        "second",
        33,
        "a bad ReturnValues beside a null",
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"The supplied Item contains a null AttributeValue: pk is null"}"#,
    ),
    (
        "second",
        34,
        "no TableName beside a null",
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"The supplied Item contains a null AttributeValue: pk is null"}"#,
    ),
];

async fn start_server() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let db = Database::memory().unwrap();
    tokio::spawn(async move {
        dynoxide::server::serve_on(listener, db).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    url
}

async fn send(url: &str, operation: &str, body: &str) -> (u16, serde_json::Value) {
    let resp = reqwest::Client::new()
        .post(url)
        .header("x-amz-target", format!("DynamoDB_20120810.{operation}"))
        .header("content-type", "application/x-amz-json-1.0")
        .header(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=fakekey/20260101/us-east-1/dynamodb/aws4_request, \
             SignedHeaders=host;x-amz-date;x-amz-target, Signature=fakesig",
        )
        .header("x-amz-date", "20260101T000000Z")
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    let status = resp.status().as_u16();
    (status, resp.json().await.unwrap())
}

/// The status DynamoDB answers a body with: 400 for an exception, 200 for a
/// result.
fn status_of(answer: &serde_json::Value) -> u16 {
    if answer.get("__type").is_some() {
        400
    } else {
        200
    }
}

async fn assert_rows(set: &str, rows: &[Row]) {
    let url = start_server().await;
    let mut wrong = Vec::new();
    let mut compared = 0;
    let mut excepted_met = 0;
    for &(number, operation, body, answer) in rows {
        let expected: serde_json::Value = serde_json::from_str(answer).unwrap();
        let got = send(&url, operation, body).await;
        compared += 1;
        if let Some(&(_, _, reason, today)) = NOT_FOLLOWED_YET
            .iter()
            .find(|&&(s, n, _, _)| s == set && n == number)
        {
            excepted_met += 1;
            let today: serde_json::Value = serde_json::from_str(today).unwrap();
            if got == (status_of(&expected), expected.clone()) {
                wrong.push(format!(
                    "{set} row {number} ({reason}) now answers as DynamoDB did: \
                     drop it from NOT_FOLLOWED_YET"
                ));
            } else if got != (status_of(&today), today.clone()) {
                wrong.push(format!(
                    "{set} row {number}: {operation} {body}\n  expected {today}\n  got      {got:?}"
                ));
            }
            continue;
        }
        if got != (status_of(&expected), expected.clone()) {
            wrong.push(format!(
                "{set} row {number}: {operation} {body}\n  expected {expected}\n  got      {got:?}"
            ));
        }
    }
    assert_eq!(compared, rows.len());
    let excepted_listed = NOT_FOLLOWED_YET
        .iter()
        .filter(|&&(s, _, _, _)| s == set)
        .count();
    assert_eq!(
        excepted_met, excepted_listed,
        "NOT_FOLLOWED_YET lists {excepted_listed} {set} rows but {excepted_met} are in the set"
    );
    assert!(
        wrong.is_empty(),
        "{} of {compared} rows differ:\n{}",
        wrong.len(),
        wrong.join("\n")
    );
}

#[tokio::test]
async fn first_set_answers_as_dynamodb_did() {
    assert_eq!(FIRST.len(), 62);
    assert_rows("first", FIRST).await;
}

#[tokio::test]
async fn second_set_answers_as_dynamodb_did() {
    assert_eq!(SECOND.len(), 34);
    assert_rows("second", SECOND).await;
}

/// The library's own reader refuses a null in the same words as the wire.
#[test]
fn the_library_reader_refuses_a_null_in_the_same_words() {
    use dynoxide::types::AttributeValue;
    use std::collections::HashMap;
    let refusal = |json: &str| {
        serde_json::from_str::<HashMap<String, AttributeValue>>(json)
            .unwrap_err()
            .to_string()
    };
    for (json, message) in [
        (
            r#"{"m": {"M": {"a": null}}}"#,
            "The supplied Item contains a null AttributeValue: a is null",
        ),
        (
            r#"{"l": {"L": [null]}}"#,
            "The supplied Item contains a null AttributeValue",
        ),
    ] {
        let got = refusal(json);
        assert!(
            got.contains(message) && (message.ends_with("is null") || !got.contains("is null")),
            "{json}: {got}"
        );
    }
    let got = serde_json::from_str::<AttributeValue>("null")
        .unwrap_err()
        .to_string();
    assert!(
        got.contains("The supplied Item contains a null AttributeValue")
            && !got.contains("is null"),
        "{got}"
    );
    // An empty value is still the empty-value refusal.
    let got = serde_json::from_str::<AttributeValue>(r#"{"S": null}"#)
        .unwrap_err()
        .to_string();
    assert!(got.contains("Supplied AttributeValue is empty"), "{got}");
}

const FIRST: &[Row] = &[
    (
        1,
        "PutItem",
        r##"{"TableName": "_capture_20261111_317_none", "Item": {"pk": null}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The supplied Item contains a null AttributeValue: pk is null"}"##,
    ),
    (
        2,
        "PutItem",
        r##"{"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}, "x": null}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The supplied Item contains a null AttributeValue: x is null"}"##,
    ),
    (
        3,
        "PutItem",
        r##"{"TableName": "_capture_20261111_317_none", "Item": {"a": null, "b": null}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The supplied Item contains a null AttributeValue: a is null"}"##,
    ),
    (
        4,
        "PutItem",
        r##"{"TableName": "_capture_20261111_317_none", "Item": {"b": null, "a": null}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The supplied Item contains a null AttributeValue: a is null"}"##,
    ),
    (
        5,
        "PutItem",
        r##"{"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}, "m": {"M": {"a": null}}}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The supplied Item contains a null AttributeValue: a is null"}"##,
    ),
    (
        6,
        "PutItem",
        r##"{"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}, "l": {"L": [null]}}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The supplied Item contains a null AttributeValue"}"##,
    ),
    (
        7,
        "PutItem",
        r##"{"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}, "l": {"L": [{"S": "x"}, null]}}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The supplied Item contains a null AttributeValue"}"##,
    ),
    (
        8,
        "PutItem",
        r##"{"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}, "m": {"M": {"a": {"M": {"b": null}}}}}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The supplied Item contains a null AttributeValue: b is null"}"##,
    ),
    (
        9,
        "PutItem",
        r##"{"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": null}}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"Supplied AttributeValue is empty, must contain exactly one of the supported datatypes"}"##,
    ),
    (
        10,
        "PutItem",
        r##"{"TableName": "_capture_20261111_317_none", "Item": {"pk": {}}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"Supplied AttributeValue is empty, must contain exactly one of the supported datatypes"}"##,
    ),
    (
        11,
        "PutItem",
        r##"{"TableName": "_capture_20261111_317_none", "Item": {"pk": null, "x": {"N": "abc"}}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The parameter cannot be converted to a numeric value: abc"}"##,
    ),
    (
        12,
        "PutItem",
        r##"{"TableName": "_capture_20261111_317_none", "Item": {"x": {"N": "abc"}, "pk": null}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The parameter cannot be converted to a numeric value: abc"}"##,
    ),
    (
        13,
        "PutItem",
        r##"{"TableName": "_capture_20261111_317_none", "Item": {"pk": null, "x": {"N": 1}}}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"NUMBER_VALUE cannot be converted to String"}"##,
    ),
    (
        14,
        "PutItem",
        r##"{"TableName": "_capture_20261111_317_none", "Item": {"x": {"N": 1}, "pk": null}}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"NUMBER_VALUE cannot be converted to String"}"##,
    ),
    (
        15,
        "PutItem",
        r##"{"TableName": "_capture_20261111_317_none", "Item": {"a": {}, "b": null}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"Supplied AttributeValue is empty, must contain exactly one of the supported datatypes"}"##,
    ),
    (
        16,
        "PutItem",
        r##"{"TableName": "_capture_20261111_317_none", "Item": {"b": null, "a": {}}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"Supplied AttributeValue is empty, must contain exactly one of the supported datatypes"}"##,
    ),
    (
        17,
        "PutItem",
        r##"{"TableName": "_capture_20261111_317_none", "Item": {"pk": null, "x": {"S": "a", "N": "1"}}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes"}"##,
    ),
    (
        18,
        "PutItem",
        r##"{"TableName": "_capture_20261111_317_none", "Item": {"pk": null, "x": {"NULL": false}}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"One or more parameter values were invalid: Null attribute value types must have the value of true"}"##,
    ),
    (
        19,
        "PutItem",
        r##"{"TableName": "_capture_20261111_317_none", "Item": {"pk": null}, "ReturnValues": "BAD"}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value 'BAD' at 'returnValues' failed to satisfy constraint: Member must satisfy enum value set: [ALL_NEW, UPDATED_OLD, ALL_OLD, NONE, UPDATED_NEW]"}"##,
    ),
    (
        20,
        "PutItem",
        r##"{"Item": {"pk": null}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value null at 'tableName' failed to satisfy constraint: Member must not be null"}"##,
    ),
    (
        21,
        "PutItem",
        r##"{"TableName": "_capture_20261111_317_none", "Item": null}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value null at 'item' failed to satisfy constraint: Member must not be null"}"##,
    ),
    (
        22,
        "PutItem",
        r##"{"TableName": "_capture_20261111_317_none"}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value null at 'item' failed to satisfy constraint: Member must not be null"}"##,
    ),
    (
        23,
        "PutItem",
        r##"{"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}, "ConditionExpression": "a = :v", "ExpressionAttributeValues": {":v": null}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"ExpressionAttributeValues contains invalid value: The supplied Item contains a null AttributeValue for key :v"}"##,
    ),
    (
        24,
        "PutItem",
        r##"{"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}, "ConditionExpression": "a = :v", "ExpressionAttributeValues": null}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Invalid ConditionExpression: An expression attribute value used in expression is not defined; attribute value: :v"}"##,
    ),
    (
        25,
        "PutItem",
        r##"{"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}, "Expected": {"a": {"Value": null}}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: One or more parameter values were invalid: Value must be provided when Exists is null for Attribute: a"}"##,
    ),
    (
        26,
        "PutItem",
        r##"{"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}, "Expected": {"a": {"AttributeValueList": [null], "ComparisonOperator": "EQ"}}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The supplied Item contains a null AttributeValue"}"##,
    ),
    (
        27,
        "PutItem",
        r##"{"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}, "Expected": {"a": null}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"One or more parameter values were invalid: ExpectedAttributeValue must not be null for Attribute: a"}"##,
    ),
    (
        28,
        "GetItem",
        r##"{"TableName": "_capture_20261111_317_none", "Key": {"pk": null}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The supplied Item contains a null AttributeValue: pk is null"}"##,
    ),
    (
        29,
        "GetItem",
        r##"{"TableName": "_capture_20261111_317_none", "Key": null}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value null at 'key' failed to satisfy constraint: Member must not be null"}"##,
    ),
    (
        30,
        "DeleteItem",
        r##"{"TableName": "_capture_20261111_317_none", "Key": {"pk": null}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The supplied Item contains a null AttributeValue: pk is null"}"##,
    ),
    (
        31,
        "DeleteItem",
        r##"{"TableName": "_capture_20261111_317_none", "Key": null}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value null at 'key' failed to satisfy constraint: Member must not be null"}"##,
    ),
    (
        32,
        "UpdateItem",
        r##"{"TableName": "_capture_20261111_317_none", "Key": {"pk": null}, "UpdateExpression": "SET a = :v", "ExpressionAttributeValues": {":v": {"S": "x"}}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The supplied Item contains a null AttributeValue: pk is null"}"##,
    ),
    (
        33,
        "UpdateItem",
        r##"{"TableName": "_capture_20261111_317_none", "Key": {"pk": {"S": "a"}}, "UpdateExpression": "SET a = :v", "ExpressionAttributeValues": {":v": null}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"ExpressionAttributeValues contains invalid value: The supplied Item contains a null AttributeValue for key :v"}"##,
    ),
    (
        34,
        "UpdateItem",
        r##"{"TableName": "_capture_20261111_317_none", "Key": {"pk": {"S": "a"}}, "UpdateExpression": "SET a = :v, b = :w", "ExpressionAttributeValues": {":w": null, ":v": null}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"ExpressionAttributeValues contains invalid value: The supplied Item contains a null AttributeValue for key :v"}"##,
    ),
    (
        35,
        "UpdateItem",
        r##"{"TableName": "_capture_20261111_317_none", "Key": {"pk": {"S": "a"}}, "AttributeUpdates": {"a": {"Value": null, "Action": "PUT"}}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: One or more parameter values were invalid: Only DELETE action is allowed when no attribute value is specified"}"##,
    ),
    (
        36,
        "UpdateItem",
        r##"{"TableName": "_capture_20261111_317_none", "Key": {"pk": {"S": "a"}}, "AttributeUpdates": {"a": null}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"Supplied AttributeValueUpdate is empty, must contain one of the supported actions: a is null"}"##,
    ),
    (
        37,
        "Query",
        r##"{"TableName": "_capture_20261111_317_none", "KeyConditionExpression": "pk = :v", "ExpressionAttributeValues": {":v": null}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"ExpressionAttributeValues contains invalid value: The supplied Item contains a null AttributeValue for key :v"}"##,
    ),
    (
        38,
        "Query",
        r##"{"TableName": "_capture_20261111_317_none", "KeyConditionExpression": "pk = :v", "ExpressionAttributeValues": {":v": {"S": "a"}}, "ExclusiveStartKey": {"pk": null}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The provided starting key is invalid: The supplied Item contains a null AttributeValue: pk is null"}"##,
    ),
    (
        39,
        "Query",
        r##"{"TableName": "_capture_20261111_317_none", "KeyConditions": {"pk": {"AttributeValueList": [null], "ComparisonOperator": "EQ"}}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The supplied Item contains a null AttributeValue"}"##,
    ),
    (
        40,
        "Query",
        r##"{"TableName": "_capture_20261111_317_none", "KeyConditions": {"pk": null}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The filter condition for an attribute cannot be null: pk"}"##,
    ),
    (
        41,
        "Scan",
        r##"{"TableName": "_capture_20261111_317_none", "ExclusiveStartKey": {"pk": null}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The provided starting key is invalid: The supplied Item contains a null AttributeValue: pk is null"}"##,
    ),
    (
        42,
        "Scan",
        r##"{"TableName": "_capture_20261111_317_none", "ScanFilter": {"a": {"AttributeValueList": [null], "ComparisonOperator": "EQ"}}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The supplied Item contains a null AttributeValue"}"##,
    ),
    (
        43,
        "Scan",
        r##"{"TableName": "_capture_20261111_317_none", "FilterExpression": "a = :v", "ExpressionAttributeValues": {":v": null}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"ExpressionAttributeValues contains invalid value: The supplied Item contains a null AttributeValue for key :v"}"##,
    ),
    (
        44,
        "BatchWriteItem",
        r##"{"RequestItems": {"_capture_20261111_317_none": [{"PutRequest": {"Item": {"pk": null}}}]}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The supplied Item contains a null AttributeValue: pk is null"}"##,
    ),
    (
        45,
        "BatchWriteItem",
        r##"{"RequestItems": {"_capture_20261111_317_none": [{"DeleteRequest": {"Key": {"pk": null}}}]}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The supplied Item contains a null AttributeValue: pk is null"}"##,
    ),
    (
        46,
        "BatchWriteItem",
        r##"{"RequestItems": {"_capture_20261111_317_none": [{"PutRequest": {"Item": {"pk": {"S": "a"}}}}, {"PutRequest": {"Item": {"pk": {"S": "b"}, "x": null}}}]}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The supplied Item contains a null AttributeValue: x is null"}"##,
    ),
    (
        47,
        "BatchGetItem",
        r##"{"RequestItems": {"_capture_20261111_317_none": {"Keys": [{"pk": null}]}}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The supplied Item contains a null AttributeValue: pk is null"}"##,
    ),
    (
        48,
        "TransactWriteItems",
        r##"{"TransactItems": [{"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": null}}}]}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The supplied Item contains a null AttributeValue: pk is null"}"##,
    ),
    (
        49,
        "TransactWriteItems",
        r##"{"TransactItems": [{"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}}}, {"Delete": {"TableName": "_capture_20261111_317_none", "Key": {"pk": null}}}]}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The supplied Item contains a null AttributeValue: pk is null"}"##,
    ),
    (
        50,
        "TransactWriteItems",
        r##"{"TransactItems": [{"Update": {"TableName": "_capture_20261111_317_none", "Key": {"pk": {"S": "a"}}, "UpdateExpression": "SET a = :v", "ExpressionAttributeValues": {":v": null}}}]}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"ExpressionAttributeValues contains invalid value: The supplied Item contains a null AttributeValue for key :v"}"##,
    ),
    (
        51,
        "TransactGetItems",
        r##"{"TransactItems": [{"Get": {"TableName": "_capture_20261111_317_none", "Key": {"pk": null}}}]}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The supplied Item contains a null AttributeValue: pk is null"}"##,
    ),
    (
        52,
        "ExecuteStatement",
        r##"{"Statement": "SELECT * FROM \"_capture_20261111_317_none\" WHERE pk = ?", "Parameters": [null]}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The supplied Item contains a null AttributeValue"}"##,
    ),
    (
        53,
        "ExecuteStatement",
        r##"{"Statement": "SELECT * FROM \"_capture_20261111_317_none\" WHERE pk = ? AND a = ?", "Parameters": [{"S": "a"}, null]}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The supplied Item contains a null AttributeValue"}"##,
    ),
    (
        54,
        "ExecuteStatement",
        r##"{"Statement": "SELECT * FROM \"_capture_20261111_317_none\" WHERE pk = ?", "Parameters": [null], "Limit": 0}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value '0' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1"}"##,
    ),
    (
        55,
        "ExecuteStatement",
        r##"{"Statement": "SELECT * FROM \"_capture_20261111_317_none\" WHERE pk = ? AND a = ?", "Parameters": [{"N": "abc"}, null]}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The parameter cannot be converted to a numeric value: abc"}"##,
    ),
    (
        56,
        "ExecuteStatement",
        r##"{"Statement": "SELECT * FROM \"_capture_20261111_317_none\" WHERE pk = ? AND a = ?", "Parameters": [null, {"N": "abc"}]}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The supplied Item contains a null AttributeValue"}"##,
    ),
    (
        57,
        "ExecuteStatement",
        r##"{"Statement": "SELECT * FROM \"_capture_20261111_317_none\" WHERE pk = ?", "Parameters": [{"M": {"a": null}}]}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The supplied Item contains a null AttributeValue: a is null"}"##,
    ),
    (
        58,
        "ExecuteStatement",
        r##"{"Statement": "SELECT * FROM \"_capture_20261111_317_none\" WHERE pk = ?", "Parameters": [{"L": [null]}]}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The supplied Item contains a null AttributeValue"}"##,
    ),
    (
        59,
        "BatchExecuteStatement",
        r##"{"Statements": [{"Statement": "SELECT * FROM \"_capture_20261111_317_none\" WHERE pk = ?", "Parameters": [null]}]}"##,
        r##"{"Responses":[{"Error":{"Code":"ValidationError","Message":"The supplied Item contains a null AttributeValue"}}]}"##,
    ),
    (
        60,
        "BatchExecuteStatement",
        r##"{"Statements": [{"Statement": "SELECT * FROM \"_capture_20261111_317_none\" WHERE pk = ?", "Parameters": [{"S": "a"}]}, {"Statement": "SELECT * FROM \"_capture_20261111_317_none\" WHERE pk = ?", "Parameters": [null]}]}"##,
        r##"{"Responses":[{"Error":{"Code":"ResourceNotFound","Message":"Requested resource not found"}},{"Error":{"Code":"ValidationError","Message":"The supplied Item contains a null AttributeValue"}}]}"##,
    ),
    (
        61,
        "ExecuteTransaction",
        r##"{"TransactStatements": [{"Statement": "SELECT * FROM \"_capture_20261111_317_none\" WHERE pk = ?", "Parameters": [null]}]}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The supplied Item contains a null AttributeValue"}"##,
    ),
    (
        62,
        "ExecuteTransaction",
        r##"{"TransactStatements": [{"Statement": "SELECT * FROM \"_capture_20261111_317_none\" WHERE pk = ?", "Parameters": [{"S": "a"}]}, {"Statement": "SELECT * FROM \"_capture_20261111_317_none\" WHERE pk = ?", "Parameters": [null]}]}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The supplied Item contains a null AttributeValue"}"##,
    ),
];

const SECOND: &[Row] = &[
    (
        1,
        "PutItem",
        r##"{"TableName": "_capture_20261111_317_none", "Item": {"b": null, "q": null}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The supplied Item contains a null AttributeValue: q is null"}"##,
    ),
    (
        2,
        "PutItem",
        r##"{"TableName": "_capture_20261111_317_none", "Item": {"q": null, "b": null}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The supplied Item contains a null AttributeValue: q is null"}"##,
    ),
    (
        3,
        "PutItem",
        r##"{"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}, "ConditionExpression": "a = :b AND c = :q", "ExpressionAttributeValues": {":b": null, ":q": null}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"ExpressionAttributeValues contains invalid value: The supplied Item contains a null AttributeValue for key :b"}"##,
    ),
    (
        4,
        "PutItem",
        r##"{"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}, "ConditionExpression": "a = :b AND c = :q", "ExpressionAttributeValues": {":q": null, ":b": null}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"ExpressionAttributeValues contains invalid value: The supplied Item contains a null AttributeValue for key :b"}"##,
    ),
    (
        5,
        "PutItem",
        r##"{"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}, "m": {"M": {"b": null, "q": null}}}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The supplied Item contains a null AttributeValue: q is null"}"##,
    ),
    (
        6,
        "PutItem",
        r##"{"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}, "l": {"L": [null, {"N": "abc"}]}}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The supplied Item contains a null AttributeValue"}"##,
    ),
    (
        7,
        "PutItem",
        r##"{"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}, "l": {"L": [{"N": "abc"}, null]}}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The parameter cannot be converted to a numeric value: abc"}"##,
    ),
    (
        8,
        "PutItem",
        r##"{"TableName": "_capture_20261111_317_none", "Item": {"pk": null}, "ConditionExpression": "a = :v", "ExpressionAttributeValues": {":v": {"N": "abc"}}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The supplied Item contains a null AttributeValue: pk is null"}"##,
    ),
    (
        9,
        "PutItem",
        r##"{"TableName": "_capture_20261111_317_none", "ConditionExpression": "a = :v", "ExpressionAttributeValues": {":v": {"N": "abc"}}, "Item": {"pk": null}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The supplied Item contains a null AttributeValue: pk is null"}"##,
    ),
    (
        10,
        "PutItem",
        r##"{"TableName": "_capture_20261111_317_none", "Item": {"pk": {"N": "abc"}}, "ConditionExpression": "a = :v", "ExpressionAttributeValues": {":v": null}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The parameter cannot be converted to a numeric value: abc"}"##,
    ),
    (
        11,
        "PutItem",
        r##"{"TableName": "_capture_20261111_317_none", "ConditionExpression": "a = :v", "ExpressionAttributeValues": {":v": null}, "Item": {"pk": {"N": "abc"}}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The parameter cannot be converted to a numeric value: abc"}"##,
    ),
    (
        12,
        "PutItem",
        r##"{"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}, "ConditionExpression": "a = :v AND b = :w", "ExpressionAttributeValues": {":v": null, ":w": {"N": "abc"}}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"ExpressionAttributeValues contains invalid value: The supplied Item contains a null AttributeValue for key :v"}"##,
    ),
    (
        13,
        "PutItem",
        r##"{"TableName": "_capture_20261111_317_none", "Item": {"pk": null}, "ConditionExpression": "a = :v", "ExpressionAttributeValues": {":v": null}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The supplied Item contains a null AttributeValue: pk is null"}"##,
    ),
    (
        14,
        "PutItem",
        r##"{"TableName": "_capture_20261111_317_none", "Item": {"pk": null}, "Expected": {"a": {"Value": {"N": "abc"}}}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The supplied Item contains a null AttributeValue: pk is null"}"##,
    ),
    (
        15,
        "UpdateItem",
        r##"{"TableName": "_capture_20261111_317_none", "Key": {"pk": null}, "Expected": {"a": {"Value": {"N": "abc"}}}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The supplied Item contains a null AttributeValue: pk is null"}"##,
    ),
    (
        16,
        "UpdateItem",
        r##"{"TableName": "_capture_20261111_317_none", "Key": {"pk": null}, "UpdateExpression": "SET a = :v", "ExpressionAttributeValues": {":v": {"N": "abc"}}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The supplied Item contains a null AttributeValue: pk is null"}"##,
    ),
    (
        17,
        "BatchWriteItem",
        r##"{"RequestItems": {"_capture_20261111_317_none": [{"PutRequest": {"Item": {"pk": null}}}, {"PutRequest": {"Item": {"pk": {"N": "abc"}}}}]}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The supplied Item contains a null AttributeValue: pk is null"}"##,
    ),
    (
        18,
        "BatchWriteItem",
        r##"{"RequestItems": {"_capture_20261111_317_none": [{"PutRequest": {"Item": {"pk": {"N": "abc"}}}}, {"PutRequest": {"Item": {"pk": null}}}]}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The parameter cannot be converted to a numeric value: abc"}"##,
    ),
    (
        19,
        "TransactWriteItems",
        r##"{"TransactItems": [{"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": null}}}, {"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"N": "abc"}}}}]}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The supplied Item contains a null AttributeValue: pk is null"}"##,
    ),
    (
        20,
        "TransactWriteItems",
        r##"{"TransactItems": [{"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": {"N": "abc"}}}}, {"Put": {"TableName": "_capture_20261111_317_none", "Item": {"pk": null}}}]}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The parameter cannot be converted to a numeric value: abc"}"##,
    ),
    (
        21,
        "GetItem",
        r##"{"TableName": "_capture_20261111_317_none", "Key": {"pk": null, "sk": {"N": "abc"}}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The parameter cannot be converted to a numeric value: abc"}"##,
    ),
    (
        22,
        "GetItem",
        r##"{"TableName": "_capture_20261111_317_none", "Key": {"sk": {"N": "abc"}, "pk": null}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The parameter cannot be converted to a numeric value: abc"}"##,
    ),
    (
        23,
        "Query",
        r##"{"TableName": "_capture_20261111_317_none", "KeyConditionExpression": "pk = :v AND sk = :w", "ExpressionAttributeValues": {":v": null, ":w": {"N": "abc"}}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"ExpressionAttributeValues contains invalid value: The supplied Item contains a null AttributeValue for key :v"}"##,
    ),
    (
        24,
        "Query",
        r##"{"TableName": "_capture_20261111_317_none", "KeyConditionExpression": "pk = :v", "ExpressionAttributeValues": {":v": {"N": "abc"}}, "ExclusiveStartKey": {"pk": null}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"The provided starting key is invalid: The supplied Item contains a null AttributeValue: pk is null"}"##,
    ),
    (
        25,
        "PutItem",
        r##"{"TableName": "_capture_20261111_317_none", "Item": {"pk": {}}, "ReturnValues": "BAD"}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value 'BAD' at 'returnValues' failed to satisfy constraint: Member must satisfy enum value set: [ALL_NEW, UPDATED_OLD, ALL_OLD, NONE, UPDATED_NEW]"}"##,
    ),
    (
        26,
        "PutItem",
        r##"{"Item": {"pk": {}}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value null at 'tableName' failed to satisfy constraint: Member must not be null"}"##,
    ),
    (
        27,
        "PutItem",
        r##"{"TableName": "_capture_20261111_317_none", "Item": {"pk": {"N": "abc"}}, "ReturnValues": "BAD"}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Failed to satisfy constraint: Member must satisfy enum value set: [ALL_OLD, UPDATED_OLD, ALL_NEW, UPDATE_NEW, NONE]"}"##,
    ),
    (
        28,
        "PutItem",
        r##"{"Item": {"pk": {"N": "abc"}}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value null at 'tableName' failed to satisfy constraint: Member must not be null"}"##,
    ),
    (
        29,
        "PutItem",
        r##"{"TableName": "_capture_20261111_317_none", "Item": {"pk": {"N": 1}}, "ReturnValues": "BAD"}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"NUMBER_VALUE cannot be converted to String"}"##,
    ),
    (
        30,
        "PutItem",
        r##"{"Item": {"pk": {"N": 1}}}"##,
        r##"{"__type":"com.amazon.coral.service#SerializationException","Message":"NUMBER_VALUE cannot be converted to String"}"##,
    ),
    (
        31,
        "PutItem",
        r##"{"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a", "N": "1"}}, "ReturnValues": "BAD"}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value 'BAD' at 'returnValues' failed to satisfy constraint: Member must satisfy enum value set: [ALL_NEW, UPDATED_OLD, ALL_OLD, NONE, UPDATED_NEW]"}"##,
    ),
    (
        32,
        "PutItem",
        r##"{"Item": {"pk": {"S": "a", "N": "1"}}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value null at 'tableName' failed to satisfy constraint: Member must not be null"}"##,
    ),
    (
        33,
        "PutItem",
        r##"{"TableName": "_capture_20261111_317_none", "Item": {"pk": null}, "ReturnValues": "BAD"}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value 'BAD' at 'returnValues' failed to satisfy constraint: Member must satisfy enum value set: [ALL_NEW, UPDATED_OLD, ALL_OLD, NONE, UPDATED_NEW]"}"##,
    ),
    (
        34,
        "PutItem",
        r##"{"Item": {"pk": null}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value null at 'tableName' failed to satisfy constraint: Member must not be null"}"##,
    ),
];
