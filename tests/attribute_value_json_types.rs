//! An attribute value member given the wrong JSON type: a number where `S`
//! belongs, a string where `L` belongs, a boolean where `B` belongs and so on.
//!
//! DynamoDB refuses each with a `SerializationException` whose message depends
//! only on the JSON type it found and the type it expected, whichever request
//! the value sits in. `CAPTURED` holds the 75 requests sent to eu-west-2 and
//! us-east-1 on 8 November 2026, with the answers both regions gave; `None` is
//! a `SerializationException` with no message. They are data captured from
//! DynamoDB, not written by hand.

#![cfg(feature = "http-server")]

use dynoxide::Database;
use dynoxide::actions::create_table::CreateTableRequest;

async fn start_server() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let db = Database::memory().unwrap();
    db.create_table(
        serde_json::from_value::<CreateTableRequest>(serde_json::json!({
            "TableName": "Tab",
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

/// The exception type and message a request is refused with, or `None` for
/// one that succeeds. A message of `None` is an exception with no message.
async fn answer(url: &str, operation: &str, body: &str) -> Option<(String, Option<String>)> {
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
    if resp.status() == 200 {
        return None;
    }
    let json: serde_json::Value = resp.json().await.unwrap();
    let kind = json["__type"]
        .as_str()
        .unwrap()
        .rsplit('#')
        .next()
        .unwrap()
        .to_string();
    let message = json
        .get("Message")
        .or_else(|| json.get("message"))
        .and_then(|m| m.as_str())
        .map(str::to_string);
    Some((kind, message))
}

async fn assert_serialization(url: &str, cases: &[(&str, &str, Option<&str>)]) {
    let mut wrong = Vec::new();
    for &(operation, body, expected) in cases {
        let expected = Some((
            "SerializationException".to_string(),
            expected.map(str::to_string),
        ));
        let got = answer(url, operation, body).await;
        if got != expected {
            wrong.push(format!(
                "{operation} {body}\n  expected {expected:?}\n  got      {got:?}"
            ));
        }
    }
    assert!(
        wrong.is_empty(),
        "{} of {} cases differ:\n{}",
        wrong.len(),
        cases.len(),
        wrong.join("\n")
    );
}

/// Captured in eu-west-2 and us-east-1, 8 November 2026; both regions gave
/// these answers to every request.
const CAPTURED: &[(&str, &str, Option<&str>)] = &[
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":5}}}"#,
        Some("NUMBER_VALUE cannot be converted to String"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":1.5}}}"#,
        Some("DECIMAL_VALUE cannot be converted to String"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":1e400}}}"#,
        Some("DECIMAL_VALUE cannot be converted to String"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":-0}}}"#,
        Some("NUMBER_VALUE cannot be converted to String"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":99999999999999999999999}}}"#,
        Some("NUMBER_VALUE cannot be converted to String"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"S":5}}}"#,
        Some("NUMBER_VALUE cannot be converted to String"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"S":[]}}}"#,
        None,
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"S":{}}}}"#,
        Some("Start of structure or map found where not expected"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"N":5}}}"#,
        Some("NUMBER_VALUE cannot be converted to String"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"N":1.5}}}"#,
        Some("DECIMAL_VALUE cannot be converted to String"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"N":true}}}"#,
        Some("TRUE_VALUE cannot be converted to String"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"N":[]}}}"#,
        None,
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"N":{}}}}"#,
        Some("Start of structure or map found where not expected"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"B":5}}}"#,
        Some("only base-64-encoded strings are convertible to bytes"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"B":1.5}}}"#,
        Some("only base-64-encoded strings are convertible to bytes"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"B":true}}}"#,
        Some("only base-64-encoded strings are convertible to bytes"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"SS":5}}}"#,
        Some("Unexpected field type"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"SS":"a"}}}"#,
        Some("Unexpected field type"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"SS":[5]}}}"#,
        Some("NUMBER_VALUE cannot be converted to String"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"SS":[1.5]}}}"#,
        Some("DECIMAL_VALUE cannot be converted to String"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"SS":[true]}}}"#,
        Some("TRUE_VALUE cannot be converted to String"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"NS":[5]}}}"#,
        Some("NUMBER_VALUE cannot be converted to String"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"NS":[1.5]}}}"#,
        Some("DECIMAL_VALUE cannot be converted to String"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"NS":5}}}"#,
        Some("Unexpected field type"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"BS":[5]}}}"#,
        Some("only base-64-encoded strings are convertible to bytes"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"L":5}}}"#,
        Some("Unexpected field type"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"L":"a"}}}"#,
        Some("Unexpected field type"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"L":{}}}}"#,
        Some("Start of structure or map found where not expected"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"M":5}}}"#,
        Some("Unexpected field type"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"M":"a"}}}"#,
        Some("Unexpected field type"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"M":[]}}}"#,
        None,
    ),
    (
        "GetItem",
        r#"{"TableName":"Tab","Key":{"pk":{"S":5}}}"#,
        Some("NUMBER_VALUE cannot be converted to String"),
    ),
    (
        "GetItem",
        r#"{"TableName":"Tab","Key":{"pk":{"S":1e400}}}"#,
        Some("DECIMAL_VALUE cannot be converted to String"),
    ),
    (
        "Scan",
        r#"{"TableName":"Tab","ExclusiveStartKey":{"pk":{"S":5}}}"#,
        Some("NUMBER_VALUE cannot be converted to String"),
    ),
    (
        "Scan",
        r#"{"TableName":"Tab","ExclusiveStartKey":{"pk":{"S":1.5}}}"#,
        Some("DECIMAL_VALUE cannot be converted to String"),
    ),
    (
        "Scan",
        r#"{"TableName":"Tab","ExclusiveStartKey":{"pk":{"S":1e400}}}"#,
        Some("DECIMAL_VALUE cannot be converted to String"),
    ),
    (
        "Scan",
        r#"{"TableName":"Tab","ExclusiveStartKey":{"pk":{"N":5}}}"#,
        Some("NUMBER_VALUE cannot be converted to String"),
    ),
    (
        "Scan",
        r#"{"TableName":"Tab","ExclusiveStartKey":{"pk":{"S":true}}}"#,
        Some("TRUE_VALUE cannot be converted to String"),
    ),
    (
        "Query",
        r#"{"TableName":"Tab","KeyConditionExpression":"pk = :v","ExpressionAttributeValues":{":v":{"S":5}}}"#,
        Some("NUMBER_VALUE cannot be converted to String"),
    ),
    (
        "Scan",
        r#"{"TableName":"Tab","FilterExpression":"x = :v","ExpressionAttributeValues":{":v":{"S":1.5}}}"#,
        Some("DECIMAL_VALUE cannot be converted to String"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"B":[]}}}"#,
        None,
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"B":{}}}}"#,
        Some("Start of structure or map found where not expected"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"S":false}}}"#,
        Some("FALSE_VALUE cannot be converted to String"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"N":false}}}"#,
        Some("FALSE_VALUE cannot be converted to String"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"BS":5}}}"#,
        Some("Unexpected field type"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"BS":"a"}}}"#,
        Some("Unexpected field type"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"BS":{}}}}"#,
        Some("Start of structure or map found where not expected"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"SS":{}}}}"#,
        Some("Start of structure or map found where not expected"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"NS":{}}}}"#,
        Some("Start of structure or map found where not expected"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"BS":[true]}}}"#,
        Some("only base-64-encoded strings are convertible to bytes"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"BS":[[]]}}}"#,
        None,
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"SS":[[]]}}}"#,
        None,
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"SS":[{}]}}}"#,
        Some("Start of structure or map found where not expected"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"NS":[true]}}}"#,
        Some("TRUE_VALUE cannot be converted to String"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"NS":[[]]}}}"#,
        None,
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"L":[5]}}}"#,
        Some("Unexpected value type in payload"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"L":["a"]}}}"#,
        Some("Unexpected value type in payload"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"L":[true]}}}"#,
        Some("Unexpected value type in payload"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"L":[[]]}}}"#,
        None,
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"M":{"a":5}}}}"#,
        Some("Unexpected value type in payload"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"M":{"a":"x"}}}}"#,
        Some("Unexpected value type in payload"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"M":{"a":[]}}}}"#,
        None,
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"SS":[-0]}}}"#,
        Some("NUMBER_VALUE cannot be converted to String"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"NS":[1e400]}}}"#,
        Some("DECIMAL_VALUE cannot be converted to String"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"NS":[99999999999999999999]}}}"#,
        Some("NUMBER_VALUE cannot be converted to String"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"N":-0}}}"#,
        Some("NUMBER_VALUE cannot be converted to String"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"N":1e400}}}"#,
        Some("DECIMAL_VALUE cannot be converted to String"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"BOOL":true,"S":5}}}"#,
        Some("NUMBER_VALUE cannot be converted to String"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"S":5,"N":"1"}}}"#,
        Some("NUMBER_VALUE cannot be converted to String"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":5}}"#,
        Some("Unexpected value type in payload"),
    ),
    (
        "Query",
        r#"{"TableName":"Tab","KeyConditionExpression":"pk = :v","ExpressionAttributeValues":{":v":{"S":"a"}},"ExclusiveStartKey":{"pk":{"S":5}}}"#,
        Some("NUMBER_VALUE cannot be converted to String"),
    ),
    (
        "BatchWriteItem",
        r#"{"RequestItems":{"Tab":[{"PutRequest":{"Item":{"pk":{"S":5}}}}]}}"#,
        Some("NUMBER_VALUE cannot be converted to String"),
    ),
    (
        "TransactWriteItems",
        r#"{"TransactItems":[{"Put":{"TableName":"Tab","Item":{"pk":{"S":1.5}}}}]}"#,
        Some("DECIMAL_VALUE cannot be converted to String"),
    ),
    (
        "ExecuteStatement",
        r#"{"Statement":"SELECT * FROM \"Tab\" WHERE pk = ?","Parameters":[{"S":5}]}"#,
        Some("NUMBER_VALUE cannot be converted to String"),
    ),
    (
        "UpdateItem",
        r#"{"TableName":"Tab","Key":{"pk":{"S":"a"}},"UpdateExpression":"SET x = :v","ExpressionAttributeValues":{":v":{"SS":[1.5]}}}"#,
        Some("DECIMAL_VALUE cannot be converted to String"),
    ),
    // Values with more than one fault, captured the same way later the same day.
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"S":5,"N":"abc"}}}"#,
        Some("NUMBER_VALUE cannot be converted to String"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"N":"abc","S":5}}}"#,
        Some("NUMBER_VALUE cannot be converted to String"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"SS":[],"S":5}}}"#,
        Some("NUMBER_VALUE cannot be converted to String"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"NULL":false,"S":5}}}"#,
        Some("NUMBER_VALUE cannot be converted to String"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"S":5,"NULL":false}}}"#,
        Some("NUMBER_VALUE cannot be converted to String"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"NS":["x",5]}}}"#,
        Some("NUMBER_VALUE cannot be converted to String"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"NS":[5,"x"]}}}"#,
        Some("NUMBER_VALUE cannot be converted to String"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"BOOL":"x","S":5}}}"#,
        Some("Unexpected token received from parser"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"L":[1e400]}}}"#,
        Some("Unexpected value type in payload"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"M":{"a":1e400}}}}"#,
        Some("Unexpected value type in payload"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"L":[-0]}}}"#,
        Some("Unexpected value type in payload"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"B":true,"S":5}}}"#,
        Some("only base-64-encoded strings are convertible to bytes"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"}},"Expected":{"a":{"Value":{"S":5}}}}"#,
        Some("NUMBER_VALUE cannot be converted to String"),
    ),
    (
        "Scan",
        r#"{"TableName":"Tab","ScanFilter":{"a":{"ComparisonOperator":"EQ","AttributeValueList":[{"S":5}]}}}"#,
        Some("NUMBER_VALUE cannot be converted to String"),
    ),
    (
        "Query",
        r#"{"TableName":"Tab","KeyConditions":{"pk":{"ComparisonOperator":"EQ","AttributeValueList":[{"S":5}]}}}"#,
        Some("NUMBER_VALUE cannot be converted to String"),
    ),
    (
        "Scan",
        r#"{"TableName":"Tab","Select":"BAD","ExclusiveStartKey":{"pk":{"S":5}}}"#,
        Some("NUMBER_VALUE cannot be converted to String"),
    ),
    (
        "Scan",
        r#"{"TableName":"Tab","FilterExpression":"a = :v","ExpressionAttributeValues":{":v":{"S":5}},"Limit":0}"#,
        Some("NUMBER_VALUE cannot be converted to String"),
    ),
    (
        "Scan",
        r#"{"TableName":"Tab","FilterExpression":"a = :v AND b = :w","ExpressionAttributeValues":{":w":{"S":5},":v":{"N":"abc"}}}"#,
        Some("NUMBER_VALUE cannot be converted to String"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"y":{"S":5},"x":{"N":"abc"}}}"#,
        Some("NUMBER_VALUE cannot be converted to String"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"L":[{"S":5}],"N":"1"}}}"#,
        Some("NUMBER_VALUE cannot be converted to String"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"M":{"a":{"S":true}},"SS":[]}}}"#,
        Some("TRUE_VALUE cannot be converted to String"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"L":[{"L":[{"S":5}]},7]}}}"#,
        Some("NUMBER_VALUE cannot be converted to String"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"B":true,"S":5}}}"#,
        Some("only base-64-encoded strings are convertible to bytes"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"BOOL":1e400}}}"#,
        Some("DECIMAL_VALUE cannot be converted to Boolean"),
    ),
    (
        "PutItem",
        r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"S":1.7976931348623158e308}}}"#,
        Some("DECIMAL_VALUE cannot be converted to String"),
    ),
];

#[tokio::test]
async fn wrong_json_types_in_attribute_values_are_refused_as_dynamodb_words_them() {
    let url = start_server().await;
    assert_serialization(&url, CAPTURED).await;
}

const TO_STRING: Option<&str> = Some("NUMBER_VALUE cannot be converted to String");
const IN_PAYLOAD: Option<&str> = Some("Unexpected value type in payload");

/// Shapes the captures don't cover, answered by the rules the captures show:
/// a wrong JSON type is refused as DynamoDB parses the value, at any depth,
/// ahead of faults found once it is read (a bad number, an empty set, a
/// second member), and the legacy condition maps read their values with the
/// same reader.
#[tokio::test]
async fn wrong_json_types_follow_the_same_rules_where_not_captured() {
    let url = start_server().await;
    assert_serialization(
        &url,
        &[
            ("PutItem", r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"L":[{"S":1e400}]}}}"#, Some("DECIMAL_VALUE cannot be converted to String")),
            ("Query", r#"{"TableName":"Tab","KeyConditionExpression":"pk = :v","ExpressionAttributeValues":{":v":{"S":"a"}},"ExclusiveStartKey":{"pk":{"N":-0}}}"#, TO_STRING),
            // A wrong JSON type nested in `L` or `M` is refused as the outer
            // value is parsed, ahead of the outer value's own faults.
            ("PutItem", r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"L":[{"S":5}],"N":"abc"}}}"#, TO_STRING),
            ("PutItem", r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"M":{"a":{"M":{"b":{"NS":[-0]}}}}}}}"#, TO_STRING),
            ("Scan", r#"{"TableName":"Tab","ScanFilter":{"a":{"ComparisonOperator":"EQ","AttributeValueList":[{"L":[{"S":5}],"N":"1"}]}}}"#, TO_STRING),
            // A legacy condition's value is read with the request, so its
            // wrong JSON type is refused ahead of faults found once the
            // request is read, here a table name that is too short.
            ("PutItem", r#"{"TableName":"ab","Item":{"pk":{"S":"a"}},"Expected":{"a":{"Value":{"S":5}}}}"#, TO_STRING),
            ("DeleteItem", r#"{"TableName":"ab","Key":{"pk":{"S":"a"}},"Expected":{"a":{"Value":{"B":5}}}}"#, Some("only base-64-encoded strings are convertible to bytes")),
            ("UpdateItem", r#"{"TableName":"ab","Key":{"pk":{"S":"a"}},"Expected":{"a":{"ComparisonOperator":"IN","AttributeValueList":[{"SS":"x"}]}}}"#, Some("Unexpected field type")),
            ("Query", r#"{"TableName":"ab","KeyConditions":{"pk":{"ComparisonOperator":"EQ","AttributeValueList":[{"S":"a"}]}},"QueryFilter":{"x":{"ComparisonOperator":"EQ","AttributeValueList":[{"S":-0}]}}}"#, TO_STRING),
            ("Scan", r#"{"TableName":"ab","ScanFilter":{"a":{"ComparisonOperator":"EQ","AttributeValueList":[{"L":[true]}]}}}"#, IN_PAYLOAD),
        ],
    )
    .await;
}

/// Values of the right type still read, and two well-typed members are still
/// refused for having more than one.
#[tokio::test]
async fn well_typed_values_are_unchanged() {
    let url = start_server().await;
    let item = r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"s":{"S":"x"},"n":{"N":"1.5"},"b":{"B":"AQ=="},"ss":{"SS":["a"]},"ns":{"NS":["1"]},"bs":{"BS":["AQ=="]},"l":{"L":[{"N":"1"}]},"m":{"M":{"a":{"BOOL":true}}},"z":{"NULL":true}}}"#;
    assert_eq!(answer(&url, "PutItem", item).await, None);
    assert_eq!(
        answer(&url, "PutItem", r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"S":"a","N":"1"}}}"#).await,
        Some((
            "ValidationException".to_string(),
            Some(
                "Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes"
                    .to_string()
            )
        ))
    );
}

/// A decimal past a double's range given for a boolean is named
/// `DECIMAL_VALUE`, as `ConsistentRead: 1e400` is (captured eu-west-2 and
/// us-east-1, 6 November 2026) and `BOOL: 1e400` is (8 November); `NULL`
/// and `Exists` follow them.
#[tokio::test]
async fn decimals_past_a_double_given_for_a_boolean_are_named_as_written() {
    const TO_BOOLEAN: Option<&str> = Some("DECIMAL_VALUE cannot be converted to Boolean");
    let url = start_server().await;
    assert_serialization(
        &url,
        &[
            (
                "GetItem",
                r#"{"TableName":"Tab","Key":{"pk":{"S":"a"}},"ConsistentRead":1e400}"#,
                TO_BOOLEAN,
            ),
            (
                "PutItem",
                r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"BOOL":1e400}}}"#,
                TO_BOOLEAN,
            ),
            (
                "PutItem",
                r#"{"TableName":"Tab","Item":{"pk":{"S":"a"},"x":{"NULL":-1E999}}}"#,
                TO_BOOLEAN,
            ),
            (
                "PutItem",
                r#"{"TableName":"Tab","Item":{"pk":{"S":"a"}},"Expected":{"a":{"Exists":1e400}}}"#,
                TO_BOOLEAN,
            ),
        ],
    )
    .await;
}
