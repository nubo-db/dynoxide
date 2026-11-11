//! A `ConditionalOperator` outside `AND` and `OR`, and where its refusal sits
//! among the request's other member errors, on Query, Scan, PutItem,
//! UpdateItem and DeleteItem. Beside it, which legacy parameters the
//! expression-mixing refusal names on Query.
//!
//! `FIRST` to `FIFTH` hold the 114 requests sent to eu-west-2 and us-east-1
//! on 11 November 2026, with the answers both regions gave, as the response
//! bodies DynamoDB returned. They are data captured from DynamoDB, not written
//! by hand. Every request names a table that doesn't exist, and the server
//! here has no tables, so a request DynamoDB accepts as far as the table
//! lookup gets the same `ResourceNotFoundException`. Every answer is a client
//! error, which DynamoDB sends with status 400.

#![cfg(feature = "http-server")]

use dynoxide::Database;

async fn start_server(db: Database) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        dynoxide::server::serve_on(listener, db).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    url
}

/// The status and parsed body a request is answered with.
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

async fn assert_captured(cases: &[(&str, &str, &str)]) {
    let url = start_server(Database::memory().unwrap()).await;
    let mut wrong = Vec::new();
    for &(operation, body, answer) in cases {
        let expected: serde_json::Value = serde_json::from_str(answer).unwrap();
        let (status, got) = send(&url, operation, body).await;
        if status != 400 || got != expected {
            wrong.push(format!(
                "{operation} {body}\n  expected 400 {expected}\n  got      {status} {got}"
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

/// The first set: the operator against `Select`, `ReturnConsumedCapacity`,
/// `Limit`, `Segment`, `TotalSegments`, `ReturnValues`, a missing table name,
/// too few conditions to join, and the expression parameters.
#[tokio::test]
async fn an_unknown_conditional_operator_is_refused_as_dynamodb_words_it() {
    assert_captured(FIRST).await;
}

/// The second set: `KeyConditions` beside each expression parameter, the
/// operator with no table name or no key, and lower case and the empty string
/// on Scan and PutItem.
#[tokio::test]
async fn key_conditions_and_a_missing_table_name_or_key_beside_the_operator() {
    assert_captured(SECOND).await;
}

/// `KeyConditions` with a `FilterExpression` or a `ProjectionExpression` is a
/// query DynamoDB runs (the captured rows reach the table lookup), so against
/// a table that exists it reads the key's items, filtered and projected by the
/// expressions.
#[tokio::test]
async fn key_conditions_run_beside_a_filter_or_projection_expression() {
    let db = Database::memory().unwrap();
    db.create_table(
        serde_json::from_value(serde_json::json!({
            "TableName": "Tab",
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"}
            ],
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"}
            ],
            "BillingMode": "PAY_PER_REQUEST"
        }))
        .unwrap(),
    )
    .unwrap();
    let url = start_server(db).await;
    for item in [
        r#"{"pk":{"S":"a"},"sk":{"S":"1"},"a":{"S":"x"},"b":{"S":"one"}}"#,
        r#"{"pk":{"S":"a"},"sk":{"S":"2"},"a":{"S":"y"},"b":{"S":"two"}}"#,
        r#"{"pk":{"S":"z"},"sk":{"S":"1"},"a":{"S":"x"},"b":{"S":"three"}}"#,
    ] {
        let (status, body) = send(
            &url,
            "PutItem",
            &format!(r#"{{"TableName":"Tab","Item":{item}}}"#),
        )
        .await;
        assert_eq!(status, 200, "{body}");
    }
    let key =
        r#""KeyConditions":{"pk":{"AttributeValueList":[{"S":"a"}],"ComparisonOperator":"EQ"}}"#;

    let (status, body) = send(
        &url,
        "Query",
        &format!(
            r#"{{"TableName":"Tab",{key},"FilterExpression":"a = :v","ExpressionAttributeValues":{{":v":{{"S":"x"}}}}}}"#
        ),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["Count"], 1, "{body}");
    assert_eq!(body["ScannedCount"], 2, "{body}");
    assert_eq!(
        body["Items"][0]["sk"],
        serde_json::json!({"S": "1"}),
        "{body}"
    );

    let (status, body) = send(
        &url,
        "Query",
        &format!(r##"{{"TableName":"Tab",{key},"ProjectionExpression":"#b","ExpressionAttributeNames":{{"#b":"b"}}}}"##),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body["Items"],
        serde_json::json!([{"b": {"S": "one"}}, {"b": {"S": "two"}}]),
        "{body}"
    );
}

/// The third set: `ExpressionAttributeValues` without an expression that can
/// use them, and an unused or undefined name or value beside `KeyConditions`,
/// each refused before the table is looked up.
#[tokio::test]
async fn expression_attributes_beside_key_conditions_are_checked_first() {
    assert_captured(THIRD).await;
}

/// The fourth set: an unused or undefined name or value on any Query or Scan,
/// refused before the table is looked up.
#[tokio::test]
async fn expression_attributes_are_checked_before_the_table_on_every_query_and_scan() {
    assert_captured(FOURTH).await;
}

/// The fifth set: a read's expressions are checked through, syntax, undefined
/// names and values and overlapping paths, before its unused attributes and
/// before the table is looked up, on Query, Scan, GetItem and BatchGetItem.
#[tokio::test]
async fn a_reads_expressions_are_checked_in_dynamodbs_order() {
    assert_captured(FIFTH).await;
}

/// The same faults against a table that exists: a KeyConditionExpression
/// value nobody defined is named as the key condition's, and a projection
/// that doesn't parse is a syntax error ahead of an unused name.
#[tokio::test]
async fn expression_faults_are_worded_the_same_on_a_table_that_exists() {
    let url = populated_server().await;
    let invalid = |message: &str| {
        serde_json::json!({
            "__type": "com.amazon.coral.validate#ValidationException",
            "message": message
        })
    };
    for (operation, body, expected) in [
        (
            "Query",
            r#"{"TableName":"Tab","KeyConditionExpression":"pk = :missing","FilterExpression":"a = :v","ExpressionAttributeValues":{":v":{"S":"x"}}}"#,
            invalid(
                "Invalid KeyConditionExpression: An expression attribute value used in expression is not defined; attribute value: :missing",
            ),
        ),
        (
            "Query",
            r#"{"TableName":"Tab","KeyConditionExpression":"pk = :missing","ExpressionAttributeValues":{":v":{"S":"x"}}}"#,
            invalid(
                "Invalid KeyConditionExpression: An expression attribute value used in expression is not defined; attribute value: :missing",
            ),
        ),
        (
            "Scan",
            r##"{"TableName":"Tab","ProjectionExpression":"#a,,","ExpressionAttributeNames":{"#a":"a"}}"##,
            invalid(r#"Invalid ProjectionExpression: Syntax error; token: ",", near: ",,""#),
        ),
        (
            "Query",
            r##"{"TableName":"Tab","KeyConditionExpression":"pk = :p","ExpressionAttributeValues":{":p":{"S":"a"}},"ProjectionExpression":"#a[","ExpressionAttributeNames":{"#a":"a"}}"##,
            invalid(r#"Invalid ProjectionExpression: Syntax error; token: "<EOF>", near: "[""#),
        ),
        (
            "GetItem",
            r##"{"TableName":"Tab","Key":{"pk":{"S":"a"},"sk":{"S":"1"}},"ProjectionExpression":"#a,,","ExpressionAttributeNames":{"#a":"a"}}"##,
            invalid(r#"Invalid ProjectionExpression: Syntax error; token: ",", near: ",,""#),
        ),
    ] {
        let (status, got) = send(&url, operation, body).await;
        assert_eq!((status, &got), (400, &expected), "{operation} {body}");
    }
}

/// A FilterExpression value nobody defined is refused whether or not there is
/// an item to evaluate the filter against: here, on a table that is empty.
#[tokio::test]
async fn an_undefined_filter_value_is_refused_on_an_empty_table() {
    let db = Database::memory().unwrap();
    db.create_table(
        serde_json::from_value(serde_json::json!({
            "TableName": "Empty",
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "BillingMode": "PAY_PER_REQUEST"
        }))
        .unwrap(),
    )
    .unwrap();
    let url = start_server(db).await;
    let expected = serde_json::json!({
        "__type": "com.amazon.coral.validate#ValidationException",
        "message": "Invalid FilterExpression: An expression attribute value used in expression is not defined; attribute value: :v"
    });
    for (operation, body) in [
        (
            "Query",
            r#"{"TableName":"Empty","KeyConditionExpression":"pk = :p","ExpressionAttributeValues":{":p":{"S":"a"}},"FilterExpression":"a = :v"}"#,
        ),
        (
            "Scan",
            r#"{"TableName":"Empty","FilterExpression":"a = :v"}"#,
        ),
    ] {
        let (status, got) = send(&url, operation, body).await;
        assert_eq!((status, &got), (400, &expected), "{operation} {body}");
    }
}

/// A table keyed on `pk` and `sk` with a global index on `gk`, holding two
/// items in partition `a` (and in index partition `g`) and one elsewhere.
async fn populated_server() -> String {
    let db = Database::memory().unwrap();
    db.create_table(
        serde_json::from_value(serde_json::json!({
            "TableName": "Tab",
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"}
            ],
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
                {"AttributeName": "gk", "AttributeType": "S"}
            ],
            "GlobalSecondaryIndexes": [{
                "IndexName": "gsi",
                "KeySchema": [{"AttributeName": "gk", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"}
            }],
            "BillingMode": "PAY_PER_REQUEST"
        }))
        .unwrap(),
    )
    .unwrap();
    let url = start_server(db).await;
    for item in [
        r#"{"pk":{"S":"a"},"sk":{"S":"1"},"gk":{"S":"g"},"a":{"S":"x"},"b":{"S":"one"}}"#,
        r#"{"pk":{"S":"a"},"sk":{"S":"2"},"gk":{"S":"g"},"a":{"S":"y"},"b":{"S":"two"}}"#,
        r#"{"pk":{"S":"z"},"sk":{"S":"1"},"gk":{"S":"h"},"a":{"S":"x"},"b":{"S":"three"}}"#,
    ] {
        let (status, body) = send(
            &url,
            "PutItem",
            &format!(r#"{{"TableName":"Tab","Item":{item}}}"#),
        )
        .await;
        assert_eq!(status, 200, "{body}");
    }
    url
}

/// The placeholders Dynoxide names `KeyConditions` through internally must not
/// take the place of the caller's own, whatever the caller calls them: a value
/// or a name the caller defines is the one its expressions read, on the table
/// and on an index.
#[tokio::test]
async fn key_conditions_leave_the_callers_placeholders_alone() {
    let url = populated_server().await;
    for target in [
        r#""KeyConditions":{"pk":{"AttributeValueList":[{"S":"a"}],"ComparisonOperator":"EQ"}}"#,
        r#""IndexName":"gsi","KeyConditions":{"gk":{"AttributeValueList":[{"S":"g"}],"ComparisonOperator":"EQ"}}"#,
    ] {
        let filters = [
            r#""FilterExpression":"a = :kc_v0","ExpressionAttributeValues":{":kc_v0":{"S":"x"}}"#,
            r##""FilterExpression":"#kc_0 = :v","ExpressionAttributeNames":{"#kc_0":"a"},"ExpressionAttributeValues":{":v":{"S":"x"}}"##,
            r##""FilterExpression":"#kc1_0 = :kc_v0","ExpressionAttributeNames":{"#kc1_0":"a"},"ExpressionAttributeValues":{":kc_v0":{"S":"x"}}"##,
        ];
        for filter in filters {
            let (status, body) = send(
                &url,
                "Query",
                &format!(r#"{{"TableName":"Tab",{target},{filter}}}"#),
            )
            .await;
            assert_eq!(status, 200, "{target} {filter}: {body}");
            assert_eq!(body["Count"], 1, "{target} {filter}: {body}");
            assert_eq!(
                body["Items"][0]["b"],
                serde_json::json!({"S": "one"}),
                "{target} {filter}: {body}"
            );
        }
        let (status, body) = send(
            &url,
            "Query",
            &format!(
                r##"{{"TableName":"Tab",{target},"ProjectionExpression":"#kc_0","ExpressionAttributeNames":{{"#kc_0":"b"}}}}"##
            ),
        )
        .await;
        assert_eq!(status, 200, "{target}: {body}");
        let mut items = body["Items"].as_array().cloned().unwrap_or_default();
        items.sort_by_key(|item| item["b"]["S"].as_str().map(str::to_string));
        assert_eq!(
            serde_json::Value::Array(items),
            serde_json::json!([{"b": {"S": "one"}}, {"b": {"S": "two"}}]),
            "{target}: {body}"
        );
    }
}

/// Captured in eu-west-2 and us-east-1, 11 November 2026; both regions gave
/// these answers to every request.
const FIRST: &[(&str, &str, &str)] = &[
    (
        "Query",
        r#"{"TableName": "_capture_20261111_317_none", "KeyConditions": {"pk": {"AttributeValueList": [{"S": "a"}], "ComparisonOperator": "EQ"}}, "QueryFilter": {"a": {"AttributeValueList": [{"S": "x"}], "ComparisonOperator": "EQ"}, "b": {"AttributeValueList": [{"S": "y"}], "ComparisonOperator": "EQ"}}, "ConditionalOperator": "XOR", "Select": "BOGUS"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Value 'XOR' at 'conditionalOperator' failed to satisfy constraint: Member must satisfy enum value set: [AND, OR]"}"#,
    ),
    (
        "Query",
        r#"{"TableName": "_capture_20261111_317_none", "KeyConditions": {"pk": {"AttributeValueList": [{"S": "a"}], "ComparisonOperator": "EQ"}}, "QueryFilter": {"a": {"AttributeValueList": [{"S": "x"}], "ComparisonOperator": "EQ"}, "b": {"AttributeValueList": [{"S": "y"}], "ComparisonOperator": "EQ"}}, "ConditionalOperator": "XOR"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Value 'XOR' at 'conditionalOperator' failed to satisfy constraint: Member must satisfy enum value set: [AND, OR]"}"#,
    ),
    (
        "Query",
        r#"{"TableName": "_capture_20261111_317_none", "KeyConditions": {"pk": {"AttributeValueList": [{"S": "a"}], "ComparisonOperator": "EQ"}}, "QueryFilter": {"a": {"AttributeValueList": [{"S": "x"}], "ComparisonOperator": "EQ"}, "b": {"AttributeValueList": [{"S": "y"}], "ComparisonOperator": "EQ"}}, "Select": "BOGUS"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value 'BOGUS' at 'select' failed to satisfy constraint: Member must satisfy enum value set: [SPECIFIC_ATTRIBUTES, COUNT, ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES]"}"#,
    ),
    (
        "Query",
        r#"{"TableName": "_capture_20261111_317_none", "KeyConditions": {"pk": {"AttributeValueList": [{"S": "a"}], "ComparisonOperator": "EQ"}}, "QueryFilter": {"a": {"AttributeValueList": [{"S": "x"}], "ComparisonOperator": "EQ"}, "b": {"AttributeValueList": [{"S": "y"}], "ComparisonOperator": "EQ"}}, "ConditionalOperator": "AND", "Select": "BOGUS"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value 'BOGUS' at 'select' failed to satisfy constraint: Member must satisfy enum value set: [SPECIFIC_ATTRIBUTES, COUNT, ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES]"}"#,
    ),
    (
        "Query",
        r#"{"TableName": "_capture_20261111_317_none", "KeyConditions": {"pk": {"AttributeValueList": [{"S": "a"}], "ComparisonOperator": "EQ"}}, "QueryFilter": {"a": {"AttributeValueList": [{"S": "x"}], "ComparisonOperator": "EQ"}}, "ConditionalOperator": "XOR", "Select": "BOGUS"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"ConditionalOperator can only be used when Filter or Expected has two or more elements"}"#,
    ),
    (
        "Query",
        r#"{"TableName": "_capture_20261111_317_none", "KeyConditions": {"pk": {"AttributeValueList": [{"S": "a"}], "ComparisonOperator": "EQ"}}, "QueryFilter": {"a": {"AttributeValueList": [{"S": "x"}], "ComparisonOperator": "EQ"}}, "ConditionalOperator": "XOR"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"ConditionalOperator can only be used when Filter or Expected has two or more elements"}"#,
    ),
    (
        "Query",
        r#"{"TableName": "_capture_20261111_317_none", "KeyConditions": {"pk": {"AttributeValueList": [{"S": "a"}], "ComparisonOperator": "EQ"}}, "ConditionalOperator": "XOR"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"ConditionalOperator cannot be used without Filter or Expected"}"#,
    ),
    (
        "Query",
        r#"{"TableName": "_capture_20261111_317_none", "KeyConditions": {"pk": {"AttributeValueList": [{"S": "a"}], "ComparisonOperator": "EQ"}}, "ConditionalOperator": "XOR", "Select": "BOGUS"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"ConditionalOperator cannot be used without Filter or Expected"}"#,
    ),
    (
        "Query",
        r#"{"TableName": "_capture_20261111_317_none", "KeyConditions": {"pk": {"AttributeValueList": [{"S": "a"}], "ComparisonOperator": "EQ"}}, "QueryFilter": {"a": {"AttributeValueList": [{"S": "x"}], "ComparisonOperator": "EQ"}, "b": {"AttributeValueList": [{"S": "y"}], "ComparisonOperator": "EQ"}}, "ConditionalOperator": "xor"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Value 'xor' at 'conditionalOperator' failed to satisfy constraint: Member must satisfy enum value set: [AND, OR]"}"#,
    ),
    (
        "Query",
        r#"{"TableName": "_capture_20261111_317_none", "KeyConditions": {"pk": {"AttributeValueList": [{"S": "a"}], "ComparisonOperator": "EQ"}}, "QueryFilter": {"a": {"AttributeValueList": [{"S": "x"}], "ComparisonOperator": "EQ"}, "b": {"AttributeValueList": [{"S": "y"}], "ComparisonOperator": "EQ"}}, "ConditionalOperator": ""}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Value '' at 'conditionalOperator' failed to satisfy constraint: Member must satisfy enum value set: [AND, OR]"}"#,
    ),
    (
        "Query",
        r#"{"TableName": "_capture_20261111_317_none", "KeyConditions": {"pk": {"AttributeValueList": [{"S": "a"}], "ComparisonOperator": "EQ"}}, "QueryFilter": {"a": {"AttributeValueList": [{"S": "x"}], "ComparisonOperator": "EQ"}, "b": {"AttributeValueList": [{"S": "y"}], "ComparisonOperator": "EQ"}}, "ConditionalOperator": "XOR", "ReturnConsumedCapacity": "BAD"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value 'BAD' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"}"#,
    ),
    (
        "Query",
        r#"{"TableName": "_capture_20261111_317_none", "KeyConditions": {"pk": {"AttributeValueList": [{"S": "a"}], "ComparisonOperator": "EQ"}}, "QueryFilter": {"a": {"AttributeValueList": [{"S": "x"}], "ComparisonOperator": "EQ"}, "b": {"AttributeValueList": [{"S": "y"}], "ComparisonOperator": "EQ"}}, "ConditionalOperator": "XOR", "ReturnConsumedCapacity": "BAD", "Select": "BOGUS"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value 'BAD' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"}"#,
    ),
    (
        "Query",
        r#"{"TableName": "_capture_20261111_317_none", "KeyConditions": {"pk": {"AttributeValueList": [{"S": "a"}], "ComparisonOperator": "EQ"}}, "QueryFilter": {"a": {"AttributeValueList": [{"S": "x"}], "ComparisonOperator": "EQ"}, "b": {"AttributeValueList": [{"S": "y"}], "ComparisonOperator": "EQ"}}, "ConditionalOperator": "XOR", "Limit": 0}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'Limit' failed to satisfy constraint: Member must have value greater than or equal to 1"}"#,
    ),
    (
        "Query",
        r#"{"TableName": "_capture_20261111_317_none", "KeyConditions": {"pk": {"AttributeValueList": [{"S": "a"}], "ComparisonOperator": "EQ"}}, "QueryFilter": {"a": {"AttributeValueList": [{"S": "x"}], "ComparisonOperator": "EQ"}, "b": {"AttributeValueList": [{"S": "y"}], "ComparisonOperator": "EQ"}}, "ConditionalOperator": "XOR", "Limit": 0, "Select": "BOGUS"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'Limit' failed to satisfy constraint: Member must have value greater than or equal to 1"}"#,
    ),
    (
        "Query",
        r#"{"TableName": "_capture_20261111_317_none", "KeyConditions": {"pk": {"AttributeValueList": [{"S": "a"}], "ComparisonOperator": "EQ"}}, "QueryFilter": {"a": {"AttributeValueList": [{"S": "x"}], "ComparisonOperator": "EQ"}, "b": {"AttributeValueList": [{"S": "y"}], "ComparisonOperator": "EQ"}}, "Select": "BOGUS", "ReturnConsumedCapacity": "BAD"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value 'BAD' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"}"#,
    ),
    (
        "Query",
        r#"{"TableName": "_capture_20261111_317_none", "KeyConditions": {"pk": {"AttributeValueList": [{"S": "a"}], "ComparisonOperator": "EQ"}}, "QueryFilter": {"a": {"AttributeValueList": [{"S": "x"}], "ComparisonOperator": "EQ"}, "b": {"AttributeValueList": [{"S": "y"}], "ComparisonOperator": "EQ"}}, "ConditionalOperator": "XOR", "FilterExpression": "a = :v", "ExpressionAttributeValues": {":v": {"S": "x"}}}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Can not use both expression and non-expression parameters in the same request: Non-expression parameters: {QueryFilter, ConditionalOperator} Expression parameters: {FilterExpression}"}"#,
    ),
    (
        "Query",
        r#"{"TableName": "_capture_20261111_317_none", "QueryFilter": {"a": {"AttributeValueList": [{"S": "x"}], "ComparisonOperator": "EQ"}, "b": {"AttributeValueList": [{"S": "y"}], "ComparisonOperator": "EQ"}}, "ConditionalOperator": "XOR", "Select": "BOGUS"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Value 'XOR' at 'conditionalOperator' failed to satisfy constraint: Member must satisfy enum value set: [AND, OR]"}"#,
    ),
    (
        "Query",
        r#"{"QueryFilter": {"a": {"AttributeValueList": [{"S": "x"}], "ComparisonOperator": "EQ"}, "b": {"AttributeValueList": [{"S": "y"}], "ComparisonOperator": "EQ"}}, "KeyConditions": {"pk": {"AttributeValueList": [{"S": "a"}], "ComparisonOperator": "EQ"}}, "ConditionalOperator": "XOR", "Select": "BOGUS"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"3 validation errors detected: Value 'BOGUS' at 'select' failed to satisfy constraint: Member must satisfy enum value set: [SPECIFIC_ATTRIBUTES, COUNT, ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES]; Value null at 'tableName' failed to satisfy constraint: Member must not be null; Value 'XOR' at 'conditionalOperator' failed to satisfy constraint: Member must satisfy enum value set: [OR, AND]"}"#,
    ),
    (
        "Query",
        r#"{"TableName": "_capture_20261111_317_none", "KeyConditionExpression": "pk = :v", "ExpressionAttributeValues": {":v": {"S": "a"}}, "ConditionalOperator": "XOR", "Select": "BOGUS"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Can not use both expression and non-expression parameters in the same request: Non-expression parameters: {ConditionalOperator} Expression parameters: {KeyConditionExpression}"}"#,
    ),
    (
        "Scan",
        r#"{"TableName": "_capture_20261111_317_none", "ScanFilter": {"a": {"AttributeValueList": [{"S": "x"}], "ComparisonOperator": "EQ"}, "b": {"AttributeValueList": [{"S": "y"}], "ComparisonOperator": "EQ"}}, "ConditionalOperator": "XOR", "Select": "BOGUS"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Value 'XOR' at 'conditionalOperator' failed to satisfy constraint: Member must satisfy enum value set: [AND, OR]"}"#,
    ),
    (
        "Scan",
        r#"{"TableName": "_capture_20261111_317_none", "ScanFilter": {"a": {"AttributeValueList": [{"S": "x"}], "ComparisonOperator": "EQ"}, "b": {"AttributeValueList": [{"S": "y"}], "ComparisonOperator": "EQ"}}, "ConditionalOperator": "XOR"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Value 'XOR' at 'conditionalOperator' failed to satisfy constraint: Member must satisfy enum value set: [AND, OR]"}"#,
    ),
    (
        "Scan",
        r#"{"TableName": "_capture_20261111_317_none", "ScanFilter": {"a": {"AttributeValueList": [{"S": "x"}], "ComparisonOperator": "EQ"}, "b": {"AttributeValueList": [{"S": "y"}], "ComparisonOperator": "EQ"}}, "Select": "BOGUS"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value 'BOGUS' at 'select' failed to satisfy constraint: Member must satisfy enum value set: [SPECIFIC_ATTRIBUTES, COUNT, ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES]"}"#,
    ),
    (
        "Scan",
        r#"{"TableName": "_capture_20261111_317_none", "ConditionalOperator": "XOR"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"ConditionalOperator cannot be used without Filter or Expected"}"#,
    ),
    (
        "Scan",
        r#"{"TableName": "_capture_20261111_317_none", "ScanFilter": {"a": {"AttributeValueList": [{"S": "x"}], "ComparisonOperator": "EQ"}}, "ConditionalOperator": "XOR", "Select": "BOGUS"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"ConditionalOperator can only be used when Filter or Expected has two or more elements"}"#,
    ),
    (
        "Scan",
        r#"{"TableName": "_capture_20261111_317_none", "ScanFilter": {"a": {"AttributeValueList": [{"S": "x"}], "ComparisonOperator": "EQ"}, "b": {"AttributeValueList": [{"S": "y"}], "ComparisonOperator": "EQ"}}, "ConditionalOperator": "XOR", "ReturnConsumedCapacity": "BAD"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value 'BAD' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"}"#,
    ),
    (
        "Scan",
        r#"{"TableName": "_capture_20261111_317_none", "ScanFilter": {"a": {"AttributeValueList": [{"S": "x"}], "ComparisonOperator": "EQ"}, "b": {"AttributeValueList": [{"S": "y"}], "ComparisonOperator": "EQ"}}, "ConditionalOperator": "XOR", "Limit": 0}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'Limit' failed to satisfy constraint: Member must have value greater than or equal to 1"}"#,
    ),
    (
        "Scan",
        r#"{"TableName": "_capture_20261111_317_none", "ScanFilter": {"a": {"AttributeValueList": [{"S": "x"}], "ComparisonOperator": "EQ"}, "b": {"AttributeValueList": [{"S": "y"}], "ComparisonOperator": "EQ"}}, "ConditionalOperator": "XOR", "Segment": -1}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'Segment' failed to satisfy constraint: Member must have value greater than or equal to 0"}"#,
    ),
    (
        "Scan",
        r#"{"TableName": "_capture_20261111_317_none", "ScanFilter": {"a": {"AttributeValueList": [{"S": "x"}], "ComparisonOperator": "EQ"}, "b": {"AttributeValueList": [{"S": "y"}], "ComparisonOperator": "EQ"}}, "ConditionalOperator": "XOR", "TotalSegments": 0, "Select": "BOGUS"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'TotalSegments' failed to satisfy constraint: Member must have value greater than or equal to 1"}"#,
    ),
    (
        "PutItem",
        r#"{"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}, "Expected": {"a": {"Value": {"S": "x"}}, "b": {"Value": {"S": "y"}}}, "ConditionalOperator": "XOR"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value 'XOR' at 'conditionalOperator' failed to satisfy constraint: Member must satisfy enum value set: [AND, OR]"}"#,
    ),
    (
        "PutItem",
        r#"{"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}, "Expected": {"a": {"Value": {"S": "x"}}, "b": {"Value": {"S": "y"}}}, "ConditionalOperator": "XOR", "ReturnValues": "BAD"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Failed to satisfy constraint: Member must satisfy enum value set: [ALL_OLD, UPDATED_OLD, ALL_NEW, UPDATE_NEW, NONE]"}"#,
    ),
    (
        "PutItem",
        r#"{"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}, "Expected": {"a": {"Value": {"S": "x"}}, "b": {"Value": {"S": "y"}}}, "ConditionalOperator": "XOR", "ReturnConsumedCapacity": "BAD"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value 'BAD' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"}"#,
    ),
    (
        "PutItem",
        r#"{"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}, "Expected": {"a": {"Value": {"S": "x"}}, "b": {"Value": {"S": "y"}}}, "ConditionalOperator": "XOR", "ReturnValues": "BAD", "ReturnConsumedCapacity": "BAD"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value 'BAD' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]; Failed to satisfy constraint: Member must satisfy enum value set: [ALL_OLD, UPDATED_OLD, ALL_NEW, UPDATE_NEW, NONE]"}"#,
    ),
    (
        "PutItem",
        r#"{"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}, "ConditionalOperator": "XOR"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: ConditionalOperator cannot be used without Filter or Expected"}"#,
    ),
    (
        "DeleteItem",
        r#"{"TableName": "_capture_20261111_317_none", "Key": {"pk": {"S": "a"}}, "Expected": {"a": {"Value": {"S": "x"}}, "b": {"Value": {"S": "y"}}}, "ConditionalOperator": "XOR"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value 'XOR' at 'conditionalOperator' failed to satisfy constraint: Member must satisfy enum value set: [AND, OR]"}"#,
    ),
    (
        "DeleteItem",
        r#"{"TableName": "_capture_20261111_317_none", "Key": {"pk": {"S": "a"}}, "Expected": {"a": {"Value": {"S": "x"}}, "b": {"Value": {"S": "y"}}}, "ConditionalOperator": "XOR", "ReturnValues": "BAD"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Failed to satisfy constraint: Member must satisfy enum value set: [ALL_OLD, UPDATED_OLD, ALL_NEW, UPDATE_NEW, NONE]"}"#,
    ),
    (
        "DeleteItem",
        r#"{"TableName": "_capture_20261111_317_none", "Key": {"pk": {"S": "a"}}, "Expected": {"a": {"Value": {"S": "x"}}, "b": {"Value": {"S": "y"}}}, "ConditionalOperator": "XOR", "ReturnConsumedCapacity": "BAD"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value 'BAD' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"}"#,
    ),
    (
        "DeleteItem",
        r#"{"TableName": "_capture_20261111_317_none", "Key": {"pk": {"S": "a"}}, "Expected": {"a": {"Value": {"S": "x"}}, "b": {"Value": {"S": "y"}}}, "ConditionalOperator": "XOR", "ReturnValues": "BAD", "ReturnConsumedCapacity": "BAD"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value 'BAD' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]; Failed to satisfy constraint: Member must satisfy enum value set: [ALL_OLD, UPDATED_OLD, ALL_NEW, UPDATE_NEW, NONE]"}"#,
    ),
    (
        "DeleteItem",
        r#"{"TableName": "_capture_20261111_317_none", "Key": {"pk": {"S": "a"}}, "ConditionalOperator": "XOR"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: ConditionalOperator cannot be used without Filter or Expected"}"#,
    ),
    (
        "UpdateItem",
        r#"{"TableName": "_capture_20261111_317_none", "Key": {"pk": {"S": "a"}}, "Expected": {"a": {"Value": {"S": "x"}}, "b": {"Value": {"S": "y"}}}, "ConditionalOperator": "XOR"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value 'XOR' at 'conditionalOperator' failed to satisfy constraint: Member must satisfy enum value set: [AND, OR]"}"#,
    ),
    (
        "UpdateItem",
        r#"{"TableName": "_capture_20261111_317_none", "Key": {"pk": {"S": "a"}}, "Expected": {"a": {"Value": {"S": "x"}}, "b": {"Value": {"S": "y"}}}, "ConditionalOperator": "XOR", "ReturnValues": "BAD"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Failed to satisfy constraint: Member must satisfy enum value set: [ALL_OLD, UPDATED_OLD, ALL_NEW, UPDATE_NEW, NONE]"}"#,
    ),
    (
        "UpdateItem",
        r#"{"TableName": "_capture_20261111_317_none", "Key": {"pk": {"S": "a"}}, "Expected": {"a": {"Value": {"S": "x"}}, "b": {"Value": {"S": "y"}}}, "ConditionalOperator": "XOR", "ReturnConsumedCapacity": "BAD"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Value 'BAD' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"}"#,
    ),
    (
        "UpdateItem",
        r#"{"TableName": "_capture_20261111_317_none", "Key": {"pk": {"S": "a"}}, "Expected": {"a": {"Value": {"S": "x"}}, "b": {"Value": {"S": "y"}}}, "ConditionalOperator": "XOR", "ReturnValues": "BAD", "ReturnConsumedCapacity": "BAD"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Failed to satisfy constraint: Member must satisfy enum value set: [ALL_OLD, UPDATED_OLD, ALL_NEW, UPDATE_NEW, NONE]"}"#,
    ),
    (
        "UpdateItem",
        r#"{"TableName": "_capture_20261111_317_none", "Key": {"pk": {"S": "a"}}, "ConditionalOperator": "XOR"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: ConditionalOperator cannot be used without Filter or Expected"}"#,
    ),
];

/// Captured the same day in the same two regions, which again agreed on every
/// request.
const SECOND: &[(&str, &str, &str)] = &[
    (
        "Query",
        r#"{"TableName": "_capture_20261111_317_none", "KeyConditions": {"pk": {"AttributeValueList": [{"S": "a"}], "ComparisonOperator": "EQ"}}, "FilterExpression": "a = :v", "ExpressionAttributeValues": {":v": {"S": "x"}}}"#,
        r#"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found"}"#,
    ),
    (
        "Query",
        r#"{"TableName": "_capture_20261111_317_none", "KeyConditions": {"pk": {"AttributeValueList": [{"S": "a"}], "ComparisonOperator": "EQ"}}, "KeyConditionExpression": "pk = :v", "ExpressionAttributeValues": {":v": {"S": "x"}}}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Can not use both expression and non-expression parameters in the same request: Non-expression parameters: {KeyConditions} Expression parameters: {KeyConditionExpression}"}"#,
    ),
    (
        "Query",
        r#"{"TableName": "_capture_20261111_317_none", "KeyConditions": {"pk": {"AttributeValueList": [{"S": "a"}], "ComparisonOperator": "EQ"}}, "QueryFilter": {"a": {"AttributeValueList": [{"S": "x"}], "ComparisonOperator": "EQ"}}, "FilterExpression": "a = :v", "ExpressionAttributeValues": {":v": {"S": "x"}}}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Can not use both expression and non-expression parameters in the same request: Non-expression parameters: {QueryFilter} Expression parameters: {FilterExpression}"}"#,
    ),
    (
        "Query",
        r#"{"TableName": "_capture_20261111_317_none", "KeyConditions": {"pk": {"AttributeValueList": [{"S": "a"}], "ComparisonOperator": "EQ"}}, "QueryFilter": {"a": {"AttributeValueList": [{"S": "x"}], "ComparisonOperator": "EQ"}}, "KeyConditionExpression": "pk = :v", "ExpressionAttributeValues": {":v": {"S": "x"}}}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Can not use both expression and non-expression parameters in the same request: Non-expression parameters: {QueryFilter, KeyConditions} Expression parameters: {KeyConditionExpression}"}"#,
    ),
    (
        "Query",
        r#"{"TableName": "_capture_20261111_317_none", "KeyConditions": {"pk": {"AttributeValueList": [{"S": "a"}], "ComparisonOperator": "EQ"}}, "ProjectionExpression": "a"}"#,
        r#"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found"}"#,
    ),
    (
        "Query",
        r#"{"TableName": "_capture_20261111_317_none", "KeyConditions": {"pk": {"AttributeValueList": [{"S": "a"}], "ComparisonOperator": "EQ"}}, "AttributesToGet": ["a"], "FilterExpression": "a = :v", "ExpressionAttributeValues": {":v": {"S": "x"}}}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Can not use both expression and non-expression parameters in the same request: Non-expression parameters: {AttributesToGet} Expression parameters: {FilterExpression}"}"#,
    ),
    (
        "Query",
        r#"{"TableName": "_capture_20261111_317_none", "KeyConditionExpression": "pk = :v", "ExpressionAttributeValues": {":v": {"S": "x"}}, "QueryFilter": {"a": {"AttributeValueList": [{"S": "x"}], "ComparisonOperator": "EQ"}}}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Can not use both expression and non-expression parameters in the same request: Non-expression parameters: {QueryFilter} Expression parameters: {KeyConditionExpression}"}"#,
    ),
    (
        "Query",
        r#"{"TableName": "_capture_20261111_317_none", "KeyConditions": {"pk": {"AttributeValueList": [{"S": "a"}], "ComparisonOperator": "EQ"}}, "QueryFilter": {"a": {"AttributeValueList": [{"S": "x"}], "ComparisonOperator": "EQ"}, "b": {"AttributeValueList": [{"S": "y"}], "ComparisonOperator": "EQ"}}, "ConditionalOperator": "AND", "FilterExpression": "a = :v", "ExpressionAttributeValues": {":v": {"S": "x"}}}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Can not use both expression and non-expression parameters in the same request: Non-expression parameters: {QueryFilter, ConditionalOperator} Expression parameters: {FilterExpression}"}"#,
    ),
    (
        "Query",
        r#"{"KeyConditions": {"pk": {"AttributeValueList": [{"S": "a"}], "ComparisonOperator": "EQ"}}, "QueryFilter": {"a": {"AttributeValueList": [{"S": "x"}], "ComparisonOperator": "EQ"}}, "ConditionalOperator": "XOR"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value null at 'tableName' failed to satisfy constraint: Member must not be null; Value 'XOR' at 'conditionalOperator' failed to satisfy constraint: Member must satisfy enum value set: [OR, AND]"}"#,
    ),
    (
        "Query",
        r#"{"KeyConditions": {"pk": {"AttributeValueList": [{"S": "a"}], "ComparisonOperator": "EQ"}}, "ConditionalOperator": "XOR"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value null at 'tableName' failed to satisfy constraint: Member must not be null; Value 'XOR' at 'conditionalOperator' failed to satisfy constraint: Member must satisfy enum value set: [OR, AND]"}"#,
    ),
    (
        "Query",
        r#"{"KeyConditions": {"pk": {"AttributeValueList": [{"S": "a"}], "ComparisonOperator": "EQ"}}, "QueryFilter": {"a": {"AttributeValueList": [{"S": "x"}], "ComparisonOperator": "EQ"}, "b": {"AttributeValueList": [{"S": "y"}], "ComparisonOperator": "EQ"}}, "ConditionalOperator": "XOR"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value null at 'tableName' failed to satisfy constraint: Member must not be null; Value 'XOR' at 'conditionalOperator' failed to satisfy constraint: Member must satisfy enum value set: [OR, AND]"}"#,
    ),
    (
        "Query",
        r#"{"KeyConditions": {"pk": {"AttributeValueList": [{"S": "a"}], "ComparisonOperator": "EQ"}}, "QueryFilter": {"a": {"AttributeValueList": [{"S": "x"}], "ComparisonOperator": "EQ"}, "b": {"AttributeValueList": [{"S": "y"}], "ComparisonOperator": "EQ"}}, "ConditionalOperator": "AND", "Select": "BOGUS"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value 'BOGUS' at 'select' failed to satisfy constraint: Member must satisfy enum value set: [SPECIFIC_ATTRIBUTES, COUNT, ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES]; Value null at 'tableName' failed to satisfy constraint: Member must not be null"}"#,
    ),
    (
        "Query",
        r#"{"QueryFilter": {"a": {"AttributeValueList": [{"S": "x"}], "ComparisonOperator": "EQ"}, "b": {"AttributeValueList": [{"S": "y"}], "ComparisonOperator": "EQ"}}, "ConditionalOperator": "XOR", "Limit": 0}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"3 validation errors detected: Value null at 'tableName' failed to satisfy constraint: Member must not be null; Value 'XOR' at 'conditionalOperator' failed to satisfy constraint: Member must satisfy enum value set: [OR, AND]; Value '0' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1"}"#,
    ),
    (
        "Scan",
        r#"{"TableName": "_capture_20261111_317_none", "ScanFilter": {"a": {"AttributeValueList": [{"S": "x"}], "ComparisonOperator": "EQ"}, "b": {"AttributeValueList": [{"S": "y"}], "ComparisonOperator": "EQ"}}, "ConditionalOperator": "xor"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Value 'xor' at 'conditionalOperator' failed to satisfy constraint: Member must satisfy enum value set: [AND, OR]"}"#,
    ),
    (
        "Scan",
        r#"{"TableName": "_capture_20261111_317_none", "ScanFilter": {"a": {"AttributeValueList": [{"S": "x"}], "ComparisonOperator": "EQ"}, "b": {"AttributeValueList": [{"S": "y"}], "ComparisonOperator": "EQ"}}, "ConditionalOperator": ""}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Value '' at 'conditionalOperator' failed to satisfy constraint: Member must satisfy enum value set: [AND, OR]"}"#,
    ),
    (
        "Scan",
        r#"{"ScanFilter": {"a": {"AttributeValueList": [{"S": "x"}], "ComparisonOperator": "EQ"}, "b": {"AttributeValueList": [{"S": "y"}], "ComparisonOperator": "EQ"}}, "ConditionalOperator": "XOR", "Select": "BOGUS"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"3 validation errors detected: Value 'BOGUS' at 'select' failed to satisfy constraint: Member must satisfy enum value set: [SPECIFIC_ATTRIBUTES, COUNT, ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES]; Value null at 'tableName' failed to satisfy constraint: Member must not be null; Value 'XOR' at 'conditionalOperator' failed to satisfy constraint: Member must satisfy enum value set: [OR, AND]"}"#,
    ),
    (
        "Scan",
        r#"{"ScanFilter": {"a": {"AttributeValueList": [{"S": "x"}], "ComparisonOperator": "EQ"}}, "ConditionalOperator": "XOR"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value null at 'tableName' failed to satisfy constraint: Member must not be null; Value 'XOR' at 'conditionalOperator' failed to satisfy constraint: Member must satisfy enum value set: [OR, AND]"}"#,
    ),
    (
        "Scan",
        r#"{"ConditionalOperator": "XOR"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value null at 'tableName' failed to satisfy constraint: Member must not be null; Value 'XOR' at 'conditionalOperator' failed to satisfy constraint: Member must satisfy enum value set: [OR, AND]"}"#,
    ),
    (
        "Scan",
        r#"{"TableName": "_capture_20261111_317_none", "ScanFilter": {"a": {"AttributeValueList": [{"S": "x"}], "ComparisonOperator": "EQ"}, "b": {"AttributeValueList": [{"S": "y"}], "ComparisonOperator": "EQ"}}, "ConditionalOperator": "XOR", "FilterExpression": "a = :v", "ExpressionAttributeValues": {":v": {"S": "x"}}}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Can not use both expression and non-expression parameters in the same request: Non-expression parameters: {ScanFilter, ConditionalOperator} Expression parameters: {FilterExpression}"}"#,
    ),
    (
        "PutItem",
        r#"{"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}, "Expected": {"a": {"Value": {"S": "x"}}, "b": {"Value": {"S": "y"}}}, "ConditionalOperator": "XOR", "ConditionExpression": "a = :v", "ExpressionAttributeValues": {":v": {"S": "x"}}}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Can not use both expression and non-expression parameters in the same request: Non-expression parameters: {Expected, ConditionalOperator} Expression parameters: {ConditionExpression}"}"#,
    ),
    (
        "PutItem",
        r#"{"Item": {"pk": {"S": "a"}}, "Expected": {"a": {"Value": {"S": "x"}}, "b": {"Value": {"S": "y"}}}, "ConditionalOperator": "XOR"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value 'XOR' at 'conditionalOperator' failed to satisfy constraint: Member must satisfy enum value set: [OR, AND]; Value null at 'tableName' failed to satisfy constraint: Member must not be null"}"#,
    ),
    (
        "PutItem",
        r#"{"TableName": "_capture_20261111_317_none", "Item": {"pk": {"S": "a"}}, "Expected": {"a": {"Value": {"S": "x"}}, "b": {"Value": {"S": "y"}}}, "ConditionalOperator": "xor"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value 'xor' at 'conditionalOperator' failed to satisfy constraint: Member must satisfy enum value set: [AND, OR]"}"#,
    ),
    (
        "UpdateItem",
        r#"{"TableName": "_capture_20261111_317_none", "Expected": {"a": {"Value": {"S": "x"}}, "b": {"Value": {"S": "y"}}}, "ConditionalOperator": "XOR"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value 'XOR' at 'conditionalOperator' failed to satisfy constraint: Member must satisfy enum value set: [OR, AND]; Value null at 'key' failed to satisfy constraint: Member must not be null"}"#,
    ),
    (
        "DeleteItem",
        r#"{"TableName": "_capture_20261111_317_none", "Expected": {"a": {"Value": {"S": "x"}}, "b": {"Value": {"S": "y"}}}, "ConditionalOperator": "XOR"}"#,
        r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"2 validation errors detected: Value 'XOR' at 'conditionalOperator' failed to satisfy constraint: Member must satisfy enum value set: [OR, AND]; Value null at 'key' failed to satisfy constraint: Member must not be null"}"#,
    ),
];

/// Captured the same day in the same two regions, which agreed on every
/// request: the expression attributes beside `KeyConditions`, and beside a
/// Query or Scan with only a `ProjectionExpression` or no expression at all.
const THIRD: &[(&str, &str, &str)] = &[
    (
        "Query",
        r##"{"TableName": "_capture_20261111_317_none", "ExpressionAttributeValues": {":v": {"S": "x"}}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"ExpressionAttributeValues can only be specified when using expressions"}"##,
    ),
    (
        "Query",
        r##"{"TableName": "_capture_20261111_317_none", "KeyConditions": {"pk": {"AttributeValueList": [{"S": "a"}], "ComparisonOperator": "EQ"}}, "ExpressionAttributeValues": {":v": {"S": "x"}}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"ExpressionAttributeValues can only be specified when using expressions"}"##,
    ),
    (
        "Query",
        r##"{"TableName": "_capture_20261111_317_none", "KeyConditions": {"pk": {"AttributeValueList": [{"S": "a"}], "ComparisonOperator": "EQ"}}, "ProjectionExpression": "a", "ExpressionAttributeValues": {":v": {"S": "x"}}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"ExpressionAttributeValues can only be specified when using expressions"}"##,
    ),
    (
        "Query",
        r##"{"TableName": "_capture_20261111_317_none", "KeyConditions": {"pk": {"AttributeValueList": [{"S": "a"}], "ComparisonOperator": "EQ"}}, "ProjectionExpression": "#a", "ExpressionAttributeNames": {"#a": "a"}}"##,
        r##"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found"}"##,
    ),
    (
        "Query",
        r##"{"TableName": "_capture_20261111_317_none", "KeyConditions": {"pk": {"AttributeValueList": [{"S": "a"}], "ComparisonOperator": "EQ"}}, "ExpressionAttributeNames": {"#a": "a"}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"ExpressionAttributeNames can only be specified when using expressions"}"##,
    ),
    (
        "Query",
        r##"{"TableName": "_capture_20261111_317_none", "KeyConditions": {"pk": {"AttributeValueList": [{"S": "a"}], "ComparisonOperator": "EQ"}}, "ProjectionExpression": "a", "ExpressionAttributeNames": {"#a": "a"}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"Value provided in ExpressionAttributeNames unused in expressions: keys: {#a}"}"##,
    ),
    (
        "Query",
        r##"{"TableName": "_capture_20261111_317_none", "KeyConditions": {"pk": {"AttributeValueList": [{"S": "a"}], "ComparisonOperator": "EQ"}}, "FilterExpression": "a = :v", "ExpressionAttributeValues": {":v": {"S": "x"}, ":w": {"S": "y"}}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"Value provided in ExpressionAttributeValues unused in expressions: keys: {:w}"}"##,
    ),
    (
        "Query",
        r##"{"TableName": "_capture_20261111_317_none", "KeyConditions": {"pk": {"AttributeValueList": [{"S": "a"}], "ComparisonOperator": "EQ"}}, "FilterExpression": "a = :v"}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"Invalid FilterExpression: An expression attribute value used in expression is not defined; attribute value: :v"}"##,
    ),
    (
        "Query",
        r##"{"TableName": "_capture_20261111_317_none", "KeyConditions": {"pk": {"AttributeValueList": [{"S": "a"}], "ComparisonOperator": "EQ"}}, "FilterExpression": "#a = :v", "ExpressionAttributeValues": {":v": {"S": "x"}}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"Invalid FilterExpression: An expression attribute name used in the document path is not defined; attribute name: #a"}"##,
    ),
    (
        "Scan",
        r##"{"TableName": "_capture_20261111_317_none", "ExpressionAttributeValues": {":v": {"S": "x"}}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"ExpressionAttributeValues can only be specified when using expressions"}"##,
    ),
    (
        "Scan",
        r##"{"TableName": "_capture_20261111_317_none", "ProjectionExpression": "a", "ExpressionAttributeValues": {":v": {"S": "x"}}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"ExpressionAttributeValues can only be specified when using expressions"}"##,
    ),
    (
        "Scan",
        r##"{"TableName": "_capture_20261111_317_none", "ScanFilter": {"a": {"AttributeValueList": [{"S": "x"}], "ComparisonOperator": "EQ"}}, "ProjectionExpression": "a"}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"Can not use both expression and non-expression parameters in the same request: Non-expression parameters: {ScanFilter} Expression parameters: {ProjectionExpression}"}"##,
    ),
];

/// Captured the same day in the same two regions, which agreed on every
/// request: the same checks on a Query with a `KeyConditionExpression` and on
/// a Scan, and which of two faults is reported first.
const FOURTH: &[(&str, &str, &str)] = &[
    (
        "Query",
        r##"{"TableName": "_capture_20261111_317_none", "KeyConditionExpression": "pk = :p", "ExpressionAttributeValues": {":p": {"S": "a"}}, "ProjectionExpression": "a", "ExpressionAttributeNames": {"#a": "a"}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"Value provided in ExpressionAttributeNames unused in expressions: keys: {#a}"}"##,
    ),
    (
        "Query",
        r##"{"TableName": "_capture_20261111_317_none", "KeyConditionExpression": "pk = :p", "FilterExpression": "a = :v", "ExpressionAttributeValues": {":p": {"S": "a"}, ":v": {"S": "x"}, ":w": {"S": "y"}}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"Value provided in ExpressionAttributeValues unused in expressions: keys: {:w}"}"##,
    ),
    (
        "Query",
        r##"{"TableName": "_capture_20261111_317_none", "KeyConditionExpression": "pk = :p", "ExpressionAttributeValues": {":p": {"S": "a"}}, "FilterExpression": "a = :v"}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"Invalid FilterExpression: An expression attribute value used in expression is not defined; attribute value: :v"}"##,
    ),
    (
        "Query",
        r##"{"TableName": "_capture_20261111_317_none", "KeyConditionExpression": "pk = :p", "ExpressionAttributeValues": {":p": {"S": "a"}, ":w": {"S": "y"}}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"Value provided in ExpressionAttributeValues unused in expressions: keys: {:w}"}"##,
    ),
    (
        "Scan",
        r##"{"TableName": "_capture_20261111_317_none", "ProjectionExpression": "a", "ExpressionAttributeNames": {"#a": "a"}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"Value provided in ExpressionAttributeNames unused in expressions: keys: {#a}"}"##,
    ),
    (
        "Scan",
        r##"{"TableName": "_capture_20261111_317_none", "FilterExpression": "a = :v", "ExpressionAttributeValues": {":v": {"S": "x"}, ":w": {"S": "y"}}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"Value provided in ExpressionAttributeValues unused in expressions: keys: {:w}"}"##,
    ),
    (
        "Scan",
        r##"{"TableName": "_capture_20261111_317_none", "FilterExpression": "a = :v"}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"Invalid FilterExpression: An expression attribute value used in expression is not defined; attribute value: :v"}"##,
    ),
    (
        "Query",
        r##"{"TableName": "_capture_20261111_317_none", "KeyConditions": {"pk": {"AttributeValueList": [{"S": "a"}], "ComparisonOperator": "EQ"}}, "FilterExpression": "a = :v", "ExpressionAttributeValues": {":w": {"S": "y"}}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"Invalid FilterExpression: An expression attribute value used in expression is not defined; attribute value: :v"}"##,
    ),
    (
        "Query",
        r##"{"TableName": "_capture_20261111_317_none", "KeyConditions": {"pk": {"AttributeValueList": [{"S": "a"}], "ComparisonOperator": "EQ"}}, "ProjectionExpression": "#b", "ExpressionAttributeNames": {"#a": "a"}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"Invalid ProjectionExpression: An expression attribute name used in the document path is not defined; attribute name: #b"}"##,
    ),
    (
        "Query",
        r##"{"TableName": "_capture_20261111_317_none", "KeyConditionExpression": "pk = :p", "ExpressionAttributeValues": {":p": {"S": "a"}}, "ProjectionExpression": "#b"}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"Invalid ProjectionExpression: An expression attribute name used in the document path is not defined; attribute name: #b"}"##,
    ),
    (
        "Scan",
        r##"{"TableName": "_capture_20261111_317_none", "FilterExpression": "#a = :v", "ExpressionAttributeValues": {":v": {"S": "x"}}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"Invalid FilterExpression: An expression attribute name used in the document path is not defined; attribute name: #a"}"##,
    ),
];

/// Captured the same day in the same two regions, which agreed on every
/// request: which of two faults in a read's expressions is reported first, on
/// Query, Scan, GetItem and BatchGetItem.
const FIFTH: &[(&str, &str, &str)] = &[
    (
        "Query",
        r##"{"TableName": "_capture_20261111_317_none", "KeyConditionExpression": "pk = :missing", "FilterExpression": "a = :v", "ExpressionAttributeValues": {":v": {"S": "x"}}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"Invalid KeyConditionExpression: An expression attribute value used in expression is not defined; attribute value: :missing"}"##,
    ),
    (
        "Query",
        r##"{"TableName": "_capture_20261111_317_none", "KeyConditionExpression": "pk = :missing", "ExpressionAttributeValues": {":v": {"S": "x"}}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"Invalid KeyConditionExpression: An expression attribute value used in expression is not defined; attribute value: :missing"}"##,
    ),
    (
        "Query",
        r##"{"TableName": "_capture_20261111_317_none", "KeyConditionExpression": "pk = :missing"}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"Invalid KeyConditionExpression: An expression attribute value used in expression is not defined; attribute value: :missing"}"##,
    ),
    (
        "Query",
        r##"{"TableName": "_capture_20261111_317_none", "KeyConditionExpression": "pk = :m1", "FilterExpression": "a = :m2", "ExpressionAttributeValues": {":z": {"S": "x"}}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"Invalid FilterExpression: An expression attribute value used in expression is not defined; attribute value: :m2"}"##,
    ),
    (
        "Query",
        r##"{"TableName": "_capture_20261111_317_none", "KeyConditionExpression": "#p = :v", "ExpressionAttributeValues": {":v": {"S": "x"}}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"Invalid KeyConditionExpression: An expression attribute name used in the document path is not defined; attribute name: #p"}"##,
    ),
    (
        "Query",
        r##"{"TableName": "_capture_20261111_317_none", "KeyConditionExpression": "#p = :v", "ExpressionAttributeValues": {":v": {"S": "x"}}, "ExpressionAttributeNames": {"#q": "pk"}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"Invalid KeyConditionExpression: An expression attribute name used in the document path is not defined; attribute name: #p"}"##,
    ),
    (
        "Scan",
        r##"{"TableName": "_capture_20261111_317_none", "ProjectionExpression": "#a,,", "ExpressionAttributeNames": {"#a": "a"}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"Invalid ProjectionExpression: Syntax error; token: \",\", near: \",,\""}"##,
    ),
    (
        "Scan",
        r##"{"TableName": "_capture_20261111_317_none", "ProjectionExpression": "#a[", "ExpressionAttributeNames": {"#a": "a"}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"Invalid ProjectionExpression: Syntax error; token: \"<EOF>\", near: \"[\""}"##,
    ),
    (
        "Scan",
        r##"{"TableName": "_capture_20261111_317_none", "ProjectionExpression": "a,,", "ExpressionAttributeNames": {"#a": "a"}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"Invalid ProjectionExpression: Syntax error; token: \",\", near: \",,\""}"##,
    ),
    (
        "Scan",
        r##"{"TableName": "_capture_20261111_317_none", "FilterExpression": "a = = :v", "ExpressionAttributeValues": {":v": {"S": "x"}, ":w": {"S": "y"}}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"Invalid FilterExpression: Syntax error; token: \"=\", near: \"= = :v\""}"##,
    ),
    (
        "Query",
        r##"{"TableName": "_capture_20261111_317_none", "KeyConditionExpression": "pk = = :v", "ExpressionAttributeValues": {":v": {"S": "x"}, ":w": {"S": "y"}}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"Invalid KeyConditionExpression: Syntax error; token: \"=\", near: \"= = :v\""}"##,
    ),
    (
        "Scan",
        r##"{"TableName": "_capture_20261111_317_none", "FilterExpression": "a = :v OR b = :a"}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"Invalid FilterExpression: An expression attribute value used in expression is not defined; attribute value: :v"}"##,
    ),
    (
        "Scan",
        r##"{"TableName": "_capture_20261111_317_none", "FilterExpression": "b = :a OR a = :v"}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"Invalid FilterExpression: An expression attribute value used in expression is not defined; attribute value: :a"}"##,
    ),
    (
        "Scan",
        r##"{"TableName": "_capture_20261111_317_none", "ProjectionExpression": "#a, #a", "ExpressionAttributeNames": {"#a": "a", "#b": "b"}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"Invalid ProjectionExpression: Two document paths overlap with each other; must remove or rewrite one of these paths; path one: [a], path two: [a]"}"##,
    ),
    (
        "Scan",
        r##"{"TableName": "_capture_20261111_317_none", "ProjectionExpression": "#a, #a", "ExpressionAttributeNames": {"#a": "a"}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"Invalid ProjectionExpression: Two document paths overlap with each other; must remove or rewrite one of these paths; path one: [a], path two: [a]"}"##,
    ),
    (
        "Scan",
        r##"{"TableName": "_capture_20261111_317_none", "FilterExpression": "a = :m", "ProjectionExpression": "#b"}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"Invalid FilterExpression: An expression attribute value used in expression is not defined; attribute value: :m"}"##,
    ),
    (
        "Scan",
        r##"{"TableName": "_capture_20261111_317_none", "ProjectionExpression": "#b, #c"}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"Invalid ProjectionExpression: An expression attribute name used in the document path is not defined; attribute name: #b"}"##,
    ),
    (
        "Scan",
        r##"{"TableName": "_capture_20261111_317_none", "ProjectionExpression": "#c, #b"}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"Invalid ProjectionExpression: An expression attribute name used in the document path is not defined; attribute name: #c"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName": "_capture_20261111_317_none", "Key": {"pk": {"S": "a"}}, "ProjectionExpression": "a", "ExpressionAttributeNames": {"#a": "a"}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"Value provided in ExpressionAttributeNames unused in expressions: keys: {#a}"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName": "_capture_20261111_317_none", "Key": {"pk": {"S": "a"}}, "ProjectionExpression": "#b"}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"Invalid ProjectionExpression: An expression attribute name used in the document path is not defined; attribute name: #b"}"##,
    ),
    (
        "GetItem",
        r##"{"TableName": "_capture_20261111_317_none", "Key": {"pk": {"S": "a"}}, "ExpressionAttributeNames": {"#a": "a"}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"ExpressionAttributeNames can only be specified when using expressions"}"##,
    ),
    (
        "BatchGetItem",
        r##"{"RequestItems": {"_capture_20261111_317_none": {"Keys": [{"pk": {"S": "a"}}], "ProjectionExpression": "a", "ExpressionAttributeNames": {"#a": "a"}}}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"Value provided in ExpressionAttributeNames unused in expressions: keys: {#a}"}"##,
    ),
    (
        "BatchGetItem",
        r##"{"RequestItems": {"_capture_20261111_317_none": {"Keys": [{"pk": {"S": "a"}}], "ProjectionExpression": "#b"}}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"Invalid ProjectionExpression: An expression attribute name used in the document path is not defined; attribute name: #b"}"##,
    ),
    (
        "BatchGetItem",
        r##"{"RequestItems": {"_capture_20261111_317_none": {"Keys": [{"pk": {"S": "a"}}], "ExpressionAttributeNames": {"#a": "a"}}}}"##,
        r##"{"__type":"com.amazon.coral.validate#ValidationException","message":"ExpressionAttributeNames can only be specified when using expressions"}"##,
    ),
];
