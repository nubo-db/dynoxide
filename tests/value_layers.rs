//! DynamoDB checks attribute values in one of two layers, and the layer an
//! operation runs decides how a number it can't store, or a set with a
//! repeated member, is worded.
//!
//! The current layer (PutItem, UpdateItem and DeleteItem inside the
//! `1 validation error detected: ` envelope; GetItem, BatchGetItem, Query and
//! Scan bare) reads numbers by the stored-number rule, ends an empty one with
//! `: `, quotes duplicated members in a particular order and never wraps an
//! expression value. The older layer (BatchWriteItem, the transactions and
//! PartiQL) reads numbers as Java's `BigDecimal` does, prints members as sent
//! and wraps expression values naming the key. Captured in eu-west-2,
//! us-east-1, eu-west-1 and us-west-2, 2026-10-09; where the regions split
//! (eu-west-2 runs the current layer on the transactions), Dynoxide follows
//! the majority. Scan was one of those until 20 October 2026, when 19 of 33
//! regions read its values on the current layer.

#![cfg(feature = "http-server")]

use dynoxide::Database;
use serde_json::{Value, json};

const INVALID: &str = "One or more parameter values were invalid: ";
const OVERFLOW: &str =
    "Number overflow. Attempting to store a number with magnitude larger than supported range";
const UNDERFLOW: &str =
    "Number underflow. Attempting to store a number with magnitude smaller than supported range";

fn one(message: &str) -> String {
    format!("1 validation error detected: {message}")
}

fn for_key(message: &str, key: &str) -> String {
    format!("ExpressionAttributeValues contains invalid value: {message} for key {key}")
}

async fn start() -> String {
    let db = Database::memory().unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        dynoxide::server::serve_on(listener, db).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let url = format!("http://{addr}");
    let (status, created) = send(
        &url,
        "CreateTable",
        json!({
            "TableName": "Vlt",
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "BillingMode": "PAY_PER_REQUEST"
        }),
    )
    .await;
    assert_eq!(status, 200, "{created}");
    url
}

async fn send(url: &str, operation: &str, body: Value) -> (u16, Value) {
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
    (status, resp.json().await.unwrap())
}

/// The ValidationException message `operation` answers `body` with.
async fn refusal(url: &str, operation: &str, body: Value) -> String {
    let (status, resp) = send(url, operation, body.clone()).await;
    assert_eq!(status, 400, "{operation} {body}: {resp}");
    assert!(
        resp["__type"]
            .as_str()
            .unwrap()
            .ends_with("ValidationException"),
        "{operation} {body}: {resp}"
    );
    resp["message"].as_str().unwrap().to_string()
}

fn n(text: &str) -> Value {
    json!({"N": text})
}

#[tokio::test]
async fn single_item_writes_refuse_numbers_in_the_current_words_inside_the_envelope() {
    let url = start().await;
    for (text, message) in [
        (
            "abc",
            "The parameter cannot be converted to a numeric value: abc",
        ),
        ("", "The parameter cannot be converted to a numeric value: "),
        ("1e2147483647", OVERFLOW),
        ("1e-131", UNDERFLOW),
    ] {
        let put = refusal(
            &url,
            "PutItem",
            json!({"TableName": "Vlt", "Item": {"pk": {"S": "a"}, "n": n(text)}}),
        )
        .await;
        assert_eq!(put, one(message), "PutItem item {text:?}");
        let update = refusal(
            &url,
            "UpdateItem",
            json!({"TableName": "Vlt", "Key": {"pk": {"S": "a"}},
                "UpdateExpression": "SET n = :v", "ExpressionAttributeValues": {":v": n(text)}}),
        )
        .await;
        assert_eq!(update, one(message), "UpdateItem value {text:?}");
        let delete = refusal(
            &url,
            "DeleteItem",
            json!({"TableName": "Vlt", "Key": {"pk": {"S": "a"}},
                "ConditionExpression": "n = :v", "ExpressionAttributeValues": {":v": n(text)}}),
        )
        .await;
        assert_eq!(delete, one(message), "DeleteItem value {text:?}");
    }
}

#[tokio::test]
async fn reads_on_the_current_layer_refuse_numbers_bare() {
    let url = start().await;
    let message = "The parameter cannot be converted to a numeric value: ";
    let get = refusal(
        &url,
        "GetItem",
        json!({"TableName": "Vlt", "Key": {"pk": n("")}}),
    )
    .await;
    assert_eq!(get, message);
    let query = refusal(
        &url,
        "Query",
        json!({"TableName": "Vlt", "KeyConditionExpression": "pk = :p",
            "FilterExpression": "n = :v",
            "ExpressionAttributeValues": {":p": {"S": "a"}, ":v": n("")}}),
    )
    .await;
    assert_eq!(query, message);
    let scan = refusal(
        &url,
        "Scan",
        json!({"TableName": "Vlt", "FilterExpression": "n = :v",
            "ExpressionAttributeValues": {":v": n("")}}),
    )
    .await;
    assert_eq!(scan, message);
}

#[tokio::test]
async fn the_older_layer_reads_numbers_as_big_decimal_and_names_expression_values() {
    let url = start().await;
    let batch = refusal(
        &url,
        "BatchWriteItem",
        json!({"RequestItems": {"Vlt": [{"PutRequest": {"Item": {"pk": {"S": "a"}, "n": n("1e2147483647")}}}]}}),
    )
    .await;
    assert_eq!(batch, UNDERFLOW);
    let transaction = refusal(
        &url,
        "TransactWriteItems",
        json!({"TransactItems": [{"Update": {"TableName": "Vlt", "Key": {"pk": {"S": "a"}},
            "UpdateExpression": "SET n = :v", "ExpressionAttributeValues": {":v": n("1e2147483647")}}}]}),
    )
    .await;
    assert_eq!(transaction, for_key(UNDERFLOW, ":v"));
}

/// A transaction's expression values are read to the end even after one is
/// refused for its number, so a later value of the wrong JSON type still fails
/// the request as it is read, as it does on its own.
#[tokio::test]
async fn a_refused_expression_value_does_not_hide_a_later_one_of_the_wrong_type() {
    let url = start().await;
    let alone = send(
        &url,
        "TransactWriteItems",
        json!({"TransactItems": [{"Update": {"TableName": "Vlt", "Key": {"pk": {"S": "a"}},
            "UpdateExpression": "SET n = :b", "ExpressionAttributeValues": {":b": {"S": 5}}}}]}),
    )
    .await;
    let after_a_refusal = send(
        &url,
        "TransactWriteItems",
        json!({"TransactItems": [{"Update": {"TableName": "Vlt", "Key": {"pk": {"S": "a"}},
            "UpdateExpression": "SET n = :a, m = :b",
            "ExpressionAttributeValues": {":a": n("1e2147483647"), ":b": {"S": 5}}}}]}),
    )
    .await;
    assert_eq!(alone.0, 400);
    assert_eq!(
        alone.1["__type"],
        "com.amazon.coral.service#SerializationException"
    );
    assert_eq!(after_a_refusal, alone);
}

#[tokio::test]
async fn the_current_layer_quotes_duplicates_in_its_own_order() {
    let url = start().await;
    for (members, shown) in [
        (json!(["b", "a", "b"]), r#"["a", "b", "b"]"#),
        (json!(["b", "a", "b", "C"]), r#"["a", "b", "b", "C"]"#),
        (
            json!(["c", "a", "c", "a", "b"]),
            r#"["a", "c", "c", "a", "b"]"#,
        ),
        (json!(["q\"q", "q\"q"]), r#"["q\"q", "q\"q"]"#),
    ] {
        let got = refusal(
            &url,
            "PutItem",
            json!({"TableName": "Vlt", "Item": {"pk": {"S": "a"}, "s": {"SS": members}}}),
        )
        .await;
        assert_eq!(
            got,
            one(&format!(
                "{INVALID}Input collection {shown} contains duplicates."
            ))
        );
    }
    let binary = refusal(
        &url,
        "PutItem",
        json!({"TableName": "Vlt", "Item": {"pk": {"S": "a"},
            "b": {"BS": ["AA==", "fw==", "gA==", "/w==", "AA=="]}}}),
    )
    .await;
    assert_eq!(
        binary,
        one(&format!(
            r#"{INVALID}Input collection ["AA==","fw==","gA==","/w==","AA=="]of type BS contains duplicates."#
        ))
    );
    let number = refusal(
        &url,
        "GetItem",
        json!({"TableName": "Vlt", "Key": {"pk": {"NS": ["2", "1", "2"]}}}),
    )
    .await;
    assert_eq!(
        number,
        format!("{INVALID}Input collection contains duplicates.")
    );
}

#[tokio::test]
async fn query_start_keys_and_legacy_filters_use_the_current_words() {
    let url = start().await;
    let shown = format!(r#"{INVALID}Input collection ["a", "b", "b"] contains duplicates."#);
    let start_key = refusal(
        &url,
        "Query",
        json!({"TableName": "Vlt", "KeyConditionExpression": "pk = :p",
            "ExpressionAttributeValues": {":p": {"S": "a"}},
            "ExclusiveStartKey": {"pk": {"SS": ["b", "a", "b"]}}}),
    )
    .await;
    assert_eq!(start_key, shown);
    let query_filter = refusal(
        &url,
        "Query",
        json!({"TableName": "Vlt",
            "KeyConditions": {"pk": {"ComparisonOperator": "EQ", "AttributeValueList": [{"S": "a"}]}},
            "QueryFilter": {"s": {"ComparisonOperator": "EQ",
                "AttributeValueList": [{"SS": ["b", "a", "b"]}]}}}),
    )
    .await;
    assert_eq!(query_filter, shown);
}

#[tokio::test]
async fn scan_values_use_the_current_words_with_no_wrapper() {
    let url = start().await;
    let got = refusal(
        &url,
        "Scan",
        json!({"TableName": "Vlt", "FilterExpression": "s = :t",
            "ExpressionAttributeValues": {":t": {"SS": ["a", "a"]}}}),
    )
    .await;
    assert_eq!(
        got,
        format!(r#"{INVALID}Input collection ["a", "a"] contains duplicates."#)
    );
}

#[tokio::test]
async fn transactions_check_every_actions_sets_up_front() {
    let url = start().await;
    let older = format!("{INVALID}Input collection [b, a, b] contains duplicates.");
    // Ahead of a missing table.
    let missing_table = refusal(
        &url,
        "TransactWriteItems",
        json!({"TransactItems": [{"Delete": {"TableName": "Missing",
            "Key": {"pk": {"SS": ["b", "a", "b"]}}}}]}),
    )
    .await;
    assert_eq!(missing_table, older);
    // Ahead of a condition's syntax error, and in a later action.
    let after_a_good_action = refusal(
        &url,
        "TransactWriteItems",
        json!({"TransactItems": [
            {"Put": {"TableName": "Vlt", "Item": {"pk": {"S": "ok"}}}},
            {"ConditionCheck": {"TableName": "Vlt", "Key": {"pk": {"S": "a"}},
                "ConditionExpression": "s = = :t",
                "ExpressionAttributeValues": {":t": {"SS": ["b", "a", "b"]}}}}
        ]}),
    )
    .await;
    assert_eq!(after_a_good_action, for_key(&older, ":t"));
    let get = refusal(
        &url,
        "TransactGetItems",
        json!({"TransactItems": [{"Get": {"TableName": "Missing",
            "Key": {"pk": {"NS": ["2", "1", "2"]}}}}]}),
    )
    .await;
    assert_eq!(get, "Input collection contains duplicates");
    // Nothing was written by the good action.
    let (_, item) = send(
        &url,
        "GetItem",
        json!({"TableName": "Vlt", "Key": {"pk": {"S": "ok"}}}),
    )
    .await;
    assert!(item.get("Item").is_none(), "{item}");
}

#[tokio::test]
async fn expected_values_and_attribute_updates_are_checked_in_the_envelope() {
    let url = start().await;
    let shown = one(&format!(
        r#"{INVALID}Input collection ["a", "b", "b"] contains duplicates."#
    ));
    for operation in ["PutItem", "UpdateItem", "DeleteItem"] {
        let mut body = if operation == "PutItem" {
            json!({"TableName": "Vlt", "Item": {"pk": {"S": "e"}}})
        } else {
            json!({"TableName": "Vlt", "Key": {"pk": {"S": "e"}}})
        };
        body["Expected"] = json!({"s": {"Value": {"SS": ["b", "a", "b"]}}});
        assert_eq!(
            refusal(&url, operation, body.clone()).await,
            shown,
            "{operation}"
        );
        body["Expected"] = json!({"s": {"Value": {"SS": []}}});
        assert_eq!(
            refusal(&url, operation, body.clone()).await,
            one(&format!("{INVALID}An string set  may not be empty")),
            "{operation}"
        );
        body["Expected"] = json!({"n": {"Value": n("")}});
        assert_eq!(
            refusal(&url, operation, body).await,
            one("The parameter cannot be converted to a numeric value: "),
            "{operation}"
        );
    }
    let attribute_updates = refusal(
        &url,
        "UpdateItem",
        json!({"TableName": "Vlt", "Key": {"pk": {"S": "e"}},
            "AttributeUpdates": {"s": {"Action": "PUT", "Value": {"SS": ["b", "a", "b"]}}}}),
    )
    .await;
    assert_eq!(attribute_updates, shown);
    // The refused Expected value wrote nothing.
    let (_, item) = send(
        &url,
        "GetItem",
        json!({"TableName": "Vlt", "Key": {"pk": {"S": "e"}}}),
    )
    .await;
    assert!(item.get("Item").is_none(), "{item}");
}

#[tokio::test]
async fn partiql_set_literals_name_their_duplicates() {
    let url = start().await;
    let ns = |path: &str, members: &str| {
        format!("{INVALID}Input collection [{members}] contains duplicates! under root{path}")
    };
    for (statement, message) in [
        (
            "INSERT INTO \"Vlt\" VALUE {'pk': 'l1', 's': <<2, 1, 2>>}",
            ns("", "2, 1, 2"),
        ),
        (
            "INSERT INTO \"Vlt\" VALUE {'pk': 'l2', 'm': {'s': <<2, 1, 2>>}}",
            ns(".s", "2, 1, 2"),
        ),
        (
            "INSERT INTO \"Vlt\" VALUE {'pk': 'l3', 'l': [<<2, 1, 2>>]}",
            ns("[0]", "2, 1, 2"),
        ),
        (
            "INSERT INTO \"Vlt\" VALUE {'pk': 'l5', 's': <<1, 1.0>>}",
            ns("", "1, 1.0"),
        ),
        (
            "UPDATE \"Vlt\" SET s = <<3, 2, 3, 1>> WHERE pk = 'u1'",
            ns("", "3, 2, 3, 1"),
        ),
        (
            "SELECT * FROM \"Vlt\" WHERE pk = 'u1' AND s = <<2, 1, 2>>",
            ns("", "2, 1, 2"),
        ),
        (
            "INSERT INTO \"Vlt\" VALUE {'pk': 'l4', 'm': {'s': <<'b', 'a', 'b'>>}}",
            format!("{INVALID}Input collection [b, a, b] contains duplicates."),
        ),
        (
            "SELECT * FROM \"Vlt\" WHERE pk = 'u1' AND s = <<'b', 'a', 'b'>>",
            format!("{INVALID}Input collection [b, a, b] contains duplicates."),
        ),
    ] {
        let got = refusal(&url, "ExecuteStatement", json!({"Statement": statement})).await;
        assert_eq!(got, message, "{statement}");
    }
}

/// Send `body` exactly as written, so the order of its members is the order
/// sent.
async fn send_text(url: &str, operation: &str, body: &str) -> (u16, Value) {
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
    (status, resp.json().await.unwrap())
}

/// The error `operation` answers `body` with, as `(type, message)`.
async fn answer(url: &str, operation: &str, body: &str) -> (String, String) {
    let (status, resp) = send_text(url, operation, body).await;
    assert_eq!(status, 400, "{operation} {body}: {resp}");
    let kind = resp["__type"].as_str().unwrap();
    let kind = kind.rsplit_once('#').map_or(kind, |(_, kind)| kind);
    let message = resp["message"]
        .as_str()
        .or_else(|| resp["Message"].as_str())
        .unwrap_or_default();
    (kind.to_string(), message.to_string())
}

/// Attribute names and the JSON of their values, in the order sent.
type Pairs<'a> = &'a [(&'a str, &'a str)];
/// An error's type and message.
type Answer = (String, String);

fn bare(message: &str) -> (String, String) {
    ("ValidationException".to_string(), message.to_string())
}

const ABC: &str = "The parameter cannot be converted to a numeric value: abc";
const NULL_FALSE: &str = "One or more parameter values were invalid: Null attribute value types must have the value of true";
const MORE_THAN_ONE: &str = "Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes";
const EMPTY_SS: &str = "One or more parameter values were invalid: An string set  may not be empty";

/// A request that holds a `BOOL` or `NULL` written as a string, or a value with
/// more than one member, is read whole by the older layer: every value refusal
/// in it, wherever it is, comes back bare on PutItem and naming its key on
/// UpdateItem, where the same refusal alone gets the envelope. Captured
/// against eu-west-2 and us-east-1, 2026-10-10.
#[tokio::test]
async fn a_string_boolean_or_a_second_member_puts_the_request_on_the_older_layer() {
    let url = start().await;
    let put =
        |values: &str| format!(r#"{{"TableName":"Vlt","Item":{{"pk":{{"S":"m1"}},{values}}}}}"#);
    let update = |pairs: &[(&str, &str)]| {
        let set: Vec<String> = pairs
            .iter()
            .map(|(name, _)| format!("{name} = :{name}"))
            .collect();
        let values: Vec<String> = pairs
            .iter()
            .map(|(name, value)| format!(r#"":{name}":{value}"#))
            .collect();
        format!(
            r#"{{"TableName":"Vlt","Key":{{"pk":{{"S":"m1"}}}},"UpdateExpression":"SET {}","ExpressionAttributeValues":{{{}}}}}"#,
            set.join(", "),
            values.join(",")
        )
    };
    let put_pairs = |pairs: &[(&str, &str)]| {
        let values: Vec<String> = pairs
            .iter()
            .map(|(name, value)| format!(r#""{name}":{value}"#))
            .collect();
        put(&values.join(","))
    };

    let cases: [(Pairs, Answer, Answer); 6] = [
        // Alone, a number DynamoDB can't read is refused in the envelope.
        (&[("v", r#"{"N":"abc"}"#)], bare(&one(ABC)), bare(&one(ABC))),
        // Beside a BOOL written as a string, in either order, it is not.
        (
            &[("v", r#"{"N":"abc"}"#), ("w", r#"{"BOOL":"no"}"#)],
            bare(ABC),
            bare(&for_key(ABC, ":v")),
        ),
        (
            &[("w", r#"{"BOOL":"no"}"#), ("v", r#"{"N":"abc"}"#)],
            bare(ABC),
            bare(&for_key(ABC, ":v")),
        ),
        // A BOOL written as JSON leaves the request where it was.
        (
            &[("v", r#"{"N":"abc"}"#), ("w", r#"{"BOOL":true}"#)],
            bare(&one(ABC)),
            bare(&one(ABC)),
        ),
        // A NULL that is JSON false gets the envelope, and as a string doesn't.
        (
            &[("v", r#"{"NULL":false}"#)],
            bare(&one(NULL_FALSE)),
            bare(&one(NULL_FALSE)),
        ),
        (
            &[("v", r#"{"NULL":"no"}"#)],
            bare(NULL_FALSE),
            bare(&for_key(NULL_FALSE, ":v")),
        ),
    ];
    for (pairs, on_put, on_update) in cases {
        assert_eq!(
            answer(&url, "PutItem", &put_pairs(pairs)).await,
            on_put,
            "PutItem {pairs:?}"
        );
        assert_eq!(
            answer(&url, "UpdateItem", &update(pairs)).await,
            on_update,
            "UpdateItem {pairs:?}"
        );
    }
}

/// A value with more than one member is checked in a fixed order: a BOOL or
/// NULL that doesn't read as a boolean first (the whole request is malformed),
/// then an empty set, then a number DynamoDB can't read, then a NULL that
/// reads as false, and only then the second member. Each is refused bare, and
/// UpdateItem names the key. Captured against eu-west-2 and us-east-1,
/// 2026-10-10.
#[tokio::test]
async fn a_value_with_two_members_is_checked_in_dynamodbs_order() {
    let url = start().await;
    let serialization = |message: &str| ("SerializationException".to_string(), message.to_string());
    let cases = [
        (
            r#"{"N":"abc","BOOL":"maybe"}"#,
            Some(serialization("Unexpected token received from parser")),
        ),
        (
            r#"{"BOOL":"maybe","N":"abc"}"#,
            Some(serialization("Unexpected token received from parser")),
        ),
        (r#"{"SS":[],"S":"x"}"#, Some(bare(EMPTY_SS))),
        (r#"{"N":"abc","SS":[]}"#, Some(bare(EMPTY_SS))),
        (r#"{"N":"abc","S":"x"}"#, Some(bare(ABC))),
        (r#"{"N":"abc","NULL":"no"}"#, Some(bare(ABC))),
        (r#"{"NULL":"no","N":"abc"}"#, Some(bare(ABC))),
        (r#"{"N":"abc","NULL":false}"#, Some(bare(ABC))),
        (r#"{"S":"x","NULL":false}"#, Some(bare(NULL_FALSE))),
        (r#"{"S":"x","NULL":"no"}"#, Some(bare(NULL_FALSE))),
        (r#"{"S":"x","N":"1"}"#, Some(bare(MORE_THAN_ONE))),
        (r#"{"S":"x","NULL":true}"#, Some(bare(MORE_THAN_ONE))),
        (r#"{"S":"x","BOOL":"no"}"#, Some(bare(MORE_THAN_ONE))),
        (r#"{"S":"x","BOOL":true}"#, Some(bare(MORE_THAN_ONE))),
    ];
    for (value, refusal) in cases {
        let refusal = refusal.unwrap();
        let put = format!(r#"{{"TableName":"Vlt","Item":{{"pk":{{"S":"m1"}},"v":{value}}}}}"#);
        assert_eq!(
            answer(&url, "PutItem", &put).await,
            refusal,
            "PutItem {value}"
        );

        let update = format!(
            r#"{{"TableName":"Vlt","Key":{{"pk":{{"S":"m1"}}}},"UpdateExpression":"SET v = :v","ExpressionAttributeValues":{{":v":{value}}}}}"#
        );
        let named = if refusal.0 == "ValidationException" {
            bare(&for_key(&refusal.1, ":v"))
        } else {
            refusal.clone()
        };
        assert_eq!(
            answer(&url, "UpdateItem", &update).await,
            named,
            "UpdateItem {value}"
        );
    }

    // A PartiQL parameter is refused the same way, bare.
    for (value, refusal) in [
        (
            r#"{"N":"abc","BOOL":"maybe"}"#,
            serialization("Unexpected token received from parser"),
        ),
        (r#"{"N":"abc","NULL":"no"}"#, bare(ABC)),
        (r#"{"N":"abc","NULL":false}"#, bare(ABC)),
        (r#"{"S":"x","NULL":"no"}"#, bare(NULL_FALSE)),
    ] {
        let statement = format!(
            r#"{{"Statement":"UPDATE \"Vlt\" SET v = ? WHERE pk = 'm1'","Parameters":[{value}]}}"#
        );
        assert_eq!(
            answer(&url, "ExecuteStatement", &statement).await,
            refusal,
            "ExecuteStatement {value}"
        );
    }
}
