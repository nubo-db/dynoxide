//! Request-member validation on the single-item operations, Query, Scan and
//! SearchVectors: which constraint DynamoDB reports, in which wording, and
//! which errors it reports together.
//!
//! DynamoDB is moving each operation to a newer validation layer region by
//! region, so the answers differ between regions. Captured on 2026-10-09 in
//! eu-west-2, us-east-1, eu-west-1, us-west-2, ap-northeast-1, eu-south-2 and
//! me-central-1, and each test follows the majority of those seven for its
//! operation: the newer layer for PutItem, UpdateItem, DeleteItem, GetItem,
//! Query and SearchVectors. Scan ran on the older layer in most regions then;
//! by 20 October 2026 the newer one had reached 20 of 33, and its checks are
//! pinned request by request in `scan_member_stages.rs`.

#![cfg(feature = "http-server")]

use dynoxide::Database;

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

/// Send one request and return the message of the ValidationException it must
/// be refused with.
async fn refused(url: &str, operation: &str, body: serde_json::Value) -> String {
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
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400, "expected a 400 for {operation} {body}");
    let text = resp.text().await.unwrap();
    assert!(
        !text.contains("VALIDATION") && !text.contains(" at line "),
        "internal marker or serde position leaked: {text}"
    );
    let parsed: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert!(
        parsed["__type"]
            .as_str()
            .unwrap()
            .ends_with("#ValidationException"),
        "unexpected __type for {operation} {body}: {parsed}"
    );
    parsed["message"].as_str().unwrap().to_string()
}

fn one(msg: &str) -> String {
    format!("1 validation error detected: {msg}")
}

fn two(a: &str, b: &str) -> String {
    format!("2 validation errors detected: {a}; {b}")
}

/// Merge `extra` into `base`.
fn with(mut base: serde_json::Value, extra: serde_json::Value) -> serde_json::Value {
    for (k, v) in extra.as_object().unwrap() {
        base[k] = v.clone();
    }
    base
}

const T: &str = "AbsentTable";
const EMPTY_NAME: &str = "Value at 'TableName' failed to satisfy constraint: Member must have length greater than or equal to 1";
const RCC: &str = "Value 'INVALID' at 'returnConsumedCapacity' failed to satisfy constraint: \
                   Member must satisfy enum value set: [INDEXES, TOTAL, NONE]";
const RICM: &str = "Value 'INVALID' at 'returnItemCollectionMetrics' failed to satisfy \
                    constraint: Member must satisfy enum value set: [SIZE, NONE]";
const RV: &str = "Failed to satisfy constraint: Member must satisfy enum value set: \
                  [ALL_OLD, UPDATED_OLD, ALL_NEW, UPDATE_NEW, NONE]";
const RVOCCF: &str = "Value 'BOGUS' at 'returnValuesOnConditionCheckFailure' failed to \
                      satisfy constraint: Member must satisfy enum value set: [ALL_OLD, NONE]";
const SELECT: &str = "Value 'BOGUS' at 'select' failed to satisfy constraint: Member must \
                      satisfy enum value set: [SPECIFIC_ATTRIBUTES, COUNT, ALL_ATTRIBUTES, \
                      ALL_PROJECTED_ATTRIBUTES]";
const NO_FILTER: &str = "ConditionalOperator cannot be used without Filter or Expected";
const ONE_FILTER: &str =
    "ConditionalOperator can only be used when Filter or Expected has two or more elements";

fn short(name: &str) -> String {
    format!(
        "Value '{name}' at 'tableName' failed to satisfy constraint: Member must have length \
         greater than or equal to 3"
    )
}

fn pattern(name: &str) -> String {
    format!(
        "Value '{name}' at 'tableName' failed to satisfy constraint: Member must satisfy \
         regular expression pattern: [a-zA-Z0-9_.-]+"
    )
}

fn key() -> serde_json::Value {
    serde_json::json!({"pk": {"S": "a"}})
}

fn one_expected() -> serde_json::Value {
    serde_json::json!({"Expected": {"a": {"Exists": false}}})
}

/// PutItem and DeleteItem: an empty name alone in the newer wording; any other
/// name error reported with the enums, one error per name; the enums in the
/// order ReturnConsumedCapacity, ReturnValues, ReturnItemCollectionMetrics,
/// ReturnValuesOnConditionCheckFailure; then `ConditionalOperator`, which is
/// not checked as an enum, only against the legacy condition it qualifies.
#[tokio::test]
async fn put_item_and_delete_item_member_errors() {
    let url = start_server().await;
    for (operation, base) in [
        (
            "PutItem",
            serde_json::json!({"TableName": T, "Item": key()}),
        ),
        (
            "DeleteItem",
            serde_json::json!({"TableName": T, "Key": key()}),
        ),
    ] {
        let send = |extra: serde_json::Value| {
            let body = with(base.clone(), extra);
            let url = url.clone();
            async move { refused(&url, operation, body).await }
        };
        let cases: Vec<(serde_json::Value, String)> = vec![
            (serde_json::json!({"TableName": ""}), one(EMPTY_NAME)),
            (
                serde_json::json!({"TableName": "", "ReturnConsumedCapacity": "INVALID"}),
                one(EMPTY_NAME),
            ),
            (serde_json::json!({"TableName": "a!"}), one(&short("a!"))),
            (
                serde_json::json!({"TableName": "ab", "ReturnConsumedCapacity": "INVALID"}),
                two(&short("ab"), RCC),
            ),
            (
                serde_json::json!({"TableName": "bad!name", "ReturnConsumedCapacity": "INVALID"}),
                two(&pattern("bad!name"), RCC),
            ),
            (
                serde_json::json!({"TableName": "ab", "ReturnValues": "INVALID"}),
                two(&short("ab"), RV),
            ),
            (
                serde_json::json!({"ReturnValuesOnConditionCheckFailure": "BOGUS"}),
                one(RVOCCF),
            ),
            (
                serde_json::json!({"ReturnValuesOnConditionCheckFailure": "BOGUS",
                    "ReturnConsumedCapacity": "INVALID"}),
                two(RCC, RVOCCF),
            ),
            (
                serde_json::json!({"ReturnValuesOnConditionCheckFailure": "BOGUS",
                    "ReturnItemCollectionMetrics": "INVALID"}),
                two(RICM, RVOCCF),
            ),
            (
                serde_json::json!({"ReturnValuesOnConditionCheckFailure": "BOGUS",
                    "ReturnValues": "INVALID"}),
                two(RV, RVOCCF),
            ),
            (
                serde_json::json!({"ConditionalOperator": "XOR"}),
                one(NO_FILTER),
            ),
            (
                serde_json::json!({"ConditionalOperator": "AND"}),
                one(NO_FILTER),
            ),
            (
                with(
                    one_expected(),
                    serde_json::json!({"ConditionalOperator": "XOR"}),
                ),
                one(ONE_FILTER),
            ),
            (
                serde_json::json!({"ConditionalOperator": "XOR",
                    "ReturnConsumedCapacity": "INVALID"}),
                one(RCC),
            ),
            (
                serde_json::json!({"ConditionalOperator": "XOR", "ReturnValues": "INVALID",
                    "ReturnConsumedCapacity": "INVALID", "ReturnItemCollectionMetrics": "INVALID"}),
                format!("3 validation errors detected: {RCC}; {RV}; {RICM}"),
            ),
        ];
        for (extra, expected) in cases {
            assert_eq!(
                send(extra.clone()).await,
                expected,
                "{operation} with {extra}"
            );
        }
    }
}

/// UpdateItem: a name error alone (a name both short and malformed lists the
/// pattern first); then one enum error, ReturnValues before
/// ReturnValuesOnConditionCheckFailure before ReturnItemCollectionMetrics
/// before ReturnConsumedCapacity, the second and fourth without the envelope;
/// then `ConditionalOperator` against the expression or legacy parameters.
#[tokio::test]
async fn update_item_member_errors() {
    let url = start_server().await;
    let set = serde_json::json!({"UpdateExpression": "SET a = :a",
        "ExpressionAttributeValues": {":a": {"S": "x"}}});
    let base = with(
        serde_json::json!({"TableName": T, "Key": key()}),
        set.clone(),
    );
    let legacy = serde_json::json!({"TableName": T, "Key": key(),
        "AttributeUpdates": {"a": {"Action": "PUT", "Value": {"S": "x"}}}});
    let bare = serde_json::json!({"TableName": T, "Key": key()});
    let cases: Vec<(serde_json::Value, String)> = vec![
        (
            with(base.clone(), serde_json::json!({"TableName": ""})),
            one(EMPTY_NAME),
        ),
        (
            with(
                base.clone(),
                serde_json::json!({"TableName": "ab", "ReturnConsumedCapacity": "INVALID"}),
            ),
            one(&short("ab")),
        ),
        (
            with(
                base.clone(),
                serde_json::json!({"TableName": "bad!name", "ReturnConsumedCapacity": "INVALID"}),
            ),
            one(&pattern("bad!name")),
        ),
        (
            with(
                base.clone(),
                serde_json::json!({"TableName": "a!", "ReturnConsumedCapacity": "INVALID"}),
            ),
            two(&pattern("a!"), &short("a!")),
        ),
        (
            with(
                base.clone(),
                serde_json::json!({"ReturnConsumedCapacity": "INVALID"}),
            ),
            RCC.to_string(),
        ),
        (
            with(
                base.clone(),
                serde_json::json!({"ReturnItemCollectionMetrics": "INVALID"}),
            ),
            one(RICM),
        ),
        (
            with(
                base.clone(),
                serde_json::json!({"ReturnConsumedCapacity": "INVALID",
                "ReturnItemCollectionMetrics": "INVALID"}),
            ),
            one(RICM),
        ),
        (
            with(
                bare.clone(),
                serde_json::json!({"ReturnValuesOnConditionCheckFailure": "BOGUS"}),
            ),
            RVOCCF.to_string(),
        ),
        (
            with(
                base.clone(),
                serde_json::json!({"ReturnValuesOnConditionCheckFailure": "BOGUS",
                "ReturnItemCollectionMetrics": "INVALID"}),
            ),
            RVOCCF.to_string(),
        ),
        (
            with(
                base.clone(),
                serde_json::json!({"ReturnValuesOnConditionCheckFailure": "BOGUS",
                "ReturnConsumedCapacity": "INVALID"}),
            ),
            RVOCCF.to_string(),
        ),
        (
            with(
                base.clone(),
                serde_json::json!({"ReturnValuesOnConditionCheckFailure": "BOGUS",
                "ReturnValues": "INVALID"}),
            ),
            one(RV),
        ),
        (
            with(
                base.clone(),
                serde_json::json!({"ConditionalOperator": "XOR"}),
            ),
            one(
                "Can not use both expression and non-expression parameters in the same \
                 request: Non-expression parameters: {ConditionalOperator} Expression \
                 parameters: {UpdateExpression}",
            ),
        ),
        (
            with(
                base.clone(),
                serde_json::json!({"ConditionalOperator": "XOR",
                "ReturnConsumedCapacity": "INVALID"}),
            ),
            RCC.to_string(),
        ),
        (
            with(
                base.clone(),
                serde_json::json!({"ConditionalOperator": "XOR",
                "ReturnItemCollectionMetrics": "INVALID"}),
            ),
            one(RICM),
        ),
        (
            with(
                legacy.clone(),
                serde_json::json!({"ConditionalOperator": "XOR"}),
            ),
            one(NO_FILTER),
        ),
        (
            with(
                legacy.clone(),
                serde_json::json!({"ConditionalOperator": "AND"}),
            ),
            one(NO_FILTER),
        ),
        (
            with(
                with(legacy.clone(), one_expected()),
                serde_json::json!({"ConditionalOperator": "XOR"}),
            ),
            one(ONE_FILTER),
        ),
        (
            with(
                bare.clone(),
                serde_json::json!({"ConditionalOperator": "XOR"}),
            ),
            one(NO_FILTER),
        ),
    ];
    for (body, expected) in cases {
        assert_eq!(
            refused(&url, "UpdateItem", body.clone()).await,
            expected,
            "{body}"
        );
    }
}

/// GetItem: an empty name alone in the newer wording; any other name error, one
/// per name, reported with ReturnConsumedCapacity.
#[tokio::test]
async fn get_item_member_errors() {
    let url = start_server().await;
    let base = serde_json::json!({"TableName": T, "Key": key()});
    let cases: Vec<(serde_json::Value, String)> = vec![
        (serde_json::json!({"TableName": ""}), one(EMPTY_NAME)),
        (
            serde_json::json!({"TableName": "", "ReturnConsumedCapacity": "INVALID"}),
            one(EMPTY_NAME),
        ),
        (serde_json::json!({"TableName": "a!"}), one(&short("a!"))),
        (
            serde_json::json!({"TableName": "ab", "ReturnConsumedCapacity": "INVALID"}),
            two(&short("ab"), RCC),
        ),
        (
            serde_json::json!({"TableName": "bad!name", "ReturnConsumedCapacity": "INVALID"}),
            two(&pattern("bad!name"), RCC),
        ),
    ];
    for (extra, expected) in cases {
        let body = with(base.clone(), extra);
        assert_eq!(
            refused(&url, "GetItem", body.clone()).await,
            expected,
            "{body}"
        );
    }
}

/// Query: `Limit` of 0 and an empty name alone, in the newer wording; a name
/// error, one per name, with ReturnConsumedCapacity; then mixing
/// `ConditionalOperator` with an expression; then `Select`. The legacy
/// `ConditionalOperator` checks answer without the envelope.
#[tokio::test]
async fn query_member_errors() {
    let url = start_server().await;
    let base = serde_json::json!({"TableName": T, "KeyConditionExpression": "pk = :p",
        "ExpressionAttributeValues": {":p": {"S": "a"}}});
    let legacy = serde_json::json!({"TableName": T,
        "KeyConditions": {"pk": {"ComparisonOperator": "EQ", "AttributeValueList": [{"S": "a"}]}}});
    let limit = one(
        "Value at 'Limit' failed to satisfy constraint: Member must have value greater than or equal to 1",
    );
    let mixing = "Can not use both expression and non-expression parameters in the same \
                  request: Non-expression parameters: {ConditionalOperator} Expression \
                  parameters: {KeyConditionExpression}";
    let cases: Vec<(serde_json::Value, String)> = vec![
        (
            with(base.clone(), serde_json::json!({"TableName": ""})),
            one(EMPTY_NAME),
        ),
        (
            with(
                base.clone(),
                serde_json::json!({"TableName": "", "ReturnConsumedCapacity": "INVALID"}),
            ),
            one(EMPTY_NAME),
        ),
        (
            with(base.clone(), serde_json::json!({"TableName": "a!"})),
            one(&short("a!")),
        ),
        (
            with(
                base.clone(),
                serde_json::json!({"TableName": "ab", "ReturnConsumedCapacity": "INVALID"}),
            ),
            two(&short("ab"), RCC),
        ),
        (
            with(
                base.clone(),
                serde_json::json!({"TableName": "bad!name", "ReturnConsumedCapacity": "INVALID"}),
            ),
            two(&pattern("bad!name"), RCC),
        ),
        (
            with(
                base.clone(),
                serde_json::json!({"TableName": "ab", "Select": "BOGUS"}),
            ),
            one(&short("ab")),
        ),
        (
            with(
                base.clone(),
                serde_json::json!({"ReturnConsumedCapacity": "INVALID", "Select": "BOGUS"}),
            ),
            one(RCC),
        ),
        (
            with(base.clone(), serde_json::json!({"Select": "BOGUS"})),
            one(SELECT),
        ),
        (
            with(
                base.clone(),
                serde_json::json!({"Select": "BOGUS", "Limit": 0}),
            ),
            limit.clone(),
        ),
        (
            with(
                base.clone(),
                serde_json::json!({"ReturnConsumedCapacity": "INVALID", "Limit": 0}),
            ),
            limit.clone(),
        ),
        (
            with(
                base.clone(),
                serde_json::json!({"ConditionalOperator": "XOR"}),
            ),
            mixing.to_string(),
        ),
        (
            with(
                base.clone(),
                serde_json::json!({"ConditionalOperator": "XOR", "Select": "BOGUS"}),
            ),
            mixing.to_string(),
        ),
        (
            with(
                base.clone(),
                serde_json::json!({"ConditionalOperator": "XOR", "Limit": 0}),
            ),
            limit.clone(),
        ),
        (
            with(
                base.clone(),
                serde_json::json!({"ConditionalOperator": "XOR",
                "ReturnConsumedCapacity": "INVALID"}),
            ),
            one(RCC),
        ),
        (
            with(
                legacy.clone(),
                serde_json::json!({"ConditionalOperator": "XOR"}),
            ),
            NO_FILTER.to_string(),
        ),
        (
            with(
                legacy.clone(),
                serde_json::json!({"ConditionalOperator": "AND"}),
            ),
            NO_FILTER.to_string(),
        ),
        (
            with(
                legacy.clone(),
                serde_json::json!({"ConditionalOperator": "XOR",
                "QueryFilter": {"b": {"ComparisonOperator": "NULL"}}}),
            ),
            ONE_FILTER.to_string(),
        ),
    ];
    for (body, expected) in cases {
        assert_eq!(
            refused(&url, "Query", body.clone()).await,
            expected,
            "{body}"
        );
    }
}

/// SearchVectors: a name error without the envelope, length before pattern and
/// one error per name, ahead of ReturnConsumedCapacity; `TopK` below 1 and an
/// empty name in the newer enveloped wording, `TopK` ahead of the name.
#[tokio::test]
async fn search_vectors_member_errors() {
    let url = start_server().await;
    let base = serde_json::json!({"TableName": T, "IndexName": "vix",
        "SearchVector": [{"N": "1"}], "TopK": 1});
    let cases: Vec<(serde_json::Value, String)> = vec![
        (serde_json::json!({"TableName": "T"}), short("T")),
        (serde_json::json!({"TableName": "ab"}), short("ab")),
        (serde_json::json!({"TableName": "a!"}), short("a!")),
        (
            serde_json::json!({"TableName": "bad!name"}),
            pattern("bad!name"),
        ),
        (serde_json::json!({"TableName": ""}), one(EMPTY_NAME)),
        (
            serde_json::json!({"TableName": "T", "TopK": 0}),
            one(
                "Value at 'TopK' failed to satisfy constraint: Member must have value greater than or equal to 1",
            ),
        ),
        (
            serde_json::json!({"TableName": "T", "ReturnConsumedCapacity": "INVALID"}),
            short("T"),
        ),
    ];
    for (extra, expected) in cases {
        let body = with(base.clone(), extra);
        assert_eq!(
            refused(&url, "SearchVectors", body.clone()).await,
            expected,
            "{body}"
        );
    }
}

const KEY_NULL: &str = "Value null at 'key' failed to satisfy constraint: Member must not be null";
const XOR_ENUM: &str = "Value 'XOR' at 'conditionalOperator' failed to satisfy constraint: \
                        Member must satisfy enum value set: [AND, OR]";

fn two_expected() -> serde_json::Value {
    serde_json::json!({"Expected": {"a": {"Exists": false}, "b": {"Exists": false}}})
}

/// UpdateItem without its key falls back to the older validation layer in all
/// seven regions captured on 2026-10-09: the enum errors in their older
/// per-value wording, every one of them, then the key. A table name error is
/// not reported beside it.
#[tokio::test]
async fn update_item_without_its_key() {
    let url = start_server().await;
    let base = serde_json::json!({"TableName": T, "UpdateExpression": "SET a = :a",
        "ExpressionAttributeValues": {":a": {"S": "x"}}});
    let rv_old = "Value 'INVALID' at 'returnValues' failed to satisfy constraint: Member must \
                  satisfy enum value set: [ALL_NEW, UPDATED_OLD, ALL_OLD, NONE, UPDATED_NEW]";
    let cases: Vec<(serde_json::Value, String)> = vec![
        (serde_json::json!({}), one(KEY_NULL)),
        (
            serde_json::json!({"ReturnValuesOnConditionCheckFailure": "BOGUS"}),
            two(RVOCCF, KEY_NULL),
        ),
        (
            serde_json::json!({"ReturnConsumedCapacity": "INVALID"}),
            two(RCC, KEY_NULL),
        ),
        (
            serde_json::json!({"ReturnValues": "INVALID"}),
            two(rv_old, KEY_NULL),
        ),
        (
            serde_json::json!({"ReturnItemCollectionMetrics": "INVALID"}),
            two(RICM, KEY_NULL),
        ),
        (
            serde_json::json!({"ReturnValuesOnConditionCheckFailure": "BOGUS",
                "ReturnItemCollectionMetrics": "INVALID"}),
            format!("3 validation errors detected: {RVOCCF}; {RICM}; {KEY_NULL}"),
        ),
        (serde_json::json!({"TableName": "ab"}), one(KEY_NULL)),
    ];
    for (extra, expected) in cases {
        let body = with(base.clone(), extra);
        assert_eq!(
            refused(&url, "UpdateItem", body.clone()).await,
            expected,
            "{body}"
        );
    }
}

/// Query reports an empty name and a `Limit` of 0 together, Limit first, and
/// nothing else beside them (captured 2026-10-09, six of seven regions).
#[tokio::test]
async fn query_reports_an_empty_name_and_a_zero_limit_together() {
    let url = start_server().await;
    let base = serde_json::json!({"TableName": "", "KeyConditionExpression": "pk = :p",
        "ExpressionAttributeValues": {":p": {"S": "a"}}, "Limit": 0});
    let limit = "Value at 'Limit' failed to satisfy constraint: Member must have value greater \
                 than or equal to 1";
    assert_eq!(
        refused(&url, "Query", base.clone()).await,
        two(limit, EMPTY_NAME)
    );
    assert_eq!(
        refused(
            &url,
            "Query",
            with(
                base,
                serde_json::json!({"ReturnConsumedCapacity": "INVALID"})
            )
        )
        .await,
        two(limit, EMPTY_NAME),
        "the capacity mode is not reported beside them"
    );
}

/// A `ConditionalOperator` that does join two or more legacy conditions but
/// isn't `AND` or `OR` gets the newer layer's enum message, naming the value
/// and the member and listing `[AND, OR]`: enveloped on PutItem, UpdateItem
/// and DeleteItem, bare on Query and Scan (captured eu-west-2 and us-east-1,
/// 2026-11-11). On 2026-10-09 and 2026-10-20 the message named neither and
/// listed `[ALL, OR]`; which requests get it, and in which envelope, has not
/// moved.
#[tokio::test]
async fn an_unknown_conditional_operator_joining_two_legacy_conditions() {
    let url = start_server().await;
    let xor = serde_json::json!({"ConditionalOperator": "XOR"});
    let cases: Vec<(&str, serde_json::Value, String)> = vec![
        (
            "PutItem",
            with(
                with(
                    serde_json::json!({"TableName": T, "Item": key()}),
                    two_expected(),
                ),
                xor.clone(),
            ),
            one(XOR_ENUM),
        ),
        (
            "DeleteItem",
            with(
                with(
                    serde_json::json!({"TableName": T, "Key": key()}),
                    two_expected(),
                ),
                xor.clone(),
            ),
            one(XOR_ENUM),
        ),
        (
            "UpdateItem",
            with(
                with(
                    serde_json::json!({"TableName": T, "Key": key(),
                        "AttributeUpdates": {"a": {"Action": "PUT", "Value": {"S": "x"}}}}),
                    two_expected(),
                ),
                xor.clone(),
            ),
            one(XOR_ENUM),
        ),
        (
            "Query",
            with(
                serde_json::json!({"TableName": T,
                    "KeyConditions": {"pk": {"ComparisonOperator": "EQ", "AttributeValueList": [{"S": "a"}]}},
                    "QueryFilter": {"b": {"ComparisonOperator": "NULL"}, "c": {"ComparisonOperator": "NULL"}}}),
                xor.clone(),
            ),
            XOR_ENUM.to_string(),
        ),
        (
            "Scan",
            with(
                serde_json::json!({"TableName": T,
                    "ScanFilter": {"b": {"ComparisonOperator": "NULL"}, "c": {"ComparisonOperator": "NULL"}}}),
                xor.clone(),
            ),
            XOR_ENUM.to_string(),
        ),
        (
            "PutItem",
            with(
                with(
                    serde_json::json!({"TableName": T, "Item": key()}),
                    two_expected(),
                ),
                serde_json::json!({"ConditionalOperator": "XOR", "ReturnConsumedCapacity": "INVALID"}),
            ),
            one(RCC),
        ),
    ];
    for (operation, body, expected) in cases {
        assert_eq!(
            refused(&url, operation, body.clone()).await,
            expected,
            "{operation} {body}"
        );
    }
}

/// PutItem and DeleteItem answer a `ConditionalOperator` beside a
/// `ConditionExpression` with the mixing message, enveloped, whatever its
/// value (captured 2026-10-09, five of seven regions).
#[tokio::test]
async fn a_conditional_operator_beside_a_condition_expression() {
    let url = start_server().await;
    let mixing = |non_expression: &str| {
        one(&format!(
            "Can not use both expression and non-expression parameters in the same request: \
             Non-expression parameters: {{{non_expression}}} Expression parameters: \
             {{ConditionExpression}}"
        ))
    };
    for (operation, base) in [
        (
            "PutItem",
            serde_json::json!({"TableName": T, "Item": key(),
            "ConditionExpression": "attribute_not_exists(pk)"}),
        ),
        (
            "DeleteItem",
            serde_json::json!({"TableName": T, "Key": key(),
            "ConditionExpression": "attribute_exists(pk)"}),
        ),
    ] {
        for (extra, expected) in [
            (
                serde_json::json!({"ConditionalOperator": "XOR"}),
                mixing("ConditionalOperator"),
            ),
            (
                serde_json::json!({"ConditionalOperator": "AND"}),
                mixing("ConditionalOperator"),
            ),
            (
                with(
                    two_expected(),
                    serde_json::json!({"ConditionalOperator": "AND"}),
                ),
                mixing("Expected, ConditionalOperator"),
            ),
        ] {
            let body = with(base.clone(), extra);
            assert_eq!(
                refused(&url, operation, body.clone()).await,
                expected,
                "{operation} {body}"
            );
        }
    }
}

/// SearchVectors reports a short table name only once its other members pass,
/// and a missing IndexName, SearchVector or TopK in serde's own words, with the
/// position in the body (captured 2026-10-09, six of six regions).
#[tokio::test]
async fn search_vectors_reports_its_other_members_before_a_short_name() {
    let url = start_server().await;
    let base = serde_json::json!({"TableName": "T", "IndexName": "vix",
        "SearchVector": [{"N": "1"}], "TopK": 1});
    let index = "Value at 'IndexName' failed to satisfy constraint: Member must have length \
                 greater than or equal to 3";
    let vector = "Value at 'SearchVector' failed to satisfy constraint: Member must have \
                  length greater than or equal to 1";
    let top_k = "Value at 'TopK' failed to satisfy constraint: Member must have value greater \
                 than or equal to 1";
    for (extra, expected) in [
        (serde_json::json!({"IndexName": "ab"}), one(index)),
        (serde_json::json!({"SearchVector": []}), one(vector)),
        (
            serde_json::json!({"IndexName": "ab", "TopK": 0}),
            two(index, top_k),
        ),
        (
            serde_json::json!({"TableName": "bad!name", "IndexName": "ab"}),
            one(index),
        ),
    ] {
        let body = with(base.clone(), extra);
        assert_eq!(
            refused(&url, "SearchVectors", body.clone()).await,
            expected,
            "{body}"
        );
    }

    for member in ["IndexName", "SearchVector", "TopK"] {
        let mut body = base.clone();
        body.as_object_mut().unwrap().remove(member);
        let resp = reqwest::Client::new()
            .post(&url)
            .header("x-amz-target", "DynamoDB_20120810.SearchVectors")
            .header("content-type", "application/x-amz-json-1.0")
            .header(
                "authorization",
                "AWS4-HMAC-SHA256 Credential=fakekey/20260101/us-east-1/dynamodb/aws4_request, \
                 SignedHeaders=host;x-amz-date;x-amz-target, Signature=fakesig",
            )
            .header("x-amz-date", "20260101T000000Z")
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 400, "missing {member}");
        let parsed: serde_json::Value = resp.json().await.unwrap();
        assert!(
            parsed["__type"]
                .as_str()
                .unwrap()
                .ends_with("#ValidationException"),
            "missing {member}: {parsed}"
        );
        let message = parsed["message"].as_str().unwrap();
        assert!(
            message.starts_with(&format!("missing field `{member}` at line 1 column ")),
            "missing {member}: {message}"
        );
    }
}

/// The newer layer measures a table name in characters (captured eu-west-2
/// and us-east-1, 2026-11-05).
#[tokio::test]
async fn table_name_length_counts_characters() {
    let url = start_server().await;
    let accented = "é".repeat(200);
    let emoji = "😀".repeat(128);
    for (operation, name, expected) in [
        ("PutItem", "éé", one(&short("éé"))),
        ("PutItem", accented.as_str(), one(&pattern(&accented))),
        ("PutItem", "😀😀", one(&short("😀😀"))),
        ("PutItem", emoji.as_str(), one(&pattern(&emoji))),
        ("UpdateItem", "😀😀", two(&pattern("😀😀"), &short("😀😀"))),
        ("GetItem", "😀😀", one(&short("😀😀"))),
        ("DeleteItem", "😀😀", one(&short("😀😀"))),
        ("Query", "😀😀", one(&short("😀😀"))),
        ("Scan", "😀😀", one(&short("😀😀"))),
        ("GetItem", emoji.as_str(), one(&pattern(&emoji))),
        ("Scan", emoji.as_str(), one(&pattern(&emoji))),
    ] {
        let body = match operation {
            "PutItem" => serde_json::json!({"TableName": name, "Item": key()}),
            "Scan" => serde_json::json!({"TableName": name}),
            "Query" => serde_json::json!({
                "TableName": name,
                "KeyConditionExpression": "pk = :p",
                "ExpressionAttributeValues": {":p": {"S": "a"}},
            }),
            _ => serde_json::json!({"TableName": name, "Key": key()}),
        };
        assert_eq!(
            refused(&url, operation, body).await,
            expected,
            "{operation} {name}"
        );
    }
}
