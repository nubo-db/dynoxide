//! Request-level validation on BatchGetItem, BatchWriteItem, TransactGetItems
//! and TransactWriteItems: the member constraints DynamoDB checks before it
//! reads a request any further, the messages it gives, and which one wins when
//! a request breaks more than one.
//!
//! Captured on 2026-10-09 in us-east-1 and eu-west-2, with the shapes the two
//! disagreed on sent to ten more regions. eu-west-2 and eu-north-1 have moved
//! to a newer validation layer for transactions; the other ten agree with each
//! other, and these tests follow them.

use dynoxide::{Database, DynoxideError};

fn setup() -> Database {
    let db = Database::memory().unwrap();
    db.create_table(request(serde_json::json!({
        "TableName": "TestTable",
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "BillingMode": "PAY_PER_REQUEST"
    })))
    .unwrap();
    db
}

fn request<T: serde::de::DeserializeOwned>(value: serde_json::Value) -> T {
    serde_json::from_value(value).unwrap()
}

/// The message of a top-level ValidationException. A cancellation, or any
/// other error, fails the test.
fn validation_message(err: DynoxideError) -> String {
    match err {
        DynoxideError::ValidationException(msg) => msg,
        other => panic!("expected a top-level ValidationException, got {other:?}"),
    }
}

fn stored(db: &Database, pk: &str) -> bool {
    db.get_item(request(serde_json::json!({
        "TableName": "TestTable",
        "Key": {"pk": {"S": pk}},
        "ConsistentRead": true
    })))
    .unwrap()
    .item
    .is_some()
}

fn one(msg: &str) -> String {
    format!("1 validation error detected: {msg}")
}

const RCC: &str = "Value 'BOGUS' at 'returnConsumedCapacity' failed to satisfy constraint: \
                   Member must satisfy enum value set: [INDEXES, TOTAL, NONE]";
const RICM: &str = "Value 'BOGUS2' at 'returnItemCollectionMetrics' failed to satisfy constraint: \
                    Member must satisfy enum value set: [SIZE, NONE]";
const MAP_KEYS: &str = "Map keys must satisfy constraint: [Member must have length less than or \
                        equal to 255, Member must have length greater than or equal to 3, Member \
                        must satisfy regular expression pattern: [a-zA-Z0-9_.-]+]";
const MAP_VALUE: &str = "Map value must satisfy constraint: [Member must have length less than or \
                         equal to 25, Member must have length greater than or equal to 1]";
const NOT_MATCHING_SCHEMA: &str = "The provided key element does not match the schema";

/// How the newer AWS SDKs for Java print an `AttributeValue` holding a string,
/// which is how DynamoDB echoes a BatchWriteItem request back.
fn java_string_value(s: &str) -> String {
    format!(
        "AttributeValue(s={s}, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, \
         nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, \
         nULL=null, bOOL=null)"
    )
}

/// An item in TransactItems that names no action.
const EMPTY_ACTION: &str = "Value '' at 'transactItems' failed to satisfy constraint: \
                            TransactWriteRequest should contain Delete or Put or Update or \
                            ConditionCheck";

/// An overlong token as TransactWriteItems words it when no required member is
/// null: the older form, with no value echoed, reported with nothing else
/// beside it but the length of `TransactItems` (captured eu-west-2 and
/// us-east-1, 2026-11-11).
const TOKEN_TOO_LONG: &str = "Value at 'ClientRequestToken' failed to satisfy constraint: Member \
                              must have length less than or equal to 36";

fn bad_name_message(name: &str, path: &str) -> String {
    format!(
        "Value '{name}' at '{path}' failed to satisfy constraint: Member must satisfy regular \
         expression pattern: [a-zA-Z0-9_.-]+"
    )
}

#[test]
fn batch_write_and_transact_write_reject_an_unknown_return_item_collection_metrics() {
    let db = setup();

    let err = db
        .batch_write_item(request(serde_json::json!({
            "RequestItems": {"TestTable": [{"PutRequest": {"Item": {"pk": {"S": "bw"}}}}]},
            "ReturnItemCollectionMetrics": "BOGUS2"
        })))
        .unwrap_err();
    assert_eq!(validation_message(err), one(RICM), "BatchWriteItem");
    assert!(!stored(&db, "bw"), "BatchWriteItem wrote despite refusing");

    let err = db
        .transact_write_items(request(serde_json::json!({
            "TransactItems": [{"Put": {"TableName": "TestTable", "Item": {"pk": {"S": "tw"}}}}],
            "ReturnItemCollectionMetrics": "BOGUS2"
        })))
        .unwrap_err();
    assert_eq!(validation_message(err), one(RICM), "TransactWriteItems");
    assert!(
        !stored(&db, "tw"),
        "TransactWriteItems wrote despite refusing"
    );

    for mode in ["SIZE", "NONE"] {
        db.batch_write_item(request(serde_json::json!({
            "RequestItems": {"TestTable": [{"PutRequest": {"Item": {"pk": {"S": "ok"}}}}]},
            "ReturnItemCollectionMetrics": mode
        })))
        .unwrap_or_else(|e| panic!("BatchWriteItem {mode}: {e:?}"));
        db.transact_write_items(request(serde_json::json!({
            "TransactItems": [{"Put": {"TableName": "TestTable", "Item": {"pk": {"S": "ok"}}}}],
            "ReturnItemCollectionMetrics": mode
        })))
        .unwrap_or_else(|e| panic!("TransactWriteItems {mode}: {e:?}"));
    }
}

#[test]
fn request_member_errors_are_reported_together() {
    let db = setup();
    let token = "x".repeat(37);

    let err = db
        .batch_write_item(request(serde_json::json!({
            "RequestItems": {"TestTable": [{"PutRequest": {"Item": {"pk": {"S": "a"}}}}]},
            "ReturnConsumedCapacity": "BOGUS",
            "ReturnItemCollectionMetrics": "BOGUS2"
        })))
        .unwrap_err();
    assert_eq!(
        validation_message(err),
        format!("2 validation errors detected: {RCC}; {RICM}"),
        "BatchWriteItem, both enums"
    );

    let err = db
        .batch_write_item(request(serde_json::json!({
            "RequestItems": {"TestTable": []},
            "ReturnConsumedCapacity": "BOGUS",
            "ReturnItemCollectionMetrics": "BOGUS2"
        })))
        .unwrap_err();
    assert_eq!(
        validation_message(err),
        format!(
            "3 validation errors detected: Value '{{TestTable=[]}}' at 'requestItems' failed to \
             satisfy constraint: {MAP_VALUE}; {RCC}; {RICM}"
        ),
        "BatchWriteItem, an empty list and both enums"
    );

    let err = db
        .batch_get_item(request(serde_json::json!({
            "RequestItems": {"bad!name": {"Keys": [{"pk": {"S": "a"}}]}},
            "ReturnConsumedCapacity": "BOGUS"
        })))
        .unwrap_err();
    assert_eq!(
        validation_message(err),
        format!(
            "2 validation errors detected: Value '{{\"bad!name\":{{\"Keys\":[{{\"pk\":{{\"S\":\"a\"}}}}]}}}}' \
             at 'requestItems' failed to satisfy constraint: {MAP_KEYS}; {RCC}"
        ),
        "BatchGetItem, a malformed table name and the capacity mode"
    );

    let transact = |extra: serde_json::Value| {
        let mut value = serde_json::json!({
            "TransactItems": [{"Put": {"TableName": "TestTable", "Item": {"pk": {"S": "t"}}}}]
        });
        for (k, v) in extra.as_object().unwrap() {
            value[k] = v.clone();
        }
        validation_message(db.transact_write_items(request(value)).unwrap_err())
    };
    assert_eq!(
        transact(serde_json::json!({
            "ReturnConsumedCapacity": "BOGUS",
            "ReturnItemCollectionMetrics": "BOGUS2"
        })),
        format!("2 validation errors detected: {RCC}; {RICM}"),
        "TransactWriteItems, both enums"
    );
    // The token's length is a stage of its own: the enums are not listed
    // beside it (captured eu-west-2 and us-east-1, 2026-11-11).
    assert_eq!(
        transact(serde_json::json!({
            "ClientRequestToken": token,
            "ReturnItemCollectionMetrics": "BOGUS2"
        })),
        one(TOKEN_TOO_LONG),
        "TransactWriteItems, the token and ReturnItemCollectionMetrics"
    );
    assert_eq!(
        transact(serde_json::json!({
            "ClientRequestToken": token,
            "ReturnConsumedCapacity": "BOGUS",
            "ReturnItemCollectionMetrics": "BOGUS2"
        })),
        one(TOKEN_TOO_LONG),
        "TransactWriteItems, the token and both enums"
    );
    assert!(!stored(&db, "t"));
}

/// BatchWriteItem and TransactGetItems check a table name after the request's
/// enums. TransactWriteItems checks it after the token and lists it with a bad
/// enum.
#[test]
fn a_table_name_follows_the_enums_except_in_a_transaction_write_which_lists_them_together() {
    let db = setup();
    let token = "x".repeat(37);

    let err = db
        .batch_write_item(request(serde_json::json!({
            "RequestItems": {"bad!name": [{"PutRequest": {"Item": {"pk": {"S": "x"}}}}]},
            "ReturnItemCollectionMetrics": "BOGUS2"
        })))
        .unwrap_err();
    assert_eq!(validation_message(err), one(RICM), "BatchWriteItem");

    // A refused table name is listed with a bad enum, the name first: "TW bad
    // name put + RCC" and "TW bad name + RICM" in the 2026-10-09
    // valid-txbatch capture, where eu-west-2 and eu-north-1 listed both and
    // the ten other regions the enum alone, and the same requests in
    // eu-west-2 and us-east-1 on 2026-11-11, which both listed both.
    let bad_put =
        serde_json::json!([{"Put": {"TableName": "bad!name", "Item": {"pk": {"S": "a"}}}}]);
    let bad_name = bad_name_message("bad!name", "transactItems.1.member.put.tableName");
    for (extra, expected) in [
        (
            serde_json::json!({"ReturnConsumedCapacity": "BOGUS"}),
            format!("2 validation errors detected: {bad_name}; {RCC}"),
        ),
        (
            serde_json::json!({"ReturnItemCollectionMetrics": "BOGUS2"}),
            format!("2 validation errors detected: {bad_name}; {RICM}"),
        ),
        (
            serde_json::json!({"ClientRequestToken": token}),
            one(TOKEN_TOO_LONG),
        ),
    ] {
        let mut value = serde_json::json!({"TransactItems": bad_put});
        for (k, v) in extra.as_object().unwrap() {
            value[k] = v.clone();
        }
        let err = db.transact_write_items(request(value)).unwrap_err();
        assert_eq!(
            validation_message(err),
            expected,
            "TransactWriteItems with {extra}"
        );
    }

    let err = db
        .transact_get_items(request(serde_json::json!({
            "TransactItems": [{"Get": {"TableName": "bad!name", "Key": {"pk": {"S": "a"}}}}],
            "ReturnConsumedCapacity": "BOGUS"
        })))
        .unwrap_err();
    assert_eq!(validation_message(err), one(RCC), "TransactGetItems");
}

#[test]
fn batch_write_reports_dynamodbs_messages_for_its_request_items() {
    let db = setup();
    let write = |items: serde_json::Value| {
        validation_message(
            db.batch_write_item(request(serde_json::json!({"RequestItems": items})))
                .unwrap_err(),
        )
    };

    assert_eq!(
        write(serde_json::json!({"TestTable": []})),
        one(&format!(
            "Value '{{TestTable=[]}}' at 'requestItems' failed to satisfy constraint: {MAP_VALUE}"
        )),
        "an empty request list"
    );
    assert_eq!(
        write(serde_json::json!({"bad!name": []})),
        one(&format!(
            "Value '{{bad!name=[]}}' at 'requestItems' failed to satisfy constraint: {MAP_VALUE}"
        )),
        "an empty request list wins over a malformed table name"
    );

    assert_eq!(
        write(serde_json::json!({"bad!name": [{"PutRequest": {"Item": {"pk": {"S": "x"}}}}]})),
        one(&format!(
            "Value '{{bad!name=[WriteRequest(putRequest=PutRequest(item={{pk={}}}, \
             workloadProfileName=null), deleteRequest=null)]}}' at 'requestItems' failed to \
             satisfy constraint: {MAP_KEYS}",
            java_string_value("x")
        )),
        "a malformed table name on a put"
    );
    assert_eq!(
        write(serde_json::json!({"ab": [{"DeleteRequest": {"Key": {"pk": {"S": "x"}}}}]})),
        one(&format!(
            "Value '{{ab=[WriteRequest(putRequest=null, deleteRequest=DeleteRequest(key={{pk={}}}, \
             workloadProfileName=null))]}}' at 'requestItems' failed to satisfy constraint: \
             {MAP_KEYS}",
            java_string_value("x")
        )),
        "a table name too short, on a delete"
    );

    assert_eq!(
        write(serde_json::json!({"TestTable": [{"PutRequest": {"Item": {"other": {"S": "x"}}}}]})),
        NOT_MATCHING_SCHEMA,
        "a put without the partition key"
    );
    assert_eq!(
        write(
            serde_json::json!({"TestTable": [{"DeleteRequest": {"Key": {"other": {"S": "x"}}}}]})
        ),
        NOT_MATCHING_SCHEMA,
        "a delete without the partition key"
    );

    // A too deep item is refused without the envelope PutItem gives it.
    let mut deep = serde_json::json!({"S": "leaf"});
    for _ in 0..32 {
        deep = serde_json::json!({"M": {"n": deep}});
    }
    assert_eq!(
        write(
            serde_json::json!({"TestTable": [{"PutRequest": {"Item": {"pk": {"S": "deep"}, "data": deep}}}]})
        ),
        "Nesting Levels have exceeded supported limits: Attributes in the item have nested \
         levels beyond supported limit",
        "an item nested 32 levels"
    );
    assert!(!stored(&db, "deep"));
}

#[test]
fn batch_get_reports_dynamodbs_messages_for_its_request_items() {
    let db = setup();
    let get = |items: serde_json::Value| {
        validation_message(
            db.batch_get_item(request(serde_json::json!({"RequestItems": items})))
                .unwrap_err(),
        )
    };

    assert_eq!(
        get(serde_json::json!({"TestTable": {"Keys": []}})),
        one(
            "Value at 'RequestItems.TestTable.member.Keys' failed to satisfy constraint: Member \
             must have length greater than or equal to 1"
        ),
        "an empty Keys list"
    );
    assert_eq!(
        get(serde_json::json!({"bad!name": {"Keys": []}})),
        one(
            "Value at 'RequestItems.bad!name.member.Keys' failed to satisfy constraint: Member \
             must have length greater than or equal to 1"
        ),
        "an empty Keys list wins over a malformed table name"
    );
    assert_eq!(
        get(serde_json::json!({"bad!name": {"Keys": [{"pk": {"S": "a"}}]}})),
        one(&format!(
            "Value '{{\"bad!name\":{{\"Keys\":[{{\"pk\":{{\"S\":\"a\"}}}}]}}}}' at 'requestItems' \
             failed to satisfy constraint: {MAP_KEYS}"
        )),
        "a malformed table name"
    );
    assert_eq!(
        get(serde_json::json!({"ab": {"Keys": [{"pk": {"S": "a"}}]}})),
        one(&format!(
            "Value '{{\"ab\":{{\"Keys\":[{{\"pk\":{{\"S\":\"a\"}}}}]}}}}' at 'requestItems' failed \
             to satisfy constraint: {MAP_KEYS}"
        )),
        "a table name too short"
    );
    // The whole map is echoed, its keys in order, each table's members by name.
    assert_eq!(
        get(serde_json::json!({
            "bad!name": {"Keys": [{"pk": {"S": "b"}}], "ProjectionExpression": "pk", "ConsistentRead": true},
            "TestTable": {"Keys": [{"pk": {"S": "a"}}]}
        })),
        one(&format!(
            "Value '{{\"TestTable\":{{\"Keys\":[{{\"pk\":{{\"S\":\"a\"}}}}]}},\"bad!name\":\
             {{\"ConsistentRead\":true,\"Keys\":[{{\"pk\":{{\"S\":\"b\"}}}}],\"ProjectionExpression\":\
             \"pk\"}}}}' at 'requestItems' failed to satisfy constraint: {MAP_KEYS}"
        )),
        "two tables, one malformed"
    );
}

#[test]
fn a_malformed_table_name_in_a_transaction_is_a_top_level_error_naming_the_action() {
    let db = setup();
    let write = |items: serde_json::Value| {
        validation_message(
            db.transact_write_items(request(serde_json::json!({"TransactItems": items})))
                .unwrap_err(),
        )
    };

    assert_eq!(
        write(serde_json::json!([{"Put": {"TableName": "bad!name", "Item": {"pk": {"S": "a"}}}}])),
        one(&bad_name_message(
            "bad!name",
            "transactItems.1.member.put.tableName"
        )),
        "Put"
    );
    assert_eq!(
        write(serde_json::json!([
            {"Put": {"TableName": "TestTable", "Item": {"pk": {"S": "first"}}}},
            {"Update": {"TableName": "bad!name", "Key": {"pk": {"S": "a"}},
                "UpdateExpression": "SET a = :a", "ExpressionAttributeValues": {":a": {"S": "x"}}}}
        ])),
        one(&bad_name_message(
            "bad!name",
            "transactItems.2.member.update.tableName"
        )),
        "Update, second"
    );
    assert!(!stored(&db, "first"), "the valid first action was written");
    assert_eq!(
        write(
            serde_json::json!([{"Delete": {"TableName": "bad!name", "Key": {"pk": {"S": "a"}}}}])
        ),
        one(&bad_name_message(
            "bad!name",
            "transactItems.1.member.delete.tableName"
        )),
        "Delete"
    );
    assert_eq!(
        write(
            serde_json::json!([{"ConditionCheck": {"TableName": "bad!name", "Key": {"pk": {"S": "a"}},
            "ConditionExpression": "attribute_exists(pk)"}}])
        ),
        one(&bad_name_message(
            "bad!name",
            "transactItems.1.member.conditionCheck.tableName"
        )),
        "ConditionCheck"
    );

    assert_eq!(
        write(serde_json::json!([{"Put": {"TableName": "ab", "Item": {"pk": {"S": "a"}}}}])),
        one(
            "Value 'ab' at 'transactItems.1.member.put.tableName' failed to satisfy constraint: \
             Member must have length greater than or equal to 3"
        ),
        "a name too short"
    );
    let long = "a".repeat(256);
    assert_eq!(
        write(serde_json::json!([{"Delete": {"TableName": long, "Key": {"pk": {"S": "a"}}}}])),
        one(&format!(
            "Value '{long}' at 'transactItems.1.member.delete.tableName' failed to satisfy \
             constraint: Member must have length less than or equal to 255"
        )),
        "a name too long"
    );
    assert_eq!(
        write(
            serde_json::json!([{"ConditionCheck": {"TableName": "a!", "Key": {"pk": {"S": "a"}},
            "ConditionExpression": "attribute_exists(pk)"}}])
        ),
        format!(
            "2 validation errors detected: {}; Value 'a!' at \
             'transactItems.1.member.conditionCheck.tableName' failed to satisfy constraint: \
             Member must have length greater than or equal to 3",
            bad_name_message("a!", "transactItems.1.member.conditionCheck.tableName")
        ),
        "a name too short and malformed, the pattern first as eu-west-2 lists them"
    );
    assert_eq!(
        write(serde_json::json!([
            {"Put": {"TableName": "bad!name", "Item": {"pk": {"S": "a"}}}},
            {"Delete": {"TableName": "bad!two", "Key": {"pk": {"S": "a"}}}}
        ])),
        format!(
            "2 validation errors detected: {}; {}",
            bad_name_message("bad!name", "transactItems.1.member.put.tableName"),
            bad_name_message("bad!two", "transactItems.2.member.delete.tableName")
        ),
        "two malformed names"
    );

    let get = |items: serde_json::Value| {
        validation_message(
            db.transact_get_items(request(serde_json::json!({"TransactItems": items})))
                .unwrap_err(),
        )
    };
    assert_eq!(
        get(serde_json::json!([{"Get": {"TableName": "bad!name", "Key": {"pk": {"S": "a"}}}}])),
        one(&bad_name_message(
            "bad!name",
            "transactItems.1.member.get.tableName"
        )),
        "TransactGetItems"
    );
    assert_eq!(
        get(serde_json::json!([
            {"Get": {"TableName": "TestTable", "Key": {"pk": {"S": "a"}}}},
            {"Get": {"TableName": "bad!name", "Key": {"pk": {"S": "a"}}}}
        ])),
        one(&bad_name_message(
            "bad!name",
            "transactItems.2.member.get.tableName"
        )),
        "TransactGetItems, second"
    );
}

#[test]
fn a_transaction_item_must_carry_exactly_one_action() {
    let db = setup();
    let write = |value: serde_json::Value| {
        validation_message(db.transact_write_items(request(value)).unwrap_err())
    };
    // An item naming no action is a member error in the newer form (captured
    // eu-west-2 and us-east-1, 2026-11-11).
    let no_action = one(EMPTY_ACTION);

    assert_eq!(
        write(serde_json::json!({"TransactItems": [{}]})),
        no_action,
        "an action with no operation"
    );
    assert_eq!(
        write(serde_json::json!({"TransactItems": [
            {},
            {"Put": {"TableName": "bad!name", "Item": {"pk": {"S": "a"}}}}
        ]})),
        no_action,
        "an action with no operation wins over a malformed table name"
    );
    // Listed together with the request's enums, the empty action first
    // (captured eu-west-2 and us-east-1, 2026-11-11, third round row 3).
    assert_eq!(
        write(serde_json::json!({"TransactItems": [{}], "ReturnItemCollectionMetrics": "BOGUS2"})),
        format!("2 validation errors detected: {EMPTY_ACTION}; {RICM}"),
        "listed with the request enums"
    );

    assert_eq!(
        write(serde_json::json!({"TransactItems": [{
            "Put": {"TableName": "TestTable", "Item": {"pk": {"S": "both-put"}}},
            "Delete": {"TableName": "TestTable", "Key": {"pk": {"S": "both-del"}}}
        }]})),
        "TransactItems can only contain one of Check, Put, Update or Delete",
        "an action with two operations"
    );
    assert!(
        !stored(&db, "both-put"),
        "the put of a two-operation action ran"
    );
}

/// Start a server on a free port and return its URL.
#[cfg(feature = "http-server")]
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

/// Send one request over HTTP and return the message of the
/// ValidationException it must be refused with.
#[cfg(feature = "http-server")]
async fn refused_over_http(url: &str, operation: &str, body: serde_json::Value) -> String {
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
    let body: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert!(
        body["__type"]
            .as_str()
            .unwrap()
            .ends_with("#ValidationException"),
        "unexpected __type for {operation}: {body}"
    );
    body["message"].as_str().unwrap().to_string()
}

/// A TransactGetItems item without its `Get` is a member constraint: refused
/// with DynamoDB's "must not be null" message, together with an unknown
/// `ReturnConsumedCapacity`, and ahead of any table name (captured in five
/// regions, 2026-10-09). Only a request body can leave the `Get` out, so this
/// goes over HTTP.
#[cfg(feature = "http-server")]
#[tokio::test]
async fn a_transact_get_item_without_its_get_is_refused_with_dynamodbs_message() {
    let url = start_server().await;
    let send = |body: serde_json::Value| refused_over_http(&url, "TransactGetItems", body);
    let missing = |position: usize| {
        format!(
            "Value null at 'transactItems.{position}.member.get' failed to satisfy constraint: \
             Member must not be null"
        )
    };

    assert_eq!(
        send(serde_json::json!({"TransactItems": [{}]})).await,
        one(&missing(1)),
        "an item without its Get"
    );
    assert_eq!(
        send(serde_json::json!({"TransactItems": [{}], "ReturnConsumedCapacity": "BOGUS"})).await,
        format!("2 validation errors detected: {}; {RCC}", missing(1)),
        "together with an unknown capacity mode"
    );
    assert_eq!(
        send(serde_json::json!({"TransactItems": [
            {},
            {"Get": {"TableName": "bad!name", "Key": {"pk": {"S": "a"}}}}
        ]}))
        .await,
        one(&missing(1)),
        "ahead of a malformed table name"
    );
    assert_eq!(
        send(serde_json::json!({"TransactItems": [
            {"Get": {"TableName": "TestTable", "Key": {"pk": {"S": "a"}}}},
            {"Get": null}
        ]}))
        .await,
        one(&missing(2)),
        "a null Get, second"
    );
}

/// `ReturnValuesOnConditionCheckFailure` on a transaction action is a member of
/// the request: the first action's that is wrong, in request order, ahead of a
/// table name. The token's length comes first, reported alone, then the two
/// enums when either is wrong, then an empty action (captured eu-west-2 and
/// us-east-1, 2026-11-11).
#[test]
fn a_transaction_actions_return_values_on_condition_check_failure_is_a_member_error() {
    let db = setup();
    let write = |value: serde_json::Value| {
        validation_message(db.transact_write_items(request(value)).unwrap_err())
    };
    let rvoccf = |path: &str, value: &str| {
        format!(
            "Value '{value}' at 'transactItems.{path}.returnValuesOnConditionCheckFailure' \
             failed to satisfy constraint: Member must satisfy enum value set: [ALL_OLD, NONE]"
        )
    };
    let put = serde_json::json!({"Put": {"TableName": "TestTable", "Item": {"pk": {"S": "a"}},
        "ReturnValuesOnConditionCheckFailure": "BOGUS"}});

    assert_eq!(
        write(serde_json::json!({"TransactItems": [put]})),
        one(&rvoccf("1.member.put", "BOGUS")),
        "Put"
    );
    assert_eq!(
        write(
            serde_json::json!({"TransactItems": [{"Update": {"TableName": "TestTable",
            "Key": {"pk": {"S": "a"}}, "UpdateExpression": "SET a = :a",
            "ExpressionAttributeValues": {":a": {"S": "x"}},
            "ReturnValuesOnConditionCheckFailure": "BOGUS"}}]})
        ),
        one(&rvoccf("1.member.update", "BOGUS")),
        "Update"
    );
    assert_eq!(
        write(
            serde_json::json!({"TransactItems": [{"Delete": {"TableName": "TestTable",
            "Key": {"pk": {"S": "a"}}, "ReturnValuesOnConditionCheckFailure": "BOGUS"}}]})
        ),
        one(&rvoccf("1.member.delete", "BOGUS")),
        "Delete"
    );
    assert_eq!(
        write(
            serde_json::json!({"TransactItems": [{"ConditionCheck": {"TableName": "TestTable",
            "Key": {"pk": {"S": "a"}}, "ConditionExpression": "attribute_exists(pk)",
            "ReturnValuesOnConditionCheckFailure": "BOGUS"}}]})
        ),
        one(&rvoccf("1.member.conditionCheck", "BOGUS")),
        "ConditionCheck"
    );
    assert_eq!(
        write(serde_json::json!({"TransactItems": [put], "ReturnConsumedCapacity": "BOGUS"})),
        one(RCC),
        "behind the capacity mode (third round row 5)"
    );
    assert_eq!(
        write(
            serde_json::json!({"TransactItems": [put], "ReturnConsumedCapacity": "BOGUS",
            "ReturnItemCollectionMetrics": "BOGUS2"})
        ),
        format!("2 validation errors detected: {RCC}; {RICM}"),
        "behind both enums"
    );
    let token = "x".repeat(37);
    assert_eq!(
        write(
            serde_json::json!({"TransactItems": [put], "ClientRequestToken": token,
            "ReturnConsumedCapacity": "BOGUS", "ReturnItemCollectionMetrics": "BOGUS2"})
        ),
        one(TOKEN_TOO_LONG),
        "behind the token, which is reported alone"
    );
    // Behind a malformed table name: "TW Put RVOCCF + bad name" in eu-west-2
    // in the 2026-10-09 valid-order capture (the other regions then gave the
    // mode), and a table name ahead of an item naming two actions ahead of a
    // mode in all four regions on 2026-11-11 (seventh round rows 11 and 12).
    assert_eq!(
        write(serde_json::json!({"TransactItems": [
            {"Put": {"TableName": "bad!name", "Item": {"pk": {"S": "a"}},
                "ReturnValuesOnConditionCheckFailure": "BOGUS"}}
        ]})),
        one(&bad_name_message(
            "bad!name",
            "transactItems.1.member.put.tableName"
        )),
        "behind a malformed table name"
    );
    // An empty action wins whichever item comes first (third round rows 1
    // and 2).
    assert_eq!(
        write(serde_json::json!({"TransactItems": [{}, put]})),
        one(EMPTY_ACTION),
        "behind an empty action written first"
    );
    assert_eq!(
        write(serde_json::json!({"TransactItems": [put, {}]})),
        one(EMPTY_ACTION),
        "behind an empty action written second"
    );
    assert_eq!(
        write(
            serde_json::json!({"TransactItems": [put, {"Delete": {"TableName": "TestTable",
            "Key": {"pk": {"S": "b"}}, "ReturnValuesOnConditionCheckFailure": "BAD"}}]})
        ),
        one(&rvoccf("1.member.put", "BOGUS")),
        "only the first action's (third round row 7)"
    );
}
