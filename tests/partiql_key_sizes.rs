//! PartiQL statements whose key values are larger than a key can be: a
//! partition key over 2,048 bytes and a sort key over 1,024. Every expectation
//! below was captured against real DynamoDB in eu-west-2 and us-east-1 on
//! 2026-10-10, and the two regions agree on each one.
//!
//! A statement that reads or writes by key is refused once its key is pinned:
//! on the table, on either kind of index, and for a sort key under any key
//! condition, and beside an OR that leaves the partition key in every
//! alternative. A statement that names several partitions (an OR between
//! them, or an IN with several values) runs as a scan. In a batch the refusal
//! echoes the table and is charged nothing.

use dynoxide::Database;
use dynoxide::actions::batch_execute_statement::BatchExecuteStatementRequest;
use dynoxide::actions::create_table::CreateTableRequest;
use dynoxide::actions::execute_statement::ExecuteStatementRequest;
use dynoxide::actions::execute_transaction::ExecuteTransactionRequest;
use dynoxide::actions::put_item::PutItemRequest;
use serde_json::{Value, json};

const HASH: &str = "One or more parameter values were invalid: \
                    Size of hashkey has exceeded the maximum size limit of2048 bytes";
const RANGE: &str = "One or more parameter values were invalid: \
                     Aggregated size of all range keys has exceeded the size limit of 1024 bytes";
const MANDATORY: &str = "Where clause does not contain a mandatory equality on all key attributes";

/// Hash and range table with an LSI and a GSI, each with a sort key.
const TK: &str = "KeySizes";
/// Hash-only table with a KEYS_ONLY GSI on a partition key alone.
const TH: &str = "HashOnly";

fn k(n: usize) -> String {
    "k".repeat(n)
}

fn s(n: usize) -> String {
    "s".repeat(n)
}

fn setup() -> Database {
    let db = Database::memory().unwrap();
    let definitions: Vec<Value> = ["pk", "sk", "lsk", "gpk", "gsk"]
        .iter()
        .map(|n| json!({"AttributeName": n, "AttributeType": "S"}))
        .collect();
    db.create_table(
        serde_json::from_value::<CreateTableRequest>(json!({
            "TableName": TK,
            "BillingMode": "PAY_PER_REQUEST",
            "AttributeDefinitions": definitions,
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"}
            ],
            "LocalSecondaryIndexes": [{
                "IndexName": "lix",
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "lsk", "KeyType": "RANGE"}
                ],
                "Projection": {"ProjectionType": "ALL"}
            }],
            "GlobalSecondaryIndexes": [{
                "IndexName": "gix",
                "KeySchema": [
                    {"AttributeName": "gpk", "KeyType": "HASH"},
                    {"AttributeName": "gsk", "KeyType": "RANGE"}
                ],
                "Projection": {"ProjectionType": "ALL"}
            }]
        }))
        .unwrap(),
    )
    .unwrap();
    db.create_table(
        serde_json::from_value::<CreateTableRequest>(json!({
            "TableName": TH,
            "BillingMode": "PAY_PER_REQUEST",
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "gpk", "AttributeType": "S"}
            ],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "GlobalSecondaryIndexes": [{
                "IndexName": "gix",
                "KeySchema": [{"AttributeName": "gpk", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "KEYS_ONLY"}
            }]
        }))
        .unwrap(),
    )
    .unwrap();
    for pk in ["a", "b"] {
        put(
            &db,
            json!({"TableName": TK, "Item": {
                "pk": {"S": pk}, "sk": {"S": pk}, "lsk": {"S": "l"},
                "gpk": {"S": "g"}, "gsk": {"S": "h"}, "x": {"N": "0"}
            }}),
        );
        put(
            &db,
            json!({"TableName": TH, "Item": {"pk": {"S": pk}, "gpk": {"S": "g1"}, "x": {"N": "0"}}}),
        );
    }
    db
}

fn put(db: &Database, request: Value) {
    db.put_item(serde_json::from_value::<PutItemRequest>(request).unwrap())
        .unwrap();
}

/// What an ExecuteStatement answers: its items, or the error's wire body.
fn single(db: &Database, request: Value) -> Result<Vec<Value>, Value> {
    let request: ExecuteStatementRequest = serde_json::from_value(request).unwrap();
    match db.execute_statement(request) {
        Ok(response) => Ok(response
            .items
            .unwrap_or_default()
            .into_iter()
            .map(|item| serde_json::to_value(item).unwrap())
            .collect()),
        Err(e) => Err(serde_json::from_str(&e.to_json()).unwrap()),
    }
}

fn statement(text: String) -> Value {
    json!({"Statement": text})
}

fn refused(message: &str) -> Value {
    json!({"__type": "com.amazon.coral.validate#ValidationException", "message": message})
}

fn batch(db: &Database, request: Value) -> Value {
    let request: BatchExecuteStatementRequest = serde_json::from_value(request).unwrap();
    serde_json::to_value(db.batch_execute_statement(request).unwrap()).unwrap()
}

fn transaction(db: &Database, request: Value) -> Value {
    let request: ExecuteTransactionRequest = serde_json::from_value(request).unwrap();
    let error = db
        .execute_transaction(request)
        .expect_err("the transaction is cancelled");
    let mut body: Value = serde_json::from_str(&error.to_json()).unwrap();
    body.as_object_mut().unwrap().remove("Message");
    body
}

fn item_error(message: &str, table: &str) -> Value {
    json!({"Error": {"Code": "ValidationError", "Message": message}, "TableName": table})
}

#[test]
fn a_partition_key_over_2048_bytes_is_refused_on_every_key_read_and_write() {
    let db = setup();
    let big = k(2049);
    let cases = [
        // 11d: an index's partition key.
        format!(r#"SELECT * FROM "{TH}"."gix" WHERE gpk = '{big}'"#),
        // 11h and 11i: the key of an UPDATE and a DELETE.
        format!(r#"UPDATE "{TH}" SET x = 1 WHERE pk = '{big}'"#),
        format!(r#"DELETE FROM "{TH}" WHERE pk = '{big}'"#),
        // k8: a partition key read on a table that also has a sort key.
        format!(r#"SELECT * FROM "{TK}" WHERE pk = '{big}'"#),
        // k9: the partition key is reported ahead of a sort key also too big.
        format!(
            r#"SELECT * FROM "{TK}" WHERE pk = '{big}' AND sk = '{}'"#,
            s(1025)
        ),
        // k14: a filter beside the key doesn't make it a scan.
        format!(r#"SELECT * FROM "{TK}" WHERE pk = '{big}' AND x = 1"#),
        // k16: on a GSI, ahead of its sort key.
        format!(
            r#"SELECT * FROM "{TK}"."gix" WHERE gpk = '{big}' AND gsk = '{}'"#,
            s(1025)
        ),
    ];
    for text in cases {
        assert_eq!(
            single(&db, statement(text.clone())),
            Err(refused(HASH)),
            "{}",
            &text[..60]
        );
    }
    // k13: through a parameter.
    assert_eq!(
        single(
            &db,
            json!({
                "Statement": format!(r#"UPDATE "{TK}" SET x = 1 WHERE pk = ? AND sk = 'a'"#),
                "Parameters": [{"S": big}]
            })
        ),
        Err(refused(HASH))
    );
}

#[test]
fn a_sort_key_over_1024_bytes_is_refused_under_any_key_condition() {
    let db = setup();
    let big = s(1025);
    let cases = [
        // 11f, k3, k4, k5 and k18: the table's sort key.
        format!(r#"SELECT * FROM "{TK}" WHERE pk = 'a' AND sk = '{big}'"#),
        format!(r#"SELECT * FROM "{TK}" WHERE pk = 'a' AND sk > '{big}'"#),
        format!(r#"SELECT * FROM "{TK}" WHERE pk = 'a' AND sk BETWEEN 'a' AND '{big}'"#),
        format!(r#"SELECT * FROM "{TK}" WHERE pk = 'a' AND begins_with(sk, '{big}')"#),
        format!(r#"SELECT * FROM "{TK}" WHERE pk = 'a' AND sk IN ['{big}']"#),
        // 11g and k7: an LSI's.
        format!(r#"SELECT * FROM "{TK}"."lix" WHERE pk = 'a' AND lsk = '{big}'"#),
        format!(r#"SELECT * FROM "{TK}"."lix" WHERE pk = 'a' AND lsk > '{big}'"#),
        // k6: a GSI's.
        format!(r#"SELECT * FROM "{TK}"."gix" WHERE gpk = 'g' AND gsk = '{big}'"#),
        // k17: a partition key at its limit doesn't hide it.
        format!(
            r#"SELECT * FROM "{TK}" WHERE pk = '{}' AND sk = '{big}'"#,
            k(2048)
        ),
        // k1, k2 and k10: the key of an UPDATE, a DELETE and an INSERT.
        format!(r#"UPDATE "{TK}" SET x = 1 WHERE pk = 'a' AND sk = '{big}'"#),
        format!(r#"DELETE FROM "{TK}" WHERE pk = 'a' AND sk = '{big}'"#),
        format!(r#"INSERT INTO "{TK}" VALUE {{'pk': 'z', 'sk': '{big}'}}"#),
    ];
    for text in cases {
        assert_eq!(
            single(&db, statement(text.clone())),
            Err(refused(RANGE)),
            "{}",
            &text[..60]
        );
    }
    // 11e: 1,024 bytes is within the limit.
    let at_limit = format!(
        r#"SELECT * FROM "{TK}" WHERE pk = 'a' AND sk = '{}'"#,
        s(1024)
    );
    assert_eq!(single(&db, statement(at_limit)), Ok(vec![]));
}

/// A key's size is its UTF-8 bytes, not its characters, and a sort key bound
/// given as a parameter is measured as a literal one is. Inferred: the
/// captures used one-byte characters and literal sort key bounds, and PutItem
/// measures a key in UTF-8 bytes.
#[test]
fn a_key_is_measured_in_utf8_bytes_and_a_parameter_bound_is_measured_too() {
    let db = setup();
    let e = |n: usize| "\u{e9}".repeat(n);
    // 513 two-byte characters are 1,026 bytes; 512 are 1,024.
    assert_eq!(
        single(
            &db,
            statement(format!(
                r#"SELECT * FROM "{TK}" WHERE pk = 'a' AND sk = '{}'"#,
                e(513)
            ))
        ),
        Err(refused(RANGE))
    );
    assert_eq!(
        single(
            &db,
            statement(format!(
                r#"SELECT * FROM "{TK}" WHERE pk = 'a' AND sk = '{}'"#,
                e(512)
            ))
        ),
        Ok(vec![])
    );
    // 1,025 are 2,050 bytes for a partition key; 1,024 are 2,048.
    assert_eq!(
        single(
            &db,
            statement(format!(r#"SELECT * FROM "{TH}" WHERE pk = '{}'"#, e(1025)))
        ),
        Err(refused(HASH))
    );
    assert_eq!(
        single(
            &db,
            statement(format!(r#"SELECT * FROM "{TH}" WHERE pk = '{}'"#, e(1024)))
        ),
        Ok(vec![])
    );
    // A BETWEEN bound given as a parameter.
    assert_eq!(
        single(
            &db,
            json!({
                "Statement": format!(
                    r#"SELECT * FROM "{TK}" WHERE pk = 'a' AND sk BETWEEN ? AND ?"#
                ),
                "Parameters": [{"S": "a"}, {"S": s(1025)}]
            })
        ),
        Err(refused(RANGE))
    );
    assert_eq!(
        single(
            &db,
            json!({
                "Statement": format!(
                    r#"SELECT * FROM "{TK}" WHERE pk = 'a' AND sk BETWEEN ? AND ?"#
                ),
                "Parameters": [{"S": "a"}, {"S": s(1024)}]
            })
        ),
        Ok(vec![json!({
            "pk": {"S": "a"}, "sk": {"S": "a"}, "lsk": {"S": "l"},
            "gpk": {"S": "g"}, "gsk": {"S": "h"}, "x": {"N": "0"}
        })])
    );
}

#[test]
fn a_statement_that_does_not_pin_the_key_is_not_refused_for_its_size() {
    let db = setup();
    let item = |pk: &str| {
        json!({
            "pk": {"S": pk}, "sk": {"S": pk}, "lsk": {"S": "l"},
            "gpk": {"S": "g"}, "gsk": {"S": "h"}, "x": {"N": "0"}
        })
    };
    // k11 and k12: several partition keys are a scan, which runs.
    for text in [
        format!(r#"SELECT * FROM "{TK}" WHERE pk IN ['{}', 'b']"#, k(2049)),
        format!(
            r#"SELECT * FROM "{TK}" WHERE pk = '{}' OR pk = 'b'"#,
            k(2049)
        ),
    ] {
        assert_eq!(single(&db, statement(text)), Ok(vec![item("b")]));
    }
    // k19: so do several sort key conditions.
    let oversized_sort_key = s(1025);
    for text in [
        format!(
            r#"SELECT * FROM "{TK}" WHERE pk = 'a' AND (sk = '{oversized_sort_key}' OR sk = 'a')"#
        ),
        format!(
            r#"SELECT * FROM "{TK}" WHERE pk = 'a' AND (sk = 'a' OR sk = '{oversized_sort_key}')"#
        ),
    ] {
        assert_eq!(single(&db, statement(text)), Ok(vec![item("a")]));
    }
    // k15: an UPDATE missing its sort key is refused for that first.
    let text = format!(r#"UPDATE "{TK}" SET x = 1 WHERE pk = '{}'"#, k(2049));
    assert_eq!(single(&db, statement(text)), Err(refused(MANDATORY)));
}

/// An OR under a key condition leaves the statement reading one partition,
/// so an oversized partition key there is refused as it is without the OR,
/// written as a literal or as a parameter (captured eu-west-2 and us-east-1,
/// 2026-10-10, cases 8a, 8b, 8c, 8e, 8f and 8h).
#[test]
fn a_partition_key_beside_an_or_is_still_measured() {
    let db = setup();
    let big = k(2049);
    for (table, alternatives) in [
        (TH, "(x = 1 OR x = 2)"),
        (TH, "(x = 1 OR y = 2)"),
        (TK, "(sk = 'a' OR sk = 'b')"),
    ] {
        let text = format!(r#"SELECT * FROM "{table}" WHERE pk = '{big}' AND {alternatives}"#);
        assert_eq!(
            single(&db, statement(text)),
            Err(refused(HASH)),
            "{table}: {alternatives}"
        );
    }
    let request = json!({
        "Statement": format!(r#"SELECT * FROM "{TH}" WHERE pk = ? AND (x = 1 OR x = 2)"#),
        "Parameters": [{"S": big}]
    });
    assert_eq!(single(&db, request), Err(refused(HASH)));
    // At the limit, it runs.
    let text = format!(
        r#"SELECT * FROM "{TH}" WHERE pk = '{}' AND (x = 1 OR x = 2)"#,
        k(2048)
    );
    assert_eq!(single(&db, statement(text)), Ok(vec![]));
    // The key named in two alternatives is refused as overlapping first.
    let text = format!(
        r#"SELECT * FROM "{TH}" WHERE (pk = '{big}' AND x = 1) OR (pk = '{big}' AND x = 2)"#
    );
    assert_eq!(
        single(&db, statement(text)),
        Err(refused(
            "Overlapping conditions with range keys are not supported in where clause"
        ))
    );
}

#[test]
fn a_batch_member_refused_for_its_key_size_echoes_its_table_and_costs_nothing() {
    let db = setup();
    let good = |x: u32| {
        statement(format!(
            r#"UPDATE "{TK}" SET x = {x} WHERE pk = 'b' AND sk = 'b'"#
        ))
    };
    // kb2, kb3, kb4 and kb5: beside an update charged 3 units (the table, the
    // GSI and the LSI), the refused member adds nothing. Each update changes
    // the item, as the captured ones did.
    for (x, (refused_member, message)) in (1..).zip([
        (
            format!(
                r#"UPDATE "{TK}" SET x = 1 WHERE pk = '{}' AND sk = 'a'"#,
                k(2049)
            ),
            HASH,
        ),
        (
            format!(
                r#"DELETE FROM "{TK}" WHERE pk = '{}' AND sk = 'a'"#,
                k(2049)
            ),
            HASH,
        ),
        (
            format!(
                r#"UPDATE "{TK}" SET x = 1 WHERE pk = 'a' AND sk = '{}'"#,
                s(1025)
            ),
            RANGE,
        ),
        (
            format!(
                r#"INSERT INTO "{TK}" VALUE {{'pk': 'y', 'sk': '{}'}}"#,
                s(1025)
            ),
            RANGE,
        ),
    ]) {
        assert_eq!(
            batch(
                &db,
                json!({
                    "ReturnConsumedCapacity": "TOTAL",
                    "Statements": [statement(refused_member.clone()), good(x)]
                })
            ),
            json!({
                "ConsumedCapacity": [{"CapacityUnits": 3.0, "TableName": TK}],
                "Responses": [item_error(message, TK), {"TableName": TK}]
            }),
            "{}",
            &refused_member[..40]
        );
    }
    // 16a: an INSERT refused beside an update on the hash-only table.
    assert_eq!(
        batch(
            &db,
            json!({
                "ReturnConsumedCapacity": "TOTAL",
                "Statements": [
                    statement(format!(r#"INSERT INTO "{TH}" VALUE {{'pk': '{}'}}"#, k(2049))),
                    statement(format!(r#"UPDATE "{TH}" SET x = 1 WHERE pk = 'b'"#))
                ]
            })
        ),
        json!({
            "ConsumedCapacity": [{"CapacityUnits": 1.0, "TableName": TH}],
            "Responses": [item_error(HASH, TH), {"TableName": TH}]
        })
    );
    // kb1, kb7 and 16b: alone, a refused member leaves nothing to report.
    for (member, message, table) in [
        (
            format!(
                r#"SELECT * FROM "{TK}" WHERE pk = 'a' AND sk = '{}'"#,
                s(1025)
            ),
            RANGE,
            TK,
        ),
        (
            format!(
                r#"UPDATE "{TK}" SET x = 1 WHERE pk = '{}' AND sk = 'a'"#,
                k(2049)
            ),
            HASH,
            TK,
        ),
        (
            format!(r#"SELECT * FROM "{TH}" WHERE pk = '{}'"#, k(2049)),
            HASH,
            TH,
        ),
    ] {
        assert_eq!(
            batch(
                &db,
                json!({"ReturnConsumedCapacity": "TOTAL", "Statements": [statement(member)]})
            ),
            json!({"Responses": [item_error(message, table)]})
        );
    }
    // kb6: beside a read, by index.
    assert_eq!(
        batch(
            &db,
            json!({
                "ReturnConsumedCapacity": "INDEXES",
                "Statements": [
                    statement(format!(
                        r#"SELECT * FROM "{TK}" WHERE pk = '{}' AND sk = 'a'"#,
                        k(2049)
                    )),
                    statement(format!(r#"SELECT * FROM "{TK}" WHERE pk = 'a' AND sk = 'a'"#))
                ]
            })
        ),
        json!({
            "ConsumedCapacity": [{
                "CapacityUnits": 0.5, "Table": {"CapacityUnits": 0.5}, "TableName": TK
            }],
            "Responses": [
                item_error(HASH, TK),
                {"Item": {
                    "pk": {"S": "a"}, "sk": {"S": "a"}, "lsk": {"S": "l"},
                    "gpk": {"S": "g"}, "gsk": {"S": "h"}, "x": {"N": "0"}
                }, "TableName": TK}
            ]
        })
    );
}

#[test]
fn a_transaction_member_refused_for_its_key_size_cancels_the_transaction() {
    let db = setup();
    let good = statement(format!(
        r#"UPDATE "{TK}" SET x = 5 WHERE pk = 'b' AND sk = 'b'"#
    ));
    let reason = |message: &str| json!({"Code": "ValidationError", "Message": message});
    let cancelled = |reasons: Value| {
        json!({
            "__type": "com.amazonaws.dynamodb.v20120810#TransactionCanceledException",
            "CancellationReasons": reasons
        })
    };
    // 12b, kt1 and kt2: beside a member that would have run.
    for (member, message) in [
        (
            format!(
                r#"UPDATE "{TK}" SET x = 1 WHERE pk = '{}' AND sk = 'a'"#,
                k(2049)
            ),
            HASH,
        ),
        (
            format!(
                r#"DELETE FROM "{TK}" WHERE pk = '{}' AND sk = 'a'"#,
                k(2049)
            ),
            HASH,
        ),
        (
            format!(
                r#"UPDATE "{TK}" SET x = 1 WHERE pk = 'a' AND sk = '{}'"#,
                s(1025)
            ),
            RANGE,
        ),
    ] {
        assert_eq!(
            transaction(
                &db,
                json!({"TransactStatements": [statement(member.clone()), good.clone()]})
            ),
            cancelled(json!([reason(message), {"Code": "None"}])),
            "{}",
            &member[..40]
        );
    }
    // kt3: a read alone.
    let member = format!(
        r#"SELECT * FROM "{TK}" WHERE pk = 'a' AND sk = '{}'"#,
        s(1025)
    );
    assert_eq!(
        transaction(&db, json!({"TransactStatements": [statement(member)]})),
        cancelled(json!([reason(RANGE)]))
    );
}
