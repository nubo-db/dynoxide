//! Write-time validation of secondary-index key attributes (#92).
//!
//! DynamoDB rejects a write whose GSI or LSI key attribute is the wrong type, a
//! non-scalar, or empty. Index key *presence* stays optional (sparse); only a
//! present-but-invalid value is rejected. Messages are asserted against the exact
//! strings captured from real DynamoDB.

use dynoxide::Database;
use dynoxide::DynoxideError;
use serde_json::json;

/// Table with GSIs on an S key (`g_s`), an N key (`g_n`), a B key (`g_b`), and an
/// LSI on an S key (`l_s`).
fn idx_table() -> serde_json::Value {
    json!({
        "TableName": "IdxT",
        "KeySchema": [
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"}
        ],
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "gk", "AttributeType": "S"},
            {"AttributeName": "nk", "AttributeType": "N"},
            {"AttributeName": "bk", "AttributeType": "B"},
            {"AttributeName": "lk", "AttributeType": "S"}
        ],
        "GlobalSecondaryIndexes": [
            {"IndexName": "g_s", "KeySchema": [{"AttributeName": "gk", "KeyType": "HASH"}], "Projection": {"ProjectionType": "ALL"}},
            {"IndexName": "g_n", "KeySchema": [{"AttributeName": "nk", "KeyType": "HASH"}], "Projection": {"ProjectionType": "ALL"}},
            {"IndexName": "g_b", "KeySchema": [{"AttributeName": "bk", "KeyType": "HASH"}], "Projection": {"ProjectionType": "ALL"}}
        ],
        "LocalSecondaryIndexes": [
            {"IndexName": "l_s", "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "lk", "KeyType": "RANGE"}], "Projection": {"ProjectionType": "ALL"}}
        ]
    })
}

fn make_db() -> Database {
    let db = Database::memory().unwrap();
    db.create_table(serde_json::from_value(idx_table()).unwrap())
        .unwrap();
    db
}

fn put(db: &Database, table: &str, item: serde_json::Value) -> Result<(), String> {
    db.put_item(serde_json::from_value(json!({"TableName": table, "Item": item})).unwrap())
        .map(|_| ())
        .map_err(|e| e.to_string())
}

// ---- PutItem: the validation matrix ----

#[test]
fn put_rejects_wrong_typed_gsi_key() {
    let db = make_db();
    let msg = put(
        &db,
        "IdxT",
        json!({"pk": {"S": "p"}, "sk": {"S": "s"}, "gk": {"N": "5"}}),
    )
    .unwrap_err();
    assert_eq!(
        msg,
        "One or more parameter values were invalid: Type mismatch for Index Key gk Expected: S Actual: N IndexName: g_s"
    );
}

#[test]
fn put_rejects_every_non_scalar_gsi_key() {
    let db = make_db();
    for (val, tag) in [
        (json!({"L": [{"S": "x"}]}), "L"),
        (json!({"M": {"a": {"S": "x"}}}), "M"),
        (json!({"BOOL": true}), "BOOL"),
        (json!({"NULL": true}), "NULL"),
        (json!({"SS": ["a"]}), "SS"),
        (json!({"NS": ["1"]}), "NS"),
    ] {
        let msg = put(
            &db,
            "IdxT",
            json!({"pk": {"S": "p"}, "sk": {"S": "s"}, "gk": val}),
        )
        .unwrap_err();
        assert_eq!(
            msg,
            format!(
                "One or more parameter values were invalid: Type mismatch for Index Key gk Expected: S Actual: {tag} IndexName: g_s"
            ),
            "non-scalar {tag}"
        );
    }
}

#[test]
fn put_rejects_empty_string_gsi_key() {
    let db = make_db();
    let msg = put(
        &db,
        "IdxT",
        json!({"pk": {"S": "p"}, "sk": {"S": "s"}, "gk": {"S": ""}}),
    )
    .unwrap_err();
    assert_eq!(
        msg,
        "One or more parameter values are not valid. A value specified for a secondary index key is not supported. The AttributeValue for a key attribute cannot contain an empty string value. IndexName: g_s, IndexKey: gk"
    );
}

#[test]
fn put_rejects_empty_binary_gsi_key() {
    let db = make_db();
    let msg = put(
        &db,
        "IdxT",
        json!({"pk": {"S": "p"}, "sk": {"S": "s"}, "bk": {"B": ""}}),
    )
    .unwrap_err();
    assert_eq!(
        msg,
        "One or more parameter values are not valid. A value specified for a secondary index key is not supported. The AttributeValue for a key attribute cannot contain an empty binary value. IndexName: g_b, IndexKey: bk"
    );
}

#[test]
fn put_accepts_valid_numeric_and_binary_keys() {
    let db = make_db();
    // bk = base64("hi"); valid non-empty binary and a valid number.
    put(
        &db,
        "IdxT",
        json!({"pk": {"S": "p"}, "sk": {"S": "s"}, "nk": {"N": "42"}, "bk": {"B": "aGk="}}),
    )
    .unwrap();
}

#[test]
fn put_accepts_absent_index_keys() {
    let db = make_db();
    // No gk/nk/bk/lk at all -- sparse, must not error.
    put(&db, "IdxT", json!({"pk": {"S": "p"}, "sk": {"S": "s"}})).unwrap();
}

#[test]
fn put_rejects_wrong_typed_lsi_key() {
    let db = make_db();
    let msg = put(
        &db,
        "IdxT",
        json!({"pk": {"S": "p"}, "sk": {"S": "s"}, "lk": {"N": "5"}}),
    )
    .unwrap_err();
    assert_eq!(
        msg,
        "One or more parameter values were invalid: Type mismatch for Index Key lk Expected: S Actual: N IndexName: l_s"
    );
}

#[test]
fn put_names_alphabetically_first_index_for_shared_attribute() {
    let db = Database::memory().unwrap();
    // Two GSIs on the same attribute, created z-before-a; AWS names the alphabetically first.
    db.create_table(serde_json::from_value(json!({
        "TableName": "MultiIdx",
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "gk", "AttributeType": "S"}
        ],
        "GlobalSecondaryIndexes": [
            {"IndexName": "g_z", "KeySchema": [{"AttributeName": "gk", "KeyType": "HASH"}], "Projection": {"ProjectionType": "ALL"}},
            {"IndexName": "g_a", "KeySchema": [{"AttributeName": "gk", "KeyType": "HASH"}], "Projection": {"ProjectionType": "ALL"}}
        ]
    })).unwrap()).unwrap();
    let msg = put(&db, "MultiIdx", json!({"pk": {"S": "p"}, "gk": {"N": "5"}})).unwrap_err();
    assert!(msg.contains("IndexName: g_a"), "{msg}");
}

#[test]
fn put_names_alphabetically_first_index_across_attributes() {
    let db = make_db();
    // gk (g_s) and nk (g_n) both invalid; g_n sorts first.
    let msg = put(
        &db,
        "IdxT",
        json!({"pk": {"S": "p"}, "sk": {"S": "s"}, "gk": {"L": []}, "nk": {"S": "x"}}),
    )
    .unwrap_err();
    assert!(msg.contains("IndexName: g_n"), "{msg}");
}

#[test]
fn table_key_error_takes_precedence_over_index_key() {
    let db = make_db();
    // Empty table partition key AND a bad index key -> the table-key error wins.
    let msg = put(
        &db,
        "IdxT",
        json!({"pk": {"S": ""}, "sk": {"S": "s"}, "gk": {"N": "5"}}),
    )
    .unwrap_err();
    assert!(
        msg.contains("Key: pk"),
        "expected table-key error, got: {msg}"
    );
    assert!(!msg.contains("Index Key"), "{msg}");
}

#[test]
fn lsi_partition_key_validated_as_table_key_not_index_key() {
    let db = make_db();
    // pk is the table key and the LSI partition key. A wrong-typed pk is a
    // table-key error; the LSI pk is not re-reported as an Index Key.
    let msg = put(&db, "IdxT", json!({"pk": {"N": "5"}, "sk": {"S": "s"}})).unwrap_err();
    assert!(!msg.contains("Index Key"), "{msg}");
    assert!(msg.contains("key pk"), "{msg}");
}

#[test]
fn gsi_sort_key_validated_when_partition_key_is_the_table_key() {
    let db = Database::memory().unwrap();
    // GSI partition key IS the table pk; only its own sort key (gsk) is index-specific.
    db.create_table(serde_json::from_value(json!({
        "TableName": "SparseG",
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}],
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "gsk", "AttributeType": "S"}
        ],
        "GlobalSecondaryIndexes": [{
            "IndexName": "gidx",
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "gsk", "KeyType": "RANGE"}],
            "Projection": {"ProjectionType": "ALL"}
        }]
    })).unwrap()).unwrap();
    // Valid pk (shared with the GSI), bad GSI sort key.
    let msg = put(
        &db,
        "SparseG",
        json!({"pk": {"S": "p"}, "sk": {"S": "s"}, "gsk": {"N": "5"}}),
    )
    .unwrap_err();
    assert!(msg.contains("Type mismatch for Index Key gsk"), "{msg}");
    assert!(msg.contains("IndexName: gidx"), "{msg}");
}

// ---- UpdateItem ----

fn update(
    db: &Database,
    key: serde_json::Value,
    expr: &str,
    vals: serde_json::Value,
) -> Result<(), String> {
    let mut req = json!({"TableName": "IdxT", "Key": key, "UpdateExpression": expr});
    if vals.as_object().is_some_and(|o| !o.is_empty()) {
        req["ExpressionAttributeValues"] = vals;
    }
    db.update_item(serde_json::from_value(req).unwrap())
        .map(|_| ())
        .map_err(|e| e.to_string())
}

#[test]
fn update_set_invalid_index_key_rejected() {
    let db = make_db();
    put(
        &db,
        "IdxT",
        json!({"pk": {"S": "p"}, "sk": {"S": "s"}, "gk": {"S": "ok"}}),
    )
    .unwrap();
    let msg = update(
        &db,
        json!({"pk": {"S": "p"}, "sk": {"S": "s"}}),
        "SET gk = :v",
        json!({":v": {"N": "5"}}),
    )
    .unwrap_err();
    assert!(msg.contains("Type mismatch for Index Key gk"), "{msg}");
}

#[test]
fn update_set_empty_string_index_key_rejected() {
    let db = make_db();
    put(
        &db,
        "IdxT",
        json!({"pk": {"S": "p"}, "sk": {"S": "s"}, "gk": {"S": "ok"}}),
    )
    .unwrap();
    let msg = update(
        &db,
        json!({"pk": {"S": "p"}, "sk": {"S": "s"}}),
        "SET gk = :v",
        json!({":v": {"S": ""}}),
    )
    .unwrap_err();
    // The update path uses a distinct message with no IndexName/IndexKey suffix.
    assert_eq!(
        msg,
        "One or more parameter values are not valid. The update expression attempted to update a secondary index key to a value that is not supported. The AttributeValue for a key attribute cannot contain an empty string value."
    );
}

#[test]
fn update_remove_index_key_allowed() {
    let db = make_db();
    put(
        &db,
        "IdxT",
        json!({"pk": {"S": "p"}, "sk": {"S": "s"}, "gk": {"S": "ok"}}),
    )
    .unwrap();
    update(
        &db,
        json!({"pk": {"S": "p"}, "sk": {"S": "s"}}),
        "REMOVE gk",
        json!({}),
    )
    .unwrap();
}

#[test]
fn update_unrelated_set_does_not_revalidate_legacy_bad_index_key() {
    // The decisive touched-only case: a row with a pre-existing bad index key value
    // (here a numeric gk under an S-typed GSI added afterwards) must accept an
    // unrelated update without re-rejecting the untouched value.
    let db = Database::memory().unwrap();
    db.create_table(serde_json::from_value(json!({
        "TableName": "IdxT",
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}],
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"}
        ]
    })).unwrap()).unwrap();
    // gk is a free numeric attribute -- no index yet, so this is accepted.
    put(
        &db,
        "IdxT",
        json!({"pk": {"S": "p"}, "sk": {"S": "s"}, "gk": {"N": "5"}}),
    )
    .unwrap();
    // Add an S-typed GSI on gk after the fact.
    db.update_table(
        serde_json::from_value(json!({
            "TableName": "IdxT",
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
                {"AttributeName": "gk", "AttributeType": "S"}
            ],
            "GlobalSecondaryIndexUpdates": [{"Create": {
                "IndexName": "g_s",
                "KeySchema": [{"AttributeName": "gk", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"}
            }}]
        }))
        .unwrap(),
    )
    .unwrap();
    // Unrelated SET -- gk untouched, so the legacy value is not re-validated.
    update(
        &db,
        json!({"pk": {"S": "p"}, "sk": {"S": "s"}}),
        "SET custom_field = :v",
        json!({":v": {"S": "x"}}),
    )
    .unwrap();
}

// ---- Other write paths share the chokepoint / hooks ----

#[test]
fn batch_put_rejects_bad_index_key() {
    let db = make_db();
    let res = db.batch_write_item(
        serde_json::from_value(json!({
            "RequestItems": {"IdxT": [
                {"PutRequest": {"Item": {"pk": {"S": "p"}, "sk": {"S": "s"}, "gk": {"N": "5"}}}}
            ]}
        }))
        .unwrap(),
    );
    assert!(res.is_err(), "batch with a bad index key should fail");
}

#[test]
fn transact_put_rejects_bad_index_key() {
    // A wrong-type index key stays a cancellation reason (ValidationError) inside a transaction.
    let db = make_db();
    let err = db
        .transact_write_items(
            serde_json::from_value(json!({
                "TransactItems": [
                    {"Put": {"TableName": "IdxT", "Item": {"pk": {"S": "p"}, "sk": {"S": "s"}, "gk": {"N": "5"}}}}
                ]
            }))
            .unwrap(),
        )
        .unwrap_err();
    match err {
        DynoxideError::TransactionCanceledException(_, reasons) => {
            assert_eq!(reasons[0].code, "ValidationError");
            assert_eq!(
                reasons[0].message.as_deref().unwrap_or_default(),
                "One or more parameter values were invalid: Type mismatch for Index Key gk Expected: S Actual: N IndexName: g_s"
            );
        }
        other => panic!("wrong-type index key must stay a cancellation reason, got {other:?}"),
    }
}

#[test]
fn transact_update_rejects_bad_index_key() {
    // A wrong-type index key set by an update stays a cancellation reason (ValidationError).
    let db = make_db();
    put(
        &db,
        "IdxT",
        json!({"pk": {"S": "p"}, "sk": {"S": "s"}, "gk": {"S": "ok"}}),
    )
    .unwrap();
    let err = db
        .transact_write_items(
            serde_json::from_value(json!({
                "TransactItems": [
                    {"Update": {
                        "TableName": "IdxT",
                        "Key": {"pk": {"S": "p"}, "sk": {"S": "s"}},
                        "UpdateExpression": "SET gk = :v",
                        "ExpressionAttributeValues": {":v": {"N": "5"}}
                    }}
                ]
            }))
            .unwrap(),
        )
        .unwrap_err();
    match err {
        DynoxideError::TransactionCanceledException(_, reasons) => {
            assert_eq!(reasons[0].code, "ValidationError");
            assert_eq!(
                reasons[0].message.as_deref().unwrap_or_default(),
                "One or more parameter values were invalid: Type mismatch for Index Key gk Expected: S Actual: N IndexName: g_s"
            );
        }
        other => {
            panic!("wrong-type index key on update must stay a cancellation reason, got {other:?}")
        }
    }
}

#[test]
fn partiql_insert_rejects_bad_index_key() {
    use dynoxide::actions::execute_statement::ExecuteStatementRequest;
    let db = make_db();
    let res = db.execute_statement(ExecuteStatementRequest {
        statement: "INSERT INTO \"IdxT\" VALUE {'pk': 'p', 'sk': 's', 'gk': 5}".to_string(),
        parameters: None,
        ..Default::default()
    });
    assert!(
        res.is_err(),
        "PartiQL INSERT with a bad index key should fail"
    );
}

#[test]
fn partiql_update_rejects_bad_index_key() {
    use dynoxide::actions::execute_statement::ExecuteStatementRequest;
    let db = make_db();
    put(
        &db,
        "IdxT",
        json!({"pk": {"S": "p"}, "sk": {"S": "s"}, "gk": {"S": "ok"}}),
    )
    .unwrap();
    let res = db.execute_statement(ExecuteStatementRequest {
        statement: "UPDATE \"IdxT\" SET gk = 5 WHERE pk = 'p' AND sk = 's'".to_string(),
        parameters: None,
        ..Default::default()
    });
    assert!(
        res.is_err(),
        "PartiQL UPDATE setting a bad index key should fail"
    );
}

#[test]
fn import_rejects_bad_index_key() {
    use dynoxide::ImportOptions;
    let db = make_db();
    let items = vec![dynoxide::item! { "pk" => "p", "sk" => "s", "gk" => 5u64 }];
    let res = db.import_items("IdxT", items, ImportOptions::default());
    assert!(
        res.is_err(),
        "import of an item with a bad index key should fail"
    );
}

// ---- empty-string index keys surface top-level; non-scalar stays a cancellation reason ----

#[test]
fn transact_put_empty_string_index_key_is_top_level_validation() {
    let db = make_db();
    let err = db
        .transact_write_items(
            serde_json::from_value(json!({
                "TransactItems": [
                    {"Put": {"TableName": "IdxT", "Item": {"pk": {"S": "p"}, "sk": {"S": "s"}, "gk": {"S": ""}}}}
                ]
            }))
            .unwrap(),
        )
        .unwrap_err();
    assert_eq!(
        err.error_type(),
        "com.amazon.coral.validate#ValidationException",
        "empty-string index key must surface as a top-level ValidationException, got {err:?}"
    );
    assert!(
        !matches!(err, DynoxideError::TransactionCanceledException(..)),
        "must not be wrapped as a transaction cancellation: {err:?}"
    );
    assert_eq!(
        err.to_string(),
        "One or more parameter values are not valid. A value specified for a secondary index key is not supported. The AttributeValue for a key attribute cannot contain an empty string value. IndexName: g_s, IndexKey: gk"
    );
}

#[test]
fn transact_update_empty_string_index_key_is_top_level_validation() {
    let db = make_db();
    put(
        &db,
        "IdxT",
        json!({"pk": {"S": "p"}, "sk": {"S": "s"}, "gk": {"S": "ok"}}),
    )
    .unwrap();
    let err = db
        .transact_write_items(
            serde_json::from_value(json!({
                "TransactItems": [
                    {"Update": {
                        "TableName": "IdxT",
                        "Key": {"pk": {"S": "p"}, "sk": {"S": "s"}},
                        "UpdateExpression": "SET gk = :v",
                        "ExpressionAttributeValues": {":v": {"S": ""}}
                    }}
                ]
            }))
            .unwrap(),
        )
        .unwrap_err();
    assert_eq!(
        err.error_type(),
        "com.amazon.coral.validate#ValidationException",
        "empty-string index key set by an update must surface top-level, got {err:?}"
    );
    assert!(
        !matches!(err, DynoxideError::TransactionCanceledException(..)),
        "must not be wrapped as a transaction cancellation: {err:?}"
    );
    let msg = err.to_string();
    // AWS uses a distinct update-path wording with no IndexName/IndexKey suffix.
    assert_eq!(
        msg,
        "One or more parameter values are not valid. The update expression attempted to update a secondary index key to a value that is not supported. The AttributeValue for a key attribute cannot contain an empty string value."
    );
    assert!(
        !msg.contains("IndexName:"),
        "the update form must drop the IndexName/IndexKey suffix: {msg}"
    );
}

// ---- empty-binary index keys mirror empty-string: top-level, hoisting in a transaction ----

#[test]
fn update_set_empty_binary_index_key_rejected() {
    let db = make_db();
    put(
        &db,
        "IdxT",
        json!({"pk": {"S": "p"}, "sk": {"S": "s"}, "bk": {"B": "AQ=="}}),
    )
    .unwrap();
    let msg = update(
        &db,
        json!({"pk": {"S": "p"}, "sk": {"S": "s"}}),
        "SET bk = :v",
        json!({":v": {"B": ""}}),
    )
    .unwrap_err();
    // The update path uses a distinct binary message with no IndexName/IndexKey suffix.
    assert_eq!(
        msg,
        "One or more parameter values are not valid. The update expression attempted to update a secondary index key to a value that is not supported. The AttributeValue for a key attribute cannot contain an empty binary value."
    );
}

#[test]
fn transact_put_empty_binary_index_key_is_top_level_validation() {
    let db = make_db();
    let err = db
        .transact_write_items(
            serde_json::from_value(json!({
                "TransactItems": [
                    {"Put": {"TableName": "IdxT", "Item": {"pk": {"S": "p"}, "sk": {"S": "s"}, "bk": {"B": ""}}}}
                ]
            }))
            .unwrap(),
        )
        .unwrap_err();
    assert_eq!(
        err.error_type(),
        "com.amazon.coral.validate#ValidationException",
        "empty-binary index key must surface as a top-level ValidationException, got {err:?}"
    );
    assert!(
        !matches!(err, DynoxideError::TransactionCanceledException(..)),
        "must not be wrapped as a transaction cancellation: {err:?}"
    );
    assert_eq!(
        err.to_string(),
        "One or more parameter values are not valid. A value specified for a secondary index key is not supported. The AttributeValue for a key attribute cannot contain an empty binary value. IndexName: g_b, IndexKey: bk"
    );
}

#[test]
fn transact_update_empty_binary_index_key_is_top_level_validation() {
    let db = make_db();
    put(
        &db,
        "IdxT",
        json!({"pk": {"S": "p"}, "sk": {"S": "s"}, "bk": {"B": "AQ=="}}),
    )
    .unwrap();
    let err = db
        .transact_write_items(
            serde_json::from_value(json!({
                "TransactItems": [
                    {"Update": {
                        "TableName": "IdxT",
                        "Key": {"pk": {"S": "p"}, "sk": {"S": "s"}},
                        "UpdateExpression": "SET bk = :v",
                        "ExpressionAttributeValues": {":v": {"B": ""}}
                    }}
                ]
            }))
            .unwrap(),
        )
        .unwrap_err();
    assert_eq!(
        err.error_type(),
        "com.amazon.coral.validate#ValidationException",
        "empty-binary index key set by an update must surface top-level, got {err:?}"
    );
    assert!(
        !matches!(err, DynoxideError::TransactionCanceledException(..)),
        "must not be wrapped as a transaction cancellation: {err:?}"
    );
    let msg = err.to_string();
    assert_eq!(
        msg,
        "One or more parameter values are not valid. The update expression attempted to update a secondary index key to a value that is not supported. The AttributeValue for a key attribute cannot contain an empty binary value."
    );
    assert!(
        !msg.contains("IndexName:"),
        "the update form must drop the IndexName/IndexKey suffix: {msg}"
    );
}

#[test]
fn transact_put_non_scalar_index_key_is_cancellation_reason() {
    let db = make_db();
    let err = db
        .transact_write_items(
            serde_json::from_value(json!({
                "TransactItems": [
                    {"Put": {"TableName": "IdxT", "Item": {"pk": {"S": "p"}, "sk": {"S": "s"}, "gk": {"L": [{"S": "x"}]}}}}
                ]
            }))
            .unwrap(),
        )
        .unwrap_err();
    match err {
        DynoxideError::TransactionCanceledException(_, reasons) => {
            assert_eq!(reasons[0].code, "ValidationError");
            assert_eq!(
                reasons[0].message.as_deref().unwrap_or_default(),
                "One or more parameter values were invalid: Type mismatch for Index Key gk Expected: S Actual: L IndexName: g_s"
            );
        }
        other => panic!("non-scalar index key must stay a cancellation reason, got {other:?}"),
    }
}
