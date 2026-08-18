//! A batch resolves each table once and hands the result to every statement
//! against it.
//!
//! The metadata and the parsed key schema are per table; only the item key is
//! per statement. So the preparation pass keeps them in a map keyed by table
//! name rather than resolving per statement.
//!
//! That map is the risk. A lookup that returned the wrong table's entry would
//! run a statement against another table's key schema, and it would go unnoticed
//! wherever both tables happen to be keyed alike, which is what the existing
//! multi-table coverage uses. Every case here therefore mixes a `pk`-only table
//! with a `pk`/`sk` one, so reading the key off the wrong schema cannot succeed
//! quietly.

use dynoxide::Database;

const COMPOSITE: &str = "shared_composite";
const HASH_ONLY: &str = "shared_hash_only";

/// Two tables that disagree about their key schema.
fn two_tables() -> Database {
    let db = Database::memory().unwrap();
    db.create_table(
        serde_json::from_value(serde_json::json!({
            "TableName": COMPOSITE,
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
    db.create_table(
        serde_json::from_value(serde_json::json!({
            "TableName": HASH_ONLY,
            "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}],
            "BillingMode": "PAY_PER_REQUEST"
        }))
        .unwrap(),
    )
    .unwrap();
    db
}

fn batch(db: &Database, statements: &[&str]) -> Vec<serde_json::Value> {
    let req = serde_json::json!({
        "Statements": statements.iter()
            .map(|s| serde_json::json!({"Statement": s}))
            .collect::<Vec<_>>()
    });
    let resp = db
        .batch_execute_statement(serde_json::from_value(req).unwrap())
        .unwrap();
    serde_json::to_value(resp.responses)
        .unwrap()
        .as_array()
        .cloned()
        .unwrap_or_default()
}

fn get(db: &Database, table: &str, key: serde_json::Value) -> Option<serde_json::Value> {
    let req = serde_json::json!({"TableName": table, "Key": key});
    let resp = db.get_item(serde_json::from_value(req).unwrap()).unwrap();
    resp.item.map(|i| serde_json::to_value(i).unwrap())
}

#[test]
fn a_batch_across_two_key_schemas_writes_each_to_its_own_table() {
    let db = two_tables();
    let responses = batch(
        &db,
        &[
            &format!("INSERT INTO \"{COMPOSITE}\" VALUE {{'pk':'p','sk':'s','v':'a'}}"),
            &format!("INSERT INTO \"{HASH_ONLY}\" VALUE {{'id':'i','v':'b'}}"),
            &format!("INSERT INTO \"{COMPOSITE}\" VALUE {{'pk':'p','sk':'t','v':'c'}}"),
        ],
    );
    for (i, r) in responses.iter().enumerate() {
        assert!(r.get("Error").is_none(), "statement {i} failed: {r}");
    }

    assert_eq!(
        get(
            &db,
            COMPOSITE,
            serde_json::json!({"pk": {"S": "p"}, "sk": {"S": "s"}})
        )
        .expect("composite row")["v"]["S"],
        "a"
    );
    assert_eq!(
        get(&db, HASH_ONLY, serde_json::json!({"id": {"S": "i"}})).expect("hash-only row")["v"]["S"],
        "b"
    );
    assert_eq!(
        get(
            &db,
            COMPOSITE,
            serde_json::json!({"pk": {"S": "p"}, "sk": {"S": "t"}})
        )
        .expect("second composite row")["v"]["S"],
        "c"
    );
}

#[test]
fn duplicate_detection_across_two_key_schemas_does_not_confuse_the_tables() {
    let db = two_tables();
    // Same key values, different tables. These are two distinct items, so the
    // batch must not read them as a duplicate pair.
    let responses = batch(
        &db,
        &[
            &format!("INSERT INTO \"{COMPOSITE}\" VALUE {{'pk':'x','sk':'y','v':'a'}}"),
            &format!("INSERT INTO \"{HASH_ONLY}\" VALUE {{'id':'x','v':'b'}}"),
        ],
    );
    assert_eq!(responses.len(), 2);
    for (i, r) in responses.iter().enumerate() {
        assert!(r.get("Error").is_none(), "statement {i} failed: {r}");
    }
}

#[test]
fn a_batch_naming_a_missing_table_still_runs_its_other_statements() {
    let db = two_tables();
    let responses = batch(
        &db,
        &[
            &format!("INSERT INTO \"{COMPOSITE}\" VALUE {{'pk':'p','sk':'s','v':'a'}}"),
            "INSERT INTO \"no_such_table\" VALUE {'pk':'p','sk':'s'}",
            &format!("INSERT INTO \"{HASH_ONLY}\" VALUE {{'id':'i','v':'b'}}"),
        ],
    );
    assert!(responses[0].get("Error").is_none(), "{}", responses[0]);
    assert!(
        responses[1].get("Error").is_some(),
        "a missing table should fail its own statement"
    );
    assert!(responses[2].get("Error").is_none(), "{}", responses[2]);

    // The rows either side of the failure still landed, each on its own schema.
    assert!(
        get(
            &db,
            COMPOSITE,
            serde_json::json!({"pk": {"S": "p"}, "sk": {"S": "s"}})
        )
        .is_some()
    );
    assert!(get(&db, HASH_ONLY, serde_json::json!({"id": {"S": "i"}})).is_some());
}

#[test]
fn a_transaction_across_two_key_schemas_writes_each_to_its_own_table() {
    let db = two_tables();
    let req = serde_json::json!({
        "TransactStatements": [
            {"Statement": format!(
                "INSERT INTO \"{COMPOSITE}\" VALUE {{'pk':'p','sk':'s','v':'a'}}"
            )},
            {"Statement": format!("INSERT INTO \"{HASH_ONLY}\" VALUE {{'id':'i','v':'b'}}")}
        ]
    });
    db.execute_transaction(serde_json::from_value(req).unwrap())
        .unwrap();

    assert_eq!(
        get(
            &db,
            COMPOSITE,
            serde_json::json!({"pk": {"S": "p"}, "sk": {"S": "s"}})
        )
        .expect("composite row")["v"]["S"],
        "a"
    );
    assert_eq!(
        get(&db, HASH_ONLY, serde_json::json!({"id": {"S": "i"}})).expect("hash-only row")["v"]["S"],
        "b"
    );
}

#[test]
fn a_transaction_still_rejects_two_operations_on_one_item() {
    let db = two_tables();
    let req = serde_json::json!({
        "TransactStatements": [
            {"Statement": format!(
                "INSERT INTO \"{COMPOSITE}\" VALUE {{'pk':'p','sk':'s','v':'a'}}"
            )},
            {"Statement": format!(
                "INSERT INTO \"{COMPOSITE}\" VALUE {{'pk':'p','sk':'s','v':'b'}}"
            )}
        ]
    });
    let err = db
        .execute_transaction(serde_json::from_value(req).unwrap())
        .unwrap_err();
    assert!(
        format!("{err:?}").contains("multiple operations on one item"),
        "expected the duplicate-target rejection, got {err:?}"
    );
}
