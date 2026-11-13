//! A PartiQL SET sum with brackets is worked out group by group, each running
//! total held to what a number holds, and a sum too large to hold is refused
//! inside a transaction and in a batch as it is on its own. ORDER BY reads one
//! or several partitions, through an IN list, an OR or an index.
use dynoxide::Database;
use dynoxide::errors::DynoxideError;
use serde_json::{Value, json};

/// 38 nines, the largest integer a number holds.
const BIG: &str = "99999999999999999999999999999999999999";

const OVERFLOW: &str =
    "Number overflow. Attempting to store a number with magnitude larger than supported range";

fn setup() -> Database {
    let db = Database::memory().unwrap();
    db.create_table(
        serde_json::from_value(json!({
            "TableName": "Sums", "BillingMode": "PAY_PER_REQUEST",
            "AttributeDefinitions": [
                {"AttributeName":"pk","AttributeType":"S"},
                {"AttributeName":"sk","AttributeType":"S"},
                {"AttributeName":"g","AttributeType":"S"}
            ],
            "KeySchema": [{"AttributeName":"pk","KeyType":"HASH"},
                          {"AttributeName":"sk","KeyType":"RANGE"}],
            "GlobalSecondaryIndexes": [{"IndexName":"by-g",
                "KeySchema":[{"AttributeName":"g","KeyType":"HASH"},
                             {"AttributeName":"sk","KeyType":"RANGE"}],
                "Projection":{"ProjectionType":"ALL"}}]
        }))
        .unwrap(),
    )
    .unwrap();
    for (pk, sk) in [("a", "1"), ("a", "2"), ("b", "1"), ("b", "2")] {
        db.put_item(
            serde_json::from_value(json!({"TableName":"Sums", "Item":{
                "pk":{"S":pk},"sk":{"S":sk},"g":{"S":"x"},"nbig":{"N":BIG}
            }}))
            .unwrap(),
        )
        .unwrap();
    }
    db
}

fn statement(db: &Database, sql: &str) -> Result<Value, String> {
    db.execute_statement(serde_json::from_value(json!({ "Statement": sql })).unwrap())
        .map(|r| serde_json::to_value(r).unwrap())
        .map_err(|e| e.to_string())
}

fn z_of(db: &Database, pk: &str, sk: &str) -> Option<Value> {
    let item = db
        .get_item(
            serde_json::from_value(json!({
                "TableName": "Sums", "Key": {"pk":{"S":pk},"sk":{"S":sk}}
            }))
            .unwrap(),
        )
        .unwrap()
        .item?;
    serde_json::to_value(item).unwrap().get("z").cloned()
}

fn update(z: &str, pk: &str, sk: &str) -> String {
    format!(r#"UPDATE "Sums" SET z = {z} WHERE pk = '{pk}' AND sk = '{sk}'"#)
}

/// A bracket of several terms is worked out on its own and then added, so
/// a total inside it that would not fit outside it doesn't arise.
#[test]
fn a_bracket_is_worked_out_before_it_is_added() {
    let db = setup();
    for z in [
        format!("{BIG} + (9 - 9)"),
        format!("2 + ({BIG} - 2)"),
        "nbig - (9 - 9)".to_string(),
        format!("({BIG} - 1) + 1"),
    ] {
        statement(&db, &update(&z, "a", "1")).unwrap_or_else(|e| panic!("{z}: {e}"));
        assert_eq!(z_of(&db, "a", "1"), Some(json!({ "N": BIG })), "{z}");
    }
}

/// A running total past what a number holds is refused wherever it arises:
/// inside a bracket, or in the run outside one, even when the sum would fit.
#[test]
fn a_running_total_too_large_is_refused_in_a_bracket_or_outside_one() {
    let db = setup();
    for z in [
        format!("{BIG} - (9 + {BIG})"),
        format!("{BIG} + 9 - 9"),
        format!("({BIG} + 9 - 9)"),
        format!("1 + ({BIG} + 9 - 9)"),
    ] {
        assert_eq!(
            statement(&db, &update(&z, "a", "2")),
            Err(OVERFLOW.to_string()),
            "{z}"
        );
        assert_eq!(z_of(&db, "a", "2"), None, "{z}");
    }
}

/// A sum too large to hold fails a transaction, writing nothing, and fails
/// its own member of a batch, while the other members are written.
#[test]
fn a_sum_too_large_is_refused_in_a_transaction_and_a_batch() {
    let db = setup();
    let too_large = update(&format!("{BIG} + 9 - 9"), "a", "1");
    let fits = update("1 + 1", "b", "1");
    let transaction = db.execute_transaction(
        serde_json::from_value(json!({"TransactStatements": [
            {"Statement": fits}, {"Statement": too_large}
        ]}))
        .unwrap(),
    );
    let Err(DynoxideError::TransactionCanceledException(_, reasons)) = transaction else {
        panic!("the transaction is cancelled: {transaction:?}");
    };
    let reasons: Vec<(&str, Option<&str>)> = reasons
        .iter()
        .map(|r| (r.code.as_str(), r.message.as_deref()))
        .collect();
    assert_eq!(
        reasons,
        [("None", None), ("ValidationError", Some(OVERFLOW))]
    );
    assert_eq!(z_of(&db, "b", "1"), None);

    let batch = db
        .batch_execute_statement(
            serde_json::from_value(json!({"Statements": [
                {"Statement": too_large}, {"Statement": fits}
            ]}))
            .unwrap(),
        )
        .unwrap();
    let responses = serde_json::to_value(batch).unwrap()["Responses"].clone();
    assert_eq!(
        responses[0]["Error"]["Message"],
        json!(OVERFLOW),
        "{responses}"
    );
    assert!(responses[1].get("Error").is_none(), "{responses}");
    assert_eq!(z_of(&db, "a", "1"), None);
    assert_eq!(z_of(&db, "b", "1"), Some(json!({"N": "2"})));
}

fn keys(response: &Value) -> Vec<String> {
    response["Items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| {
            format!(
                "{}{}",
                item["pk"]["S"].as_str().unwrap(),
                item["sk"]["S"].as_str().unwrap()
            )
        })
        .collect()
}

/// ORDER BY reads several partitions named by an IN list or an OR of
/// equalities, ordered by the partition key and then the sort key, and reads
/// an index by its own keys.
#[test]
fn order_by_reads_an_in_list_an_or_and_an_index() {
    let db = setup();
    for clause in ["pk IN ['b', 'a']", "pk = 'b' OR pk = 'a'"] {
        let sql = format!(r#"SELECT pk, sk FROM "Sums" WHERE {clause} ORDER BY pk, sk"#);
        let response = statement(&db, &sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
        assert_eq!(keys(&response), ["a1", "a2", "b1", "b2"], "{sql}");
        let sql = format!(r#"SELECT pk, sk FROM "Sums" WHERE {clause} ORDER BY pk DESC, sk DESC"#);
        let response = statement(&db, &sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
        assert_eq!(keys(&response), ["b2", "b1", "a2", "a1"], "{sql}");
    }
    let sql = r#"SELECT pk, sk FROM "Sums"."by-g" WHERE g = 'x' ORDER BY sk DESC"#;
    let response = statement(&db, sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut sort_keys: Vec<String> = keys(&response)
        .into_iter()
        .map(|key| key[1..].to_string())
        .collect();
    assert_eq!(sort_keys.len(), 4);
    sort_keys.dedup();
    assert_eq!(sort_keys, ["2", "1"]);
}
