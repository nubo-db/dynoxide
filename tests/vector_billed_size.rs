//! How a vector attribute is sized on a table with vector indexes.
//!
//! DynamoDB has two sizes for such an item. Write units, read units and
//! TableSizeBytes charge an attribute a vector index reads at its name plus 4
//! bytes per dimension, whatever numbers it holds. The 400 KB limit sizes the
//! vector as the list of numbers it is. Captured eu-west-2 and us-east-1,
//! 2026-10-09, at 1, 3, 6, 8, 256 and 1,024 dimensions, each figure exact to
//! the byte on the 1 KB write-unit, 4 KB read-unit and 400 KB boundaries.
//! Reads were then sized by the numbers too; they moved to the billed size by
//! 2026-10-22, on every read path (captured again 2026-10-23).

use dynoxide::actions::vector_lifecycle::VectorIndexLifecycle;
use dynoxide::actions::{
    batch_execute_statement, batch_get_item, batch_write_item, create_table, delete_item,
    describe_table, execute_statement, execute_transaction, get_item, put_item, query, scan,
    transact_get_items, transact_write_items, update_item, update_table,
};
use dynoxide::storage::Storage;
use dynoxide::types::{ConsumedCapacity, Item, item_size};
use serde_json::{Value, json};

fn vector_index(name: &str, attr: &str, dims: u32, projection: &str) -> Value {
    json!({
        "IndexName": name,
        "VectorAttribute": {"AttributeName": attr},
        "Dimensions": dims,
        "DistanceFunction": "COSINE",
        "Projection": {"ProjectionType": projection}
    })
}

async fn create(storage: &Storage, table: &str, vector_indexes: Option<Value>) {
    let mut req = json!({
        "TableName": table,
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "BillingMode": "PAY_PER_REQUEST"
    });
    if let Some(vixs) = vector_indexes {
        req["VectorIndexes"] = vixs;
    }
    create_table::execute(storage, serde_json::from_value(req).unwrap())
        .await
        .unwrap();
}

fn numbers(values: &[&str]) -> Value {
    json!({"L": values.iter().map(|n| json!({"N": n})).collect::<Vec<_>>()})
}

/// An item whose billed size is exactly `billed`: key `pk`, each vector in
/// `vectors` (attribute, dimensions, value), and a string `pad` making up the
/// rest. Returns the item and its size by the number rule, so a test can show
/// the two sizes fall on opposite sides of the boundary it checks.
fn item_billed_at(pk: &str, vectors: &[(&str, u32, Value)], billed: usize) -> (Value, usize) {
    let mut item = json!({"pk": {"S": pk}});
    let mut fixed = "pk".len() + pk.len() + "pad".len();
    for (attr, dims, value) in vectors {
        item[*attr] = value.clone();
        fixed += attr.len() + 4 * *dims as usize;
    }
    item["pad"] = json!({"S": "x".repeat(billed - fixed)});
    let parsed: Item = serde_json::from_value(item.clone()).unwrap();
    (item, item_size(&parsed))
}

fn table_units(cc: &ConsumedCapacity) -> f64 {
    cc.capacity_units
}

async fn put(storage: &Storage, table: &str, item: Value) -> f64 {
    let req = json!({"TableName": table, "Item": item, "ReturnConsumedCapacity": "TOTAL"});
    let resp = put_item::execute(storage, serde_json::from_value(req).unwrap())
        .await
        .unwrap();
    table_units(&resp.consumed_capacity.unwrap())
}

const EIGHTHS: [&str; 3] = ["0.125", "0.125", "0.125"];
const SEVEN_DIGITS: [&str; 8] = [
    "0.1234567",
    "0.2345678",
    "0.3456789",
    "0.4567891",
    "0.5678912",
    "0.6789123",
    "0.7891234",
    "0.8912345",
];

/// PutItem at billed 1,024 is one unit and at 1,025 two, for a three- and an
/// eight-dimension vector whose number-rule sizes sit above the boundary.
#[tokio::test(flavor = "current_thread")]
async fn put_item_bills_a_vector_at_four_bytes_per_dimension() {
    let storage = Storage::memory().unwrap();
    create(
        &storage,
        "VecPut",
        Some(json!([
            vector_index("vix3", "e3", 3, "KEYS_ONLY"),
            vector_index("vix8", "e8", 8, "KEYS_ONLY")
        ])),
    )
    .await;

    for (attr, dims, value) in [
        ("e3", 3, numbers(&EIGHTHS)),
        ("e8", 8, numbers(&SEVEN_DIGITS)),
    ] {
        let (at, number_rule) = item_billed_at("a", &[(attr, dims, value.clone())], 1024);
        assert!(
            number_rule > 1024,
            "{attr}: the number rule ({number_rule}) must cross the boundary for this to test anything"
        );
        assert_eq!(
            put(&storage, "VecPut", at).await,
            1.0,
            "{attr} billed at 1,024"
        );

        let (over, _) = item_billed_at("b", &[(attr, dims, value)], 1025);
        assert_eq!(
            put(&storage, "VecPut", over).await,
            2.0,
            "{attr} billed at 1,025"
        );
    }
}

/// Two indexes on different attributes each charge their own dimensions.
#[tokio::test(flavor = "current_thread")]
async fn two_vector_attributes_are_each_billed_at_their_own_dimensions() {
    let storage = Storage::memory().unwrap();
    create(
        &storage,
        "VecTwo",
        Some(json!([
            vector_index("vix3", "e3", 3, "KEYS_ONLY"),
            vector_index("vix8", "e8", 8, "ALL")
        ])),
    )
    .await;
    let vectors = [
        ("e3", 3, numbers(&EIGHTHS)),
        ("e8", 8, numbers(&SEVEN_DIGITS)),
    ];

    let (at, number_rule) = item_billed_at("a", &vectors, 1024);
    assert!(number_rule > 1024);
    assert_eq!(put(&storage, "VecTwo", at).await, 1.0);
    let (over, _) = item_billed_at("b", &vectors, 1025);
    assert_eq!(put(&storage, "VecTwo", over).await, 2.0);
}

/// BatchWriteItem, TransactWriteItems, UpdateItem, PartiQL INSERT and
/// DeleteItem charge the billed size too. The first three were captured at
/// billed 3,072 (3, 6 and 3 units); DeleteItem and PartiQL follow the same
/// write rule.
#[tokio::test(flavor = "current_thread")]
async fn every_write_path_charges_the_billed_size() {
    let storage = Storage::memory().unwrap();
    create(
        &storage,
        "VecWrites",
        Some(json!([vector_index("vix3", "e3", 3, "KEYS_ONLY")])),
    )
    .await;
    let vector = || vec![("e3", 3, numbers(&EIGHTHS))];

    let (batch_item, number_rule) = item_billed_at("batch", &vector(), 1024);
    assert!(number_rule > 1024);
    let req = json!({
        "RequestItems": {"VecWrites": [{"PutRequest": {"Item": batch_item}}]},
        "ReturnConsumedCapacity": "TOTAL"
    });
    let resp = batch_write_item::execute(&storage, serde_json::from_value(req).unwrap())
        .await
        .unwrap();
    assert_eq!(
        resp.consumed_capacity.unwrap()[0].capacity_units,
        1.0,
        "batch"
    );

    let (tx_item, _) = item_billed_at("tx", &vector(), 1024);
    let req = json!({
        "TransactItems": [{"Put": {"TableName": "VecWrites", "Item": tx_item}}],
        "ReturnConsumedCapacity": "TOTAL"
    });
    let resp = transact_write_items::execute(&storage, serde_json::from_value(req).unwrap())
        .await
        .unwrap();
    assert_eq!(
        resp.consumed_capacity.unwrap()[0].capacity_units,
        2.0,
        "transaction"
    );

    // UpdateItem setting another attribute, its after-image at billed 1,024.
    let (mut base, _) = item_billed_at("upd", &vector(), 1024);
    let pad = base["pad"]["S"].as_str().unwrap().to_string();
    base["pad"] = json!({"S": "x"});
    put(&storage, "VecWrites", base).await;
    let req = json!({
        "TableName": "VecWrites",
        "Key": {"pk": {"S": "upd"}},
        "UpdateExpression": "SET #p = :p",
        "ExpressionAttributeNames": {"#p": "pad"},
        "ExpressionAttributeValues": {":p": {"S": pad}},
        "ReturnConsumedCapacity": "TOTAL"
    });
    let resp = update_item::execute(&storage, serde_json::from_value(req).unwrap())
        .await
        .unwrap();
    assert_eq!(
        resp.consumed_capacity.unwrap().capacity_units,
        1.0,
        "update"
    );

    let (partiql_item, _) = item_billed_at("pq", &vector(), 1024);
    let req = json!({
        "Statement": "INSERT INTO \"VecWrites\" VALUE {'pk': ?, 'e3': ?, 'pad': ?}",
        "Parameters": [partiql_item["pk"], partiql_item["e3"], partiql_item["pad"]],
        "ReturnConsumedCapacity": "TOTAL"
    });
    let resp = execute_statement::execute(&storage, serde_json::from_value(req).unwrap())
        .await
        .unwrap();
    assert_eq!(
        resp.consumed_capacity.unwrap().capacity_units,
        1.0,
        "PartiQL"
    );

    let req = json!({
        "TableName": "VecWrites",
        "Key": {"pk": {"S": "batch"}},
        "ReturnConsumedCapacity": "TOTAL"
    });
    let resp = delete_item::execute(&storage, serde_json::from_value(req).unwrap())
        .await
        .unwrap();
    assert_eq!(
        resp.consumed_capacity.unwrap().capacity_units,
        1.0,
        "delete"
    );
}

/// The four items read back from DynamoDB in eu-west-2: two carry a
/// three-dimension vector, two don't.
fn captured_items() -> Vec<Value> {
    vec![
        json!({"pk": {"S": "v1"}, "data": {"S": "x".repeat(100)}, "embedding": numbers(&["0.5", "0.25", "0.125"])}),
        json!({"pk": {"S": "v2"}, "data": {"S": "y".repeat(200)}, "embedding": numbers(&["1", "2", "3"])}),
        json!({"pk": {"S": "v3"}, "data": {"S": "z".repeat(300)}}),
        json!({"pk": {"S": "v4"}, "data": {"S": "w".repeat(400)}}),
    ]
}

async fn describe(storage: &Storage, table: &str) -> dynoxide::actions::TableDescription {
    describe_table::execute(
        storage,
        describe_table::DescribeTableRequest {
            table_name: table.to_string(),
        },
        &VectorIndexLifecycle::new(),
    )
    .await
    .unwrap()
    .table
}

/// TableSizeBytes and each vector index's figures for the captured items:
/// 1,074 for the table (1,075 by the number rule), 2 entries and 358 bytes for
/// the ALL index, 2 and 50 for the KEYS_ONLY one.
#[tokio::test(flavor = "current_thread")]
async fn describe_reports_the_captured_table_and_vector_index_figures() {
    let storage = Storage::memory().unwrap();
    create(
        &storage,
        "Cap",
        Some(json!([
            vector_index("vAll", "embedding", 3, "ALL"),
            vector_index("vKeys", "embedding", 3, "KEYS_ONLY")
        ])),
    )
    .await;
    for item in captured_items() {
        put(&storage, "Cap", item).await;
    }

    let table = describe(&storage, "Cap").await;
    assert_eq!(table.item_count, Some(4));
    assert_eq!(table.table_size_bytes, Some(1074));

    let figures: Vec<(String, Option<i64>, Option<i64>)> = table
        .vector_indexes
        .unwrap()
        .into_iter()
        .map(|v| (v.index_name, v.item_count, v.index_size_bytes))
        .collect();
    assert!(
        figures.contains(&("vAll".to_string(), Some(2), Some(358))),
        "{figures:?}"
    );
    assert!(
        figures.contains(&("vKeys".to_string(), Some(2), Some(50))),
        "{figures:?}"
    );
}

/// A vector index added to a populated table changes what its items are billed
/// at, and deleting it changes them back.
#[tokio::test(flavor = "current_thread")]
async fn creating_and_deleting_a_vector_index_resizes_the_table() {
    let storage = Storage::memory().unwrap();
    create(&storage, "Grow", None).await;
    for item in captured_items() {
        put(&storage, "Grow", item).await;
    }
    assert_eq!(
        describe(&storage, "Grow").await.table_size_bytes,
        Some(1075)
    );

    let update = |body: Value| async {
        update_table::execute(
            &storage,
            serde_json::from_value(body).unwrap(),
            &VectorIndexLifecycle::new(),
        )
        .await
        .unwrap();
    };
    update(json!({
        "TableName": "Grow",
        "VectorIndexUpdates": [{"Create": vector_index("vAll", "embedding", 3, "ALL")}]
    }))
    .await;
    assert_eq!(
        describe(&storage, "Grow").await.table_size_bytes,
        Some(1074)
    );

    update(json!({
        "TableName": "Grow",
        "VectorIndexUpdates": [{"Delete": {"IndexName": "vAll"}}]
    }))
    .await;
    assert_eq!(
        describe(&storage, "Grow").await.table_size_bytes,
        Some(1075)
    );
}

/// A database written before vector attributes were billed holds number-rule
/// sizes. Opening it recomputes the rows of every table with a vector index,
/// once, and leaves other tables alone.
#[tokio::test(flavor = "current_thread")]
async fn opening_an_older_database_recomputes_vector_table_sizes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("older.db");
    let path = path.to_str().unwrap();
    {
        let storage = Storage::new(path).unwrap();
        create(
            &storage,
            "Cap",
            Some(json!([vector_index("vAll", "embedding", 3, "ALL")])),
        )
        .await;
        create(&storage, "Plain", None).await;
        for item in captured_items() {
            put(&storage, "Cap", item.clone()).await;
            put(&storage, "Plain", item).await;
        }
        // Put the rows back as an earlier version stored them: the number-rule
        // size, and the schema version from before this change.
        for item in captured_items() {
            let parsed: Item = serde_json::from_value(item.clone()).unwrap();
            storage
                .conn()
                .execute(
                    "UPDATE \"Cap\" SET item_size = ?1 WHERE pk = ?2",
                    rusqlite::params![
                        item_size(&parsed) as i64,
                        format!("S:{}", item["pk"]["S"].as_str().unwrap())
                    ],
                )
                .unwrap();
        }
        storage
            .conn()
            .execute(
                "UPDATE _config SET value = '10' WHERE key = 'schema_version'",
                [],
            )
            .unwrap();
        assert_eq!(describe(&storage, "Cap").await.table_size_bytes, Some(1075));
    }

    let storage = Storage::new(path).unwrap();
    assert_eq!(describe(&storage, "Cap").await.table_size_bytes, Some(1074));
    assert_eq!(
        describe(&storage, "Plain").await.table_size_bytes,
        Some(1075)
    );
}

/// Every read of the table is charged on the billed size, as writes are: an
/// item billed at 4,096 whose numbers take it over is one strongly consistent
/// unit, not two, on every read path. DynamoDB moved reads over between
/// 2026-10-09 and 2026-10-22; captured eu-west-2 and us-east-1, 2026-10-23.
#[tokio::test(flavor = "current_thread")]
async fn reads_charge_the_billed_size() {
    let storage = Storage::memory().unwrap();
    create(
        &storage,
        "VecRead",
        Some(json!([vector_index("vix8", "e8", 8, "KEYS_ONLY")])),
    )
    .await;
    let (item, number_rule) = item_billed_at("a", &[("e8", 8, numbers(&SEVEN_DIGITS))], 4096);
    assert!(number_rule > 4096 && number_rule <= 8192);
    put(&storage, "VecRead", item).await;

    // Billed 4,096: one 4 KB unit. By the numbers it would be two.
    let (strong, eventual, transactional) = read_units(&storage, "VecRead").await;
    assert_eq!(strong, ReadUnits::all(1.0), "strongly consistent");
    assert_eq!(eventual, ReadUnits::all(0.5), "eventually consistent");
    assert_eq!(transactional, 2.0, "transactional");
}

/// The control for `reads_charge_the_billed_size`: the same item on a table
/// with no vector index is sized by its numbers, so every read path lands a
/// unit higher. Without it the test above could pass on a fixture that never
/// crossed the boundary.
#[tokio::test(flavor = "current_thread")]
async fn reads_without_a_vector_index_charge_the_stored_size() {
    let storage = Storage::memory().unwrap();
    create(&storage, "PlainRead", None).await;
    let (item, number_rule) = item_billed_at("a", &[("e8", 8, numbers(&SEVEN_DIGITS))], 4096);
    assert!(number_rule > 4096 && number_rule <= 8192);
    put(&storage, "PlainRead", item).await;

    let (strong, eventual, transactional) = read_units(&storage, "PlainRead").await;
    assert_eq!(strong, ReadUnits::all(2.0), "strongly consistent");
    assert_eq!(eventual, ReadUnits::all(1.0), "eventually consistent");
    assert_eq!(transactional, 4.0, "transactional");
}

/// A read is charged on the whole item whatever its projection returns:
/// a 9,006-byte item read for its key alone is three units on GetItem,
/// TransactGetItems (six), BatchGetItem and a PartiQL SELECT of the key.
/// Captured eu-west-2, 2026-10-23.
#[tokio::test(flavor = "current_thread")]
async fn a_projection_does_not_lower_the_read_charge() {
    let storage = Storage::memory().unwrap();
    create(&storage, "ProjRead", None).await;
    put(
        &storage,
        "ProjRead",
        json!({"pk": {"S": "k"}, "pad": {"S": "x".repeat(9000)}}),
    )
    .await;
    let key = json!({"pk": {"S": "k"}});

    let resp = get_item::execute(
        &storage,
        serde_json::from_value(json!({"TableName": "ProjRead", "Key": key,
            "ConsistentRead": true, "ProjectionExpression": "pk",
            "ReturnConsumedCapacity": "TOTAL"}))
        .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(resp.consumed_capacity.unwrap().capacity_units, 3.0);

    let resp = get_item::execute(
        &storage,
        serde_json::from_value(json!({"TableName": "ProjRead", "Key": key,
            "ConsistentRead": true, "AttributesToGet": ["pk"],
            "ReturnConsumedCapacity": "TOTAL"}))
        .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(resp.consumed_capacity.unwrap().capacity_units, 3.0);

    let resp = transact_get_items::execute(
        &storage,
        serde_json::from_value(json!({"TransactItems": [{"Get": {"TableName": "ProjRead",
            "Key": key, "ProjectionExpression": "pk"}}],
            "ReturnConsumedCapacity": "TOTAL"}))
        .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(units_of(&resp), 6.0);

    let resp = batch_get_item::execute(
        &storage,
        serde_json::from_value(json!({"RequestItems": {"ProjRead": {"Keys": [key],
            "ConsistentRead": true, "ProjectionExpression": "pk"}},
            "ReturnConsumedCapacity": "TOTAL"}))
        .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(units_of(&resp), 3.0);
}

/// One read's units on each path, in the order GetItem, BatchGetItem, Query,
/// Scan, ExecuteStatement, BatchExecuteStatement.
#[derive(Debug, PartialEq)]
struct ReadUnits([f64; 6]);

impl ReadUnits {
    fn all(units: f64) -> Self {
        Self([units; 6])
    }
}

/// The table-level `CapacityUnits` a response reports, whether it carries one
/// `ConsumedCapacity` or a list of them with one entry.
fn units_of(resp: &impl serde::Serialize) -> f64 {
    let value = serde_json::to_value(resp).unwrap();
    let cc = &value["ConsumedCapacity"];
    let cc = if cc.is_array() { &cc[0] } else { cc };
    cc["CapacityUnits"]
        .as_f64()
        .unwrap_or_else(|| panic!("no CapacityUnits in {value}"))
}

/// Read item `a` of `table` every way there is: strongly and eventually
/// consistent on each non-transactional path, then transactionally through
/// TransactGetItems and a PartiQL transaction, which must agree.
async fn read_units(storage: &Storage, table: &str) -> (ReadUnits, ReadUnits, f64) {
    let key = json!({"pk": {"S": "a"}});
    let select = format!("SELECT * FROM \"{table}\" WHERE pk = 'a'");
    let mut by_consistency = Vec::new();
    for consistent in [true, false] {
        let get = get_item::execute(
            storage,
            serde_json::from_value(json!({"TableName": table, "Key": key,
                "ConsistentRead": consistent, "ReturnConsumedCapacity": "TOTAL"}))
            .unwrap(),
        )
        .await
        .unwrap();
        let batch_get = batch_get_item::execute(
            storage,
            serde_json::from_value(json!({"RequestItems": {table: {"Keys": [key],
                "ConsistentRead": consistent}}, "ReturnConsumedCapacity": "TOTAL"}))
            .unwrap(),
        )
        .await
        .unwrap();
        let query_resp = query::execute(
            storage,
            serde_json::from_value(json!({"TableName": table,
                "KeyConditionExpression": "pk = :p",
                "ExpressionAttributeValues": {":p": {"S": "a"}},
                "ConsistentRead": consistent, "ReturnConsumedCapacity": "TOTAL"}))
            .unwrap(),
        )
        .await
        .unwrap();
        let scan_resp = scan::execute(
            storage,
            serde_json::from_value(json!({"TableName": table,
                "ConsistentRead": consistent, "ReturnConsumedCapacity": "TOTAL"}))
            .unwrap(),
        )
        .await
        .unwrap();
        let statement = execute_statement::execute(
            storage,
            serde_json::from_value(json!({"Statement": select,
                "ConsistentRead": consistent, "ReturnConsumedCapacity": "TOTAL"}))
            .unwrap(),
        )
        .await
        .unwrap();
        let batch_statement = batch_execute_statement::execute(
            storage,
            serde_json::from_value(json!({"Statements": [{"Statement": select,
                "ConsistentRead": consistent}], "ReturnConsumedCapacity": "TOTAL"}))
            .unwrap(),
        )
        .await
        .unwrap();
        by_consistency.push(ReadUnits([
            units_of(&get),
            units_of(&batch_get),
            units_of(&query_resp),
            units_of(&scan_resp),
            units_of(&statement),
            units_of(&batch_statement),
        ]));
    }

    let transact = transact_get_items::execute(
        storage,
        serde_json::from_value(json!({"TransactItems": [{"Get": {"TableName": table,
            "Key": key}}], "ReturnConsumedCapacity": "TOTAL"}))
        .unwrap(),
    )
    .await
    .unwrap();
    let partiql_transact = execute_transaction::execute(
        storage,
        serde_json::from_value(json!({"TransactStatements": [{"Statement": select}],
            "ReturnConsumedCapacity": "TOTAL"}))
        .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        units_of(&transact),
        units_of(&partiql_transact),
        "TransactGetItems and a PartiQL transaction"
    );

    let eventual = by_consistency.pop().unwrap();
    let strong = by_consistency.pop().unwrap();
    (strong, eventual, units_of(&transact))
}

/// Guard: the 400 KB limit sits on the number rule, so an item billed under
/// the limit is still refused once its numbers take it over.
#[tokio::test(flavor = "current_thread")]
async fn the_item_limit_sizes_the_vector_by_its_numbers() {
    let storage = Storage::memory().unwrap();
    create(
        &storage,
        "VecLimit",
        Some(json!([vector_index("vix8", "e8", 8, "ALL")])),
    )
    .await;
    let vector = numbers(&SEVEN_DIGITS);
    let (_, number_rule) = item_billed_at("a", &[("e8", 8, vector.clone())], 1024);
    let extra = number_rule - 1024;

    // Number rule exactly 409,600: accepted. Billed, it is `extra` under.
    let (at_limit, at) = item_billed_at("a", &[("e8", 8, vector.clone())], 409_600 - extra);
    assert_eq!(at, 409_600);
    let req = json!({"TableName": "VecLimit", "Item": at_limit});
    put_item::execute(&storage, serde_json::from_value(req).unwrap())
        .await
        .unwrap();

    // One byte more by the number rule: refused, although billed it is under.
    let (over, over_size) = item_billed_at("b", &[("e8", 8, vector)], 409_601 - extra);
    assert_eq!(over_size, 409_601);
    let req = json!({"TableName": "VecLimit", "Item": over});
    let err = put_item::execute(&storage, serde_json::from_value(req).unwrap())
        .await
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("Item size has exceeded the maximum allowed size"),
        "{err}"
    );
}

/// On a table with a vector index, a GSI's or LSI's share of a write is sized
/// by the number rule even when the entry carries the vector (captured
/// eu-west-2 and us-east-1, 2026-10-09), so their `IndexSizeBytes` is the
/// number-rule size of the entries while `TableSizeBytes` stays on the billed
/// size. For these three items, as written to the capture table
/// `_capture_20261009_vidx_size2`: 690 for the table, 750 for each ALL index
/// (`3.5` costs 3 bytes, its digits paired on each side of the point).
#[tokio::test(flavor = "current_thread")]
async fn secondary_indexes_on_a_vector_table_size_entries_by_their_numbers() {
    let storage = Storage::memory().unwrap();
    let req = json!({
        "TableName": "Vidx",
        "KeySchema": [
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"}
        ],
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "g", "AttributeType": "S"},
            {"AttributeName": "l", "AttributeType": "S"}
        ],
        "GlobalSecondaryIndexes": [{"IndexName": "gAll",
            "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
            "Projection": {"ProjectionType": "ALL"}}],
        "LocalSecondaryIndexes": [{"IndexName": "lAll",
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "l", "KeyType": "RANGE"}
            ],
            "Projection": {"ProjectionType": "ALL"}}],
        "VectorIndexes": [vector_index("vAll", "embedding", 3, "ALL")],
        "BillingMode": "PAY_PER_REQUEST"
    });
    create_table::execute(&storage, serde_json::from_value(req).unwrap())
        .await
        .unwrap();
    let big = "12345678901234567890123456789012345678";
    for item in [
        json!({"pk": {"S": "a"}, "sk": {"S": "1"}, "g": {"S": "g1"}, "l": {"S": "l1"},
            "embedding": numbers(&[big, big, big]), "data": {"S": "x".repeat(100)}}),
        json!({"pk": {"S": "a"}, "sk": {"S": "2"}, "g": {"S": "g2"}, "l": {"S": "l2"},
            "embedding": numbers(&["0.123456789", "-7", "3.5"]), "data": {"S": "y".repeat(200)}}),
        json!({"pk": {"S": "b"}, "sk": {"S": "1"}, "g": {"S": "g3"}, "l": {"S": "l3"},
            "data": {"S": "z".repeat(300)}}),
    ] {
        put(&storage, "Vidx", item).await;
    }

    let table = describe(&storage, "Vidx").await;
    assert_eq!(table.table_size_bytes, Some(690));
    let gsi = &table.global_secondary_indexes.unwrap()[0];
    assert_eq!((gsi.item_count, gsi.index_size_bytes), (Some(3), Some(750)));
    let lsi = &table.local_secondary_indexes.unwrap()[0];
    assert_eq!((lsi.item_count, lsi.index_size_bytes), (Some(3), Some(750)));
}

/// A table with a vector index on `e8`, an ALL GSI on `g`, an ALL LSI on `l`
/// and a KEYS_ONLY LSI on `k`, holding one item billed at exactly 4,096 bytes
/// whose numbers take it past 4,096. Returns the item's number-rule size.
async fn indexed_vector_table(storage: &Storage) -> usize {
    let req = json!({
        "TableName": "VidxRead",
        "KeySchema": [
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"}
        ],
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "g", "AttributeType": "S"},
            {"AttributeName": "l", "AttributeType": "S"},
            {"AttributeName": "k", "AttributeType": "S"}
        ],
        "GlobalSecondaryIndexes": [{"IndexName": "gAll",
            "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
            "Projection": {"ProjectionType": "ALL"}}],
        "LocalSecondaryIndexes": [
            {"IndexName": "lAll",
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "l", "KeyType": "RANGE"}
                ],
                "Projection": {"ProjectionType": "ALL"}},
            {"IndexName": "lKeys",
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "k", "KeyType": "RANGE"}
                ],
                "Projection": {"ProjectionType": "KEYS_ONLY"}}
        ],
        "VectorIndexes": [vector_index("vix8", "e8", 8, "KEYS_ONLY")],
        "BillingMode": "PAY_PER_REQUEST"
    });
    create_table::execute(storage, serde_json::from_value(req).unwrap())
        .await
        .unwrap();

    let mut item = json!({"pk": {"S": "a"}, "sk": {"S": "1"}, "g": {"S": "g1"},
        "l": {"S": "l1"}, "k": {"S": "k1"}, "e8": numbers(&SEVEN_DIGITS)});
    let without_vector: Item = serde_json::from_value(json!({"pk": {"S": "a"},
        "sk": {"S": "1"}, "g": {"S": "g1"}, "l": {"S": "l1"}, "k": {"S": "k1"}}))
    .unwrap();
    let fixed = item_size(&without_vector) + "e8".len() + 4 * 8 + "pad".len();
    item["pad"] = json!({"S": "x".repeat(4096 - fixed)});
    let parsed: Item = serde_json::from_value(item.clone()).unwrap();
    let number_rule = item_size(&parsed);
    put(storage, "VidxRead", item).await;
    number_rule
}

/// The `ConsumedCapacity` a read reports, as JSON.
fn capacity_json(resp: &impl serde::Serialize) -> Value {
    serde_json::to_value(resp).unwrap()["ConsumedCapacity"].clone()
}

/// The arm a read is charged on, from its `ConsumedCapacity`: `Table`, or an
/// index named under `GlobalSecondaryIndexes` or `LocalSecondaryIndexes`.
fn arm(cc: &Value, index: Option<&str>) -> f64 {
    let node = match index {
        None => &cc["Table"],
        Some(name) if !cc["GlobalSecondaryIndexes"][name].is_null() => {
            &cc["GlobalSecondaryIndexes"][name]
        }
        Some(name) => &cc["LocalSecondaryIndexes"][name],
    };
    node["CapacityUnits"]
        .as_f64()
        .unwrap_or_else(|| panic!("no arm {index:?} in {cc}"))
}

/// A read served from a GSI or LSI is charged on the index entry's
/// number-rule size, as the entry's own write is (captured eu-west-2 and
/// us-east-1, 2026-10-09, see
/// `secondary_indexes_on_a_vector_table_size_entries_by_their_numbers`).
/// Reads from an index were not captured separately; this pins the reading
/// that keeps them consistent with that write sizing. A read of the base item,
/// the LSI fetch behind `Select: ALL_ATTRIBUTES` on a KEYS_ONLY LSI and a
/// PartiQL reach-back, is a table read and charged on the billed size. The
/// item is billed at 4,096 bytes and over 4,096 by its numbers, so each arm
/// lands on one side of a read-unit boundary or the other.
#[tokio::test(flavor = "current_thread")]
async fn index_reads_charge_the_number_rule_and_base_reads_the_billed_size() {
    let storage = Storage::memory().unwrap();
    let number_rule = indexed_vector_table(&storage).await;
    assert!(number_rule > 4096 && number_rule <= 8192, "{number_rule}");

    let query = |req: Value| {
        let storage = &storage;
        async move {
            let resp = query::execute(storage, serde_json::from_value(req).unwrap())
                .await
                .unwrap();
            capacity_json(&resp)
        }
    };
    let by_pk = json!({":p": {"S": "a"}});

    // GSI, eventually consistent: 1 unit by the numbers, 0.5 if billed.
    let cc = query(json!({"TableName": "VidxRead", "IndexName": "gAll",
        "KeyConditionExpression": "g = :g", "ExpressionAttributeValues": {":g": {"S": "g1"}},
        "ReturnConsumedCapacity": "INDEXES"}))
    .await;
    assert_eq!(arm(&cc, Some("gAll")), 1.0, "GSI query: {cc}");

    // LSI, strongly consistent: 2 units by the numbers, 1 if billed.
    let cc = query(json!({"TableName": "VidxRead", "IndexName": "lAll",
        "KeyConditionExpression": "pk = :p", "ExpressionAttributeValues": by_pk,
        "ConsistentRead": true, "ReturnConsumedCapacity": "INDEXES"}))
    .await;
    assert_eq!(arm(&cc, Some("lAll")), 2.0, "LSI query: {cc}");

    // KEYS_ONLY LSI with ALL_ATTRIBUTES: the base fetch is a table read on the
    // billed size (1 unit, 2 by the numbers); the small index entry is 1.
    let cc = query(json!({"TableName": "VidxRead", "IndexName": "lKeys",
        "KeyConditionExpression": "pk = :p", "ExpressionAttributeValues": by_pk,
        "ConsistentRead": true, "Select": "ALL_ATTRIBUTES",
        "ReturnConsumedCapacity": "INDEXES"}))
    .await;
    assert_eq!(arm(&cc, None), 1.0, "LSI base fetch: {cc}");
    assert_eq!(arm(&cc, Some("lKeys")), 1.0, "LSI base fetch: {cc}");

    // Scans of either index charge the numbers too.
    let scan = |req: Value| {
        let storage = &storage;
        async move {
            let resp = scan::execute(storage, serde_json::from_value(req).unwrap())
                .await
                .unwrap();
            capacity_json(&resp)
        }
    };
    let cc = scan(json!({"TableName": "VidxRead", "IndexName": "gAll",
        "ReturnConsumedCapacity": "INDEXES"}))
    .await;
    assert_eq!(arm(&cc, Some("gAll")), 1.0, "GSI scan: {cc}");
    let cc = scan(json!({"TableName": "VidxRead", "IndexName": "lAll",
        "ConsistentRead": true, "ReturnConsumedCapacity": "TOTAL"}))
    .await;
    assert_eq!(cc["CapacityUnits"].as_f64(), Some(2.0), "LSI scan: {cc}");

    // PartiQL: a GSI read charges the numbers; a KEYS_ONLY LSI read naming an
    // unprojected attribute reaches back, and the reach-back is billed.
    let statement = |stmt: &str, consistent: bool| {
        let storage = &storage;
        let req = json!({"Statement": stmt, "ConsistentRead": consistent,
            "ReturnConsumedCapacity": "INDEXES"});
        async move {
            let resp = execute_statement::execute(storage, serde_json::from_value(req).unwrap())
                .await
                .unwrap();
            capacity_json(&resp)
        }
    };
    let cc = statement("SELECT * FROM \"VidxRead\".\"gAll\" WHERE g = 'g1'", false).await;
    assert_eq!(arm(&cc, Some("gAll")), 1.0, "PartiQL GSI read: {cc}");
    let cc = statement(
        "SELECT pad FROM \"VidxRead\".\"lKeys\" WHERE pk = 'a'",
        true,
    )
    .await;
    assert_eq!(arm(&cc, None), 1.0, "PartiQL reach-back: {cc}");
}
