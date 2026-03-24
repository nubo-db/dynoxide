use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use dynoxide::actions::batch_get_item::{BatchGetItemRequest, KeysAndAttributes};
use dynoxide::actions::batch_write_item::{BatchWriteItemRequest, PutRequest, WriteRequest};
use dynoxide::actions::delete_item::DeleteItemRequest;
use dynoxide::actions::get_item::GetItemRequest;
use dynoxide::actions::put_item::PutItemRequest;
use dynoxide::actions::query::QueryRequest;
use dynoxide::actions::scan::ScanRequest;
use dynoxide::actions::transact_write_items::{
    TransactConditionCheck, TransactPut, TransactUpdate, TransactWriteItem,
    TransactWriteItemsRequest,
};
use dynoxide::actions::update_item::UpdateItemRequest;
use dynoxide::types::AttributeValue;
use dynoxide_benchmarks::{
    BENCH_GSI, BENCH_TABLE, ItemSize, generate_item, generate_items, make_key, setup_database,
};
use std::collections::HashMap;

const PRE_POPULATED_COUNT: usize = 1_000;

// ---------------------------------------------------------------------------
// PutItem
// ---------------------------------------------------------------------------

fn bench_put_item(c: &mut Criterion) {
    let mut group = c.benchmark_group("put_item");

    for (label, size) in [
        ("small", ItemSize::Small),
        ("medium", ItemSize::Medium),
        ("large", ItemSize::Large),
    ] {
        group.bench_function(BenchmarkId::new("put_item", label), |b| {
            let db = setup_database(PRE_POPULATED_COUNT, ItemSize::Medium);
            let mut counter = PRE_POPULATED_COUNT;
            b.iter(|| {
                counter += 1;
                // Item is generated outside the hot path of what we're measuring:
                // generate_item is cheap (~200ns) relative to put_item (~10us+),
                // but we need unique keys per iteration so we can't fully pre-generate.
                let item = generate_item(counter, size);
                db.put_item(PutItemRequest {
                    table_name: BENCH_TABLE.to_string(),
                    item,
                    ..Default::default()
                })
                .unwrap();
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// GetItem
// ---------------------------------------------------------------------------

fn bench_get_item(c: &mut Criterion) {
    let db = setup_database(PRE_POPULATED_COUNT, ItemSize::Medium);
    let items = generate_items(PRE_POPULATED_COUNT, ItemSize::Medium);

    c.bench_function("get_item", |b| {
        let mut idx = 0;
        b.iter(|| {
            let item = &items[idx % items.len()];
            let key = make_key(item);
            db.get_item(GetItemRequest {
                table_name: BENCH_TABLE.to_string(),
                key,
                ..Default::default()
            })
            .unwrap();
            idx += 1;
        });
    });
}

// ---------------------------------------------------------------------------
// Query (base table)
// ---------------------------------------------------------------------------

fn bench_query_base_table(c: &mut Criterion) {
    let db = setup_database(PRE_POPULATED_COUNT, ItemSize::Medium);

    c.bench_function("query_base_table", |b| {
        let mut idx = 0;
        b.iter(|| {
            let pk = format!("user#{:06}", idx % 10);
            let mut eav = HashMap::new();
            eav.insert(":pk".to_string(), AttributeValue::S(pk));
            eav.insert(":lo".to_string(), AttributeValue::N("0".to_string()));
            eav.insert(":hi".to_string(), AttributeValue::N("1000".to_string()));
            eav.insert(":age".to_string(), AttributeValue::N("25".to_string()));

            db.query(QueryRequest {
                table_name: BENCH_TABLE.to_string(),
                key_condition_expression: Some("pk = :pk AND sk BETWEEN :lo AND :hi".to_string()),
                filter_expression: Some("age > :age".to_string()),
                expression_attribute_values: Some(eav),
                scan_index_forward: true,
                ..Default::default()
            })
            .unwrap();
            idx += 1;
        });
    });
}

// ---------------------------------------------------------------------------
// Query (GSI)
// ---------------------------------------------------------------------------

fn bench_query_gsi(c: &mut Criterion) {
    let db = setup_database(PRE_POPULATED_COUNT, ItemSize::Medium);

    c.bench_function("query_gsi", |b| {
        let mut idx = 0;
        b.iter(|| {
            let email = format!("user{}@example.com", idx % PRE_POPULATED_COUNT);
            let mut eav = HashMap::new();
            eav.insert(":email".to_string(), AttributeValue::S(email));

            db.query(QueryRequest {
                table_name: BENCH_TABLE.to_string(),
                key_condition_expression: Some("email = :email".to_string()),
                expression_attribute_values: Some(eav),
                scan_index_forward: true,
                index_name: Some(BENCH_GSI.to_string()),
                ..Default::default()
            })
            .unwrap();
            idx += 1;
        });
    });
}

// ---------------------------------------------------------------------------
// Scan with filter
// ---------------------------------------------------------------------------

fn bench_scan_with_filter(c: &mut Criterion) {
    let db = setup_database(PRE_POPULATED_COUNT, ItemSize::Medium);

    c.bench_function("scan_with_filter", |b| {
        b.iter(|| {
            let mut eav = HashMap::new();
            eav.insert(":age".to_string(), AttributeValue::N("70".to_string()));

            db.scan(ScanRequest {
                table_name: BENCH_TABLE.to_string(),
                filter_expression: Some("age > :age".to_string()),
                expression_attribute_values: Some(eav),
                ..Default::default()
            })
            .unwrap();
        });
    });
}

// ---------------------------------------------------------------------------
// UpdateItem
// ---------------------------------------------------------------------------

fn bench_update_item(c: &mut Criterion) {
    // Use iter_batched so each iteration starts with a fresh database.
    // This prevents unbounded list growth from list_append across iterations,
    // matching the plan's workload spec: SET scores = list_append(scores, :val)
    // with attribute_exists(pk) condition.
    c.bench_function("update_item", |b| {
        b.iter_batched(
            || {
                let db = setup_database(PRE_POPULATED_COUNT, ItemSize::Medium);
                let item = generate_item(500, ItemSize::Medium);
                let key = make_key(&item);
                let mut eav = HashMap::new();
                eav.insert(
                    ":val".to_string(),
                    AttributeValue::L(vec![AttributeValue::N("99".to_string())]),
                );
                (db, key, eav)
            },
            |(db, key, eav)| {
                db.update_item(UpdateItemRequest {
                    table_name: BENCH_TABLE.to_string(),
                    key,
                    update_expression: Some("SET scores = list_append(scores, :val)".to_string()),
                    condition_expression: Some("attribute_exists(pk)".to_string()),
                    expression_attribute_values: Some(eav),
                    ..Default::default()
                })
                .unwrap();
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

// ---------------------------------------------------------------------------
// DeleteItem
// ---------------------------------------------------------------------------

fn bench_delete_item(c: &mut Criterion) {
    // Use iter_batched to create a fresh item per iteration, avoiding pool exhaustion.
    // Setup cost (database + insert) is excluded from the measurement.
    c.bench_function("delete_item", |b| {
        b.iter_batched(
            || {
                let db = setup_database(1, ItemSize::Medium);
                let item = generate_item(0, ItemSize::Medium);
                let key = make_key(&item);
                (db, key)
            },
            |(db, key)| {
                db.delete_item(DeleteItemRequest {
                    table_name: BENCH_TABLE.to_string(),
                    key,
                    ..Default::default()
                })
                .unwrap();
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

// ---------------------------------------------------------------------------
// BatchWriteItem (25 items)
// ---------------------------------------------------------------------------

fn bench_batch_write_item(c: &mut Criterion) {
    c.bench_function("batch_write_item_25", |b| {
        let db = setup_database(PRE_POPULATED_COUNT, ItemSize::Medium);
        let mut counter = PRE_POPULATED_COUNT;
        b.iter(|| {
            let batch: Vec<WriteRequest> = (0..25)
                .map(|i| {
                    counter += 1;
                    WriteRequest {
                        put_request: Some(PutRequest {
                            item: generate_item(counter + i, ItemSize::Medium),
                        }),
                        delete_request: None,
                    }
                })
                .collect();

            let mut request_items = HashMap::new();
            request_items.insert(BENCH_TABLE.to_string(), batch);

            db.batch_write_item(BatchWriteItemRequest {
                request_items,
                return_consumed_capacity: None,
                return_item_collection_metrics: None,
            })
            .unwrap();
        });
    });
}

// ---------------------------------------------------------------------------
// BatchGetItem (100 keys)
// ---------------------------------------------------------------------------

fn bench_batch_get_item(c: &mut Criterion) {
    let db = setup_database(PRE_POPULATED_COUNT, ItemSize::Medium);
    let items = generate_items(PRE_POPULATED_COUNT, ItemSize::Medium);

    c.bench_function("batch_get_item_100", |b| {
        let mut offset = 0;
        b.iter(|| {
            let keys: Vec<HashMap<String, AttributeValue>> = (0..100)
                .map(|i| make_key(&items[(offset + i) % items.len()]))
                .collect();
            offset = (offset + 100) % items.len();

            let mut request_items = HashMap::new();
            request_items.insert(
                BENCH_TABLE.to_string(),
                KeysAndAttributes {
                    keys,
                    ..Default::default()
                },
            );

            db.batch_get_item(BatchGetItemRequest {
                request_items,
                return_consumed_capacity: None,
            })
            .unwrap();
        });
    });
}

// ---------------------------------------------------------------------------
// TransactWriteItems (4 actions)
// ---------------------------------------------------------------------------

fn bench_transact_write_items(c: &mut Criterion) {
    let db = setup_database(PRE_POPULATED_COUNT, ItemSize::Medium);

    c.bench_function("transact_write_items_4", |b| {
        let mut counter = PRE_POPULATED_COUNT + 100_000;
        b.iter(|| {
            counter += 10;

            let item1 = generate_item(counter, ItemSize::Medium);
            let item2 = generate_item(counter + 1, ItemSize::Medium);

            // Item to update (must exist)
            let update_key = make_key(&generate_item(
                counter % PRE_POPULATED_COUNT,
                ItemSize::Medium,
            ));
            // Item to condition check (must exist)
            let check_key = make_key(&generate_item(
                (counter + 1) % PRE_POPULATED_COUNT,
                ItemSize::Medium,
            ));

            let mut update_eav = HashMap::new();
            update_eav.insert(":val".to_string(), AttributeValue::N(format!("{counter}")));

            db.transact_write_items(TransactWriteItemsRequest {
                transact_items: vec![
                    TransactWriteItem {
                        put: Some(TransactPut {
                            table_name: BENCH_TABLE.to_string(),
                            item: item1,
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                    TransactWriteItem {
                        put: Some(TransactPut {
                            table_name: BENCH_TABLE.to_string(),
                            item: item2,
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                    TransactWriteItem {
                        update: Some(TransactUpdate {
                            table_name: BENCH_TABLE.to_string(),
                            key: update_key,
                            update_expression: "SET age = :val".to_string(),
                            expression_attribute_values: Some(update_eav),
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                    TransactWriteItem {
                        condition_check: Some(TransactConditionCheck {
                            table_name: BENCH_TABLE.to_string(),
                            key: check_key,
                            condition_expression: "attribute_exists(pk)".to_string(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                ],
                client_request_token: None,
                return_consumed_capacity: None,
                return_item_collection_metrics: None,
            })
            .unwrap();
        });
    });
}

// ---------------------------------------------------------------------------
// Criterion configuration
// ---------------------------------------------------------------------------

criterion_group! {
    name = embedded_micro;
    config = Criterion::default()
        .sample_size(100)
        .measurement_time(std::time::Duration::from_secs(5));
    targets =
        bench_put_item,
        bench_get_item,
        bench_query_base_table,
        bench_query_gsi,
        bench_scan_with_filter,
        bench_update_item,
        bench_delete_item,
        bench_batch_write_item,
        bench_batch_get_item,
        bench_transact_write_items
}

criterion_main!(embedded_micro);
