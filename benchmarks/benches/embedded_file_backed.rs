use criterion::{Criterion, criterion_group, criterion_main};
use dynoxide::Database;
use dynoxide::actions::batch_get_item::{BatchGetItemRequest, KeysAndAttributes};
use dynoxide::actions::batch_write_item::{BatchWriteItemRequest, PutRequest, WriteRequest};
use dynoxide::actions::delete_item::DeleteItemRequest;
use dynoxide::actions::delete_table::DeleteTableRequest;
use dynoxide::actions::get_item::GetItemRequest;
use dynoxide::actions::put_item::PutItemRequest;
use dynoxide::actions::query::QueryRequest;
use dynoxide::actions::scan::ScanRequest;
use dynoxide::actions::update_item::UpdateItemRequest;
use dynoxide::types::AttributeValue;
use dynoxide_benchmarks::{
    BENCH_GSI, BENCH_TABLE, ItemSize, create_table_request, generate_items, generate_mixed_items,
    make_key,
};
use std::collections::HashMap;
use tempfile::TempDir;

/// Run the full standard workload against a file-backed database.
fn bench_full_workload_file(c: &mut Criterion) {
    let mut group = c.benchmark_group("embedded_file_backed");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(30));

    group.bench_function("full_workload_file", |b| {
        b.iter(|| {
            let tmpdir = TempDir::new().unwrap();
            let db_path = tmpdir.path().join("bench.db");
            run_workload(db_path.to_str().unwrap());
            // tmpdir drops here, cleaning up
        });
    });

    group.finish();
}

// Simplified workload: 11 steps (omits paginated query and TransactWriteItems
// from the full 13-step standard workload in workload_driver).
fn run_workload(db_path: &str) {
    let db = Database::new(db_path).expect("failed to open database");

    // Step 1: CreateTable
    db.create_table(create_table_request(BENCH_TABLE)).unwrap();

    // Step 2: BatchWriteItem -- 10,000 medium items
    let items = generate_items(10_000, ItemSize::Medium);
    for chunk in items.chunks(25) {
        let write_requests: Vec<WriteRequest> = chunk
            .iter()
            .map(|item| WriteRequest {
                put_request: Some(PutRequest { item: item.clone() }),
                delete_request: None,
            })
            .collect();

        let mut request_items = HashMap::new();
        request_items.insert(BENCH_TABLE.to_string(), write_requests);

        db.batch_write_item(BatchWriteItemRequest {
            request_items,
            return_consumed_capacity: None,
            return_item_collection_metrics: None,
        })
        .unwrap();
    }

    // Step 3: GetItem -- 1,000
    for i in 0..1_000 {
        let item = &items[i * 10 % items.len()];
        db.get_item(GetItemRequest {
            table_name: BENCH_TABLE.to_string(),
            key: make_key(item),
            ..Default::default()
        })
        .unwrap();
    }

    // Step 4: PutItem -- 1,000 mixed
    let mixed = generate_mixed_items(1_000);
    for (i, item) in mixed.iter().enumerate() {
        let mut item = item.clone();
        item.insert(
            "pk".to_string(),
            AttributeValue::S(format!("mixed#{:06}", i)),
        );
        db.put_item(PutItemRequest {
            table_name: BENCH_TABLE.to_string(),
            item,
            ..Default::default()
        })
        .unwrap();
    }

    // Step 5: Query (base table) -- 100
    for i in 0..100 {
        let pk = format!("user#{:06}", i % 100);
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
    }

    // Step 6: Query (GSI) -- 100
    for i in 0..100 {
        let email = format!("user{}@example.com", i * 100 % 10_000);
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
    }

    // Step 7: Scan with filter
    {
        let mut eav = HashMap::new();
        eav.insert(":age".to_string(), AttributeValue::N("70".to_string()));

        db.scan(ScanRequest {
            table_name: BENCH_TABLE.to_string(),
            filter_expression: Some("age > :age".to_string()),
            expression_attribute_values: Some(eav),
            ..Default::default()
        })
        .unwrap();
    }

    // Step 8: UpdateItem -- 500
    for i in 0..500 {
        let item = &items[i % items.len()];
        let mut eav = HashMap::new();
        eav.insert(
            ":val".to_string(),
            AttributeValue::L(vec![AttributeValue::N(format!("{i}"))]),
        );

        db.update_item(UpdateItemRequest {
            table_name: BENCH_TABLE.to_string(),
            key: make_key(item),
            update_expression: Some("SET scores = list_append(scores, :val)".to_string()),
            condition_expression: Some("attribute_exists(pk)".to_string()),
            expression_attribute_values: Some(eav),
            ..Default::default()
        })
        .unwrap();
    }

    // Step 9: BatchGetItem -- 100 batches of 100
    for batch_idx in 0..100 {
        let keys: Vec<HashMap<String, AttributeValue>> = (0..100)
            .map(|i| make_key(&items[(batch_idx * 100 + i) % items.len()]))
            .collect();

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
    }

    // Step 10: DeleteItem -- 500
    for i in 0..500 {
        let item = &items[i];
        db.delete_item(DeleteItemRequest {
            table_name: BENCH_TABLE.to_string(),
            key: make_key(item),
            ..Default::default()
        })
        .unwrap();
    }

    // Step 11: DeleteTable
    db.delete_table(DeleteTableRequest {
        table_name: BENCH_TABLE.to_string(),
    })
    .unwrap();
}

criterion_group! {
    name = embedded_file_backed;
    config = Criterion::default()
        .sample_size(10)
        .measurement_time(std::time::Duration::from_secs(60));
    targets = bench_full_workload_file
}

criterion_main!(embedded_file_backed);
