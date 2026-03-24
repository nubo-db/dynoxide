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
use dynoxide::actions::transact_write_items::{
    TransactConditionCheck, TransactPut, TransactUpdate, TransactWriteItem,
    TransactWriteItemsRequest,
};
use dynoxide::actions::update_item::UpdateItemRequest;
use dynoxide::types::AttributeValue;
use dynoxide_benchmarks::{
    BENCH_GSI, BENCH_TABLE, ItemSize, create_table_request, generate_item, generate_items,
    generate_mixed_items, make_key,
};
use std::collections::HashMap;

/// Run the full standard workload against an in-memory database.
fn bench_full_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("embedded_workload");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(30));

    group.bench_function("full_workload_memory", |b| {
        b.iter(|| run_workload());
    });

    group.finish();
}

fn run_workload() {
    let db = Database::memory().unwrap();

    // Step 1: CreateTable
    db.create_table(create_table_request(BENCH_TABLE)).unwrap();

    // Step 2: BatchWriteItem -- 10,000 medium items in batches of 25
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

    // Step 3: GetItem -- 1,000 random items by primary key
    for i in 0..1_000 {
        let item = &items[i * 10 % items.len()];
        db.get_item(GetItemRequest {
            table_name: BENCH_TABLE.to_string(),
            key: make_key(item),
            ..Default::default()
        })
        .unwrap();
    }

    // Step 4: PutItem -- 1,000 individual items (mixed sizes)
    let mixed = generate_mixed_items(1_000);
    for (i, item) in mixed.iter().enumerate() {
        // Offset keys to avoid overwriting existing items
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

    // Step 5: Query (base table) -- 100 queries
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

    // Step 6: Query (GSI) -- 100 queries
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

    // Step 7: Query (paginated) -- 50 queries following LastEvaluatedKey
    for i in 0..50 {
        let pk = format!("user#{:06}", i % 100);
        let mut last_key = None;
        loop {
            let mut eav = HashMap::new();
            eav.insert(":pk".to_string(), AttributeValue::S(pk.clone()));

            let resp = db
                .query(QueryRequest {
                    table_name: BENCH_TABLE.to_string(),
                    key_condition_expression: Some("pk = :pk".to_string()),
                    expression_attribute_values: Some(eav),
                    scan_index_forward: true,
                    limit: Some(100),
                    exclusive_start_key: last_key,
                    ..Default::default()
                })
                .unwrap();

            last_key = resp.last_evaluated_key;
            if last_key.is_none() {
                break;
            }
        }
    }

    // Step 8: Scan -- full table scan with filter returning ~10%
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

    // Step 9: UpdateItem -- 500 updates with condition expressions
    for i in 0..500 {
        let item = &items[i % items.len()];
        let key = make_key(item);

        let mut eav = HashMap::new();
        eav.insert(
            ":val".to_string(),
            AttributeValue::L(vec![AttributeValue::N(format!("{i}"))]),
        );

        db.update_item(UpdateItemRequest {
            table_name: BENCH_TABLE.to_string(),
            key,
            update_expression: Some("SET scores = list_append(scores, :val)".to_string()),
            condition_expression: Some("attribute_exists(pk)".to_string()),
            expression_attribute_values: Some(eav),
            ..Default::default()
        })
        .unwrap();
    }

    // Step 10: TransactWriteItems -- 50 transactions of 4 actions each
    for i in 0..50 {
        let base = 200_000 + i * 10;
        let item1 = generate_item(base, ItemSize::Medium);
        let item2 = generate_item(base + 1, ItemSize::Medium);
        let update_key = make_key(&items[i % items.len()]);
        let check_key = make_key(&items[(i + 1) % items.len()]);

        let mut update_eav = HashMap::new();
        update_eav.insert(":val".to_string(), AttributeValue::N(format!("{}", base)));

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
    }

    // Step 11: BatchGetItem -- 100 batch reads of 100 keys each
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

    // Step 12: DeleteItem -- 500 items
    for i in 0..500 {
        let item = &items[i];
        db.delete_item(DeleteItemRequest {
            table_name: BENCH_TABLE.to_string(),
            key: make_key(item),
            ..Default::default()
        })
        .unwrap();
    }

    // Step 13: DeleteTable
    db.delete_table(DeleteTableRequest {
        table_name: BENCH_TABLE.to_string(),
    })
    .unwrap();
}

criterion_group! {
    name = embedded_macro;
    config = Criterion::default()
        .sample_size(10)
        .measurement_time(std::time::Duration::from_secs(60));
    targets = bench_full_workload
}

criterion_main!(embedded_macro);
