use aws_sdk_dynamodb::types::{AttributeValue, KeysAndAttributes, PutRequest, WriteRequest};
use criterion::{Criterion, criterion_group, criterion_main};
use dynoxide_benchmarks::http::{
    GSI_NAME, TABLE_NAME, create_sdk_table, make_sdk_client, make_sdk_item, make_sdk_key,
};
use tokio::runtime::Runtime;

fn bench_http_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("http_workload");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(60));

    group.bench_function("full_workload_http", |b| {
        let rt = Runtime::new().expect("failed to create tokio runtime");
        b.iter(|| {
            rt.block_on(async {
                // Start fresh server for each iteration
                let db = dynoxide::Database::memory().expect("failed to create database");
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                    .await
                    .expect("failed to bind listener");
                let addr = listener.local_addr().expect("failed to get local addr");

                let server_handle = tokio::spawn(async move {
                    dynoxide::server::serve_on(listener, db).await;
                });

                let client = make_sdk_client(&format!("http://{addr}"));

                // Wait for server to be ready
                loop {
                    if client.list_tables().send().await.is_ok() {
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                }

                run_workload(&client).await;

                // Abort the server task to prevent resource leaks across iterations
                server_handle.abort();
            });
        });
    });

    group.finish();
}

// Simplified workload: 11 steps (omits paginated query and TransactWriteItems
// from the full 13-step standard workload). This keeps per-iteration time
// reasonable for criterion's measurement window.
async fn run_workload(client: &aws_sdk_dynamodb::Client) {
    // Step 1: CreateTable
    create_sdk_table(client, TABLE_NAME).await;

    // Step 2: BatchWriteItem -- 10,000 items
    for batch_start in (0..10_000).step_by(25) {
        let items: Vec<WriteRequest> = (batch_start..batch_start + 25)
            .map(|idx| {
                WriteRequest::builder()
                    .put_request(
                        PutRequest::builder()
                            .set_item(Some(make_sdk_item(idx)))
                            .build()
                            .unwrap(),
                    )
                    .build()
            })
            .collect();

        client
            .batch_write_item()
            .request_items(TABLE_NAME, items)
            .send()
            .await
            .unwrap();
    }

    // Step 3: GetItem -- 1,000
    for i in 0..1_000 {
        client
            .get_item()
            .table_name(TABLE_NAME)
            .set_key(Some(make_sdk_key(i * 10 % 10_000)))
            .send()
            .await
            .unwrap();
    }

    // Step 4: PutItem -- 1,000
    for i in 0..1_000 {
        let mut item = make_sdk_item(50_000 + i);
        item.insert(
            "pk".to_string(),
            AttributeValue::S(format!("mixed#{:06}", i)),
        );
        client
            .put_item()
            .table_name(TABLE_NAME)
            .set_item(Some(item))
            .send()
            .await
            .unwrap();
    }

    // Step 5: Query (base table) -- 100
    for i in 0..100 {
        let pk = format!("user#{:06}", i % 100);
        client
            .query()
            .table_name(TABLE_NAME)
            .key_condition_expression("pk = :pk AND sk BETWEEN :lo AND :hi")
            .expression_attribute_values(":pk", AttributeValue::S(pk))
            .expression_attribute_values(":lo", AttributeValue::N("0".to_string()))
            .expression_attribute_values(":hi", AttributeValue::N("1000".to_string()))
            .filter_expression("age > :age")
            .expression_attribute_values(":age", AttributeValue::N("25".to_string()))
            .send()
            .await
            .unwrap();
    }

    // Step 6: Query (GSI) -- 100
    for i in 0..100 {
        let email = format!("user{}@example.com", i * 100 % 10_000);
        client
            .query()
            .table_name(TABLE_NAME)
            .index_name(GSI_NAME)
            .key_condition_expression("email = :email")
            .expression_attribute_values(":email", AttributeValue::S(email))
            .send()
            .await
            .unwrap();
    }

    // Step 7: Scan
    client
        .scan()
        .table_name(TABLE_NAME)
        .filter_expression("age > :age")
        .expression_attribute_values(":age", AttributeValue::N("70".to_string()))
        .send()
        .await
        .unwrap();

    // Step 8: UpdateItem -- 500 (uses SET age = :val to avoid unbounded list growth)
    for i in 0..500 {
        client
            .update_item()
            .table_name(TABLE_NAME)
            .set_key(Some(make_sdk_key(i % 10_000)))
            .update_expression("SET age = :val")
            .condition_expression("attribute_exists(pk)")
            .expression_attribute_values(":val", AttributeValue::N(format!("{}", 20 + (i % 60))))
            .send()
            .await
            .unwrap();
    }

    // Step 9: BatchGetItem -- 100 batches of 100
    for batch_idx in 0..100 {
        let mut builder = KeysAndAttributes::builder();
        for i in 0..100 {
            builder = builder.keys(make_sdk_key((batch_idx * 100 + i) % 10_000));
        }
        client
            .batch_get_item()
            .request_items(TABLE_NAME, builder.build().unwrap())
            .send()
            .await
            .unwrap();
    }

    // Step 10: DeleteItem -- 500
    for i in 0..500 {
        client
            .delete_item()
            .table_name(TABLE_NAME)
            .set_key(Some(make_sdk_key(i)))
            .send()
            .await
            .unwrap();
    }

    // Step 11: DeleteTable
    client
        .delete_table()
        .table_name(TABLE_NAME)
        .send()
        .await
        .unwrap();
}

criterion_group! {
    name = http_macro;
    config = Criterion::default()
        .sample_size(10)
        .measurement_time(std::time::Duration::from_secs(120));
    targets = bench_http_workload
}

criterion_main!(http_macro);
