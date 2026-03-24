use aws_sdk_dynamodb::types::{
    self as ddb_types, AttributeValue, KeysAndAttributes, PutRequest, WriteRequest,
};
use dynoxide_benchmarks::http::{
    GSI_NAME, TABLE_NAME, create_sdk_table, make_sdk_client, make_sdk_item, make_sdk_key,
    warmup_jvm,
};
use dynoxide_benchmarks::stats::percentile;
use serde::Serialize;
use std::time::Instant;

#[derive(Debug, Serialize)]
struct StepResult {
    step: String,
    count: usize,
    total_ms: f64,
    ops_per_sec: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
}

#[derive(Debug, Serialize)]
struct WorkloadResults {
    endpoint: String,
    steps: Vec<StepResult>,
    total_ms: f64,
}

fn step_result(step: &str, count: usize, latencies_ms: &mut [f64]) -> StepResult {
    latencies_ms.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let total_ms: f64 = latencies_ms.iter().sum();
    let ops_per_sec = if total_ms > 0.0 {
        count as f64 / (total_ms / 1000.0)
    } else {
        0.0
    };
    StepResult {
        step: step.to_string(),
        count,
        total_ms,
        ops_per_sec,
        p50_ms: percentile(latencies_ms, 50.0),
        p95_ms: percentile(latencies_ms, 95.0),
        p99_ms: percentile(latencies_ms, 99.0),
    }
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();

    let mut endpoint_url = "http://localhost:8000".to_string();
    let mut output_path: Option<String> = None;
    let mut do_warmup = true;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--endpoint-url" => {
                i += 1;
                endpoint_url = args[i].clone();
            }
            "--output" => {
                i += 1;
                output_path = Some(args[i].clone());
            }
            "--no-warmup" => {
                do_warmup = false;
            }
            _ => {
                eprintln!("Unknown arg: {}", args[i]);
                std::process::exit(1);
            }
        }
        i += 1;
    }

    eprintln!("=== Workload Driver ===");
    eprintln!("Endpoint: {endpoint_url}");

    let client = make_sdk_client(&endpoint_url);
    let overall_start = Instant::now();
    let mut results = Vec::new();

    // Step 0: Warmup (excluded from timing)
    if do_warmup {
        eprintln!("Warmup: creating temp table and running 500 Put + 100 Get...");
        warmup_jvm(&client).await;
        eprintln!("Warmup complete.");
    }

    // Step 1: CreateTable
    {
        let t = Instant::now();
        create_sdk_table(&client, TABLE_NAME).await;
        let ms = t.elapsed().as_secs_f64() * 1000.0;
        results.push(StepResult {
            step: "CreateTable".to_string(),
            count: 1,
            total_ms: ms,
            ops_per_sec: 1000.0 / ms,
            p50_ms: ms,
            p95_ms: ms,
            p99_ms: ms,
        });
        eprintln!("CreateTable: {ms:.1}ms");
    }

    // Step 2: BatchWriteItem -- 10,000 items in batches of 25
    {
        let mut latencies = Vec::new();
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

            let t = Instant::now();
            client
                .batch_write_item()
                .request_items(TABLE_NAME, items)
                .send()
                .await
                .unwrap();
            latencies.push(t.elapsed().as_secs_f64() * 1000.0);
        }
        let r = step_result("BatchWriteItem", 400, &mut latencies);
        eprintln!(
            "BatchWriteItem (400 batches of 25): {:.1}ms total, {:.0} batches/s",
            r.total_ms, r.ops_per_sec
        );
        results.push(r);
    }

    // Step 3: GetItem -- 1,000
    {
        let mut latencies = Vec::new();
        for i in 0..1_000 {
            let key = make_sdk_key(i * 10 % 10_000);
            let t = Instant::now();
            client
                .get_item()
                .table_name(TABLE_NAME)
                .set_key(Some(key))
                .send()
                .await
                .unwrap();
            latencies.push(t.elapsed().as_secs_f64() * 1000.0);
        }
        let r = step_result("GetItem", 1_000, &mut latencies);
        eprintln!(
            "GetItem (1000): p50={:.2}ms p95={:.2}ms p99={:.2}ms",
            r.p50_ms, r.p95_ms, r.p99_ms
        );
        results.push(r);
    }

    // Step 4: PutItem -- 1,000
    {
        let mut latencies = Vec::new();
        for i in 0..1_000 {
            let mut item = make_sdk_item(50_000 + i);
            item.insert(
                "pk".to_string(),
                AttributeValue::S(format!("mixed#{:06}", i)),
            );
            let t = Instant::now();
            client
                .put_item()
                .table_name(TABLE_NAME)
                .set_item(Some(item))
                .send()
                .await
                .unwrap();
            latencies.push(t.elapsed().as_secs_f64() * 1000.0);
        }
        let r = step_result("PutItem", 1_000, &mut latencies);
        eprintln!(
            "PutItem (1000): p50={:.2}ms p95={:.2}ms p99={:.2}ms",
            r.p50_ms, r.p95_ms, r.p99_ms
        );
        results.push(r);
    }

    // Step 5: Query (base table) -- 100
    {
        let mut latencies = Vec::new();
        for i in 0..100 {
            let pk = format!("user#{:06}", i % 100);
            let t = Instant::now();
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
            latencies.push(t.elapsed().as_secs_f64() * 1000.0);
        }
        let r = step_result("Query_Base", 100, &mut latencies);
        eprintln!(
            "Query base (100): p50={:.2}ms p95={:.2}ms p99={:.2}ms",
            r.p50_ms, r.p95_ms, r.p99_ms
        );
        results.push(r);
    }

    // Step 6: Query (GSI) -- 100
    {
        let mut latencies = Vec::new();
        for i in 0..100 {
            let email = format!("user{}@example.com", i * 100 % 10_000);
            let t = Instant::now();
            client
                .query()
                .table_name(TABLE_NAME)
                .index_name(GSI_NAME)
                .key_condition_expression("email = :email")
                .expression_attribute_values(":email", AttributeValue::S(email))
                .send()
                .await
                .unwrap();
            latencies.push(t.elapsed().as_secs_f64() * 1000.0);
        }
        let r = step_result("Query_GSI", 100, &mut latencies);
        eprintln!(
            "Query GSI (100): p50={:.2}ms p95={:.2}ms p99={:.2}ms",
            r.p50_ms, r.p95_ms, r.p99_ms
        );
        results.push(r);
    }

    // Step 7: Query (paginated) -- 50
    {
        let mut latencies = Vec::new();
        for i in 0..50 {
            let pk = format!("user#{:06}", i % 100);
            let t = Instant::now();
            let mut last_key = None;
            loop {
                let mut req = client
                    .query()
                    .table_name(TABLE_NAME)
                    .key_condition_expression("pk = :pk")
                    .expression_attribute_values(":pk", AttributeValue::S(pk.clone()))
                    .limit(100);

                if let Some(key) = last_key {
                    req = req.set_exclusive_start_key(Some(key));
                }

                let resp = req.send().await.unwrap();
                last_key = resp.last_evaluated_key().map(|k| k.to_owned());
                if last_key.is_none() {
                    break;
                }
            }
            latencies.push(t.elapsed().as_secs_f64() * 1000.0);
        }
        let r = step_result("Query_Paginated", 50, &mut latencies);
        eprintln!(
            "Query paginated (50): p50={:.2}ms p95={:.2}ms",
            r.p50_ms, r.p95_ms
        );
        results.push(r);
    }

    // Step 8: Scan with filter
    {
        let t = Instant::now();
        client
            .scan()
            .table_name(TABLE_NAME)
            .filter_expression("age > :age")
            .expression_attribute_values(":age", AttributeValue::N("70".to_string()))
            .send()
            .await
            .unwrap();
        let ms = t.elapsed().as_secs_f64() * 1000.0;
        results.push(StepResult {
            step: "Scan".to_string(),
            count: 1,
            total_ms: ms,
            ops_per_sec: 1000.0 / ms,
            p50_ms: ms,
            p95_ms: ms,
            p99_ms: ms,
        });
        eprintln!("Scan: {ms:.1}ms");
    }

    // Step 9: UpdateItem -- 500
    {
        let mut latencies = Vec::new();
        for i in 0..500 {
            let key = make_sdk_key(i % 10_000);
            let t = Instant::now();
            client
                .update_item()
                .table_name(TABLE_NAME)
                .set_key(Some(key))
                .update_expression("SET scores = list_append(scores, :val)")
                .condition_expression("attribute_exists(pk)")
                .expression_attribute_values(
                    ":val",
                    AttributeValue::L(vec![AttributeValue::N(format!("{i}"))]),
                )
                .send()
                .await
                .unwrap();
            latencies.push(t.elapsed().as_secs_f64() * 1000.0);
        }
        let r = step_result("UpdateItem", 500, &mut latencies);
        eprintln!(
            "UpdateItem (500): p50={:.2}ms p95={:.2}ms p99={:.2}ms",
            r.p50_ms, r.p95_ms, r.p99_ms
        );
        results.push(r);
    }

    // Step 10: TransactWriteItems -- 50
    {
        let mut latencies = Vec::new();
        for i in 0..50 {
            let base = 200_000 + i * 10;
            let item1 = make_sdk_item(base);
            let item2 = make_sdk_item(base + 1);
            let update_key = make_sdk_key(i % 10_000);
            let check_key = make_sdk_key((i + 1) % 10_000);

            let t = Instant::now();
            client
                .transact_write_items()
                .transact_items(
                    ddb_types::TransactWriteItem::builder()
                        .put(
                            ddb_types::Put::builder()
                                .table_name(TABLE_NAME)
                                .set_item(Some(item1))
                                .build()
                                .unwrap(),
                        )
                        .build(),
                )
                .transact_items(
                    ddb_types::TransactWriteItem::builder()
                        .put(
                            ddb_types::Put::builder()
                                .table_name(TABLE_NAME)
                                .set_item(Some(item2))
                                .build()
                                .unwrap(),
                        )
                        .build(),
                )
                .transact_items(
                    ddb_types::TransactWriteItem::builder()
                        .update(
                            ddb_types::Update::builder()
                                .table_name(TABLE_NAME)
                                .set_key(Some(update_key))
                                .update_expression("SET age = :val")
                                .expression_attribute_values(
                                    ":val",
                                    AttributeValue::N(format!("{base}")),
                                )
                                .build()
                                .unwrap(),
                        )
                        .build(),
                )
                .transact_items(
                    ddb_types::TransactWriteItem::builder()
                        .condition_check(
                            ddb_types::ConditionCheck::builder()
                                .table_name(TABLE_NAME)
                                .set_key(Some(check_key))
                                .condition_expression("attribute_exists(pk)")
                                .build()
                                .unwrap(),
                        )
                        .build(),
                )
                .send()
                .await
                .unwrap();
            latencies.push(t.elapsed().as_secs_f64() * 1000.0);
        }
        let r = step_result("TransactWriteItems", 50, &mut latencies);
        eprintln!(
            "TransactWriteItems (50): p50={:.2}ms p95={:.2}ms",
            r.p50_ms, r.p95_ms
        );
        results.push(r);
    }

    // Step 11: BatchGetItem -- 100 batches of 100 keys
    {
        let mut latencies = Vec::new();
        for batch_idx in 0..100 {
            let keys_and_attrs = {
                let mut builder = KeysAndAttributes::builder();
                for i in 0..100 {
                    builder = builder.keys(make_sdk_key((batch_idx * 100 + i) % 10_000));
                }
                builder.build().unwrap()
            };

            let t = Instant::now();
            client
                .batch_get_item()
                .request_items(TABLE_NAME, keys_and_attrs)
                .send()
                .await
                .unwrap();
            latencies.push(t.elapsed().as_secs_f64() * 1000.0);
        }
        let r = step_result("BatchGetItem", 100, &mut latencies);
        eprintln!(
            "BatchGetItem (100 batches of 100): p50={:.2}ms p95={:.2}ms",
            r.p50_ms, r.p95_ms
        );
        results.push(r);
    }

    // Step 12: DeleteItem -- 500
    {
        let mut latencies = Vec::new();
        for i in 0..500 {
            let key = make_sdk_key(i);
            let t = Instant::now();
            client
                .delete_item()
                .table_name(TABLE_NAME)
                .set_key(Some(key))
                .send()
                .await
                .unwrap();
            latencies.push(t.elapsed().as_secs_f64() * 1000.0);
        }
        let r = step_result("DeleteItem", 500, &mut latencies);
        eprintln!(
            "DeleteItem (500): p50={:.2}ms p95={:.2}ms",
            r.p50_ms, r.p95_ms
        );
        results.push(r);
    }

    // Step 13: DeleteTable
    {
        let t = Instant::now();
        client
            .delete_table()
            .table_name(TABLE_NAME)
            .send()
            .await
            .unwrap();
        let ms = t.elapsed().as_secs_f64() * 1000.0;
        results.push(StepResult {
            step: "DeleteTable".to_string(),
            count: 1,
            total_ms: ms,
            ops_per_sec: 1000.0 / ms,
            p50_ms: ms,
            p95_ms: ms,
            p99_ms: ms,
        });
        eprintln!("DeleteTable: {ms:.1}ms");
    }

    let total_ms = overall_start.elapsed().as_secs_f64() * 1000.0;
    eprintln!("\nTotal workload time: {total_ms:.0}ms");

    let output = WorkloadResults {
        endpoint: endpoint_url,
        steps: results,
        total_ms,
    };

    let json = serde_json::to_string_pretty(&output).unwrap();

    if let Some(path) = output_path {
        std::fs::write(&path, &json).unwrap();
        eprintln!("Results written to {path}");
    } else {
        println!("{json}");
    }
}
