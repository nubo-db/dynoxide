use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, GlobalSecondaryIndex, KeySchemaElement, KeyType,
    Projection, ProjectionType, ProvisionedThroughput, PutRequest, ScalarAttributeType,
    WriteRequest,
};
use dynoxide_benchmarks::http::make_sdk_client;
use dynoxide_benchmarks::stats::mean;
use serde::Serialize;
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Number of simulated integration tests.
const NUM_TESTS: usize = 50;
/// Items written per test.
const ITEMS_PER_TEST: usize = 100;
/// Queries per test.
const QUERIES_PER_TEST: usize = 10;
/// Parallelism level for concurrent tests.
const PARALLEL_WORKERS: usize = 4;

#[derive(Debug, Clone, Serialize)]
struct TestResult {
    test_index: usize,
    setup_ms: f64,
    execute_ms: f64,
    teardown_ms: f64,
    total_ms: f64,
}

#[derive(Debug, Clone, Serialize)]
struct ModeResult {
    mode: String,
    execution: String, // "sequential" or "parallel_4"
    num_tests: usize,
    startup_ms: f64,
    total_wall_clock_ms: f64,
    test_execution_ms: f64,
    teardown_ms: f64,
    per_test_avg_ms: f64,
    tests: Vec<TestResult>,
}

#[derive(Debug, Serialize)]
struct AllResults {
    results: Vec<ModeResult>,
    summary: Vec<SummaryRow>,
}

#[derive(Debug, Serialize)]
struct SummaryRow {
    mode: String,
    execution: String,
    wall_clock_ms: f64,
    speedup_vs_ddb_local: Option<f64>,
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();

    let output_path = args
        .iter()
        .position(|a| a == "--output")
        .and_then(|i| args.get(i + 1).cloned());

    let ci_mode = args.iter().any(|a| a == "--ci");

    let ddb_endpoint = args
        .iter()
        .position(|a| a == "--ddb-endpoint")
        .and_then(|i| args.get(i + 1).cloned());

    eprintln!("=== CI Pipeline Benchmark ({NUM_TESTS} simulated tests) ===\n");

    let mut all_results = Vec::new();

    // --- Mode A: Dynoxide Embedded (Sequential) ---
    {
        eprintln!("Mode A: Dynoxide Embedded (sequential)...");
        let result = run_embedded_sequential().await;
        eprintln!(
            "  Total: {:.0}ms  (startup: {:.1}ms, tests: {:.0}ms, teardown: {:.1}ms, per-test avg: {:.1}ms)",
            result.total_wall_clock_ms,
            result.startup_ms,
            result.test_execution_ms,
            result.teardown_ms,
            result.per_test_avg_ms
        );
        all_results.push(result);
    }

    // --- Mode A: Dynoxide Embedded (Parallel) ---
    {
        eprintln!("\nMode A: Dynoxide Embedded (parallel, {PARALLEL_WORKERS} workers)...");
        let result = run_embedded_parallel().await;
        eprintln!(
            "  Total: {:.0}ms  (startup: {:.1}ms, tests: {:.0}ms, teardown: {:.1}ms, per-test avg: {:.1}ms)",
            result.total_wall_clock_ms,
            result.startup_ms,
            result.test_execution_ms,
            result.teardown_ms,
            result.per_test_avg_ms
        );
        all_results.push(result);
    }

    // --- Mode B: Dynoxide HTTP (Sequential) ---
    {
        eprintln!("\nMode B: Dynoxide HTTP (sequential)...");
        let result = run_http_mode("dynoxide_http", None).await;
        eprintln!(
            "  Total: {:.0}ms  (startup: {:.1}ms, tests: {:.0}ms, teardown: {:.1}ms, per-test avg: {:.1}ms)",
            result.total_wall_clock_ms,
            result.startup_ms,
            result.test_execution_ms,
            result.teardown_ms,
            result.per_test_avg_ms
        );
        all_results.push(result);
    }

    // --- Mode B: Dynoxide HTTP (Parallel) ---
    {
        eprintln!("\nMode B: Dynoxide HTTP (parallel, {PARALLEL_WORKERS} workers)...");
        let result = run_http_mode_parallel("dynoxide_http", None).await;
        eprintln!(
            "  Total: {:.0}ms  (startup: {:.1}ms, tests: {:.0}ms, teardown: {:.1}ms, per-test avg: {:.1}ms)",
            result.total_wall_clock_ms,
            result.startup_ms,
            result.test_execution_ms,
            result.teardown_ms,
            result.per_test_avg_ms
        );
        all_results.push(result);
    }

    // --- Mode C: DynamoDB Local (Sequential, if available) ---
    if let Some(ref endpoint) = ddb_endpoint {
        eprintln!("\nMode C: DynamoDB Local (sequential) at {endpoint}...");
        let result = run_http_mode("dynamodb_local", Some(endpoint.clone())).await;
        eprintln!(
            "  Total: {:.0}ms  (startup: {:.1}ms, tests: {:.0}ms, teardown: {:.1}ms, per-test avg: {:.1}ms)",
            result.total_wall_clock_ms,
            result.startup_ms,
            result.test_execution_ms,
            result.teardown_ms,
            result.per_test_avg_ms
        );
        all_results.push(result);

        // Parallel
        eprintln!(
            "\nMode C: DynamoDB Local (parallel, {PARALLEL_WORKERS} workers) at {endpoint}..."
        );
        let result = run_http_mode_parallel("dynamodb_local", Some(endpoint.clone())).await;
        eprintln!(
            "  Total: {:.0}ms  (startup: {:.1}ms, tests: {:.0}ms, teardown: {:.1}ms, per-test avg: {:.1}ms)",
            result.total_wall_clock_ms,
            result.startup_ms,
            result.test_execution_ms,
            result.teardown_ms,
            result.per_test_avg_ms
        );
        all_results.push(result);
    } else {
        eprintln!(
            "\nSkipping DynamoDB Local (pass --ddb-endpoint http://localhost:8000 to include)"
        );
    }

    // Build summary
    let ddb_seq = all_results
        .iter()
        .find(|r| r.mode == "dynamodb_local" && r.execution == "sequential")
        .map(|r| r.total_wall_clock_ms);
    let ddb_par = all_results
        .iter()
        .find(|r| r.mode == "dynamodb_local" && r.execution.starts_with("parallel"))
        .map(|r| r.total_wall_clock_ms);

    let summary: Vec<SummaryRow> = all_results
        .iter()
        .map(|r| {
            let baseline = if r.execution == "sequential" {
                ddb_seq
            } else {
                ddb_par
            };
            SummaryRow {
                mode: r.mode.clone(),
                execution: r.execution.clone(),
                wall_clock_ms: r.total_wall_clock_ms,
                speedup_vs_ddb_local: baseline.map(|b| b / r.total_wall_clock_ms),
            }
        })
        .collect();

    // Print summary table
    eprintln!("\n=== Summary ===");
    eprintln!(
        "{:<25} {:<15} {:>12} {:>12}",
        "Mode", "Execution", "Wall Clock", "vs DDB Local"
    );
    eprintln!("{}", "-".repeat(68));
    for row in &summary {
        let speedup = row
            .speedup_vs_ddb_local
            .map(|s| format!("{:.1}x", s))
            .unwrap_or_else(|| "baseline".to_string());
        eprintln!(
            "{:<25} {:<15} {:>9.0}ms {:>12}",
            row.mode, row.execution, row.wall_clock_ms, speedup
        );
    }

    let output = AllResults {
        results: all_results,
        summary,
    };

    if ci_mode {
        // Machine-readable ratio output for CI
        let ratios: HashMap<String, f64> = output
            .summary
            .iter()
            .filter_map(|s| {
                s.speedup_vs_ddb_local
                    .map(|v| (format!("{}_{}", s.mode, s.execution), v))
            })
            .collect();
        let json = serde_json::to_string_pretty(&ratios).unwrap();
        println!("{json}");
    } else {
        let json = serde_json::to_string_pretty(&output).unwrap();
        if let Some(path) = output_path {
            std::fs::write(&path, &json).unwrap();
            eprintln!("\nResults written to {path}");
        } else {
            println!("{json}");
        }
    }
}

// ---------------------------------------------------------------------------
// Mode A: Dynoxide Embedded
// ---------------------------------------------------------------------------

async fn run_embedded_sequential() -> ModeResult {
    let wall_start = Instant::now();

    // No startup cost for embedded mode
    let startup_ms = 0.0;

    let mut tests = Vec::new();
    for test_idx in 0..NUM_TESTS {
        let result = run_single_embedded_test(test_idx);
        tests.push(result);
    }

    let total_wall_clock_ms = wall_start.elapsed().as_secs_f64() * 1000.0;
    let test_execution_ms: f64 = tests.iter().map(|t| t.execute_ms).sum();
    let teardown_ms: f64 = tests.iter().map(|t| t.teardown_ms).sum();
    let per_test_avg_ms = mean(&tests.iter().map(|t| t.total_ms).collect::<Vec<_>>());

    ModeResult {
        mode: "dynoxide_embedded".to_string(),
        execution: "sequential".to_string(),
        num_tests: NUM_TESTS,
        startup_ms,
        total_wall_clock_ms,
        test_execution_ms,
        teardown_ms,
        per_test_avg_ms,
        tests,
    }
}

async fn run_embedded_parallel() -> ModeResult {
    let wall_start = Instant::now();
    let startup_ms = 0.0;

    // Split tests across workers
    let mut handles = Vec::new();
    let tests_per_worker = NUM_TESTS / PARALLEL_WORKERS;

    for worker in 0..PARALLEL_WORKERS {
        let start_idx = worker * tests_per_worker;
        let end_idx = if worker == PARALLEL_WORKERS - 1 {
            NUM_TESTS
        } else {
            start_idx + tests_per_worker
        };

        let handle = tokio::task::spawn_blocking(move || {
            let mut results = Vec::new();
            for test_idx in start_idx..end_idx {
                results.push(run_single_embedded_test(test_idx));
            }
            results
        });
        handles.push(handle);
    }

    let mut tests = Vec::new();
    for handle in handles {
        tests.extend(handle.await.unwrap());
    }

    let total_wall_clock_ms = wall_start.elapsed().as_secs_f64() * 1000.0;
    let test_execution_ms: f64 = tests.iter().map(|t| t.execute_ms).sum();
    let teardown_ms: f64 = tests.iter().map(|t| t.teardown_ms).sum();
    let per_test_avg_ms = mean(&tests.iter().map(|t| t.total_ms).collect::<Vec<_>>());

    ModeResult {
        mode: "dynoxide_embedded".to_string(),
        execution: format!("parallel_{PARALLEL_WORKERS}"),
        num_tests: NUM_TESTS,
        startup_ms,
        total_wall_clock_ms,
        test_execution_ms,
        teardown_ms,
        per_test_avg_ms,
        tests,
    }
}

fn run_single_embedded_test(test_idx: usize) -> TestResult {
    use dynoxide::actions::batch_write_item::{BatchWriteItemRequest, PutRequest, WriteRequest};
    use dynoxide::actions::delete_table::DeleteTableRequest;
    use dynoxide::actions::query::QueryRequest;
    use dynoxide::types::AttributeValue;

    let total_start = Instant::now();

    // Setup: create isolated in-memory database + table
    let setup_start = Instant::now();
    let db = dynoxide::Database::memory().unwrap();
    let table_name = format!("test_{test_idx}");

    db.create_table(dynoxide_benchmarks::create_table_request(&table_name))
        .unwrap();
    let setup_ms = setup_start.elapsed().as_secs_f64() * 1000.0;

    // Execute: write items + run queries
    let exec_start = Instant::now();

    // Write ITEMS_PER_TEST items in batches of 25
    let items: Vec<HashMap<String, AttributeValue>> = (0..ITEMS_PER_TEST)
        .map(|i| {
            dynoxide_benchmarks::generate_item(
                test_idx * ITEMS_PER_TEST + i,
                dynoxide_benchmarks::ItemSize::Medium,
            )
        })
        .collect();

    for chunk in items.chunks(25) {
        let write_requests: Vec<WriteRequest> = chunk
            .iter()
            .map(|item| WriteRequest {
                put_request: Some(PutRequest { item: item.clone() }),
                delete_request: None,
            })
            .collect();

        let mut request_items = HashMap::new();
        request_items.insert(table_name.clone(), write_requests);

        db.batch_write_item(BatchWriteItemRequest {
            request_items,
            return_consumed_capacity: None,
            return_item_collection_metrics: None,
        })
        .unwrap();
    }

    // Run QUERIES_PER_TEST queries
    for q in 0..QUERIES_PER_TEST {
        let pk = format!("user#{:06}", (test_idx * ITEMS_PER_TEST + q * 10) / 100);
        let mut eav = HashMap::new();
        eav.insert(":pk".to_string(), AttributeValue::S(pk));

        db.query(QueryRequest {
            table_name: table_name.clone(),
            key_condition_expression: Some("pk = :pk".to_string()),
            expression_attribute_values: Some(eav),
            scan_index_forward: true,
            ..Default::default()
        })
        .unwrap();
    }

    let execute_ms = exec_start.elapsed().as_secs_f64() * 1000.0;

    // Teardown: embedded mode just drops the database -- zero cost
    let teardown_start = Instant::now();
    db.delete_table(DeleteTableRequest {
        table_name: table_name.clone(),
    })
    .unwrap();
    drop(db);
    let teardown_ms = teardown_start.elapsed().as_secs_f64() * 1000.0;

    let total_ms = total_start.elapsed().as_secs_f64() * 1000.0;

    TestResult {
        test_index: test_idx,
        setup_ms,
        execute_ms,
        teardown_ms,
        total_ms,
    }
}

// ---------------------------------------------------------------------------
// Mode B & C: HTTP modes (Dynoxide HTTP or DynamoDB Local)
// ---------------------------------------------------------------------------

/// Start Dynoxide HTTP server in-process, returning its endpoint URL.
async fn start_dynoxide_http() -> (String, tokio::task::JoinHandle<()>) {
    let db = dynoxide::Database::memory().unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let endpoint = format!("http://{addr}");

    let handle = tokio::spawn(async move {
        dynoxide::server::serve_on(listener, db).await;
    });

    // Wait until ready
    let client = make_sdk_client(&endpoint);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if Instant::now() > deadline {
            panic!("Dynoxide HTTP server failed to start within 10s");
        }
        if client.list_tables().send().await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    (endpoint, handle)
}

async fn run_http_mode(mode_name: &str, external_endpoint: Option<String>) -> ModeResult {
    let wall_start = Instant::now();

    // Startup
    let startup_start = Instant::now();
    let (endpoint, _server_handle) = if let Some(ep) = external_endpoint {
        (ep, tokio::spawn(async {}))
    } else {
        start_dynoxide_http().await
    };
    let startup_ms = startup_start.elapsed().as_secs_f64() * 1000.0;

    let client = make_sdk_client(&endpoint);

    // Run tests sequentially
    let mut tests = Vec::new();
    for test_idx in 0..NUM_TESTS {
        let result = run_single_http_test(&client, test_idx).await;
        tests.push(result);
    }

    let total_wall_clock_ms = wall_start.elapsed().as_secs_f64() * 1000.0;
    let test_execution_ms: f64 = tests.iter().map(|t| t.execute_ms).sum();
    let teardown_ms: f64 = tests.iter().map(|t| t.teardown_ms).sum();
    let per_test_avg_ms = mean(&tests.iter().map(|t| t.total_ms).collect::<Vec<_>>());

    ModeResult {
        mode: mode_name.to_string(),
        execution: "sequential".to_string(),
        num_tests: NUM_TESTS,
        startup_ms,
        total_wall_clock_ms,
        test_execution_ms,
        teardown_ms,
        per_test_avg_ms,
        tests,
    }
}

async fn run_http_mode_parallel(mode_name: &str, external_endpoint: Option<String>) -> ModeResult {
    let wall_start = Instant::now();

    // Startup
    let startup_start = Instant::now();
    let (endpoint, _server_handle) = if let Some(ep) = external_endpoint {
        (ep, tokio::spawn(async {}))
    } else {
        start_dynoxide_http().await
    };
    let startup_ms = startup_start.elapsed().as_secs_f64() * 1000.0;

    // Split tests across workers
    let tests_per_worker = NUM_TESTS / PARALLEL_WORKERS;
    let mut handles = Vec::new();

    for worker in 0..PARALLEL_WORKERS {
        let start_idx = worker * tests_per_worker;
        let end_idx = if worker == PARALLEL_WORKERS - 1 {
            NUM_TESTS
        } else {
            start_idx + tests_per_worker
        };
        let client = make_sdk_client(&endpoint);

        let handle = tokio::spawn(async move {
            let mut results = Vec::new();
            for test_idx in start_idx..end_idx {
                results.push(run_single_http_test(&client, test_idx).await);
            }
            results
        });
        handles.push(handle);
    }

    let mut tests = Vec::new();
    for handle in handles {
        tests.extend(handle.await.unwrap());
    }

    let total_wall_clock_ms = wall_start.elapsed().as_secs_f64() * 1000.0;
    let test_execution_ms: f64 = tests.iter().map(|t| t.execute_ms).sum();
    let teardown_ms: f64 = tests.iter().map(|t| t.teardown_ms).sum();
    let per_test_avg_ms = mean(&tests.iter().map(|t| t.total_ms).collect::<Vec<_>>());

    ModeResult {
        mode: mode_name.to_string(),
        execution: format!("parallel_{PARALLEL_WORKERS}"),
        num_tests: NUM_TESTS,
        startup_ms,
        total_wall_clock_ms,
        test_execution_ms,
        teardown_ms,
        per_test_avg_ms,
        tests,
    }
}

async fn run_single_http_test(client: &Client, test_idx: usize) -> TestResult {
    let total_start = Instant::now();

    // Setup: create table with unique prefix
    let setup_start = Instant::now();
    let table_name = format!("ci_test_{test_idx}");

    client
        .create_table()
        .table_name(&table_name)
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("sk")
                .key_type(KeyType::Range)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("sk")
                .attribute_type(ScalarAttributeType::N)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("email")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .global_secondary_indexes(
            GlobalSecondaryIndex::builder()
                .index_name("EmailIndex")
                .key_schema(
                    KeySchemaElement::builder()
                        .attribute_name("email")
                        .key_type(KeyType::Hash)
                        .build()
                        .unwrap(),
                )
                .projection(
                    Projection::builder()
                        .projection_type(ProjectionType::All)
                        .build(),
                )
                .provisioned_throughput(
                    ProvisionedThroughput::builder()
                        .read_capacity_units(100)
                        .write_capacity_units(100)
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .provisioned_throughput(
            ProvisionedThroughput::builder()
                .read_capacity_units(100)
                .write_capacity_units(100)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let setup_ms = setup_start.elapsed().as_secs_f64() * 1000.0;

    // Execute: write items + run queries
    let exec_start = Instant::now();

    // Write ITEMS_PER_TEST items using BatchWriteItem (4 batches of 25)
    for batch_start in (0..ITEMS_PER_TEST).step_by(25) {
        let batch_end = (batch_start + 25).min(ITEMS_PER_TEST);
        let items: Vec<WriteRequest> = (batch_start..batch_end)
            .map(|i| {
                let idx = test_idx * ITEMS_PER_TEST + i;
                let pk = format!("user#{:06}", idx / 100);
                let sk = format!("{idx}");
                let mut item = HashMap::new();
                item.insert("pk".to_string(), AttributeValue::S(pk));
                item.insert("sk".to_string(), AttributeValue::N(sk));
                item.insert("name".to_string(), AttributeValue::S(format!("User {idx}")));
                item.insert(
                    "email".to_string(),
                    AttributeValue::S(format!("user{idx}@example.com")),
                );
                item.insert(
                    "age".to_string(),
                    AttributeValue::N(format!("{}", 20 + (i % 60))),
                );
                WriteRequest::builder()
                    .put_request(PutRequest::builder().set_item(Some(item)).build().unwrap())
                    .build()
            })
            .collect();

        client
            .batch_write_item()
            .request_items(&table_name, items)
            .send()
            .await
            .unwrap();
    }

    // Run QUERIES_PER_TEST queries
    for q in 0..QUERIES_PER_TEST {
        let pk = format!("user#{:06}", (test_idx * ITEMS_PER_TEST + q * 10) / 100);
        client
            .query()
            .table_name(&table_name)
            .key_condition_expression("pk = :pk")
            .expression_attribute_values(":pk", AttributeValue::S(pk))
            .send()
            .await
            .unwrap();
    }

    let execute_ms = exec_start.elapsed().as_secs_f64() * 1000.0;

    // Teardown: delete table
    let teardown_start = Instant::now();
    client
        .delete_table()
        .table_name(&table_name)
        .send()
        .await
        .unwrap();
    let teardown_ms = teardown_start.elapsed().as_secs_f64() * 1000.0;

    let total_ms = total_start.elapsed().as_secs_f64() * 1000.0;

    TestResult {
        test_index: test_idx,
        setup_ms,
        execute_ms,
        teardown_ms,
        total_ms,
    }
}
