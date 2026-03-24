use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, KeySchemaElement, KeyType, ScalarAttributeType,
};
use dynoxide_benchmarks::http::make_sdk_client;
use dynoxide_benchmarks::stats::{mean, stddev};
use serde::Serialize;
use std::process::Command;
use std::time::{Duration, Instant};

#[derive(Debug, Serialize)]
struct StartupResult {
    target: String,
    mode: String, // "cold" or "warm"
    samples: Vec<f64>,
    mean_ms: f64,
    stddev_ms: f64,
}

#[derive(Debug, Serialize)]
struct AllResults {
    results: Vec<StartupResult>,
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let repetitions = args
        .iter()
        .position(|a| a == "--reps")
        .and_then(|i| args.get(i + 1))
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(10);

    let output_path = args
        .iter()
        .position(|a| a == "--output")
        .and_then(|i| args.get(i + 1).cloned());

    eprintln!("=== Startup Benchmark ({repetitions} repetitions) ===\n");

    let mut all_results = Vec::new();

    // --- Dynoxide Embedded (cold start) ---
    {
        eprintln!("Dynoxide Embedded (cold start):");
        let mut samples = Vec::new();
        for _ in 0..repetitions {
            let t = Instant::now();
            let db = dynoxide::Database::memory().unwrap();
            // Verify it's ready by listing tables
            db.list_tables(dynoxide::actions::list_tables::ListTablesRequest {
                exclusive_start_table_name: None,
                limit: None,
            })
            .unwrap();
            let us = t.elapsed().as_nanos() as f64 / 1000.0;
            samples.push(us / 1000.0); // Store as ms
        }
        let m = mean(&samples);
        let s = stddev(&samples);
        eprintln!(
            "  mean: {:.3}ms  stddev: {:.3}ms  ({:.1}us)",
            m,
            s,
            m * 1000.0
        );
        all_results.push(StartupResult {
            target: "dynoxide_embedded".to_string(),
            mode: "cold".to_string(),
            samples: samples.clone(),
            mean_ms: m,
            stddev_ms: s,
        });
    }

    // --- Dynoxide HTTP (cold start) ---
    {
        eprintln!("\nDynoxide HTTP (cold start):");
        let mut samples = Vec::new();
        for _ in 0..repetitions {
            let ms = measure_dynoxide_http_startup().await;
            samples.push(ms);
        }
        let m = mean(&samples);
        let s = stddev(&samples);
        eprintln!("  mean: {:.1}ms  stddev: {:.1}ms", m, s);
        all_results.push(StartupResult {
            target: "dynoxide_http".to_string(),
            mode: "cold".to_string(),
            samples,
            mean_ms: m,
            stddev_ms: s,
        });
    }

    // --- DynamoDB Local (cold start) ---
    if docker_available() {
        eprintln!("\nDynamoDB Local (cold start):");
        let mut samples = Vec::new();
        for i in 0..repetitions {
            eprintln!("  rep {}/{repetitions}...", i + 1);
            match measure_docker_startup(
                "amazon/dynamodb-local:latest",
                "dynoxide-bench-ddb",
                8002,
                8000,
                &["-jar", "DynamoDBLocal.jar", "-inMemory", "-sharedDb"],
            )
            .await
            {
                Some(ms) => samples.push(ms),
                None => {
                    eprintln!("  (skipped -- Docker image not available)");
                    break;
                }
            }
        }
        if !samples.is_empty() {
            let m = mean(&samples);
            let s = stddev(&samples);
            eprintln!("  mean: {:.0}ms  stddev: {:.0}ms", m, s);
            all_results.push(StartupResult {
                target: "dynamodb_local".to_string(),
                mode: "cold".to_string(),
                samples,
                mean_ms: m,
                stddev_ms: s,
            });
        }

        // --- DynamoDB Local (warm start) ---
        eprintln!("\nDynamoDB Local (warm start):");
        let mut samples = Vec::new();
        // Start once, warm up, then measure
        if let Some(ms) = measure_warm_start("amazon/dynamodb-local:latest", 8002, 8000).await {
            samples.push(ms);
            // Only one measurement for warm start (it's the same container)
            let m = mean(&samples);
            eprintln!("  time after warmup: {:.1}ms", m);
            all_results.push(StartupResult {
                target: "dynamodb_local".to_string(),
                mode: "warm".to_string(),
                samples,
                mean_ms: m,
                stddev_ms: 0.0,
            });
        }
    } else {
        eprintln!("\nDocker not available -- skipping DynamoDB Local and LocalStack");
    }

    // --- LocalStack (cold start) ---
    if docker_available() && image_available("localstack/localstack:latest") {
        eprintln!("\nLocalStack (cold start):");
        let mut samples = Vec::new();
        for i in 0..repetitions.min(5) {
            // Fewer reps -- LocalStack is slow to start
            eprintln!("  rep {}/{}...", i + 1, repetitions.min(5));
            match measure_docker_startup(
                "localstack/localstack:latest",
                "dynoxide-bench-ls",
                4566,
                4566,
                &[],
            )
            .await
            {
                Some(ms) => samples.push(ms),
                None => {
                    eprintln!("  (skipped -- Docker image not available)");
                    break;
                }
            }
        }
        if !samples.is_empty() {
            let m = mean(&samples);
            let s = stddev(&samples);
            eprintln!("  mean: {:.0}ms  stddev: {:.0}ms", m, s);
            all_results.push(StartupResult {
                target: "localstack".to_string(),
                mode: "cold".to_string(),
                samples,
                mean_ms: m,
                stddev_ms: s,
            });
        }
    }

    // Output
    let output = AllResults {
        results: all_results,
    };
    let json = serde_json::to_string_pretty(&output).unwrap();

    if let Some(path) = output_path {
        std::fs::write(&path, &json).unwrap();
        eprintln!("\nResults written to {path}");
    } else {
        println!("{json}");
    }
}

async fn measure_dynoxide_http_startup() -> f64 {
    let t = Instant::now();

    let db = dynoxide::Database::memory().unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        dynoxide::server::serve_on(listener, db).await;
    });

    // Poll until ListTables succeeds
    let client = make_sdk_client(&format!("http://{addr}"));
    poll_until_ready(&client, Duration::from_secs(10)).await;

    t.elapsed().as_secs_f64() * 1000.0
}

async fn measure_docker_startup(
    image: &str,
    container_name: &str,
    host_port: u16,
    container_port: u16,
    extra_args: &[&str],
) -> Option<f64> {
    // Remove any existing container
    let _ = Command::new("docker")
        .args(["rm", "-f", container_name])
        .output();

    tokio::time::sleep(Duration::from_millis(500)).await;

    let t = Instant::now();

    let port_mapping = format!("{host_port}:{container_port}");
    let mut cmd_args = vec![
        "run",
        "--rm",
        "-d",
        "-p",
        &port_mapping,
        "--name",
        container_name,
    ];

    // Forward LocalStack auth token if available
    let ls_token;
    if image.contains("localstack") && let Ok(token) = std::env::var("LOCALSTACK_AUTH_TOKEN") {
        ls_token = format!("LOCALSTACK_AUTH_TOKEN={token}");
        cmd_args.extend_from_slice(&["-e", &ls_token]);
    }

    cmd_args.push(image);
    cmd_args.extend_from_slice(extra_args);

    let output = Command::new("docker").args(&cmd_args).output().ok()?;

    if !output.status.success() {
        eprintln!(
            "  Failed to start {image}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        return None;
    }

    let endpoint = format!("http://localhost:{host_port}");

    let client = make_sdk_client(&endpoint);
    let ready = poll_until_ready(&client, Duration::from_secs(300)).await;

    let ms = t.elapsed().as_secs_f64() * 1000.0;

    // Clean up
    let _ = Command::new("docker")
        .args(["rm", "-f", container_name])
        .output();

    tokio::time::sleep(Duration::from_millis(500)).await;

    if ready {
        Some(ms)
    } else {
        eprintln!("  Timed out waiting for {image}");
        None
    }
}

async fn measure_warm_start(image: &str, host_port: u16, container_port: u16) -> Option<f64> {
    let container_name = "dynoxide-bench-warm";

    // Remove any existing
    let _ = Command::new("docker")
        .args(["rm", "-f", container_name])
        .output();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Start container
    let output = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-d",
            "-p",
            &format!("{host_port}:{container_port}"),
            "--name",
            container_name,
            image,
            "-jar",
            "DynamoDBLocal.jar",
            "-inMemory",
            "-sharedDb",
        ])
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let endpoint = format!("http://localhost:{host_port}");
    let client = make_sdk_client(&endpoint);

    // Wait for container to be ready
    if !poll_until_ready(&client, Duration::from_secs(60)).await {
        let _ = Command::new("docker")
            .args(["rm", "-f", container_name])
            .output();
        return None;
    }

    // Run warmup: 500 Put + 100 Get
    let _ = client
        .create_table()
        .table_name("_warmup")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
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
        .send()
        .await;

    for i in 0..500 {
        let _ = client
            .put_item()
            .table_name("_warmup")
            .item("pk", AttributeValue::S(format!("w{i}")))
            .item("data", AttributeValue::S(format!("data-{i}")))
            .send()
            .await;
    }
    for i in 0..100 {
        let _ = client
            .get_item()
            .table_name("_warmup")
            .key("pk", AttributeValue::S(format!("w{i}")))
            .send()
            .await;
    }

    // Now measure first operation after warmup
    let t = Instant::now();
    let _ = client.list_tables().send().await;
    let ms = t.elapsed().as_secs_f64() * 1000.0;

    // Clean up
    let _ = Command::new("docker")
        .args(["rm", "-f", container_name])
        .output();

    Some(ms)
}

async fn poll_until_ready(client: &aws_sdk_dynamodb::Client, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if Instant::now() > deadline {
            return false;
        }
        match client.list_tables().send().await {
            Ok(_) => return true,
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
}

fn docker_available() -> bool {
    Command::new("docker")
        .args(["info"])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn image_available(image: &str) -> bool {
    Command::new("docker")
        .args(["image", "inspect", image])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}
