//! CLI tests for the `--schema` flag on `serve` and the no-subcommand form.
//!
//! These tests spawn the compiled binary as a subprocess so the arg-parsing
//! and startup routing are exercised end to end, not just the library API.

#![cfg(all(feature = "http-server", feature = "import"))]

use std::io::{BufRead, BufReader};
use std::process::{Child, ChildStderr, Command, Stdio};
use std::time::{Duration, Instant};

fn dynoxide_bin() -> &'static str {
    env!("CARGO_BIN_EXE_dynoxide")
}

/// Write a minimal single-table DescribeTable schema file.
fn write_schema_file(path: &std::path::Path, table_name: &str) {
    let schema = serde_json::json!([{
        "Table": {
            "TableName": table_name,
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}]
        }
    }]);
    std::fs::write(path, serde_json::to_string_pretty(&schema).unwrap()).unwrap();
}

/// Bind and immediately release a loopback listener to obtain a free port.
fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

/// Spawn `dynoxide` with the given args and a piped stderr stream.
fn spawn_dynoxide(args: &[&str]) -> (Child, BufReader<ChildStderr>) {
    let mut child = Command::new(dynoxide_bin())
        .args(args)
        .stderr(Stdio::piped())
        .stdout(Stdio::null())
        .spawn()
        .expect("spawn dynoxide");
    let stderr = child.stderr.take().unwrap();
    (child, BufReader::new(stderr))
}

/// Read lines from `reader` until one contains `needle` or `timeout` elapses.
fn wait_for_line(reader: &mut impl BufRead, needle: &str, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    let mut line = String::new();
    while Instant::now() < deadline {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) | Err(_) => break,
            Ok(_) if line.contains(needle) => return true,
            Ok(_) => {}
        }
    }
    false
}

/// Spawn `dynoxide` with the given args, wait up to `timeout` for a line on
/// stderr matching `needle`, kill the process, then return whether it was found.
fn spawn_and_wait_for_stderr(args: &[&str], needle: &str, timeout: Duration) -> bool {
    let (mut child, mut reader) = spawn_dynoxide(args);
    let found = wait_for_line(&mut reader, needle, timeout);
    let _ = child.kill();
    let _ = child.wait();
    found
}

/// Send a minimal DynamoDB `DescribeTable` request. Dynoxide doesn't verify
/// SigV4 signatures, so a well-formed-but-fake Authorization header is enough.
async fn describe_table(port: u16, table_name: &str) -> serde_json::Value {
    reqwest::Client::new()
        .post(format!("http://127.0.0.1:{port}"))
        .header("x-amz-target", "DynamoDB_20120810.DescribeTable")
        .header("content-type", "application/x-amz-json-1.0")
        .header(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=fakekey/20260101/us-east-1/dynamodb/aws4_request, SignedHeaders=host;x-amz-date;x-amz-target, Signature=fakesig",
        )
        .header("x-amz-date", "20260101T000000Z")
        .json(&serde_json::json!({ "TableName": table_name }))
        .send()
        .await
        .expect("send DescribeTable request")
        .json()
        .await
        .expect("parse DescribeTable response")
}

#[test]
fn serve_help_lists_schema_flag() {
    let output = Command::new(dynoxide_bin())
        .args(["serve", "--help"])
        .output()
        .expect("spawn dynoxide");
    assert!(output.status.success(), "--help should exit 0");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("--schema"),
        "`dynoxide serve --help` should list --schema; got:\n{stdout}"
    );
}

#[test]
fn root_help_lists_schema_flag() {
    let output = Command::new(dynoxide_bin())
        .args(["--help"])
        .output()
        .expect("spawn dynoxide");
    assert!(output.status.success(), "--help should exit 0");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("--schema"),
        "`dynoxide --help` should list --schema; got:\n{stdout}"
    );
}

#[test]
fn serve_schema_scaffolds_tables_on_startup() {
    let tmp = tempfile::tempdir().unwrap();
    let schema_file = tmp.path().join("schema.json");
    write_schema_file(&schema_file, "Widgets");

    let port = free_port();
    let found = spawn_and_wait_for_stderr(
        &[
            "serve",
            "--port",
            &port.to_string(),
            "--schema",
            schema_file.to_str().unwrap(),
        ],
        "Scaffolded 1 table(s) from schema",
        Duration::from_secs(10),
    );
    assert!(
        found,
        "`dynoxide serve --schema` did not emit scaffold message within timeout"
    );
}

#[test]
fn root_schema_scaffolds_tables_on_startup() {
    let tmp = tempfile::tempdir().unwrap();
    let schema_file = tmp.path().join("schema.json");
    write_schema_file(&schema_file, "Gadgets");

    let port = free_port();
    let found = spawn_and_wait_for_stderr(
        &[
            "--port",
            &port.to_string(),
            "--schema",
            schema_file.to_str().unwrap(),
        ],
        "Scaffolded 1 table(s) from schema",
        Duration::from_secs(10),
    );
    assert!(
        found,
        "`dynoxide --schema` (no subcommand) did not emit scaffold message within timeout"
    );
}

/// Regression test: `created_at` is used only by the LSI's sort key, not by
/// the table's own key schema or any other index. A hand-parsed
/// `CreateTableRequest` that drops `LocalSecondaryIndexes` leaves this
/// attribute definition orphaned, which real table creation rejects outright.
#[tokio::test]
async fn serve_schema_scaffolds_lsi_with_orphaned_attribute() {
    let tmp = tempfile::tempdir().unwrap();
    let schema_file = tmp.path().join("schema.json");
    let schema = serde_json::json!([{
        "Table": {
            "TableName": "Orders",
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"}
            ],
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
                {"AttributeName": "created_at", "AttributeType": "S"}
            ],
            "LocalSecondaryIndexes": [{
                "IndexName": "by-created-at",
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "created_at", "KeyType": "RANGE"}
                ],
                "Projection": {"ProjectionType": "ALL"}
            }]
        }
    }]);
    std::fs::write(&schema_file, serde_json::to_string_pretty(&schema).unwrap()).unwrap();

    let port = free_port();
    let (mut child, mut reader) = spawn_dynoxide(&[
        "serve",
        "--port",
        &port.to_string(),
        "--schema",
        schema_file.to_str().unwrap(),
    ]);
    let scaffolded = wait_for_line(
        &mut reader,
        "Scaffolded 1 table(s) from schema",
        Duration::from_secs(10),
    );
    assert!(scaffolded, "server did not report scaffolding the table");

    let body = describe_table(port, "Orders").await;
    let _ = child.kill();
    let _ = child.wait();

    let lsis = &body["Table"]["LocalSecondaryIndexes"];
    assert!(
        lsis.is_array(),
        "expected LocalSecondaryIndexes in DescribeTable response: {body}"
    );
    assert_eq!(lsis[0]["IndexName"], "by-created-at");
}
