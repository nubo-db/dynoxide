//! CLI tests for the `--schema` flag on `serve` and the no-subcommand form.
//!
//! These tests spawn the compiled binary as a subprocess so the arg-parsing
//! and startup routing are exercised end to end, not just the library API.

#![cfg(all(feature = "http-server", feature = "import"))]

mod common;

use common::{Listener, Server};
use std::process::Command;
use std::time::Duration;

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

/// The command for `dynoxide` with the given args.
fn dynoxide(args: &[&str]) -> Command {
    let mut command = Command::new(dynoxide_bin());
    command.args(args);
    command
}

/// Spawn `dynoxide` with the given args, wait up to `timeout` for a line on
/// stderr containing `needle`, kill the process, then return whether it was
/// found.
fn spawn_and_wait_for_stderr(args: &[&str], needle: &str, timeout: Duration) -> bool {
    Server::spawn(dynoxide(args))
        .wait_for_line(|line| line.contains(needle), timeout)
        .is_ok()
}

/// Send a minimal DynamoDB `DescribeTable` request. Dynoxide doesn't verify
/// SigV4 signatures, so a well-formed-but-fake Authorization header is enough.
async fn describe_table(port: u16, table_name: &str) -> reqwest::Result<serde_json::Value> {
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
        .await?
        .json()
        .await
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
fn serve_help_lists_log() {
    let output = Command::new(dynoxide_bin())
        .args(["serve", "--help"])
        .output()
        .expect("spawn dynoxide");
    assert!(output.status.success(), "--help should exit 0");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("--log <MODE>"), "missing --log:\n{stdout}");
}

#[test]
fn root_help_lists_log() {
    let output = Command::new(dynoxide_bin())
        .args(["--help"])
        .output()
        .expect("spawn dynoxide");
    assert!(output.status.success(), "--help should exit 0");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("--log <MODE>"), "missing --log:\n{stdout}");
}

#[test]
fn serve_schema_scaffolds_tables_on_startup() {
    let tmp = tempfile::tempdir().unwrap();
    let schema_file = tmp.path().join("schema.json");
    write_schema_file(&schema_file, "Widgets");

    let found = spawn_and_wait_for_stderr(
        &[
            "serve",
            "--port",
            "0",
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

    let found = spawn_and_wait_for_stderr(
        &["--port", "0", "--schema", schema_file.to_str().unwrap()],
        "Scaffolded 1 table(s) from schema",
        Duration::from_secs(10),
    );
    assert!(
        found,
        "`dynoxide --schema` (no subcommand) did not emit scaffold message within timeout"
    );
}

/// Unix only: the server is stopped with `kill -TERM`. Off unix it only
/// listens for Ctrl+C, which a test cannot easily send to a child process.
///
/// Quiet mode prints no listening line to read a port from, so the port is
/// picked up front, and the run is started again on a fresh one if another
/// process takes it before the child binds it.
#[cfg(unix)]
#[test]
fn log_quiet_suppresses_startup_and_shutdown_messages() {
    let tmp = tempfile::tempdir().unwrap();
    let schema_file = tmp.path().join("schema.json");
    write_schema_file(&schema_file, "QuietTable");

    for uses_subcommand in [true, false] {
        let build = |ports: &[u16]| {
            let mut args = Vec::new();
            if uses_subcommand {
                args.push("serve".to_owned());
            }
            args.extend([
                "--log".to_owned(),
                "quiet".to_owned(),
                "--port".to_owned(),
                ports[0].to_string(),
                "--schema".to_owned(),
                schema_file.to_string_lossy().into_owned(),
            ]);
            let mut command = Command::new(dynoxide_bin());
            command.args(&args);
            command
        };

        let output =
            common::run_until_ready_then_term(build, &[Listener::Http], common::Startup::Silent);
        assert!(
            output.status.success(),
            "dynoxide did not shut down gracefully: {:?}",
            output.status
        );

        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            !stderr.contains("Shutting down"),
            "quiet mode emitted shutdown message: {stderr}"
        );

        assert!(
            !stderr.contains("Scaffolded"),
            "quiet mode emitted scaffold message: {stderr}"
        );
        assert!(
            !stderr.contains("Dynoxide listening"),
            "quiet mode emitted listening message: {stderr}"
        );
    }
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

    let server = Server::spawn(dynoxide(&[
        "serve",
        "--port",
        "0",
        "--schema",
        schema_file.to_str().unwrap(),
    ]));
    let scaffolded = server
        .wait_for_line(
            |line| line.contains("Scaffolded 1 table(s) from schema"),
            Duration::from_secs(10),
        )
        .is_ok();
    assert!(scaffolded, "server did not report scaffolding the table");

    // The scaffold line comes before the bind, so the port is read from the
    // listening line, which comes after it.
    let port = server.port(Listener::Http);
    let body = describe_table(port, "Orders")
        .await
        .unwrap_or_else(|e| panic!("DescribeTable failed ({e}); stderr:\n{}", server.stderr()));
    drop(server);

    let lsis = &body["Table"]["LocalSecondaryIndexes"];
    assert!(
        lsis.is_array(),
        "expected LocalSecondaryIndexes in DescribeTable response: {body}"
    );
    assert_eq!(lsis[0]["IndexName"], "by-created-at");
}
