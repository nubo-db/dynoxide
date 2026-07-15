//! CLI tests for the `--schema` flag on `serve` and the no-subcommand form.
//!
//! These tests spawn the compiled binary as a subprocess so the arg-parsing
//! and startup routing are exercised end to end, not just the library API.

#![cfg(all(feature = "http-server", feature = "import"))]

use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
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

/// Spawn `dynoxide` with the given args, wait up to `timeout` for a line on
/// stderr matching `needle`, kill the process, then return whether it was found.
fn spawn_and_wait_for_stderr(args: &[&str], needle: &str, timeout: Duration) -> bool {
    let mut child = Command::new(dynoxide_bin())
        .args(args)
        .stderr(Stdio::piped())
        .stdout(Stdio::null())
        .spawn()
        .expect("spawn dynoxide");

    let stderr = child.stderr.take().unwrap();
    let mut reader = BufReader::new(stderr);
    let deadline = Instant::now() + timeout;
    let mut found = false;
    let mut line = String::new();

    while Instant::now() < deadline {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) | Err(_) => break,
            Ok(_) if line.contains(needle) => {
                found = true;
                break;
            }
            Ok(_) => {}
        }
    }

    let _ = child.kill();
    let _ = child.wait();
    found
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
