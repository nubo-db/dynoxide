//! Integration tests for the `dynoxide healthcheck` subcommand.
//!
//! These tests spawn the compiled binary as a subprocess so the exit-code
//! contract that `HEALTHCHECK` and Kubernetes probes rely on is exercised
//! end to end.

#![cfg(feature = "http-server")]

use std::io::Write;
use std::process::Command;
use std::time::{Duration, Instant};

fn dynoxide_bin() -> &'static str {
    env!("CARGO_BIN_EXE_dynoxide")
}

/// Spawn a real dynoxide HTTP server in-process on a random loopback port.
async fn start_loopback_server() -> (u16, tokio::task::JoinHandle<()>) {
    let db = dynoxide::Database::memory().unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let handle = tokio::spawn(async move {
        dynoxide::server::serve_on(listener, db).await;
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    (port, handle)
}

#[tokio::test]
async fn happy_path_succeeds_against_running_server() {
    let (port, _handle) = start_loopback_server().await;
    let status = tokio::process::Command::new(dynoxide_bin())
        .args(["healthcheck", "--port", &port.to_string()])
        .status()
        .await
        .expect("spawn dynoxide");
    assert!(status.success(), "expected exit 0, got {status:?}");
}

#[test]
fn fails_fast_on_unused_port() {
    // Bind and immediately drop a listener to grab a port the OS just freed.
    let port = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    };
    let started = Instant::now();
    let status = Command::new(dynoxide_bin())
        .args(["healthcheck", "--port", &port.to_string(), "--timeout", "2"])
        .status()
        .expect("spawn dynoxide");
    let elapsed = started.elapsed();
    assert!(!status.success(), "expected non-zero exit on unused port");
    assert!(
        elapsed < Duration::from_secs(5),
        "healthcheck took {elapsed:?}; expected sub-5s"
    );
}

#[test]
fn fails_on_non_200_status() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || {
        if let Ok((mut s, _)) = listener.accept() {
            let _ = s.write_all(b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n");
        }
    });
    let status = Command::new(dynoxide_bin())
        .args(["healthcheck", "--port", &port.to_string()])
        .status()
        .expect("spawn dynoxide");
    assert!(!status.success(), "expected non-zero exit on 500 response");
}

#[test]
fn times_out_on_listener_that_never_writes() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    // Accept connections but never write a byte; healthcheck must time out
    // on its read budget rather than hang forever.
    std::thread::spawn(move || {
        let mut held = Vec::new();
        while let Ok((s, _)) = listener.accept() {
            held.push(s);
        }
    });
    let started = Instant::now();
    let status = Command::new(dynoxide_bin())
        .args(["healthcheck", "--port", &port.to_string(), "--timeout", "1"])
        .status()
        .expect("spawn dynoxide");
    let elapsed = started.elapsed();
    assert!(!status.success(), "expected non-zero exit on timeout");
    assert!(
        elapsed < Duration::from_secs(3),
        "healthcheck took {elapsed:?}; expected sub-3s with --timeout 1"
    );
}

#[tokio::test]
async fn rewrites_ipv4_wildcard_host() {
    let db = dynoxide::Database::memory().unwrap();
    let listener = tokio::net::TcpListener::bind("0.0.0.0:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let _handle = tokio::spawn(async move {
        dynoxide::server::serve_on(listener, db).await;
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let output = tokio::process::Command::new(dynoxide_bin())
        .args([
            "healthcheck",
            "--host",
            "0.0.0.0",
            "--port",
            &port.to_string(),
        ])
        .output()
        .await
        .expect("spawn dynoxide");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "expected exit 0; stderr: {stderr}");
    assert!(
        stderr.contains("rewriting wildcard host 0.0.0.0"),
        "stderr did not mention wildcard rewrite: {stderr}"
    );
}

#[test]
fn help_text_lists_all_flags() {
    let output = Command::new(dynoxide_bin())
        .args(["healthcheck", "--help"])
        .output()
        .expect("spawn dynoxide");
    assert!(output.status.success(), "--help should exit 0");
    let stdout = String::from_utf8_lossy(&output.stdout);
    for flag in ["--host", "--port", "--timeout"] {
        assert!(stdout.contains(flag), "help missing {flag}: {stdout}");
    }
}
