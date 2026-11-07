//! Characterisation tests for the behaviours external tooling waits on.
//!
//! Nothing here is new behaviour. These assertions exist because things outside
//! this repository depend on them: a container orchestrator deciding when
//! Dynoxide is ready has only the `GET /` response and the startup line to go
//! on, and neither was covered by a test until this file. `docs/versioning.md`
//! names both, so a change that breaks one is a major rather than an accident.
//!
//! The container's own `HEALTHCHECK` (`do_healthcheck` in `src/main.rs`) reads
//! the status line and never the body, so the body is pinned only here.

#![cfg(all(feature = "http-server", feature = "cli"))]

mod common;

use common::{Listener, Server};
use dynoxide::Database;
use std::io::{BufRead, BufReader};
use std::process::{Child, ChildStderr, ChildStdout, Command, Stdio};
use std::time::{Duration, Instant};

/// The exact `GET /` body, trailing space included.
///
/// The space is part of the response, not an accident of formatting. A wait
/// strategy that matches the whole string breaks if it is trimmed, so it is
/// asserted by equality rather than with `starts_with`.
const HEALTH_BODY: &str = "healthy: dynamodb.us-east-1.amazonaws.com ";

/// Start a server on an ephemeral loopback port and return its base URL.
///
/// No settling delay, and no cleanup: the listener is bound before the task is
/// spawned, so the socket is already queueing connections by the time this
/// returns, and `#[tokio::test]` drops its runtime when the test body ends,
/// which drops everything spawned on it. Same shape as `tests/http_server.rs`.
async fn start_server() -> (String, tokio::task::JoinHandle<()>) {
    let db = Database::memory().unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());

    let handle = tokio::spawn(async move {
        dynoxide::server::serve_on(listener, db).await;
    });
    (url, handle)
}

#[tokio::test]
async fn health_check_answers_200_with_the_documented_body() {
    let (url, _handle) = start_server().await;

    let resp = reqwest::get(&url).await.unwrap();

    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), HEALTH_BODY);
}

#[tokio::test]
async fn health_body_does_not_follow_the_bind_address() {
    // The body must carry nothing about where the server is listening. It
    // cannot today, because the region is a const, so this pins the property
    // rather than catching a live bug: two servers on different ephemeral ports
    // answer byte for byte the same. Note that it does not cover a future
    // `--region` flag, which would thread a value in without changing the port.
    let (first, _a) = start_server().await;
    let (second, _b) = start_server().await;
    assert_ne!(
        first, second,
        "the two servers should be on different ports"
    );

    for url in [first, second] {
        let body = reqwest::get(&url).await.unwrap().text().await.unwrap();
        assert_eq!(body, HEALTH_BODY, "body changed with the bind address");
    }
}

#[tokio::test]
async fn a_misaimed_wait_path_fails_rather_than_passing_by_accident() {
    // A wait strategy pointed anywhere but `/` has to fail. If an unknown path
    // answered 200, a probe with a typo in it would report ready forever.
    let (url, _handle) = start_server().await;

    let resp = reqwest::get(format!("{url}/shell")).await.unwrap();

    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn a_post_without_credentials_is_refused() {
    // Why a container fixture has to hand back dummy credentials: an SDK client
    // with none configured sends no `Authorization` header and gets this, which
    // names authentication and sends people down the wrong path.
    let (url, _handle) = start_server().await;

    let resp = reqwest::Client::new()
        .post(&url)
        .header("x-amz-target", "DynamoDB_20120810.ListTables")
        .header("content-type", "application/x-amz-json-1.0")
        .body("{}")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 400);
    assert!(
        resp.text()
            .await
            .unwrap()
            .contains("MissingAuthenticationTokenException"),
        "the refusal should name the missing token"
    );
}

#[tokio::test]
async fn a_healthy_answer_means_the_engine_will_serve() {
    // A 200 from `/` is only worth waiting on if the engine behind it serves.
    // In process the engine is built before the listener binds, so this cannot
    // reproduce a container's accept-before-ready window; what it pins is that
    // the health path and the operation path are the same server, rather than
    // `/` answering from somewhere that knows nothing about the engine.
    let (url, _handle) = start_server().await;

    let health = reqwest::get(&url).await.unwrap();
    assert_eq!(health.status(), 200);

    let created = reqwest::Client::new()
        .post(&url)
        .header("x-amz-target", "DynamoDB_20120810.CreateTable")
        .header("content-type", "application/x-amz-json-1.0")
        .header(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=fake/20260101/us-east-1/dynamodb/aws4_request, \
             SignedHeaders=host;x-amz-date;x-amz-target, Signature=fake",
        )
        .header("x-amz-date", "20260101T000000Z")
        .json(&serde_json::json!({
            "TableName": "ReadyCheck",
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}]
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(
        created.status(),
        200,
        "CreateTable failed on a connection the health check had already passed"
    );
}

/// Spawn the built binary with both output streams piped.
fn spawn_serve(port: u16) -> (Child, BufReader<ChildStderr>, ChildStdout) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_dynoxide"))
        .args(["serve", "--host", "127.0.0.1", "--port", &port.to_string()])
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn dynoxide");
    let stderr = child.stderr.take().unwrap();
    let stdout = child.stdout.take().unwrap();
    (child, BufReader::new(stderr), stdout)
}

#[test]
fn the_startup_line_is_exact_and_goes_to_stderr() {
    // A log-based wait matches this line, so both its text and its stream are
    // promises. The binary prints it with `eprintln!` while tracing events go
    // to stdout, and a wait strategy pointed at the wrong stream never returns.
    //
    // The server is started on port 0 and the port it reports is read back,
    // so no other test can take the port first. The line is then held to the
    // exact text that port implies. `Server` drains stdout on its own thread:
    // tracing writes there, so with `RUST_LOG=trace` a full pipe would block
    // the child before it reaches the `eprintln!` this test waits for. Its
    // waits time out rather than block on a read, so a line that never
    // reaches stderr fails the test instead of hanging the run.
    let mut command = Command::new(env!("CARGO_BIN_EXE_dynoxide"));
    command.args(["serve", "--host", "127.0.0.1", "--port", "0"]);
    let server = Server::spawn(command);

    let port = server
        .try_port(Listener::Http, Duration::from_secs(10))
        .unwrap_or_else(|e| panic!("no startup line: {e}"));
    let expected = format!("Dynoxide listening on http://127.0.0.1:{port}");
    let line = server
        .wait_for_line(
            |l| l.starts_with(common::HTTP_LISTENING),
            Duration::from_secs(10),
        )
        .expect("the line the port was read from");
    assert_eq!(
        line.trim_end(),
        expected,
        "the startup line is not exactly `{expected}`"
    );

    let output = server.kill_and_collect();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        !stdout.contains("Dynoxide listening"),
        "the startup line reached stdout as well as stderr: {stdout:?}"
    );
}

/// Kills the child when dropped, so a failed assertion does not leave the
/// server running after the test.
struct KillOnDrop(Child);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test]
async fn port_zero_reports_the_port_it_was_assigned() {
    // A harness running several servers starts each with `--port 0` and reads
    // the port back from the startup line, which only works if the line names
    // the port the OS assigned rather than the `0` it was given. There is no
    // port race to lose here, so unlike the test above this does not retry.
    let (child, stderr, mut stdout) = spawn_serve(0);
    let _child = KillOnDrop(child);

    // Drained for the same reason as in `read_startup_line`.
    std::thread::spawn(move || {
        let _ = std::io::copy(&mut stdout, &mut std::io::sink());
    });

    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for line in stderr.lines() {
            let Ok(line) = line else { break };
            if tx.send(line).is_err() {
                break;
            }
        }
    });

    let prefix = "Dynoxide listening on http://127.0.0.1:";
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut saw: Vec<String> = Vec::new();
    let port = loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match rx.recv_timeout(remaining) {
            Ok(line) => match line.strip_prefix(prefix) {
                Some(port) => break port.parse::<u16>().expect("a numeric port"),
                None => saw.push(line),
            },
            Err(e) => panic!("no startup line ({e:?}); saw {saw:?}"),
        }
    };

    assert_ne!(port, 0, "the startup line reported the requested port 0");

    let resp = reqwest::get(format!("http://127.0.0.1:{port}"))
        .await
        .expect("the reported port should accept connections");
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), HEALTH_BODY);
}
