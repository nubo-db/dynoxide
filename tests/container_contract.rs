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

#![cfg(feature = "http-server")]

use dynoxide::Database;
use std::io::{BufRead, BufReader, Read};
use std::process::{Child, ChildStderr, ChildStdout, Command, Stdio};
use std::sync::mpsc::RecvTimeoutError;
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

/// Bind and release a loopback listener to find a port nothing is using.
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
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

/// What one spawn produced, kept apart so a lost port race does not read as a
/// regression in the line itself.
enum Attempt {
    Found,
    /// The child closed stderr and exited without printing it. Losing the port
    /// to another test in this binary looks like this.
    Exited {
        status: String,
        saw: Vec<String>,
    },
    /// The child stayed up for the whole window and never printed it.
    TimedOut {
        saw: Vec<String>,
    },
}

/// Spawn on `port`, watch stderr for `expected`, and return what happened
/// alongside everything the child wrote to stdout.
fn read_startup_line(port: u16, expected: &str) -> (Attempt, String) {
    let (mut child, mut stderr, mut stdout) = spawn_serve(port);

    // Read on a worker thread and time out on the channel. `read_line` blocks
    // until a line arrives, so a deadline checked between reads never fires on
    // the one failure this test exists to catch: a startup line that never
    // reaches stderr at all. That hangs the run instead of failing it.
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut line = String::new();
        loop {
            line.clear();
            match stderr.read_line(&mut line) {
                Ok(0) | Err(_) => break,
                Ok(_) => {
                    if tx.send(line.trim_end().to_string()).is_err() {
                        break;
                    }
                }
            }
        }
    });

    let deadline = Instant::now() + Duration::from_secs(10);
    let mut saw: Vec<String> = Vec::new();
    let mut found = false;
    let mut exited = false;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        match rx.recv_timeout(remaining) {
            Ok(line) if line == expected => {
                found = true;
                break;
            }
            Ok(line) => saw.push(line),
            Err(RecvTimeoutError::Timeout) => break,
            // The reader ended, so the child closed stderr and is on its way out.
            Err(RecvTimeoutError::Disconnected) => {
                exited = true;
                break;
            }
        }
    }

    let _ = child.kill();
    let status = child.wait().ok();

    // Read stdout only once the process is gone, so this cannot block.
    let mut out = String::new();
    let _ = stdout.read_to_string(&mut out);

    let attempt = if found {
        Attempt::Found
    } else if exited {
        Attempt::Exited {
            status: status.map_or_else(|| "unknown".to_owned(), |s| s.to_string()),
            saw,
        }
    } else {
        Attempt::TimedOut { saw }
    };
    (attempt, out)
}

#[test]
fn the_startup_line_is_exact_and_goes_to_stderr() {
    // A log-based wait matches this line, so both its text and its stream are
    // promises. The binary prints it with `eprintln!` while tracing events go
    // to stdout, and a wait strategy pointed at the wrong stream never returns.
    //
    // `free_port()` releases the port before the child binds it, and the other
    // tests here bind ephemeral ports in parallel, so the child can lose that
    // race and exit. That is retried; a child that stays up without printing
    // the line is the real regression and fails immediately.
    let mut lost = Vec::new();
    for _ in 0..3 {
        let port = free_port();
        let expected = format!("Dynoxide listening on http://127.0.0.1:{port}");
        let (attempt, stdout) = read_startup_line(port, &expected);

        match attempt {
            Attempt::Found => {
                assert!(
                    !stdout.contains("Dynoxide listening"),
                    "the startup line reached stdout as well as stderr: {stdout:?}"
                );
                return;
            }
            Attempt::TimedOut { saw } => {
                panic!("the server stayed up without printing `{expected}`; saw {saw:?}")
            }
            Attempt::Exited { status, saw } => lost.push(format!("{status}, saw {saw:?}")),
        }
    }
    panic!("the server exited before printing the startup line three times: {lost:?}");
}
