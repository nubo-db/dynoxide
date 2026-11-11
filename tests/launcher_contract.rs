//! The behaviours a language wrapper relies on to start and stop Dynoxide:
//! the machine-readable ready line (`--ready-json`) and shutting down when
//! stdin closes (`--exit-with-parent`). `docs/launcher.md` describes the
//! contract these pin, and `docs/versioning.md` holds the project to it.
//!
//! Every test spawns the compiled binary, because what matters is what a
//! wrapper sees: which stream a line arrives on, when it arrives, and when the
//! process exits.

#![cfg(all(feature = "http-server", feature = "cli"))]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc::{Receiver, channel};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

const READY_TIMEOUT: Duration = Duration::from_secs(10);

/// A dummy SigV4 header. Dynoxide checks the shape and not the signature.
const AUTHORIZATION: &str = "AWS4-HMAC-SHA256 Credential=dynoxide/20260101/us-east-1/dynamodb/aws4_request, SignedHeaders=host;x-amz-date;x-amz-target, Signature=fakesig";

fn dynoxide() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_dynoxide"));
    // The runner's own RUST_LOG would change what lands on each stream.
    command.env_remove("RUST_LOG");
    command
}

/// A spawned server with both output streams drained on their own threads, so
/// a full pipe can never stall it.
struct Launched {
    child: Child,
    stdout_lines: Receiver<String>,
    stdout: Arc<Mutex<Vec<String>>>,
    stderr: Arc<Mutex<Vec<String>>>,
    readers: Vec<JoinHandle<()>>,
}

impl Launched {
    fn spawn(mut command: Command, stdin: Stdio) -> Launched {
        let mut child = command
            .stdin(stdin)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn dynoxide");
        let out = child.stdout.take().unwrap();
        let err = child.stderr.take().unwrap();

        let stdout = Arc::new(Mutex::new(Vec::new()));
        let stderr = Arc::new(Mutex::new(Vec::new()));
        let (tx, stdout_lines) = channel();

        let collected = Arc::clone(&stdout);
        let out_reader = std::thread::spawn(move || {
            for line in BufReader::new(out).lines() {
                let Ok(line) = line else { break };
                collected.lock().unwrap().push(line.clone());
                let _ = tx.send(line);
            }
        });
        let collected = Arc::clone(&stderr);
        let err_reader = std::thread::spawn(move || {
            for line in BufReader::new(err).lines() {
                let Ok(line) = line else { break };
                collected.lock().unwrap().push(line);
            }
        });

        Launched {
            child,
            stdout_lines,
            stdout,
            stderr,
            readers: vec![out_reader, err_reader],
        }
    }

    /// The first stdout line, parsed. Panics with stderr if it doesn't arrive.
    fn ready_line(&self) -> serde_json::Value {
        match self.stdout_lines.recv_timeout(READY_TIMEOUT) {
            Ok(line) => serde_json::from_str(&line)
                .unwrap_or_else(|e| panic!("first stdout line is not JSON ({e}): {line}")),
            Err(e) => panic!(
                "no ready line on stdout ({e:?}); stderr:\n{}",
                self.stderr_text()
            ),
        }
    }

    fn stderr_text(&self) -> String {
        self.stderr.lock().unwrap().join("\n")
    }

    /// Wait for a stderr line containing `needle`.
    fn wait_for_stderr(&self, needle: &str) -> String {
        let deadline = Instant::now() + READY_TIMEOUT;
        loop {
            if let Some(line) = self
                .stderr
                .lock()
                .unwrap()
                .iter()
                .find(|l| l.contains(needle))
            {
                return line.clone();
            }
            assert!(
                Instant::now() < deadline,
                "no stderr line containing {needle:?}; stderr:\n{}",
                self.stderr_text()
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// Wait up to `timeout` for the process to exit on its own.
    fn wait_exit(&mut self, timeout: Duration) -> Option<ExitStatus> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                return Some(status);
            }
            if Instant::now() >= deadline {
                return None;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// Kill the process if it is still running and wait for both streams to
    /// close, so everything it printed has been collected.
    fn finish(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        for reader in self.readers.drain(..) {
            let _ = reader.join();
        }
    }

    fn stdout_after_finish(&mut self) -> Vec<String> {
        self.finish();
        self.stdout.lock().unwrap().clone()
    }
}

impl Drop for Launched {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// POST a `ListTables` to `endpoint` (`http://host:port`) and return the
/// status code and body.
fn list_tables(endpoint: &str) -> (u16, String) {
    dynamo(endpoint, "ListTables", "{}")
}

/// POST one DynamoDB operation to `endpoint` (`http://host:port`) and return
/// the status code and body.
fn dynamo(endpoint: &str, operation: &str, body: &str) -> (u16, String) {
    let authority = endpoint
        .strip_prefix("http://")
        .unwrap_or_else(|| panic!("endpoint is not http://: {endpoint}"));
    let mut stream = TcpStream::connect(authority)
        .unwrap_or_else(|e| panic!("connect to {authority} failed: {e}"));
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let request = format!(
        "POST / HTTP/1.1\r\nHost: {authority}\r\nContent-Type: application/x-amz-json-1.0\r\n\
         X-Amz-Target: DynamoDB_20120810.{operation}\r\nX-Amz-Date: 20260101T000000Z\r\n\
         Authorization: {AUTHORIZATION}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(request.as_bytes()).unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    let status = response
        .split(' ')
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("no status line in response: {response}"));
    let body = response
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_owned())
        .unwrap_or_default();
    (status, body)
}

fn product_version() -> String {
    let out = dynoxide().arg("--version").output().unwrap();
    let text = String::from_utf8(out.stdout).unwrap();
    text.trim()
        .strip_prefix("dynoxide ")
        .unwrap_or_else(|| panic!("unexpected --version output: {text}"))
        .to_owned()
}

fn serve(args: &[&str]) -> Command {
    let mut command = dynoxide();
    command.arg("serve").args(args);
    command
}

// --- The ready line -------------------------------------------------------

#[test]
fn the_ready_line_is_the_only_stdout_line_and_names_the_endpoint() {
    let mut server = Launched::spawn(serve(&["--port", "0", "--ready-json"]), Stdio::piped());
    let line = server.ready_line();

    assert_eq!(line["event"], "ready");
    assert_eq!(line["schema"], 1);
    let port = line["port"].as_u64().expect("port is a number");
    assert_ne!(
        port, 0,
        "the ready line reports the requested port, not the assigned one"
    );
    assert_eq!(line["host"], "127.0.0.1");
    assert_eq!(line["endpoint"], format!("http://127.0.0.1:{port}"));
    assert_eq!(line["pid"].as_u64(), Some(u64::from(server.child.id())));

    let stdout = server.stdout_after_finish();
    assert_eq!(
        stdout.len(),
        1,
        "stdout carried more than the ready line: {stdout:?}"
    );
    // Launchers must not depend on field order, but the documented examples
    // show this one, so the line should match them.
    let keys = [
        "event", "schema", "version", "endpoint", "host", "port", "pid",
    ];
    let positions: Vec<_> = keys
        .iter()
        .map(|k| stdout[0].find(&format!("\"{k}\":")))
        .collect();
    assert!(
        positions.iter().all(Option::is_some) && positions.is_sorted(),
        "fields are not in the documented order: {}",
        stdout[0]
    );
}

#[test]
fn a_request_sent_straight_after_the_ready_line_is_served() {
    // No retry and no sleep: the line has to mean the server will answer.
    let server = Launched::spawn(serve(&["--port", "0", "--ready-json"]), Stdio::piped());
    let line = server.ready_line();
    let (status, body) = list_tables(line["endpoint"].as_str().unwrap());
    assert_eq!(status, 200, "body: {body}");
    assert!(body.contains("TableNames"), "body: {body}");
}

#[test]
fn verbose_logging_goes_to_stderr_when_the_ready_line_is_asked_for() {
    let mut command = serve(&["--port", "0", "--ready-json"]);
    command.env("RUST_LOG", "debug");
    let mut server = Launched::spawn(command, Stdio::piped());
    let line = server.ready_line();
    let endpoint = line["endpoint"].as_str().unwrap().to_owned();
    for _ in 0..3 {
        assert_eq!(list_tables(&endpoint).0, 200);
    }
    // The request events are what RUST_LOG=debug adds.
    server.wait_for_stderr("ListTables");

    let stdout = server.stdout_after_finish();
    assert_eq!(
        stdout.len(),
        1,
        "stdout carried more than the ready line: {stdout:?}"
    );
}

#[test]
fn without_the_flag_there_is_no_ready_line_and_logging_stays_on_stdout() {
    // Existing behaviour, unchanged: tracing writes to stdout, and the only
    // startup signal is the human line on stderr.
    let mut command = serve(&["--port", "0"]);
    command.env("RUST_LOG", "debug");
    let mut server = Launched::spawn(command, Stdio::piped());
    let startup = server.wait_for_stderr("Dynoxide listening on http://");
    let endpoint = startup
        .strip_prefix("Dynoxide listening on ")
        .unwrap()
        .to_owned();
    assert_eq!(list_tables(&endpoint).0, 200);

    let deadline = Instant::now() + READY_TIMEOUT;
    while !server
        .stdout
        .lock()
        .unwrap()
        .iter()
        .any(|l| l.contains("ListTables"))
    {
        assert!(Instant::now() < deadline, "no request event on stdout");
        std::thread::sleep(Duration::from_millis(20));
    }
    let stdout = server.stdout_after_finish();
    assert!(
        !stdout.iter().any(|l| l.contains("\"event\"")),
        "a ready line appeared without the flag: {stdout:?}"
    );
}

#[test]
fn the_ready_line_still_prints_under_quiet_logging() {
    let mut server = Launched::spawn(
        serve(&["--port", "0", "--ready-json", "--log", "quiet"]),
        Stdio::piped(),
    );
    assert_eq!(server.ready_line()["event"], "ready");
    // Collect everything stderr carried before looking for what shouldn't be
    // there, or the check could run before the line had been read.
    server.finish();
    assert!(
        !server.stderr_text().contains("Dynoxide listening"),
        "quiet still printed the startup line:\n{}",
        server.stderr_text()
    );
}

#[test]
fn the_no_subcommand_form_takes_the_flag() {
    let mut command = dynoxide();
    command.args(["--port", "0", "--ready-json"]);
    let server = Launched::spawn(command, Stdio::piped());
    let line = server.ready_line();
    assert_eq!(line["event"], "ready");
    assert_eq!(list_tables(line["endpoint"].as_str().unwrap()).0, 200);
}

#[test]
fn a_wildcard_ipv4_bind_reports_a_loopback_endpoint() {
    let server = Launched::spawn(
        serve(&["--host", "0.0.0.0", "--port", "0", "--ready-json"]),
        Stdio::piped(),
    );
    let line = server.ready_line();
    let port = line["port"].as_u64().unwrap();
    assert_eq!(line["host"], "0.0.0.0");
    assert_eq!(line["endpoint"], format!("http://127.0.0.1:{port}"));
    assert_eq!(list_tables(line["endpoint"].as_str().unwrap()).0, 200);
}

#[test]
fn a_wildcard_ipv6_bind_reports_a_loopback_endpoint() {
    if std::net::TcpListener::bind("[::1]:0").is_err() {
        eprintln!("skipped: this machine has no IPv6 loopback");
        return;
    }
    let server = Launched::spawn(
        serve(&["--host", "[::]", "--port", "0", "--ready-json"]),
        Stdio::piped(),
    );
    let line = server.ready_line();
    let port = line["port"].as_u64().unwrap();
    assert_eq!(line["host"], "::");
    assert_eq!(line["endpoint"], format!("http://[::1]:{port}"));
    assert_eq!(list_tables(line["endpoint"].as_str().unwrap()).0, 200);
}

#[cfg(feature = "import")]
#[test]
fn tables_from_a_schema_exist_once_the_ready_line_arrives() {
    let dir = tempfile::tempdir().unwrap();
    let schema = dir.path().join("schema.json");
    let tables = serde_json::json!([{
        "Table": {
            "TableName": "Launched",
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}]
        }
    }]);
    std::fs::write(&schema, tables.to_string()).unwrap();

    let server = Launched::spawn(
        serve(&[
            "--port",
            "0",
            "--ready-json",
            "--schema",
            schema.to_str().unwrap(),
        ]),
        Stdio::piped(),
    );
    let line = server.ready_line();
    let (status, body) = list_tables(line["endpoint"].as_str().unwrap());
    assert_eq!(status, 200);
    assert!(body.contains("\"Launched\""), "body: {body}");
}

#[test]
fn a_server_that_fails_to_bind_prints_no_ready_line() {
    let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = taken.local_addr().unwrap().port().to_string();
    let mut server = Launched::spawn(serve(&["--port", &port, "--ready-json"]), Stdio::piped());
    let status = server
        .wait_exit(READY_TIMEOUT)
        .expect("a server that can't bind should exit");
    assert!(!status.success());
    let stdout = server.stdout_after_finish();
    assert!(stdout.is_empty(), "stdout: {stdout:?}");
}

#[test]
fn the_ready_line_reports_the_product_version() {
    let server = Launched::spawn(serve(&["--port", "0", "--ready-json"]), Stdio::piped());
    assert_eq!(server.ready_line()["version"], product_version());
}

// --- Shutting down when stdin closes --------------------------------------

/// How long a server is given to shut down once asked.
const STOP_TIMEOUT: Duration = Duration::from_secs(5);

/// Set on the copy of this test binary that stands in for a launcher.
const LAUNCHER_ENV: &str = "DYNOXIDE_TEST_STAND_IN_LAUNCHER";

/// Not a test of its own. With `LAUNCHER_ENV` set, this process plays a
/// launcher: it starts a server with its stdin on a pipe this process holds,
/// hands the server its own stdout, and waits to be killed. Without the
/// variable, which is every normal run, it returns at once.
#[test]
fn stand_in_launcher() {
    if std::env::var_os(LAUNCHER_ENV).is_none() {
        return;
    }
    let mut server = dynoxide()
        .args(["serve", "--port", "0", "--ready-json", "--exit-with-parent"])
        .stdin(Stdio::piped())
        .stdout(Stdio::inherit())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn dynoxide");
    // Taken out so `wait` can't close it: only this process dying closes the
    // pipe, and the server runs until then, so the wait never returns.
    let _stdin = server.stdin.take();
    let _ = server.wait();
}

#[test]
fn the_server_exits_when_its_launcher_is_killed() {
    // The launcher is killed outright, so no shutdown code of its own runs.
    // The only thing that can tell the server is the operating system closing
    // the launcher's end of the stdin pipe.
    let mut launcher = KillOnDrop(
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "stand_in_launcher", "--nocapture"])
            .env(LAUNCHER_ENV, "1")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn the stand-in launcher"),
    );

    // The server shares this pipe with the launcher, so it reaches end-of-file
    // only once both have exited. The test harness prints its own lines on it
    // too, so look for the one that parses as the ready line.
    let (tx, lines) = channel();
    let stdout = launcher.0.stdout.take().unwrap();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            let Ok(line) = line else { break };
            if tx.send(line).is_err() {
                break;
            }
        }
    });
    let deadline = Instant::now() + READY_TIMEOUT;
    // The server is stopped by its pid if the test fails with it still
    // running, since this test has no handle to it.
    let (endpoint, server) = loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let line = lines
            .recv_timeout(remaining)
            .unwrap_or_else(|e| panic!("no ready line from the server ({e:?})"));
        if let Ok(ready) = serde_json::from_str::<serde_json::Value>(&line) {
            let pid = ready["pid"].as_u64().unwrap() as u32;
            break (
                ready["endpoint"].as_str().unwrap().to_owned(),
                KillPidOnDrop(pid),
            );
        }
    };
    assert_eq!(list_tables(&endpoint).0, 200);

    launcher.0.kill().unwrap();
    launcher.0.wait().unwrap();

    let deadline = Instant::now() + STOP_TIMEOUT;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match lines.recv_timeout(remaining) {
            Ok(_) => continue,
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                panic!("the server was still running {STOP_TIMEOUT:?} after its launcher died")
            }
        }
    }
    let authority = endpoint.strip_prefix("http://").unwrap();
    assert!(
        TcpStream::connect(authority).is_err(),
        "something still answers on {authority}"
    );
    // It exited on its own, so there's nothing left to stop, and its pid may
    // already belong to something else.
    std::mem::forget(server);
}

/// Kills a child this test spawned, if the test ends with it still running.
struct KillOnDrop(Child);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Kills a process by id when the test has no handle to it: the server a
/// stand-in launcher started, if the test fails before it has exited.
struct KillPidOnDrop(u32);

impl Drop for KillPidOnDrop {
    fn drop(&mut self) {
        let pid = self.0.to_string();
        #[cfg(unix)]
        let _ = Command::new("kill").args(["-9", &pid]).status();
        #[cfg(windows)]
        let _ = Command::new("taskkill").args(["/F", "/PID", &pid]).status();
    }
}

#[test]
fn closing_stdin_shuts_the_server_down_cleanly() {
    let mut server = Launched::spawn(
        serve(&["--port", "0", "--ready-json", "--exit-with-parent"]),
        Stdio::piped(),
    );
    server.ready_line();
    drop(server.child.stdin.take());

    let status = server
        .wait_exit(STOP_TIMEOUT)
        .expect("the server kept running after stdin closed");
    assert!(status.success(), "exit status {status}");
    server.finish();
    assert!(
        server.stderr_text().contains("Shutting down"),
        "no shutdown line:\n{}",
        server.stderr_text()
    );
}

#[test]
fn closing_stdin_under_quiet_logging_prints_no_shutdown_line() {
    let mut server = Launched::spawn(
        serve(&[
            "--port",
            "0",
            "--ready-json",
            "--exit-with-parent",
            "--log",
            "quiet",
        ]),
        Stdio::piped(),
    );
    server.ready_line();
    drop(server.child.stdin.take());

    let status = server
        .wait_exit(STOP_TIMEOUT)
        .expect("the server kept running after stdin closed");
    assert!(status.success(), "exit status {status}");
    server.finish();
    assert!(
        !server.stderr_text().contains("Shutting down"),
        "quiet printed the shutdown line:\n{}",
        server.stderr_text()
    );
}

#[test]
fn the_server_keeps_serving_while_stdin_stays_open() {
    let mut server = Launched::spawn(
        serve(&["--port", "0", "--ready-json", "--exit-with-parent"]),
        Stdio::piped(),
    );
    let line = server.ready_line();
    // Anything a launcher writes is read and thrown away.
    server
        .child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(b"ignored\n")
        .unwrap();
    std::thread::sleep(Duration::from_secs(2));
    assert!(
        server.child.try_wait().unwrap().is_none(),
        "the server exited"
    );
    assert_eq!(list_tables(line["endpoint"].as_str().unwrap()).0, 200);
}

#[cfg(unix)]
#[test]
fn sigterm_stops_the_server_while_stdin_is_still_open() {
    // The stdin reader must not hold shutdown up. A launcher that keeps the
    // pipe open and stops the server with SIGTERM has to see it exit.
    let mut server = Launched::spawn(
        serve(&["--port", "0", "--ready-json", "--exit-with-parent"]),
        Stdio::piped(),
    );
    server.ready_line();
    let status = Command::new("kill")
        .args(["-TERM", &server.child.id().to_string()])
        .status()
        .unwrap();
    assert!(status.success());

    let status = server
        .wait_exit(STOP_TIMEOUT)
        .expect("SIGTERM did not stop the server while its stdin was open");
    assert!(status.success(), "exit status {status}");
}

#[test]
fn writes_survive_a_shutdown_through_stdin() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("data.db");
    let db = db.to_str().unwrap();
    let args = [
        "--port",
        "0",
        "--ready-json",
        "--exit-with-parent",
        "--db-path",
        db,
    ];

    let mut first = Launched::spawn(serve(&args), Stdio::piped());
    let endpoint = first.ready_line()["endpoint"].as_str().unwrap().to_owned();
    let create = r#"{"TableName":"Kept","KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],"AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"BillingMode":"PAY_PER_REQUEST"}"#;
    assert_eq!(dynamo(&endpoint, "CreateTable", create).0, 200);
    let put = r#"{"TableName":"Kept","Item":{"pk":{"S":"one"}}}"#;
    assert_eq!(dynamo(&endpoint, "PutItem", put).0, 200);
    drop(first.child.stdin.take());
    let status = first
        .wait_exit(STOP_TIMEOUT)
        .expect("the first server kept running");
    assert!(status.success());
    first.finish();

    let second = Launched::spawn(serve(&args), Stdio::piped());
    let endpoint = second.ready_line()["endpoint"].as_str().unwrap().to_owned();
    let get = r#"{"TableName":"Kept","Key":{"pk":{"S":"one"}}}"#;
    let (status, body) = dynamo(&endpoint, "GetItem", get);
    assert_eq!(status, 200);
    assert!(body.contains("\"one\""), "the item was lost: {body}");
}

/// A launcher that reads stderr through a pipe loses that pipe's reader when
/// it's killed, so the shutdown that follows writes to a broken pipe. The
/// write must fail quietly: a panic there aborts the release build before the
/// database closes, leaving its write-ahead log behind.
///
/// Only the release build aborts on panic. In this debug build the panic is
/// caught in the task axum runs the shutdown future in, and the server still
/// exits cleanly, so this test passes with or without the fix. It pins the
/// behaviour; the npm package's "a write survives the runner being killed
/// outright" test runs a release build and is the one that fails without it.
#[test]
fn a_shutdown_with_no_one_reading_stderr_still_closes_the_database() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("data.db");
    let mut server = KillOnDrop(
        serve(&[
            "--port",
            "0",
            "--ready-json",
            "--exit-with-parent",
            "--db-path",
            db.to_str().unwrap(),
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn dynoxide"),
    );
    let mut stdout = BufReader::new(server.0.stdout.take().unwrap());
    let mut line = String::new();
    stdout.read_line(&mut line).unwrap();
    let ready: serde_json::Value = serde_json::from_str(&line).expect("a ready line");
    let endpoint = ready["endpoint"].as_str().unwrap().to_owned();
    let create = r#"{"TableName":"Kept","KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],"AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"BillingMode":"PAY_PER_REQUEST"}"#;
    assert_eq!(dynamo(&endpoint, "CreateTable", create).0, 200);

    // What a killed launcher leaves: nobody reading either output stream.
    drop(server.0.stderr.take());
    drop(stdout);
    drop(server.0.stdin.take());

    let deadline = Instant::now() + STOP_TIMEOUT;
    let status = loop {
        if let Some(status) = server.0.try_wait().unwrap() {
            break status;
        }
        assert!(
            Instant::now() < deadline,
            "the server kept running after stdin closed"
        );
        std::thread::sleep(Duration::from_millis(50));
    };
    assert!(status.success(), "exit status {status}");
    let wal = dir.path().join("data.db-wal");
    assert!(
        !wal.exists(),
        "the database wasn't closed: {} is still there",
        wal.display()
    );
}

#[test]
fn a_server_started_with_stdin_already_closed_stops_at_once() {
    // Passing the flag with no stdin is a launcher bug, and it shows up as a
    // server that stops as soon as it starts, rather than as a hang.
    let mut server = Launched::spawn(
        serve(&["--port", "0", "--ready-json", "--exit-with-parent"]),
        Stdio::null(),
    );
    let status = server
        .wait_exit(STOP_TIMEOUT)
        .expect("the server kept running with stdin closed");
    assert!(status.success(), "exit status {status}");
}

#[test]
fn without_the_flag_a_closed_stdin_changes_nothing() {
    let mut server = Launched::spawn(serve(&["--port", "0", "--ready-json"]), Stdio::null());
    let line = server.ready_line();
    std::thread::sleep(Duration::from_secs(1));
    assert!(
        server.child.try_wait().unwrap().is_none(),
        "the server exited"
    );
    assert_eq!(list_tables(line["endpoint"].as_str().unwrap()).0, 200);
}

#[test]
fn the_no_subcommand_form_exits_when_stdin_closes() {
    let mut command = dynoxide();
    command.args(["--port", "0", "--ready-json", "--exit-with-parent"]);
    let mut server = Launched::spawn(command, Stdio::piped());
    server.ready_line();
    drop(server.child.stdin.take());
    let status = server
        .wait_exit(STOP_TIMEOUT)
        .expect("the server kept running after stdin closed");
    assert!(status.success(), "exit status {status}");
}

#[cfg(feature = "mcp-server")]
#[test]
fn closing_stdin_stops_the_mcp_listener_too() {
    let mut server = Launched::spawn(
        serve(&[
            "--port",
            "0",
            "--ready-json",
            "--exit-with-parent",
            "--mcp",
            "--mcp-port",
            "0",
            "--mcp-no-auth",
        ]),
        Stdio::piped(),
    );
    server.ready_line();
    server.wait_for_stderr("MCP HTTP server listening on");
    drop(server.child.stdin.take());
    let status = server
        .wait_exit(STOP_TIMEOUT)
        .expect("the servers kept running after stdin closed");
    assert!(status.success(), "exit status {status}");
}

// --- The MCP server -------------------------------------------------------

/// Run `dynoxide mcp` with `args` and check it refuses `flag`, naming the
/// flag and the reason `reason`, with the exit code of a run-time refusal and
/// nothing on stdout.
#[cfg(feature = "mcp-server")]
fn assert_mcp_refuses(args: &[&str], flag: &str, reason: &str) {
    let out = dynoxide()
        .arg("mcp")
        .args(args)
        .stdin(Stdio::null())
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(
        out.status.code(),
        Some(1),
        "dynoxide mcp {args:?}: {stderr}"
    );
    assert!(
        stderr.contains(flag),
        "the refusal doesn't name {flag}: {stderr}"
    );
    assert!(
        stderr.contains(reason),
        "the refusal doesn't say {reason:?}: {stderr}"
    );
    assert!(
        out.stdout.is_empty(),
        "stdout: {}",
        String::from_utf8_lossy(&out.stdout)
    );
}

#[cfg(feature = "mcp-server")]
#[test]
fn the_stdio_mcp_server_refuses_the_launcher_flags() {
    assert_mcp_refuses(&["--ready-json"], "--ready-json", "stdio");
    assert_mcp_refuses(&["--exit-with-parent"], "--exit-with-parent", "stdio");
}

#[cfg(feature = "mcp-server")]
#[test]
fn the_mcp_http_transport_refuses_the_launcher_flags_and_points_to_serve() {
    assert_mcp_refuses(
        &["--http", "--port", "0", "--ready-json"],
        "--ready-json",
        "dynoxide serve --mcp",
    );
    assert_mcp_refuses(
        &["--http", "--port", "0", "--exit-with-parent"],
        "--exit-with-parent",
        "dynoxide serve --mcp",
    );
}
