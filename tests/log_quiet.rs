//! `--log quiet` on every CLI entry point that starts a server: `serve --mcp`,
//! `mcp` and `import --serve`/`--mcp`. The plain `serve` case lives in
//! `serve_schema.rs`.
//!
//! Each test runs the command once without `--log` and checks that every line
//! it looks for is really printed, then runs it again with `--log quiet` and
//! checks that the same lines are gone. Without the first half a test whose
//! needle matched nothing would pass on any build.
//!
//! The binary tests spawn `dynoxide` as a subprocess so the argument parsing
//! and the startup routing are exercised end to end.

#![cfg(all(feature = "http-server", feature = "mcp-server", feature = "cli"))]

mod common;

use common::{Listener, Readiness, Server, Startup};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

fn dynoxide_bin() -> &'static str {
    env!("CARGO_BIN_EXE_dynoxide")
}

fn data_model_fixture() -> String {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/onetable-test-schema.json")
        .to_string_lossy()
        .into_owned()
}

/// Announced runs read their ports from the listening lines; quiet ones print
/// none, so they are given ports picked up front.
fn startup(quiet: bool) -> Startup {
    if quiet {
        Startup::Silent
    } else {
        Startup::Announced
    }
}

#[test]
fn a_child_that_loses_its_port_is_reported_as_exited_not_as_slow() {
    // Something else already holds the port, so the child cannot bind it and
    // exits. That has to read as a lost race, which the callers start again,
    // and not as a server that never came up.
    let holder = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let taken = holder.local_addr().unwrap().port();
    let mut server = Server::spawn(command(
        &args(&["mcp", "--http", "--port", &taken.to_string()]),
        &[("DYNOXIDE_MCP_AUTH_TOKEN", "log-quiet-token")],
    ));
    let startup = server.wait_until_answering(&[(taken, "/mcp")], Duration::from_secs(10));
    assert!(
        matches!(startup, Readiness::Exited),
        "a child that cannot bind its port exits"
    );
}

#[test]
fn a_child_that_exits_before_its_listening_line_fails_with_its_stderr() {
    // The wait for a port must end when the child does, and say why, rather
    // than run out its timeout or hang on a read.
    let holder = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let taken = holder.local_addr().unwrap().port();
    let server = Server::spawn(command(
        &args(&["serve", "--port", &taken.to_string()]),
        &[],
    ));
    let started = Instant::now();
    let err = server
        .try_port(Listener::Http, Duration::from_secs(30))
        .expect_err("a child that cannot bind prints no listening line");
    assert!(
        matches!(err, common::WaitError::Exited { .. }),
        "the child exited, so the wait should say so: {err}"
    );
    assert!(
        common::is_bind_error(err.stderr()),
        "the failure should carry the child's bind error: {err}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(20),
        "the wait should end when the child exits, not at its timeout"
    );
}

#[cfg(unix)]
#[test]
fn a_listening_line_that_never_comes_fails_with_the_stderr_so_far() {
    // A child that stays up without printing the line must fail the wait at
    // its timeout, carrying whatever it did print, rather than block forever.
    let mut sh = Command::new("sh");
    sh.args(["-c", "echo 'not a listening line' >&2; exec sleep 60"]);
    let server = Server::spawn(sh);
    let started = Instant::now();
    let err = server
        .try_port(Listener::Http, Duration::from_millis(500))
        .expect_err("no listening line was printed");
    assert!(
        matches!(err, common::WaitError::TimedOut { .. }),
        "the child is still running, so the wait should time out: {err}"
    );
    assert!(
        err.stderr().contains("not a listening line"),
        "the failure should carry the child's stderr: {err}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "the wait should end at its timeout"
    );
}

#[test]
fn a_quiet_child_that_loses_its_port_is_started_again_on_another() {
    // Another process holds the first port offered. The quiet child prints no
    // listening line, cannot bind the port and exits with a bind error, so it
    // has to be started again on a fresh port rather than fail the test.
    let holder = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let taken = holder.local_addr().unwrap().port();
    let offered = std::cell::RefCell::new(Vec::new());
    let envs = [("DYNOXIDE_MCP_AUTH_TOKEN", "log-quiet-token")];

    let (ports, server) = common::start(
        |ports| {
            offered.borrow_mut().push(ports[0]);
            let port = if offered.borrow().len() == 1 {
                taken
            } else {
                ports[0]
            };
            command(
                &with_quiet(args(&["mcp", "--http", "--port", &port.to_string()])),
                &envs,
            )
        },
        &[Listener::Mcp],
        Startup::Silent,
    );
    let output = server.kill_and_collect();

    assert_eq!(
        offered.borrow().len(),
        2,
        "the child should have been started exactly twice"
    );
    assert_ne!(ports[0], taken, "the second start is on a fresh port");
    assert!(
        !common::is_bind_error(&stderr_of(&output)),
        "the second start bound its port. stderr:\n{}",
        stderr_of(&output)
    );
}

/// Wait for `child` to exit on its own, killing it after `timeout`.
fn wait_with_timeout(mut child: Child, timeout: Duration) -> Output {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if child.try_wait().expect("poll dynoxide").is_some() {
            return child.wait_with_output().expect("collect dynoxide output");
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    let _ = child.kill();
    let output = child.wait_with_output().expect("collect dynoxide output");
    panic!(
        "dynoxide did not exit within {timeout:?}; stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn stderr_of(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

/// Run an MCP stdio command to exit and return its stderr.
///
/// Fails if the arguments were refused: clap exits 2 on a usage error, and a
/// refused `--log` would otherwise read as a quiet run that printed nothing.
/// Also fails if stdout carries anything but JSON-RPC, since stdout is the
/// stdio transport's channel and a stray line there breaks the client.
fn run_stdio_to_exit(args: &[String], timeout: Duration) -> String {
    let output = wait_with_timeout(spawn(args, &[]), timeout);
    let stderr = stderr_of(&output);
    assert_ne!(
        output.status.code(),
        Some(2),
        "dynoxide refused the arguments {args:?}; stderr:\n{stderr}"
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    for line in stdout.lines().filter(|l| !l.trim().is_empty()) {
        let message: serde_json::Value = serde_json::from_str(line)
            .unwrap_or_else(|e| panic!("non-JSON line on the stdio channel ({e}): {line:?}"));
        assert_eq!(
            message["jsonrpc"], "2.0",
            "non-JSON-RPC message on the stdio channel: {line:?}"
        );
    }
    stderr
}

/// The command for `dynoxide` with stdin closed, stdout and stderr piped, and
/// no MCP token inherited from the environment.
fn command(args: &[String], envs: &[(&str, &str)]) -> Command {
    let mut cmd = Command::new(dynoxide_bin());
    cmd.args(args)
        .env_remove("DYNOXIDE_MCP_AUTH_TOKEN")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for (k, v) in envs {
        cmd.env(k, v);
    }
    cmd
}

/// Spawn `dynoxide` as [`command`] describes it.
fn spawn(args: &[String], envs: &[(&str, &str)]) -> Child {
    command(args, envs).spawn().expect("spawn dynoxide")
}

/// Start a long-running server, wait until every listener answers HTTP, stop
/// it with SIGTERM and return its output. `build` gets one port per listener,
/// in order, and returns the arguments to run.
///
/// Unix only: off unix the HTTP server listens for Ctrl+C alone, which a test
/// cannot easily send to a child process.
#[cfg(unix)]
fn run_until_ready_then_term(
    build: impl Fn(&[u16]) -> Vec<String>,
    envs: &[(&str, &str)],
    listeners: &[Listener],
    startup: Startup,
) -> Output {
    common::run_until_ready_then_term(|ports| command(&build(ports), envs), listeners, startup)
}

fn args(list: &[&str]) -> Vec<String> {
    list.iter().map(|s| s.to_string()).collect()
}

fn with_quiet(mut args: Vec<String>) -> Vec<String> {
    args.extend(["--log".to_string(), "quiet".to_string()]);
    args
}

fn assert_all_present(stderr: &str, needles: &[&str], context: &str) {
    for needle in needles {
        assert!(
            stderr.contains(needle),
            "{context}: expected {needle:?} in the default output, so the quiet check means something. stderr:\n{stderr}"
        );
    }
}

fn assert_all_absent(stderr: &str, needles: &[&str], context: &str) {
    for needle in needles {
        assert!(
            !stderr.contains(needle),
            "{context}: quiet mode printed {needle:?}. stderr:\n{stderr}"
        );
    }
}

// ---------------------------------------------------------------------------
// Library surface
// ---------------------------------------------------------------------------

/// The crate-root path and the `server` path name one type, so a value built
/// through either is accepted by both servers.
#[test]
fn server_options_is_one_type_at_both_paths() {
    let quiet: dynoxide::server::ServerOptions = dynoxide::ServerOptions::quiet();
    let _round_trip: dynoxide::ServerOptions = quiet;
}

// ---------------------------------------------------------------------------
// serve --mcp
// ---------------------------------------------------------------------------

/// Every informational line `serve --mcp` prints goes under quiet, including
/// the MCP listener line and the data model line.
#[cfg(unix)]
#[test]
fn serve_mcp_quiet_suppresses_every_informational_line() {
    let needles = [
        "Starting DynamoDB HTTP server",
        "Dynoxide listening",
        "MCP HTTP server listening",
        "Loaded data model",
        "Shutting down",
    ];
    let envs = [("DYNOXIDE_MCP_AUTH_TOKEN", "log-quiet-token")];
    let model = data_model_fixture();

    let run = |quiet: bool| {
        let build = |ports: &[u16]| {
            let a = args(&[
                "serve",
                "--port",
                &ports[0].to_string(),
                "--mcp",
                "--mcp-port",
                &ports[1].to_string(),
                "--mcp-data-model",
                &model,
            ]);
            if quiet { with_quiet(a) } else { a }
        };
        let output = run_until_ready_then_term(
            build,
            &envs,
            &[Listener::Http, Listener::Mcp],
            startup(quiet),
        );
        assert!(
            output.status.success(),
            "serve --mcp did not shut down cleanly: {:?}\n{}",
            output.status,
            stderr_of(&output)
        );
        stderr_of(&output)
    };

    assert_all_present(&run(false), &needles, "serve --mcp");
    assert_all_absent(&run(true), &needles, "serve --mcp --log quiet");
}

/// The auth-disabled warning is a security notice, not information, so quiet
/// mode keeps it.
#[cfg(unix)]
#[test]
fn serve_mcp_quiet_keeps_the_auth_disabled_warning() {
    let build = |ports: &[u16]| {
        with_quiet(args(&[
            "serve",
            "--port",
            &ports[0].to_string(),
            "--mcp",
            "--mcp-port",
            &ports[1].to_string(),
            "--mcp-no-auth",
        ]))
    };
    let stderr = stderr_of(&run_until_ready_then_term(
        build,
        &[],
        &[Listener::Http, Listener::Mcp],
        Startup::Silent,
    ));
    assert!(
        stderr.contains("MCP auth disabled"),
        "quiet mode dropped the auth-disabled warning. stderr:\n{stderr}"
    );
    assert_all_absent(
        &stderr,
        &["MCP HTTP server listening", "Dynoxide listening"],
        "serve --mcp --mcp-no-auth --log quiet",
    );
}

/// The first-run message is the only place a generated token is shown, so
/// quiet mode keeps it. `HOME` and `XDG_CONFIG_HOME` point at a temp dir so
/// the token lands there rather than in the real per-user config. Every start
/// gets its own temp dir, so a start on fresh ports after a lost one is still
/// a first run.
#[cfg(unix)]
#[test]
fn serve_mcp_quiet_keeps_the_first_run_token_message() {
    let homes = std::cell::RefCell::new(Vec::new());
    let build = |ports: &[u16]| {
        let home = tempfile::tempdir().unwrap();
        let home_path = home.path().to_string_lossy().into_owned();
        let config_path = home.path().join(".config").to_string_lossy().into_owned();
        homes.borrow_mut().push(home);
        let a = with_quiet(args(&[
            "serve",
            "--port",
            &ports[0].to_string(),
            "--mcp",
            "--mcp-port",
            &ports[1].to_string(),
        ]));
        let mut cmd = command(&a, &[]);
        cmd.env("HOME", home_path)
            .env("XDG_CONFIG_HOME", config_path);
        cmd
    };
    let stderr = stderr_of(&common::run_until_ready_then_term(
        build,
        &[Listener::Http, Listener::Mcp],
        Startup::Silent,
    ));

    assert!(
        stderr.contains("Generated an MCP auth token"),
        "quiet mode dropped the first-run token message. stderr:\n{stderr}"
    );
    let homes = homes.borrow();
    let home = homes.last().expect("the server was started");
    let token_file = walk(home.path())
        .into_iter()
        .find(|p| p.file_name().is_some_and(|n| n == "mcp-token"));
    assert!(
        token_file.is_some(),
        "the token should have been written under the temp HOME"
    );
}

#[cfg(unix)]
fn walk(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir).into_iter().flatten().flatten() {
        let path = entry.path();
        if path.is_dir() {
            out.extend(walk(&path));
        } else {
            out.push(path);
        }
    }
    out
}

/// Quiet mode is for information only: a failure to start still says why.
#[test]
fn quiet_mode_still_reports_errors() {
    let held = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = held.local_addr().unwrap().port();
    let a = with_quiet(args(&["serve", "--port", &port.to_string()]));
    let output = wait_with_timeout(spawn(&a, &[]), Duration::from_secs(15));
    assert!(
        !output.status.success(),
        "serve on a held port should fail to start"
    );
    let stderr = stderr_of(&output);
    assert!(
        stderr.contains("error:"),
        "quiet mode dropped the startup error. stderr:\n{stderr}"
    );
}

/// An MCP auth configuration error is refused at startup with a reason, quiet
/// or not. `HOME` points at a temp dir so no persisted token can be found.
#[test]
fn quiet_mode_still_reports_an_mcp_auth_configuration_error() {
    let home = tempfile::tempdir().unwrap();
    let home_path = home.path().to_string_lossy().into_owned();
    let config_path = home.path().join(".config").to_string_lossy().into_owned();
    let envs = [
        ("HOME", home_path.as_str()),
        ("XDG_CONFIG_HOME", config_path.as_str()),
    ];
    // The configuration is refused before either server binds, so port 0 is
    // never bound and there is no port to lose.
    let a = with_quiet(args(&[
        "serve",
        "--port",
        "0",
        "--mcp",
        "--mcp-host",
        "0.0.0.0",
        "--mcp-port",
        "0",
    ]));
    let output = wait_with_timeout(spawn(&a, &envs), Duration::from_secs(15));
    let stderr = stderr_of(&output);
    assert!(
        !output.status.success(),
        "a non-loopback MCP bind with no token should refuse to start. stderr:\n{stderr}"
    );
    assert!(
        stderr.contains("error:") && stderr.contains("requires an explicit token"),
        "quiet mode dropped the auth configuration error. stderr:\n{stderr}"
    );
}

// ---------------------------------------------------------------------------
// mcp
// ---------------------------------------------------------------------------

/// `mcp --http` prints the same listener line as `serve --mcp`, from the same
/// function, so it obeys quiet mode too. The child is stopped only once it
/// answers HTTP, which comes after the line would have been printed.
#[test]
fn mcp_http_quiet_suppresses_the_listener_line() {
    let envs = [("DYNOXIDE_MCP_AUTH_TOKEN", "log-quiet-token")];
    // The default run is on port 0 and its port comes from the listening
    // line. The quiet run prints no line, so it is given a port picked up
    // front, and another process can take that port before the child binds
    // it. That is a lost race, not a regression, and is tried again on a fresh
    // port.
    let run = |quiet: bool| {
        let (_, server) = common::start(
            |ports| {
                let a = args(&["mcp", "--http", "--port", &ports[0].to_string()]);
                command(&if quiet { with_quiet(a) } else { a }, &envs)
            },
            &[Listener::Mcp],
            startup(quiet),
        );
        stderr_of(&server.kill_and_collect())
    };

    let needles = ["MCP HTTP server listening"];
    assert_all_present(&run(false), &needles, "mcp --http");
    assert_all_absent(&run(true), &needles, "mcp --http --log quiet");
}

/// Over stdio, the only informational line is the data model one. stdin is
/// closed, so the transport ends at once; the assertion is on that line only.
#[test]
fn mcp_stdio_quiet_suppresses_the_data_model_line() {
    let model = data_model_fixture();
    let run = |quiet: bool| {
        let mut a = args(&["mcp", "--data-model", &model]);
        if quiet {
            a = with_quiet(a);
        }
        run_stdio_to_exit(&a, Duration::from_secs(15))
    };

    let needles = ["Loaded data model"];
    assert_all_present(&run(false), &needles, "mcp --data-model");
    assert_all_absent(&run(true), &needles, "mcp --data-model --log quiet");
}

#[test]
fn mcp_help_lists_log() {
    let output = Command::new(dynoxide_bin())
        .args(["mcp", "--help"])
        .env_remove("DYNOXIDE_MCP_AUTH_TOKEN")
        .output()
        .expect("spawn dynoxide");
    assert!(output.status.success(), "--help should exit 0");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("--log <MODE>"), "missing --log:\n{stdout}");
}

// ---------------------------------------------------------------------------
// import --serve / --mcp
// ---------------------------------------------------------------------------

#[cfg(feature = "import")]
mod import {
    use super::*;
    use std::io::Write;

    /// A one-table export plus its schema, under a temp dir that lives as long
    /// as the returned guard.
    pub(super) struct Export {
        _dir: tempfile::TempDir,
        pub source: String,
        pub schema: String,
        pub rules: String,
    }

    pub(super) fn export() -> Export {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("export");
        let data = source.join("Users").join("data");
        std::fs::create_dir_all(&data).unwrap();
        let mut f = std::fs::File::create(data.join("00000000.json")).unwrap();
        writeln!(
            f,
            r#"{{"Item": {{"pk": {{"S": "USER#1"}}, "sk": {{"S": "PROFILE"}}, "email": {{"S": "a@real.co.uk"}}}}}}"#
        )
        .unwrap();

        let schema = dir.path().join("schema.json");
        let schema_json = serde_json::json!([{
            "Table": {
                "TableName": "Users",
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "sk", "KeyType": "RANGE"}
                ],
                "AttributeDefinitions": [
                    {"AttributeName": "pk", "AttributeType": "S"},
                    {"AttributeName": "sk", "AttributeType": "S"}
                ]
            }
        }]);
        std::fs::write(&schema, serde_json::to_string(&schema_json).unwrap()).unwrap();

        // A rule whose path matches nothing, so the import reports an exposure.
        let rules = dir.path().join("rules.toml");
        std::fs::write(
            &rules,
            "[[rules]]\nmatch = \"attribute_exists(pk)\"\npath = \"emial\"\naction = { type = \"redact\" }\n",
        )
        .unwrap();

        Export {
            source: source.to_string_lossy().into_owned(),
            schema: schema.to_string_lossy().into_owned(),
            rules: rules.to_string_lossy().into_owned(),
            _dir: dir,
        }
    }

    pub(super) fn import_args(e: &Export, rest: &[&str]) -> Vec<String> {
        let mut a = args(&["import", "--source", &e.source, "--schema", &e.schema]);
        a.extend(rest.iter().map(|s| s.to_string()));
        a
    }
}

/// `import --serve` quiets the server it starts and keeps the import report.
#[cfg(all(unix, feature = "import"))]
#[test]
fn import_serve_quiet_suppresses_server_lines_and_keeps_the_report() {
    let e = import::export();
    let run = |quiet: bool| {
        let build = |ports: &[u16]| {
            let a = import::import_args(&e, &["--serve", "--port", &ports[0].to_string()]);
            if quiet { with_quiet(a) } else { a }
        };
        let output = run_until_ready_then_term(build, &[], &[Listener::Http], startup(quiet));
        assert!(
            output.status.success(),
            "import --serve did not shut down cleanly: {:?}\n{}",
            output.status,
            stderr_of(&output)
        );
        stderr_of(&output)
    };

    let needles = ["Dynoxide listening", "Shutting down"];
    assert_all_present(&run(false), &needles, "import --serve");
    let quiet = run(true);
    assert_all_absent(&quiet, &needles, "import --serve --log quiet");
    assert!(
        quiet.contains("Import complete:"),
        "quiet mode dropped the import summary. stderr:\n{quiet}"
    );
}

/// `import --serve --mcp` quiets both servers and the combined start line.
#[cfg(all(unix, feature = "import"))]
#[test]
fn import_serve_mcp_quiet_suppresses_every_server_line() {
    let e = import::export();
    let envs = [("DYNOXIDE_MCP_AUTH_TOKEN", "log-quiet-token")];
    let run = |quiet: bool| {
        let build = |ports: &[u16]| {
            let a = import::import_args(
                &e,
                &[
                    "--serve",
                    "--port",
                    &ports[0].to_string(),
                    "--mcp",
                    "--mcp-port",
                    &ports[1].to_string(),
                ],
            );
            if quiet { with_quiet(a) } else { a }
        };
        let output = run_until_ready_then_term(
            build,
            &envs,
            &[Listener::Http, Listener::Mcp],
            startup(quiet),
        );
        assert!(
            output.status.success(),
            "import --serve --mcp did not shut down cleanly: {:?}\n{}",
            output.status,
            stderr_of(&output)
        );
        stderr_of(&output)
    };

    let needles = [
        "Starting DynamoDB HTTP server",
        "Dynoxide listening",
        "MCP HTTP server listening",
        "Shutting down",
    ];
    assert_all_present(&run(false), &needles, "import --serve --mcp");
    let quiet = run(true);
    assert_all_absent(&quiet, &needles, "import --serve --mcp --log quiet");
    assert!(
        quiet.contains("Import complete:"),
        "quiet mode dropped the import summary. stderr:\n{quiet}"
    );
}

/// `import --mcp` over stdio: the data model line from `--mcp-data-model`
/// goes under quiet. `--data-model` is avoided because the import itself
/// prints its own line for it, and that line is part of the report.
#[cfg(feature = "import")]
#[test]
fn import_mcp_stdio_quiet_suppresses_the_data_model_line() {
    let e = import::export();
    let model = data_model_fixture();
    let run = |quiet: bool| {
        let mut a = import::import_args(&e, &["--mcp", "--mcp-data-model", &model]);
        if quiet {
            a = with_quiet(a);
        }
        run_stdio_to_exit(&a, Duration::from_secs(30))
    };

    let needles = ["Loaded data model"];
    assert_all_present(&run(false), &needles, "import --mcp");
    let quiet = run(true);
    assert_all_absent(&quiet, &needles, "import --mcp --log quiet");
    assert!(
        quiet.contains("Import complete:"),
        "quiet mode dropped the import summary. stderr:\n{quiet}"
    );
}

/// The exposure gate stops the run before any server starts, quiet or not.
#[cfg(feature = "import")]
#[test]
fn import_serve_quiet_still_stops_on_an_exposure() {
    let e = import::export();
    // The exposure gate stops the run before the server binds, so port 0 is
    // never bound and there is no port to lose.
    let a = with_quiet(import::import_args(
        &e,
        &["--rules", &e.rules, "--serve", "--port", "0"],
    ));
    let output = wait_with_timeout(spawn(&a, &[]), Duration::from_secs(30));
    let stderr = stderr_of(&output);
    assert_eq!(
        output.status.code(),
        Some(3),
        "an exposure should exit 3 under quiet too. stderr:\n{stderr}"
    );
    assert!(
        stderr.contains("reached the output"),
        "quiet mode dropped the exposure block. stderr:\n{stderr}"
    );
}

#[cfg(feature = "import")]
#[test]
fn import_help_lists_log() {
    let output = Command::new(dynoxide_bin())
        .args(["import", "--help"])
        .env_remove("DYNOXIDE_MCP_AUTH_TOKEN")
        .output()
        .expect("spawn dynoxide");
    assert!(output.status.success(), "--help should exit 0");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("--log <MODE>"), "missing --log:\n{stdout}");
}
