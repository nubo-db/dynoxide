//! Starting the `dynoxide` binary as a child process and learning its ports.
//!
//! A test that picks a free port, releases it and hands it to the child with
//! `--port` races every other test in the run: anything can bind the port
//! between the release and the child's own bind, and the child then exits with
//! "Address already in use". So a child that prints its listening lines is
//! started on port 0 and the port is read back from those lines, which leaves
//! nothing to race. Quiet mode prints no listening line, so a quiet child is
//! still given ports picked up front, and is started again on fresh ones when
//! it fails to bind them.
//!
//! Each test crate uses only some of these helpers.

#![allow(dead_code)]

use std::io::{BufRead, BufReader, Read, Write};
use std::process::{Child, Command, Output, Stdio};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// The start of the DynamoDB HTTP server's listening line.
pub const HTTP_LISTENING: &str = "Dynoxide listening on http://";
/// The start of the MCP HTTP server's listening line.
pub const MCP_LISTENING: &str = "MCP HTTP server listening on http://";

/// How long a child gets to print a listening line or to start answering.
pub const STARTUP_TIMEOUT: Duration = Duration::from_secs(30);

/// How many fresh ports a quiet child is tried on before the test gives up.
const ATTEMPTS: usize = 5;

/// One of the servers a child runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Listener {
    /// The DynamoDB HTTP server, ready once `GET /` answers.
    Http,
    /// The MCP HTTP server, ready once `GET /mcp` answers.
    Mcp,
}

impl Listener {
    fn prefix(self) -> &'static str {
        match self {
            Listener::Http => HTTP_LISTENING,
            Listener::Mcp => MCP_LISTENING,
        }
    }

    fn path(self) -> &'static str {
        match self {
            Listener::Http => "/",
            Listener::Mcp => "/mcp",
        }
    }

    /// The port named by this listener's own listening line, or `None` if
    /// `line` is not that line. Panics on a listening line with no port in
    /// it, since that is the line regressing rather than a line to skip.
    fn port_in(self, line: &str) -> Option<u16> {
        let rest = line.strip_prefix(self.prefix())?;
        let addr = match self {
            Listener::Http => rest.trim_end(),
            Listener::Mcp => rest.trim_end().strip_suffix("/mcp").unwrap_or(rest),
        };
        let port = addr
            .rsplit_once(':')
            .and_then(|(_, port)| port.parse::<u16>().ok())
            .unwrap_or_else(|| panic!("no port in the listening line {line:?}"));
        Some(port)
    }
}

/// Whether the child prints its listening lines.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Startup {
    /// It does: start it on port 0 and read each port from its line.
    Announced,
    /// It does not (`--log quiet`): pick the ports up front and start it again
    /// on fresh ones if it loses one of them.
    Silent,
}

/// Why a wait for a stderr line ended without the line.
#[derive(Debug)]
pub enum WaitError {
    /// The child closed stderr, so it exited or is about to.
    Exited { stderr: String },
    /// The child stayed up for the whole wait and never printed it.
    TimedOut { stderr: String },
}

impl WaitError {
    pub fn stderr(&self) -> &str {
        match self {
            WaitError::Exited { stderr } | WaitError::TimedOut { stderr } => stderr,
        }
    }
}

impl std::fmt::Display for WaitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WaitError::Exited { stderr } => {
                write!(f, "the child exited first; stderr:\n{stderr}")
            }
            WaitError::TimedOut { stderr } => {
                write!(f, "the child never printed it; stderr:\n{stderr}")
            }
        }
    }
}

/// How a freshly spawned server got on.
#[derive(Debug)]
pub enum Readiness {
    /// Every port answers HTTP and the child is still running.
    Ready,
    /// The child exited first. A child that lost the race for a port does this.
    Exited,
    /// Some port neither answered nor did the child exit in time.
    TimedOut,
}

#[derive(Default)]
struct Captured {
    lines: Vec<String>,
    closed: bool,
}

#[derive(Default)]
struct StderrLog {
    captured: Mutex<Captured>,
    changed: Condvar,
}

/// A running `dynoxide` child whose output is read for its whole life.
///
/// stderr is read line by line on a worker thread and every line is kept, so
/// a test can wait for one line and still assert on all of them afterwards.
/// stdout is drained on another, because tracing writes there and a full pipe
/// would block the child before it reaches the line a test waits for. Waits
/// time out on a condition variable rather than on a blocking read, so a line
/// that never comes fails the test with the child's stderr instead of hanging
/// it. The child is killed when this is dropped, panics included.
pub struct Server {
    child: Child,
    stderr: Arc<StderrLog>,
    stderr_thread: Option<JoinHandle<()>>,
    stdout_thread: Option<JoinHandle<Vec<u8>>>,
}

impl Server {
    /// Spawn `command` with stdout and stderr piped to the worker threads.
    pub fn spawn(mut command: Command) -> Server {
        let mut child = command
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn dynoxide");
        let mut stdout = child.stdout.take().unwrap();
        let stderr = child.stderr.take().unwrap();

        let stdout_thread = std::thread::spawn(move || {
            let mut out = Vec::new();
            let _ = stdout.read_to_end(&mut out);
            out
        });

        let log = Arc::new(StderrLog::default());
        let writer = Arc::clone(&log);
        let stderr_thread = std::thread::spawn(move || {
            let mut reader = BufReader::new(stderr);
            let mut buf = Vec::new();
            loop {
                buf.clear();
                match reader.read_until(b'\n', &mut buf) {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {
                        let line = String::from_utf8_lossy(&buf);
                        let line = line.trim_end_matches(['\n', '\r']).to_owned();
                        writer.captured.lock().unwrap().lines.push(line);
                        writer.changed.notify_all();
                    }
                }
            }
            writer.captured.lock().unwrap().closed = true;
            writer.changed.notify_all();
        });

        Server {
            child,
            stderr: log,
            stderr_thread: Some(stderr_thread),
            stdout_thread: Some(stdout_thread),
        }
    }

    pub fn id(&self) -> u32 {
        self.child.id()
    }

    /// Every stderr line so far, one per line.
    pub fn stderr(&self) -> String {
        join_lines(&self.stderr.captured.lock().unwrap().lines)
    }

    /// Wait up to `timeout` for a stderr line that `matches`, looking at every
    /// line printed so far as well as the ones still to come.
    pub fn wait_for_line(
        &self,
        matches: impl Fn(&str) -> bool,
        timeout: Duration,
    ) -> Result<String, WaitError> {
        let deadline = Instant::now() + timeout;
        let mut captured = self.stderr.captured.lock().unwrap();
        loop {
            if let Some(line) = captured.lines.iter().find(|l| matches(l)) {
                return Ok(line.clone());
            }
            if captured.closed {
                return Err(WaitError::Exited {
                    stderr: join_lines(&captured.lines),
                });
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(WaitError::TimedOut {
                    stderr: join_lines(&captured.lines),
                });
            }
            captured = self
                .stderr
                .changed
                .wait_timeout(captured, remaining)
                .unwrap()
                .0;
        }
    }

    /// The port `listener` reports in its own listening line.
    pub fn try_port(&self, listener: Listener, timeout: Duration) -> Result<u16, WaitError> {
        let line = self.wait_for_line(|l| listener.port_in(l).is_some(), timeout)?;
        Ok(listener.port_in(&line).unwrap())
    }

    /// Like [`Server::try_port`], failing the test with the child's stderr.
    pub fn port(&self, listener: Listener) -> u16 {
        self.try_port(listener, STARTUP_TIMEOUT)
            .unwrap_or_else(|e| panic!("no {listener:?} listening line: {e}"))
    }

    /// Poll until every `(port, path)` answers HTTP, the child exits, or
    /// `timeout` elapses.
    ///
    /// A bare TCP connect is not enough. Both servers bind before they print
    /// their listening line and before they start serving, so a connect can
    /// succeed while the line is still unprinted. An HTTP answer means the
    /// serve loop is running, which comes after the line.
    pub fn wait_until_answering(&mut self, ready: &[(u16, &str)], timeout: Duration) -> Readiness {
        let deadline = Instant::now() + timeout;
        for &(port, path) in ready {
            loop {
                if self.has_exited() {
                    return Readiness::Exited;
                }
                if answers_http(port, path) {
                    break;
                }
                if Instant::now() >= deadline {
                    return Readiness::TimedOut;
                }
                std::thread::sleep(Duration::from_millis(20));
            }
        }
        // Something answers. On a port picked up front, a child that lost the
        // race for it is still on its way to exiting, and the answer is
        // another process's, so watch it for a moment before believing it.
        for _ in 0..12 {
            if self.has_exited() {
                return Readiness::Exited;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        Readiness::Ready
    }

    fn has_exited(&mut self) -> bool {
        self.child.try_wait().expect("poll dynoxide").is_some()
    }

    /// Send SIGTERM, which both servers handle by shutting down cleanly.
    ///
    /// Unix only: off unix the HTTP server listens for Ctrl+C alone, which a
    /// test cannot easily send to a child process.
    #[cfg(unix)]
    pub fn terminate(&mut self) {
        if self.has_exited() {
            return;
        }
        let signal = Command::new("kill")
            .args(["-TERM", &self.child.id().to_string()])
            .status()
            .expect("send SIGTERM");
        assert!(signal.success(), "failed to send SIGTERM");
    }

    /// Wait up to `timeout` for the child to exit on its own and return all
    /// of its output. Kills it and fails the test if it is still running.
    pub fn wait_with_output(mut self, timeout: Duration) -> Output {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if self.has_exited() {
                return self.collect();
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        let _ = self.child.kill();
        let output = self.collect();
        panic!(
            "dynoxide did not exit within {timeout:?}; stderr:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    /// Kill the child and return all of its output.
    pub fn kill_and_collect(mut self) -> Output {
        let _ = self.child.kill();
        self.collect()
    }

    fn collect(&mut self) -> Output {
        let status = self.child.wait().expect("wait for dynoxide");
        // Both readers end once the child has closed its pipes, which exiting
        // does.
        if let Some(thread) = self.stderr_thread.take() {
            let _ = thread.join();
        }
        let stdout = self
            .stdout_thread
            .take()
            .and_then(|t| t.join().ok())
            .unwrap_or_default();
        let mut stderr = self.stderr().into_bytes();
        if !stderr.is_empty() {
            stderr.push(b'\n');
        }
        Output {
            status,
            stdout,
            stderr,
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn join_lines(lines: &[String]) -> String {
    lines.join("\n")
}

/// Whether `stderr` carries the error a child prints when a port it was given
/// is already taken.
pub fn is_bind_error(stderr: &str) -> bool {
    stderr.contains("already in use")
        // Windows words it differently.
        || stderr.contains("os error 10048")
}

/// `n` distinct loopback ports that are free at the moment of asking. Every
/// listener is held until all are bound, so the set has no repeats, and all
/// are released before this returns, so another process can still take one.
pub fn free_ports(n: usize) -> Vec<u16> {
    let listeners: Vec<_> = (0..n)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    listeners
        .iter()
        .map(|l| l.local_addr().unwrap().port())
        .collect()
}

/// Send one `GET <path>` and report whether an HTTP status line came back.
pub fn answers_http(port: u16, path: &str) -> bool {
    let Ok(mut stream) = std::net::TcpStream::connect(("127.0.0.1", port)) else {
        return false;
    };
    let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
    let _ = stream.set_write_timeout(Some(Duration::from_secs(2)));
    let request =
        format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n");
    if stream.write_all(request.as_bytes()).is_err() {
        return false;
    }
    let mut head = [0u8; 9];
    stream.read_exact(&mut head).is_ok() && head.starts_with(b"HTTP/1.")
}

/// Start the child `build` describes and wait until every listener answers
/// HTTP. `build` gets one port per listener, in order. Returns those ports and
/// the running child.
///
/// An announced child is given port 0 for every listener and each port is
/// read from its own listening line. A silent child is given ports picked up
/// front and is started again on fresh ones, up to five times, when it exits
/// with a bind error. Any other failure to start fails the test with the
/// child's stderr.
pub fn start(
    build: impl Fn(&[u16]) -> Command,
    listeners: &[Listener],
    startup: Startup,
) -> (Vec<u16>, Server) {
    match startup {
        Startup::Announced => {
            let mut server = Server::spawn(build(&vec![0; listeners.len()]));
            let ports: Vec<u16> = listeners.iter().map(|&l| server.port(l)).collect();
            let ready: Vec<_> = ports
                .iter()
                .zip(listeners)
                .map(|(&port, l)| (port, l.path()))
                .collect();
            match server.wait_until_answering(&ready, STARTUP_TIMEOUT) {
                Readiness::Ready => (ports, server),
                not_ready => panic!(
                    "dynoxide never answered on {ready:?} ({not_ready:?}); stderr:\n{}",
                    server.stderr()
                ),
            }
        }
        Startup::Silent => {
            let mut lost = Vec::new();
            for _ in 0..ATTEMPTS {
                let ports = free_ports(listeners.len());
                let mut server = Server::spawn(build(&ports));
                let ready: Vec<_> = ports
                    .iter()
                    .zip(listeners)
                    .map(|(&port, l)| (port, l.path()))
                    .collect();
                let readiness = server.wait_until_answering(&ready, STARTUP_TIMEOUT);
                if let Readiness::Ready = readiness {
                    return (ports, server);
                }
                // A child that lost only one of two ports can keep serving the
                // other, so it is stopped before its stderr is judged.
                let stderr =
                    String::from_utf8_lossy(&server.kill_and_collect().stderr).into_owned();
                if !is_bind_error(&stderr) {
                    panic!(
                        "dynoxide never answered on {ready:?} ({readiness:?}); stderr:\n{stderr}"
                    );
                }
                lost.push(format!("ports {ports:?}: {stderr}"));
            }
            panic!("dynoxide lost its ports {ATTEMPTS} times over: {lost:?}");
        }
    }
}

/// Start the child `build` describes as [`start`] does, stop it with SIGTERM
/// once every listener answers, and return its output.
///
/// A silent child that lost one of two ports can still pass the readiness
/// check, since the lost port answers from whichever process took it, so a
/// bind error in its output after the stop also starts it again on fresh
/// ports.
#[cfg(unix)]
pub fn run_until_ready_then_term(
    build: impl Fn(&[u16]) -> Command,
    listeners: &[Listener],
    startup: Startup,
) -> Output {
    let mut lost = Vec::new();
    for _ in 0..ATTEMPTS {
        let (ports, mut server) = start(&build, listeners, startup);
        // The SIGTERM handler is installed by a task the HTTP server spawns
        // as it starts serving. An answered request means that task has been
        // spawned, and this pause gives the runtime time to have polled it,
        // so the signal reaches the handler rather than the default action.
        std::thread::sleep(Duration::from_millis(100));
        server.terminate();
        let output = server.wait_with_output(Duration::from_secs(15));
        let stderr = String::from_utf8_lossy(&output.stderr);
        if startup == Startup::Silent && is_bind_error(&stderr) {
            lost.push(format!("ports {ports:?}: {stderr}"));
            continue;
        }
        return output;
    }
    panic!("dynoxide lost its ports {ATTEMPTS} times over: {lost:?}");
}
