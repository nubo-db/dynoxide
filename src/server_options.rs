//! Startup options shared by the DynamoDB HTTP server and the MCP HTTP
//! transport.
//!
//! Lives outside both server modules because either can be compiled without
//! the other (`mcp-server` without `http-server` is a supported build).

use std::io::Write;

/// Writes an informational line to stderr, ignoring a failed write.
///
/// `eprintln!` panics when the write fails, and stderr fails as soon as a
/// launcher that read it through a pipe has been killed. The panic would land
/// in the shutdown that follows, and the release build aborts on panic, so the
/// database would never close. A line nobody can read isn't worth that.
#[doc(hidden)]
pub fn stderr_line(args: std::fmt::Arguments<'_>) {
    // One lock for the whole line, as `eprintln!` takes, so a log line on
    // stderr can't land between the message and its newline.
    write_line(&mut std::io::stderr().lock(), args);
}

fn write_line(out: &mut impl Write, args: std::fmt::Arguments<'_>) {
    let _ = writeln!(out, "{args}");
}

/// Options controlling the output of the DynamoDB HTTP server
/// (`server::start_with_options`) and the MCP HTTP transport
/// (`mcp::serve_http_with_options`).
#[derive(Debug, Clone, Default)]
pub struct ServerOptions {
    suppress_informational_messages: bool,
    ready_line: bool,
    shutdown_trigger: Option<ShutdownTrigger>,
}

/// The receiving end of a shutdown request.
///
/// A wrapper rather than the bare receiver so `ServerOptions` keeps the
/// unwind-safety auto traits it has always had: the receiver's shared state
/// holds an `UnsafeCell`, which opts it out of both. Asserting them back is
/// sound because nothing here can be left half-updated by a panic: the only
/// shared value is the `bool` the sender writes, which is read whole.
#[derive(Debug, Clone)]
struct ShutdownTrigger(tokio::sync::watch::Receiver<bool>);

impl std::panic::UnwindSafe for ShutdownTrigger {}
impl std::panic::RefUnwindSafe for ShutdownTrigger {}

impl ServerOptions {
    /// Suppress informational server output: the listening lines and the
    /// shutdown line. Warnings and errors still print.
    pub const fn quiet() -> Self {
        Self {
            suppress_informational_messages: true,
            ready_line: false,
            shutdown_trigger: None,
        }
    }

    /// Print one JSON line to stdout once the DynamoDB HTTP server is
    /// listening, for a launcher to read the endpoint from. The line is
    /// printed even when informational messages are suppressed. Its fields are
    /// listed in `docs/launcher.md`. The MCP transport ignores this option.
    pub fn with_ready_line(mut self) -> Self {
        self.ready_line = true;
        self
    }

    /// Shut the DynamoDB HTTP server down gracefully once `trigger` holds
    /// `true`, in addition to Ctrl-C and SIGTERM. A trigger whose sender is
    /// dropped without sending `true` never fires. The MCP transport ignores
    /// this option.
    pub fn with_shutdown_trigger(mut self, trigger: tokio::sync::watch::Receiver<bool>) -> Self {
        self.shutdown_trigger = Some(ShutdownTrigger(trigger));
        self
    }

    pub(crate) const fn suppresses_informational_messages(&self) -> bool {
        self.suppress_informational_messages
    }

    #[cfg_attr(not(feature = "http-server"), allow(dead_code))]
    pub(crate) const fn prints_ready_line(&self) -> bool {
        self.ready_line
    }

    #[cfg_attr(not(feature = "http-server"), allow(dead_code))]
    pub(crate) fn shutdown_trigger(&self) -> Option<tokio::sync::watch::Receiver<bool>> {
        self.shutdown_trigger.as_ref().map(|t| t.0.clone())
    }
}

// `ServerOptions` is public, so the auto traits it had before are part of the
// crate's API. Losing one is a breaking change even when only methods are
// added, and nothing else in the build would notice.
const _: () = {
    fn assert_auto_traits<
        T: Send + Sync + Unpin + std::panic::UnwindSafe + std::panic::RefUnwindSafe,
    >() {
    }
    let _ = assert_auto_traits::<ServerOptions>;
};

#[cfg(test)]
mod stderr_line_tests {
    use super::write_line;
    use std::io::{self, Write};

    /// Fails every write, as stderr does once its reader has gone.
    struct BrokenPipe;

    impl Write for BrokenPipe {
        fn write(&mut self, _: &[u8]) -> io::Result<usize> {
            Err(io::ErrorKind::BrokenPipe.into())
        }
        fn flush(&mut self) -> io::Result<()> {
            Err(io::ErrorKind::BrokenPipe.into())
        }
    }

    #[test]
    fn a_line_nobody_can_read_is_dropped_without_a_panic() {
        write_line(&mut BrokenPipe, format_args!("Shutting down..."));
    }

    #[test]
    fn a_line_with_a_reader_arrives_whole() {
        let mut out = Vec::new();
        write_line(&mut out, format_args!("listening on {}", 8000));
        assert_eq!(out, b"listening on 8000\n");
    }
}
