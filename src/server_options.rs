//! Startup options shared by the DynamoDB HTTP server and the MCP HTTP
//! transport.
//!
//! Lives outside both server modules because either can be compiled without
//! the other (`mcp-server` without `http-server` is a supported build).

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
