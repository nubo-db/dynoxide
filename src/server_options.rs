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
    shutdown_trigger: Option<tokio::sync::watch::Receiver<bool>>,
}

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
        self.shutdown_trigger = Some(trigger);
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
        self.shutdown_trigger.clone()
    }
}
