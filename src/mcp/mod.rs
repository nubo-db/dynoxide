//! MCP (Model Context Protocol) server for Dynoxide.
//!
//! Exposes DynamoDB operations as MCP tools, allowing coding agents to
//! interact with the local DynamoDB emulator through structured tool calls.

mod auth;
mod errors;
mod server;

pub use auth::{
    AuthError, AuthMode, ResolvedAuth, first_run_message, is_loopback_host, resolve_auth,
};
pub use server::{McpConfig, McpServer};

use crate::Database;
use std::sync::Arc;

/// Start the MCP server over stdio transport.
///
/// This blocks until the client disconnects. All logging goes to stderr;
/// stdout is reserved for the JSON-RPC transport.
pub async fn serve_stdio(
    db: Database,
    config: McpConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    use rmcp::{ServiceExt, transport::stdio};

    let server = McpServer::with_config(Arc::new(db), config)
        .serve(stdio())
        .await
        .map_err(|e| format!("MCP server error: {e}"))?;

    server.waiting().await?;
    Ok(())
}

/// Transport-level options for the MCP HTTP server.
///
/// Distinct from [`McpConfig`], which is per-session server behaviour cloned
/// into every connection. These are resolved once at startup: where to bind,
/// the bearer-token auth mode, and any operator-added `Host`/`Origin` allowlist
/// entries beyond loopback.
#[derive(Clone, Debug)]
pub struct HttpOptions {
    pub host: String,
    pub port: u16,
    pub auth: AuthMode,
    /// Extra hosts to accept beyond the loopback default. Each entry also adds
    /// a matching `http://<host>` origin. Empty preserves loopback-only.
    pub extra_allowed_hosts: Vec<String>,
}

/// Wrap a bare IPv6 literal in brackets for a URL authority. Leaves IPv4,
/// hostnames, and already-bracketed literals untouched.
fn bracket_ipv6(host: &str) -> String {
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]")
    } else {
        host.to_string()
    }
}

/// Format a `host:port` bind address, bracketing bare IPv6 literals.
fn format_bind_addr(host: &str, port: u16) -> String {
    format!("{}:{}", bracket_ipv6(host), port)
}

/// Start the MCP server over Streamable HTTP transport.
///
/// Binds to `opts.host:opts.port` and serves MCP at `/mcp`, behind the
/// bearer-token auth layer.
pub async fn serve_http(
    db: Database,
    opts: HttpOptions,
    config: McpConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    serve_http_with_shutdown(db, opts, config, None).await
}

/// Start the MCP server over Streamable HTTP with an external shutdown signal.
pub async fn serve_http_with_shutdown(
    db: Database,
    opts: HttpOptions,
    config: McpConfig,
    shutdown: Option<tokio_util::sync::CancellationToken>,
) -> Result<(), Box<dyn std::error::Error>> {
    use rmcp::transport::streamable_http_server::{
        StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
    };

    let ct = shutdown.unwrap_or_default();
    let db = Arc::new(db);

    // Loopback defaults, plus any operator-added hosts (for non-loopback binds
    // reached by name). Each added host gets a matching http:// origin.
    let mut allowed_hosts: Vec<String> = vec!["localhost".into(), "127.0.0.1".into(), "::1".into()];
    let mut allowed_origins: Vec<String> =
        vec!["http://localhost".into(), "http://127.0.0.1".into()];
    for host in &opts.extra_allowed_hosts {
        allowed_hosts.push(host.clone());
        allowed_origins.push(format!("http://{}", bracket_ipv6(host)));
    }

    // load-bearing: rmcp's config struct is #[non_exhaustive], so struct-literal
    // init does not compile. Keep the field-reassign block.
    #[allow(clippy::field_reassign_with_default)]
    let http_config = {
        let mut c = StreamableHttpServerConfig::default();
        c.stateful_mode = false;
        c.json_response = true;
        c.sse_keep_alive = None;
        c.cancellation_token = ct.clone();
        // Explicit DNS rebinding defences. Stating the lists protects against an
        // rmcp default flip. Native clients pass because rmcp skips Origin
        // validation when the header is absent (rmcp 1.6.0 tower.rs:385-387).
        // Bearer-token auth (the .layer below) is the primary control once the
        // bind widens; this allowlist is defense-in-depth for browser origins.
        c.allowed_hosts = allowed_hosts;
        c.allowed_origins = allowed_origins;
        c
    };

    let service: StreamableHttpService<McpServer, LocalSessionManager> = StreamableHttpService::new(
        {
            let db = db.clone();
            let config = config.clone();
            move || Ok(McpServer::with_config(db.clone(), config.clone()))
        },
        Default::default(),
        http_config,
    );

    // Auth runs outside rmcp's Host/Origin checks: unauthenticated callers get
    // 401 regardless of Host; a token holder spoofing Host still hits 403.
    let router = axum::Router::new().nest_service("/mcp", service).layer(
        axum::middleware::from_fn_with_state(opts.auth.clone(), auth::enforce),
    );
    let addr = format_bind_addr(&opts.host, opts.port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    let local_addr = listener.local_addr()?;

    eprintln!("MCP HTTP server listening on http://{local_addr}/mcp");

    // Don't use with_graceful_shutdown for the MCP transport: persistent
    // Streamable-HTTP sessions held open by MCP clients (Claude Code, Cursor,
    // etc.) don't close on their own, so the drain phase can hang
    // indefinitely. Race the cancellation against the serve future and drop
    // the serve future on cancel; the listener closes, this function returns,
    // and the surrounding tokio::join! in main.rs proceeds so the process
    // exits.
    use std::future::IntoFuture;
    let serve_fut = axum::serve(listener, router).into_future();
    tokio::select! {
        res = serve_fut => res?,
        _ = ct.cancelled_owned() => {}
    }

    Ok(())
}
