//! MCP (Model Context Protocol) server for Dynoxide.
//!
//! Exposes DynamoDB operations as MCP tools, allowing coding agents to
//! interact with the local DynamoDB emulator through structured tool calls.

mod errors;
mod server;

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

/// Start the MCP server over Streamable HTTP transport.
///
/// Binds to `127.0.0.1:{port}` and serves MCP at `/mcp`.
/// If a `shutdown` token is provided, the server will stop when it is cancelled.
pub async fn serve_http(
    db: Database,
    port: u16,
    config: McpConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    serve_http_with_shutdown(db, port, config, None).await
}

/// Start the MCP server over Streamable HTTP with an external shutdown signal.
pub async fn serve_http_with_shutdown(
    db: Database,
    port: u16,
    config: McpConfig,
    shutdown: Option<tokio_util::sync::CancellationToken>,
) -> Result<(), Box<dyn std::error::Error>> {
    use rmcp::transport::streamable_http_server::{
        StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
    };

    let ct = shutdown.unwrap_or_default();
    let db = Arc::new(db);

    #[allow(clippy::field_reassign_with_default)]
    let http_config = {
        let mut c = StreamableHttpServerConfig::default();
        c.stateful_mode = false;
        c.json_response = true;
        c.sse_keep_alive = None;
        c.cancellation_token = ct.clone();
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

    let router = axum::Router::new().nest_service("/mcp", service);
    let addr = format!("127.0.0.1:{port}");
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
