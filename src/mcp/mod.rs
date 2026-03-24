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

    let http_config = StreamableHttpServerConfig {
        stateful_mode: false,
        json_response: true,
        sse_keep_alive: None,
        cancellation_token: ct.clone(),
        ..Default::default()
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

    axum::serve(listener, router)
        .with_graceful_shutdown(async move { ct.cancelled_owned().await })
        .await?;

    Ok(())
}
