//! Axum-based HTTP server exposing the DynamoDB JSON API.
//!
//! Only compiled with the `http-server` feature flag.

mod auth;
mod serialization_checks;

use crate::serde_errors::{deserialize, serialize};
use auth::validate_auth;
use serialization_checks::pre_check_serialization_types;

use crate::Database;
use crate::net::bind_exclusive;
use axum::{
    Router,
    body::Body,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Uri, header::SERVER},
    response::Response,
    routing::any,
};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, TcpStream};
use std::time::Duration;
use tower_http::set_header::SetResponseHeaderLayer;

/// AWS region used in the health-check response body (mirrors DynamoDB Local behaviour).
const AWS_REGION: &str = "us-east-1";

const CONTENT_TYPE: &str = "application/x-amz-json-1.0";
const TARGET_PREFIX: &str = "DynamoDB_20120810.";
const STREAMS_TARGET_PREFIX: &str = "DynamoDBStreams_20120810.";

/// Check whether the port is already in use by attempting a TCP connection.
///
/// Probes both the requested address and the cross-address (wildcard vs localhost)
/// to detect conflicts like Docker binding `0.0.0.0` when dynoxide requests `127.0.0.1`.
fn check_port_available(addr: SocketAddr) -> Result<(), String> {
    let timeout = Duration::from_millis(100);
    let port = addr.port();

    // Cross-check: if binding to loopback, also probe the wildcard (and vice versa),
    // within the same address family.
    let cross = SocketAddr::new(
        match addr.ip() {
            IpAddr::V4(ip) if ip.is_loopback() => IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            IpAddr::V4(_) => IpAddr::V4(Ipv4Addr::LOCALHOST),
            IpAddr::V6(ip) if ip.is_loopback() => IpAddr::V6(Ipv6Addr::UNSPECIFIED),
            IpAddr::V6(_) => IpAddr::V6(Ipv6Addr::LOCALHOST),
        },
        port,
    );

    for probe in [addr, cross] {
        if TcpStream::connect_timeout(&probe, timeout).is_ok() {
            return Err(format!(
                "port {port} is already in use (detected listener on {probe})"
            ));
        }
    }
    Ok(())
}

/// Start the HTTP server.
pub async fn start(host: &str, port: u16, db: Database) -> Result<(), String> {
    let addr: SocketAddr = format!("{host}:{port}")
        .parse()
        .map_err(|e| format!("invalid address {host}:{port}: {e}"))?;

    // Runs before any async tasks are spawned, so blocking connect probes are safe.
    check_port_available(addr)?;

    let std_listener = bind_exclusive(addr)?;
    let listener = tokio::net::TcpListener::from_std(std_listener)
        .map_err(|e| format!("failed to create async listener: {e}"))?;

    let app = build_router(db);

    eprintln!("Dynoxide listening on http://{addr}");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(|e| format!("server failed: {e}"))
}

/// Start on a specific listener (for tests).
pub async fn serve_on(listener: tokio::net::TcpListener, db: Database) {
    let app = build_router(db);
    axum::serve(listener, app).await.unwrap();
}

/// DynamoDB accepts bodies up to 16 MB.
const MAX_BODY_SIZE: usize = 16 * 1024 * 1024;

/// Build the shared axum router used by both `start` and `serve_on`.
fn build_router(db: Database) -> Router {
    Router::new()
        .route("/", any(handle_root))
        .fallback(handle_fallback)
        .layer(DefaultBodyLimit::max(MAX_BODY_SIZE))
        .layer(SetResponseHeaderLayer::overriding(
            SERVER,
            HeaderValue::from_static(concat!("Dynoxide/", env!("CARGO_PKG_VERSION"))),
        ))
        .layer(SetResponseHeaderLayer::overriding(
            HeaderName::from_static("x-dynoxide-version"),
            HeaderValue::from_static(env!("CARGO_PKG_VERSION")),
        ))
        .with_state(db)
}

/// The 404 body DynamoDB returns for non-POST methods.
const NOT_FOUND_BODY: &str = "<UnknownOperationException/>\n";

/// Single handler for all methods on `/`. Dispatches based on method.
///
/// - GET: health check (200)
/// - POST: DynamoDB API dispatch
/// - OPTIONS with Origin: CORS preflight response (200)
/// - OPTIONS without Origin: 404
/// - All other methods (DELETE, PUT, etc.): 404
async fn handle_root(
    method: Method,
    uri: Uri,
    State(db): State<Database>,
    headers: HeaderMap,
    body: String,
) -> Response {
    let has_origin = headers.get("origin").is_some();

    let mut resp = match method {
        Method::GET => {
            let body_str = format!("healthy: dynamodb.{AWS_REGION}.amazonaws.com ");
            dynamo_response_raw(StatusCode::OK, &body_str)
        }
        Method::OPTIONS if has_origin => {
            // CORS preflight: return 200 with CORS headers
            let mut r = Response::builder()
                .status(StatusCode::OK)
                .body(Body::from(""))
                .unwrap();
            add_dynamo_headers(&mut r, b"");
            // Set content-length to 0 explicitly
            r.headers_mut().insert(
                HeaderName::from_static("content-length"),
                HeaderValue::from_static("0"),
            );
            r.headers_mut().insert(
                HeaderName::from_static("access-control-allow-origin"),
                HeaderValue::from_static("*"),
            );
            r.headers_mut().insert(
                HeaderName::from_static("access-control-max-age"),
                HeaderValue::from_static("172800"),
            );
            // Echo back request headers and method if present
            if let Some(req_headers) = headers.get("access-control-request-headers") {
                r.headers_mut().insert(
                    HeaderName::from_static("access-control-allow-headers"),
                    req_headers.clone(),
                );
            }
            if let Some(req_method) = headers.get("access-control-request-method") {
                r.headers_mut().insert(
                    HeaderName::from_static("access-control-allow-methods"),
                    req_method.clone(),
                );
            }
            return r;
        }
        Method::POST => handle_request(uri, State(db), headers.clone(), body).await,
        _ => {
            // OPTIONS without Origin, DELETE, PUT, PATCH, etc. — all return 404.
            dynamo_response_raw(StatusCode::NOT_FOUND, NOT_FOUND_BODY)
        }
    };

    // Add CORS header to all responses if Origin is present
    if has_origin {
        resp.headers_mut().insert(
            HeaderName::from_static("access-control-allow-origin"),
            HeaderValue::from_static("*"),
        );
    }

    resp
}

/// Fallback for all unmatched routes — returns 404.
async fn handle_fallback() -> Response {
    dynamo_response_raw(StatusCode::NOT_FOUND, NOT_FOUND_BODY)
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigterm =
            signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = sigterm.recv() => {},
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install CTRL+C handler");
    }
    eprintln!("\nShutting down...");
}

async fn handle_request(
    uri: Uri,
    State(db): State<Database>,
    headers: HeaderMap,
    body: String,
) -> Response {
    // Check Content-Type header — DynamoDB accepts both application/json and
    // application/x-amz-json-1.0, with optional parameters (e.g. ;charset=utf-8).
    // The response Content-Type echoes the base media type from the request.
    let raw_ct = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    // Strip parameters and whitespace: "  application/json  ; charset=utf-8" → "application/json"
    let base_ct = raw_ct.split(';').next().unwrap_or("").trim();

    let is_amz_json = base_ct.eq_ignore_ascii_case(CONTENT_TYPE);
    let is_plain_json = base_ct.eq_ignore_ascii_case("application/json");

    // If neither recognised Content-Type AND there is a body, return 404.
    // A POST with no body (or empty body) and no Content-Type is still treated
    // as a DynamoDB request (DynamoDB accepts it and parses the empty body as JSON).
    if !is_amz_json && !is_plain_json && (!body.is_empty() || !raw_ct.is_empty()) {
        return dynamo_response_raw(StatusCode::NOT_FOUND, NOT_FOUND_BODY);
    }

    // Determine response content-type: echo the request's base media type.
    // application/x-amz-json-1.0 requests get that back; everything else gets application/json.
    let response_ct = if is_amz_json {
        CONTENT_TYPE
    } else {
        "application/json"
    };

    // Try to parse body as JSON. DynamoDB requires a valid JSON object.
    // Non-JSON → SerializationException (no message).
    if !body.is_empty() && serde_json::from_str::<serde_json::Value>(&body).is_err() {
        return serialization_exception_bare(response_ct);
    }

    // Check x-amz-target header
    // NOTE: empty body check happens after target resolution — DynamoDB returns
    // UnknownOperationException if no target, even with empty body.
    let target = match headers.get("x-amz-target").and_then(|v| v.to_str().ok()) {
        Some(t) => t,
        None => {
            // No target header — UnknownOperationException (no message)
            return unknown_operation_response(response_ct);
        }
    };

    let operation = target
        .strip_prefix(TARGET_PREFIX)
        .or_else(|| target.strip_prefix(STREAMS_TARGET_PREFIX));

    let operation = match operation {
        Some(op) if crate::dynamo_ops::is_known_operation(op) => op,
        _ => {
            // Unrecognised target prefix or unknown operation
            return unknown_operation_response(response_ct);
        }
    };

    // Validate authentication headers — DynamoDB checks auth after target resolution.
    if let Some(auth_error) = validate_auth(&headers, &uri, response_ct) {
        return auth_error;
    }

    // Empty body with a valid target → SerializationException (bare, no message).
    // DynamoDB requires a JSON body for all operations.
    if body.is_empty() {
        return serialization_exception_bare(response_ct);
    }

    tracing::debug!(operation, body_len = body.len(), "request");
    tracing::trace!(operation, body = %body, "request body");

    match dispatch(&db, operation, &body) {
        Ok(json) => {
            tracing::debug!(operation, body_len = json.len(), "response");
            tracing::trace!(operation, body = %json, "response body");
            dynamo_response(StatusCode::OK, response_ct, json)
        }
        Err(e) => {
            let status = StatusCode::from_u16(e.status_code()).unwrap_or(StatusCode::BAD_REQUEST);
            let json = e.to_json();
            tracing::warn!(operation, status = %status, "error response");
            tracing::trace!(operation, body = %json, "error response body");
            dynamo_response(status, response_ct, json)
        }
    }
}

/// SerializationException with no message (just `__type`).
/// Used for JSON parse failures at the connection level.
fn serialization_exception_bare(content_type: &str) -> Response {
    let body = r#"{"__type":"com.amazon.coral.service#SerializationException"}"#.to_string();
    dynamo_response(StatusCode::BAD_REQUEST, content_type, body)
}

/// UnknownOperationException with no message (just `__type`).
fn unknown_operation_response(content_type: &str) -> Response {
    let body = r#"{"__type":"com.amazon.coral.service#UnknownOperationException"}"#.to_string();
    dynamo_response(StatusCode::BAD_REQUEST, content_type, body)
}

fn dispatch(db: &Database, operation: &str, body: &str) -> crate::Result<String> {
    let result = dispatch_operation(db, operation, body);

    // DynamoDB wraps the request-validation family in the
    // "1 validation error detected: " envelope on PutItem and UpdateItem and
    // reports it bare everywhere else. Either way the wire-invisible
    // EnvelopedValidation tag must never escape dispatch: enveloping is
    // idempotent for untagged errors, and untagging leaves everything else
    // unchanged.
    match operation {
        "PutItem" | "UpdateItem" => result.map_err(crate::validation::envelope_request_validation),
        _ => result.map_err(crate::validation::strip_request_validation_tag),
    }
}

fn dispatch_operation(db: &Database, operation: &str, body: &str) -> crate::Result<String> {
    // Pre-check JSON field types for operations that use serde_json::Value internally.
    // These checks must run before serde deserialisation because serde_json::Value accepts
    // any JSON type, so type mismatches on list/struct fields would silently pass through.
    pre_check_serialization_types(operation, body)?;

    match operation {
        "CreateTable" => {
            let req = deserialize(body)?;
            let resp = db.create_table(req)?;
            serialize(&resp)
        }
        "DeleteTable" => {
            let req = deserialize(body)?;
            let resp = db.delete_table(req)?;
            serialize(&resp)
        }
        "DescribeTable" => {
            let req = deserialize(body)?;
            let resp = db.describe_table(req)?;
            serialize(&resp)
        }
        "ListTables" => {
            let req = deserialize(body)?;
            let resp = db.list_tables(req)?;
            serialize(&resp)
        }
        "UpdateTable" => {
            let req = deserialize(body)?;
            let resp = db.update_table(req)?;
            serialize(&resp)
        }
        "PutItem" => {
            let req = deserialize(body)?;
            let resp = db.put_item(req)?;
            serialize(&resp)
        }
        "GetItem" => {
            let req = deserialize(body)?;
            let resp = db.get_item(req)?;
            serialize(&resp)
        }
        "DeleteItem" => {
            let req = deserialize(body)?;
            let resp = db.delete_item(req)?;
            serialize(&resp)
        }
        "UpdateItem" => {
            let req = deserialize(body)?;
            let resp = db.update_item(req)?;
            serialize(&resp)
        }
        "Query" => {
            let req = deserialize(body)?;
            let resp = db.query(req)?;
            serialize(&resp)
        }
        "Scan" => {
            let req = deserialize(body)?;
            let resp = db.scan(req)?;
            serialize(&resp)
        }
        "BatchGetItem" => {
            let req = deserialize(body)?;
            let resp = db.batch_get_item(req)?;
            serialize(&resp)
        }
        "BatchWriteItem" => {
            let req = deserialize(body)?;
            let resp = db.batch_write_item(req)?;
            serialize(&resp)
        }
        "TransactWriteItems" => {
            let req = deserialize(body)?;
            let resp = db.transact_write_items(req)?;
            serialize(&resp)
        }
        "TransactGetItems" => {
            let req = deserialize(body)?;
            let resp = db.transact_get_items(req)?;
            serialize(&resp)
        }
        "ListStreams" => {
            let req = deserialize(body)?;
            let resp = db.list_streams(req)?;
            serialize(&resp)
        }
        "DescribeStream" => {
            let req = deserialize(body)?;
            let resp = db.describe_stream(req)?;
            serialize(&resp)
        }
        "GetShardIterator" => {
            let req = deserialize(body)?;
            let resp = db.get_shard_iterator(req)?;
            serialize(&resp)
        }
        "GetRecords" => {
            let req = deserialize(body)?;
            let resp = db.get_records(req)?;
            serialize(&resp)
        }
        "UpdateTimeToLive" => {
            let req = deserialize(body)?;
            let resp = db.update_time_to_live(req)?;
            serialize(&resp)
        }
        "DescribeTimeToLive" => {
            let req = deserialize(body)?;
            let resp = db.describe_time_to_live(req)?;
            serialize(&resp)
        }
        "ExecuteStatement" => {
            let req = deserialize(body)?;
            let resp = db.execute_statement(req)?;
            serialize(&resp)
        }
        "ExecuteTransaction" => {
            let req = deserialize(body)?;
            let resp = db.execute_transaction(req)?;
            serialize(&resp)
        }
        "BatchExecuteStatement" => {
            let req = deserialize(body)?;
            let resp = db.batch_execute_statement(req)?;
            serialize(&resp)
        }
        "TagResource" => {
            let req = deserialize(body)?;
            let resp = db.tag_resource(req)?;
            serialize(&resp)
        }
        "UntagResource" => {
            let req = deserialize(body)?;
            let resp = db.untag_resource(req)?;
            serialize(&resp)
        }
        "ListTagsOfResource" => {
            let req = deserialize(body)?;
            let resp = db.list_tags_of_resource(req)?;
            serialize(&resp)
        }
        _ => {
            // This should not be reachable because the dynamo_ops::is_known_operation
            // gate on the target match filters first, but handle it defensively.
            Err(crate::DynoxideError::SerializationException(
                "UnknownOperationException".to_string(),
            ))
        }
    }
}

/// Generate a DynamoDB-style request ID: 52 uppercase hex characters.
/// Real DynamoDB uses `[0-9A-Z]{52}`.
fn generate_request_id() -> String {
    use uuid::Uuid;
    // Generate two UUIDs and concat their uppercase hex (32 chars each → 64 chars, take 52)
    let u1 = Uuid::now_v7();
    let u2 = Uuid::now_v7();
    let hex = format!(
        "{}{}",
        u1.as_simple().to_string().to_ascii_uppercase(),
        u2.as_simple().to_string().to_ascii_uppercase()
    );
    hex[..52].to_string()
}

/// Compute CRC32 of response body and return as string.
fn compute_crc32(body: &[u8]) -> String {
    crc32fast::hash(body).to_string()
}

/// Add standard DynamoDB headers to a response: x-amzn-requestid, x-amz-crc32, content-length.
fn add_dynamo_headers(response: &mut Response, body_bytes: &[u8]) {
    let headers = response.headers_mut();
    headers.insert(
        HeaderName::from_static("x-amzn-requestid"),
        HeaderValue::from_str(&generate_request_id()).unwrap(),
    );
    headers.insert(
        HeaderName::from_static("x-amz-crc32"),
        HeaderValue::from_str(&compute_crc32(body_bytes)).unwrap(),
    );
    headers.insert(
        HeaderName::from_static("content-length"),
        HeaderValue::from_str(&body_bytes.len().to_string()).unwrap(),
    );
}

/// Build a response with proper DynamoDB headers (requestid, crc32, content-length).
fn dynamo_response(status: StatusCode, content_type: &str, body_str: String) -> Response {
    let body_bytes = body_str.as_bytes();
    let mut resp = Response::builder()
        .status(status)
        .header("content-type", content_type)
        .body(Body::from(body_str.clone()))
        .unwrap();
    add_dynamo_headers(&mut resp, body_bytes);
    resp
}

/// Build a response with proper DynamoDB headers for a raw byte body (e.g. HTML 404).
fn dynamo_response_raw(status: StatusCode, body_str: &str) -> Response {
    let body_bytes = body_str.as_bytes();
    let mut resp = Response::builder()
        .status(status)
        .body(Body::from(body_str.to_string()))
        .unwrap();
    add_dynamo_headers(&mut resp, body_bytes);
    resp
}
