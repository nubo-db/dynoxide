//! Axum-based HTTP server exposing the DynamoDB JSON API.
//!
//! Only compiled with the `http-server` feature flag.

use crate::Database;
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

/// Bind a TCP socket without `SO_REUSEADDR` so the OS rejects the bind if the
/// port is already held by another process.
fn bind_exclusive(addr: SocketAddr) -> Result<std::net::TcpListener, String> {
    use socket2::{Domain, Protocol, Socket, Type};

    let domain = if addr.is_ipv6() {
        Domain::IPV6
    } else {
        Domain::IPV4
    };

    let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))
        .map_err(|e| format!("failed to create socket: {e}"))?;

    // Deliberately do NOT set SO_REUSEADDR — this is the whole point.
    socket
        .set_nonblocking(true)
        .map_err(|e| format!("failed to set nonblocking: {e}"))?;
    socket
        .bind(&addr.into())
        .map_err(|e| format!("failed to bind {addr}: {e}"))?;
    socket
        .listen(1024)
        .map_err(|e| format!("failed to listen on {addr}: {e}"))?;

    Ok(std::net::TcpListener::from(socket))
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
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install CTRL+C handler");
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
        Some(op) if is_known_operation(op) => op,
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

/// Validate AWS authentication headers/query parameters.
///
/// DynamoDB checks auth after resolving the target operation. Returns `Some(Response)` if
/// auth validation fails, `None` if auth is present (or if we choose to skip full
/// signature verification).
fn validate_auth(headers: &HeaderMap, uri: &Uri, response_ct: &str) -> Option<Response> {
    let auth_header = headers.get("authorization").and_then(|v| v.to_str().ok());

    // Check query string for X-Amz-Algorithm
    let query = uri.query().unwrap_or("");
    let has_algorithm_query = query.split('&').any(|p| {
        let key = p.split('=').next().unwrap_or("");
        key == "X-Amz-Algorithm"
    });

    // If both Authorization header AND X-Amz-Algorithm query → InvalidSignatureException
    if auth_header.is_some() && has_algorithm_query {
        let body = serde_json::json!({
            "__type": "com.amazon.coral.service#InvalidSignatureException",
            "message": "Found both 'X-Amz-Algorithm' as a query-string param and 'Authorization' as HTTP header."
        })
        .to_string();
        return Some(dynamo_response(StatusCode::BAD_REQUEST, response_ct, body));
    }

    // Query-string auth (X-Amz-Algorithm present)
    if has_algorithm_query {
        let mut missing = Vec::new();
        let query_params: Vec<&str> = query
            .split('&')
            .map(|p| p.split('=').next().unwrap_or(""))
            .collect();

        // Check if X-Amz-Algorithm has a non-empty value
        let algo_has_value = query.split('&').any(|p| {
            let mut parts = p.splitn(2, '=');
            let key = parts.next().unwrap_or("");
            let val = parts.next().unwrap_or("");
            key == "X-Amz-Algorithm" && !val.is_empty()
        });

        if !algo_has_value {
            missing.push("'X-Amz-Algorithm'");
        }
        for (param, label) in [
            ("X-Amz-Credential", "'X-Amz-Credential'"),
            ("X-Amz-Signature", "'X-Amz-Signature'"),
            ("X-Amz-SignedHeaders", "'X-Amz-SignedHeaders'"),
            ("X-Amz-Date", "'X-Amz-Date'"),
        ] {
            if !query_params.contains(&param) {
                missing.push(label);
            }
        }

        if !missing.is_empty() {
            let parts: Vec<String> = missing
                .iter()
                .map(|p| format!("AWS query-string parameters must include {p}. "))
                .collect();
            let msg = format!("{}Re-examine the query-string parameters.", parts.join(""));
            let body = serde_json::json!({
                "__type": "com.amazon.coral.service#IncompleteSignatureException",
                "message": msg
            })
            .to_string();
            return Some(dynamo_response(StatusCode::BAD_REQUEST, response_ct, body));
        }

        // Query auth is present and complete — allow through
        return None;
    }

    // Header-based auth
    match auth_header {
        None => {
            // No Authorization header at all → MissingAuthenticationTokenException
            let body = serde_json::json!({
                "__type": "com.amazon.coral.service#MissingAuthenticationTokenException",
                "message": "Request is missing Authentication Token"
            })
            .to_string();
            Some(dynamo_response(StatusCode::BAD_REQUEST, response_ct, body))
        }
        Some(auth) => {
            if !auth.starts_with("AWS4-") {
                // Authorization header doesn't start with AWS4- → MissingAuthenticationTokenException
                let body = serde_json::json!({
                    "__type": "com.amazon.coral.service#MissingAuthenticationTokenException",
                    "message": "Request is missing Authentication Token"
                })
                .to_string();
                return Some(dynamo_response(StatusCode::BAD_REQUEST, response_ct, body));
            }

            // AWS4- prefix present — check for required parameters
            let has_date = headers.get("x-amz-date").is_some() || headers.get("date").is_some();

            // Parse auth header for Credential, Signature, SignedHeaders
            // These can be separated by spaces or commas
            let has_credential = auth.contains("Credential=") || auth.contains("credential=");
            let has_signature = auth.contains("Signature=") || auth.contains("signature=");
            let has_signed_headers =
                auth.contains("SignedHeaders=") || auth.contains("signedheaders=");

            let mut missing = Vec::new();
            if !has_credential {
                missing.push("'Credential'");
            }
            if !has_signature {
                missing.push("'Signature'");
            }
            if !has_signed_headers {
                missing.push("'SignedHeaders'");
            }
            if !has_date {
                missing.push("existence of either a 'X-Amz-Date' or a 'Date' header.");
            }

            if missing.is_empty() {
                // All required parts present — allow through (we don't verify signatures)
                return None;
            }

            // Build the IncompleteSignatureException message
            let mut parts: Vec<String> = missing
                .iter()
                .map(|p| {
                    if p.contains("existence of") {
                        format!("Authorization header requires {p}")
                    } else {
                        format!("Authorization header requires {p} parameter.")
                    }
                })
                .collect();
            parts.push(format!("Authorization={auth}"));
            let msg = parts.join(" ");
            let body = serde_json::json!({
                "__type": "com.amazon.coral.service#IncompleteSignatureException",
                "message": msg
            })
            .to_string();
            Some(dynamo_response(StatusCode::BAD_REQUEST, response_ct, body))
        }
    }
}

/// Java ClassCastException message that DynamoDB leaks for certain type mismatches.
const PARAMETERIZED_TYPE_CAST_ERROR: &str = "class sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl cannot be cast to class java.lang.Class (sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl and java.lang.Class are in module java.base of loader 'bootstrap')";

/// Known DynamoDB operations — used to distinguish unknown targets from known ones.
fn is_known_operation(op: &str) -> bool {
    matches!(
        op,
        "CreateTable"
            | "DeleteTable"
            | "DescribeTable"
            | "ListTables"
            | "UpdateTable"
            | "PutItem"
            | "GetItem"
            | "DeleteItem"
            | "UpdateItem"
            | "Query"
            | "Scan"
            | "BatchGetItem"
            | "BatchWriteItem"
            | "TransactWriteItems"
            | "TransactGetItems"
            | "ListStreams"
            | "DescribeStream"
            | "GetShardIterator"
            | "GetRecords"
            | "UpdateTimeToLive"
            | "DescribeTimeToLive"
            | "ExecuteStatement"
            | "ExecuteTransaction"
            | "BatchExecuteStatement"
            | "TagResource"
            | "UntagResource"
            | "ListTagsOfResource"
    )
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

/// Pre-check JSON field types that are deserialized as `serde_json::Value`.
///
/// DynamoDB returns SerializationException for type mismatches on fields like
/// AttributeDefinitions, KeySchema, etc. Because our raw request structs use
/// `Option<serde_json::Value>` for these fields, serde accepts any JSON type.
/// This function inspects the raw JSON and returns the appropriate
/// SerializationException before serde gets involved.
fn pre_check_serialization_types(operation: &str, body: &str) -> crate::Result<()> {
    let json: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| crate::DynoxideError::SerializationException(e.to_string()))?;

    let obj = match json.as_object() {
        Some(o) => o,
        None => return Ok(()),
    };

    match operation {
        "CreateTable" => {
            check_field_is_list(obj, "AttributeDefinitions")?;
            check_field_is_list(obj, "KeySchema")?;
            check_field_is_list(obj, "LocalSecondaryIndexes")?;
            check_field_is_list(obj, "GlobalSecondaryIndexes")?;
            check_list_elements_are_structs(obj, "AttributeDefinitions")?;
            check_list_elements_are_structs(obj, "KeySchema")?;
            check_list_elements_are_structs(obj, "LocalSecondaryIndexes")?;
            check_list_elements_are_structs(obj, "GlobalSecondaryIndexes")?;

            // Check struct fields and their inner scalar types
            check_field_is_struct(obj, "ProvisionedThroughput")?;
            check_nested_pt_fields(obj)?;

            // Check nested fields inside KeySchema elements
            check_nested_list_structs(obj, "KeySchema")?;
            // Check nested fields inside AttributeDefinitions elements
            check_nested_list_structs(obj, "AttributeDefinitions")?;

            // Check nested list fields inside LocalSecondaryIndexes
            if let Some(serde_json::Value::Array(arr)) = obj.get("LocalSecondaryIndexes") {
                for item in arr {
                    if let Some(inner) = item.as_object() {
                        check_field_is_struct(inner, "Projection")?;
                        check_field_is_list(inner, "KeySchema")?;
                        check_list_elements_are_structs(inner, "KeySchema")?;
                        check_field_is_string(inner, "IndexName")?;
                        check_nested_list_structs(inner, "KeySchema")?;
                        check_nested_projection_fields(inner)?;
                        if let Some(proj) = inner.get("Projection").and_then(|p| p.as_object()) {
                            check_field_is_list(proj, "NonKeyAttributes")?;
                            check_nested_list_strings(proj, "NonKeyAttributes")?;
                        }
                    }
                }
            }

            // Check nested list fields inside GlobalSecondaryIndexes
            if let Some(serde_json::Value::Array(arr)) = obj.get("GlobalSecondaryIndexes") {
                for item in arr {
                    if let Some(inner) = item.as_object() {
                        check_field_is_struct(inner, "Projection")?;
                        check_field_is_struct(inner, "ProvisionedThroughput")?;
                        check_field_is_list(inner, "KeySchema")?;
                        check_list_elements_are_structs(inner, "KeySchema")?;
                        check_field_is_string(inner, "IndexName")?;
                        check_nested_list_structs(inner, "KeySchema")?;
                        check_nested_projection_fields(inner)?;
                        check_nested_pt_fields(inner)?;
                        if let Some(proj) = inner.get("Projection").and_then(|p| p.as_object()) {
                            check_field_is_list(proj, "NonKeyAttributes")?;
                            check_nested_list_strings(proj, "NonKeyAttributes")?;
                        }
                    }
                }
            }
        }
        "UpdateTable" => {
            check_field_is_list(obj, "GlobalSecondaryIndexUpdates")?;
            check_list_elements_are_structs(obj, "GlobalSecondaryIndexUpdates")?;
            check_field_is_struct(obj, "ProvisionedThroughput")?;
            check_nested_pt_fields(obj)?;
            // Check inside GlobalSecondaryIndexUpdates
            if let Some(serde_json::Value::Array(arr)) = obj.get("GlobalSecondaryIndexUpdates") {
                for item in arr {
                    if let Some(inner) = item.as_object() {
                        check_field_is_struct(inner, "Create")?;
                        check_field_is_struct(inner, "Update")?;
                        check_field_is_struct(inner, "Delete")?;
                        if let Some(create) = inner.get("Create").and_then(|v| v.as_object()) {
                            check_field_is_struct(create, "Projection")?;
                            check_field_is_struct(create, "ProvisionedThroughput")?;
                            check_field_is_list(create, "KeySchema")?;
                            check_list_elements_are_structs(create, "KeySchema")?;
                            check_nested_list_structs(create, "KeySchema")?;
                            check_nested_projection_fields(create)?;
                            check_nested_pt_fields(create)?;
                        }
                        if let Some(update) = inner.get("Update").and_then(|v| v.as_object()) {
                            check_field_is_struct(update, "ProvisionedThroughput")?;
                            check_nested_pt_fields(update)?;
                        }
                    }
                }
            }
        }
        "PutItem" | "DeleteItem" | "UpdateItem" => {
            check_field_is_map(
                obj,
                "AttributeUpdates",
                "com.amazonaws.dynamodb.v20120810.AttributeValueUpdate",
            )?;
            check_map_values_are_structs(obj, "AttributeUpdates")?;
        }
        "Query" => {
            check_field_is_map(
                obj,
                "KeyConditions",
                "com.amazonaws.dynamodb.v20120810.Condition",
            )?;
            check_field_is_map(
                obj,
                "QueryFilter",
                "com.amazonaws.dynamodb.v20120810.Condition",
            )?;
            check_map_values_are_structs(obj, "QueryFilter")?;
            check_map_values_are_structs(obj, "KeyConditions")?;
            check_filter_inner_fields(obj, "QueryFilter")?;
            check_filter_inner_fields(obj, "KeyConditions")?;
            check_filter_attribute_value_lists(obj, "QueryFilter")?;
            check_field_is_map(
                obj,
                "ExclusiveStartKey",
                "com.amazonaws.dynamodb.v20120810.AttributeValue",
            )?;
        }
        "Scan" => {
            check_field_is_map(
                obj,
                "ScanFilter",
                "com.amazonaws.dynamodb.v20120810.Condition",
            )?;
            check_map_values_are_structs(obj, "ScanFilter")?;
            check_filter_inner_fields(obj, "ScanFilter")?;
            check_filter_attribute_value_lists(obj, "ScanFilter")?;
            check_field_is_map(
                obj,
                "ExclusiveStartKey",
                "com.amazonaws.dynamodb.v20120810.AttributeValue",
            )?;
        }
        "BatchGetItem" => {
            check_field_is_map(
                obj,
                "RequestItems",
                "com.amazonaws.dynamodb.v20120810.KeysAndAttributes",
            )?;
            check_map_values_are_structs(obj, "RequestItems")?;
            // Check nested fields inside RequestItems
            if let Some(serde_json::Value::Object(ri)) = obj.get("RequestItems") {
                for (_table, val) in ri {
                    if let Some(inner) = val.as_object() {
                        check_field_is_map(inner, "ExpressionAttributeNames", "java.lang.String")?;
                        // Check Keys array elements are maps, and their values are AV structs
                        if let Some(serde_json::Value::Array(keys)) = inner.get("Keys") {
                            for key in keys {
                                if !key.is_object() && !key.is_null() {
                                    return Err(crate::DynoxideError::SerializationException(
                                        PARAMETERIZED_TYPE_CAST_ERROR.to_string(),
                                    ));
                                }
                                if let Some(key_map) = key.as_object() {
                                    for (_k, v) in key_map {
                                        if !v.is_object() && !v.is_null() {
                                            return Err(
                                                crate::DynoxideError::SerializationException(
                                                    "Unexpected value type in payload".to_string(),
                                                ),
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        "BatchWriteItem" => {
            check_field_is_map(
                obj,
                "RequestItems",
                "java.util.List<com.amazonaws.dynamodb.v20120810.WriteRequest>",
            )?;
            // Check nested fields inside RequestItems
            if let Some(serde_json::Value::Object(ri)) = obj.get("RequestItems") {
                for (_table, val) in ri {
                    // Each value must be an array of WriteRequests
                    if !val.is_array() && !val.is_null() {
                        return Err(crate::DynoxideError::SerializationException(
                            PARAMETERIZED_TYPE_CAST_ERROR.to_string(),
                        ));
                    }
                    if let Some(items) = val.as_array() {
                        // Check array elements are structs (WriteRequest)
                        for item in items {
                            if !item.is_object() && !item.is_null() {
                                let msg = if item.is_array() {
                                    "Unrecognized collection type class com.amazonaws.dynamodb.v20120810.WriteRequest".to_string()
                                } else {
                                    "Unexpected value type in payload".to_string()
                                };
                                return Err(crate::DynoxideError::SerializationException(msg));
                            }
                        }
                        for item in items {
                            if let Some(inner) = item.as_object() {
                                check_field_is_struct(inner, "DeleteRequest")?;
                                check_field_is_struct(inner, "PutRequest")?;
                                if let Some(dr) =
                                    inner.get("DeleteRequest").and_then(|v| v.as_object())
                                {
                                    check_field_is_map(
                                        dr,
                                        "Key",
                                        "com.amazonaws.dynamodb.v20120810.AttributeValue",
                                    )?;
                                    check_map_values_are_structs(dr, "Key")?;
                                }
                                if let Some(pr) =
                                    inner.get("PutRequest").and_then(|v| v.as_object())
                                {
                                    check_field_is_map(
                                        pr,
                                        "Item",
                                        "com.amazonaws.dynamodb.v20120810.AttributeValue",
                                    )?;
                                    check_map_values_are_structs(pr, "Item")?;
                                }
                            }
                        }
                    }
                }
            }
        }
        "TagResource" => {
            check_field_is_list(obj, "Tags")?;
            check_list_elements_are_structs(obj, "Tags")?;
        }
        _ => {}
    }

    // Common map fields — checked AFTER operation-specific nested fields
    check_field_is_map(
        obj,
        "Key",
        "com.amazonaws.dynamodb.v20120810.AttributeValue",
    )?;
    check_field_is_map(
        obj,
        "Item",
        "com.amazonaws.dynamodb.v20120810.AttributeValue",
    )?;
    check_field_is_map(obj, "ExpressionAttributeNames", "java.lang.String")?;
    check_field_is_map(
        obj,
        "ExpressionAttributeValues",
        "com.amazonaws.dynamodb.v20120810.AttributeValue",
    )?;
    check_field_is_map(
        obj,
        "Expected",
        "com.amazonaws.dynamodb.v20120810.ExpectedAttributeValue",
    )?;

    // Check that attribute value map entries are structs (not scalars)
    check_map_values_are_structs(obj, "Key")?;
    check_map_values_are_structs(obj, "Item")?;
    check_map_values_are_structs(obj, "ExpressionAttributeValues")?;
    check_map_values_are_structs(obj, "ExclusiveStartKey")?;
    check_map_values_are_structs(obj, "Expected")?;

    // Check Expected.Attr inner fields
    if let Some(serde_json::Value::Object(expected)) = obj.get("Expected") {
        for (_attr, cond) in expected {
            if let Some(cond_obj) = cond.as_object() {
                check_field_is_bool(cond_obj, "Exists")?;
            }
        }
    }

    // Common scalar fields — checked AFTER nested fields to match DynamoDB ordering
    check_field_is_string(obj, "TableName")?;
    check_field_is_string(obj, "IndexName")?;
    check_field_is_string(obj, "ReturnConsumedCapacity")?;
    check_field_is_string(obj, "ReturnValues")?;
    check_field_is_string(obj, "ReturnItemCollectionMetrics")?;
    check_field_is_string(obj, "ConditionalOperator")?;
    check_field_is_string(obj, "Select")?;
    check_field_is_string(obj, "ConditionExpression")?;
    check_field_is_string(obj, "FilterExpression")?;
    check_field_is_string(obj, "KeyConditionExpression")?;
    check_field_is_string(obj, "ProjectionExpression")?;
    check_field_is_string(obj, "UpdateExpression")?;
    check_field_is_int(obj, "Limit")?;
    check_field_is_int(obj, "Segment")?;
    check_field_is_int(obj, "TotalSegments")?;
    check_field_is_bool(obj, "ScanIndexForward")?;
    check_field_is_bool(obj, "ConsistentRead")?;

    Ok(())
}

/// Check that a field, if present, is a JSON number (integer).
/// `java_type` is "Long" for PT fields, "Integer" for Limit/Segment/etc.
fn check_field_is_integer_typed(
    obj: &serde_json::Map<String, serde_json::Value>,
    field: &str,
    java_type: &str,
) -> crate::Result<()> {
    let val = match obj.get(field) {
        Some(v) if !v.is_null() => v,
        _ => return Ok(()),
    };

    if val.is_number() {
        return Ok(());
    }

    let msg = if val.is_array() {
        format!("Unrecognized collection type class java.lang.{java_type}")
    } else if val.is_object() {
        "Start of structure or map found where not expected".to_string()
    } else if val.is_boolean() {
        if val.as_bool() == Some(true) {
            format!("TRUE_VALUE cannot be converted to {java_type}")
        } else {
            format!("FALSE_VALUE cannot be converted to {java_type}")
        }
    } else if val.is_string() {
        format!("STRING_VALUE cannot be converted to {java_type}")
    } else {
        "Unexpected field type".to_string()
    };

    Err(crate::DynoxideError::SerializationException(msg))
}

/// Check integer field using "Long" type (for PT fields).
fn check_field_is_integer(
    obj: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> crate::Result<()> {
    check_field_is_integer_typed(obj, field, "Long")
}

/// Check integer field using "Integer" type (for Limit, Segment, etc.).
fn check_field_is_int(
    obj: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> crate::Result<()> {
    check_field_is_integer_typed(obj, field, "Integer")
}

/// Check that a field, if present and not null, is a JSON string.
/// Returns SerializationException for wrong types.
fn check_field_is_string(
    obj: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> crate::Result<()> {
    let val = match obj.get(field) {
        Some(v) if !v.is_null() => v,
        _ => return Ok(()),
    };

    if val.is_string() {
        return Ok(());
    }

    let msg = if val.is_array() {
        "Unrecognized collection type class java.lang.String".to_string()
    } else if val.is_object() {
        "Start of structure or map found where not expected".to_string()
    } else if val.as_bool() == Some(true) {
        "TRUE_VALUE cannot be converted to String".to_string()
    } else if val.as_bool() == Some(false) {
        "FALSE_VALUE cannot be converted to String".to_string()
    } else if val.is_number() {
        // DynamoDB distinguishes DECIMAL_VALUE (float) from NUMBER_VALUE (int)
        if val.is_f64() && !val.is_i64() && !val.is_u64() {
            "DECIMAL_VALUE cannot be converted to String".to_string()
        } else {
            "NUMBER_VALUE cannot be converted to String".to_string()
        }
    } else {
        "Unexpected field type".to_string()
    };

    Err(crate::DynoxideError::SerializationException(msg))
}

/// Check that a field, if present and not null, is a JSON boolean.
/// Returns SerializationException for wrong types.
fn check_field_is_bool(
    obj: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> crate::Result<()> {
    let val = match obj.get(field) {
        Some(v) if !v.is_null() => v,
        _ => return Ok(()),
    };

    if val.is_boolean() {
        return Ok(());
    }

    let msg = if val.is_array() {
        "Unrecognized collection type class java.lang.Boolean".to_string()
    } else if val.is_object() {
        "Start of structure or map found where not expected".to_string()
    } else if val.is_string() {
        "Unexpected token received from parser".to_string()
    } else if val.is_number() {
        if val.is_f64() && !val.is_i64() && !val.is_u64() {
            "DECIMAL_VALUE cannot be converted to Boolean".to_string()
        } else {
            "NUMBER_VALUE cannot be converted to Boolean".to_string()
        }
    } else {
        "Unexpected field type".to_string()
    };

    Err(crate::DynoxideError::SerializationException(msg))
}

/// Check that all elements in a list field are JSON objects (structs).
/// Returns "Unexpected value type in payload" for non-struct elements.
fn check_list_elements_are_structs(
    obj: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> crate::Result<()> {
    let java_class = match field {
        "KeySchema" => "com.amazonaws.dynamodb.v20120810.KeySchemaElement",
        "AttributeDefinitions" => "com.amazonaws.dynamodb.v20120810.AttributeDefinition",
        "LocalSecondaryIndexes" => "com.amazonaws.dynamodb.v20120810.LocalSecondaryIndex",
        "GlobalSecondaryIndexes" => "com.amazonaws.dynamodb.v20120810.GlobalSecondaryIndex",
        "GlobalSecondaryIndexUpdates" => {
            "com.amazonaws.dynamodb.v20120810.GlobalSecondaryIndexUpdate"
        }
        "Tags" => "com.amazonaws.dynamodb.v20120810.Tag",
        _ => "Unknown",
    };
    if let Some(serde_json::Value::Array(arr)) = obj.get(field) {
        for item in arr {
            if !item.is_object() && !item.is_null() {
                let msg = if item.is_array() {
                    format!("Unrecognized collection type class {java_class}")
                } else {
                    "Unexpected value type in payload".to_string()
                };
                return Err(crate::DynoxideError::SerializationException(msg));
            }
        }
    }
    Ok(())
}

/// Check scalar fields inside a ProvisionedThroughput struct.
fn check_nested_pt_fields(obj: &serde_json::Map<String, serde_json::Value>) -> crate::Result<()> {
    if let Some(pt) = obj.get("ProvisionedThroughput").and_then(|v| v.as_object()) {
        check_field_is_integer(pt, "WriteCapacityUnits")?;
        check_field_is_integer(pt, "ReadCapacityUnits")?;
    }
    Ok(())
}

/// Check scalar fields inside a Projection struct.
fn check_nested_projection_fields(
    obj: &serde_json::Map<String, serde_json::Value>,
) -> crate::Result<()> {
    if let Some(proj) = obj.get("Projection").and_then(|v| v.as_object()) {
        check_field_is_string(proj, "ProjectionType")?;
    }
    Ok(())
}

/// Check that elements inside a list field are structs, and check their scalar fields.
fn check_nested_list_structs(
    obj: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> crate::Result<()> {
    if let Some(serde_json::Value::Array(arr)) = obj.get(field) {
        for item in arr {
            if let Some(inner) = item.as_object() {
                // Common struct fields in KeySchema/AttributeDefinitions elements
                check_field_is_string(inner, "KeyType")?;
                check_field_is_string(inner, "AttributeName")?;
                check_field_is_string(inner, "AttributeType")?;
                check_field_is_string(inner, "IndexName")?;
            }
        }
    }
    Ok(())
}

/// Check that elements inside a string list field are actually strings.
fn check_nested_list_strings(
    obj: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> crate::Result<()> {
    if let Some(serde_json::Value::Array(arr)) = obj.get(field) {
        for item in arr {
            if !item.is_string() && !item.is_null() {
                if item.is_boolean() {
                    let val = if item.as_bool() == Some(true) {
                        "TRUE_VALUE"
                    } else {
                        "FALSE_VALUE"
                    };
                    return Err(crate::DynoxideError::SerializationException(format!(
                        "{val} cannot be converted to String"
                    )));
                } else if item.is_number() {
                    return Err(crate::DynoxideError::SerializationException(
                        "NUMBER_VALUE cannot be converted to String".to_string(),
                    ));
                }
            }
        }
    }
    Ok(())
}

/// Check that all values in a map field (if present) are JSON objects (attribute value structs).
fn check_map_values_are_structs(
    obj: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> crate::Result<()> {
    let java_class = match field {
        "Key" | "Item" | "ExpressionAttributeValues" | "ExclusiveStartKey" => {
            "com.amazonaws.dynamodb.v20120810.AttributeValue"
        }
        "Expected" => "com.amazonaws.dynamodb.v20120810.ExpectedAttributeValue",
        "AttributeUpdates" => "com.amazonaws.dynamodb.v20120810.AttributeValueUpdate",
        "RequestItems" => "com.amazonaws.dynamodb.v20120810.KeysAndAttributes",
        "KeyConditions" | "QueryFilter" | "ScanFilter" => {
            "com.amazonaws.dynamodb.v20120810.Condition"
        }
        _ => "Unknown",
    };
    if let Some(serde_json::Value::Object(map)) = obj.get(field) {
        for (_key, val) in map {
            if !val.is_object() && !val.is_null() {
                let msg = if val.is_array() {
                    format!("Unrecognized collection type class {java_class}")
                } else {
                    "Unexpected value type in payload".to_string()
                };
                return Err(crate::DynoxideError::SerializationException(msg));
            }
        }
    }
    Ok(())
}

/// Check that a field, if present and not null, is a JSON object (map).
/// Returns SerializationException with the DynamoDB Java type in the message.
fn check_field_is_map(
    obj: &serde_json::Map<String, serde_json::Value>,
    field: &str,
    java_value_type: &str,
) -> crate::Result<()> {
    let val = match obj.get(field) {
        Some(v) if !v.is_null() => v,
        _ => return Ok(()),
    };

    if val.is_object() {
        return Ok(());
    }

    let msg = if val.is_array() {
        format!("Unrecognized collection type java.util.Map<java.lang.String, {java_value_type}>")
    } else {
        // Scalar value where map expected → DynamoDB returns "Unexpected field type"
        "Unexpected field type".to_string()
    };

    Err(crate::DynoxideError::SerializationException(msg))
}

/// Check that a field, if present and not null, is a JSON object (struct).
/// Returns SerializationException with the appropriate message for the wrong type.
fn check_field_is_struct(
    obj: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> crate::Result<()> {
    let val = match obj.get(field) {
        Some(v) if !v.is_null() => v,
        _ => return Ok(()),
    };

    if val.is_object() {
        return Ok(());
    }

    let msg = if val.is_array() {
        // Try to map field name to DynamoDB Java class
        let dynamo_class = match field {
            "ProvisionedThroughput" => {
                Some("com.amazonaws.dynamodb.v20120810.ProvisionedThroughput")
            }
            "Projection" => Some("com.amazonaws.dynamodb.v20120810.Projection"),
            "DeleteRequest" => Some("com.amazonaws.dynamodb.v20120810.DeleteRequest"),
            "PutRequest" => Some("com.amazonaws.dynamodb.v20120810.PutRequest"),
            "Create" => Some("com.amazonaws.dynamodb.v20120810.CreateGlobalSecondaryIndexAction"),
            "Update" => Some("com.amazonaws.dynamodb.v20120810.UpdateGlobalSecondaryIndexAction"),
            "Delete" => Some("com.amazonaws.dynamodb.v20120810.DeleteGlobalSecondaryIndexAction"),
            _ => None,
        };
        if let Some(cls) = dynamo_class {
            format!("Unrecognized collection type class {cls}")
        } else {
            "Start of structure or map found where not expected".to_string()
        }
    } else {
        // Scalar value where struct expected
        "Unexpected field type".to_string()
    };

    Err(crate::DynoxideError::SerializationException(msg))
}

/// Check that a field, if present and not null, is a JSON array.
/// Returns the appropriate SerializationException message for the wrong type.
fn check_field_is_list(
    obj: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> crate::Result<()> {
    let val = match obj.get(field) {
        Some(v) if !v.is_null() => v,
        _ => return Ok(()),
    };

    if val.is_array() {
        return Ok(());
    }

    let msg = if val.is_object() {
        "Start of structure or map found where not expected".to_string()
    } else {
        "Unexpected field type".to_string()
    };

    Err(crate::DynoxideError::SerializationException(msg))
}

/// Check scalar fields inside filter condition map entries (QueryFilter/ScanFilter/KeyConditions).
fn check_filter_inner_fields(
    obj: &serde_json::Map<String, serde_json::Value>,
    filter_field: &str,
) -> crate::Result<()> {
    let filter = match obj.get(filter_field) {
        Some(v) if v.is_object() => v.as_object().unwrap(),
        _ => return Ok(()),
    };

    for (_attr_name, condition) in filter {
        if let Some(cond_obj) = condition.as_object() {
            check_field_is_string(cond_obj, "ComparisonOperator")?;
            check_field_is_list(cond_obj, "AttributeValueList")?;
            // Check AVL elements are attr structs
            if let Some(serde_json::Value::Array(avl)) = cond_obj.get("AttributeValueList") {
                for item in avl {
                    if !item.is_object() && !item.is_null() {
                        let msg = if item.is_array() {
                            "Unrecognized collection type class com.amazonaws.dynamodb.v20120810.AttributeValue"
                                .to_string()
                        } else {
                            "Unexpected value type in payload".to_string()
                        };
                        return Err(crate::DynoxideError::SerializationException(msg));
                    }
                }
            }
        }
    }
    Ok(())
}

/// Check AttributeValueList fields inside a filter map (QueryFilter/ScanFilter).
///
/// The filter is a map of attribute names to condition objects, each of which
/// may contain an AttributeValueList that must be an array.
fn check_filter_attribute_value_lists(
    obj: &serde_json::Map<String, serde_json::Value>,
    filter_field: &str,
) -> crate::Result<()> {
    let filter = match obj.get(filter_field) {
        Some(v) if v.is_object() => v.as_object().unwrap(),
        _ => return Ok(()),
    };

    for (_attr_name, condition) in filter {
        if let Some(cond_obj) = condition.as_object() {
            check_field_is_list(cond_obj, "AttributeValueList")?;
        }
    }

    Ok(())
}

fn dispatch(db: &Database, operation: &str, body: &str) -> crate::Result<String> {
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
            // This should not be reachable because is_known_operation() filters first,
            // but handle it defensively.
            Err(crate::DynoxideError::SerializationException(
                "UnknownOperationException".to_string(),
            ))
        }
    }
}

fn deserialize<T: serde::de::DeserializeOwned>(body: &str) -> crate::Result<T> {
    serde_json::from_str(body).map_err(|e| {
        let msg = e.to_string();
        // Custom validation errors from our Deserialize impls use a "VALIDATION:" prefix
        // to signal that these should be ValidationException, not SerializationException.
        if let Some(stripped) = msg.strip_prefix("VALIDATION:") {
            // serde_json appends " at line N column N" to custom errors — strip it
            let clean = strip_serde_position(stripped);
            return crate::DynoxideError::ValidationException(clean.to_string());
        }
        // DynamoDB returns ValidationException for missing required fields,
        // null values, and unrecognised enum variants. Only true JSON type
        // mismatches (e.g. number where string is expected) produce a
        // SerializationException.
        if msg.contains("missing field")
            || msg.contains("unknown variant")
            || msg.contains("invalid type: null")
        {
            crate::DynoxideError::ValidationException(msg)
        } else if msg.contains("empty AttributeValue") {
            crate::DynoxideError::ValidationException(
                "Supplied AttributeValue is empty, must contain exactly one of the supported datatypes".to_string(),
            )
        } else if msg.contains("Supplied AttributeValue") {
            // Multi-datatype or empty AV error — strip position info and return as-is
            let clean = strip_serde_position(&msg);
            crate::DynoxideError::ValidationException(clean)
        } else {
            crate::DynoxideError::SerializationException(map_serde_to_dynamodb_message(&msg, body))
        }
    })
}

/// Strip serde_json's " at line N column N" suffix from error messages.
fn strip_serde_position(msg: &str) -> String {
    if let Some(idx) = msg.rfind(" at line ") {
        // Verify the suffix looks like " at line N column N"
        let suffix = &msg[idx..];
        if suffix.contains("column") {
            return msg[..idx].to_string();
        }
    }
    msg.to_string()
}

/// Map serde deserialisation error messages to DynamoDB-style SerializationException messages.
///
/// DynamoDB returns specific messages like "NUMBER_VALUE cannot be converted to String"
/// whereas serde returns "invalid type: integer `23`, expected a string at line 1 column 42".
fn map_serde_to_dynamodb_message(msg: &str, body: &str) -> String {
    // "invalid type: <type>, expected <target>"
    if let Some(rest) = msg.strip_prefix("invalid type: ") {
        // Extract the source type and target type
        let (source_part, target_part) = match rest.split_once(", expected ") {
            Some((s, t)) => (s, t),
            None => return msg.to_string(),
        };
        // Strip " at line N column N" from target
        let target = target_part
            .split(" at line ")
            .next()
            .unwrap_or(target_part)
            .trim();

        return map_type_mismatch(source_part.trim(), target);
    }

    // "invalid length N, expected struct X ..." → struct-level errors
    if msg.contains("expected struct") && msg.starts_with("invalid length ") {
        // Extract struct name from "invalid length N, expected struct X with M elements"
        if let Some(rest) = msg.split("expected struct ").nth(1) {
            let struct_name = rest.split(' ').next().unwrap_or("Unknown");
            if let Some(dynamo_class) = map_struct_to_dynamo_class(struct_name) {
                return format!("Unrecognized collection type class {dynamo_class}");
            }
        }
        return "Start of structure or map found where not expected".to_string();
    }

    // "expected string for X at line N column N" → wrong type inside AttributeValue
    if msg.starts_with("expected string for ") {
        return infer_type_conversion_error(msg, body, "String");
    }

    // "expected value at line N column N" → wrong value type at position
    if msg.starts_with("expected value at line ") {
        return infer_type_conversion_error(msg, body, "String");
    }

    msg.to_string()
}

/// Map a serde type mismatch to DynamoDB's SerializationException message.
fn map_type_mismatch(source: &str, target: &str) -> String {
    // Determine target type category
    let target_is_string = target == "a string";
    let target_is_bool = target == "a boolean";
    let target_is_sequence = target == "a sequence";
    let target_is_integer = target == "i64" || target == "u64";
    let target_is_struct = target.starts_with("struct ");
    let target_is_map = target.starts_with("a map") || target.starts_with("map");

    // Determine source type
    let is_integer = source.starts_with("integer ");
    let is_float = source.starts_with("floating point ");
    let is_bool_true = source == "boolean `true`";
    let is_bool_false = source == "boolean `false`";
    let _is_bool = is_bool_true || is_bool_false;
    let is_string = source.starts_with("string ");
    let is_sequence = source == "sequence";
    let is_map = source == "map";

    // Map to DynamoDB message based on (source_type, target_type) combination
    if target_is_sequence {
        // List/array fields
        if is_map {
            return "Start of structure or map found where not expected".to_string();
        }
        return "Unexpected field type".to_string();
    }

    if target_is_string {
        if is_bool_true {
            return "TRUE_VALUE cannot be converted to String".to_string();
        }
        if is_bool_false {
            return "FALSE_VALUE cannot be converted to String".to_string();
        }
        if is_float {
            return "DECIMAL_VALUE cannot be converted to String".to_string();
        }
        if is_integer {
            return "NUMBER_VALUE cannot be converted to String".to_string();
        }
        if is_sequence {
            return "Unrecognized collection type class java.lang.String".to_string();
        }
        if is_map {
            return "Start of structure or map found where not expected".to_string();
        }
    }

    if target_is_bool {
        if is_string {
            return "Unexpected token received from parser".to_string();
        }
        if is_float {
            return "DECIMAL_VALUE cannot be converted to Boolean".to_string();
        }
        if is_integer {
            return "NUMBER_VALUE cannot be converted to Boolean".to_string();
        }
        if is_sequence {
            return "Unrecognized collection type class java.lang.Boolean".to_string();
        }
        if is_map {
            return "Start of structure or map found where not expected".to_string();
        }
    }

    if target_is_integer {
        if is_string {
            return "STRING_VALUE cannot be converted to Long".to_string();
        }
        if is_bool_true {
            return "TRUE_VALUE cannot be converted to Long".to_string();
        }
        if is_bool_false {
            return "FALSE_VALUE cannot be converted to Long".to_string();
        }
        if is_sequence {
            return "Unrecognized collection type class java.lang.Long".to_string();
        }
        if is_map {
            return "Start of structure or map found where not expected".to_string();
        }
    }

    if target_is_struct || target_is_map {
        if is_sequence {
            // Need to figure out the class from target
            if let Some(struct_name) = target.strip_prefix("struct ") {
                let name = struct_name.split(' ').next().unwrap_or("Unknown");
                if let Some(dynamo_class) = map_struct_to_dynamo_class(name) {
                    return format!("Unrecognized collection type class {dynamo_class}");
                }
            }
        }
        if is_map && target_is_struct {
            return "Start of structure or map found where not expected".to_string();
        }
        if !is_map && !is_sequence {
            return "Unexpected field type".to_string();
        }
    }

    // Fallback: return the original message
    source
        .split(" at line ")
        .next()
        .unwrap_or(source)
        .to_string()
}

/// Infer the DynamoDB type conversion error from a serde error message.
/// Uses the column position to inspect the actual JSON value in the body.
fn infer_type_conversion_error(msg: &str, body: &str, target_type: &str) -> String {
    // Try to extract column number from "at line N column N"
    if let Some(col_str) = msg.rsplit("column ").next() {
        if let Ok(col) = col_str.trim().parse::<usize>() {
            // Column is 1-based. Look at the character just before the column
            // to determine what type of value serde encountered.
            if col > 0 && col <= body.len() {
                let ch = body.as_bytes()[col - 1];
                return match ch {
                    b't' => format!("TRUE_VALUE cannot be converted to {target_type}"),
                    b'f' => format!("FALSE_VALUE cannot be converted to {target_type}"),
                    b'0'..=b'9' | b'-' => {
                        format!("NUMBER_VALUE cannot be converted to {target_type}")
                    }
                    _ => format!("TRUE_VALUE cannot be converted to {target_type}"),
                };
            }
        }
    }
    format!("TRUE_VALUE cannot be converted to {target_type}")
}

/// Map Rust struct names to DynamoDB Java class names for SerializationException messages.
fn map_struct_to_dynamo_class(struct_name: &str) -> Option<&'static str> {
    match struct_name {
        "ProvisionedThroughput" | "ProvisionedThroughputRaw" => {
            Some("com.amazonaws.dynamodb.v20120810.ProvisionedThroughput")
        }
        "Projection" | "ProjectionRaw" => Some("com.amazonaws.dynamodb.v20120810.Projection"),
        "KeySchemaElement" | "KeySchemaElementRaw" => {
            Some("com.amazonaws.dynamodb.v20120810.KeySchemaElement")
        }
        "AttributeDefinition" | "AttributeDefinitionRaw" => {
            Some("com.amazonaws.dynamodb.v20120810.AttributeDefinition")
        }
        "LocalSecondaryIndex" | "LocalSecondaryIndexRaw" => {
            Some("com.amazonaws.dynamodb.v20120810.LocalSecondaryIndex")
        }
        "GlobalSecondaryIndex" | "GlobalSecondaryIndexRaw" => {
            Some("com.amazonaws.dynamodb.v20120810.GlobalSecondaryIndex")
        }
        "DeleteGsiAction" | "DeleteGsiActionRaw" => {
            Some("com.amazonaws.dynamodb.v20120810.DeleteGlobalSecondaryIndexAction")
        }
        "CreateGsiAction" | "CreateGsiActionRaw" => {
            Some("com.amazonaws.dynamodb.v20120810.CreateGlobalSecondaryIndexAction")
        }
        "UpdateGsiAction" | "UpdateGsiActionRaw" => {
            Some("com.amazonaws.dynamodb.v20120810.UpdateGlobalSecondaryIndexAction")
        }
        "GlobalSecondaryIndexUpdate" | "GlobalSecondaryIndexUpdateRaw" => {
            Some("com.amazonaws.dynamodb.v20120810.GlobalSecondaryIndexUpdate")
        }
        "Tag" | "TagRaw" => Some("com.amazonaws.dynamodb.v20120810.Tag"),
        _ => None,
    }
}

fn serialize<T: serde::Serialize>(val: &T) -> crate::Result<String> {
    serde_json::to_string(val).map_err(|e| crate::DynoxideError::InternalServerError(e.to_string()))
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
