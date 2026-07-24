//! AWS authentication header validation.
//!
//! Dynoxide never verifies signatures, but it mirrors DynamoDB's validation
//! of the auth material itself: header-based and query-string SigV4 are
//! checked for presence and completeness, with the same error types and
//! messages DynamoDB returns when parts are missing or conflicting.

use axum::{
    http::{HeaderMap, StatusCode, Uri},
    response::Response,
};

use super::dynamo_response;

/// Validate AWS authentication headers/query parameters.
///
/// A thin transport adapter: it lifts the auth material out of the axum request
/// and hands it to [`crate::auth_material::validate`], which holds the rules.
/// The wasm engine's HTTP path calls the same function, so the two surfaces
/// cannot drift. Returns `Some(Response)` when auth is missing or incomplete.
pub(super) fn validate_auth(headers: &HeaderMap, uri: &Uri, response_ct: &str) -> Option<Response> {
    let material = crate::auth_material::AuthMaterial {
        authorization: headers.get("authorization").and_then(|v| v.to_str().ok()),
        query: uri.query().unwrap_or(""),
        has_date_header: headers.get("x-amz-date").is_some() || headers.get("date").is_some(),
    };

    crate::auth_material::validate(material)
        .map(|body| dynamo_response(StatusCode::BAD_REQUEST, response_ct, body))
}
