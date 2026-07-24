//! Transport-neutral validation of AWS authentication material.
//!
//! Dynoxide never verifies signatures, but it mirrors DynamoDB's validation of
//! the auth material itself: header-based and query-string SigV4 are checked for
//! presence and completeness, with the same error types and messages DynamoDB
//! returns when parts are missing or conflicting.
//!
//! This lives outside `server/` because dynoxide now has two HTTP surfaces: the
//! native axum server and the wasm engine's [`dispatch_http`](crate::wasm_api),
//! which is fronted by a transport shim. Both call this, so neither can drift
//! from the other. Nothing here knows about axum or about wasm.

/// The auth material one request carries, lifted out of whatever transport
/// delivered it.
#[derive(Debug, Default, Clone, Copy)]
pub struct AuthMaterial<'a> {
    /// The `Authorization` header, if present.
    pub authorization: Option<&'a str>,
    /// The raw query string, without a leading `?`. Empty when there is none.
    pub query: &'a str,
    /// Whether the request carried an `X-Amz-Date` or a `Date` header.
    pub has_date_header: bool,
}

/// Validate the auth material, returning the DynamoDB error envelope to send
/// when it is missing or incomplete, or `None` when the request may proceed.
///
/// DynamoDB checks auth after resolving the target operation, so callers run
/// this once the operation is known.
pub fn validate(material: AuthMaterial<'_>) -> Option<String> {
    let AuthMaterial {
        authorization,
        query,
        has_date_header,
    } = material;

    let has_algorithm_query = query.split('&').any(|p| {
        let key = p.split('=').next().unwrap_or("");
        key == "X-Amz-Algorithm"
    });

    // Both header and query-string auth: DynamoDB rejects the ambiguity.
    if authorization.is_some() && has_algorithm_query {
        return Some(envelope(
            "InvalidSignatureException",
            "Found both 'X-Amz-Algorithm' as a query-string param and 'Authorization' as HTTP header.",
        ));
    }

    if has_algorithm_query {
        return validate_query_auth(query);
    }

    validate_header_auth(authorization, has_date_header)
}

fn validate_query_auth(query: &str) -> Option<String> {
    let mut missing = Vec::new();
    let query_params: Vec<&str> = query
        .split('&')
        .map(|p| p.split('=').next().unwrap_or(""))
        .collect();

    // Present-but-empty counts as missing for the algorithm itself.
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

    if missing.is_empty() {
        return None;
    }

    let parts: Vec<String> = missing
        .iter()
        .map(|p| format!("AWS query-string parameters must include {p}. "))
        .collect();
    Some(envelope(
        "IncompleteSignatureException",
        &format!("{}Re-examine the query-string parameters.", parts.join("")),
    ))
}

fn validate_header_auth(authorization: Option<&str>, has_date_header: bool) -> Option<String> {
    const MISSING_TOKEN: &str = "Request is missing Authentication Token";

    let Some(auth) = authorization else {
        return Some(envelope(
            "MissingAuthenticationTokenException",
            MISSING_TOKEN,
        ));
    };

    if !auth.starts_with("AWS4-") {
        return Some(envelope(
            "MissingAuthenticationTokenException",
            MISSING_TOKEN,
        ));
    }

    // Parts may be separated by spaces or commas, so look for the keys rather
    // than parsing a grammar. Signatures are never verified.
    let has_credential = auth.contains("Credential=") || auth.contains("credential=");
    let has_signature = auth.contains("Signature=") || auth.contains("signature=");
    let has_signed_headers = auth.contains("SignedHeaders=") || auth.contains("signedheaders=");

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
    if !has_date_header {
        missing.push("existence of either a 'X-Amz-Date' or a 'Date' header.");
    }

    if missing.is_empty() {
        return None;
    }

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
    Some(envelope("IncompleteSignatureException", &parts.join(" ")))
}

fn envelope(error_type: &str, message: &str) -> String {
    serde_json::json!({
        "__type": format!("com.amazon.coral.service#{error_type}"),
        "message": message,
    })
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    const SIGNED: &str = "AWS4-HMAC-SHA256 Credential=fake/20260724/eu-west-2/dynamodb/aws4_request, SignedHeaders=host;x-amz-date, Signature=abc";

    fn header(auth: &str) -> AuthMaterial<'_> {
        AuthMaterial {
            authorization: Some(auth),
            query: "",
            has_date_header: true,
        }
    }

    #[test]
    fn a_well_formed_signed_request_passes() {
        assert!(validate(header(SIGNED)).is_none());
    }

    #[test]
    fn a_missing_authorization_header_is_a_missing_token() {
        let out = validate(AuthMaterial::default()).expect("should reject");
        assert!(out.contains("MissingAuthenticationTokenException"), "{out}");
    }

    #[test]
    fn a_non_sigv4_authorization_header_is_a_missing_token() {
        let out = validate(header("Basic abc123")).expect("should reject");
        assert!(out.contains("MissingAuthenticationTokenException"), "{out}");
    }

    #[test]
    fn an_incomplete_authorization_header_names_each_missing_part() {
        let out = validate(header("AWS4-HMAC-SHA256 Credential=fake")).expect("should reject");
        assert!(out.contains("IncompleteSignatureException"), "{out}");
        assert!(out.contains("'Signature'"), "{out}");
        assert!(out.contains("'SignedHeaders'"), "{out}");
    }

    #[test]
    fn a_signed_request_without_a_date_header_is_incomplete() {
        let material = AuthMaterial {
            authorization: Some(SIGNED),
            query: "",
            has_date_header: false,
        };
        let out = validate(material).expect("should reject");
        assert!(out.contains("X-Amz-Date"), "{out}");
    }

    #[test]
    fn complete_query_string_auth_passes() {
        let material = AuthMaterial {
            authorization: None,
            query: "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=c&X-Amz-Signature=s&X-Amz-SignedHeaders=host&X-Amz-Date=d",
            has_date_header: false,
        };
        assert!(validate(material).is_none());
    }

    #[test]
    fn incomplete_query_string_auth_names_the_missing_parameters() {
        let material = AuthMaterial {
            authorization: None,
            query: "X-Amz-Algorithm=AWS4-HMAC-SHA256",
            has_date_header: false,
        };
        let out = validate(material).expect("should reject");
        assert!(out.contains("IncompleteSignatureException"), "{out}");
        assert!(out.contains("'X-Amz-Signature'"), "{out}");
    }

    #[test]
    fn mixing_header_and_query_auth_is_an_invalid_signature() {
        let material = AuthMaterial {
            authorization: Some(SIGNED),
            query: "X-Amz-Algorithm=AWS4-HMAC-SHA256",
            has_date_header: true,
        };
        let out = validate(material).expect("should reject");
        assert!(out.contains("InvalidSignatureException"), "{out}");
    }
}
