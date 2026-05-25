//! Bearer-token authentication for the MCP HTTP transport.
//!
//! The HTTP transport always enforces a bearer token; there is no loopback
//! exemption in the request path. When no token is supplied, a loopback bind
//! auto-generates and persists one. A non-loopback bind must be given an
//! explicit token. Stdio transport is process-scoped and never uses this.

use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};

/// Resolved authentication mode for the HTTP transport.
#[derive(Clone, Debug)]
pub enum AuthMode {
    /// Enforce this bearer token on every `/mcp` request.
    Enabled(String),
    /// Auth disabled. Reachable only via the loopback-only `--no-auth` escape hatch.
    Disabled,
}

/// Outcome of resolving the auth mode, including whether a token was just
/// generated (so the caller can print the one-time first-run guidance) and the
/// persisted path (for that message). I/O policy stays with the caller.
#[derive(Clone, Debug)]
pub struct ResolvedAuth {
    pub mode: AuthMode,
    pub first_run: bool,
    pub token_path: Option<PathBuf>,
}

#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("--no-auth is only allowed on a loopback bind")]
    NoAuthRequiresLoopback,
    #[error("MCP auth token is empty (set --mcp-token/--token or DYNOXIDE_MCP_AUTH_TOKEN)")]
    EmptyToken,
    #[error(
        "a non-loopback MCP bind requires an explicit token (set --mcp-token/--token or DYNOXIDE_MCP_AUTH_TOKEN)"
    )]
    NonLoopbackRequiresToken,
    #[error("could not determine a config directory for the MCP auth token")]
    NoConfigDir,
    #[error("cannot create MCP auth token file at {path}: {source}")]
    CannotCreate {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("MCP auth token file at {path} is unreadable: {source}")]
    Unreadable {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("MCP auth token file at {path} is empty or corrupt; delete it to regenerate")]
    CorruptTokenFile { path: PathBuf },
}

/// Classify a bind host against a strict, closed loopback set. Everything not
/// in the set — including `0.0.0.0`, other `127.x.x.x`, IPv4-mapped IPv6, and
/// resolved DNS names — is non-loopback. The literal string is matched; we do
/// not resolve and re-classify.
pub fn is_loopback_host(host: &str) -> bool {
    matches!(host, "127.0.0.1" | "::1" | "[::1]" | "localhost")
}

/// Constant-time comparison of a presented token against the expected one.
/// Token length is fixed and not secret, so an early length check is fine.
pub fn token_matches(expected: &str, presented: &str) -> bool {
    use subtle::ConstantTimeEq;
    let a = expected.as_bytes();
    let b = presented.as_bytes();
    if a.len() != b.len() {
        return false;
    }
    a.ct_eq(b).into()
}

/// Identical 401 body for both missing and wrong tokens — no oracle.
const UNAUTHORIZED_BODY: &str = r#"{"error":"unauthorized"}"#;

/// axum middleware enforcing the bearer token on every request.
///
/// Runs *outside* rmcp's Host/Origin checks (it wraps the whole router), so an
/// unauthenticated caller gets 401 regardless of Host. A caller holding a valid
/// token who spoofs the Host still hits rmcp's 403 — auth does not replace that
/// defense-in-depth, it sits in front of it.
pub async fn enforce(
    axum::extract::State(mode): axum::extract::State<AuthMode>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    match &mode {
        AuthMode::Disabled => next.run(req).await,
        AuthMode::Enabled(expected) => {
            let authorized = bearer_token(req.headers())
                .map(|presented| token_matches(expected, presented))
                .unwrap_or(false);
            if authorized {
                next.run(req).await
            } else {
                unauthorized()
            }
        }
    }
}

/// Extract the token from an `Authorization: Bearer <token>` header. The scheme
/// is matched case-insensitively per RFC 7235; absent or non-Bearer → None.
fn bearer_token(headers: &axum::http::HeaderMap) -> Option<&str> {
    let value = headers
        .get(axum::http::header::AUTHORIZATION)?
        .to_str()
        .ok()?;
    let (scheme, token) = value.split_once([' ', '\t'])?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    Some(token.trim())
}

fn unauthorized() -> axum::response::Response {
    use axum::http::{StatusCode, header};
    axum::response::Response::builder()
        .status(StatusCode::UNAUTHORIZED)
        // No `resource_metadata` parameter: it triggers OAuth-discovery bugs in
        // Claude Code, Cursor, and Copilot CLI.
        .header(header::WWW_AUTHENTICATE, r#"Bearer realm="dynoxide-mcp""#)
        .header(header::CONTENT_TYPE, "application/json")
        .body(axum::body::Body::from(UNAUTHORIZED_BODY))
        .expect("static 401 response is always valid")
}

/// Resolve the auth mode from the merged CLI/env token, the no-auth flag, and
/// whether the bind is loopback. `token_path_override` exists for tests; in
/// production the per-OS config path is used.
pub fn resolve_auth(
    bind_is_loopback: bool,
    cli_token: Option<String>,
    no_auth: bool,
    token_path_override: Option<PathBuf>,
) -> Result<ResolvedAuth, AuthError> {
    if no_auth {
        if !bind_is_loopback {
            return Err(AuthError::NoAuthRequiresLoopback);
        }
        // Never reads, writes, or creates the token file.
        return Ok(ResolvedAuth {
            mode: AuthMode::Disabled,
            first_run: false,
            token_path: None,
        });
    }

    if let Some(token) = cli_token {
        // Trim to match the presented-token side (bearer_token trims) and the
        // persisted-file path; otherwise a token supplied with stray whitespace
        // would never match.
        let token = token.trim();
        if token.is_empty() {
            return Err(AuthError::EmptyToken);
        }
        return Ok(ResolvedAuth {
            mode: AuthMode::Enabled(token.to_string()),
            first_run: false,
            token_path: None,
        });
    }

    if !bind_is_loopback {
        return Err(AuthError::NonLoopbackRequiresToken);
    }

    let path = match token_path_override {
        Some(p) => p,
        None => default_token_path()?,
    };

    // Atomic create (O_EXCL): exactly one of two concurrent first-runs wins the
    // create; the loser observes AlreadyExists and reads the winner's token.
    match create_new_token_file(&path) {
        Ok(token) => Ok(ResolvedAuth {
            mode: AuthMode::Enabled(token),
            first_run: true,
            token_path: Some(path),
        }),
        Err(CreateError::AlreadyExists) => {
            let token = read_token_file(&path)?;
            Ok(ResolvedAuth {
                mode: AuthMode::Enabled(token),
                first_run: false,
                token_path: Some(path),
            })
        }
        Err(CreateError::Io(source)) => Err(AuthError::CannotCreate { path, source }),
    }
}

/// One-time guidance printed to stderr when a token is first generated.
pub fn first_run_message(url: &str, token: &str, path: &Path) -> String {
    format!(
        "Generated an MCP auth token and saved it to {path}.\n\
         Add it to your MCP client config — e.g. Claude Code .mcp.json:\n\
         \n\
         \x20\x20\"dynoxide\": {{\n\
         \x20\x20\x20\x20\"type\": \"http\",\n\
         \x20\x20\x20\x20\"url\": \"{url}\",\n\
         \x20\x20\x20\x20\"headers\": {{ \"Authorization\": \"Bearer {token}\" }}\n\
         \x20\x20}}\n\
         \n\
         This token persists across restarts. Pin it with DYNOXIDE_MCP_AUTH_TOKEN or --mcp-token.",
        path = path.display(),
    )
}

fn default_token_path() -> Result<PathBuf, AuthError> {
    let dirs = directories::ProjectDirs::from("", "", "dynoxide").ok_or(AuthError::NoConfigDir)?;
    Ok(dirs.config_dir().join("mcp-token"))
}

enum CreateError {
    AlreadyExists,
    Io(std::io::Error),
}

fn create_new_token_file(path: &Path) -> Result<String, CreateError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(CreateError::Io)?;
    }

    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }

    match opts.open(path) {
        Ok(mut file) => {
            let token = generate_token();
            file.write_all(token.as_bytes()).map_err(CreateError::Io)?;
            // Durably flush before returning so a process that lost the create
            // race reads a complete token rather than a partial/empty file.
            file.sync_all().map_err(CreateError::Io)?;
            Ok(token)
        }
        Err(e) if e.kind() == ErrorKind::AlreadyExists => Err(CreateError::AlreadyExists),
        Err(e) => Err(CreateError::Io(e)),
    }
}

fn read_token_file(path: &Path) -> Result<String, AuthError> {
    let raw = std::fs::read_to_string(path).map_err(|source| AuthError::Unreadable {
        path: path.to_path_buf(),
        source,
    })?;
    let token = raw.trim().to_string();
    if token.is_empty() {
        return Err(AuthError::CorruptTokenFile {
            path: path.to_path_buf(),
        });
    }
    Ok(token)
}

/// Generate a high-entropy, URL-safe token. Randomness comes from two v4 UUIDs
/// (CSPRNG-backed, 244 bits combined) to avoid a direct dependency on a
/// specific `getrandom` major; 244 bits is well above the bearer-token bar.
fn generate_token() -> String {
    use base64::Engine;
    let mut bytes = [0u8; 32];
    bytes[..16].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
    bytes[16..].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_token_path() -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("dynoxide-mcp-token-test-{}", uuid::Uuid::new_v4()));
        dir.join("mcp-token")
    }

    #[test]
    fn loopback_set_is_closed() {
        assert!(is_loopback_host("127.0.0.1"));
        assert!(is_loopback_host("::1"));
        assert!(is_loopback_host("[::1]"));
        assert!(is_loopback_host("localhost"));
        assert!(!is_loopback_host("0.0.0.0"));
        assert!(!is_loopback_host("127.0.0.2"));
        assert!(!is_loopback_host("::ffff:127.0.0.1"));
        assert!(!is_loopback_host("example.com"));
    }

    #[test]
    fn token_matches_is_correct() {
        assert!(token_matches("abc123", "abc123"));
        assert!(!token_matches("abc123", "abc124"));
        assert!(!token_matches("abc123", "abc12")); // length mismatch
        assert!(!token_matches("abc123", "abc1234"));
    }

    #[test]
    fn first_run_generates_persists_and_signals() {
        let path = temp_token_path();
        let resolved = resolve_auth(true, None, false, Some(path.clone())).unwrap();
        assert!(resolved.first_run);
        assert_eq!(resolved.token_path.as_deref(), Some(path.as_path()));
        let token = match resolved.mode {
            AuthMode::Enabled(t) => t,
            AuthMode::Disabled => panic!("expected Enabled"),
        };
        assert!(!token.is_empty());
        // Persisted with exactly the generated token.
        assert_eq!(std::fs::read_to_string(&path).unwrap(), token);
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[cfg(unix)]
    #[test]
    fn persisted_file_is_0600() {
        use std::os::unix::fs::PermissionsExt;
        let path = temp_token_path();
        resolve_auth(true, None, false, Some(path.clone())).unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn second_run_reads_existing_silently() {
        let path = temp_token_path();
        let first = resolve_auth(true, None, false, Some(path.clone())).unwrap();
        let first_token = match first.mode {
            AuthMode::Enabled(t) => t,
            AuthMode::Disabled => panic!("expected Enabled"),
        };
        let second = resolve_auth(true, None, false, Some(path.clone())).unwrap();
        assert!(!second.first_run);
        match second.mode {
            AuthMode::Enabled(t) => assert_eq!(t, first_token),
            AuthMode::Disabled => panic!("expected Enabled"),
        }
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn explicit_token_wins_over_file() {
        let path = temp_token_path();
        let resolved = resolve_auth(
            true,
            Some("supplied".to_string()),
            false,
            Some(path.clone()),
        )
        .unwrap();
        assert!(!resolved.first_run);
        assert!(resolved.token_path.is_none());
        match resolved.mode {
            AuthMode::Enabled(t) => assert_eq!(t, "supplied"),
            AuthMode::Disabled => panic!("expected Enabled"),
        }
        // File must not have been created.
        assert!(!path.exists());
    }

    #[test]
    fn empty_token_is_error() {
        assert!(matches!(
            resolve_auth(true, Some(String::new()), false, None),
            Err(AuthError::EmptyToken)
        ));
        assert!(matches!(
            resolve_auth(true, Some("   ".to_string()), false, None),
            Err(AuthError::EmptyToken)
        ));
    }

    #[test]
    fn non_loopback_without_token_is_error() {
        let path = temp_token_path();
        assert!(matches!(
            resolve_auth(false, None, false, Some(path.clone())),
            Err(AuthError::NonLoopbackRequiresToken)
        ));
        // No file touched.
        assert!(!path.exists());
    }

    #[test]
    fn no_auth_loopback_disables_and_skips_file() {
        let path = temp_token_path();
        let resolved = resolve_auth(true, None, true, Some(path.clone())).unwrap();
        assert!(matches!(resolved.mode, AuthMode::Disabled));
        assert!(!resolved.first_run);
        assert!(!path.exists());
    }

    #[test]
    fn no_auth_non_loopback_is_error() {
        assert!(matches!(
            resolve_auth(false, None, true, None),
            Err(AuthError::NoAuthRequiresLoopback)
        ));
    }

    #[test]
    fn unreadable_file_errors_without_regenerating() {
        // A directory where the token file is expected is unreadable as a file.
        let dir =
            std::env::temp_dir().join(format!("dynoxide-unreadable-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        // path points at a directory, so create_new fails with AlreadyExists,
        // then read_to_string fails -> Unreadable.
        let result = resolve_auth(true, None, false, Some(dir.clone()));
        assert!(matches!(result, Err(AuthError::Unreadable { .. })));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn corrupt_empty_file_errors() {
        let path = temp_token_path();
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, "   \n").unwrap();
        let result = resolve_auth(true, None, false, Some(path.clone()));
        assert!(matches!(result, Err(AuthError::CorruptTokenFile { .. })));
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }
}
