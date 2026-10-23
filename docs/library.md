# Using Dynoxide as a Rust library

```rust
use dynoxide::Database;

// In-memory (for tests)
let db = Database::memory().unwrap();

// Persistent (backed by SQLite file)
let db = Database::new("data.db").unwrap();

// Encrypted (requires `encryption` feature)
// cargo add dynoxide-rs --no-default-features --features encryption
let db = Database::new_encrypted("data.db", "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f").unwrap();
```

Operations use DynamoDB-compatible request/response types:

```rust
use dynoxide::Database;
use serde_json::json;

let db = Database::memory().unwrap();

// Create a table
let req = serde_json::from_value(json!({
    "TableName": "Users",
    "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
    "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}]
})).unwrap();
db.create_table(req).unwrap();

// Put an item
let req = serde_json::from_value(json!({
    "TableName": "Users",
    "Item": {"pk": {"S": "user#1"}, "name": {"S": "Alice"}}
})).unwrap();
db.put_item(req).unwrap();

// Query
let req = serde_json::from_value(json!({
    "TableName": "Users",
    "KeyConditionExpression": "pk = :pk",
    "ExpressionAttributeValues": {":pk": {"S": "user#1"}}
})).unwrap();
let resp = db.query(req).unwrap();
```

## Serving HTTP from your own program

With the `http-server` feature (on by default), `server::serve_with_shutdown`
serves the DynamoDB API on a listener you bind, until a future you supply
resolves. It then stops accepting, closes idle keep-alive connections, lets
running requests finish and returns, so the database is released when it does.

```rust
use dynoxide::Database;

#[tokio::main]
async fn main() -> Result<(), String> {
    let db = Database::new("data.db").map_err(|e| e.to_string())?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:8000")
        .await
        .map_err(|e| e.to_string())?;
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();

    // Hand `stop` to whatever decides when to shut down.
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        let _ = stop.send(());
    });

    dynoxide::server::serve_with_shutdown(listener, db, async {
        let _ = stopped.await;
    })
    .await
}
```

It installs no signal handler; `server::start` is the one that stops on Ctrl-C
or SIGTERM. The rustdoc on `serve_with_shutdown` covers what to expect when a
request never finishes and when a file-backed database's files are deleted
straight after.

## Testing with Embedded Mode

Each test gets a fully isolated database with no shared state:

```rust
#[test]
fn test_user_creation() {
    let db = Database::memory().unwrap();

    // Set up table
    db.create_table(/* ... */).unwrap();

    // Test your logic
    db.put_item(/* ... */).unwrap();
    let result = db.get_item(/* ... */).unwrap();

    assert!(result.item.is_some());
    // db is dropped automatically - nothing to clean up
}
```

No Docker. No port conflicts. No table name prefixes. Tests run in parallel without coordination.

## Feature flags

| Flag | Default | Description |
|------|---------|-------------|
| `native-sqlite` | Yes | Bundles plain SQLite. No OpenSSL. |
| `http-server` | Yes | Adds axum-based HTTP server exposing the DynamoDB JSON API. |
| `mcp-server` | Yes | Adds MCP server for coding agents (stdio and Streamable HTTP transports). |
| `import` | Yes | Adds `dynoxide import` CLI for importing DynamoDB Export data with anonymisation. |
| `cli` | Indirect | Gates the `dynoxide` binary. Pulled in automatically by `http-server`, `mcp-server`, or `import`, so default builds include it; a library-only or `wasm-sqlite` build omits the binary. |
| `wasm-sqlite` | No | wasm32 browser backend (@sqlite.org/sqlite-wasm over OPFS). Mutually exclusive with the native backends and the CLI, so it needs `default-features = false`. See the WASM section. |
| `encryption` | No | Bundles SQLCipher + vendored OpenSSL. Adds `Database::new_encrypted()` for encryption at rest. |
| `encryption-cc` | No | Like `encryption` but uses Apple CommonCrypto instead of bundled OpenSSL. For macOS and iOS builds. |
| `encrypted-server` | No | Convenience: enables `encryption` + `http-server`. |
| `encrypted-server-cc` | No | Convenience: enables `encryption-cc` + `http-server`. |
| `encrypted-full` | No | Convenience: enables `encryption` + `http-server` + `mcp-server` + `import`. |
| `full` | - | Alias for default features (backward compatibility). |

`native-sqlite`, `encryption` and `wasm-sqlite` are **mutually exclusive** - they select different SQLite backends. `wasm-sqlite` additionally excludes the CLI, server and MCP features, which are native-only. Cargo adds the default features to whatever you list, so naming one of these on its own is not enough: `features = ["wasm-sqlite"]` still enables `native-sqlite` and the CLI from the defaults, and the build stops on a `compile_error!` saying so. Always pair a non-default backend with `default-features = false`. To use encryption:

```toml
dynoxide-rs = { version = "3.2", default-features = false, features = ["encryption"] }
```

**Workspace note:** Cargo unifies features across a workspace. If any crate depends on `dynoxide-rs` with default features (getting `native-sqlite`) and another uses `encryption`, both activate and the build fails. Use `default-features = false` on all `dynoxide-rs` dependencies in the workspace.

