# Using Dynoxide as a Rust library

```rust
use dynoxide::Database;

// In-memory (for tests)
let db = Database::memory().unwrap();

// Persistent (backed by SQLite file)
let db = Database::new("data.db").unwrap();

// Encrypted (requires `encryption` feature)
// cargo add dynoxide-rs --features encryption
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
| `wasm-sqlite` | No | wasm32 browser backend (@sqlite.org/sqlite-wasm over OPFS), a preview. Pulls neither native SQLite nor the CLI. See the WASM section. |
| `encryption` | No | Bundles SQLCipher + vendored OpenSSL. Adds `Database::new_encrypted()` for encryption at rest. |
| `encryption-cc` | No | Like `encryption` but uses Apple CommonCrypto instead of bundled OpenSSL. For macOS and iOS builds. |
| `encrypted-server` | No | Convenience: enables `encryption` + `http-server`. |
| `encrypted-server-cc` | No | Convenience: enables `encryption-cc` + `http-server`. |
| `encrypted-full` | No | Convenience: enables `encryption` + `http-server` + `mcp-server` + `import`. |
| `full` | - | Alias for default features (backward compatibility). |

`native-sqlite` and `encryption` are **mutually exclusive** - they select different SQLite backends. To use encryption:

```toml
dynoxide-rs = { version = "0.10", default-features = false, features = ["encryption"] }
```

**Workspace note:** Cargo unifies features across a workspace. If any crate depends on `dynoxide-rs` with default features (getting `native-sqlite`) and another uses `encryption`, both activate and the build fails. Use `default-features = false` on all `dynoxide-rs` dependencies in the workspace.

