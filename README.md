# Dynoxide

A lightweight DynamoDB emulator backed by SQLite. Runs as an HTTP server compatible with the AWS DynamoDB API, as an MCP server for coding agents, or embeds directly into Rust and iOS applications as a library.

## Why Dynoxide?

DynamoDB Local requires Docker and a JVM. It takes <!-- prose:ddb_local_cold_start -->3–4 seconds<!-- /bench --> to cold-start, uses <!-- prose:ddb_local_idle_memory -->~163 MB<!-- /bench --> of memory at idle, and pulls a <!-- prose:ddb_local_image_size -->~225MB<!-- /bench --> Docker image. For CI pipelines running hundreds of integration tests, that overhead adds up. For local development, it means waiting for Docker and burning resources in the background.

Dynoxide is a native binary. It starts in milliseconds, idles at <!-- prose:dynoxide_idle_memory -->~4.9 MB<!-- /bench -->, and ships as a <!-- prose:dynoxide_binary_size -->~5MB<!-- /bench --> download. Point any DynamoDB SDK at it and run your tests — same API, faster feedback loop.

For Rust projects, Dynoxide also offers an **embedded mode**: direct API calls via `Database::memory()` with no HTTP layer at all. Each test gets an isolated in-memory database with zero startup cost. And because it compiles to a native library with no runtime dependencies, it runs on platforms where DynamoDB Local can't — including iOS.

### Performance

#### Local Development (Apple Silicon)

| Metric | Dynoxide (embedded) | Dynoxide (HTTP) | DynamoDB Local |
|---|---|---|---|
| Cold startup | <!-- bench:local_startup_embedded -->**~0.2ms**<!-- /bench --> | <!-- bench:local_startup_http -->**~15ms**<!-- /bench --> | <!-- bench:local_startup_ddb_local -->~2,287ms<!-- /bench --> |
| GetItem (p50) | <!-- bench:local_getitem_embedded -->9µs<!-- /bench --> | <!-- bench:local_getitem_http -->0.1ms<!-- /bench --> | <!-- bench:local_getitem_ddb_local -->0.8ms<!-- /bench --> |
| PutItem throughput | <!-- bench:local_putitem_embedded -->~51,613 ops/s<!-- /bench --> | <!-- bench:local_putitem_http -->~6,703 ops/s<!-- /bench --> | <!-- bench:local_putitem_ddb_local -->~945 ops/s<!-- /bench --> |
| 50-test suite (sequential) | <!-- bench:local_ci_suite_embedded_seq -->~484ms<!-- /bench --> | <!-- bench:local_ci_suite_http_seq -->~569ms<!-- /bench --> | <!-- bench:local_ci_suite_ddb_local_seq -->~2,407ms<!-- /bench --> |
| 50-test suite (4x parallel) | <!-- bench:local_ci_suite_embedded_par -->~203ms<!-- /bench --> | <!-- bench:local_ci_suite_http_par -->~235ms<!-- /bench --> | <!-- bench:local_ci_suite_ddb_local_par -->~1,189ms<!-- /bench --> |

#### CI (GitHub Actions)

Numbers from `ubuntu-latest` (2-core AMD EPYC 7763, 8GB RAM). Commit <!-- bench:ci_commit_link_root -->[`006fa80`](../../commit/006fa8060c37561d79f8d455e8a752a93188ac9a)<!-- /bench -->.

| Metric | Dynoxide (embedded) | Dynoxide (HTTP) | DynamoDB Local | LocalStack (all services) |
|---|---|---|---|---|
| Cold startup | <!-- bench:ci_startup_embedded -->**<1ms**<!-- /bench --> | <!-- bench:ci_startup_http -->**~3ms**<!-- /bench --> | <!-- bench:ci_startup_ddb_local -->~3,715ms<!-- /bench --> | <!-- bench:ci_startup_localstack -->~5,243ms<!-- /bench --> |
| GetItem (p50) | <!-- bench:ci_getitem_embedded -->16µs<!-- /bench --> | <!-- bench:ci_getitem_http -->0.4ms<!-- /bench --> | <!-- bench:ci_getitem_ddb_local -->0.9ms<!-- /bench --> | — |
| 50-test CI suite | <!-- bench:ci_suite_embedded_seq -->775ms<!-- /bench --> | <!-- bench:ci_suite_http_seq -->784ms<!-- /bench --> | <!-- bench:ci_suite_ddb_local_seq -->3,156ms<!-- /bench --> | — |
| Full workload (10K items) | — | <!-- bench:ci_workload_http -->**3.2s**<!-- /bench --> | <!-- bench:ci_workload_ddb_local -->15.4s<!-- /bench --> | — |
| Binary / Docker image | <!-- bench:ci_binary_size -->5 MB<!-- /bench --> | <!-- bench:ci_binary_size_http -->5 MB<!-- /bench --> | <!-- bench:ci_image_ddb_local -->225 MB<!-- /bench --> | <!-- bench:ci_image_localstack -->1.1 GB<!-- /bench --> |
| Idle memory (RSS) | <!-- bench:ci_memory_embedded_idle -->~4.9 MB<!-- /bench --> | <!-- bench:ci_memory_http_idle -->~8 MB<!-- /bench --> | <!-- bench:ci_memory_ddb_local_idle -->~163 MB<!-- /bench --> | <!-- bench:ci_memory_localstack_idle -->~259 MB<!-- /bench --> |

> The gap is wider on Apple Silicon because the faster CPU amplifies the difference between native code and JVM overhead. Both are real measurements of the same benchmark suite. [Full methodology and per-operation breakdowns →](benchmarks/README.md)

### Conformance

Verified against real DynamoDB by the [dynamodb-conformance](https://github.com/nubo-db/dynamodb-conformance) suite:

| Target | Tests | Pass Rate |
|---|---|---|
| DynamoDB | 526 | 100% |
| **Dynoxide** | **526** | **100%** |
| DynamoDB Local | 526 | 92.0% |

See [full results by tier](https://github.com/nubo-db/dynamodb-conformance#results).

### How It Compares

| | Dynoxide | DynamoDB Local | LocalStack (all services) | dynalite |
|---|---|---|---|---|
| Conformance (526 tests) | **100%** | 92% | 93% | 81% |
| Language | Rust | Java | Python + Java | Node.js |
| Storage | SQLite | SQLite | SQLite (via DDB Local) | LevelDB |
| Docker required | — | ✓ | ✓ | — |
| JVM required | — | ✓ | ✓ | — |
| Embeddable (Rust / iOS) | ✓ | — | — | — |
| MCP server for agents | ✓ | — | — | — |

LocalStack uses DynamoDB Local internally as its DynamoDB engine — its startup and memory overhead includes DynamoDB Local's JVM plus LocalStack's own Python routing layer.

## Installation

### Homebrew (macOS)

```sh
brew install nubo-db/tap/dynoxide
```

### Pre-built binaries

Download from [GitHub Releases](https://github.com/nubo-db/dynoxide/releases) for Linux (x86_64, aarch64, musl), macOS (Intel, Apple Silicon), and Windows.

```sh
# Example: Linux x86_64
curl -fsSL https://github.com/nubo-db/dynoxide/releases/latest/download/dynoxide-x86_64-unknown-linux-gnu.tar.gz | tar xz
sudo mv dynoxide /usr/local/bin/
```

### Cargo

```sh
cargo install dynoxide-rs

# With encryption support (SQLCipher + vendored OpenSSL)
cargo install dynoxide-rs --no-default-features --features encrypted-full
```

### As a library (Rust)

```toml
[dependencies]
# Minimal — just the embedded database, no server or CLI dependencies
dynoxide-rs = { version = "0.9", default-features = false, features = ["native-sqlite"] }

# Or with encryption:
# dynoxide-rs = { version = "0.9", default-features = false, features = ["encryption"] }
```

### GitHub Actions

```yaml
- uses: nubo-db/dynoxide@v1
  with:
    snapshot-url: https://example.com/test-data.db.zst  # optional
    port: 8000
```

See [action/action.yml](action/action.yml) for all inputs and outputs.

## HTTP Server

Start the server:

```sh
dynoxide --port 8000
```

With a persistent database:

```sh
dynoxide --db-path data.db --port 8000
```

With encryption (requires the `encrypted-server` build):

```sh
# Generate a key
openssl rand -hex 32 > key.hex
chmod 600 key.hex

# Start with key file
dynoxide --db-path data.db --encryption-key-file key.hex

# Or via environment variable
DYNOXIDE_ENCRYPTION_KEY=$(cat key.hex) dynoxide --db-path data.db
```

Then use the AWS CLI or any DynamoDB SDK pointed at localhost:

```sh
aws dynamodb list-tables --endpoint-url http://localhost:8000

aws dynamodb put-item \
  --endpoint-url http://localhost:8000 \
  --table-name Users \
  --item '{"pk": {"S": "user#1"}, "name": {"S": "Alice"}}'

aws dynamodb get-item \
  --endpoint-url http://localhost:8000 \
  --table-name Users \
  --key '{"pk": {"S": "user#1"}}'
```

Works with any language or SDK that supports custom endpoints — Python (boto3), Node.js (AWS SDK v3), Go, Java, etc.

## MCP Server

Dynoxide includes an [MCP](https://modelcontextprotocol.io) server that exposes DynamoDB operations as tools for coding agents (Claude Code, Cursor, etc.).

### stdio transport (default)

```sh
dynoxide mcp
dynoxide mcp --db-path data.db
```

### Streamable HTTP transport

```sh
dynoxide mcp --http --port 19280
```

### Claude Code configuration

Add to your `mcp.json`:

```json
{
  "mcpServers": {
    "dynoxide": {
      "command": "dynoxide",
      "args": ["mcp"]
    }
  }
}
```

Or with a persistent database:

```json
{
  "mcpServers": {
    "dynoxide": {
      "command": "dynoxide",
      "args": ["mcp", "--db-path", "dev.db"]
    }
  }
}
```

With a OneTable data model for single-table designs:

```json
{
  "mcpServers": {
    "dynoxide": {
      "command": "dynoxide",
      "args": ["mcp", "--db-path", "dev.db", "--data-model", "onetable.json"]
    }
  }
}
```

### Available tools (33)

| Category | Tools |
|----------|-------|
| Tables | `list_tables`, `describe_table`, `create_table`, `delete_table`, `update_table` |
| Items | `get_item`, `put_item`, `update_item`, `delete_item` |
| Batch | `batch_get_item`, `batch_write_item`, `bulk_put_items` |
| Query | `query`, `scan` |
| Transactions | `transact_get_items`, `transact_write_items` |
| PartiQL | `execute_partiql`, `batch_execute_partiql` |
| TTL | `update_time_to_live`, `describe_time_to_live`, `sweep_ttl` |
| Tags | `tag_resource`, `untag_resource`, `list_tags_of_resource` |
| Streams | `list_streams`, `describe_stream`, `get_shard_iterator`, `get_records` |
| Snapshots | `create_snapshot`, `restore_snapshot`, `list_snapshots`, `delete_snapshot` |
| Info | `get_database_info` |

### Safety options

```sh
# Read-only mode — rejects all write operations
dynoxide mcp --read-only --db-path prod-snapshot.db

# Limit query/scan results
dynoxide mcp --max-items 100 --max-size-bytes 65536
```

### Snapshots

The MCP server supports database snapshots for safe experimentation:

- `create_snapshot` — saves a point-in-time copy of the database
- `restore_snapshot` — rolls back to a previous snapshot
- `list_snapshots` — lists available snapshots
- Auto-snapshot before `delete_table` (last 10 kept automatically)

### Data Model Context

For single-table designs, raw DynamoDB metadata (`pk` is type `S`, `GSI1` exists) tells an agent almost nothing. The `--data-model` flag loads a [OneTable](https://doc.onetable.io/) schema so the agent sees entity names, key templates, GSI mappings, and type discriminator attributes.

```sh
dynoxide mcp --data-model schema.json
dynoxide mcp --data-model schema.json --db-path data.db
```

With `--data-model`, the MCP instructions include a compact entity summary and `get_database_info` returns the full model:

```json
{
  "data_model": {
    "schema_format": "onetable:1.1.0",
    "type_attribute": "_type",
    "entities": [
      {
        "name": "Account",
        "pk_template": "account#${id}",
        "sk_template": "account#",
        "type_attribute": "_type",
        "gsi_mappings": []
      },
      {
        "name": "User",
        "pk_template": "account#${accountId}",
        "sk_template": "user#${email}",
        "type_attribute": "_type",
        "gsi_mappings": [
          { "index_name": "GSI1", "pk_template": "user#${email}", "sk_template": "user#" }
        ]
      }
    ]
  }
}
```

The agent knows which entity types exist, how their keys are structured, and which GSI to query for a given access pattern — before making a single query.

**Index name resolution:** OneTable uses shorthand keys internally (e.g. `gs1`). If the index definition includes a `name` field (e.g. `"name": "GSI1"`), the parser uses the DynamoDB-facing name so it matches `describe_table` output and works directly with `query --index-name`.

**Options:**

```sh
# Control how many entities appear in MCP instructions (default: 20, 0 = suppress)
dynoxide mcp --data-model schema.json --data-model-summary-limit 10

# With serve --mcp (uses --mcp-data-model prefix)
dynoxide serve --mcp --mcp-data-model schema.json
```

The data model is context-only — dynoxide does not validate writes against the schema. The instructions note this explicitly so agents don't assume enforcement.

## DynamoDB Streams

Dynoxide supports DynamoDB Streams with all four view types: `NEW_IMAGE`, `OLD_IMAGE`, `NEW_AND_OLD_IMAGES`, and `KEYS_ONLY`.

### Enabling streams

Streams are enabled per-table via `StreamSpecification` in `CreateTable` or `UpdateTable`, exactly like real DynamoDB:

```sh
# Via AWS CLI
aws dynamodb create-table \
  --endpoint-url http://localhost:8000 \
  --table-name Events \
  --key-schema AttributeName=pk,KeyType=HASH \
  --attribute-definitions AttributeName=pk,AttributeType=S \
  --stream-specification StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES

# Enable on an existing table
aws dynamodb update-table \
  --endpoint-url http://localhost:8000 \
  --table-name Events \
  --stream-specification StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES
```

Via the MCP server, pass `stream_specification` to `create_table` or `update_table`.

### Reading stream records

```sh
# List streams
aws dynamodbstreams list-streams --endpoint-url http://localhost:8000

# Describe a stream to get shard IDs
aws dynamodbstreams describe-stream \
  --endpoint-url http://localhost:8000 \
  --stream-arn arn:aws:dynamodb:local:000000000000:table/Events/stream/...

# Get a shard iterator and read records
aws dynamodbstreams get-shard-iterator \
  --endpoint-url http://localhost:8000 \
  --stream-arn <stream-arn> \
  --shard-id <shard-id> \
  --shard-iterator-type TRIM_HORIZON
```

### Streams with import

If the `--schema` file (DescribeTable JSON) contains a `StreamSpecification`, streams are automatically enabled on the imported table. No extra flags needed — the import faithfully reproduces the source table's configuration:

```json
{
  "Table": {
    "TableName": "Events",
    "StreamSpecification": {
      "StreamEnabled": true,
      "StreamViewType": "NEW_AND_OLD_IMAGES"
    }
  }
}
```

Note: Imported items do not generate stream records by default (bulk import bypasses stream recording for performance). Stream recording begins for writes made after import completes.

## Import CLI

Import data from DynamoDB Export (JSON Lines format) into a Dynoxide database, with optional anonymisation.

### Basic import

```sh
dynoxide import \
  --source ./export-data/ \
  --schema schema.json \
  --output snapshot.db
```

The `--source` directory should follow DynamoDB Export structure:

```
export-data/
├── Users/
│   └── data/
│       └── 00000000.json.gz
└── Orders/
    └── data/
        └── 00000000.json.gz
```

The `--schema` file contains DescribeTable JSON (the output of `aws dynamodb describe-table`):

```sh
aws dynamodb describe-table --table-name Users > schema.json
```

### Table filtering

```sh
dynoxide import --source ./export/ --schema schema.json --output snapshot.db \
  --tables Users,Orders
```

### Anonymisation

Create a rules file (`rules.toml`):

```toml
[[rules]]
match = "attribute_exists(email)"
path = "email"
action = { type = "fake", generator = "safe_email" }

[[rules]]
match = "attribute_exists(phone)"
path = "phone"
action = { type = "mask", keep_last = 4, mask_char = "*" }

[[rules]]
match = "attribute_exists(ssn)"
path = "ssn"
action = { type = "hash", salt_env = "ANON_SALT" }

[[rules]]
match = "attribute_exists(notes)"
path = "notes"
action = { type = "redact" }

[consistency]
fields = ["userId", "email"]
```

```sh
ANON_SALT=my-secret-salt dynoxide import \
  --source ./export/ \
  --schema schema.json \
  --rules rules.toml \
  --output anonymised.db
```

**Action types:**

| Action | Description |
|--------|-------------|
| `fake` | Replace with generated data (`safe_email`, `name`, `phone_number`, `address`, `company_name`, `sentence`, `word`, `first_name`, `last_name`) |
| `mask` | Keep last N characters, mask the rest (`keep_last`, `mask_char`) |
| `hash` | SHA-256 hash with salt from env var (`salt_env`, required) |
| `redact` | Replace with `[REDACTED]` |
| `null` | Replace with NULL |

**Consistency:** Fields listed in `[consistency].fields` produce the same anonymised value across all tables in a single import run. Same input + same salt = same output.

### Options

```sh
# Overwrite an existing output file
dynoxide import --source ./export/ --schema schema.json --output snapshot.db --force

# Continue importing when a batch fails instead of aborting
dynoxide import --source ./export/ --schema schema.json --output snapshot.db --continue-on-error

# Compress output with zstd
dynoxide import --source ./export/ --schema schema.json --output snapshot.db --compress
# Produces snapshot.db.zst
```

## Library Usage (Rust)

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

### Testing with Embedded Mode

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
    // db is dropped automatically — nothing to clean up
}
```

No Docker. No port conflicts. No table name prefixes. Tests run in parallel without coordination.

## Feature Flags

| Flag | Default | Description |
|------|---------|-------------|
| `native-sqlite` | Yes | Bundles plain SQLite. No OpenSSL. |
| `http-server` | Yes | Adds axum-based HTTP server exposing the DynamoDB JSON API. |
| `mcp-server` | Yes | Adds MCP server for coding agents (stdio and Streamable HTTP transports). |
| `import` | Yes | Adds `dynoxide import` CLI for importing DynamoDB Export data with anonymisation. |
| `encryption` | No | Bundles SQLCipher + vendored OpenSSL. Adds `Database::new_encrypted()` for encryption at rest. |
| `encrypted-server` | No | Convenience: enables `encryption` + `http-server`. |
| `encrypted-full` | No | Convenience: enables `encryption` + `http-server` + `mcp-server` + `import`. |
| `full` | — | Alias for default features (backward compatibility). |

`native-sqlite` and `encryption` are **mutually exclusive** — they select different SQLite backends. To use encryption:

```toml
dynoxide-rs = { version = "0.9", default-features = false, features = ["encryption"] }
```

**Workspace note:** Cargo unifies features across a workspace. If any crate depends on `dynoxide-rs` with default features (getting `native-sqlite`) and another uses `encryption`, both activate and the build fails. Use `default-features = false` on all `dynoxide-rs` dependencies in the workspace.

## Supported Operations

| Category | Operations |
|----------|-----------|
| Table | CreateTable, DeleteTable, DescribeTable, ListTables, UpdateTable |
| Item | PutItem, GetItem, DeleteItem, UpdateItem |
| Query & Scan | Query, Scan |
| Batch | BatchGetItem, BatchWriteItem |
| Transactions | TransactWriteItems, TransactGetItems |
| PartiQL | ExecuteStatement, BatchExecuteStatement |
| Streams | DescribeStream, GetShardIterator, GetRecords, ListStreams |
| TTL | UpdateTimeToLive, DescribeTimeToLive |
| Tags | TagResource, UntagResource, ListTagsOfResource |

### Expression Support

- KeyConditionExpression
- FilterExpression
- ConditionExpression (attribute_exists, attribute_not_exists, begins_with, contains, size, between, in)
- ProjectionExpression
- UpdateExpression (SET, REMOVE, ADD, DELETE)

### Additional Features

- Global Secondary Indexes (GSI)
- DynamoDB Streams (NEW_IMAGE, OLD_IMAGE, NEW_AND_OLD_IMAGES, KEYS_ONLY)
- TTL with background sweep
- ReturnConsumedCapacity (TOTAL and INDEXES)
- ReturnValuesOnConditionCheckFailure
- ClientRequestToken idempotency for TransactWriteItems
- PartiQL SELECT, INSERT, UPDATE, DELETE with EXISTS/BEGINS_WITH functions
- Pagination with LastEvaluatedKey/ExclusiveStartKey (1MB page limit)
- Item size validation (400KB limit)
- Transaction size validation (4MB aggregate, 100 action limit)
- Batch size limits (16MB response, 100 keys for get, 25 items for write)

## Acknowledgements

Dynoxide's DynamoDB API semantics and validation logic were informed by [dynalite](https://github.com/architect/dynalite), the excellent DynamoDB emulator built on LevelDB by Michael Hart and now maintained by the Architect team.

Dynoxide is a clean-room Rust implementation — no code was ported directly — but [dynalite](https://github.com/architect/dynalite)'s thorough approach to matching live DynamoDB behaviour, including edge cases and error messages, was an invaluable reference.

Dynoxide utilises SQLite as its storage layer. (A choice validated by AWS's [DynamoDB Local](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html), which also uses SQLite internally.)

## License

Dual-licensed under MIT and Apache 2.0. See [LICENSE-MIT](LICENSE-MIT) and [LICENSE-APACHE](LICENSE-APACHE).
