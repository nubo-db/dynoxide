# Dynoxide

A DynamoDB emulator backed by SQLite. Runs as an HTTP server, an MCP server for coding agents, or embeds directly into Rust and iOS applications as a library.

## Why Dynoxide?

I built Dynoxide because DynamoDB Local is slow, heavy, and can't embed. It needs a JVM, and the typical Docker-based setups adds <!-- prose:ddb_local_cold_start -->2–3 seconds<!-- /bench --> of cold-start, <!-- prose:ddb_local_idle_memory -->~188 MB<!-- /bench --> of memory at idle, and a <!-- prose:ddb_local_image_size -->~225MB<!-- /bench --> Docker image (<!-- prose:ddb_local_image_size_disk -->~471 MB<!-- /bench --> on disk) before you've done anything useful. If you're running integration tests, that's Docker starting, the JVM warming up, and your pipeline waiting.

Dynoxide is a native binary. It starts in milliseconds, idles at <!-- prose:dynoxide_idle_memory -->~4.9 MB<!-- /bench -->, and ships as a <!-- prose:dynoxide_binary_size -->~3 MB<!-- /bench --> download. Point any DynamoDB SDK at it and your tests just work.

For Rust projects, there's also an **embedded mode** - direct API calls via `Database::memory()` with no HTTP layer at all. Each test gets an isolated in-memory database with zero startup cost. And because it compiles to a native library with no runtime dependencies, it runs on platforms where DynamoDB Local can't, including iOS.

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

Numbers from `ubuntu-latest` (2-core AMD EPYC 7763, 8GB RAM). Commit <!-- bench:ci_commit_link_root -->[`f5052db`](../../commit/f5052db2ac87597e5a1993037f89df9740e4cd74)<!-- /bench -->.

| Metric | Dynoxide (embedded) | Dynoxide (HTTP) | DynamoDB Local | LocalStack (all services) |
|---|---|---|---|---|
| Cold startup | <!-- bench:ci_startup_embedded -->**<1ms**<!-- /bench --> | <!-- bench:ci_startup_http -->**~2ms**<!-- /bench --> | <!-- bench:ci_startup_ddb_local -->~2,769ms<!-- /bench --> | <!-- bench:ci_startup_localstack -->~8,627ms<!-- /bench --> |
| GetItem (p50) | <!-- bench:ci_getitem_embedded -->14µs<!-- /bench --> | <!-- bench:ci_getitem_http -->0.3ms<!-- /bench --> | <!-- bench:ci_getitem_ddb_local -->0.8ms<!-- /bench --> | — |
| 50-test CI suite | <!-- bench:ci_suite_embedded_seq -->722ms<!-- /bench --> | <!-- bench:ci_suite_http_seq -->731ms<!-- /bench --> | <!-- bench:ci_suite_ddb_local_seq -->2,265ms<!-- /bench --> | — |
| Full workload (10K items) | — | <!-- bench:ci_workload_http -->**2.9s**<!-- /bench --> | <!-- bench:ci_workload_ddb_local -->10.8s<!-- /bench --> | — |
| Binary / image (download) | <!-- prose:ci_binary_download -->~3 MB<!-- /bench --> | <!-- prose:ci_binary_download_http -->~3 MB<!-- /bench --> | <!-- prose:ci_image_ddb_local_download -->225 MB<!-- /bench --> | <!-- prose:ci_image_localstack_download -->1.1 GB<!-- /bench --> |
| Binary / image (on disk) | <!-- bench:ci_binary_size -->6 MB<!-- /bench --> | <!-- bench:ci_binary_size_http -->6 MB<!-- /bench --> | <!-- bench:ci_image_ddb_local -->471 MB<!-- /bench --> | <!-- bench:ci_image_localstack -->1.1 GB<!-- /bench --> |
| Idle memory (RSS) | <!-- bench:ci_memory_embedded_idle -->~4.9 MB<!-- /bench --> | <!-- bench:ci_memory_http_idle -->~8 MB<!-- /bench --> | <!-- bench:ci_memory_ddb_local_idle -->~188 MB<!-- /bench --> | <!-- bench:ci_memory_localstack_idle -->~358 MB<!-- /bench --> |

> The gap is wider on Apple Silicon because the faster CPU amplifies the difference between native code and JVM overhead. Both are real measurements of the same benchmark suite. [Full methodology and per-operation breakdowns →](benchmarks/README.md)

### Conformance

Dynoxide is continuously verified against real DynamoDB by the [dynamodb-conformance](https://github.com/nubo-db/dynamodb-conformance) suite, which runs one test matrix against AWS itself and every major DynamoDB emulator. Pass rates move as the suite grows and each engine changes, so rather than pin a snapshot that goes stale, see the live standings:

- **[paritysuite.org](https://paritysuite.org)**: current pass rates for every engine, broken down by tier
- **[nubo-db/dynamodb-conformance](https://github.com/nubo-db/dynamodb-conformance#results)**: the suite itself, the raw results, and how each target is run

This covers the native build. The [WebAssembly](#webassembly-preview) build is a preview and isn't run against the suite yet.

### How It Compares

| | Dynoxide | DynamoDB Local | LocalStack (all services) | dynalite |
|---|---|---|---|---|
| Language | Rust | Java | Python + Java | Node.js |
| Storage | SQLite | SQLite | SQLite (via DDB Local) | LevelDB |
| Runtime dependency | — | JVM | Docker + LocalStack | Node.js |
| Embeddable (Rust / iOS) | ✓ | — | — | — |
| MCP server for agents | ✓ | — | — | — |

LocalStack uses DynamoDB Local internally as its DynamoDB engine, so its startup and memory overhead includes DynamoDB Local's JVM plus LocalStack's own Python routing layer.

## Installation

### npm

```sh
npm install --save-dev dynoxide
```

Or run directly without installing:

```sh
npx dynoxide --port 8000
```

### Homebrew (macOS)

```sh
brew install nubo-db/tap/dynoxide
```

### Pre-built binaries

Download from [GitHub Releases](https://github.com/nubo-db/dynoxide/releases) for Linux (x86_64, aarch64), macOS (Intel, Apple Silicon), and Windows.

```sh
# Example: Linux x86_64
curl -fsSL https://github.com/nubo-db/dynoxide/releases/latest/download/dynoxide-x86_64-unknown-linux-musl.tar.gz | tar xz
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
# Minimal - just the embedded database, no server or CLI dependencies
dynoxide-rs = { version = "0.10", default-features = false, features = ["native-sqlite"] }

# Or with encryption:
# dynoxide-rs = { version = "0.10", default-features = false, features = ["encryption"] }
```

### Upgrading from 0.9.x

0.10.0 is a breaking release, but most of the breaks are library-only. The [CHANGELOG](CHANGELOG.md) has the full list.

**Running the binary** (Homebrew, npm, the release archives, or the Docker image)? One change affects you:

- **MCP over HTTP now requires a bearer token.** Existing HTTP-transport clients break until they send an `Authorization: Bearer <token>` header. A loopback bind generates and persists a token on first run; a non-loopback bind will not start without one (`--mcp-token` or `DYNOXIDE_MCP_AUTH_TOKEN`). The stdio transport is unaffected, and plain `dynoxide serve` (DynamoDB only, no MCP) is unchanged. See [MCP Server](#mcp-server).

**Depending on the `dynoxide-rs` crate?** Also note:

- **`DynoxideError` is now `#[non_exhaustive]`.** Code that matches it exhaustively needs a `_ =>` arm.
- **`Database` is now generic, `Database<S>`.** The parameter defaults to the native backend, so code that names `Database` keeps compiling; a new `NativeDatabase` alias names that default explicitly.
- **Embedding the MCP HTTP server:** `dynoxide::mcp::serve_http` and `serve_http_with_shutdown` take an `HttpOptions` struct (bind host, auth mode, allowed hosts) in place of a bare port.

### GitHub Actions

```yaml
- uses: nubo-db/dynoxide/action@v0.10.0
  with:
    snapshot-url: https://example.com/test-data.db.zst  # optional
    port: 8000
```

See [action/action.yml](action/action.yml) for all inputs and outputs.

### Docker

A 5 MB drop-in for `amazon/dynamodb-local` in containerised test suites. Same DynamoDB-compatible API, faster startup, smaller image. Note that this is a packaging convenience for test fixtures, not a containerised database product; production-database-on-Kubernetes patterns are out of scope.

```sh
docker run --rm -p 8000:8000 ghcr.io/nubo-db/dynoxide
```

With persistent storage:

```sh
docker run --rm -p 8000:8000 \
  -v "$(pwd)/data:/data" \
  ghcr.io/nubo-db/dynoxide \
  serve --host 0.0.0.0 --port 8000 --db-path /data/dynoxide.sqlite
```

The image runs as root by default, matching `amazon/dynamodb-local`, so bind mounts on Linux Just Work without `--user`. The canonical image lives at `ghcr.io/nubo-db/dynoxide`. Mirrors are pushed to `docker.io/nubodb/dynoxide` and `public.ecr.aws/h4s0n6a2/dynoxide` on a best-effort basis. SLSA provenance and SBOM attestations are published to GHCR only; if you want to verify provenance, pull from the GHCR canonical.

If you override `CMD` to bind to a different port, set the healthcheck target with environment variables so the container's `HEALTHCHECK` follows:

```sh
docker run -e DYNOXIDE_HEALTHCHECK_PORT=9000 ghcr.io/nubo-db/dynoxide serve --port 9000
```

`DYNOXIDE_HEALTHCHECK_HOST` and `DYNOXIDE_HEALTHCHECK_PORT` are documented public surface and will not be renamed in a patch or minor release.

#### Running as nonroot

For security-conscious operators, opt into a nonroot uid:

```sh
docker run --rm -p 8000:8000 --user 65532:65532 ghcr.io/nubo-db/dynoxide
```

Persistent mode under nonroot needs a host-owned bind mount, since the in-image `/data` is owned by root:

```sh
docker run --rm -p 8000:8000 \
  --user "$(id -u):$(id -g)" \
  -v "$(pwd)/data:/data" \
  ghcr.io/nubo-db/dynoxide \
  serve --host 0.0.0.0 --port 8000 --db-path /data/dynoxide.sqlite
```

The default in-memory mode needs no flags whether root or nonroot. The uid 65532 is the well-known nonroot uid used by Google's distroless images; pick any uid you prefer with `--user <uid>:<gid>`.

#### MCP over HTTP in Docker

The default image serves DynamoDB only. To also expose the [MCP](#mcp-server) Streamable HTTP transport, override the command to start it on `0.0.0.0` and supply a bearer token. The token is **mandatory** for any non-loopback bind. Pass it via the `DYNOXIDE_MCP_AUTH_TOKEN` environment variable (which keeps it out of shell history and `ps`), not a `--mcp-token` flag:

```sh
TOKEN=$(openssl rand -base64 24)

docker run --rm -p 8000:8000 -p 19280:19280 \
  -e DYNOXIDE_MCP_AUTH_TOKEN="$TOKEN" \
  ghcr.io/nubo-db/dynoxide \
  serve --host 0.0.0.0 --port 8000 \
        --mcp --mcp-host 0.0.0.0 --mcp-port 19280
```

DynamoDB is then reachable on `http://localhost:8000` and MCP on `http://localhost:19280/mcp`. Point an HTTP-transport MCP client at the latter with an `Authorization: Bearer <token>` header. See [MCP Server](#mcp-server) for the client config shape.

A few things to know:

- **The token is not optional.** Omit it and the container exits immediately with `a non-loopback MCP bind requires an explicit token`. The default `docker run ghcr.io/nubo-db/dynoxide` stays DynamoDB-only precisely because a token-less `0.0.0.0` MCP bind cannot boot.
- **Reaching MCP from another container** by service name (rather than `localhost`) needs that name added to the Host allowlist: `--mcp-allowed-host <name>` (e.g. `--mcp-allowed-host dynoxide`). The `-p`-mapped `localhost` access above needs nothing extra.
- **`--network host`** (Linux only) is an alternative to `-p`, but it bypasses Docker network isolation and binds MCP directly on the host's network interface, reachable from the LAN, not just the host. Prefer `-p` unless you specifically need host networking.

## WebAssembly (preview)

Dynoxide compiles to `wasm32-unknown-unknown` and runs in the browser. The same engine that backs the native build runs against [wa-sqlite](https://github.com/rhashimoto/wa-sqlite) - a WASM build of SQLite - over a wasm-bindgen bridge, with the database persisted to [OPFS](https://developer.mozilla.org/en-US/docs/Web/API/File_System_API/Origin_private_file_system) (the origin private file system).

Both backends issue the same SQL. The native and wasm code share one set of query builders, so a query fixed on one is fixed on both.

It's a preview. The wasm build is **not** run against the conformance suite that backs the native build, so its correctness rests on its own tests for now. A build made with `--features wasm-sqlite` exposes `dynoxide::WASM_PREVIEW` (`true`) so you can tell which path you're on.

**What works:** create and delete tables, describe and list them, put, get, delete, and update items, query, scan, and the batch and transactional reads (`BatchGetItem`, `BatchWriteItem`, `TransactGetItems`), over base tables and both secondary index types (GSI and LSI). Index maintenance is atomic with the base write, same as native.

**What doesn't, yet:** TTL returns a typed `Unsupported` error (it needs a background sweep the browser doesn't drive). Streams are planned but not wired - the delivery mechanism is still to be decided. `TransactWriteItems`, tags, table-setting updates, table stats, and bulk import return a preview "not yet implemented" error.

The engine runs in a Web Worker (OPFS's synchronous file handles are Worker-only), and the page talks to it over a message channel. It needs no special server headers (no COOP/COEP cross-origin isolation), so it works on ordinary static hosting.

### Building and shipping it

`npm install` then `npm run build:wasm` produces a self-contained `dist/` (use `build:wasm:dev` to skip wasm-opt for speed):

```bash
npm install
npm run build:wasm
```

`dist/` is the two `.wasm` plus the bundled Worker, kept separate so the `.wasm` cache independently of the JS bundle, and a small manifest:

| File | Size | What |
|---|---|---|
| `dynoxide_bg.wasm` | ~960 KB | the engine (release, wasm-opt) |
| `wa-sqlite.wasm` | ~545 KB | SQLite (the synchronous build) |
| `dynoxide-worker.js` | ~130 KB | the bundled Web Worker (wa-sqlite glue + bridge) |
| `manifest.json` | <1 KB | engine version, contract version, file list |

About 1.6 MB total. Not tiny, but the `.wasm` files are immutable and cache well, and using wa-sqlite's synchronous build keeps it off the larger Asyncify async build.

Drop `dist/` on any origin that's a [secure context](https://developer.mozilla.org/en-US/docs/Web/Security/Secure_Contexts) - HTTPS in production, or `localhost` for development. OPFS needs a secure context, but **no COOP/COEP headers and no cross-origin isolation**, so plain static hosting works. (SQLite in the browser usually needs cross-origin isolation, because the common technique makes an async storage API look synchronous via `SharedArrayBuffer`. Dynoxide avoids that by running wa-sqlite's synchronous OPFS VFS inside a Worker, where synchronous file handles are available directly.) One header does matter: if you set a Content-Security-Policy it must allow `'wasm-unsafe-eval'`, or the engine won't instantiate. Serve the `.wasm` as `application/wasm` while you're at it.

### The embed contract

Spawn the bundle as a module Worker and drive it over `postMessage`; the two `.wasm` files must sit next to `dynoxide-worker.js`, which is where the build puts them. The Worker speaks one coarse RPC: a message in, a reply out, correlated by an `id` you supply.

```text
in:   { id, op, payload, contractVersion? }
out:  { id, ok: true,  result }      // result is a JSON string
      { id, ok: false, error }       // error is a JSON string
```

Three ops carry the engine:

- `open` - `payload: { name }` opens (or reopens) the OPFS-backed database and resolves with the contract descriptor, `{ contractVersion, capabilities }`. Call it once before any operation.
- `execute` - `payload: { op, request }` runs one DynamoDB operation, where `op` is the operation name (`PutItem`, `Query`, `Scan`, ...) and `request` is a plain DynamoDB-JSON object. It resolves with the response JSON and rejects with an error envelope (the same `__type`/`message` shape the native HTTP server speaks). Ask `capabilities` for the supported set rather than guessing; anything outside it comes back as an `UnsupportedOperation` envelope.
- `capabilities` and `contractVersion` - the supported op list and the engine's contract version, for a client that wants them without opening a database.

`contractVersion` stamps the envelope shape, not the engine version. Adding an op is additive and leaves it alone; changing the request, response, or error envelope bumps it. Stamp your messages with the version you built against and the Worker rejects a mismatch loudly, so a stale embed fails with a clear error instead of mis-parsing a newer engine. The shipped version sits in `manifest.json` and is what `open` echoes back.

The harness under `harness/` is a working example, and it loads the same bundled Worker a production consumer would:

```bash
npm run build:wasm
python3 -m http.server 8081
# then open http://localhost:8081/harness/
```

It opens the engine, creates a table, writes a few rows, then runs a query and a filtered scan against the OPFS-backed database so you can see `ScannedCount` come back higher than `Count`. Because it drives the shipping bundle rather than a parallel build, a green harness means the shipping artefact works. (The older smoke ops live behind `npm run build:wasm:harness`, which adds them on top of the same Worker.)

### The engine package

Rather than build the engine yourself, you can depend on the same artefacts as an npm package, `@nubo-db/dynoxide-engine`. `npm run build:wasm` assembles it under `npm/dynoxide-engine/` - the Worker, the two `.wasm`, the manifest, and an `EngineClient` that owns the RPC above so you deal in objects, not `postMessage` envelopes:

```js
import { EngineClient } from "@nubo-db/dynoxide-engine";

const client = new EngineClient();        // resolves the Worker beside the package
await client.ready();

await client.execute("CreateTable", { /* ... */ });
const { Items } = await client.execute("Query", { /* ... */ });
```

`new EngineClient()` with no arguments resolves the Worker next to the package, and the Worker resolves the `.wasm` next to itself, so a bundler that copies the package's files - or a plain static deploy of them - needs no configuration. Serving the assets from a CDN or another origin? Pass `assetBase` (the directory they sit in) or `workerUrl` (the exact Worker URL).

The package also exports `EngineError` (the typed rejection, carrying the engine's `__type` on `.type`) and `CONTRACT_VERSION`. The client checks that version against the engine on boot and fails loudly on a mismatch, so a pinned consumer never mis-reads a newer engine. Hosting matches `dist/`: a secure context, no COOP/COEP, a CSP that allows `'wasm-unsafe-eval'`, and `.wasm` served as `application/wasm`. It's a preview, like the rest of the wasm build.

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

Works with any language or SDK that supports custom endpoints: Python (boto3), Node.js (AWS SDK v3), Go, Java, etc.

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

The HTTP transport requires a bearer token on every request. On a loopback
bind with no token supplied, dynoxide generates one on first run, saves it to a
per-user config file (`~/.config/dynoxide/mcp-token` on Linux,
`~/Library/Application Support/dynoxide/mcp-token` on macOS), and prints a
ready-to-paste client snippet; later runs reuse it silently. Supply your own
with `--token` or the `DYNOXIDE_MCP_AUTH_TOKEN` environment variable (the flag
wins if both are set).

| Flag | Purpose |
|------|---------|
| `--host <HOST>` | Bind address (default `127.0.0.1`). Non-loopback binds require an explicit token. |
| `--token <TOKEN>` / `DYNOXIDE_MCP_AUTH_TOKEN` | Use a fixed token instead of the persisted one. |
| `--allowed-host <HOST>` | Accept an additional `Host` header by name (repeatable); needed for non-loopback access by hostname. |
| `--no-auth` | Disable authentication. Loopback binds only; prints a warning. |

Prefer the environment variable or the persisted file over `--token` for
anything beyond one-shot debugging, because flag values leak into shell history and
`ps`. To rotate the token, delete the persisted file (or change
`DYNOXIDE_MCP_AUTH_TOKEN`) and restart; there is no rotation mechanism by
design.

On the `serve` subcommand the equivalent flags are prefixed
(`--mcp-host`, `--mcp-token`, `--mcp-no-auth`, `--mcp-allowed-host`) because
`serve` already owns `--host`/`--port` for the DynamoDB server.

To run the HTTP transport from the container image, see
[MCP over HTTP in Docker](#mcp-over-http-in-docker).

#### HTTP client configuration

Point an HTTP-transport MCP client at the endpoint and send the token in an
`Authorization` header:

```json
{
  "mcpServers": {
    "dynoxide": {
      "type": "http",
      "url": "http://127.0.0.1:19280/mcp",
      "headers": { "Authorization": "Bearer <TOKEN>" }
    }
  }
}
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

### Available tools (34)

| Category | Tools |
|----------|-------|
| Tables | `list_tables`, `describe_table`, `create_table`, `delete_table`, `update_table` |
| Items | `get_item`, `put_item`, `update_item`, `delete_item` |
| Batch | `batch_get_item`, `batch_write_item`, `bulk_put_items` |
| Query | `query`, `scan` |
| Transactions | `transact_get_items`, `transact_write_items` |
| PartiQL | `execute_partiql`, `batch_execute_partiql`, `execute_transaction_partiql` |
| TTL | `update_time_to_live`, `describe_time_to_live`, `sweep_ttl` |
| Tags | `tag_resource`, `untag_resource`, `list_tags_of_resource` |
| Streams | `list_streams`, `describe_stream`, `get_shard_iterator`, `get_records` |
| Snapshots | `create_snapshot`, `restore_snapshot`, `list_snapshots`, `delete_snapshot` |
| Info | `get_database_info` |

### Safety options

```sh
# Read-only mode - rejects all write operations
dynoxide mcp --read-only --db-path prod-snapshot.db

# Limit query/scan results
dynoxide mcp --max-items 100 --max-size-bytes 65536
```

### Snapshots

The MCP server supports database snapshots for safe experimentation:

- `create_snapshot` - saves a point-in-time copy of the database
- `restore_snapshot` - rolls back to a previous snapshot
- `list_snapshots` - lists available snapshots
- Auto-snapshot before `delete_table` (last 10 kept automatically)

### Data Model Context

For single-table designs, raw DynamoDB metadata (`pk` is type `S`, `GSI1` exists) tells an agent almost nothing. The `--data-model` flag loads a [OneTable](https://doc.onetable.io/) schema so the agent sees entity names, key templates, GSI mappings, and type discriminator attributes.

```sh
dynoxide mcp --data-model schema.json
dynoxide mcp --data-model schema.json --db-path data.db
```

The data model is context-only - dynoxide does not validate writes against the schema. See [docs/mcp-data-model.md](docs/mcp-data-model.md) for the full format reference, options, and examples.

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

If the `--schema` file (DescribeTable JSON) contains a `StreamSpecification`, streams are automatically enabled on the imported table. No extra flags needed. The import faithfully reproduces the source table's configuration:

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
    // db is dropped automatically - nothing to clean up
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
| `cli` | Indirect | Gates the `dynoxide` binary. Pulled in automatically by `http-server`, `mcp-server`, or `import`, so default builds include it; a library-only or `wasm-sqlite` build omits the binary. |
| `wasm-sqlite` | No | wasm32 browser backend (wa-sqlite over OPFS), a preview. Pulls neither native SQLite nor the CLI. See the WASM section. |
| `encryption` | No | Bundles SQLCipher + vendored OpenSSL. Adds `Database::new_encrypted()` for encryption at rest. |
| `encryption-cc` | No | Like `encryption` but uses Apple CommonCrypto instead of bundled OpenSSL. For macOS and iOS builds. |
| `encrypted-server` | No | Convenience: enables `encryption` + `http-server`. |
| `encrypted-server-cc` | No | Convenience: enables `encryption-cc` + `http-server`. |
| `encrypted-full` | No | Convenience: enables `encryption` + `http-server` + `mcp-server` + `import`. |
| `full` | — | Alias for default features (backward compatibility). |

`native-sqlite` and `encryption` are **mutually exclusive** - they select different SQLite backends. To use encryption:

```toml
dynoxide-rs = { version = "0.10", default-features = false, features = ["encryption"] }
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
| PartiQL | ExecuteStatement, BatchExecuteStatement, ExecuteTransaction |
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

Dynoxide is a clean-room Rust implementation. No code was ported directly, but [dynalite](https://github.com/architect/dynalite)'s thorough approach to matching live DynamoDB behaviour, including edge cases and error messages, was an invaluable reference.

Dynoxide uses SQLite as its storage layer. (AWS's [DynamoDB Local](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html) also uses SQLite internally.)

## License

Dual-licensed under MIT and Apache 2.0. See [LICENSE-MIT](LICENSE-MIT) and [LICENSE-APACHE](LICENSE-APACHE).
