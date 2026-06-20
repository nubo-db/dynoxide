# Dynoxide

[![crates.io](https://img.shields.io/crates/v/dynoxide-rs.svg)](https://crates.io/crates/dynoxide-rs) [![docs.rs](https://img.shields.io/docsrs/dynoxide-rs)](https://docs.rs/dynoxide-rs) [![CI](https://github.com/nubo-db/dynoxide/actions/workflows/ci.yml/badge.svg)](https://github.com/nubo-db/dynoxide/actions/workflows/ci.yml) [![conformance](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/nubo-db/dynamodb-conformance/main/results/dynoxide.badge.json)](https://paritysuite.org) [![license](https://img.shields.io/crates/l/dynoxide-rs.svg)](#license)

A DynamoDB emulator backed by SQLite. Runs as an HTTP server, an MCP server for coding agents, or embeds directly into Rust and iOS applications as a library.

## Why Dynoxide?

I built Dynoxide because DynamoDB Local is slow, heavy, and can't embed. It needs a JVM, and the typical Docker-based setups adds <!-- prose:ddb_local_cold_start -->~3 seconds<!-- /bench --> of cold-start, <!-- prose:ddb_local_idle_memory -->~194 MB<!-- /bench --> of memory at idle, and a <!-- prose:ddb_local_image_size -->~225MB<!-- /bench --> Docker image (<!-- prose:ddb_local_image_size_disk -->~471 MB<!-- /bench --> on disk) before you've done anything useful. If you're running integration tests, that's Docker starting, the JVM warming up, and your pipeline waiting.

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

Numbers from `ubuntu-latest` (2-core AMD EPYC 7763, 8GB RAM). Commit <!-- bench:ci_commit_link_root -->[`128d5f4`](../../commit/128d5f46e09e227423975ee772d7b8b031537126)<!-- /bench -->.

| Metric | Dynoxide (embedded) | Dynoxide (HTTP) | DynamoDB Local | LocalStack (all services) |
|---|---|---|---|---|
| Cold startup | <!-- bench:ci_startup_embedded -->**<1ms**<!-- /bench --> | <!-- bench:ci_startup_http -->**~2ms**<!-- /bench --> | <!-- bench:ci_startup_ddb_local -->~3,026ms<!-- /bench --> | <!-- bench:ci_startup_localstack -->~11,444ms<!-- /bench --> |
| GetItem (p50) | <!-- bench:ci_getitem_embedded -->17µs<!-- /bench --> | <!-- bench:ci_getitem_http -->0.3ms<!-- /bench --> | <!-- bench:ci_getitem_ddb_local -->0.8ms<!-- /bench --> | - |
| 50-test CI suite | <!-- bench:ci_suite_embedded_seq -->800ms<!-- /bench --> | <!-- bench:ci_suite_http_seq -->754ms<!-- /bench --> | <!-- bench:ci_suite_ddb_local_seq -->2,652ms<!-- /bench --> | - |
| Full workload (10K items) | - | <!-- bench:ci_workload_http -->**3.0s**<!-- /bench --> | <!-- bench:ci_workload_ddb_local -->11.5s<!-- /bench --> | - |
| Binary / image (download) | <!-- prose:ci_binary_download -->~3 MB<!-- /bench --> | <!-- prose:ci_binary_download_http -->~3 MB<!-- /bench --> | <!-- prose:ci_image_ddb_local_download -->225 MB<!-- /bench --> | <!-- prose:ci_image_localstack_download -->1.1 GB<!-- /bench --> |
| Binary / image (on disk) | <!-- bench:ci_binary_size -->6 MB<!-- /bench --> | <!-- bench:ci_binary_size_http -->6 MB<!-- /bench --> | <!-- bench:ci_image_ddb_local -->471 MB<!-- /bench --> | <!-- bench:ci_image_localstack -->1.1 GB<!-- /bench --> |
| Idle memory (RSS) | <!-- bench:ci_memory_embedded_idle -->~4.9 MB<!-- /bench --> | <!-- bench:ci_memory_http_idle -->~8 MB<!-- /bench --> | <!-- bench:ci_memory_ddb_local_idle -->~194 MB<!-- /bench --> | <!-- bench:ci_memory_localstack_idle -->~494 MB<!-- /bench --> |

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
| Runtime dependency | - | JVM | Docker + LocalStack | Node.js |
| Embeddable (Rust / iOS) | ✓ | - | - | - |
| MCP server for agents | ✓ | - | - | - |

LocalStack uses DynamoDB Local internally as its DynamoDB engine, so its startup and memory overhead includes DynamoDB Local's JVM plus LocalStack's own Python routing layer.


## Quick Start

Install from npm and start a local server:

```sh
npm install --save-dev dynoxide
npx dynoxide --port 8000
```

Or run it in Docker, a drop-in for `amazon/dynamodb-local`:

```sh
docker run --rm -p 8000:8000 ghcr.io/nubo-db/dynoxide
```

Point any AWS SDK or DynamoDB client at `http://localhost:8000`. For Homebrew, Cargo, pre-built binaries, and embedding as a Rust library, see the [installation guide](https://github.com/nubo-db/dynoxide/blob/main/docs/installation.md).

## Documentation

- [Installation](https://github.com/nubo-db/dynoxide/blob/main/docs/installation.md) - npm, Homebrew, Cargo, binaries, GitHub Actions, and Docker
- [HTTP server](https://github.com/nubo-db/dynoxide/blob/main/docs/http-server.md) - running the DynamoDB-compatible HTTP API
- [MCP server](https://github.com/nubo-db/dynoxide/blob/main/docs/mcp.md) - the Model Context Protocol server for coding agents
- [DynamoDB Streams](https://github.com/nubo-db/dynoxide/blob/main/docs/streams.md) - enabling and reading stream records
- [Import CLI](https://github.com/nubo-db/dynoxide/blob/main/docs/import.md) - loading data, table filtering, and anonymisation
- [WebAssembly (preview)](https://github.com/nubo-db/dynoxide/blob/main/docs/wasm.md) - the browser build and embed contract
- [Using as a Rust library](https://github.com/nubo-db/dynoxide/blob/main/docs/library.md) - embedded mode and feature flags
- [Compatibility](https://github.com/nubo-db/dynoxide/blob/main/docs/compatibility-summary.md) - operation, expression, and PartiQL coverage versus DynamoDB
- [Releasing](https://github.com/nubo-db/dynoxide/blob/main/docs/RELEASING.md) - release cadence and process

## Supported Operations

Dynoxide implements the DynamoDB API across tables, items, query and scan, batches, transactions, PartiQL, streams, TTL, and tags, with GSI and LSI support, the full expression syntax, and DynamoDB-compatible pagination, validation, and error codes. For the operation-by-operation breakdown and a comparison, see the [compatibility summary](https://github.com/nubo-db/dynoxide/blob/main/docs/compatibility-summary.md).

## Limitations

Dynoxide is built for local development, testing, and CI, not as a production DynamoDB replacement, so two classes of thing are missing on purpose.

Cloud-only operations with no local equivalent aren't implemented: backups and point-in-time restore, global tables, Kinesis streaming, resource policies, and capacity management. Call one and you get an `UnknownOperationException`.

A few behavioural differences are also worth knowing when you test against it:

- `ConsistentRead` is accepted but changes nothing. SQLite is strongly consistent, so every read already is - you can't reproduce eventually-consistent reads.
- Streams expose a single shard. `DescribeStream` returns one shard, and its `ExclusiveStartShardId` and `Limit` paging parameters are accepted but ignored.
- Transaction-contention errors (`TransactionConflictException`, `TransactionInProgressException`) aren't emulated - there's no concurrent contention in a single process.

For the live, per-feature support matrix see [paritysuite.org/capabilities](https://paritysuite.org/capabilities), and the full operation-by-operation breakdown is in the [compatibility summary](https://github.com/nubo-db/dynoxide/blob/main/docs/compatibility-summary.md).

## Acknowledgements

Dynoxide's DynamoDB API semantics and validation logic were informed by [dynalite](https://github.com/architect/dynalite), the excellent DynamoDB emulator built on LevelDB by Michael Hart and now maintained by the Architect team.

Dynoxide is a clean-room Rust implementation. No code was ported directly, but [dynalite](https://github.com/architect/dynalite)'s thorough approach to matching live DynamoDB behaviour, including edge cases and error messages, was an invaluable reference.

Dynoxide uses SQLite as its storage layer. (AWS's [DynamoDB Local](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html) also uses SQLite internally.)

## License

Dual-licensed under MIT and Apache 2.0. See [LICENSE-MIT](LICENSE-MIT) and [LICENSE-APACHE](LICENSE-APACHE).

## Trademarks

Amazon DynamoDB, DynamoDB, and AWS are trademarks of Amazon.com, Inc. or its affiliates. Dynoxide is an independent project and is not affiliated with, endorsed by, or sponsored by Amazon, and nothing here grants any right to use those names or marks.
