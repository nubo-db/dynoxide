# Dynoxide

[![crates.io](https://img.shields.io/crates/v/dynoxide-rs.svg)](https://crates.io/crates/dynoxide-rs) [![docs.rs](https://img.shields.io/docsrs/dynoxide-rs)](https://docs.rs/dynoxide-rs) [![CI](https://github.com/nubo-db/dynoxide/actions/workflows/ci.yml/badge.svg)](https://github.com/nubo-db/dynoxide/actions/workflows/ci.yml) [![conformance](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/paritysuite/dynamodb-conformance/main/results/dynoxide.badge.json)](https://paritysuite.org) [![license](https://img.shields.io/crates/l/dynoxide-rs.svg)](#license)

A DynamoDB emulator in Rust, backed by SQLite. Runs as an HTTP server or an MCP server for coding agents, embeds into Rust and iOS applications as a library, and compiles to WebAssembly for the browser.

> The crates.io badge above shows the **crate** version, which covers the Rust library alone. Everything you install rather than depend on carries a separate **product** version: the binary, the npm packages, the container images, the browser engine and the Action. That is the number `dynoxide --version` prints and the one to quote in a bug report. [Why there are two](docs/versioning.md).

## Why Dynoxide?

I built Dynoxide because DynamoDB Local is slow, heavy, and can't embed. It needs a JVM, and the typical Docker-based setups adds <!-- prose:ddb_local_cold_start -->~2 seconds<!-- /bench --> of cold-start, <!-- prose:ddb_local_idle_memory -->~142 MB<!-- /bench --> of memory at idle, and a <!-- prose:ddb_local_image_size -->~225MB<!-- /bench --> Docker image (<!-- prose:ddb_local_image_size_disk -->~230 MB<!-- /bench --> on disk) before you've done anything useful. If you're running integration tests, that's Docker starting, the JVM warming up, and your pipeline waiting.

Dynoxide is a native binary. It starts in milliseconds, idles at <!-- prose:dynoxide_idle_memory -->~7.7 MB<!-- /bench -->, and ships as a <!-- prose:dynoxide_binary_size -->~3 MB<!-- /bench --> download. Point any DynamoDB SDK at it and your tests just work.

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

Numbers from `ubuntu-latest` (<!-- prose:ci_runner_hardware -->4-core AMD EPYC 7763, 16GB RAM<!-- /bench -->). Commit <!-- bench:ci_commit_link_root -->[`74b9474`](../../commit/74b947497c35cae049e45ad7050d0e3f823cf6e3)<!-- /bench -->.

| Metric | Dynoxide (embedded) | Dynoxide (HTTP) | DynamoDB Local | LocalStack (all services) | Floci | MiniStack |
|---|---|---|---|---|---|---|
| Cold startup | <!-- bench:ci_startup_embedded -->**<1ms**<!-- /bench --> | <!-- bench:ci_startup_http -->**~2ms**<!-- /bench --> | <!-- bench:ci_startup_ddb_local -->~1,714ms<!-- /bench --> | <!-- bench:ci_startup_localstack -->~12,431ms<!-- /bench --> | <!-- bench:ci_startup_floci -->~271ms<!-- /bench --> | <!-- bench:ci_startup_ministack -->~2,438ms<!-- /bench --> |
| GetItem (p50) | <!-- bench:ci_getitem_embedded -->16µs<!-- /bench --> | <!-- bench:ci_getitem_http -->0.3ms<!-- /bench --> | <!-- bench:ci_getitem_ddb_local -->0.8ms<!-- /bench --> | - | - | - |
| 50-test CI suite | <!-- bench:ci_suite_embedded_seq -->789ms<!-- /bench --> | <!-- bench:ci_suite_http_seq -->764ms<!-- /bench --> | <!-- bench:ci_suite_ddb_local_seq -->2,494ms<!-- /bench --> | - | - | - |
| Full workload (10K items) | - | <!-- bench:ci_workload_http -->**2.9s**<!-- /bench --> | <!-- bench:ci_workload_ddb_local -->11.6s<!-- /bench --> | - | - | - |
| Binary / image (download) | <!-- prose:ci_binary_download -->~3 MB<!-- /bench --> | <!-- prose:ci_binary_download_http -->~3 MB<!-- /bench --> | <!-- prose:ci_image_ddb_local_download -->225 MB<!-- /bench --> | <!-- prose:ci_image_localstack_download -->1.1 GB<!-- /bench --> | - | - |
| Binary / image (on disk) | <!-- bench:ci_binary_size -->8 MB<!-- /bench --> | <!-- bench:ci_binary_size_http -->8 MB<!-- /bench --> | <!-- bench:ci_image_ddb_local -->230 MB<!-- /bench --> | <!-- bench:ci_image_localstack -->333 MB<!-- /bench --> | <!-- bench:ci_image_floci -->90 MB<!-- /bench --> | <!-- bench:ci_image_ministack -->65 MB<!-- /bench --> |
| Idle memory (RSS) | <!-- bench:ci_memory_embedded_idle -->~7.7 MB<!-- /bench --> | <!-- bench:ci_memory_http_idle -->~8 MB<!-- /bench --> | <!-- bench:ci_memory_ddb_local_idle -->~142 MB<!-- /bench --> | <!-- bench:ci_memory_localstack_idle -->~390 MB<!-- /bench --> | <!-- bench:ci_memory_floci_idle -->~26 MB<!-- /bench --> | <!-- bench:ci_memory_ministack_idle -->~43 MB<!-- /bench --> |

> The gap is wider on Apple Silicon because the faster CPU amplifies the difference between native code and JVM overhead. Both are real measurements of the same benchmark suite. [Full methodology and per-operation breakdowns →](benchmarks/README.md)

### Conformance

Dynoxide is continuously verified against real DynamoDB by **Parity Suite**, the [DynamoDB conformance suite](https://github.com/paritysuite/dynamodb-conformance) that runs one test matrix against AWS itself and every major DynamoDB emulator. Pass rates move as the suite grows and each engine changes, so rather than pin a snapshot that goes stale, see the live standings:

- **[Live standings](https://paritysuite.org)**: current pass rates for every engine, broken down by tier
- **[paritysuite/dynamodb-conformance](https://github.com/paritysuite/dynamodb-conformance#results)**: the suite itself, the raw results, and how each target is run

Disclosure: Dynoxide and Parity Suite are maintained by the same person. The suite scores Dynoxide on the same public matrix it runs against every other engine, and the results and test code are open.

This covers the native build. The [WebAssembly](https://github.com/nubo-db/dynoxide/blob/main/docs/wasm.md) build is scored as its own row: it passes every test it implements, with a far higher skip count than any other target because several operations are still missing.

### How It Compares

| | Dynoxide | DynamoDB Local | LocalStack (all services) | Floci | MiniStack | dynalite |
|---|---|---|---|---|---|---|
| Language | Rust | Java | Python + Java | Java (GraalVM native image) | Python | Node.js |
| Storage | SQLite | SQLite | SQLite (via DDB Local) | Its own (in-memory, persistent or WAL) | Its own (in-memory, optional on-disk state) | LevelDB |
| Runtime dependency | - | JVM | Docker + a LocalStack account (auth token) | Docker | Docker or Python | Node.js |
| Embeddable (Rust / iOS) | ✓ | - | - | - | - | - |
| MCP server for agents | ✓ | - | - | - | - | - |

LocalStack uses DynamoDB Local internally as its DynamoDB engine, so its startup and memory overhead includes DynamoDB Local's JVM plus LocalStack's own Python routing layer. Since March 2026 its image needs a LocalStack account and `LOCALSTACK_AUTH_TOKEN` to start, even for the services that used to be free. Floci and MiniStack are general AWS emulators like LocalStack, each with its own DynamoDB engine rather than DynamoDB Local.


## Quick Start

Run a local server without installing anything:

```sh
npx dynoxide --port 8000
```

Or install it into a project to pin the version, after which `npx dynoxide` uses that copy:

```sh
npm install --save-dev dynoxide
```

Or run it in Docker, a drop-in for `amazon/dynamodb-local`:

```sh
docker run --rm -p 8000:8000 ghcr.io/nubo-db/dynoxide
```

Point any AWS SDK or DynamoDB client at `http://localhost:8000`. For Homebrew, Cargo, pre-built binaries, and embedding as a Rust library, see the [installation guide](https://github.com/nubo-db/dynoxide/blob/main/docs/installation.md).

Add `--log quiet` to suppress informational startup and shutdown messages on
stderr (useful in test suites). It works on `serve`, `mcp` and `import --serve`
or `--mcp`, and warnings and errors still print. Without `--log`, the output is
unchanged.

## Documentation

- [Installation](https://github.com/nubo-db/dynoxide/blob/main/docs/installation.md) - npm, Homebrew, Cargo, binaries, GitHub Actions, and Docker
- [HTTP server](https://github.com/nubo-db/dynoxide/blob/main/docs/http-server.md) - running the DynamoDB-compatible HTTP API
- [Testcontainers](https://github.com/nubo-db/dynoxide/blob/main/docs/testcontainers.md) - driving the container from a test suite, and the wait strategy it needs
- [MCP server](https://github.com/nubo-db/dynoxide/blob/main/docs/mcp.md) - the Model Context Protocol server for coding agents
- [DynamoDB Streams](https://github.com/nubo-db/dynoxide/blob/main/docs/streams.md) - enabling and reading stream records
- [Import CLI](https://github.com/nubo-db/dynoxide/blob/main/docs/import.md) - loading data, table filtering, and anonymisation
- [WebAssembly](https://github.com/nubo-db/dynoxide/blob/main/docs/wasm.md) - the browser build and embed contract
- [Using as a Rust library](https://github.com/nubo-db/dynoxide/blob/main/docs/library.md) - embedded mode and feature flags
- [Compatibility](https://github.com/nubo-db/dynoxide/blob/main/docs/compatibility-summary.md) - operation, expression, and PartiQL coverage versus DynamoDB
- [Versioning](https://github.com/nubo-db/dynoxide/blob/main/docs/versioning.md) - what the version number promises and what forces a major
- [Releasing](https://github.com/nubo-db/dynoxide/blob/main/docs/RELEASING.md) - release cadence and process

## Community

These integrations are built and maintained by people outside the project. There's a bit more on each at [dynoxide.dev/community](https://dynoxide.dev/community).

- [vitest-dynoxide](https://github.com/seek-oss/vitest-dynoxide) - runs Vitest integration tests against Dynoxide, with an in-memory instance per test file (SEEK, maintained by [Sam Chung](https://github.com/samchungy))
- [sbt-dynoxide](https://github.com/Hapag-Lloyd/sbt-dynoxide) - an sbt plugin for Scala builds that downloads Dynoxide and runs it around `test` and `testOnly` (Hapag-Lloyd, maintained by [Marcin Szałomski](https://github.com/baldram))
- [dynoxide-scala-core](https://github.com/Hapag-Lloyd/dynoxide-scala-core) - the build-tool-agnostic library behind sbt-dynoxide, for wiring Dynoxide into other JVM build tools (Hapag-Lloyd, maintained by [Marcin Szałomski](https://github.com/baldram))

Built something on top of Dynoxide? [Propose it for the list](https://github.com/nubo-db/dynoxide/issues/new?template=community-integration.yml).

## Supported Operations

Dynoxide implements the DynamoDB API across tables, items, query and scan, batches, transactions, PartiQL, streams, TTL, and tags, with GSI and LSI support, the full expression syntax, and DynamoDB-compatible pagination, validation, and error codes. For the operation-by-operation breakdown and a comparison, see the [compatibility summary](https://github.com/nubo-db/dynoxide/blob/main/docs/compatibility-summary.md).

## Limitations

Dynoxide is built for local development, testing, and CI, not as a production DynamoDB replacement, so two classes of thing are missing on purpose.

Cloud-only operations with no local equivalent aren't implemented: backups and point-in-time restore, global tables, Kinesis streaming, resource policies, and capacity management. Call one and you get an `UnknownOperationException`.

A few behavioural differences are also worth knowing when you test against it:

- `ConsistentRead` is accepted but changes nothing. SQLite is strongly consistent, so every read already is - you can't reproduce eventually-consistent reads.
- Streams expose a single shard. `DescribeStream` returns one shard, and its `ExclusiveStartShardId` and `Limit` paging parameters are accepted but ignored.
- Transaction-contention errors (`TransactionConflictException`, `TransactionInProgressException`) aren't emulated - there's no concurrent contention in a single process.

For the per-feature support matrix, see the [live capability matrix](https://paritysuite.org/capabilities); the full operation-by-operation breakdown is in the [compatibility summary](https://github.com/nubo-db/dynoxide/blob/main/docs/compatibility-summary.md).

## Acknowledgements

Dynoxide's DynamoDB API semantics and validation logic were informed by [dynalite](https://github.com/architect/dynalite), the excellent DynamoDB emulator built on LevelDB by Michael Hart and now maintained by the Architect team.

Dynoxide is a clean-room Rust implementation. No code was ported directly, but [dynalite](https://github.com/architect/dynalite)'s thorough approach to matching live DynamoDB behaviour, including edge cases and error messages, was an invaluable reference.

Dynoxide uses SQLite as its storage layer. (AWS's [DynamoDB Local](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html) also uses SQLite internally.)

## License

Dual-licensed under MIT and Apache 2.0. See [LICENSE-MIT](LICENSE-MIT) and [LICENSE-APACHE](LICENSE-APACHE).

## Trademarks

Amazon DynamoDB, DynamoDB, and AWS are trademarks of Amazon.com, Inc. or its affiliates. Dynoxide is an independent project and is not affiliated with, endorsed by, or sponsored by Amazon, and nothing here grants any right to use those names or marks.
