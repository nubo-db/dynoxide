# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- A `StorageBackend` trait in the new `dynoxide::storage_backend` module, decoupling the data layer from a specific SQLite binding. The native rusqlite-backed `Storage` implements the trait, and the action handlers and `Database` now consume it (see Changed). The trait surface also carries a `clock()` accessor for the stream and TTL paths and batch-shaped `put_base_items` / `insert_gsi_items` methods that replaced the last two raw `Storage::conn()` escape hatches in the handlers.
- A `BackendError` enum returned by the trait surface, with an explicit `rusqlite::Error -> BackendError` mapping for the common failure modes (`NotADatabase`, locked / busy, constraint violations, I/O failures). It is `#[non_exhaustive]` so future backends can add failure modes without a breaking change.
- A `Clock` capability on `Storage` so the trait surface does not assume `std::time`. Stream and TTL paths route their `created_at` and sweep timestamps through the clock; `SystemClock` is the default and `ManualClock` ships as a deterministic test helper. Other `std::time` call sites (idempotency cache, action-handler timestamps, snapshots) remain native-only and are unchanged.
- A `wasm-stub` cargo feature that builds a placeholder `WaSqliteBackend` whose method bodies are `unimplemented!()`. The stub exists to catch trait-shape drift at type-check time before a real wa-sqlite backend has to absorb it. CI gains a `wasm-stub-check` job that runs `cargo check --features wasm-stub --lib` on every PR.
- Official Docker image. `docker run -p 8000:8000 ghcr.io/nubo-db/dynoxide` is a ~5 MB drop-in for `amazon/dynamodb-local` in containerised test suites: multi-arch (`linux/amd64` and `linux/arm64`), `FROM scratch`, published to GHCR on each release with Docker Hub and ECR Public mirrors pushed best-effort. The image ships a `HEALTHCHECK` backed by a new `dynoxide healthcheck` subcommand, so `docker ps` and Compose health gates report status without extra tooling ([#3](https://github.com/nubo-db/dynoxide/issues/3)).
- `SECURITY.md`, documenting the MCP HTTP transport's threat model: the bearer-token authentication it now requires, plus the Host and Origin allowlists that back it ([#27](https://github.com/nubo-db/dynoxide/issues/27)).
- MCP HTTP transport options: `--mcp-host`/`--host` to bind beyond loopback, `--mcp-allowed-host`/`--allowed-host` to accept additional `Host` headers by name, and `--mcp-no-auth`/`--no-auth` to disable authentication on loopback binds only. With a token set, these make the transport reachable from outside a container, unblocking the Docker MCP path ([#24](https://github.com/nubo-db/dynoxide/issues/24)).

### Changed

- `Database` is now generic over its storage backend: `Database<S>`, monomorphised, no `dyn`. The parameter defaults to the native rusqlite backend, so existing code that names `Database` is unaffected, and a new `NativeDatabase` alias names that default explicitly. The action handlers are now `async` and route through the `StorageBackend` trait. `NativeDatabase` keeps the historical synchronous public API: each method drives the handler future to completion with `block_on` (via `pollster`), and because the native backend's futures never suspend, that `block_on` never parks the thread, so it stays safe inside the tokio-based HTTP and MCP servers.
- `DynoxideError` is now `#[non_exhaustive]`. Match arms in downstream code must include a wildcard. Done now, while 0.10.0 is already a breaking release, so later variant additions stay non-breaking.
- **Breaking:** the MCP HTTP transport (`dynoxide mcp --http`, `dynoxide serve --mcp`) now requires bearer-token authentication on every request. On a loopback bind, dynoxide generates a token on first run, persists it to a per-user config file, and prints a client-config snippet; later runs reuse it silently. **Existing clients break until updated**: add `"headers": { "Authorization": "Bearer <token>" }` to your MCP client config. A non-loopback bind requires an explicit token via `--mcp-token`/`--token` or `DYNOXIDE_MCP_AUTH_TOKEN` and will not start without one. The stdio transport is unaffected ([#27](https://github.com/nubo-db/dynoxide/issues/27)).
- **Breaking (library API):** `dynoxide::mcp::serve_http` and `serve_http_with_shutdown` now take an `HttpOptions` struct (bind host, `AuthMode`, extra allowed hosts) in place of a bare `port: u16`. Embedders constructing the MCP HTTP server must build `HttpOptions` and choose an `AuthMode`.

### Fixed

- PartiQL `ExecuteStatement` accepts the bracket `IN [...]` list form, not just `IN (...)`, and evaluates `NOT begins_with(...)` as a negated predicate. `IS NOT MISSING` already evaluated; the gaps were the bracket list and the `NOT` function arm, which the same statement bundles together ([#40](https://github.com/nubo-db/dynoxide/issues/40)).
- `DescribeTable` now round-trips `OnDemandThroughput` and reports the full `SSEDescription` shape. A table created with `OnDemandThroughput` reports its `MaxReadRequestUnits` and `MaxWriteRequestUnits` back; the value lives in a new `on_demand_throughput` column added through the versioned schema migration, so existing on-disk databases pick it up on open. Server-side encryption enabled with the AWS-managed key now reports `SSEType: KMS` and a `KMSMasterKeyArn` alongside `Status: ENABLED`, where before it returned the status alone ([#44](https://github.com/nubo-db/dynoxide/issues/44)).
- `UpdateTable` now accepts a lone `TableClass` or `OnDemandThroughput` change instead of rejecting it with `At least one of ProvisionedThroughput, BillingMode, ... is required`. Both fields are validated (an unknown `TableClass` is a `ValidationException`) and persisted, so the change shows up on the next `DescribeTable` ([#45](https://github.com/nubo-db/dynoxide/issues/45)).
- `DeleteTable` on a table with deletion protection enabled now returns the exact AWS message, `Resource cannot be deleted as it is currently protected against deletion. Disable deletion protection first.`, in place of the ARN-prefixed wording dynoxide used before ([#46](https://github.com/nubo-db/dynoxide/issues/46)).
- `TransactGetItems` now omits `Item` from a response entry when a `ProjectionExpression` matches no attribute on an otherwise-present item, matching AWS. The projection always re-injects the table key, so the entry previously came back as a key-only object instead of being omitted ([#39](https://github.com/nubo-db/dynoxide/issues/39)).
- `BatchWriteItem` now rejects a `PutRequest` whose item is missing the table key with a 400 `ValidationException` rather than a 500 `InternalServerError`. The duplicate-key detection pass extracted keys before validating them; it now validates first, the same ordering the single-item write paths already use ([#39](https://github.com/nubo-db/dynoxide/issues/39)).
- Paginating a `Scan` over a GSI no longer drops items when several entries share the same index key and the base table has only a partition key. On a hash-only base table the continuation cursor lost its base-key component and stalled after the first page; it now carries the base partition key, so every tied item is returned across the paged walk ([#38](https://github.com/nubo-db/dynoxide/issues/38)).
- A single-item write (`PutItem`, `DeleteItem`, `UpdateItem`) and its GSI/LSI index fan-out now run in a single transaction. A failure partway through the fan-out rolls the whole write back rather than leaving a base row with a half-applied (torn) index. The same per-item atomicity now also covers `BatchWriteItem` (each write request) and the TTL sweep (each expired-item delete). This matches DynamoDB, where a single-item write does not half-apply to its indexes.
- Write paths now roll back on a failed `COMMIT` and surface a failed `ROLLBACK` rather than leaving the connection stuck mid-transaction, which would make the next write fail. Every write path shares one transaction helper for this.
- A client-facing `ValidationException` raised inside a backend method (the 50-tag limit in `set_tags`) keeps its 400 status across the `StorageBackend` boundary instead of collapsing to a 500.
- Tighter expression and scan validation, to match what real DynamoDB rejects
  (surfaced by the conformance suite). Dynoxide now turns away redundant
  parentheses like `((a = :b))` in condition, filter, and key-condition
  expressions; `contains(x, x)` with the same operand on both sides; and
  `begins_with` handed a number instead of a string or binary. These are
  rejected up front, before any items are scanned
  ([#31](https://github.com/nubo-db/dynoxide/issues/31)).
- `size()` now measures strings in UTF-16 code units rather than bytes, so
  values with emoji or accented characters report the length DynamoDB returns.
- A negative `Segment` on a parallel scan is now rejected rather than accepted.

### Notes

- Existing native code that names `Database` keeps working unchanged: the new generic parameter defaults to the rusqlite backend and the synchronous method signatures are identical. The one deliberate behaviour change is index fan-out atomicity (see Fixed); it is more DynamoDB-correct, and the conformance suite still passes. Tests, conformance, and benchmarks pass against the same observable surface as before.
- Building dynoxide for `wasm32-unknown-unknown` is not yet supported. The trait surface is in place; making the rest of the codebase target-agnostic is the next pass and lands when a working WASM backend does.

## [0.9.13] - 2026-05-11

### Security

- Close a DNS rebinding vulnerability in the MCP HTTP transport
  ([GHSA-89vp-x53w-74fx](https://github.com/modelcontextprotocol/rust-sdk/security/advisories/GHSA-89vp-x53w-74fx) /
  [CVE-2026-42559](https://www.cve.org/CVERecord?id=CVE-2026-42559)) by upgrading `rmcp`
  from 1.1.1 to 1.6.0 in both lockfiles. A malicious page could make the
  user's browser send requests to a loopback MCP server with a non-loopback
  `Host` header, which the server would then process. Affects 0.9.3 to 0.9.12.
  Users running `dynoxide mcp --http` or `dynoxide serve --mcp` should
  upgrade; stdio transport is unaffected.

- Close a related cross-origin CSRF gap: a page could `fetch` the loopback
  endpoint with `mode: 'no-cors'`, and the Host check would pass while the
  Origin header went unchecked. Affected write tools: `put_item`,
  `update_item`, `delete_item`, `create_table`, and `batch_write_item`.
  Fixed by setting an explicit Host and Origin allowlist on
  `StreamableHttpServerConfig`. Native MCP clients (Claude Code, Cursor,
  the dynoxide CLI) don't send an Origin header and are unaffected.

## [0.9.12] - 2026-05-04

### Fixed

- Unix: port releases immediately after `dynoxide serve` shuts down. The listener used to skip `SO_REUSEADDR`, leaving leftover `TIME_WAIT` sockets from connected clients to block restart for ~60s. Live-listener conflict detection is unaffected: `SO_REUSEADDR` only bypasses `TIME_WAIT`, not active sockets.

  Windows: unchanged. `SO_REUSEADDR` lets another process hijack an active bind there, so we leave it off.

## [0.9.11] - 2026-05-04

### Fixed

- `dynoxide serve --mcp` now exits cleanly on Ctrl+C when an MCP client (Claude Code, Cursor) is holding a connection open. The MCP server's graceful-shutdown drain used to wait for those connections forever, hanging the process until something SIGKILLed it ([#22](https://github.com/nubo-db/dynoxide/issues/22))

### Security

- Refresh `Cargo.lock` for the dependabot patches reachable within MSRV: `aws-lc-sys` 0.37.1 to 0.40.0 (5 high-severity AWS-LC issues), `openssl` 0.10.75 to 0.10.79 (5 buffer-overflow advisories), `rand` 0.8.5 to 0.8.6. Remaining `rustls-webpki` / `time` / `aws-sdk-dynamodb` alerts are dev-dependency only (test-suite AWS SDK chain, not the production binary) and stay pinned by MSRV 1.85 until v0.10.0

## [0.9.10] - 2026-04-27

### Fixed

- 16 places where dynoxide's error strings drifted from real AWS DynamoDB. Mostly small things you only notice when you assert the message: `tableName` length validation is now per-operation (1 char on read/write, 3 stays on `CreateTable`), `Select` enum order matches AWS rather than alphabetical, `Query` vs `Scan` `Limit=0` messages are different on purpose now, batch/transact empty and oversize requests use the standard validation envelope, and `UpdateExpression`/`ProjectionExpression` syntax errors include the AWS `near: "..."` window ([#11](https://github.com/nubo-db/dynoxide/issues/11), [#12](https://github.com/nubo-db/dynoxide/issues/12), [#13](https://github.com/nubo-db/dynoxide/issues/13), [#15](https://github.com/nubo-db/dynoxide/issues/15), [#16](https://github.com/nubo-db/dynoxide/issues/16), [#17](https://github.com/nubo-db/dynoxide/issues/17), [#18](https://github.com/nubo-db/dynoxide/issues/18))
- `TransactGetItems` with a bad action key now comes back as a `TransactionCanceledException` with `ValidationError` rather than HTTP 500. The 500 was a real leak: the dedup loop called the server-fault helper before key validation ([#19](https://github.com/nubo-db/dynoxide/issues/19))

## [0.9.9] - 2026-04-24

### Fixed

- `KeyConditionExpression` now accepts parenthesised sub-expressions, matching DynamoDB. Forms like `(#pk = :pk) AND (#sk = :sk)` previously returned `ValidationException: Expected attribute name, got (`. Both outer-wrap and per-condition parens are now handled ([#4](https://github.com/nubo-db/dynoxide/issues/4), [#7](https://github.com/nubo-db/dynoxide/pull/7))
- `UpdateItem` and `TransactWriteItems.Update` now evaluate `ConditionExpression` against the existing item before populating key attributes for upsert. Previously `attribute_exists(pk)` on a non-existent key succeeded and created a ghost item ([#5](https://github.com/nubo-db/dynoxide/pull/5))
- Paginated `Scan` on a GSI now returns all items when multiple items share the same GSI partition key. Previously the second page returned 0 items because the pagination cursor used only `(gsi_pk, gsi_sk)` instead of the full 4-tuple primary key ([#6](https://github.com/nubo-db/dynoxide/pull/6))
- `<>` on missing attributes now returns true, matching DynamoDB. All other comparison operators continue to return false on missing operands. Previously `<>` also returned false, breaking `PutItem` conditional idioms like `status <> "working"` against fresh keys ([#8](https://github.com/nubo-db/dynoxide/pull/8))

## [0.9.8] - 2026-04-06

### Fixed

- Dynoxide no longer orphans when backgrounded in npm scripts (`dynoxide & sleep 1 && npm run seed && react-router dev`) -- the port is released when the parent process exits ([nubo-db/dynoxide#2](https://github.com/nubo-db/dynoxide/issues/2))
- The Rust server now handles SIGTERM for graceful shutdown, not just SIGINT (Ctrl+C) -- `kill <pid>` now works as expected
- The npm wrapper switches from `spawnSync` to async `spawn` with explicit signal forwarding (SIGINT, SIGTERM, SIGHUP) and double-signal SIGKILL escalation
- Parent-death detection via PPID polling catches the backgrounded case where no signal is delivered to the wrapper

## [0.9.7] - 2026-04-02

### Fixed

- Benchmark sanity checks were blocking README updates during release - 10 stale values from the v0.9.6 pipeline now corrected
- Binary download size in README was wrong (~5 MB, actually ~3 MB compressed / ~6 MB on disk)
- Docker image sizes now show both download and on-disk measurements - the old "225 MB" was the compressed download, the actual on-disk size is 471 MB
- MCP tool count in README was 33, should be 34 - `execute_transaction_partiql` was missing from the list
- npm README had incorrect `--input` and `--db-path` flags for the import command (should be `--source`, `--schema`, `--output`)
- Dropped the `serve` subcommand from npm examples (bare `dynoxide --port 8000` is the preferred form)

### Changed

- Restructured release pipeline for token efficiency and reliability - dispatch verification, idempotent crate/npm publishing, template-based Homebrew formula updates
- npm publishing uses OIDC provenance via a dedicated `npm.yml` workflow
- Cross-compilation switched to cargo-zigbuild for aarch64-musl targets
- Commit Cargo.lock for reproducible CI builds (was previously gitignored)
- Updated npm package README to reflect current CLI usage and features

### Security

- Updated `aws-lc-sys` 0.37.1 to 0.39.1 (10 high-severity advisories - PKCS7 verification bypass, timing side-channel in AES-CCM, CRL/name constraint issues)
- Updated `rustls-webpki` 0.103.9 to 0.103.10 (2 medium-severity CRL Distribution Point matching issues)

## [0.9.6] - 2026-03-27

### Fixed

- Statically link the MSVC C runtime on Windows so the release binary no longer requires VCRUNTIME140.dll
- Switch Linux aarch64 target to musl for fully static binaries (matching x86_64)

### Changed

- Drop the separate x86_64-unknown-linux-gnu release target (the musl build is already fully portable)

## [0.9.5] - 2026-03-24

### Added

- **DynamoDB conformance suite** — 526 independently written tests across 3 tiers, validated against real DynamoDB ground truth. Dynoxide: 100%. DynamoDB Local: 92%. See [dynamodb-conformance](https://github.com/nubo-db/dynamodb-conformance).
- **Dynalite external conformance** — 817/1039 passing (87.1% DynamoDB parity) against Dynalite's test suite, where real DynamoDB itself only passes 51%
- **DynamoDB compatibility audit** — code-verified compatibility matrix with file/line references, public-facing summary with DynamoDB Local comparison column, prioritised gap tracking
- **Correctness audit** — 41 issues identified and resolved across core operations and PartiQL
- **Reserved word validation** — 573 DynamoDB reserved keywords rejected in ConditionExpression, UpdateExpression, FilterExpression, and ProjectionExpression with correct error messages
- **README benchmark automation** — CI benchmark numbers auto-updated via template markers and Python script; PR-based review with sanity checking
- **IdempotentParameterMismatchException** — TransactWriteItems detects same token with different payload
- **AccessDeniedException** — returned for tag operations on non-existent ARNs (matches DynamoDB behaviour)

### Changed

- **BigDecimal replaces f64** for all number comparisons and arithmetic — eliminates silent precision loss beyond 15 significant digits; f64 fast-path for ≤15 significant digits preserves performance
- **PartiQL INSERT** now fails with `DuplicateItemException` if item already exists (previously silently overwrote)
- **PartiQL tokeniser** — correct handling of negative numbers, escaped single quotes, unknown characters (error instead of silent skip)
- **Query/Scan COUNT** now returns filtered count, not scanned count, when `FilterExpression` is present
- **begins_with sort key** — SQL LIKE wildcards (`%`, `_`) properly escaped
- **Condition + write operations** wrapped in SQLite transactions to prevent TOCTOU races
- **1MB response limit** now counts all scanned items, not just filtered results
- **GSI query/scan LastEvaluatedKey** now includes base table key attributes
- **BatchWriteItem** rejects duplicate keys within the same request
- **TransactWriteItems** — 4MB size check uses accurate item size calculation; CancellationReasons returned as structured top-level JSON field; `ReturnValuesOnConditionCheckFailure` returns ALL_OLD item on condition failure
- **UpdateItem** rejects empty update expressions; protects key attributes from REMOVE/ADD/DELETE
- **ReturnValues** validated against allowed values per operation
- **UnprocessedKeys** in BatchGetItem preserves per-table settings
- **SET on list index beyond bounds** extends the list with NULL padding (previously returned error)
- **SET on empty list** at index 0 now succeeds
- **Projection with list index** correctly reconstructs list structure (previously created Map where List was needed)
- **Select validation** — invalid Select values and SPECIFIC_ATTRIBUTES without ProjectionExpression rejected
- **ConsistentRead on GSI** rejected with correct error message
- **Limit of 0** rejected with constraint error
- **Query/Scan validation ordering** matches DynamoDB (input validation before table existence check)
- **Expression attribute usage** validated syntactically (at parse time) not semantically (at runtime) — fixes false positives with `if_not_exists` short-circuiting
- **SerializationException** pre-checks for non-list field types with DynamoDB-compatible error format
- **Error type prefix** — `ValidationException` uses `com.amazon.coral.validate#` prefix matching real DynamoDB
- **BatchExecuteStatement** uses short error codes (`ResourceNotFound` not fully qualified type) and rejects empty Statements array
- **UpdateTable GSI delete** returns `ResourceNotFoundException` for non-existent GSI (previously `ValidationException`)
- **StreamSpecification** included in DescribeTable response
- Stack overflow protection: 32-level nesting depth limit on item validation (matches DynamoDB)
- AND/OR short-circuit evaluation in condition expressions

### Fixed

- `size()` function no longer evaluates on invalid attribute types
- Idempotency tokens correctly compared in TransactWriteItems
- PutItem no longer double-reads item for conditional checks
- GSI sort key replacement handles all edge cases
- Nested projection preserves document structure (no longer flattens)
- Double-quote identifier escaping in PartiQL
- PartiQL DELETE with missing sort key returns proper error
- PartiQL nested SET paths create correct nested structure (no longer creates literal dot-notation keys)
- PartiQL SELECT with nested map paths resolves correctly
- TTL expiry cleans up LSI entries (previously left orphans)
- GSI/LSI name collision detected and rejected at CreateTable time
- LSI pagination uses composite cursor to handle duplicate sort key values
- ExecuteTransaction breaks on first failure (previously continued executing then rolled back)
- Partition size calculation for ItemCollectionMetrics sums across base table and all LSI tables
- Error message fidelity improvements across empty string, deletion protection, scan segment, and query validation messages

## [0.9.4] - 2026-03-16

### Added

- **Local Secondary Indexes (LSI)** — full lifecycle: creation, query/scan routing, projection types (ALL, KEYS_ONLY, INCLUDE), sparse index behaviour, write path maintenance across all operations including TTL expiry
- **ExecuteTransaction** — PartiQL transactional execution with all-or-nothing semantics, condition checks, per-statement cancellation reasons, ConsumedCapacity support
- **Parallel Scan** — SQLite-level segment filtering via registered FNV-1a scalar function; validated segment/total parameters
- **CreateTable extensions** — `SSESpecification`, `TableClass` (validated), `Tags` (inline), `DeletionProtectionEnabled` with enforcement on DeleteTable and toggle via UpdateTable
- **PartiQL WHERE clause extensions** — `BETWEEN`, `IN`, `CONTAINS`, `IS MISSING`, `IS NOT MISSING`, `OR`, `NOT`, parenthesised grouping
- **PartiQL nested path projections** — `SELECT address.city, tags[0] FROM ...` with correct nested structure preservation
- **PartiQL REMOVE clause** — `UPDATE ... REMOVE attribute`
- **PartiQL SET expressions** — arithmetic (`count + 1`), `list_append`, `if_not_exists` in SET clauses
- **PartiQL IF NOT EXISTS** — `INSERT ... VALUE {...} IF NOT EXISTS`
- **PartiQL set literals** — `<< 'a', 'b', 'c' >>` syntax for SS/NS/BS
- **PartiQL COUNT(*)** and **LIMIT** support
- **Item validation** — empty string/set rejection, number precision validation (38 significant digits, ±9.99E+125 range), set deduplication (NS by numeric equivalence)
- **Unused expression attribute rejection** — unreferenced `ExpressionAttributeNames`/`ExpressionAttributeValues` entries return `ValidationException`
- **ReturnItemCollectionMetrics** — partition collection size across base table and all LSI tables
- **Per-GSI ConsumedCapacity** — `INDEXES` mode returns per-GSI breakdown in `GlobalSecondaryIndexes` map

### Changed

- **TrackedExpressionAttributes** — unified expression resolution with usage tracking; removed duplicate untracked code paths (~400 LOC reduction)
- **ScanParams / QueryParams structs** replace parameter sprawl in storage layer
- **CreateTableMetadata** consolidates previously triple-duplicated row mapping
- **GSI/LSI secondary indexes** on `(base_pk, base_sk)` / `(table_pk, table_sk)` columns — eliminates full table scans during index maintenance
- **Schema v5 migration** with automatic secondary index creation on existing tables

## [0.9.3] - 2026-03-12

### Added

- **MCP Server** — 33 tools exposing DynamoDB operations for coding agents (Claude Code, Cursor, etc.)
  - stdio and Streamable HTTP transports
  - `--read-only`, `--max-items`, `--max-size-bytes` safety flags
  - `bulk_put_items` tool for batch loading
  - OneTable `--data-model` integration with entity-aware agent context and `--data-model-summary-limit`
  - `--mcp` flag on `dynoxide serve` to run MCP alongside HTTP server
  - Snapshots: `create_snapshot`, `restore_snapshot`, `list_snapshots`, `delete_snapshot` with auto-snapshot before `delete_table`
  - `get_database_info` tool with data model context
- **Import CLI** — `dynoxide import` for DynamoDB Export data (JSON Lines format)
  - Anonymisation rules: fake, mask, hash, redact, null actions
  - Cross-table consistency for specified fields
  - zstd compression (`--compress`)
  - `--continue-on-error`, `--tables` filtering, atomic `--force` overwrite
  - Stream-aware import (reproduces source table's StreamSpecification)
- **CLI restructuring** — `dynoxide serve`, `dynoxide mcp`, `dynoxide import` subcommands
- Database introspection and port conflict detection on startup
- `RUST_LOG` debug tracing throughout HTTP and MCP servers

## [0.9.2] - 2026-03-04

### Added

- **SQLCipher encryption** — `encryption` feature (vendored OpenSSL via SQLCipher) and `encryption-cc` feature (Apple CommonCrypto backend) for encryption at rest
- Secure key handling via `--encryption-key-file` or `DYNOXIDE_ENCRYPTION_KEY` environment variable
- **UpdateTable** — `StreamSpecification` support, GSI create/delete with backfill
- **Tag operations** — `TagResource`, `UntagResource`, `ListTagsOfResource`
- **ReturnValuesOnConditionCheckFailure** for TransactWriteItems
- **GitHub Action** — `nubo-db/dynoxide@v1` with optional `snapshot-url` preloading
- **Homebrew formula** — `brew install nubo-db/tap/dynoxide`
- Release CI workflow with cross-platform binary builds (Linux x86_64/aarch64/musl, macOS Intel/Apple Silicon, Windows)
- Private-to-public repo publishing pipeline
- DynamoDBStreams target prefix — server accepts `DynamoDB_20120810.ListStreams` and Streams-prefixed actions
- `From`/`TryFrom` conversions for request/response types
- `item!` macro for ergonomic item construction in tests
- Table metadata cache for reduced SQLite round-trips
- Stripped release binaries

### Changed

- `nubo-app` → `nubo-db` GitHub organisation rename

## [0.9.1] - 2026-02-16

### Added

- `Server` and `X-Dynoxide-Version` headers on all HTTP responses
- `TableArn`, `LatestStreamArn`, and related ARN fields in API responses
- Comprehensive benchmarking suite comparing Dynoxide against DynamoDB Local and LocalStack
  - Criterion, iai-callgrind, and custom benchmark binaries
  - CI workflows for regression detection and historical tracking
  - Standard 13-step workload with JVM warmup protocol

### Changed

- `http-server` feature is now enabled by default
- Package renamed to `dynoxide-rs` for crates.io publishing

### Fixed

- Rustdoc warnings
- README version reference

## [0.9.0] - 2026-02-15

### Added

- Core DynamoDB emulator backed by SQLite via `rusqlite`
- In-memory and persistent database modes
- Table operations: CreateTable, DeleteTable, DescribeTable, ListTables
- Item operations: PutItem, GetItem, DeleteItem, UpdateItem
- Query and Scan with full expression support and pagination
- Batch operations: BatchGetItem, BatchWriteItem
- Transactions: TransactWriteItems, TransactGetItems
- Global Secondary Indexes (GSI)
- DynamoDB Streams (all four view types)
- TTL with background sweep
- Full expression language: KeyCondition, Filter, Condition, Projection, Update
- PartiQL: ExecuteStatement, BatchExecuteStatement
- ReturnConsumedCapacity (TOTAL and INDEXES modes)
- HTTP server (axum-based, DynamoDB JSON wire protocol)
- 300+ tests