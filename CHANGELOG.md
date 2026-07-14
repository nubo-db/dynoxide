# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `dynoxide serve` and `dynoxide` (no-subcommand) now accept a `--schema` flag, taking the same DynamoDB DescribeTable JSON format as `import --schema`. On startup, dynoxide creates each table defined in the file and skips any that already exist. This lets you pre-populate an empty database — in-memory or persistent — with the correct table structure without running an import first.

## [0.11.3] - 2026-07-05

### Security

- On Windows, the HTTP and MCP listeners now bind with `SO_EXCLUSIVEADDRUSE`, closing a hole where another process running as the same user could take over either port with `SO_REUSEADDR` while dynoxide was serving. Restarting immediately after a clean shutdown still works; a regression test covers the rebind, and CI now runs the unit tests on Windows ([#23](https://github.com/nubo-db/dynoxide/issues/23)).

## [0.11.2] - 2026-07-02

### Fixed

- A `CreateTable` request whose `StreamSpecification` sets `StreamEnabled: false` but also supplies a `StreamViewType` is now rejected with the `ValidationException` real DynamoDB returns (`One or more parameter values were invalid: Table is being created with a stream disabled, UpdateViewType should not be specified`), where dynoxide accepted it. A view type only has meaning when the stream is enabled, so the two cannot be combined at table creation ([#115](https://github.com/nubo-db/dynoxide/issues/115)).
- A `CreateTable` global or local secondary index using `ProjectionType: INCLUDE` without a `NonKeyAttributes` list is now rejected with the `ValidationException` real DynamoDB returns (`One or more parameter values were invalid: ProjectionType is INCLUDE, but NonKeyAttributes is not specified`), where dynoxide accepted it and created the table. `INCLUDE` projects the index key attributes plus an explicit list, so the list is mandatory; the shared projection validator now requires it, closing the gap for both index types ([#116](https://github.com/nubo-db/dynoxide/issues/116)).
- `Query` and `Scan` now return DynamoDB's exact message when `Select: SPECIFIC_ATTRIBUTES` is given with no `ProjectionExpression` or `AttributesToGet`, where dynoxide rejected the request correctly but with its own wording. Both carried the same non-AWS string; the corrected phrase is `Must specify the AttributesToGet or ProjectionExpression when choosing to get SPECIFIC_ATTRIBUTES`, which `Query` wraps in the `1 validation error detected:` envelope and `Scan` returns bare, matching real DynamoDB ([#121](https://github.com/nubo-db/dynoxide/issues/121)).
- `TransactWriteItems` now reports top-level `ReadCapacityUnits` and `WriteCapacityUnits` in its `ConsumedCapacity`, where only the nested `Table` breakdown carried them. A transactional write reports write capacity (a standalone `ConditionCheck` costs 2 write units on its own table line under `INDEXES`), and a same-token idempotent replay now reports a recomputed transactional read cost, rounded at 4KB read granularity, rather than re-reporting the first call's write units relabelled as read. The two magnitudes diverge above 1KB (writes round at 1KB, reads at 4KB); for a ~1.5KB item the first call reports 4 write units and the replay 2 read units. The replay honours its own `ReturnConsumedCapacity` mode. Single-item operations are unchanged. Confirmed against real DynamoDB by the conformance suite.
- A `TransactWriteItems` call with a `ClientRequestToken` now holds the idempotency lock across the whole first call, closing a window where two concurrent same-token calls could both execute the transaction. The lock was previously released between the cache check and execution, so racing same-token calls each ran the transaction; the second now waits and replays the first's result. Transactions without a token are unaffected.
- PartiQL `ExecuteTransaction` now honours `ClientRequestToken` idempotency, where it ignored the token and re-applied the statements on every call. A same-token, same-statements call within the 600-second window replays the stored result without re-executing (a same-token call with different statements returns `IdempotentParameterMismatchException`), using the same hold-the-lock-across-execute guard as `TransactWriteItems` so concurrent same-token calls serialise rather than double-apply. The cache is separate from the `TransactWriteItems` one, since idempotency is scoped per API operation. `ExecuteTransaction` also now reports transactional `ConsumedCapacity` split by statement kind (write capacity for a write set, read capacity for an all-`SELECT` read set, and read on a replay) at 2 units per statement, replacing a flat 1-unit-per-statement estimate with no read/write split. Confirmed against real DynamoDB by the conformance suite.
- `GetItem`, `Query`, `Scan`, `BatchGetItem`, and `TransactGetItems` now reject an invalid `ProjectionExpression` before any item is read, where dynoxide validated it lazily per row. Overlapping paths (`a` and `a.b`), duplicate paths (`a` and `a`), and undefined expression-attribute names are rejected with DynamoDB's `Invalid ProjectionExpression:` messages, so a lookup that matches nothing still rejects rather than returning an empty result. Confirmed against real DynamoDB in eu-west-2.
- A `ProjectionExpression` selecting several indices of one list now returns them compacted and in ascending index order, where dynoxide returned them in request order: `#l[2], #l[0]` on `[l0, l1, l2]` now yields `[l0, l2]`. Confirmed against real DynamoDB in eu-west-2.
- A `ProjectionExpression` naming two or more sub-attributes of the same list index now returns them merged into a single list element, where dynoxide split each path into its own element: `l[0].a, l[0].b` on `{ l: [ { a, b } ] }` returned `[ { a }, { b } ]` and now returns `[ { a, b } ]`. The merge holds at depth (nested maps and nested lists under one index), distinct indices still stay separate and compact to ascending order, and the fix reaches every projecting read through the shared reconstruction path (`GetItem`, `Query`, `Scan`, `BatchGetItem`, `TransactGetItems`). Confirmed against real DynamoDB in eu-west-2 ([#126](https://github.com/nubo-db/dynoxide/issues/126)).
- `Query` now accepts a `KeyConditionExpression` sort-key comparison with the value on the left (`:lo <= #sk`), treating it as the attribute-on-left form (`#sk >= :lo`) for each of `<`, `<=`, `>`, `>=`, where dynoxide rejected it. A nested or indexed path on a key attribute is now rejected with DynamoDB's message (`Invalid KeyConditionExpression: KeyConditionExpressions cannot have conditions on nested attributes`), replacing dynoxide's own wording. Confirmed against real DynamoDB in eu-west-2.
- `BatchGetItem` now rejects a request that uses an expression `ProjectionExpression` on one table's block and a non-expression `AttributesToGet` on another, where dynoxide accepted it. Real DynamoDB rejects the whole request even when each block is internally consistent. Confirmed against real DynamoDB in eu-west-2.
- `PutItem` and `UpdateItem` validation ordering now matches real DynamoDB: an empty or invalid `TableName` is reported on its own, before the `Return*` enum checks, where dynoxide aggregated them into one envelope. `UpdateItem` additionally stops at the first invalid enum (reporting `ReturnValues`), where `PutItem` continues to aggregate every invalid enum, matching each operation's own behaviour. Confirmed against real DynamoDB in eu-west-2.
- `UpdateTable` now merges the request's `AttributeDefinitions` into the table's existing set, where each call replaced the stored list with only the attributes it carried. DynamoDB treats these as a delta: adding a global secondary index only requires the new index's key attributes, so the table keys and prior indexes' attributes need not be re-declared. Adding two GSIs with delta-only attributes therefore dropped the table keys and the first index's attributes from `DescribeTable`, and a later `PutItem` failed index-key validation with `Index key attribute GSI1PK missing from AttributeDefinitions`. The definitions are now unioned by attribute name, preserving those declared earlier; a redeclared attribute keeps its existing type, matching real DynamoDB, which ignores a conflicting type in the delta rather than overwriting or rejecting it. `UpdateTable` now also keeps `AttributeDefinitions` equal to exactly the attributes used by the table key schema and the current index key schemas: deleting a GSI prunes its now-orphaned key attributes, and an entry supplied in the delta that is used by no key schema is dropped rather than stored (neither is an error). All verified against AWS in eu-west-2 ([#129](https://github.com/nubo-db/dynoxide/issues/129)).

## [0.11.1] - 2026-06-26

### Fixed

- A `ConditionExpression` comparing a Map (`M`) or List (`L`) attribute for equality now works, where `=` always reported not-equal and `<>` always equal regardless of the values. `compare_values` had no arm for document types, so every map or list comparison fell through to the not-equal default; it now compares them deeply - maps order-independently, lists element-wise in order - with nested numbers normalised as elsewhere. The same path backs `IN`, `BETWEEN`, and `contains` over document operands, so those are fixed too ([#103](https://github.com/nubo-db/dynoxide/issues/103)).
- `ExpressionAttributeValues` nested beyond DynamoDB's 32-level document limit are now rejected up front with the same `ValidationException` AWS returns, where before they were accepted and evaluated. The check runs on every path that takes expression values - PutItem, UpdateItem, DeleteItem, Query, Scan, and TransactWriteItems. The stored-item nesting check was also one level too lenient (it accepted a value AWS rejects) and carried a non-AWS message; both now match DynamoDB's limit and wording, confirmed against real AWS in eu-west-2 ([#110](https://github.com/nubo-db/dynoxide/issues/110)).
- Number-set equality in a condition or filter expression now compares at full precision, where it parsed each member to `f64` and so reported two sets differing only beyond ~15 significant digits as equal. It now uses the canonical numeric form, matching DynamoDB and the way number-set duplicates are already detected on write; the fix also covers number sets nested inside a map or list ([#111](https://github.com/nubo-db/dynoxide/issues/111)).
- A `Number` with a leading `+` on the mantissa (`+5`, `+1.5`, `+1e2`) is now accepted and stored normalised (`+5` reads back as `5`), matching real DynamoDB, where dynoxide rejected it with a `ValidationException`. The validator was reworked to accept exactly DynamoDB's numeric grammar, which also closes two pre-existing gaps in the same direction: malformed forms such as `1+2`, `1.2.3`, `+e2`, and a digitless exponent are now rejected, as is any surrounding or internal whitespace (`" 5"` was previously trimmed and accepted). The accept and reject boundary was verified against real DynamoDB ([#109](https://github.com/nubo-db/dynoxide/issues/109)).

## [0.11.0] - 2026-06-24

### Added

- `UpdateTable` on the wasm preview engine: add or delete a global secondary index, with existing rows backfilled into a newly added index, and change the simple table settings (provisioned throughput, billing mode, table class, on-demand throughput, deletion protection). A stream-specification change through `UpdateTable` stays unsupported, since streams remain a preview gap, and a newly added GSI is reported immediately `ACTIVE` rather than transitioning through `CREATING`.
- The wasm engine gained an operation-level `execute` API, and a new npm package, `@dynoxide/wasm-engine`, that ships it. The Worker answers a small versioned RPC - `open`, `execute`, `capabilities`, `contractVersion` - with `{id, op, payload}` in and `{id, ok, result|error}` out, and a bundled `EngineClient` owns the round trip so you deal in objects instead of hand-building postMessage envelopes. `npm run build:wasm` assembles the package: the Worker, the two `.wasm`, the `EngineClient`, and a `manifest.json` stamped with the engine and contract versions. Depend on that built package, not this repo's source. The client checks its `CONTRACT_VERSION` against the engine on boot and fails loudly if they differ, so a stale embed can't quietly mis-read a newer one. The package ships TypeScript types for the client. Still a preview: the wasm path isn't run against the conformance suite.

### Changed

- On the wasm backend, the per-write and per-delete secondary-index fan-out now crosses the JS bridge once per index type rather than once per index operation. Keeping a table's GSIs and LSIs in step with a write is a delete and a re-insert per index, each previously its own bridge crossing; a new `exec_script` primitive carries the whole ordered batch over in a single crossing, so an indexed `PutItem` or `DeleteItem` on a table with K GSIs and L LSIs drops from order K+L crossings to a constant two. Index contents and native behaviour are unchanged ([#85](https://github.com/nubo-db/dynoxide/issues/85)).
- The browser backend moved from `wa-sqlite` to the official [`@sqlite.org/sqlite-wasm`](https://github.com/sqlite/sqlite-wasm) engine, maintained by the SQLite team and versioned to track SQLite releases. The bridge now runs through the `sqlite3.oo1` API over the OPFS SAHPool VFS, which keeps the no-COOP/COEP guarantee that motivated the original VFS choice (it needs no `SharedArrayBuffer`). The `open`/`exec`/`query`/`close` contract is unchanged, so consumers of `@dynoxide/wasm-engine` need no code change. A busy database now recovers once the holder releases it rather than staying busy until reload, and the full 64-bit integer round-trip and the `fnv1a_hash` scalar are re-proven on the new engine ([#61](https://github.com/nubo-db/dynoxide/issues/61)). The shipped SQLite `.wasm` is larger than before (~845 KB against wa-sqlite's ~545 KB).

### Fixed

- An empty-binary key value now surfaces as a top-level `ValidationException` on every path, matching DynamoDB. Previously the lookup path (`GetItem`/`DeleteItem`/`UpdateItem`, batch, and a transact `Update`/`Delete`/`ConditionCheck` `Key`) returned the older `...were invalid:...` wording and, inside a transaction, a `ValidationError` cancellation reason rather than hoisting; the same cancellation-instead-of-hoist gap also affected an empty-binary table item key and an empty-binary secondary-index key in a transaction. This is the binary counterpart to the empty-string key fix [#98](https://github.com/nubo-db/dynoxide/issues/98); real DynamoDB returns the same top-level `...are not valid. ... empty binary value...` messages (table keys, and the put and update forms for secondary-index keys), confirmed identical across four regions.
- Inside a `TransactWriteItems`, an empty-string value in the lookup `Key` of an `Update`, `Delete`, or `ConditionCheck` was wrapped in a `TransactionCanceledException`; it now surfaces as a top-level `ValidationException`, matching DynamoDB and completing the empty-string key fix [#95](https://github.com/nubo-db/dynoxide/issues/95) made for the `Put` item key. Wrong-type and non-scalar lookup keys still cancel with a `ValidationError` reason, and the corrected empty-string message now also matches DynamoDB on the single-action `GetItem`/`DeleteItem`/`UpdateItem` and batch lookup paths ([#98](https://github.com/nubo-db/dynoxide/issues/98)).
- `BatchWriteItem` now reports a wrong-type or non-scalar table key in a put request with DynamoDB's generic `The provided key element does not match the schema`, rather than borrowing `PutItem`'s `Type mismatch for key ...` wording. Real DynamoDB collapses both cases to the schema error inside a batch. The empty-string table-key message and the secondary-index key messages already matched and are unchanged, and `PutItem` and the other put-shaped paths keep the specific type-mismatch message ([#97](https://github.com/nubo-db/dynoxide/issues/97)).
- `Query` and `Scan` now reject two `Select`/`ProjectionExpression` combinations that real DynamoDB rejects before reading any item, where dynoxide previously returned results: a `ProjectionExpression` with any `Select` other than `SPECIFIC_ATTRIBUTES` (such as `ALL_ATTRIBUTES`), and `Select: ALL_PROJECTED_ATTRIBUTES` without an `IndexName`. Both now return a `ValidationException` with DynamoDB's message ([#96](https://github.com/nubo-db/dynoxide/issues/96)).
- Inside a `TransactWriteItems`, a key (table or secondary index) carrying an empty string was wrapped in a `TransactionCanceledException`; it now surfaces as a top-level `ValidationException`, matching DynamoDB. Wrong-type and non-scalar key values still cancel with a `ValidationError` reason, so only the empty-string case changes. A non-scalar table key no longer fails as an internal error before the transaction runs, and an update that sets a secondary-index key to an empty string now returns DynamoDB's distinct update-path message rather than the put-shaped one ([#95](https://github.com/nubo-db/dynoxide/issues/95)).
- A write whose secondary-index (GSI or LSI) key attribute is the wrong type, a non-scalar, or an empty string is now rejected with a `ValidationException` matching DynamoDB's exact message, where before it was silently accepted (kept out of the index but still written to the base table). Validation runs on every write path - put, update, batch, transactional, PartiQL, and import. An update only re-checks an index key it actually changes, so an unrelated update to a row holding a pre-existing bad value still succeeds ([#92](https://github.com/nubo-db/dynoxide/issues/92)).
- A `Scan` or `Query` on a composite global secondary index no longer returns items that are missing the index sort key; they are now excluded from the index (sparse-index behaviour), matching DynamoDB. Index membership was gated on the partition key alone, so an item carrying the partition key but no sort key was written into the index at an empty sort-key position. Membership is now a single shared rule across both global and local secondary indexes, applied on every write path - put, update, batch, transactional, PartiQL, import, and GSI backfill - and it also excludes an item whose index key attribute is present but not a scalar. In-memory databases start fresh each run and are unaffected; only a file-backed database, or a snapshot taken from one, written by an older build carries stray index rows. They clear as each affected item is next written, and a persisted store can rebuild an index by dropping and re-adding it ([#91](https://github.com/nubo-db/dynoxide/issues/91)).
- `PutItem` and the other write paths now accept a `{"NULL": false}` attribute value and read it back as `{"NULL": true}`, where before they rejected it with `One or more parameter values were invalid: Null attribute value types must have the value of true`. The NULL member is typed as a plain boolean in the model, so `false` was valid input all along; AWS has dropped the server-side true-only rule and normalises `false` to `true` on read, and dynoxide now matches. A non-boolean NULL such as `{"NULL": "no"}` is still rejected as a type error ([#62](https://github.com/nubo-db/dynoxide/issues/62)).
- Hardened the wasm engine preview ahead of a stable `@dynoxide/wasm-engine` publish. A body-less operation such as `ListTables` now round-trips instead of failing as a `SerializationException` ([#65](https://github.com/nubo-db/dynoxide/issues/65)). OPFS open tells a busy database (another tab holding its lock) apart from one that is genuinely unavailable: the busy case surfaces a stable `com.dynoxide.wasm#OpfsUnavailable` error rather than silently forking to a separate in-memory store, while a private window or quota error still degrades to an ephemeral session. Re-opening opens the new database before closing the old, so a failed re-open leaves the working session intact, and closing a database releases its OPFS handles so the name is free for another tab ([#64](https://github.com/nubo-db/dynoxide/issues/64)). The bridge round-trips full 64-bit integers, and a cross-backend test pins the `fnv1a_hash` scalar the wasm and native backends share ([#61](https://github.com/nubo-db/dynoxide/issues/61)). A headless-browser CI job exercises the shipped bundle against the real wasm engine and OPFS on every PR ([#68](https://github.com/nubo-db/dynoxide/issues/68)).

## [0.10.0] - 2026-05-29

### Added

- A `StorageBackend` trait in the new `dynoxide::storage_backend` module, decoupling the data layer from a specific SQLite binding. The native rusqlite-backed `Storage` implements the trait, and the action handlers and `Database` now consume it (see Changed). The trait surface also carries a `clock()` accessor for the stream and TTL paths and batch-shaped `put_base_items` / `insert_gsi_items` methods that replaced the last two raw `Storage::conn()` escape hatches in the handlers.
- A `BackendError` enum returned by the trait surface, with an explicit `rusqlite::Error -> BackendError` mapping for the common failure modes (`NotADatabase`, locked / busy, constraint violations, I/O failures), plus an `Unsupported { capability }` variant for a capability a backend cannot serve (the wasm preview uses it for TTL). It is `#[non_exhaustive]` so future backends can add failure modes without a breaking change.
- A `Clock` capability on `Storage` so the trait surface does not assume `std::time`. Stream and TTL paths route their `created_at` and sweep timestamps through the clock; `SystemClock` is the default and `ManualClock` ships as a deterministic test helper. Other `std::time` call sites (idempotency cache, action-handler timestamps, snapshots) remain native-only and are unchanged.
- A `wasm-sqlite` cargo feature and a working WebAssembly backend. dynoxide compiles to `wasm32-unknown-unknown` and runs in the browser against [wa-sqlite](https://github.com/rhashimoto/wa-sqlite) (a WASM build of SQLite) over a wasm-bindgen bridge, persisting to OPFS. `WasmBridgeBackend` implements `StorageBackend`, and `WasmDatabase` (`Database<WasmBridgeBackend>`) exposes the handlers as `async fn` with no `block_on`. It covers create-table, put, get, delete, query, and scan over base tables and both index types (GSI and LSI), with index fan-out atomic with the base write. TTL returns `BackendError::Unsupported`; streams return a preview "not yet implemented" error pending a delivery design; `TransactWriteItems`, tags, table-setting updates, stats, and bulk import are preview placeholders. The native and wasm backends share one set of SQL builders (`storage_backend::sql_builders`), so both issue identical SQL.
- A self-contained browser build: `npm run build:wasm` (wasm-pack + esbuild) emits a `dist/` of three files - a bundled Web Worker plus the two `.wasm` assets (dynoxide ~550 KB, wa-sqlite ~545 KB; ~1.2 MB total). The engine runs in a Web Worker because wa-sqlite's OPFS persistence uses synchronous access handles, which browsers expose only in a Worker; pairing wa-sqlite's synchronous VFS (`AccessHandlePoolVFS`) with its non-async build needs no `SharedArrayBuffer`, and so **no cross-origin isolation (COOP/COEP)** - it drops onto ordinary static hosting. A build-visible `WASM_PREVIEW` constant (`true` under `wasm-sqlite`) marks the preview. The harness under `harness/` loads the same bundled Worker that ships, so a green harness means the shipping artefact works; it exercises CRUD, GSI query/scan, and error-envelope fidelity on OPFS. CI builds the `wasm32-unknown-unknown` target for both the `wasm-sqlite` and `wasm-harness` features on every PR, so the harness's use of `WasmDatabase` and the action types is type-checked too.
- Official Docker image. `docker run -p 8000:8000 ghcr.io/nubo-db/dynoxide` is a ~5 MB drop-in for `amazon/dynamodb-local` in containerised test suites: multi-arch (`linux/amd64` and `linux/arm64`), `FROM scratch`, published to GHCR on each release with Docker Hub and ECR Public mirrors pushed best-effort. The image ships a `HEALTHCHECK` backed by a new `dynoxide healthcheck` subcommand, so `docker ps` and Compose health gates report status without extra tooling ([#3](https://github.com/nubo-db/dynoxide/issues/3)).
- `SECURITY.md`, documenting the MCP HTTP transport's threat model: the bearer-token authentication it now requires, plus the Host and Origin allowlists that back it ([#27](https://github.com/nubo-db/dynoxide/issues/27)).
- MCP HTTP transport options: `--mcp-host`/`--host` to bind beyond loopback, `--mcp-allowed-host`/`--allowed-host` to accept additional `Host` headers by name, and `--mcp-no-auth`/`--no-auth` to disable authentication on loopback binds only. With a token set, these make the transport reachable from outside a container, unblocking the Docker MCP path ([#24](https://github.com/nubo-db/dynoxide/issues/24)).

### Changed

- `Database` is now generic over its storage backend: `Database<S>`, monomorphised, no `dyn`. The parameter defaults to the native rusqlite backend, so existing code that names `Database` is unaffected, and a new `NativeDatabase` alias names that default explicitly. The action handlers are now `async` and route through the `StorageBackend` trait. `NativeDatabase` keeps the historical synchronous public API: each method drives the handler future to completion with `block_on` (via `pollster`), and because the native backend's futures never suspend, that `block_on` never parks the thread, so it stays safe inside the tokio-based HTTP and MCP servers.
- `DynoxideError` is now `#[non_exhaustive]`. Match arms in downstream code must include a wildcard. Done now, while 0.10.0 is already a breaking release, so later variant additions stay non-breaking.
- **Breaking:** the MCP HTTP transport (`dynoxide mcp --http`, `dynoxide serve --mcp`) now requires bearer-token authentication on every request. On a loopback bind, dynoxide generates a token on first run, persists it to a per-user config file, and prints a client-config snippet; later runs reuse it silently. **Existing clients break until updated**: add `"headers": { "Authorization": "Bearer <token>" }` to your MCP client config. A non-loopback bind requires an explicit token via `--mcp-token`/`--token` or `DYNOXIDE_MCP_AUTH_TOKEN` and will not start without one. The stdio transport is unaffected ([#27](https://github.com/nubo-db/dynoxide/issues/27)).
- **Breaking (library API):** `dynoxide::mcp::serve_http` and `serve_http_with_shutdown` now take an `HttpOptions` struct (bind host, `AuthMode`, extra allowed hosts) in place of a bare `port: u16`. Embedders constructing the MCP HTTP server must build `HttpOptions` and choose an `AuthMode`.
- `rusqlite` is now an optional dependency behind the `native-sqlite` feature (on by default, so native builds are unchanged). The crate type-checks with rusqlite absent, which is the precondition for the wasm build. Cross-platform wall-clock paths (the idempotency cache, `created_at` stamps, and `SystemClock`) moved to `web-time` - `std::time` on native, the browser clock on wasm. The native binary now builds behind a `cli` marker feature (pulled in by `http-server`, `mcp-server`, and `import`), so it is skipped in backend-neutral builds such as `--features wasm-sqlite`. The `DynoxideError::SqliteError` variant is consequently `native-sqlite`-gated and absent on backend-neutral builds, which matters only for code that matches it by name on a wasm target.

### Fixed

- PartiQL `DELETE` and `UPDATE` now evaluate the non-key predicates in a `WHERE` clause instead of acting on the key alone. Before, the executor pulled the primary key out of the `WHERE` and ignored the rest, so `DELETE FROM "t" WHERE pk = 'a' AND NOT begins_with(name, 'x')` deleted the row even when `name` began with `x`, mutating a row the filter should have excluded (a data-correctness bug predating v0.9.5). The write paths now run the full condition against the fetched item, the same `matches_where` pass `SELECT` already uses: a present item whose non-key predicate is false raises `ConditionalCheckFailedException`, matching how AWS treats a PartiQL write whose condition fails, and a missing item stays a silent no-op ([#54](https://github.com/nubo-db/dynoxide/issues/54)).
- `DescribeTable` now returns a stable `TableId` instead of a freshly generated UUID on every call. The id is a random UUID assigned once at create time and persisted (a new `table_id` column, added to existing databases through the versioned schema migration and backfilled), so it stays the same across calls, `CreateTable` returns the same value, and a dropped-and-recreated table gets a new one, matching AWS ([#55](https://github.com/nubo-db/dynoxide/issues/55)).
- `UpdateItem` evaluates an `UpdateExpression` against the pre-update item image and accepts parenthesised arithmetic. `SET a = :v, b = a` now gives `b` the old value of `a` rather than the value assigned earlier in the same call, and `SET c = (c - :v)` parses and applies on the `BigDecimal` path instead of being rejected with `Expected operand in SET, got (` ([#35](https://github.com/nubo-db/dynoxide/issues/35)).
- `UpdateItem` `ReturnValues: UPDATED_NEW` matches AWS granularity. A nested `SET parent.child = :v` returns only the changed fragment `{parent: {M: {child}}}` instead of the whole `parent` map, and a REMOVE-only update omits `Attributes` entirely rather than returning an empty map ([#36](https://github.com/nubo-db/dynoxide/issues/36)).
- Paginating a `Query` over a GSI no longer drops items when several entries share the same index key and the base table has only a partition key. On a hash-only base table the continuation cursor lost its base-key component and stalled after the first page, the same defect [#38](https://github.com/nubo-db/dynoxide/issues/38) fixed for `Scan`; the `Query` path now carries the base partition key, so every tied item is returned across the paged walk ([#52](https://github.com/nubo-db/dynoxide/issues/52)).
- `TransactWriteItems`, `TransactGetItems` and PartiQL `ExecuteStatement` now report `ConsumedCapacity` the way AWS does. A transactional write charges 2 WCU per item and a transactional read 2 RCU per item including a missing one (each item rounded up before the 2x factor); the `TransactGetItems` `INDEXES` breakdown carries `Table.ReadCapacityUnits`; and `ExecuteStatement` returns the `ConsumedCapacity` block whenever `ReturnConsumedCapacity` is requested instead of omitting it ([#37](https://github.com/nubo-db/dynoxide/issues/37)).
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
- Building dynoxide for `wasm32-unknown-unknown` is now supported via the `wasm-sqlite` feature (see Added). The wasm backend is a preview: it is not run against the conformance suite that covers the native build, so its correctness rests on its own CRUD/query/scan/GSI/LSI tests for now. The engine runs in a Web Worker (OPFS's synchronous file handles are Worker-only) and needs no cross-origin isolation, so it works on ordinary static hosting.

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

- **DynamoDB conformance suite** - 526 independently written tests across 3 tiers, validated against real DynamoDB ground truth. Dynoxide: 100%. DynamoDB Local: 92%. See [dynamodb-conformance](https://github.com/paritysuite/dynamodb-conformance).
- **Dynalite external conformance** - 817/1039 passing (87.1% DynamoDB parity) against Dynalite's test suite, where real DynamoDB itself only passes 51%
- **DynamoDB compatibility documentation** - a public compatibility summary covering operation, expression, index, and PartiQL support, with a DynamoDB Local comparison column
- **Correctness fixes** - 41 issues resolved across core operations and PartiQL
- **Reserved word validation** - 573 DynamoDB reserved keywords rejected in ConditionExpression, UpdateExpression, FilterExpression, and ProjectionExpression with correct error messages
- **README benchmark automation** - CI benchmark numbers auto-updated via template markers and Python script; PR-based review with sanity checking
- **IdempotentParameterMismatchException** - TransactWriteItems detects same token with different payload
- **AccessDeniedException** - returned for tag operations on non-existent ARNs (matches DynamoDB behaviour)

### Changed

- **BigDecimal replaces f64** for all number comparisons and arithmetic - eliminates silent precision loss beyond 15 significant digits; f64 fast-path for ≤15 significant digits preserves performance
- **PartiQL INSERT** now fails with `DuplicateItemException` if item already exists (previously silently overwrote)
- **PartiQL tokeniser** - correct handling of negative numbers, escaped single quotes, unknown characters (error instead of silent skip)
- **Query/Scan COUNT** now returns filtered count, not scanned count, when `FilterExpression` is present
- **begins_with sort key** - SQL LIKE wildcards (`%`, `_`) properly escaped
- **Condition + write operations** wrapped in SQLite transactions to prevent TOCTOU races
- **1MB response limit** now counts all scanned items, not just filtered results
- **GSI query/scan LastEvaluatedKey** now includes base table key attributes
- **BatchWriteItem** rejects duplicate keys within the same request
- **TransactWriteItems** - 4MB size check uses accurate item size calculation; CancellationReasons returned as structured top-level JSON field; `ReturnValuesOnConditionCheckFailure` returns ALL_OLD item on condition failure
- **UpdateItem** rejects empty update expressions; protects key attributes from REMOVE/ADD/DELETE
- **ReturnValues** validated against allowed values per operation
- **UnprocessedKeys** in BatchGetItem preserves per-table settings
- **SET on list index beyond bounds** extends the list with NULL padding (previously returned error)
- **SET on empty list** at index 0 now succeeds
- **Projection with list index** correctly reconstructs list structure (previously created Map where List was needed)
- **Select validation** - invalid Select values and SPECIFIC_ATTRIBUTES without ProjectionExpression rejected
- **ConsistentRead on GSI** rejected with correct error message
- **Limit of 0** rejected with constraint error
- **Query/Scan validation ordering** matches DynamoDB (input validation before table existence check)
- **Expression attribute usage** validated syntactically (at parse time) not semantically (at runtime) - fixes false positives with `if_not_exists` short-circuiting
- **SerializationException** pre-checks for non-list field types with DynamoDB-compatible error format
- **Error type prefix** - `ValidationException` uses `com.amazon.coral.validate#` prefix matching real DynamoDB
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

- **Local Secondary Indexes (LSI)** - full lifecycle: creation, query/scan routing, projection types (ALL, KEYS_ONLY, INCLUDE), sparse index behaviour, write path maintenance across all operations including TTL expiry
- **ExecuteTransaction** - PartiQL transactional execution with all-or-nothing semantics, condition checks, per-statement cancellation reasons, ConsumedCapacity support
- **Parallel Scan** - SQLite-level segment filtering via registered FNV-1a scalar function; validated segment/total parameters
- **CreateTable extensions** - `SSESpecification`, `TableClass` (validated), `Tags` (inline), `DeletionProtectionEnabled` with enforcement on DeleteTable and toggle via UpdateTable
- **PartiQL WHERE clause extensions** - `BETWEEN`, `IN`, `CONTAINS`, `IS MISSING`, `IS NOT MISSING`, `OR`, `NOT`, parenthesised grouping
- **PartiQL nested path projections** - `SELECT address.city, tags[0] FROM ...` with correct nested structure preservation
- **PartiQL REMOVE clause** - `UPDATE ... REMOVE attribute`
- **PartiQL SET expressions** - arithmetic (`count + 1`), `list_append`, `if_not_exists` in SET clauses
- **PartiQL IF NOT EXISTS** - `INSERT ... VALUE {...} IF NOT EXISTS`
- **PartiQL set literals** - `<< 'a', 'b', 'c' >>` syntax for SS/NS/BS
- **PartiQL COUNT(*)** and **LIMIT** support
- **Item validation** - empty string/set rejection, number precision validation (38 significant digits, ±9.99E+125 range), set deduplication (NS by numeric equivalence)
- **Unused expression attribute rejection** - unreferenced `ExpressionAttributeNames`/`ExpressionAttributeValues` entries return `ValidationException`
- **ReturnItemCollectionMetrics** - partition collection size across base table and all LSI tables
- **Per-GSI ConsumedCapacity** - `INDEXES` mode returns per-GSI breakdown in `GlobalSecondaryIndexes` map

### Changed

- **TrackedExpressionAttributes** - unified expression resolution with usage tracking; removed duplicate untracked code paths (~400 LOC reduction)
- **ScanParams / QueryParams structs** replace parameter sprawl in storage layer
- **CreateTableMetadata** consolidates previously triple-duplicated row mapping
- **GSI/LSI secondary indexes** on `(base_pk, base_sk)` / `(table_pk, table_sk)` columns - eliminates full table scans during index maintenance
- **Schema v5 migration** with automatic secondary index creation on existing tables

## [0.9.3] - 2026-03-12

### Added

- **MCP Server** - 33 tools exposing DynamoDB operations for coding agents (Claude Code, Cursor, etc.)
  - stdio and Streamable HTTP transports
  - `--read-only`, `--max-items`, `--max-size-bytes` safety flags
  - `bulk_put_items` tool for batch loading
  - OneTable `--data-model` integration with entity-aware agent context and `--data-model-summary-limit`
  - `--mcp` flag on `dynoxide serve` to run MCP alongside HTTP server
  - Snapshots: `create_snapshot`, `restore_snapshot`, `list_snapshots`, `delete_snapshot` with auto-snapshot before `delete_table`
  - `get_database_info` tool with data model context
- **Import CLI** - `dynoxide import` for DynamoDB Export data (JSON Lines format)
  - Anonymisation rules: fake, mask, hash, redact, null actions
  - Cross-table consistency for specified fields
  - zstd compression (`--compress`)
  - `--continue-on-error`, `--tables` filtering, atomic `--force` overwrite
  - Stream-aware import (reproduces source table's StreamSpecification)
- **CLI restructuring** - `dynoxide serve`, `dynoxide mcp`, `dynoxide import` subcommands
- Database introspection and port conflict detection on startup
- `RUST_LOG` debug tracing throughout HTTP and MCP servers

## [0.9.2] - 2026-03-04

### Added

- **SQLCipher encryption** - `encryption` feature (vendored OpenSSL via SQLCipher) and `encryption-cc` feature (Apple CommonCrypto backend) for encryption at rest
- Secure key handling via `--encryption-key-file` or `DYNOXIDE_ENCRYPTION_KEY` environment variable
- **UpdateTable** - `StreamSpecification` support, GSI create/delete with backfill
- **Tag operations** - `TagResource`, `UntagResource`, `ListTagsOfResource`
- **ReturnValuesOnConditionCheckFailure** for TransactWriteItems
- **GitHub Action** - `nubo-db/dynoxide@v1` with optional `snapshot-url` preloading
- **Homebrew formula** - `brew install nubo-db/tap/dynoxide`
- Release CI workflow with cross-platform binary builds (Linux x86_64/aarch64/musl, macOS Intel/Apple Silicon, Windows)
- Private-to-public repo publishing pipeline
- DynamoDBStreams target prefix - server accepts `DynamoDB_20120810.ListStreams` and Streams-prefixed actions
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

[Unreleased]: https://github.com/nubo-db/dynoxide/compare/v0.11.3...HEAD
[0.11.3]: https://github.com/nubo-db/dynoxide/compare/v0.11.2...v0.11.3
[0.11.2]: https://github.com/nubo-db/dynoxide/compare/v0.11.1...v0.11.2
[0.11.1]: https://github.com/nubo-db/dynoxide/compare/v0.11.0...v0.11.1
[0.11.0]: https://github.com/nubo-db/dynoxide/compare/v0.10.0...v0.11.0
[0.10.0]: https://github.com/nubo-db/dynoxide/compare/v0.9.13...v0.10.0
[0.9.13]: https://github.com/nubo-db/dynoxide/compare/v0.9.12...v0.9.13
[0.9.12]: https://github.com/nubo-db/dynoxide/compare/v0.9.11...v0.9.12
[0.9.11]: https://github.com/nubo-db/dynoxide/compare/v0.9.10...v0.9.11
[0.9.10]: https://github.com/nubo-db/dynoxide/compare/v0.9.9...v0.9.10
[0.9.9]: https://github.com/nubo-db/dynoxide/compare/v0.9.8...v0.9.9
[0.9.8]: https://github.com/nubo-db/dynoxide/compare/v0.9.7...v0.9.8
[0.9.7]: https://github.com/nubo-db/dynoxide/compare/v0.9.6...v0.9.7
[0.9.6]: https://github.com/nubo-db/dynoxide/compare/v0.9.5...v0.9.6
[0.9.5]: https://github.com/nubo-db/dynoxide/releases/tag/v0.9.5