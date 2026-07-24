# WebAssembly (preview)

Dynoxide compiles to `wasm32-unknown-unknown` and runs in the browser. The same engine that backs the native build runs against the official [@sqlite.org/sqlite-wasm](https://github.com/sqlite/sqlite-wasm) build of SQLite over a wasm-bindgen bridge, with the database persisted to [OPFS](https://developer.mozilla.org/en-US/docs/Web/API/File_System_API/Origin_private_file_system) (the origin private file system).

Both backends issue the same SQL. The native and wasm code share one set of query builders, so a query fixed on one is fixed on both.

It's a preview. The wasm build is **not** run against the conformance suite that backs the native build, so its correctness rests on its own tests for now. A build made with `--features wasm-sqlite` exposes `dynoxide::WASM_PREVIEW` (`true`) so you can tell which path you're on.

**What works:** create and delete tables, describe and list them, update tables (add or delete a GSI, with existing rows backfilled into the new index, and change provisioned throughput, billing mode, table class, on-demand throughput, and deletion protection), put, get, delete, and update items, query, scan, and the batch and transactional reads (`BatchGetItem`, `BatchWriteItem`, `TransactGetItems`), over base tables and both secondary index types (GSI and LSI). Index maintenance is atomic with the base write, same as native.

**What doesn't, yet:** TTL returns a typed `Unsupported` error (it needs a background sweep the browser doesn't drive). Streams are planned but not wired - the delivery mechanism is still to be decided, so an `UpdateTable` that changes a stream specification is refused. `TransactWriteItems`, tags, table stats, and bulk import return a preview "not yet implemented" error.

One fidelity note on what *is* supported: adding a GSI is synchronous. The new index is immediately `ACTIVE` and queryable, where AWS reports it `CREATING` with a background backfill that finishes before it becomes `ACTIVE`. The backfilled data matches; only the lifecycle is simplified.

The engine runs in a Web Worker (OPFS's synchronous file handles are Worker-only), and the page talks to it over a message channel. It needs no special server headers (no COOP/COEP cross-origin isolation), so it works on ordinary static hosting.

Keeping secondary indexes in step with a write means a delete and a re-insert per GSI and per LSI, and on wasm each of those statements is a bridge crossing. The maintenance for one write (or one delete) is issued as a single ordered batch through the `exec_script` bridge primitive - one crossing for the whole GSI fan-out and one for the LSI fan-out - rather than one crossing per index operation. A table with no indexes crosses zero extra times. Native runs the same fan-out in-process, so it has no crossings to save.

## Persistence and durability

The database lives in OPFS, reached through the official OPFS SAHPool VFS - a pool of synchronous access handles. Where a browser can't provide those handles - a Firefox private window, an older Safari - the engine falls back to an in-memory database that works for the session but doesn't survive a reload, and `open` reports which mode you got as `persistenceMode` so you can warn the user.

Each database name gets its own OPFS directory and its own pool of file handles, so opening several databases on one page never makes them contend for a shared pool. Opening the *same* database a second time while another tab or client still holds it doesn't quietly fork to a private in-memory copy - that would split reads from writes and lose everything on reload - it fails with a clear "OPFS is busy" error instead. That state is recoverable, not sticky: once the holder releases the database, a later open of the same name succeeds rather than replaying the earlier failure. Closing a database relinquishes its access handles (without destroying the data) so another tab can pick it up.

The wasm path runs in rollback-journal mode, not WAL. The SAHPool VFS doesn't implement WAL's shared-memory interface, so `PRAGMA journal_mode = WAL` is a no-op there and SQLite keeps a rollback journal. That costs no atomicity: the backend funnels every write through a single connection it serialises, so it never needs the concurrent readers WAL buys, and each commit flushes through the synchronous handle. (The native build enables WAL because it has the concurrency to gain from it.)

Integers round-trip at full 64-bit width - a value past 2^53 crosses the bridge as a BigInt rather than losing precision as a JavaScript double. DynamoDB number attributes travel as text inside the item JSON regardless, so this only touches the engine's own integer columns.

## Building and shipping it

`npm install` then `npm run build:wasm` produces a self-contained `dist/` (use `build:wasm:dev` to skip wasm-opt for speed):

```bash
npm install
npm run build:wasm
```

`dist/` is the two `.wasm` plus the bundled Worker, kept separate so the `.wasm` cache independently of the JS bundle, and a small manifest:

| File | Size | What |
|---|---|---|
| `dynoxide_bg.wasm` | ~1.0 MB | the engine (release, wasm-opt) |
| `sqlite3.wasm` | ~845 KB | SQLite (the official @sqlite.org/sqlite-wasm build) |
| `dynoxide-worker.js` | ~225 KB | the bundled Web Worker (engine glue + bridge, fully minified) |
| `manifest.json` | <1 KB | engine version, contract version, file list |

About 2.1 MB raw, but that's not the number that reaches a browser. The `.wasm` and the Worker JS all compress well, so served with gzip it's around 860 KB over the wire, and brotli takes it lower again - turn one of them on at the host (most CDNs do by default). The `.wasm` files are immutable, so they cache hard after the first load, and the SAHPool VFS is synchronous, so the engine needs neither the larger Asyncify async build nor `SharedArrayBuffer`.

Drop `dist/` on any origin that's a [secure context](https://developer.mozilla.org/en-US/docs/Web/Security/Secure_Contexts) - HTTPS in production, or `localhost` for development. OPFS needs a secure context, but **no COOP/COEP headers and no cross-origin isolation**, so plain static hosting works. (SQLite in the browser usually needs cross-origin isolation, because the common technique makes an async storage API look synchronous via `SharedArrayBuffer`. Dynoxide avoids that by running the official synchronous OPFS SAHPool VFS inside a Worker, where synchronous file handles are available directly.) One header does matter: if you set a Content-Security-Policy it must allow `'wasm-unsafe-eval'`, or the engine won't instantiate. Serve the `.wasm` as `application/wasm` while you're at it.

## The embed contract

Spawn the bundle as a module Worker and drive it over `postMessage`; the two `.wasm` files must sit next to `dynoxide-worker.js`, which is where the build puts them. The Worker speaks one coarse RPC: a message in, a reply out, correlated by an `id` you supply.

```text
in:   { id, op, payload, contractVersion? }
out:  { id, ok: true,  result }      // result is a JSON string
      { id, ok: false, error }       // error is a JSON string
```

Four ops carry the engine:

- `open` - `payload: { name, ephemeral? }` opens (or reopens) the OPFS-backed database and resolves with the contract descriptor, `{ contractVersion, capabilities, persistenceMode }`. `ephemeral: true` forces an in-memory session. Call it once before any operation.
- `execute` - `payload: { op, request }` runs one DynamoDB operation, where `op` is the operation name (`PutItem`, `Query`, `Scan`, ...) and `request` is a plain DynamoDB-JSON object. It resolves with the response JSON and rejects with an error envelope (the same `__type`/`message` shape the native HTTP server speaks). Ask `capabilities` for the supported set rather than guessing; anything outside it comes back as an `UnsupportedOperation` envelope.
- `dispatchHttp` - `payload: { target, body }` resolves one whole DynamoDB HTTP request, where `target` is the raw `X-Amz-Target` header (or `null`) and `body` is the raw request body. It resolves with `{ status, body }` for a transport to write verbatim. Where `execute` takes an already-resolved operation, this hands the engine the request as it arrived, so target resolution, body parsing, and the unimplemented-operation response all happen in the engine. A protocol rejection comes back as a status rather than a rejected promise; only calling it before `open` rejects.
- `capabilities` and `contractVersion` - the supported op list and the engine's contract version, for a client that wants them without opening a database.

`contractVersion` stamps the envelope shape, not the engine version. Adding an op is additive and leaves it alone; changing the request, response, or error envelope bumps it. Stamp your messages with the version you built against and the Worker rejects a mismatch loudly, so a stale embed fails with a clear error instead of mis-parsing a newer engine. The shipped version sits in `manifest.json` and is what `open` echoes back.

The harness under `harness/` is a working example, and it loads the same bundled Worker a production consumer would:

```bash
npm run build:wasm
python3 -m http.server 8081
# then open http://localhost:8081/harness/
```

It opens the engine, creates a table, writes a few rows, then runs a query and a filtered scan against the OPFS-backed database so you can see `ScannedCount` come back higher than `Count`. Because it drives the shipping bundle rather than a parallel build, a green harness means the shipping artefact works. (The older smoke ops live behind `npm run build:wasm:harness`, which adds them on top of the same Worker.)

## Serving it over HTTP, for testing only

**This is test infrastructure, not a way to run dynoxide.** It exists so the
[Parity Suite](https://paritysuite.org) can reach the browser build over a
socket. If you want a DynamoDB endpoint to develop against, use the native
build - it is faster, persists, implements far more, and is what the `dynoxide`
binary, npm package and container image all give you. This server is
deliberately not distributed in any of them.

Knowing that, the engine has no wire protocol of its own, so nothing that speaks
DynamoDB over a socket can reach it directly. `npm run wasm:serve` puts an
endpoint in front: it drives the shipping `dist/` bundle in a headless Chromium
it manages itself and serves DynamoDB JSON-1.0 on a local port, so an AWS SDK
client cannot tell it from the native build.

```bash
npm run build:wasm
npm run wasm:serve                # port 8003, --port to change it
```

It installs its own browser on first run, so the only prerequisite is a built
bundle. A caller needs the endpoint and nothing else.

```bash
aws dynamodb list-tables --endpoint-url http://localhost:8003
```

It carries one browser page's worth of concurrency and no TLS.

Two things about it are load-bearing rather than incidental:

- **The engine owns the wire envelope.** `dispatchHttp` decides what a malformed
  body, a missing target, or an unimplemented operation returns, and the bridge
  writes back whatever it gets. There is one implementation of the protocol, not
  one per transport, so the HTTP surface cannot drift from the native server's.
- **Each start is a clean database.** The session opens `ephemeral`, so a run
  never inherits state. Test suites that create fixtures up front and never
  reset would otherwise collide with the previous run's tables. It also means
  this path does not exercise OPFS; the browser specs under `tests/browser`
  cover persistence, and this covers the DynamoDB surface.

An operation the preview does not implement returns HTTP 501 with an
`UnsupportedOperation` envelope, so a conformance runner can tell "out of scope"
from "implemented and wrong" without guessing. `tests/bridge` drives the whole
path over a socket; run it with `npm run test:bridge`.

## The engine package

Rather than build the engine yourself, you can depend on the same artefacts as an npm package, `@dynoxide/wasm-engine`. `npm run build:wasm` assembles it under `npm/wasm-engine/` - the Worker, the two `.wasm`, the manifest, and an `EngineClient` that owns the RPC above so you deal in objects, not `postMessage` envelopes:

```js
import { EngineClient } from "@dynoxide/wasm-engine";

const client = new EngineClient();        // resolves the Worker beside the package
await client.ready();

await client.execute("CreateTable", { /* ... */ });
const { Items } = await client.execute("Query", { /* ... */ });
```

`new EngineClient()` with no arguments resolves the Worker next to the package, and the Worker resolves the `.wasm` next to itself, so a bundler that copies the package's files - or a plain static deploy of them - needs no configuration. Serving the assets from a CDN or another origin? Pass `assetBase` (the directory they sit in) or `workerUrl` (the exact Worker URL).

The package also exports `EngineError` (the typed rejection, carrying the engine's `__type` on `.type`) and `CONTRACT_VERSION`. The client checks that version against the engine on boot and fails loudly on a mismatch, so a pinned consumer never mis-reads a newer engine. Hosting matches `dist/`: a secure context, no COOP/COEP, a CSP that allows `'wasm-unsafe-eval'`, and `.wasm` served as `application/wasm`. It's a preview, like the rest of the wasm build; `npm install @dynoxide/wasm-engine` gets it (the version carries `-preview`), or pin the exact version.

