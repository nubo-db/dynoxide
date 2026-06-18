# WebAssembly (preview)

Dynoxide compiles to `wasm32-unknown-unknown` and runs in the browser. The same engine that backs the native build runs against [wa-sqlite](https://github.com/rhashimoto/wa-sqlite) - a WASM build of SQLite - over a wasm-bindgen bridge, with the database persisted to [OPFS](https://developer.mozilla.org/en-US/docs/Web/API/File_System_API/Origin_private_file_system) (the origin private file system).

Both backends issue the same SQL. The native and wasm code share one set of query builders, so a query fixed on one is fixed on both.

It's a preview. The wasm build is **not** run against the conformance suite that backs the native build, so its correctness rests on its own tests for now. A build made with `--features wasm-sqlite` exposes `dynoxide::WASM_PREVIEW` (`true`) so you can tell which path you're on.

**What works:** create and delete tables, describe and list them, put, get, delete, and update items, query, scan, and the batch and transactional reads (`BatchGetItem`, `BatchWriteItem`, `TransactGetItems`), over base tables and both secondary index types (GSI and LSI). Index maintenance is atomic with the base write, same as native.

**What doesn't, yet:** TTL returns a typed `Unsupported` error (it needs a background sweep the browser doesn't drive). Streams are planned but not wired - the delivery mechanism is still to be decided. `TransactWriteItems`, tags, table-setting updates, table stats, and bulk import return a preview "not yet implemented" error.

The engine runs in a Web Worker (OPFS's synchronous file handles are Worker-only), and the page talks to it over a message channel. It needs no special server headers (no COOP/COEP cross-origin isolation), so it works on ordinary static hosting.

## Building and shipping it

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

## The embed contract

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

## The engine package

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

