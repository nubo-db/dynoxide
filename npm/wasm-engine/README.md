# @dynoxide/wasm-engine

The [dynoxide](https://github.com/nubo-db/dynoxide) DynamoDB-compatible engine, compiled to WebAssembly and packaged to run in the browser. It carries the engine `.wasm`, the official [@sqlite.org/sqlite-wasm](https://github.com/sqlite/sqlite-wasm) SQLite build, the bundled Web Worker, and an `EngineClient` that drives them, so you can run real DynamoDB operations client-side against a SQLite-backed store persisted to OPFS.

It's a preview. The wasm build is not run against the conformance suite that backs dynoxide's native build, so treat its behaviour as illustrative rather than authoritative.

## Install

```bash
npm install @dynoxide/wasm-engine
```

This is a preview build: the version carries `-preview` and the wasm path is not run against the conformance suite. `npm install @dynoxide/wasm-engine` gets the current preview; pin the exact version (`0.11.0-preview`) to lock to one.

## Quick start

```js
import { EngineClient } from "@dynoxide/wasm-engine";

const client = new EngineClient();
await client.ready();

await client.execute("CreateTable", {
  TableName: "Music",
  KeySchema: [
    { AttributeName: "artist", KeyType: "HASH" },
    { AttributeName: "song", KeyType: "RANGE" },
  ],
  AttributeDefinitions: [
    { AttributeName: "artist", AttributeType: "S" },
    { AttributeName: "song", AttributeType: "S" },
  ],
  BillingMode: "PAY_PER_REQUEST",
});

await client.execute("PutItem", {
  TableName: "Music",
  Item: { artist: { S: "Pixies" }, song: { S: "Debaser" } },
});

const { Items } = await client.execute("Query", {
  TableName: "Music",
  KeyConditionExpression: "artist = :a",
  ExpressionAttributeValues: { ":a": { S: "Pixies" } },
});
```

`execute` takes the operation name and a plain DynamoDB-JSON request, resolves with the response, and rejects with a typed `EngineError`. The client runs the engine in a Web Worker (OPFS's synchronous file handles are Worker-only) and owns the message round-trip, so you deal in objects rather than `postMessage` envelopes.

## API

- `new EngineClient(opts?)` - boots the engine eagerly. Calls queue behind boot, so you can issue work before `ready()` resolves.
- `await client.ready()` - resolves with the boot descriptor (`{ contractVersion, capabilities, persistenceMode }`).
- `client.execute(op, request)` - run one operation.
- `client.supports(op)` - whether the engine implements an operation, for capability-gating your UI.
- `client.persistent` - `true` when the session persists across reloads (OPFS), `false` in the in-memory fallback.
- `client.terminate()` - tear the Worker down and reject any in-flight calls.

The package also exports `EngineError` (the engine's `__type` is on `.type`) and `CONTRACT_VERSION`. TypeScript declarations ship with it, so the client and its boot descriptor are typed out of the box.

## Where the assets live

The Worker and the two `.wasm` travel inside the package. `new EngineClient()` with no arguments resolves the Worker next to this module, and the Worker resolves its `.wasm` next to itself, so a bundler that copies the package's files - or a plain static deploy of them - needs no configuration.

Serving the assets from a CDN or a different origin? Two options:

- `assetBase` - the directory the assets sit in, e.g. `new EngineClient({ assetBase: "https://cdn.example.com/dynoxide/" })`.
- `workerUrl` - the exact Worker URL, if it doesn't sit beside its `.wasm` under a shared base.

If you'd rather construct the Worker yourself - to let a bundler handle it, say - the package exposes it at the `./worker` subpath. Build it and hand it back through `createWorker`:

```js
new EngineClient({
  createWorker: () =>
    new Worker(new URL("@dynoxide/wasm-engine/worker", import.meta.url), { type: "module" }),
});
```

Two clients on one page are fine: each gets its own storage pool, keyed on `name` (default `dynoxide.db`), so give them distinct names if both should persist.

## Hosting

The engine needs a [secure context](https://developer.mozilla.org/en-US/docs/Web/Security/Secure_Contexts) (HTTPS, or `localhost` for development) for OPFS, but **no COOP/COEP cross-origin isolation**. A Content-Security-Policy must allow `'wasm-unsafe-eval'`, or the engine won't instantiate. Serve the `.wasm` as `application/wasm`, and serve the assets gzip- or brotli-encoded - the wasm and the Worker both more than halve over the wire.

## Versioning

`CONTRACT_VERSION` stamps the message-envelope shape, not the engine version. Adding an operation leaves it alone; changing a request, response, or error envelope bumps it. The client validates it against the engine on boot and fails loudly on a mismatch, so a pinned consumer fails with a clear error rather than mis-reading a newer engine. The shipped engine and contract versions sit in `manifest.json`. `manifest.engineVersion` is the dynoxide crate version (e.g. `0.11.0`); the npm package version layers a `-preview` suffix on top (e.g. `0.11.0-preview`), since the package is a preview distribution of that crate version.

## Persistence

State persists across reloads via OPFS. Where OPFS synchronous access handles are unavailable (Firefox private windows, older Safari), the client falls back to an in-memory session and reports it through `persistent` / `persistenceMode` rather than failing.

## Licence

MIT OR Apache-2.0. See `LICENSE-MIT` and `LICENSE-APACHE`.
