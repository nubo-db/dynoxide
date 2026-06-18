/**
 * dynoxide Web Worker: owns the wasm engine and the wa-sqlite + OPFS database.
 *
 * The wasm-bindgen build runs here, not on the main thread, because wa-sqlite's
 * OPFS VFS needs sync access handles that browsers expose only inside a Worker.
 * The page talks to this Worker over a coarse message RPC: one request per
 * operation, `{ id, op, payload, contractVersion? }` in,
 * `{ id, ok, result | error }` out.
 *
 * ## Protocol
 *
 * | op             | payload                  | result                              |
 * |----------------|--------------------------|-------------------------------------|
 * | `open`         | `{ name, ephemeral? }`   | contract descriptor JSON string     |
 * | `execute`      | `{ op, request }`        | response JSON string (rejects with  |
 * |                |                          | an error-envelope string on failure)|
 * | `capabilities` | (none)                   | JSON array of supported op names    |
 * | `contractVersion` | (none)                | the engine contract version (number)|
 *
 * `ephemeral: true` on `open` forces an in-memory session. Every failure - the
 * engine's own errors and the Worker's (contract mismatch, unknown op) - is an
 * error-envelope string `{ __type, message }`, so the client always parses it
 * the same way. `request` is a plain object; the Worker serialises it for the
 * engine, so a caller never hand-builds JSON. The smoke ops (`smoke`/`index`/
 * `errors`) exist only in `wasm-harness` builds; the shipping `wasm-sqlite`
 * bundle strips them at build time via the `__DYNOXIDE_HARNESS__` esbuild define
 * (see scripts/build-wasm.sh), so they are not dead weight in what consumers
 * download.
 *
 * ## Contract version
 *
 * A caller may stamp each message with the `contractVersion` it was built
 * against. The Worker rejects a mismatch loudly at the boundary, so a stale
 * embed against a newer engine fails with a clear error instead of mis-parsing
 * a changed envelope. Additive ops do not bump the version; envelope-shape
 * changes do.
 */

import init from "../pkg/dynoxide.js";
import * as engine from "../pkg/dynoxide.js";

let ready = null;
function ensureInit() {
  if (!ready) ready = init();
  return ready;
}

// A stable error-envelope string for the Worker's own errors, matching the
// shape the engine returns so the client parses every failure the same way.
function envelope(type, message) {
  return JSON.stringify({ __type: type, message });
}

self.onmessage = async (event) => {
  const { id, op, payload, contractVersion } = event.data ?? {};
  try {
    await ensureInit();

    // A caller's compiled-in contract version must match the engine's, or the
    // envelope shape it expects may have changed. Reject loudly rather than
    // mis-parse.
    const engineVersion = engine.contract_version();
    if (contractVersion != null && contractVersion !== engineVersion) {
      throw envelope(
        "com.dynoxide.wasm#ContractMismatch",
        `dynoxide contract mismatch: client=${contractVersion} engine=${engineVersion}`,
      );
    }

    let result;
    switch (op) {
      case "open":
        // Resolves with `{ contractVersion, capabilities, persistenceMode }`
        // so the client can validate, learn the op set, and warn when a session
        // will not persist - all in one round trip. `ephemeral` forces an
        // in-memory session.
        result = await engine.open(
          payload?.name ?? "dynoxide.db",
          payload?.ephemeral === true,
        );
        break;
      case "execute":
        // engine.execute resolves with the response JSON and rejects with a
        // stable error-envelope string; both pass straight through to the page.
        if (payload?.op == null) {
          throw envelope("com.dynoxide.wasm#InvalidRequest", "execute requires payload.op");
        }
        // A body-less op (ListTables, say) carries no request. Default to {}
        // rather than stringifying undefined, which the engine would reject as a
        // SerializationException for a request that was never malformed.
        result = await engine.execute(payload.op, JSON.stringify(payload.request ?? {}));
        break;
      case "capabilities":
        result = engine.capabilities();
        break;
      case "contractVersion":
        result = engineVersion;
        break;
      default: {
        // The smoke/index/errors harness ops exist only in `--harness` builds.
        // esbuild replaces __DYNOXIDE_HARNESS__ with a literal, so for the
        // shipping build this whole block (and the SMOKE_OPS table) is
        // dead-code-eliminated (#69); build-wasm.sh asserts it is gone.
        if (__DYNOXIDE_HARNESS__) {
          // Bracket access (not `engine.smoke_test`) so the bundler does not
          // flag these as missing exports in a non-harness compile. Adding an op
          // here means updating the strip assertion in scripts/build-wasm.sh.
          const SMOKE_OPS = {
            smoke: () => engine["smoke_test"]?.(),
            index: () => engine["index_scan_test"]?.(),
            errors: () => engine["error_fidelity_test"]?.(),
          };
          if (op in SMOKE_OPS) {
            const run = SMOKE_OPS[op]();
            if (run === undefined) {
              throw envelope("com.dynoxide.wasm#UnsupportedOperation", `op "${op}" needs a wasm-harness build`);
            }
            result = await run;
            break;
          }
        }
        // Reached for any op not handled above: a genuine unknown op, or (in the
        // shipping build, where the block above is stripped) a harness op.
        throw envelope("com.dynoxide.wasm#UnsupportedOperation", `unknown op: ${op}`);
      }
    }
    self.postMessage({ id, ok: true, result });
  } catch (err) {
    // Engine errors already arrive as a JSON envelope string; worker-own errors
    // are thrown as envelope strings too. Wrap anything else so every failure
    // reaches the client as a parseable { __type, message }.
    const error =
      typeof err === "string"
        ? err
        : envelope("com.dynoxide.wasm#WorkerError", err?.message ?? String(err));
    self.postMessage({ id, ok: false, error });
  }
};
