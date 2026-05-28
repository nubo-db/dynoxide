/**
 * dynoxide Web Worker: owns the wasm engine and the wa-sqlite + OPFS database.
 *
 * The wasm-bindgen build runs here, not on the main thread, because wa-sqlite's
 * OPFS VFS needs sync access handles that browsers expose only inside a Worker.
 * The page talks to this Worker over a coarse message RPC: one request per
 * operation, `{ id, op, payload }` in, `{ id, ok, result | error }` out.
 *
 * This preview wires the `smoke` op (a create/put/get round-trip via the
 * harness build). Per-operation ops (create_table, put_item, ...) are the
 * follow-on JS API layer.
 */

import init, {
  smoke_test,
  index_scan_test,
  error_fidelity_test,
} from "../pkg/dynoxide.js";

let ready = null;
function ensureInit() {
  if (!ready) ready = init();
  return ready;
}

self.onmessage = async (event) => {
  const { id, op } = event.data ?? {};
  try {
    await ensureInit();
    let result;
    switch (op) {
      case "smoke":
        result = await smoke_test();
        break;
      case "index":
        result = await index_scan_test();
        break;
      case "errors":
        result = await error_fidelity_test();
        break;
      default:
        throw new Error(`unknown op: ${op}`);
    }
    self.postMessage({ id, ok: true, result });
  } catch (err) {
    self.postMessage({ id, ok: false, error: String(err) });
  }
};
