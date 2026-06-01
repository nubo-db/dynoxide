/**
 * Engine client: the public entry point of the dynoxide engine package.
 *
 * Spawns the dynoxide wasm Worker, talks to it over a promise-per-`id` RPC, and
 * exposes a small async transport: `execute` one operation, plus the boot-time
 * facts a consumer needs (the supported op set, and whether this session
 * persists). Higher-level conveniences (seeding, snapshots, share/import) layer
 * on top of this client; they are the consumer's, not the transport's.
 *
 * The Worker protocol, from this client's side (the repo README documents it
 * from the Worker's side, so the directions read mirrored):
 *   posts:    { id, op, payload, contractVersion }
 *   receives: { id, ok: true, result } | { id, ok: false, error }
 * where `result` and `error` are JSON strings. This client owns the round trip
 * so callers deal in objects, never JSON.
 *
 * The RPC envelope and op-dispatch shape are kept deliberately close to a
 * DocumentClient-style transport, so a future TypeScript SDK can reuse the
 * boundary rather than reinventing it. This is not that SDK.
 */

/**
 * Engine contract version this client is built against. Validated against the
 * engine's own version on boot; a mismatch is a loud failure, not a silent
 * mis-parse. Adding an op does not bump it; an envelope-shape change does.
 */
export const CONTRACT_VERSION = 1;

/**
 * A parsed engine error. The engine rejects with the same `__type`/`message`
 * envelope the native server speaks (plus the engine's own
 * `UnsupportedOperation`/`EngineNotOpened` sentinels); this surfaces the type
 * so callers branch on it instead of string-matching.
 */
export class EngineError extends Error {
  constructor(rawEnvelope) {
    let type = "EngineError";
    let message = typeof rawEnvelope === "string" ? rawEnvelope : String(rawEnvelope);
    try {
      const parsed = JSON.parse(rawEnvelope);
      type = parsed.__type ?? type;
      message = parsed.message ?? parsed.Message ?? message;
    } catch {
      // Not JSON (e.g. a worker-level Error string); keep the raw text.
    }
    super(message);
    this.name = "EngineError";
    this.type = type;
    this.envelope = rawEnvelope;
  }
}

let messageCounter = 0;
function nextId() {
  messageCounter += 1;
  return `m${messageCounter}`;
}

/**
 * Resolve the URL of the bundled engine Worker. An explicit `workerUrl` wins;
 * otherwise `assetBase` (a directory, for serving the assets from a CDN or a
 * different origin) locates the worker beside the other engine assets; with
 * neither, the worker resolves next to this module, which is how the package
 * ships - engine-client.js, the worker and the two .wasm travel together, so a
 * consumer that just imports the package needs no configuration.
 */
function resolveWorkerUrl({ workerUrl, assetBase }) {
  if (workerUrl) return workerUrl;
  if (assetBase) {
    // Force a trailing slash so the worker resolves *under* the base rather
    // than replacing its last path segment, which is how `new URL` treats a
    // base without one ("https://cdn/x" + "w.js" -> "https://cdn/w.js").
    const base = String(assetBase).replace(/\/?$/, "/");
    return new URL("dynoxide-worker.js", base);
  }
  return new URL("./dynoxide-worker.js", import.meta.url);
}

export class EngineClient {
  #worker;
  #onMessage;
  #onError;
  #onMessageError;
  #pending = new Map();
  #ready;
  // Non-null once the worker is unusable - crashed, failed to deserialise a
  // reply, or terminated. Carries the reason so calls reject with what went
  // wrong instead of hanging on a dead worker.
  #deadError = null;

  /**
   * @param {object} opts
   * @param {string|URL} [opts.workerUrl] Explicit URL of the bundled dynoxide
   *   Worker. Overrides `assetBase`.
   * @param {string|URL} [opts.assetBase] Directory the engine assets are served
   *   from, for hosting them on a CDN or a different origin; the worker is
   *   resolved as `dynoxide-worker.js` under this base. Ignored when `workerUrl`
   *   is given.
   * @param {() => object} [opts.createWorker] Factory for a Worker-like object
   *   (postMessage + addEventListener). Overrides workerUrl; used by tests.
   * @param {string} [opts.name] Database name (also the per-instance OPFS pool).
   * @param {boolean} [opts.ephemeral] Force an in-memory, non-persistent session.
   *
   * With none of `workerUrl`, `assetBase` or `createWorker`, the worker
   * resolves next to this module - the layout the published package ships.
   */
  constructor({
    workerUrl,
    assetBase,
    createWorker,
    name = "dynoxide.db",
    ephemeral = false,
  } = {}) {
    this.name = name;
    this.ephemeral = ephemeral;

    // Boot facts, populated by #boot.
    this.contractVersion = null;
    this.capabilities = [];
    this.persistenceMode = "unknown";

    this.#worker = createWorker
      ? createWorker()
      : new Worker(resolveWorkerUrl({ workerUrl, assetBase }), { type: "module" });

    this.#onMessage = (event) => {
      const { id, ok, result, error } = event.data ?? {};
      const entry = this.#pending.get(id);
      if (!entry) return;
      this.#pending.delete(id);
      if (ok) entry.resolve(result);
      else entry.reject(new EngineError(error));
    };
    this.#worker.addEventListener("message", this.#onMessage);

    // A hard Worker crash (wasm trap/OOM, a failed .wasm fetch, a parse error)
    // fires 'error'; a reply that cannot be structured-cloned back fires
    // 'messageerror'. Either way the worker is dead - without handling both,
    // every in-flight call (including boot) would hang forever, and a crash
    // after boot would leave the next call posting into a corpse. Latch the
    // reason so in-flight calls reject and later calls fail fast.
    this.#onError = (event) => {
      this.#die(new EngineError(`engine worker error: ${event.message ?? "crashed"}`));
    };
    this.#worker.addEventListener("error", this.#onError);

    this.#onMessageError = () => {
      this.#die(new EngineError("engine worker messageerror: a reply could not be deserialised"));
    };
    this.#worker.addEventListener("messageerror", this.#onMessageError);

    // Boot eagerly. Operation calls await this, so a request issued before the
    // engine finishes initialising queues rather than races.
    this.#ready = this.#boot();
    // The eager boot promise has no awaiter yet; swallow here so a boot failure
    // does not surface as an unhandledrejection before a caller attaches via
    // ready()/execute(). This is the same promise callers await, so they still
    // see the rejection.
    this.#ready.catch(() => {});
  }

  #rejectAll(error) {
    for (const { reject } of this.#pending.values()) reject(error);
    this.#pending.clear();
  }

  // Latch the worker as dead (first reason wins) and reject every in-flight call.
  #die(error) {
    this.#deadError ??= error;
    this.#rejectAll(this.#deadError);
  }

  #post(op, payload) {
    if (this.#deadError) {
      return Promise.reject(this.#deadError);
    }
    return new Promise((resolve, reject) => {
      const id = nextId();
      this.#pending.set(id, { resolve, reject });
      this.#worker.postMessage({ id, op, payload, contractVersion: CONTRACT_VERSION });
    });
  }

  async #boot() {
    const raw = await this.#post("open", {
      name: this.name,
      ephemeral: this.ephemeral,
    });
    const descriptor = JSON.parse(raw);
    if (descriptor.contractVersion !== CONTRACT_VERSION) {
      // Reject as an EngineError (like every other failure path) so a consumer
      // branching on `instanceof EngineError` catches the mismatch too.
      throw new EngineError(
        JSON.stringify({
          __type: "com.dynoxide.wasm#ContractMismatch",
          message:
            `dynoxide engine contract mismatch: client expects ${CONTRACT_VERSION}, ` +
            `engine reports ${descriptor.contractVersion}. Rebuild the embed against the matching engine.`,
        }),
      );
    }
    this.contractVersion = descriptor.contractVersion;
    this.capabilities = descriptor.capabilities ?? [];
    this.persistenceMode = descriptor.persistenceMode ?? "unknown";
    return descriptor;
  }

  /** Resolves with the boot descriptor once the engine is ready. */
  ready() {
    return this.#ready;
  }

  /** Whether this session persists across reload (false in the memory fallback). */
  get persistent() {
    return this.persistenceMode === "opfs";
  }

  /** Whether the engine supports an operation, for capability-gating the UI. */
  supports(op) {
    return this.capabilities.includes(op);
  }

  /**
   * Run one DynamoDB operation. Resolves with the parsed response, rejects with
   * an {@link EngineError}. Queues behind boot if called before the engine is
   * ready.
   */
  async execute(op, request) {
    await this.#ready;
    const raw = await this.#post("execute", { op, request });
    return JSON.parse(raw);
  }

  /** Tear down the Worker and reject any in-flight calls. */
  terminate() {
    this.#die(new EngineError("engine has been terminated"));
    this.#worker.removeEventListener?.("message", this.#onMessage);
    this.#worker.removeEventListener?.("error", this.#onError);
    this.#worker.removeEventListener?.("messageerror", this.#onMessageError);
    this.#worker.terminate?.();
  }
}
