/**
 * Type declarations for the dynoxide engine client. Hand-written to match
 * engine-client.js; keep them in step when the client's surface changes.
 */

/** Engine contract version this client is built against (see CONTRACT_VERSION). */
export const CONTRACT_VERSION: number;

/** Whether a session persists across reload. */
export type PersistenceMode = "opfs" | "memory" | "unknown";

/** The descriptor `ready()` resolves with, validated by the client on boot. */
export interface BootDescriptor {
  contractVersion: number;
  capabilities: string[];
  persistenceMode: PersistenceMode;
}

/** The status and body a transport writes verbatim for one HTTP request. */
export interface HttpOutcome {
  status: number;
  body: string;
}

export interface EngineClientOptions {
  /** Explicit URL of the bundled Worker. Overrides assetBase. */
  workerUrl?: string | URL;
  /** Directory the engine assets are served from, for a CDN or other origin. */
  assetBase?: string | URL;
  /** Factory for a Worker-like object (postMessage + addEventListener); for tests. */
  createWorker?: () => Worker;
  /** Database name (also the per-instance OPFS pool). */
  name?: string;
  /** Force an in-memory, non-persistent session. */
  ephemeral?: boolean;
}

/**
 * A parsed engine error. `type` carries the envelope's `__type` (the same shape
 * the native server speaks), so callers branch on it instead of string-matching.
 */
export class EngineError extends Error {
  constructor(rawEnvelope: string);
  readonly type: string;
  readonly envelope: string;
}

/**
 * Spawns the dynoxide wasm Worker and runs one DynamoDB operation at a time over
 * a promise-per-id RPC.
 */
export class EngineClient {
  constructor(options?: EngineClientOptions);
  readonly name: string;
  readonly ephemeral: boolean;
  /** Boot state, populated once `ready()` resolves; until then `null`. */
  contractVersion: number | null;
  /** Boot state, populated once `ready()` resolves; until then `[]`. */
  capabilities: string[];
  /** Boot state, populated once `ready()` resolves; until then `"unknown"`. */
  persistenceMode: PersistenceMode;
  /** Resolves with the boot descriptor once the engine is ready. */
  ready(): Promise<BootDescriptor>;
  /** Whether this session persists across reload (false in the memory fallback). */
  get persistent(): boolean;
  /** Whether the engine supports an operation, for capability-gating the UI. */
  supports(op: string): boolean;
  /**
   * Run one DynamoDB operation; resolves with the parsed response. The response
   * shape is op-dependent, so it defaults to `unknown`; pass a type argument when
   * you know it, e.g. `execute<ScanOutput>("Scan", request)`.
   */
  execute<T = unknown>(op: string, request?: unknown): Promise<T>;
  /**
   * Resolve one whole DynamoDB HTTP request inside the engine, for a transport
   * fronting it on a real port. The engine owns the wire envelope, so target
   * resolution, body parsing and the unimplemented-operation response all
   * happen there. Rejects only if called before the engine is open.
   */
  dispatchHttp(target: string | null, body: string): Promise<HttpOutcome>;
  /** Tear down the Worker and reject any in-flight calls. */
  terminate(): void;
}
