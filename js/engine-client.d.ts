/**
 * Type declarations for the dynoxide engine client. Hand-written to match
 * engine-client.js; keep them in step when the client's surface changes.
 */

/** Engine contract version this client is built against (see CONTRACT_VERSION). */
export const CONTRACT_VERSION: number;

/** Whether a session persists across reload. */
export type PersistenceMode = "opfs" | "memory" | "unknown";

/** The descriptor `open` resolves with, validated by the client on boot. */
export interface BootDescriptor {
  contractVersion: number;
  capabilities: string[];
  persistenceMode: PersistenceMode;
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
  contractVersion: number | null;
  capabilities: string[];
  persistenceMode: PersistenceMode;
  /** Resolves with the boot descriptor once the engine is ready. */
  ready(): Promise<BootDescriptor>;
  /** Whether this session persists across reload (false in the memory fallback). */
  get persistent(): boolean;
  /** Whether the engine supports an operation, for capability-gating the UI. */
  supports(op: string): boolean;
  /** Run one DynamoDB operation; resolves with the parsed response. */
  execute(op: string, request?: unknown): Promise<any>;
  /** Tear down the Worker and reject any in-flight calls. */
  terminate(): void;
}
