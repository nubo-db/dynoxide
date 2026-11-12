// Declared here rather than taken from TypeScript's esnext.disposable lib, so a
// project whose `lib` setting predates it can still import this package.
declare global {
  interface SymbolConstructor {
    readonly asyncDispose: unique symbol;
  }
}

export interface StartOptions {
  /** Path to a dynoxide binary (1.7.0 or later). Overrides DYNOXIDE_BINARY. */
  binary?: string;
  /** A database file that survives a restart. Give each server its own. */
  dbPath?: string;
  /** A schema file whose tables are created at startup. */
  schema?: string;
  /** How long to wait for the server to be ready. Defaults to 10000. */
  readyTimeoutMs?: number;
}

export interface ExitStatus {
  code: number | null;
  signal: string | null;
}

export interface DynoxideServer {
  /** The URL to give an SDK, such as http://127.0.0.1:54321. */
  readonly endpoint: string;
  readonly host: string;
  readonly port: number;
  /** The server's version, from its ready line. */
  readonly version: string;
  /** Where the binary came from: the binary option, DYNOXIDE_BINARY or the platform package. */
  readonly binarySource: "option" | "env" | "package";
  readonly region: "us-east-1";
  readonly credentials: { readonly accessKeyId: string; readonly secretAccessKey: string };
  /** Stops the server. Every call returns the same promise. */
  stop(): Promise<ExitStatus>;
  /** Stops the server at the end of an `await using` block, where the runtime supports it. */
  [Symbol.asyncDispose](): Promise<void>;
}

/** The `code` on an error from start(). */
export type DynoxideErrorCode =
  | "DYNOXIDE_BINARY_MISSING"
  | "DYNOXIDE_UNSUPPORTED_PLATFORM"
  | "DYNOXIDE_PACKAGE_MISSING"
  | "DYNOXIDE_TOO_OLD"
  | "DYNOXIDE_VERSION_CHECK_FAILED"
  | "DYNOXIDE_SPAWN_FAILED"
  | "DYNOXIDE_EXITED"
  | "DYNOXIDE_BAD_READY_LINE"
  | "DYNOXIDE_PORT_ZERO"
  | "DYNOXIDE_TIMEOUT";

export interface DynoxideError extends Error {
  code: DynoxideErrorCode;
  /** The last lines of the server's stderr, for errors after it was spawned. */
  stderr?: string[];
}

/** Starts a Dynoxide server on a free port. Rejects with a DynoxideError. */
export function start(options?: StartOptions): Promise<DynoxideServer>;
