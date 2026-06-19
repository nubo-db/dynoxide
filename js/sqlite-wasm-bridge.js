/**
 * Bridge between dynoxide's wasm-bindgen backend and the official
 * @sqlite.org/sqlite-wasm engine.
 *
 * Exposes four async functions - `open`, `exec`, `query`, `close` - consumed by
 * `src/storage_backend/wasm_backend.rs` through a `#[wasm_bindgen]` extern
 * block. The Rust side builds every SQL statement (shared with the native
 * backend via `sql_builders`) and hands it here with a positional parameter
 * array; this module only opens the database and runs statements through the
 * `sqlite3.oo1` object API.
 *
 * Runs inside the dynoxide Web Worker (see js/dynoxide-worker.js). Persistence
 * uses the official OPFS SAHPool VFS (`installOpfsSAHPoolVfs`), backed by sync
 * access handles, which browsers expose only in a Worker. The SAHPool VFS is
 * the one official OPFS option that needs no cross-origin isolation: unlike the
 * default OPFS VFS it does not rely on SharedArrayBuffer, so no COOP/COEP
 * headers are required. That property is the whole reason the bridge picks it.
 *
 * Where OPFS sync access handles are unavailable - Firefox private windows,
 * older Safari - or an embedder asks for it, `open` falls back to an in-memory
 * `:memory:` database: the session works but does not survive a reload. The
 * active mode comes back on the open handle as `persistenceMode` so the engine
 * can warn the user.
 *
 * One case is surfaced rather than hidden: a *busy* database (another tab
 * holding its sync access handles) throws an `OpfsUnavailableError`, since
 * silently forking to a separate in-memory store would lose data on reload.
 * Crucially the failure is recoverable, not sticky-until-reload: the installer
 * caches a failed init per VFS name, so every later open passes
 * `forceReinitIfPreviouslyFailed: true` to retry instead of replaying the
 * cached rejection once the other tab releases the lock. Other OPFS failures
 * still fall back to memory.
 *
 * Each database opens against its own named SAHPool VFS over a per-name OPFS
 * directory, so two engine instances (two Workers) never contend on a shared
 * pool. Releasing a database on its last reference uses `pauseVfs()`, which
 * relinquishes the sync access handles so another tab can acquire them while
 * leaving the files intact. It deliberately does NOT use `removeVfs()`, which
 * would delete the directory and destroy the data.
 *
 * Imports use bare specifiers, so this module is bundler-friendly. The official
 * package is its own bundler-friendly entry: it locates `sqlite3.wasm` at
 * runtime via `new URL("sqlite3.wasm", import.meta.url)`, which esbuild
 * preserves so the .wasm resolves as a sibling of the worker bundle rather than
 * being inlined. (Under Node, the package's own entry resolves the .wasm from
 * node_modules, which is how the bridge unit test runs off-browser.) Not
 * exercised by the conformance suite (see the WASM note in the README).
 */

import sqlite3InitModule from "@sqlite.org/sqlite-wasm";
import { fnv1aHash } from "./fnv1a.js";

/** Persistent OPFS-backed session: survives reload. */
const PERSISTENT = "opfs";
/** Ephemeral in-memory session: lost on reload. */
const EPHEMERAL = "memory";

// The single database file inside each per-name SAHPool. The pool's directory
// already isolates one database name from another, so a fixed absolute path is
// unambiguous. The SAHPool VFS requires absolute paths (relative paths are not
// recognised), hence the leading slash.
const MAIN_DB_PATH = "/dynoxide.db";

// Pool capacity is the number of *files* the VFS pre-opens and holds, not the
// number of tables: dynoxide keeps all its tables and indexes inside one
// database file, so a session needs the main file, its rollback journal, and a
// little headroom for SQLite temp files. The upstream default of 6 covers this;
// 8 leaves room for temp files during large scans/sorts. Growth on demand is
// available via `reserveMinimumCapacity` should a workload ever need more.
const INITIAL_CAPACITY = 8;

// Lazily initialised sqlite3 module handle, shared across opens within this
// Worker. We memoise the in-flight promise rather than the resolved value, so
// two concurrent first callers share one initialisation. On failure we clear it
// so a later call can retry rather than caching the error.
let sqlite3Promise = null;

function moduleHandle() {
  if (!sqlite3Promise) {
    // No locateFile override: the official package resolves sqlite3.wasm itself
    // (next to the worker bundle in the browser via import.meta.url, from
    // node_modules under Node). Overriding it would point at this source file's
    // directory, where no .wasm sits.
    sqlite3Promise = sqlite3InitModule().catch((err) => {
      sqlite3Promise = null;
      throw err;
    });
  }
  return sqlite3Promise;
}

// Per-name pool registry for this Worker, keyed by database name. Each value is
// the in-flight promise resolving to { poolUtil, refs }; `refs` counts live
// connections on the pool. A failed install is dropped so a later open can
// retry; a resolved pool stays cached (possibly paused) so a re-open in this
// Worker re-acquires it rather than installing a second time.
const pools = new Map();

/**
 * OPFS is present but its pool could not be acquired. Distinct from the
 * unavailable case (no sync access handles at all), which falls back to memory:
 * this is a real failure - usually another tab or Worker holding the lock - and
 * is surfaced rather than silently swapped for an in-memory store.
 */
export class OpfsUnavailableError extends Error {
  constructor(message, options) {
    super(message);
    this.name = "OpfsUnavailableError";
    if (options && "cause" in options) this.cause = options.cause;
  }
}

// A sync access handle already held elsewhere surfaces as one of these. Used to
// phrase a "busy" message apart from a generic OPFS open failure. The SAHPool
// installer re-throws the raw DOMException from createSyncAccessHandle(), so the
// name check is on the original error.
function isBusyLock(err) {
  const name = err && err.name;
  return name === "NoModificationAllowedError" || name === "InvalidStateError";
}

// FNV-1a (32-bit) over the raw name so two names that sanitise alike (e.g. "a.b"
// and "a_b") still get distinct pools. Deliberately NOT fnv1aHash from fnv1a.js:
// this hashes UTF-16 code units and returns base-36 for a filesystem-safe slug.
// Do not consolidate them - the output feeds every OPFS pool path, so changing
// the hash would relocate persisted databases and lose their data.
function nameHash(name) {
  let hash = 0x811c9dc5;
  for (let i = 0; i < name.length; i += 1) {
    hash ^= name.charCodeAt(i);
    hash = Math.imul(hash, 0x01000193) >>> 0;
  }
  return hash.toString(36);
}

// A filesystem-safe, collision-free slug for a database name, used to derive a
// per-instance OPFS directory and VFS name so two engine instances never share
// a pool. The hash suffix keeps distinct raw names distinct even when their
// sanitised characters collide.
function slug(name) {
  const raw = name || "default";
  return `${raw.replace(/[^a-zA-Z0-9_-]/g, "_")}-${nameHash(raw)}`;
}

// The SQLite VFS registry name for a database. Must be URI-safe and unique per
// database so distinct instances register distinct VFSes; the slug guarantees
// both.
function vfsName(name) {
  return `dynoxide-${slug(name)}`;
}

// The per-name OPFS directory the SAHPool stores its files under. Absolute and
// multi-level (created automatically); one directory per database name so no
// two pools collide.
function poolDir(name) {
  return `/dynoxide/${slug(name)}`;
}

// Whether this context can back the synchronous OPFS VFS. The definitive test
// is installing the pool (done in getPool); this is the cheap pre-check that
// lets an obviously-unsupported context (e.g. a Node test run) skip straight to
// the in-memory fallback.
function opfsSyncAvailable() {
  try {
    return (
      typeof navigator !== "undefined" &&
      !!navigator.storage &&
      typeof navigator.storage.getDirectory === "function" &&
      typeof FileSystemFileHandle !== "undefined" &&
      "createSyncAccessHandle" in FileSystemFileHandle.prototype
    );
  } catch {
    return false;
  }
}

// Register fnv1a_hash for GSI/LSI parallel-scan segment filtering, matching the
// native scalar function: FNV-1a (32-bit) over the value's UTF-8 bytes. The
// value is returned as a BigInt so it is stored as an INTEGER, exactly like the
// native i64 scalar, keeping `fnv1a_hash(col) % total` integer modulo on both
// backends. fnv1aHash matches the native scalar (src/storage.rs); see fnv1a.js.
function registerFnv1a(db) {
  db.createFunction("fnv1a_hash", {
    xFunc: (_ctxPtr, value) => BigInt(fnv1aHash(value)),
    arity: 1,
    deterministic: true,
  });
}

// Open an ephemeral in-memory database. Used for the explicit ephemeral path
// and as the degraded fallback when OPFS is advertised but unusable.
function openMemory(sqlite3) {
  const db = new sqlite3.oo1.DB(":memory:");
  registerFnv1a(db);
  return { db, persistenceMode: EPHEMERAL, name: null };
}

// Get (installing once per name) the SAHPool for `name`, returning the entry
// carrying its poolUtil and live-connection refcount. Relies on the installer's
// per-name memoisation so a same-name re-open returns the cached pool; passes
// `forceReinitIfPreviouslyFailed` so a previously busy name retries rather than
// replaying its cached rejection. If a prior close paused the pool (releasing
// its handles for another tab), unpause it to re-acquire the handles before
// use. A busy lock - on the initial install or on unpause - propagates so the
// caller can map it to OpfsUnavailableError.
async function getPool(sqlite3, name) {
  let pending = pools.get(name);
  if (!pending) {
    pending = (async () => {
      const poolUtil = await sqlite3.installOpfsSAHPoolVfs({
        name: vfsName(name),
        directory: poolDir(name),
        initialCapacity: INITIAL_CAPACITY,
        forceReinitIfPreviouslyFailed: true,
      });
      if (poolUtil.getCapacity() < INITIAL_CAPACITY) {
        await poolUtil.reserveMinimumCapacity(INITIAL_CAPACITY);
      }
      return { poolUtil, refs: 0 };
    })();
    pools.set(name, pending);
    // Drop a failed install so a later open re-attempts rather than awaiting the
    // same rejected promise; a resolved pool stays cached.
    pending.catch(() => {
      if (pools.get(name) === pending) pools.delete(name);
    });
  }
  const entry = await pending;
  // A pool paused by a prior close has released its sync access handles and is
  // unregistered from SQLite; re-acquire them before opening a database on it.
  if (entry.poolUtil.isPaused()) {
    await entry.poolUtil.unpauseVfs();
  }
  return entry;
}

/**
 * Open (or create) a database under `name`. When `ephemeral` is true, or OPFS
 * sync access handles are unavailable, the session is in-memory and does not
 * persist. Returns an opaque handle (passed back to `exec`/`query`) carrying
 * the active `persistenceMode`.
 */
export async function open(name, ephemeral = false) {
  const sqlite3 = await moduleHandle();
  if (
    ephemeral ||
    !opfsSyncAvailable() ||
    typeof sqlite3.installOpfsSAHPoolVfs !== "function"
  ) {
    return openMemory(sqlite3);
  }

  let entry;
  try {
    entry = await getPool(sqlite3, name);
  } catch (err) {
    // A busy lock is the one failure worth surfacing: OPFS works here, another
    // tab just holds this database. Throw so the caller can prompt to close it
    // rather than silently forking to a separate in-memory store. The cached
    // rejection is cleared on the next attempt by forceReinitIfPreviouslyFailed,
    // so this is recoverable once the holder releases.
    if (isBusyLock(err)) {
      throw new OpfsUnavailableError(
        `OPFS is busy for "${name}": another tab or client holds its lock. ` +
          `Close the other session, or open with ephemeral: true for an in-memory session.`,
        { cause: err },
      );
    }
    // Any other failure means OPFS is advertised but not usable here (private
    // window, quota or security error, transient DOMException). Degrade to
    // memory rather than failing the open.
    return openMemory(sqlite3);
  }

  const db = new entry.poolUtil.OpfsSAHPoolDb(MAIN_DB_PATH);
  entry.refs += 1; // count a live connection on this pool; close releases it
  registerFnv1a(db);
  return { db, persistenceMode: PERSISTENT, name };
}

/**
 * Execute a statement that returns no rows (DDL, INSERT, DELETE, BEGIN/COMMIT).
 * `params` is a positional array binding `?`, `?2`, ... in order. Multi-statement
 * batches (schema and index DDL) pass no params; every parameterised builder
 * emits a single statement, so the bind applies to that one statement.
 */
export async function exec(handle, sql, params) {
  handle.db.exec({ sql, bind: params && params.length ? params : undefined });
}

/**
 * Run a query and return its rows.
 * Each row is an array of column values in SELECT order (`rowMode: "array"`),
 * which is the shape the Rust col_* readers consume.
 */
export async function query(handle, sql, params) {
  return handle.db.exec({
    sql,
    bind: params && params.length ? params : undefined,
    rowMode: "array",
    returnValue: "resultRows",
  });
}

/**
 * Close a database handle, releasing its connection so a re-open does not leak
 * the old one. When the last connection on a persistent pool closes, the pool
 * is paused: its OPFS sync access handles are relinquished (so another tab can
 * acquire the same database) while the files stay intact, and the pool stays
 * cached so a same-name re-open in this Worker unpauses rather than reinstalls.
 * The database is closed before pausing, since pauseVfs refuses to run with an
 * open file on the VFS. Nulling `handle.db` first makes a duplicate close a
 * no-op.
 */
export async function close(handle) {
  if (!handle || handle.db == null) return;
  const db = handle.db;
  handle.db = null;
  db.close();

  if (handle.name == null) return; // in-memory session: nothing pooled
  const pending = pools.get(handle.name);
  const entry = pending ? await pending.catch(() => null) : null;
  if (!entry) return;
  entry.refs -= 1;
  if (entry.refs <= 0 && !entry.poolUtil.isPaused()) {
    // Relinquish the sync access handles for another tab without destroying the
    // data. The pool stays in the registry (paused) for a later re-open.
    entry.poolUtil.pauseVfs();
  }
}
