/**
 * Bridge between dynoxide's wasm-bindgen backend and wa-sqlite.
 *
 * Exposes three async functions - `open`, `exec`, `query` - consumed by
 * `src/storage_backend/wasm_backend.rs` through a `#[wasm_bindgen]` extern
 * block. The Rust side builds every SQL statement (shared with the native
 * backend via `sql_builders`) and hands it here with a positional parameter
 * array; this module only opens the database and runs statements.
 *
 * Runs inside the dynoxide Web Worker (see js/dynoxide-worker.js). Persistence
 * uses wa-sqlite's synchronous OPFS VFS (`AccessHandlePoolVFS`), backed by sync
 * access handles, which browsers expose only in a Worker. Because that VFS is
 * synchronous it pairs with the smaller non-async wa-sqlite build (no Asyncify
 * instrumentation), which roughly halves the wa-sqlite wasm. No cross-origin
 * isolation (COOP/COEP) is required.
 *
 * Where OPFS sync access handles are unavailable - Firefox private windows,
 * older Safari - or an embedder asks for it, `open` falls back to an in-memory
 * VFS (`MemoryVFS`, also synchronous, so no async build): the session works but
 * does not survive a reload. The active mode comes back on the open handle as
 * `persistenceMode` so the engine can warn the user. A persistent IndexedDB
 * fallback would need the Asyncify async build (~2x the wasm) and is out of
 * scope here.
 *
 * One OPFS failure is surfaced rather than hidden: a *busy* database - another
 * tab or Worker already holding its sync access handles - throws an
 * `OpfsUnavailableError`, because silently forking to a separate in-memory store
 * would split reads and writes across two datasets and lose everything on
 * reload. Any other failure (a private window whose handles never come up, a
 * quota or security error, a transient `DOMException`) means OPFS is advertised
 * but unusable here, so `open` degrades to the in-memory session - the same as a
 * context with no OPFS at all - rather than failing to boot.
 *
 * Each database opens against its own named VFS over a per-name OPFS directory,
 * so two engine instances (two Workers) never contend on a shared pool. The
 * base `AccessHandlePoolVFS` hardcodes its registry name, so we shadow it with
 * a per-pool name and select it explicitly through `open_v2`'s `zVfs` argument.
 *
 * Imports use bare specifiers, so this module is bundler-friendly. wa-sqlite's
 * `.wasm` is located at runtime via `locateFile` relative to the bundle, so it
 * ships as a sibling asset rather than being inlined. Not exercised by the
 * conformance suite (see the WASM note in the README).
 */

import * as SQLite from "wa-sqlite";
import SQLiteESMFactory from "wa-sqlite/dist/wa-sqlite.mjs";
import { AccessHandlePoolVFS } from "wa-sqlite/src/examples/AccessHandlePoolVFS.js";
import { MemoryVFS } from "wa-sqlite/src/examples/MemoryVFS.js";
import { fnv1aHash } from "./fnv1a.js";

/** Persistent OPFS-backed session: survives reload. */
const PERSISTENT = "opfs";
/** Ephemeral in-memory session: lost on reload. */
const EPHEMERAL = "memory";

// Lazily initialised SQLite API handle, shared across opens within this Worker.
// We memoise the in-flight promise rather than the resolved value, so two
// concurrent first callers share one initialisation. On failure we clear it so
// a later call can retry rather than caching the error. No VFS is registered
// here; `open` registers the right one for its database.
let sqlite3Promise = null;

function moduleHandle() {
  if (!sqlite3Promise) {
    sqlite3Promise = (async () => {
      // Locate wa-sqlite's .wasm next to this module at runtime. After
      // bundling, import.meta.url is the bundle's URL, so the .wasm resolves as
      // a sibling asset in dist/.
      const module = await SQLiteESMFactory({
        locateFile: (file) => new URL(file, import.meta.url).href,
      });
      return SQLite.Factory(module);
    })().catch((err) => {
      sqlite3Promise = null;
      throw err;
    });
  }
  return sqlite3Promise;
}

// VFS registry for this Worker, keyed by OPFS pool path so repeated opens of
// one database reuse a single registration. Each entry is the *in-flight*
// registration promise, resolving to { vfsName, mode, vfs, poolPath, refs },
// not the resolved value, so two concurrent opens of one database share a single
// registration rather than both running vfs_register. `refs` counts live
// connections on the pool; a failed registration is dropped so a later open can
// retry, and a persistent pool is dropped once its last connection closes (see
// `close`) so its OPFS handles are released. A memory pool stays cached.
const vfsByPool = new Map();

// Monotonic suffix so each fresh registration gets a unique SQLite VFS name.
// wa-sqlite exposes no vfs_unregister, so once a released pool's name lingers in
// SQLite's registry, a later registration for the same database must not collide
// with it - a new suffix sidesteps that.
let vfsSeq = 0;

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
// phrase a "busy" message apart from a generic OPFS open failure.
function isBusyLock(err) {
  const name = err && err.name;
  return name === "NoModificationAllowedError" || name === "InvalidStateError";
}

function registerMemoryVfs(s, name, poolPath) {
  const vfsName = `dynoxide-memory-${slug(name)}-${vfsSeq++}`;
  const vfs = new MemoryVFS();
  vfs.name = vfsName; // MemoryVFS exposes `name` as a writable field.
  s.vfs_register(vfs, false);
  return { vfsName, mode: EPHEMERAL, vfs, poolPath, refs: 0 };
}

async function registerOpfsVfs(s, name, poolPath) {
  const vfsName = `dynoxide-opfs-${slug(name)}-${vfsSeq++}`;
  const vfs = new AccessHandlePoolVFS(poolPath);
  // The base class hardcodes `get name() { return 'AccessHandlePool' }`;
  // shadow it with a per-pool name so distinct instances register distinct
  // VFSes and open_v2 selects the right one.
  Object.defineProperty(vfs, "name", { value: vfsName, configurable: true });
  await vfs.isReady;
  s.vfs_register(vfs, false);
  return { vfsName, mode: PERSISTENT, vfs, poolPath, refs: 0 };
}

// FNV-1a (32-bit) over the raw name, so two names that sanitise to the same
// characters (e.g. "a.b" and "a_b") still get distinct pools. Deliberately NOT
// fnv1aHash from fnv1a.js: this hashes UTF-16 code units (charCodeAt) and emits
// base-36 for a filesystem-safe slug, where fnv1aHash hashes UTF-8 bytes and
// returns a number. They agree on ASCII names but diverge beyond it. Do not
// consolidate them - the output feeds slug() and every OPFS pool path, so a
// hash change would relocate persisted databases and lose their data.
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

// Whether this context can back the synchronous OPFS VFS. The definitive test
// is constructing the pool and awaiting isReady (done in registerVfs); this is
// the cheap pre-check that lets an obviously-unsupported context skip straight
// to the in-memory fallback.
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

// Register (once per pool path) a VFS for `name`, returning the in-flight
// promise so concurrent opens share one registration. Uses the in-memory VFS
// only when `ephemeral` is requested or OPFS sync access handles are genuinely
// unavailable - both stable for the Worker's life, so caching them is correct.
// When OPFS is available but the pool will not come up, it rejects with an
// `OpfsUnavailableError` (distinguishing a busy lock) instead of silently
// forking to memory. Resolves to { vfsName, mode }.
function registerVfs(s, name, ephemeral) {
  const poolPath = `/dynoxide/${slug(name)}`;
  const cached = vfsByPool.get(poolPath);
  if (cached) return cached;

  const pending = (async () => {
    if (ephemeral || !opfsSyncAvailable()) {
      return registerMemoryVfs(s, name, poolPath);
    }
    try {
      return await registerOpfsVfs(s, name, poolPath);
    } catch (err) {
      // A busy lock is the one OPFS failure worth surfacing: the API works here,
      // another tab or Worker just holds this database. Throw so the caller can
      // tell the user to close the other session, rather than silently forking
      // to a separate in-memory store (the bug that motivated this path).
      if (isBusyLock(err)) {
        throw new OpfsUnavailableError(
          `OPFS is busy for "${name}": another tab or client holds its lock. ` +
            `Close the other session, or open with ephemeral: true for an in-memory session.`,
          { cause: err },
        );
      }
      // Any other failure means OPFS is advertised but not actually usable here
      // (a private window whose handles never come up, a quota or security
      // error, a transient DOMException). Degrade to an ephemeral session rather
      // than failing to boot: opfsSyncAvailable() proves the API is present, not
      // that it works.
      return registerMemoryVfs(s, name, poolPath);
    }
  })();

  vfsByPool.set(poolPath, pending);
  // Drop a failed registration so a later open can retry rather than being
  // handed the same rejected promise. A resolved one stays cached.
  pending.catch(() => {
    if (vfsByPool.get(poolPath) === pending) vfsByPool.delete(poolPath);
  });
  return pending;
}

/**
 * Open (or create) a database under `name`. When `ephemeral` is true, or OPFS
 * sync access handles are unavailable, the session is in-memory and does not
 * persist. Returns an opaque handle (passed back to `exec`/`query`) carrying
 * the active `persistenceMode`.
 */
export async function open(name, ephemeral = false) {
  const s = await moduleHandle();
  const entry = await registerVfs(s, name, ephemeral);
  const db = await s.open_v2(
    name,
    SQLite.SQLITE_OPEN_CREATE | SQLite.SQLITE_OPEN_READWRITE,
    entry.vfsName,
  );
  entry.refs += 1; // count a live connection on this pool; close releases it

  // Register fnv1a_hash for GSI/LSI parallel-scan segment filtering, matching
  // the native scalar function: FNV-1a (32-bit) over the value's UTF-8 bytes,
  // returned as an integer so `fnv1a_hash(col) % total` is integer modulo.
  s.create_function(
    db,
    "fnv1a_hash",
    1,
    SQLite.SQLITE_UTF8,
    0,
    (context, values) => {
      // fnv1aHash matches the native scalar (src/storage.rs); see js/fnv1a.js.
      s.result(context, BigInt(fnv1aHash(s.value(values[0]))));
    },
    null,
    null,
  );

  return { db, persistenceMode: entry.mode, poolPath: entry.poolPath };
}

/**
 * Execute a statement that returns no rows (DDL, INSERT, DELETE, BEGIN/COMMIT).
 * `params` is a positional array binding `?1`, `?2`, ... in order.
 */
export async function exec(handle, sql, params) {
  const s = await moduleHandle();
  // Positional params bind to the first statement only. Every parameterised
  // builder emits a single statement; multi-statement batches (schema and index
  // DDL) pass no params. Guarding on the first statement avoids silently
  // re-binding the same array to later statements in a batch.
  let first = true;
  for await (const stmt of s.statements(handle.db, sql)) {
    if (first && params && params.length) s.bind_collection(stmt, params);
    first = false;
    while ((await s.step(stmt)) === SQLite.SQLITE_ROW) {
      // exec consumes no rows
    }
  }
}

/**
 * Run a query and return its rows.
 * Each row is an array of column values in SELECT order.
 */
export async function query(handle, sql, params) {
  const s = await moduleHandle();
  const rows = [];
  // Params bind to the first statement only; see exec for the rationale.
  let first = true;
  for await (const stmt of s.statements(handle.db, sql)) {
    if (first && params && params.length) s.bind_collection(stmt, params);
    first = false;
    while ((await s.step(stmt)) === SQLite.SQLITE_ROW) {
      rows.push(s.row(stmt));
    }
  }
  return rows;
}

/**
 * Close a database handle, releasing its wa-sqlite connection. The engine calls
 * this on the previous database after a re-open swaps in a new one, so the old
 * connection does not leak.
 *
 * When the closed connection was the last one on a persistent pool, the pool's
 * OPFS sync access handles are released too (they are owned by the VFS instance,
 * not the connection, and survive SQLite's `xClose`), and its registration is
 * forgotten - so re-opening a *different* database frees the old one's name for
 * another tab, and a later open of the same name builds a fresh pool that
 * re-acquires the persisted files. Memory pools hold no OS handles and stay
 * cached. Nulling `handle.db` first makes a duplicate close a genuine no-op.
 */
export async function close(handle) {
  if (!handle || handle.db == null) return;
  const s = await moduleHandle();
  const db = handle.db;
  handle.db = null;
  await s.close(db);

  const pending = handle.poolPath != null ? vfsByPool.get(handle.poolPath) : null;
  const entry = pending ? await pending.catch(() => null) : null;
  if (entry) {
    entry.refs -= 1;
    if (entry.refs <= 0 && entry.mode === PERSISTENT) {
      await entry.vfs.close();
      if (vfsByPool.get(handle.poolPath) === pending) {
        vfsByPool.delete(handle.poolPath);
      }
    }
  }
}
