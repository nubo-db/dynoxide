/**
 * Bridge between dynoxide's wasm-bindgen backend and wa-sqlite.
 *
 * Exposes three async functions - `open`, `exec`, `query` - consumed by
 * `src/storage_backend/wasm_backend.rs` through a `#[wasm_bindgen]` extern
 * block. The Rust side builds every SQL statement (shared with the native
 * backend via `sql_builders`) and hands it here with a positional parameter
 * array; this module only opens the database and runs statements.
 *
 * Preview: this wires wa-sqlite's async build to its main-thread OPFS VFS
 * (`OriginPrivateFileSystemVFS`) so the database persists to OPFS without a Web
 * Worker. It is not exercised by the conformance suite (see the WASM note in
 * the README). If a future wa-sqlite renames or moves that VFS, adjust the
 * import below; the IndexedDB VFS (`IDBBatchAtomicVFS`) is the documented
 * fallback when OPFS is unavailable.
 */

import SQLiteESMFactory from "wa-sqlite/dist/wa-sqlite-async.mjs";
import * as SQLite from "wa-sqlite";

// Lazily initialised SQLite API handle, shared across opens.
let sqlite3 = null;

async function moduleHandle() {
  if (sqlite3) return sqlite3;
  const module = await SQLiteESMFactory();
  sqlite3 = SQLite.Factory(module);

  // Main-thread async OPFS VFS: persists to OPFS without a Web Worker.
  // Registered as the default VFS so open_v2 uses it.
  const { OriginPrivateFileSystemVFS } = await import(
    "wa-sqlite/src/examples/OriginPrivateFileSystemVFS.js"
  );
  const vfs = new OriginPrivateFileSystemVFS();
  sqlite3.vfs_register(vfs, true);
  return sqlite3;
}

/**
 * Open (or create) a database persisted under `name`.
 * Returns an opaque handle passed back to `exec`/`query`.
 */
export async function open(name) {
  const s = await moduleHandle();
  const db = await s.open_v2(name);
  return { db };
}

/**
 * Execute a statement that returns no rows (DDL, INSERT, DELETE, BEGIN/COMMIT).
 * `params` is a positional array binding `?1`, `?2`, ... in order.
 */
export async function exec(handle, sql, params) {
  const s = sqlite3;
  for await (const stmt of s.statements(handle.db, sql)) {
    if (params && params.length) s.bind_collection(stmt, params);
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
  const s = sqlite3;
  const rows = [];
  for await (const stmt of s.statements(handle.db, sql)) {
    if (params && params.length) s.bind_collection(stmt, params);
    while ((await s.step(stmt)) === SQLite.SQLITE_ROW) {
      rows.push(s.row(stmt));
    }
  }
  return rows;
}
