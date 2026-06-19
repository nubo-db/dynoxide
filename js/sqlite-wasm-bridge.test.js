import { test } from "node:test";
import assert from "node:assert/strict";

import { open, exec, query, close, exec_batch } from "./sqlite-wasm-bridge.js";
import { fnv1aHash } from "./fnv1a.js";

// Off-browser proof of the bridge's SQL contract against the official
// @sqlite.org/sqlite-wasm engine. The official core wasm and `:memory:`
// databases run under Node (the OPFS SAHPool VFS is browser-only and is proven
// in the Playwright suite, tests/browser/), so these drive an ephemeral handle
// directly through the same open/exec/query/close the Rust backend calls.
//
// `open(name, true)` forces the in-memory path: there is no OPFS in Node, so a
// persistent open would degrade to memory anyway, but the explicit flag keeps
// the intent clear and the handle's persistenceMode deterministic.

async function withMemoryDb(fn) {
  const handle = await open("bridge-test", true);
  try {
    return await fn(handle);
  } finally {
    await close(handle);
  }
}

test("the bridge exports exactly the open/exec/query/close contract", () => {
  // The Rust extern block binds these four names by js_name; the migration must
  // not rename or drop any of them.
  for (const fn of [open, exec, query, close]) {
    assert.equal(typeof fn, "function");
  }
});

test("an ephemeral open reports memory mode", async () => {
  await withMemoryDb((handle) => {
    assert.equal(handle.persistenceMode, "memory");
  });
});

test("happy path: positional binds round-trip as column arrays in SELECT order", async () => {
  await withMemoryDb(async (handle) => {
    await exec(handle, "CREATE TABLE t (pk TEXT, sk TEXT, n INTEGER)", []);
    await exec(handle, "INSERT INTO t (pk, sk, n) VALUES (?, ?, ?)", ["u#1", "a", 10]);
    await exec(handle, "INSERT INTO t (pk, sk, n) VALUES (?, ?, ?)", ["u#1", "b", 20]);

    const rows = await query(handle, "SELECT pk, sk, n FROM t ORDER BY sk", []);
    // Rows come back as arrays of column values in SELECT order - the shape the
    // Rust col_* readers consume.
    assert.deepEqual(rows, [
      ["u#1", "a", 10],
      ["u#1", "b", 20],
    ]);
  });
});

test("a parameterised query binds and filters", async () => {
  await withMemoryDb(async (handle) => {
    await exec(handle, "CREATE TABLE t (pk TEXT, sk TEXT)", []);
    for (const [pk, sk] of [["u#1", "a"], ["u#1", "b"], ["u#2", "c"]]) {
      await exec(handle, "INSERT INTO t (pk, sk) VALUES (?, ?)", [pk, sk]);
    }
    const rows = await query(handle, "SELECT sk FROM t WHERE pk = ? ORDER BY sk", ["u#1"]);
    assert.deepEqual(rows, [["a"], ["b"]]);
  });
});

test("multi-statement exec applies every statement (no binds)", async () => {
  // The schema bootstrap (sql_builders::INIT_SCHEMA) and the index DDL are
  // multi-statement batches passed with no params; every statement must run.
  await withMemoryDb(async (handle) => {
    await exec(
      handle,
      "CREATE TABLE a (x TEXT); CREATE TABLE b (y TEXT); CREATE INDEX b_y ON b (y);",
      [],
    );
    const names = (
      await query(
        handle,
        "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name",
        [],
      )
    ).map((row) => row[0]);
    assert.deepEqual(names, ["a", "b"]);
  });
});

test("integer round-trip > 2^53 is bit-identical to the stored i64 (hard gate)", async () => {
  // The decisive migration check (the col_i64 > 2^53 parity item). The Rust side
  // binds an integer outside f64's safe range as a BigInt and reads it back
  // through col_i64's BigInt branch; this proves the official OO1 exec preserves
  // the exact i64 transparently, so the bridge needs no explicit conversion of
  // its own. The assertion is bit-identity, not "looks non-lossy".
  await withMemoryDb(async (handle) => {
    const exact = 9007199254740993n; // 2^53 + 1: the smallest i64 f64 cannot hold
    await exec(handle, "CREATE TABLE big (id INTEGER)", []);
    await exec(handle, "INSERT INTO big (id) VALUES (?)", [exact]);

    const [[readBack]] = await query(handle, "SELECT id FROM big", []);
    assert.equal(typeof readBack, "bigint", "an i64 past 2^53 must return as BigInt, not a lossy number");
    assert.equal(readBack, exact, "the read value must equal the exact i64 stored");
  });
});

test("a small integer round-trips as a plain JS number", async () => {
  // The other side of the contract: values inside f64's safe range come back as
  // numbers (Rust's col_i64 reads them via as_f64), not BigInt.
  await withMemoryDb(async (handle) => {
    await exec(handle, "CREATE TABLE small (id INTEGER)", []);
    await exec(handle, "INSERT INTO small (id) VALUES (?)", [42]);
    const [[readBack]] = await query(handle, "SELECT id FROM small", []);
    assert.equal(typeof readBack, "number");
    assert.equal(readBack, 42);
  });
});

test("fnv1a_hash scalar matches js/fnv1a.js byte-for-byte", async () => {
  // The scalar drives parallel-scan segment assignment (fnv1a_hash(pk) % total),
  // so the in-engine function and the JS reference must agree. Reuses the same
  // inputs the native parity test covers (src/storage.rs / js/fnv1a.test.js).
  await withMemoryDb(async (handle) => {
    for (const input of ["", "a", "u#1", "artist#42", "café", "tenant#9007199254740993"]) {
      const [[hashed]] = await query(handle, "SELECT fnv1a_hash(?)", [input]);
      assert.equal(
        Number(hashed),
        fnv1aHash(input),
        `fnv1a_hash(${JSON.stringify(input)})`,
      );
    }
  });
});

// --- exec_batch: the batched-write primitive (issue #71) -------------------

// The GSI-shaped table and insert that the wasm backend's insert_gsi_items
// drives through exec_batch (sql_builders::gsi_insert_sql): five placeholders,
// one statement, many rows.
const GSI_DDL =
  "CREATE TABLE g (gsi_pk TEXT, gsi_sk TEXT, table_pk TEXT, table_sk TEXT, item_json TEXT, " +
  "PRIMARY KEY (gsi_pk, gsi_sk, table_pk, table_sk))";
const GSI_INSERT =
  "INSERT OR REPLACE INTO g (gsi_pk, gsi_sk, table_pk, table_sk, item_json) VALUES (?1, ?2, ?3, ?4, ?5)";

test("exec_batch inserts every row in one call", async () => {
  await withMemoryDb(async (handle) => {
    await exec(handle, GSI_DDL, []);
    const rows = [];
    for (let i = 0; i < 5; i += 1) rows.push([`g${i}`, `s${i}`, `p${i}`, `t${i}`, `{"n":${i}}`]);
    await exec_batch(handle, GSI_INSERT, rows);

    const read = await query(handle, "SELECT gsi_pk, gsi_sk, item_json FROM g ORDER BY gsi_pk", []);
    assert.equal(read.length, 5);
    assert.deepEqual(read[0], ["g0", "s0", '{"n":0}']);
    assert.deepEqual(read[4], ["g4", "s4", '{"n":4}']);
  });
});

test("exec_batch keeps each row's own value, including an empty sort key", async () => {
  // The GSI path binds an absent sort key as the empty string, not a missing
  // parameter, so every row carries five values. Each row must keep its own
  // gsi_sk rather than inherit the previous row's.
  await withMemoryDb(async (handle) => {
    await exec(handle, GSI_DDL, []);
    await exec_batch(handle, GSI_INSERT, [
      ["g", "sk1", "p1", "t1", "{}"],
      ["g", "", "p2", "t2", "{}"],
      ["g", "sk3", "p3", "t3", "{}"],
    ]);
    const sks = (await query(handle, "SELECT gsi_sk FROM g ORDER BY table_pk", [])).map((r) => r[0]);
    assert.deepEqual(sks, ["sk1", "", "sk3"]);
  });
});

test("exec_batch rejects a row of the wrong arity, naming the row index", async () => {
  // The internal-primitive contract (issue #71): a mismatched row is a caller
  // bug, rejected rather than silently NULL-padded. The five-placeholder insert
  // gets a four-element row at index 1.
  await withMemoryDb(async (handle) => {
    await exec(handle, GSI_DDL, []);
    await assert.rejects(
      () =>
        exec_batch(handle, GSI_INSERT, [
          ["g0", "s0", "p0", "t0", "{}"],
          ["g1", "s1", "p1", "t1"], // only four values
        ]),
      (e) => {
        assert.match(e.message, /row 1/);
        assert.match(e.message, /expected 5 parameters, got 4/);
        return true;
      },
    );
  });
});

test("exec_batch is a no-op on an empty batch", async () => {
  await withMemoryDb(async (handle) => {
    await exec(handle, GSI_DDL, []);
    await exec_batch(handle, GSI_INSERT, []);
    const [[count]] = await query(handle, "SELECT count(*) FROM g", []);
    assert.equal(Number(count), 0);
  });
});

test("exec_batch runs inside the caller's open transaction", async () => {
  // The real usage shape: the backfill runs between the BEGIN and COMMIT that
  // UpdateTable owns. The primitive itself issues no BEGIN/COMMIT.
  await withMemoryDb(async (handle) => {
    await exec(handle, GSI_DDL, []);
    await exec(handle, "BEGIN IMMEDIATE", []);
    await exec_batch(handle, GSI_INSERT, [
      ["g0", "s0", "p0", "t0", "{}"],
      ["g1", "s1", "p1", "t1", "{}"],
    ]);
    await exec(handle, "COMMIT", []);
    const [[count]] = await query(handle, "SELECT count(*) FROM g", []);
    assert.equal(Number(count), 2);
  });
});

test("a mid-batch failure names the row and the caller's rollback undoes the batch", async () => {
  // Atomicity comes from the caller's transaction, not the primitive: a row that
  // violates a constraint mid-batch throws with its index, and ROLLBACK undoes
  // even the rows that applied before it.
  await withMemoryDb(async (handle) => {
    await exec(handle, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL)", []);
    await exec(handle, "BEGIN IMMEDIATE", []);
    await assert.rejects(
      () =>
        exec_batch(handle, "INSERT INTO t (id, v) VALUES (?1, ?2)", [
          [1, "ok"],
          [2, null], // NOT NULL violation at step
        ]),
      (e) => {
        assert.match(e.message, /row 1/);
        return true;
      },
    );
    await exec(handle, "ROLLBACK", []);
    const [[count]] = await query(handle, "SELECT count(*) FROM t", []);
    assert.equal(Number(count), 0); // row 0 was rolled back with the outer transaction
  });
});
