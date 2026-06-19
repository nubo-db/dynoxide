import { test, expect } from "@playwright/test";

// End-to-end tests of the shipped wasm engine in a real browser: the bundled
// Worker, both .wasm, wa-sqlite and OPFS. This is the path the conformance
// suite does not exercise (it runs against the native backend), so it is the
// safety net for the preview-to-stable transition - it builds the same dist/ a
// consumer installs and runs real DynamoDB operations through EngineClient.

const MUSIC = {
  TableName: "Music",
  KeySchema: [
    { AttributeName: "artist", KeyType: "HASH" },
    { AttributeName: "song", KeyType: "RANGE" },
  ],
  AttributeDefinitions: [
    { AttributeName: "artist", AttributeType: "S" },
    { AttributeName: "song", AttributeType: "S" },
  ],
  BillingMode: "PAY_PER_REQUEST",
};

test.beforeEach(async ({ page }) => {
  await page.goto("/harness/engine-harness.html");
  await page.waitForFunction(() => globalThis.__HARNESS_READY__ === true);
});

test("CRUD round-trip: persists to OPFS, and a filtered scan reads more than it counts", async ({ page }) => {
  const result = await page.evaluate(async (table) => {
    const client = globalThis.dynoxide.makeClient({ name: `crud-${crypto.randomUUID()}` });
    await client.ready();
    await client.execute("CreateTable", table);
    for (const [song, genre] of [["s1", "rock"], ["s2", "jazz"], ["s3", "rock"]]) {
      await client.execute("PutItem", {
        TableName: table.TableName,
        Item: { artist: { S: "a" }, song: { S: song }, genre: { S: genre } },
      });
    }
    const query = await client.execute("Query", {
      TableName: table.TableName,
      KeyConditionExpression: "artist = :a",
      ExpressionAttributeValues: { ":a": { S: "a" } },
    });
    const scan = await client.execute("Scan", {
      TableName: table.TableName,
      FilterExpression: "genre = :g",
      ExpressionAttributeValues: { ":g": { S: "rock" } },
    });
    const out = {
      persistenceMode: client.persistenceMode,
      queryCount: query.Count,
      scanCount: scan.Count,
      scannedCount: scan.ScannedCount,
    };
    client.terminate();
    return out;
  }, MUSIC);

  expect(result.persistenceMode).toBe("opfs");
  expect(result.queryCount).toBe(3);
  expect(result.scanCount).toBe(2);
  expect(result.scannedCount).toBe(3);
});

test("a body-less op (ListTables) round-trips instead of a SerializationException (#65)", async ({ page }) => {
  const result = await page.evaluate(async (table) => {
    const client = globalThis.dynoxide.makeClient({ name: `list-${crypto.randomUUID()}` });
    await client.ready();
    await client.execute("CreateTable", table);
    // No request body: this used to stringify undefined and reject as a
    // SerializationException.
    const listed = await client.execute("ListTables");
    client.terminate();
    return listed;
  }, MUSIC);

  expect(result.TableNames).toContain(MUSIC.TableName);
});

test("data survives a reload: a fresh client on the same name sees the writes (#64)", async ({ page }) => {
  const name = `persist-${Date.now()}-${Math.floor(Math.random() * 1e6)}`;

  const firstMode = await page.evaluate(async ({ name, table }) => {
    const client = globalThis.dynoxide.makeClient({ name });
    await client.ready();
    await client.execute("CreateTable", table);
    await client.execute("PutItem", {
      TableName: table.TableName,
      Item: { artist: { S: "a" }, song: { S: "s1" } },
    });
    const mode = client.persistenceMode;
    client.terminate(); // tears down the Worker, releasing the OPFS handles
    return mode;
  }, { name, table: MUSIC });
  expect(firstMode).toBe("opfs");

  // Let the terminated Worker's OPFS handles release before re-opening.
  await page.waitForTimeout(150);

  const reopened = await page.evaluate(async ({ name, table }) => {
    const client = globalThis.dynoxide.makeClient({ name });
    await client.ready();
    const scan = await client.execute("Scan", { TableName: table.TableName });
    const out = { mode: client.persistenceMode, count: scan.Count };
    client.terminate();
    return out;
  }, { name, table: MUSIC });

  expect(reopened.mode).toBe("opfs");
  expect(reopened.count).toBe(1);
});

test("a second client on a busy OPFS database fails clearly instead of silently forking to memory (#64)", async ({ page }) => {
  const result = await page.evaluate(async () => {
    const name = `busy-${crypto.randomUUID()}`;
    const a = globalThis.dynoxide.makeClient({ name });
    await a.ready(); // holds this database's OPFS sync access handles

    const b = globalThis.dynoxide.makeClient({ name });
    let bError = null;
    let bMode = null;
    try {
      await b.ready();
      bMode = b.persistenceMode; // a silent fork would land here as "memory"
    } catch (e) {
      bError = { type: e.type, message: e.message };
    }

    const aMode = a.persistenceMode;
    a.terminate();
    b.terminate();
    return { aMode, bError, bMode };
  });

  expect(result.aMode).toBe("opfs");
  // The contended second client must report the conflict, not quietly become an
  // independent in-memory store that loses its writes on reload.
  expect(result.bMode).not.toBe("memory");
  expect(result.bError).not.toBeNull();
  expect(result.bError.message).toMatch(/busy|OPFS/i);
  // A stable, dynoxide-specific type so a consumer can branch on the conflict
  // (e.g. prompt to close the other tab) rather than string-matching the message.
  expect(result.bError.type).toBe("com.dynoxide.wasm#OpfsUnavailable");
});

const TABLE_T = {
  TableName: "Reopens",
  KeySchema: [{ AttributeName: "pk", KeyType: "HASH" }],
  AttributeDefinitions: [{ AttributeName: "pk", AttributeType: "S" }],
  BillingMode: "PAY_PER_REQUEST",
};

test("a failed re-open leaves the previous database open and usable (#64)", async ({ page }) => {
  const result = await page.evaluate(async (table) => {
    const nameA = `reopenA-${crypto.randomUUID()}`;
    const nameB = `reopenB-${crypto.randomUUID()}`;

    const w1 = globalThis.dynoxide.makeRawWorker();
    await w1.open(nameA);
    await w1.execute("CreateTable", table);
    await w1.execute("PutItem", { TableName: "Reopens", Item: { pk: { S: "a1" } } });

    // A second worker holds nameB, so w1's re-open to nameB must fail busy.
    const w2 = globalThis.dynoxide.makeRawWorker();
    await w2.open(nameB);

    let reopenErr = null;
    try {
      await w1.open(nameB);
    } catch (e) {
      try {
        reopenErr = JSON.parse(e.message);
      } catch {
        reopenErr = { message: e.message };
      }
    }

    // The failed re-open must not have torn down the working nameA session.
    const scan = await w1.execute("Scan", { TableName: "Reopens" });

    w1.terminate();
    w2.terminate();
    return { reopenErr, count: scan.Count };
  }, TABLE_T);

  expect(result.reopenErr).not.toBeNull();
  expect(result.reopenErr.__type).toBe("com.dynoxide.wasm#OpfsUnavailable");
  expect(result.count).toBe(1); // the prior session survived the failed re-open
});

test("re-open keeps same-name data and frees the old database when switching names (#64)", async ({ page }) => {
  const nameA = `switchA-${Date.now()}-${Math.floor(Math.random() * 1e6)}`;
  const nameB = `switchB-${Date.now()}-${Math.floor(Math.random() * 1e6)}`;

  const out = await page.evaluate(async ({ nameA, nameB, table }) => {
    const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
    const w1 = globalThis.dynoxide.makeRawWorker();
    const d1 = await w1.open(nameA);
    await w1.execute("CreateTable", table);
    await w1.execute("PutItem", { TableName: "Reopens", Item: { pk: { S: "a1" } } });

    // Same-name re-open in one worker keeps the persisted row.
    await w1.open(nameA);
    const sameNameScan = await w1.execute("Scan", { TableName: "Reopens" });

    // Switch this worker to a different database. close(nameA) should release
    // nameA's OPFS handles, freeing it for another worker.
    await w1.open(nameB);
    await w1.execute("CreateTable", table);
    await w1.execute("PutItem", { TableName: "Reopens", Item: { pk: { S: "b1" } } });
    const bScan = await w1.execute("Scan", { TableName: "Reopens" });

    // w1 still holds nameB. A fresh worker opening nameA proves the switch
    // released nameA (a leak would leave it busy-locked). Small retry to absorb
    // any lag in the OS releasing the access handles.
    let aReopen = null;
    for (let attempt = 0; attempt < 10 && !aReopen; attempt += 1) {
      const w = globalThis.dynoxide.makeRawWorker();
      try {
        const d = await w.open(nameA);
        const scan = await w.execute("Scan", { TableName: "Reopens" });
        aReopen = { mode: d.persistenceMode, count: scan.Count };
        w.terminate();
      } catch {
        w.terminate();
        await sleep(50);
      }
    }

    w1.terminate();
    return { mode: d1.persistenceMode, sameNameCount: sameNameScan.Count, bCount: bScan.Count, aReopen };
  }, { nameA, nameB, table: TABLE_T });

  expect(out.mode).toBe("opfs");
  expect(out.sameNameCount).toBe(1); // same-name re-open kept the row
  expect(out.bCount).toBe(1); // the switched-to database is independent
  expect(out.aReopen).not.toBeNull(); // nameA was freed, not leaked-busy
  expect(out.aReopen.mode).toBe("opfs");
  expect(out.aReopen.count).toBe(1); // nameA's data persisted across the switch
});

test("the shipping worker rejects a stripped harness op as unknown (#69)", async ({ page }) => {
  // The shipping build strips the smoke/index/errors handling, so a harness op
  // sent to it falls through to the unknown-op envelope - the runtime proof that
  // the build-time strip is real, complementing build-wasm.sh's grep assertion.
  const err = await page.evaluate(async () => {
    const w = globalThis.dynoxide.makeRawWorker();
    let parsed = null;
    try {
      await w.call("smoke", {});
    } catch (e) {
      try {
        parsed = JSON.parse(e.message);
      } catch {
        parsed = { message: e.message };
      }
    }
    w.terminate();
    return parsed;
  });

  expect(err).not.toBeNull();
  expect(err.__type).toBe("com.dynoxide.wasm#UnsupportedOperation");
  expect(err.message).toMatch(/unknown op/);
});

// --- Migration to @sqlite.org/sqlite-wasm: re-proven guarantees ------------

test("OPFS persistence works with no cross-origin isolation (no COOP/COEP)", async ({ page }) => {
  // The whole reason for the SAHPool VFS: unlike the default OPFS VFS it needs
  // no SharedArrayBuffer, so it works on a page served without COOP/COEP. This
  // pins crossOriginIsolated === false while persistence still reports "opfs".
  const result = await page.evaluate(async (table) => {
    const isolated = globalThis.crossOriginIsolated;
    const client = globalThis.dynoxide.makeClient({ name: `noiso-${crypto.randomUUID()}` });
    await client.ready();
    await client.execute("CreateTable", table);
    await client.execute("PutItem", {
      TableName: table.TableName,
      Item: { artist: { S: "a" }, song: { S: "s1" } },
    });
    const scan = await client.execute("Scan", { TableName: table.TableName });
    const out = { isolated, mode: client.persistenceMode, count: scan.Count };
    client.terminate();
    return out;
  }, MUSIC);

  expect(result.isolated).toBe(false);
  expect(result.mode).toBe("opfs");
  expect(result.count).toBe(1);
});

test("persistence mode reports opfs for a persistent open and memory for an ephemeral one", async ({ page }) => {
  const result = await page.evaluate(async () => {
    const w1 = globalThis.dynoxide.makeRawWorker();
    const persistent = await w1.open(`mode-opfs-${crypto.randomUUID()}`); // ephemeral defaults false
    w1.terminate();

    const w2 = globalThis.dynoxide.makeRawWorker();
    const ephemeral = await w2.open(`mode-mem-${crypto.randomUUID()}`, true);
    w2.terminate();

    return { persistent: persistent.persistenceMode, ephemeral: ephemeral.persistenceMode };
  });

  expect(result.persistent).toBe("opfs");
  expect(result.ephemeral).toBe("memory");
});

test("a busy OPFS database recovers once the holder releases, not sticky until reload", async ({ page }) => {
  // The installer caches a failed init per VFS name, so without the bridge's
  // forceReinitIfPreviouslyFailed a once-busy name would stay busy until reload.
  // The retry here runs on the SAME worker that saw the busy failure, so a
  // success proves the cached rejection was cleared rather than replayed.
  const result = await page.evaluate(async () => {
    const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
    const name = `recover-${crypto.randomUUID()}`;

    const a = globalThis.dynoxide.makeRawWorker();
    await a.open(name); // holds this database's OPFS sync access handles

    const b = globalThis.dynoxide.makeRawWorker();
    let firstErr = null;
    try {
      await b.open(name);
    } catch (e) {
      try {
        firstErr = JSON.parse(e.message);
      } catch {
        firstErr = { message: e.message };
      }
    }

    a.terminate(); // release the handles

    let recovered = null;
    for (let attempt = 0; attempt < 20 && !recovered; attempt += 1) {
      try {
        const d = await b.open(name);
        recovered = d.persistenceMode;
      } catch {
        await sleep(50);
      }
    }
    b.terminate();
    return { firstErr, recovered };
  });

  expect(result.firstErr).not.toBeNull();
  expect(result.firstErr.__type).toBe("com.dynoxide.wasm#OpfsUnavailable");
  expect(result.recovered).toBe("opfs");
});

const BIG_N = "9007199254740993"; // 2^53 + 1: beyond f64 integer precision

test("a Number attribute beyond 2^53 round-trips bit-identical through put and read (sign-off gate)", async ({ page }) => {
  const result = await page.evaluate(async ({ table, big }) => {
    const client = globalThis.dynoxide.makeClient({ name: `bign-${crypto.randomUUID()}` });
    await client.ready();
    await client.execute("CreateTable", table);
    await client.execute("PutItem", {
      TableName: table.TableName,
      Item: { artist: { S: "a" }, song: { S: "s1" }, plays: { N: big } },
    });
    const read = await client.execute("Query", {
      TableName: table.TableName,
      KeyConditionExpression: "artist = :a",
      ExpressionAttributeValues: { ":a": { S: "a" } },
    });
    const out = { plays: read.Items[0].plays.N };
    client.terminate();
    return out;
  }, { table: MUSIC, big: BIG_N });

  // DynamoDB Numbers are arbitrary-precision decimal strings; the value must
  // come back verbatim, exactly as the native rusqlite backend returns it, with
  // no float rounding at 2^53. The SQLite-level i64 > 2^53 round-trip is proven
  // separately in the Node bridge test (js/sqlite-wasm-bridge.test.js).
  expect(result.plays).toBe(BIG_N);
});

const SEGMENTED = {
  TableName: "Segmented",
  KeySchema: [
    { AttributeName: "pk", KeyType: "HASH" },
    { AttributeName: "sk", KeyType: "RANGE" },
  ],
  AttributeDefinitions: [
    { AttributeName: "pk", AttributeType: "S" },
    { AttributeName: "sk", AttributeType: "S" },
    { AttributeName: "gpk", AttributeType: "S" },
    { AttributeName: "gsk", AttributeType: "S" },
  ],
  GlobalSecondaryIndexes: [
    {
      IndexName: "byG",
      KeySchema: [
        { AttributeName: "gpk", KeyType: "HASH" },
        { AttributeName: "gsk", KeyType: "RANGE" },
      ],
      Projection: { ProjectionType: "ALL" },
    },
  ],
  BillingMode: "PAY_PER_REQUEST",
};

test("a segmented parallel scan over a GSI matches a full scan, proving the fnv1a scalar", async ({ page }) => {
  // GSI scans filter by `fnv1a_hash(table_pk) % totalSegments` in SQL, so this
  // exercises the bridge's registered scalar end to end. The union of all
  // segments must equal a full scan exactly, with no item in two segments.
  const result = await page.evaluate(async (table) => {
    const client = globalThis.dynoxide.makeClient({ name: `seg-${crypto.randomUUID()}` });
    await client.ready();
    await client.execute("CreateTable", table);
    for (let i = 0; i < 24; i += 1) {
      await client.execute("PutItem", {
        TableName: table.TableName,
        Item: { pk: { S: `p${i}` }, sk: { S: "s" }, gpk: { S: `g${i % 7}` }, gsk: { S: `k${i}` } },
      });
    }
    const keyOf = (it) => `${it.pk.S}|${it.sk.S}`;

    const full = await client.execute("Scan", { TableName: table.TableName, IndexName: "byG" });
    const fullKeys = full.Items.map(keyOf).sort();

    const SEG = 4;
    const segKeys = [];
    for (let s = 0; s < SEG; s += 1) {
      const part = await client.execute("Scan", {
        TableName: table.TableName,
        IndexName: "byG",
        Segment: s,
        TotalSegments: SEG,
      });
      segKeys.push(...part.Items.map(keyOf));
    }
    const out = {
      fullCount: fullKeys.length,
      dupes: segKeys.length !== new Set(segKeys).size,
      unionMatches: JSON.stringify(segKeys.slice().sort()) === JSON.stringify(fullKeys),
    };
    client.terminate();
    return out;
  }, SEGMENTED);

  expect(result.fullCount).toBe(24);
  expect(result.dupes).toBe(false); // no item is assigned to two segments
  expect(result.unionMatches).toBe(true); // the segments partition the full scan exactly
});

test("a heavy multi-table workload stays within the SAH pool without exhausting capacity", async ({ page }) => {
  // dynoxide keeps every DynamoDB table and index inside one SQLite database
  // file, so the SAH pool's slots are consumed by that file, its rollback
  // journal and SQLite temp files, not by table count. This drives a realistic
  // load (several tables, many items, a scan each) to prove the chosen
  // initialCapacity is adequate and the pool never surfaces a spurious failure.
  const result = await page.evaluate(async () => {
    const client = globalThis.dynoxide.makeClient({ name: `cap-${crypto.randomUUID()}` });
    await client.ready();
    let created = 0;
    let total = 0;
    for (let t = 0; t < 6; t += 1) {
      const TableName = `Cap${t}`;
      await client.execute("CreateTable", {
        TableName,
        KeySchema: [
          { AttributeName: "pk", KeyType: "HASH" },
          { AttributeName: "sk", KeyType: "RANGE" },
        ],
        AttributeDefinitions: [
          { AttributeName: "pk", AttributeType: "S" },
          { AttributeName: "sk", AttributeType: "S" },
        ],
        BillingMode: "PAY_PER_REQUEST",
      });
      created += 1;
      for (let i = 0; i < 20; i += 1) {
        await client.execute("PutItem", {
          TableName,
          Item: { pk: { S: `p${i % 5}` }, sk: { S: `s${i}` } },
        });
      }
      total += (await client.execute("Scan", { TableName })).Count;
    }
    client.terminate();
    return { created, total };
  });

  expect(result.created).toBe(6);
  expect(result.total).toBe(120); // 6 tables x 20 items, all readable: no capacity failure
});
