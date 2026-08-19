import { test, expect } from "@playwright/test";

// End-to-end tests of the shipped wasm engine in a real browser: the bundled
// Worker, both .wasm, the SQLite-wasm engine and OPFS. This is the path the conformance
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

test("a vector index survives a reload and is still searchable from OPFS", async ({ page }) => {
  // The migration risk in one test: the vector definitions live in a column
  // added to _tables by an ALTER on open, and the shadow rows live in their own
  // table. Both have to come back after the handles are released and retaken,
  // or a browser database created today stops answering tomorrow.
  const name = `vec-${Date.now()}-${Math.floor(Math.random() * 1e6)}`;
  const VEC = {
    TableName: "Vecs",
    KeySchema: [{ AttributeName: "pk", KeyType: "HASH" }],
    AttributeDefinitions: [{ AttributeName: "pk", AttributeType: "S" }],
    BillingMode: "PAY_PER_REQUEST",
    VectorIndexes: [
      {
        IndexName: "vix",
        VectorAttribute: { AttributeName: "emb" },
        Projection: { ProjectionType: "ALL" },
        Dimensions: 3,
        DistanceFunction: "COSINE",
      },
    ],
  };

  const first = await page.evaluate(async ({ name, table }) => {
    const client = globalThis.dynoxide.makeClient({ name });
    await client.ready();
    await client.execute("CreateTable", table);
    for (const [pk, emb] of [["a", ["1", "0", "0"]], ["b", ["0", "1", "0"]]]) {
      await client.execute("PutItem", {
        TableName: table.TableName,
        Item: { pk: { S: pk }, emb: { L: emb.map((n) => ({ N: n })) } },
      });
    }
    const mode = client.persistenceMode;
    client.terminate();
    return mode;
  }, { name, table: VEC });
  expect(first).toBe("opfs");

  await page.waitForTimeout(150);

  const reopened = await page.evaluate(async ({ name, table }) => {
    const client = globalThis.dynoxide.makeClient({ name });
    await client.ready();
    const described = await client.execute("DescribeTable", {
      TableName: table.TableName,
    });
    const search = await client.execute("SearchVectors", {
      TableName: table.TableName,
      IndexName: "vix",
      SearchVector: [{ N: "1" }, { N: "0" }, { N: "0" }],
      TopK: 2,
    });
    const out = {
      mode: client.persistenceMode,
      indexName: described.Table.VectorIndexes?.[0]?.IndexName,
      indexStatus: described.Table.VectorIndexes?.[0]?.IndexStatus,
      top: search.SearchResults?.[0]?.Item?.pk?.S,
      topScore: search.SearchResults?.[0]?.Score,
      returned: search.SearchResults?.length,
    };
    client.terminate();
    return out;
  }, { name, table: VEC });

  expect(reopened.mode).toBe("opfs");
  expect(reopened.indexName).toBe("vix");
  expect(reopened.indexStatus).toBe("ACTIVE");
  // The shadow rows survived, so the search still ranks both entries and the
  // self match is still exact (COSINE is a distance, so 0).
  expect(reopened.returned).toBe(2);
  expect(reopened.top).toBe("a");
  expect(reopened.topScore).toBe(0);
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

// --- UpdateTable on the wasm preview ---------------------------------------

test("UpdateTable adds a GSI to a populated table and backfills the existing rows (OPFS)", async ({ page }) => {
  const result = await page.evaluate(async (table) => {
    const client = globalThis.dynoxide.makeClient({ name: `addgsi-${crypto.randomUUID()}` });
    await client.ready();
    await client.execute("CreateTable", table);
    for (const [song, genre] of [["s1", "rock"], ["s2", "jazz"], ["s3", "rock"]]) {
      await client.execute("PutItem", {
        TableName: table.TableName,
        Item: { artist: { S: "a" }, song: { S: song }, genre: { S: genre } },
      });
    }
    // The rows exist before the index does: adding the GSI must backfill them.
    await client.execute("UpdateTable", {
      TableName: table.TableName,
      AttributeDefinitions: [
        { AttributeName: "artist", AttributeType: "S" },
        { AttributeName: "song", AttributeType: "S" },
        { AttributeName: "genre", AttributeType: "S" },
      ],
      GlobalSecondaryIndexUpdates: [
        {
          Create: {
            IndexName: "GenreIndex",
            KeySchema: [{ AttributeName: "genre", KeyType: "HASH" }],
            Projection: { ProjectionType: "ALL" },
          },
        },
      ],
    });
    const q = await client.execute("Query", {
      TableName: table.TableName,
      IndexName: "GenreIndex",
      KeyConditionExpression: "genre = :g",
      ExpressionAttributeValues: { ":g": { S: "rock" } },
    });
    // The new index reports its status: dynoxide creates a GSI synchronously, so
    // it is immediately ACTIVE rather than transitioning through CREATING.
    const desc = await client.execute("DescribeTable", { TableName: table.TableName });
    const gsi = (desc.Table.GlobalSecondaryIndexes || []).find((g) => g.IndexName === "GenreIndex");
    const out = {
      mode: client.persistenceMode,
      count: q.Count,
      songs: q.Items.map((i) => i.song.S).sort(),
      indexStatus: gsi && gsi.IndexStatus,
    };
    client.terminate();
    return out;
  }, MUSIC);

  expect(result.mode).toBe("opfs");
  expect(result.count).toBe(2); // s1 and s3 were backfilled into the new index
  expect(result.songs).toEqual(["s1", "s3"]);
  // A freshly added GSI is synchronously ACTIVE, a deliberate preview divergence
  // from AWS, where the index is CREATING with a background backfill first.
  expect(result.indexStatus).toBe("ACTIVE");
});

test("UpdateTable deletes a GSI; the index stops answering and the base table survives", async ({ page }) => {
  const result = await page.evaluate(async () => {
    const client = globalThis.dynoxide.makeClient({ name: `delgsi-${crypto.randomUUID()}` });
    await client.ready();
    await client.execute("CreateTable", {
      TableName: "Music",
      KeySchema: [
        { AttributeName: "artist", KeyType: "HASH" },
        { AttributeName: "song", KeyType: "RANGE" },
      ],
      AttributeDefinitions: [
        { AttributeName: "artist", AttributeType: "S" },
        { AttributeName: "song", AttributeType: "S" },
        { AttributeName: "genre", AttributeType: "S" },
      ],
      GlobalSecondaryIndexes: [
        {
          IndexName: "GenreIndex",
          KeySchema: [{ AttributeName: "genre", KeyType: "HASH" }],
          Projection: { ProjectionType: "ALL" },
        },
      ],
      BillingMode: "PAY_PER_REQUEST",
    });
    await client.execute("PutItem", {
      TableName: "Music",
      Item: { artist: { S: "a" }, song: { S: "s1" }, genre: { S: "rock" } },
    });
    const queryGenre = () =>
      client.execute("Query", {
        TableName: "Music",
        IndexName: "GenreIndex",
        KeyConditionExpression: "genre = :g",
        ExpressionAttributeValues: { ":g": { S: "rock" } },
      });
    const before = await queryGenre();
    await client.execute("UpdateTable", {
      TableName: "Music",
      GlobalSecondaryIndexUpdates: [{ Delete: { IndexName: "GenreIndex" } }],
    });
    const baseScan = await client.execute("Scan", { TableName: "Music" });
    let indexErr = null;
    try {
      await queryGenre();
    } catch (e) {
      indexErr = e.message;
    }
    const out = { beforeCount: before.Count, baseCount: baseScan.Count, indexErr };
    client.terminate();
    return out;
  });

  expect(result.beforeCount).toBe(1);
  expect(result.baseCount).toBe(1); // the base table survives the GSI delete
  expect(result.indexErr).not.toBeNull(); // the deleted index no longer answers
});

test("UpdateTable persists deletion protection and table class, reflected by DescribeTable", async ({ page }) => {
  // Two distinct table-setting behaviours: a boolean flag and an enum class.
  const result = await page.evaluate(async (table) => {
    const client = globalThis.dynoxide.makeClient({ name: `setters-${crypto.randomUUID()}` });
    await client.ready();
    await client.execute("CreateTable", table);
    await client.execute("UpdateTable", { TableName: table.TableName, DeletionProtectionEnabled: true });
    await client.execute("UpdateTable", {
      TableName: table.TableName,
      TableClass: "STANDARD_INFREQUENT_ACCESS",
    });
    const d = await client.execute("DescribeTable", { TableName: table.TableName });
    const out = {
      deletionProtection: d.Table.DeletionProtectionEnabled,
      tableClass: d.Table.TableClassSummary && d.Table.TableClassSummary.TableClass,
    };
    client.terminate();
    return out;
  }, MUSIC);

  expect(result.deletionProtection).toBe(true);
  expect(result.tableClass).toBe("STANDARD_INFREQUENT_ACCESS");
});

test("UpdateTable switches billing mode to provisioned with its throughput, reflected by DescribeTable", async ({ page }) => {
  // The interrelated setter group: switching to PROVISIONED carries throughput,
  // which the shared handler validates and DescribeTable then reflects.
  const result = await page.evaluate(async (table) => {
    const client = globalThis.dynoxide.makeClient({ name: `billing-${crypto.randomUUID()}` });
    await client.ready();
    await client.execute("CreateTable", table); // PAY_PER_REQUEST
    await client.execute("UpdateTable", {
      TableName: table.TableName,
      BillingMode: "PROVISIONED",
      ProvisionedThroughput: { ReadCapacityUnits: 5, WriteCapacityUnits: 5 },
    });
    const d = await client.execute("DescribeTable", { TableName: table.TableName });
    const out = {
      // BillingModeSummary is absent for PROVISIONED tables (matches AWS and the
      // native conformance suite); the persisted switch shows up as the
      // provisioned throughput now being present with the values we set.
      hasBillingModeSummary: !!d.Table.BillingModeSummary,
      rcu: d.Table.ProvisionedThroughput && d.Table.ProvisionedThroughput.ReadCapacityUnits,
      wcu: d.Table.ProvisionedThroughput && d.Table.ProvisionedThroughput.WriteCapacityUnits,
    };
    client.terminate();
    return out;
  }, MUSIC);

  expect(result.hasBillingModeSummary).toBe(false); // absent for PROVISIONED, per AWS and conformance
  expect(result.rcu).toBe(5); // the switch persisted: provisioned throughput now reflects it
  expect(result.wcu).toBe(5);
});

test("UpdateTable with a StreamSpecification is refused and leaves the table intact", async ({ page }) => {
  // Streams are a deeper preview gap on wasm, so a stream-spec change must be
  // refused with a typed error and roll back rather than half-apply.
  const result = await page.evaluate(async (table) => {
    const client = globalThis.dynoxide.makeClient({ name: `stream-${crypto.randomUUID()}` });
    await client.ready();
    await client.execute("CreateTable", table);
    await client.execute("PutItem", {
      TableName: table.TableName,
      Item: { artist: { S: "a" }, song: { S: "s1" } },
    });
    let err = null;
    try {
      await client.execute("UpdateTable", {
        TableName: table.TableName,
        StreamSpecification: { StreamEnabled: true, StreamViewType: "NEW_AND_OLD_IMAGES" },
      });
    } catch (e) {
      err = { type: e.type, message: e.message };
    }
    const scan = await client.execute("Scan", { TableName: table.TableName });
    const d = await client.execute("DescribeTable", { TableName: table.TableName });
    const out = {
      err,
      count: scan.Count,
      streamEnabled: (d.Table.StreamSpecification && d.Table.StreamSpecification.StreamEnabled) || false,
    };
    client.terminate();
    return out;
  }, MUSIC);

  expect(result.err).not.toBeNull();
  expect(result.err.message).toMatch(/not supported|streams/i);
  expect(result.count).toBe(1); // the table survived the refused update
  expect(result.streamEnabled).toBe(false); // streams were not enabled
});

test("a GSI added to a populated table survives a reload (OPFS)", async ({ page }) => {
  const name = `addgsi-persist-${Date.now()}-${Math.floor(Math.random() * 1e6)}`;

  await page.evaluate(async ({ name, table }) => {
    const client = globalThis.dynoxide.makeClient({ name });
    await client.ready();
    await client.execute("CreateTable", table);
    await client.execute("PutItem", {
      TableName: table.TableName,
      Item: { artist: { S: "a" }, song: { S: "s1" }, genre: { S: "rock" } },
    });
    await client.execute("UpdateTable", {
      TableName: table.TableName,
      AttributeDefinitions: [
        { AttributeName: "artist", AttributeType: "S" },
        { AttributeName: "song", AttributeType: "S" },
        { AttributeName: "genre", AttributeType: "S" },
      ],
      GlobalSecondaryIndexUpdates: [
        {
          Create: {
            IndexName: "GenreIndex",
            KeySchema: [{ AttributeName: "genre", KeyType: "HASH" }],
            Projection: { ProjectionType: "ALL" },
          },
        },
      ],
    });
    client.terminate();
  }, { name, table: MUSIC });

  await page.waitForTimeout(150);

  const reopened = await page.evaluate(async ({ name }) => {
    const client = globalThis.dynoxide.makeClient({ name });
    await client.ready();
    const q = await client.execute("Query", {
      TableName: "Music",
      IndexName: "GenreIndex",
      KeyConditionExpression: "genre = :g",
      ExpressionAttributeValues: { ":g": { S: "rock" } },
    });
    const out = { mode: client.persistenceMode, count: q.Count };
    client.terminate();
    return out;
  }, { name });

  expect(reopened.mode).toBe("opfs");
  expect(reopened.count).toBe(1); // the backfilled index persisted across reload
});

test("an overwrite and a delete keep GSI and LSI in step with the fan-out batched over the bridge", async ({ page }) => {
  // Each indexed write and delete maintains its GSI and LSI through one batched
  // bridge crossing (apply_index_writes -> exec_script). This proves the batched
  // delete-then-insert (overwrite) and the batched delete-only (delete) fan-outs
  // are correct end to end on OPFS, not just in the Node bridge test.
  const result = await page.evaluate(async () => {
    const client = globalThis.dynoxide.makeClient({ name: `fanout-${crypto.randomUUID()}` });
    await client.ready();
    await client.execute("CreateTable", {
      TableName: "Fanout",
      KeySchema: [
        { AttributeName: "pk", KeyType: "HASH" },
        { AttributeName: "sk", KeyType: "RANGE" },
      ],
      AttributeDefinitions: [
        { AttributeName: "pk", AttributeType: "S" },
        { AttributeName: "sk", AttributeType: "S" },
        { AttributeName: "gpk", AttributeType: "S" },
        { AttributeName: "lsk", AttributeType: "S" },
      ],
      GlobalSecondaryIndexes: [
        {
          IndexName: "byG",
          KeySchema: [{ AttributeName: "gpk", KeyType: "HASH" }],
          Projection: { ProjectionType: "ALL" },
        },
      ],
      LocalSecondaryIndexes: [
        {
          IndexName: "byL",
          KeySchema: [
            { AttributeName: "pk", KeyType: "HASH" },
            { AttributeName: "lsk", KeyType: "RANGE" },
          ],
          Projection: { ProjectionType: "ALL" },
        },
      ],
      BillingMode: "PAY_PER_REQUEST",
    });

    const put = (gpk, lsk) =>
      client.execute("PutItem", {
        TableName: "Fanout",
        Item: { pk: { S: "p1" }, sk: { S: "s1" }, gpk: { S: gpk }, lsk: { S: lsk } },
      });
    const queryGsi = (gpk) =>
      client.execute("Query", {
        TableName: "Fanout",
        IndexName: "byG",
        KeyConditionExpression: "gpk = :g",
        ExpressionAttributeValues: { ":g": { S: gpk } },
      });
    const queryLsi = () =>
      client.execute("Query", {
        TableName: "Fanout",
        IndexName: "byL",
        KeyConditionExpression: "pk = :p",
        ExpressionAttributeValues: { ":p": { S: "p1" } },
      });

    await put("g1", "l1");
    const g1Initial = (await queryGsi("g1")).Count;
    const lsiInitial = (await queryLsi()).Items.map((i) => i.lsk.S);

    // Overwrite with changed index keys: the batched delete-then-insert must
    // drop the stale index entries and write the new ones in one crossing.
    await put("g2", "l2");
    const g1AfterOverwrite = (await queryGsi("g1")).Count;
    const g2AfterOverwrite = (await queryGsi("g2")).Count;
    const lsiAfterOverwrite = (await queryLsi()).Items.map((i) => i.lsk.S);

    // Delete: the batched delete-only fan-out must clear both indexes.
    await client.execute("DeleteItem", {
      TableName: "Fanout",
      Key: { pk: { S: "p1" }, sk: { S: "s1" } },
    });
    const g2AfterDelete = (await queryGsi("g2")).Count;
    const lsiAfterDelete = (await queryLsi()).Count;

    const out = {
      mode: client.persistenceMode,
      g1Initial,
      lsiInitial,
      g1AfterOverwrite,
      g2AfterOverwrite,
      lsiAfterOverwrite,
      g2AfterDelete,
      lsiAfterDelete,
    };
    client.terminate();
    return out;
  });

  expect(result.mode).toBe("opfs");
  expect(result.g1Initial).toBe(1); // the put landed in the GSI
  expect(result.lsiInitial).toEqual(["l1"]); // and in the LSI
  expect(result.g1AfterOverwrite).toBe(0); // stale GSI entry deleted in the batch
  expect(result.g2AfterOverwrite).toBe(1); // new GSI entry inserted in the same batch
  expect(result.lsiAfterOverwrite).toEqual(["l2"]); // LSI re-pointed, no stale row left
  expect(result.g2AfterDelete).toBe(0); // delete-path fan-out cleared the GSI
  expect(result.lsiAfterDelete).toBe(0); // and the LSI
});

test("PartiQL round-trips against OPFS and survives a worker restart", async ({ page }) => {
  const result = await page.evaluate(async (table) => {
    const name = `partiql-${crypto.randomUUID()}`;
    const first = globalThis.dynoxide.makeClient({ name });
    await first.ready();
    await first.execute("CreateTable", table);

    const statement = async (Statement, Parameters) =>
      first.execute("ExecuteStatement", Parameters ? { Statement, Parameters } : { Statement });

    await statement(
      `INSERT INTO "Music" VALUE {'artist': ?, 'song': ?, 'plays': 1}`,
      [{ S: "a" }, { S: "s1" }],
    );
    await statement(`UPDATE "Music" SET plays = 4 WHERE artist = 'a' AND song = 's1'`);
    const selected = await statement(`SELECT * FROM "Music" WHERE artist = 'a' AND song = 's1'`);

    const capabilities = first.capabilities;
    first.terminate();

    // A second worker on the same OPFS database must see the committed rows.
    const second = globalThis.dynoxide.makeClient({ name });
    await second.ready();
    const afterRestart = await second.execute(
      "ExecuteStatement",
      { Statement: `SELECT * FROM "Music" WHERE artist = 'a' AND song = 's1'` },
    );
    await second.execute("ExecuteStatement", {
      Statement: `DELETE FROM "Music" WHERE artist = 'a' AND song = 's1'`,
    });
    const afterDelete = await second.execute("ExecuteStatement", {
      Statement: `SELECT * FROM "Music" WHERE artist = 'a' AND song = 's1'`,
    });
    second.terminate();

    return {
      plays: selected.Items[0].plays.N,
      playsAfterRestart: afterRestart.Items[0].plays.N,
      remaining: afterDelete.Items.length,
      capabilities,
    };
  }, MUSIC);

  expect(result.plays).toBe("4");
  expect(result.playsAfterRestart).toBe("4"); // the OPFS commit survived the restart
  expect(result.remaining).toBe(0);
  // Positive feature detection: a client hides what the engine does not list.
  expect(result.capabilities).toEqual(
    expect.arrayContaining(["ExecuteStatement", "BatchExecuteStatement", "ExecuteTransaction"]),
  );
});

test("a cancelled PartiQL transaction unwinds every statement it had applied", async ({ page }) => {
  const result = await page.evaluate(async (table) => {
    const client = globalThis.dynoxide.makeClient({ name: `txn-${crypto.randomUUID()}` });
    await client.ready();
    await client.execute("CreateTable", table);
    await client.execute("ExecuteStatement", {
      Statement: `INSERT INTO "Music" VALUE {'artist': 'a', 'song': 'taken'}`,
    });

    // The second statement collides, so the first must not survive. Proves the
    // bridge's ROLLBACK genuinely unwinds a write already made against OPFS.
    let cancelled = null;
    try {
      await client.execute("ExecuteTransaction", {
        TransactStatements: [
          { Statement: `INSERT INTO "Music" VALUE {'artist': 'a', 'song': 'fresh'}` },
          { Statement: `INSERT INTO "Music" VALUE {'artist': 'a', 'song': 'taken'}` },
        ],
      });
    } catch (e) {
      cancelled = { type: e.type, reasons: JSON.parse(e.envelope).CancellationReasons };
    }

    const survivors = await client.execute("ExecuteStatement", {
      Statement: `SELECT * FROM "Music" WHERE artist = 'a' AND song = 'fresh'`,
    });
    client.terminate();
    return { cancelled, survivors: survivors.Items.length };
  }, MUSIC);

  expect(result.cancelled).not.toBeNull();
  expect(result.cancelled.type).toMatch(/TransactionCanceledException$/);
  // Per-statement reasons keep their positions: the collision is the second.
  expect(result.cancelled.reasons.map((r) => r.Code)).toEqual(["None", "DuplicateItem"]);
  expect(result.survivors).toBe(0);
});

test("a repeated ClientRequestToken does not apply the transaction twice", async ({ page }) => {
  // Guard-scope dependent by design: the engine holds the backend lock for a
  // whole dispatch, so the second call queues and replays the first result. If
  // that scope is ever narrowed this test moves, because the second call would
  // instead meet a live claim in the idempotency cache.
  const result = await page.evaluate(async (table) => {
    const client = globalThis.dynoxide.makeClient({ name: `token-${crypto.randomUUID()}` });
    await client.ready();
    await client.execute("CreateTable", table);

    // A statement that cannot succeed twice, so a double-apply is observable
    // as a cancellation rather than needing a counter the page cannot see.
    const request = {
      TransactStatements: [
        { Statement: `INSERT INTO "Music" VALUE {'artist': 'a', 'song': 'once'}` },
      ],
      ClientRequestToken: "same-token",
    };

    // Fire both without awaiting the first, so they overlap in the worker.
    const settled = await Promise.allSettled([
      client.execute("ExecuteTransaction", request),
      client.execute("ExecuteTransaction", request),
    ]);

    // Control: the same statements under a different token must collide, which
    // is what shows the token is the reason the pair above did not.
    let control = null;
    try {
      await client.execute("ExecuteTransaction", {
        ...request,
        ClientRequestToken: "other-token",
      });
    } catch (e) {
      control = e.type;
    }

    const rows = await client.execute("ExecuteStatement", {
      Statement: `SELECT * FROM "Music" WHERE artist = 'a' AND song = 'once'`,
    });
    client.terminate();
    return {
      statuses: settled.map((s) => s.status),
      values: settled.map((s) => s.value),
      control,
      rows: rows.Items.length,
    };
  }, MUSIC);

  expect(result.statuses).toEqual(["fulfilled", "fulfilled"]);
  // The replay is the first call's response, not an empty stand-in.
  expect(result.values[1]).toEqual(result.values[0]);
  expect(result.control).toMatch(/TransactionCanceledException$/);
  expect(result.rows).toBe(1);
});
