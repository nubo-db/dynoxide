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
});
