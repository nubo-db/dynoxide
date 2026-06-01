import { test } from "node:test";
import assert from "node:assert/strict";

import { EngineClient, EngineError } from "./engine-client.js";
import { makeStubWorker } from "./test-support/stub-worker.js";

// A minimal table the stub worker understands (HASH + RANGE on string keys).
const TABLE = {
  TableName: "Widgets",
  KeySchema: [
    { AttributeName: "pk", KeyType: "HASH" },
    { AttributeName: "sk", KeyType: "RANGE" },
  ],
  AttributeDefinitions: [
    { AttributeName: "pk", AttributeType: "S" },
    { AttributeName: "sk", AttributeType: "S" },
  ],
  BillingMode: "PAY_PER_REQUEST",
};

const put = (client, pk, sk) =>
  client.execute("PutItem", { TableName: TABLE.TableName, Item: { pk: { S: pk }, sk: { S: sk } } });

const scanCount = async (client) =>
  (await client.execute("Scan", { TableName: TABLE.TableName })).Count;

function clientWith(opts = {}) {
  let worker;
  const client = new EngineClient({
    createWorker: () => {
      worker = makeStubWorker(opts.stub);
      return worker;
    },
    ...opts.client,
  });
  return { client, getWorker: () => worker };
}

test("boots, reports the contract, capabilities, and persistence mode", async () => {
  const { client } = clientWith();
  const descriptor = await client.ready();
  assert.equal(descriptor.contractVersion, 1);
  assert.equal(client.persistenceMode, "opfs");
  assert.equal(client.persistent, true);
  assert.ok(client.supports("Query"));
  assert.equal(client.supports("UpdateTimeToLive"), false);
  client.terminate();
});

test("CreateTable then PutItem then Query surfaces the items", async () => {
  const { client } = clientWith();
  await client.execute("CreateTable", TABLE);
  await put(client, "u#1", "a");
  await put(client, "u#1", "b");
  const res = await client.execute("Query", {
    TableName: TABLE.TableName,
    KeyConditionExpression: "pk = :pk",
    ExpressionAttributeValues: { ":pk": { S: "u#1" } },
  });
  assert.equal(res.Count, 2);
  client.terminate();
});

test("state survives a simulated reload against the same store", async () => {
  // A shared backing store stands in for OPFS persistence across reload.
  const store = new Map();
  const first = new EngineClient({ createWorker: () => makeStubWorker({ store }) });
  await first.execute("CreateTable", TABLE);
  await put(first, "u#1", "a");
  first.terminate();

  const second = new EngineClient({ createWorker: () => makeStubWorker({ store }) });
  await second.ready();
  assert.equal(await scanCount(second), 1);
  second.terminate();
});

test("a request issued before boot completes is queued and resolves once ready", async () => {
  // Defer the open reply, then issue execute immediately: it must wait for boot
  // rather than racing ahead or dropping.
  const { client } = clientWith({ stub: { openDelay: 25 } });
  const created = client.execute("CreateTable", TABLE); // issued while boot is still pending
  assert.equal(client.contractVersion, null); // not booted yet
  await created;
  assert.equal(client.contractVersion, 1);
  assert.equal(await scanCount(client), 0);
  client.terminate();
});

test("fallback (memory) mode reports non-persistent and still serves operations", async () => {
  const { client } = clientWith({ stub: { persistenceMode: "memory" } });
  await client.ready();
  assert.equal(client.persistenceMode, "memory");
  assert.equal(client.persistent, false);
  await client.execute("CreateTable", TABLE);
  await put(client, "u#1", "a");
  assert.equal(await scanCount(client), 1);
  client.terminate();
});

test("an engine error envelope surfaces as a typed EngineError", async () => {
  const { client } = clientWith();
  await client.execute("CreateTable", TABLE);
  await assert.rejects(
    () => client.execute("CreateTable", TABLE), // table already exists
    (e) => {
      assert.ok(e instanceof EngineError);
      assert.match(e.type, /ResourceInUseException/);
      return true;
    },
  );
  client.terminate();
});

test("a contract-version mismatch surfaces a clear boot error", async () => {
  const { client } = clientWith({ stub: { contractVersion: 999 } });
  await assert.rejects(
    () => client.ready(),
    (e) => {
      assert.ok(e instanceof EngineError);
      assert.match(e.type, /ContractMismatch/);
      assert.match(e.message, /contract mismatch/);
      assert.match(e.message, /999/);
      return true;
    },
  );
  client.terminate();
});

test("a worker crash rejects boot rather than hanging forever", async () => {
  const { client, getWorker } = clientWith({ stub: { openDelay: 50 } });
  getWorker().__fireError("wasm trap");
  await assert.rejects(() => client.ready(), (e) => e instanceof EngineError && /worker error|wasm trap/.test(e.message));
  client.terminate();
});

test("a worker crash AFTER boot rejects the next call instead of hanging", async () => {
  const { client, getWorker } = clientWith();
  await client.ready(); // boot succeeds; #pending is now empty
  getWorker().__fireError("wasm trap after boot");
  await assert.rejects(
    () => client.execute("Scan", { TableName: TABLE.TableName }),
    (e) => e instanceof EngineError && /worker error|wasm trap/.test(e.message),
  );
  client.terminate();
});

test("a worker messageerror rejects in-flight calls instead of hanging", async () => {
  const { client, getWorker } = clientWith({ stub: { openDelay: 50 } });
  getWorker().__fireMessageError(); // fires while boot is still pending
  await assert.rejects(
    () => client.ready(),
    (e) => e instanceof EngineError && /messageerror/.test(e.message),
  );
  client.terminate();
});

test("execute after terminate rejects fast instead of posting to a dead worker", async () => {
  const { client } = clientWith();
  await client.ready();
  client.terminate();
  await assert.rejects(
    () => client.execute("Scan", { TableName: "X" }),
    (e) => e instanceof EngineError && /terminated/.test(e.message),
  );
});
