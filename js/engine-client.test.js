import { test } from "node:test";
import assert from "node:assert/strict";

import { EngineClient, EngineError, CONTRACT_VERSION } from "./engine-client.js";
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

test("a body-less op defaults its request to {} rather than sending undefined", async () => {
  // ListTables and other no-body ops carry no request. Sent as undefined, the
  // worker stringifies it and the engine rejects the result as a
  // SerializationException - a confusing error for a request that wasn't
  // malformed, there just wasn't one. The client defaults a missing body to {}.
  let captured;
  const recordingWorker = () => {
    const listeners = new Set();
    const reply = (id, result) => {
      queueMicrotask(() => {
        for (const l of listeners) l({ data: { id, ok: true, result } });
      });
    };
    return {
      addEventListener(type, h) {
        if (type === "message") listeners.add(h);
      },
      removeEventListener(type, h) {
        listeners.delete(h);
      },
      postMessage(msg) {
        if (msg.op === "open") {
          reply(msg.id, JSON.stringify({ contractVersion: 1, capabilities: ["ListTables"], persistenceMode: "memory" }));
        } else if (msg.op === "execute") {
          captured = msg.payload;
          reply(msg.id, JSON.stringify({ TableNames: [] }));
        }
      },
      terminate() {
        listeners.clear();
      },
    };
  };

  const client = new EngineClient({ createWorker: recordingWorker });
  const res = await client.execute("ListTables");
  assert.deepEqual(captured.request, {}, "body-less request must default to {}");
  assert.deepEqual(res, { TableNames: [] });
  client.terminate();
});

test("the public surface declared in engine-client.d.ts exists at runtime", () => {
  // A dependency-free guard that the hand-written .d.ts has not drifted from the
  // exports it describes. It pins export names and kinds, not TypeScript types -
  // a deeper check would need a tsc toolchain we deliberately don't depend on.
  assert.equal(typeof CONTRACT_VERSION, "number");
  for (const method of ["ready", "execute", "dispatchHttp", "supports", "terminate"]) {
    assert.equal(typeof EngineClient.prototype[method], "function", `EngineClient.${method}`);
  }
  assert.ok(
    Object.getOwnPropertyDescriptor(EngineClient.prototype, "persistent")?.get,
    "EngineClient.persistent getter",
  );
  const err = new EngineError(JSON.stringify({ __type: "X", message: "m" }));
  assert.ok(err instanceof Error);
  assert.equal(err.type, "X");
  assert.equal(typeof err.envelope, "string");
});

test("dispatchHttp returns the status and body for the transport to write", async () => {
  const client = new EngineClient({ createWorker: () => makeStubWorker() });

  const created = await client.dispatchHttp(
    "DynamoDB_20120810.CreateTable",
    JSON.stringify(TABLE),
  );
  assert.equal(created.status, 200);

  const listed = await client.dispatchHttp("DynamoDB_20120810.ListTables", "{}");
  assert.equal(listed.status, 200);
  assert.deepEqual(JSON.parse(listed.body), { TableNames: ["Widgets"] });

  client.terminate();
});

test("dispatchHttp reports a protocol rejection as a status, not a thrown error", async () => {
  // The transport writes whatever comes back. A rejection here would force it
  // to reimplement the mapping from error to status, which is the split this
  // path exists to avoid.
  const client = new EngineClient({ createWorker: () => makeStubWorker() });

  const noTarget = await client.dispatchHttp(null, "{}");
  assert.equal(noTarget.status, 400);
  assert.match(JSON.parse(noTarget.body).__type, /UnknownOperationException$/);

  const badBody = await client.dispatchHttp("DynamoDB_20120810.ListTables", "not json");
  assert.equal(badBody.status, 400);
  assert.match(JSON.parse(badBody.body).__type, /SerializationException$/);

  client.terminate();
});

test("dispatchHttp returns 501 for an operation the engine does not implement", async () => {
  // 501 is what makes the conformance suite score the preview's unimplemented
  // surface as skipped rather than failed.
  const client = new EngineClient({ createWorker: () => makeStubWorker() });

  const out = await client.dispatchHttp("DynamoDB_20120810.UpdateTimeToLive", "{}");
  assert.equal(out.status, 501);
  assert.match(JSON.parse(out.body).message, /is not supported/i);

  client.terminate();
});
