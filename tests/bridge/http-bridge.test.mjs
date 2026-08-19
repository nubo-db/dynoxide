/**
 * End-to-end test for the wasm HTTP bridge.
 *
 * Starts `js/wasm-http-bridge.mjs` for real and drives it over a socket, so it
 * covers the whole path the conformance suite takes: HTTP in, headless browser,
 * Worker, wasm engine, and back. The Rust tests in src/wasm_api.rs pin the
 * envelope rules; this pins that they survive the transport.
 *
 * Needs a built bundle and a browser, so it is not part of `npm test`:
 *   npm run build:wasm:dev && npx playwright install chromium
 *   npm run test:bridge
 */
import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const repoRoot = join(dirname(fileURLToPath(import.meta.url)), "..", "..");

// Away from the documented 8003/8004 so a bridge left running by hand does not
// silently satisfy this test, and so the test does not evict one.
const PORT = 8103;
const ASSET_PORT = 8104;
const BASE = `http://127.0.0.1:${PORT}/`;

// A well-formed SigV4 header, as the AWS SDK always sends. Auth material is
// validated but never signature-verified, so the values are arbitrary.
const SIGNED =
  "AWS4-HMAC-SHA256 Credential=fake/20260724/eu-west-2/dynamodb/aws4_request, " +
  "SignedHeaders=host;x-amz-date, Signature=abc";

let bridge;

/** POST one DynamoDB request, returning the status and parsed body. */
async function call(target, body = "{}", { signed = true } = {}) {
  const res = await fetch(BASE, {
    method: "POST",
    headers: {
      "content-type": "application/x-amz-json-1.0",
      ...(target === null ? {} : { "x-amz-target": target }),
      ...(signed ? { authorization: SIGNED, "x-amz-date": "20260724T000000Z" } : {}),
    },
    body,
  });
  const text = await res.text();
  let parsed;
  try {
    parsed = JSON.parse(text);
  } catch {
    parsed = text;
  }
  return { status: res.status, body: parsed, contentType: res.headers.get("content-type") };
}

before(async () => {
  bridge = spawn(
    "node",
    [
      join(repoRoot, "js", "wasm-http-bridge.mjs"),
      "--port", String(PORT),
      "--asset-port", String(ASSET_PORT),
    ],
    { cwd: repoRoot, stdio: ["ignore", "pipe", "pipe"] },
  );
  bridge.stderr.on("data", (d) => process.stderr.write(`[bridge] ${d}`));

  // Wait for the listening line rather than sleeping: a cold browser start is
  // far slower than a warm one, and a fixed sleep would either flake or waste.
  await new Promise((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error("bridge did not start within 120s")),
      120_000,
    );
    bridge.on("exit", (code) => {
      clearTimeout(timer);
      reject(new Error(`bridge exited early with code ${code}`));
    });
    bridge.stdout.on("data", (d) => {
      process.stdout.write(`[bridge] ${d}`);
      if (String(d).includes("wasm engine on")) {
        clearTimeout(timer);
        resolve();
      }
    });
  });
}, { timeout: 180_000 });

after(() => {
  bridge?.kill();
});

test("a supported operation round-trips through the browser to the engine", async () => {
  const created = await call(
    "DynamoDB_20120810.CreateTable",
    JSON.stringify({
      TableName: "Bridge",
      KeySchema: [{ AttributeName: "pk", KeyType: "HASH" }],
      AttributeDefinitions: [{ AttributeName: "pk", AttributeType: "S" }],
      BillingMode: "PAY_PER_REQUEST",
    }),
  );
  assert.equal(created.status, 200);
  assert.match(created.contentType, /application\/x-amz-json-1\.0/);

  const put = await call(
    "DynamoDB_20120810.PutItem",
    JSON.stringify({ TableName: "Bridge", Item: { pk: { S: "a" }, v: { N: "1" } } }),
  );
  assert.equal(put.status, 200);

  const got = await call(
    "DynamoDB_20120810.GetItem",
    JSON.stringify({ TableName: "Bridge", Key: { pk: { S: "a" } } }),
  );
  assert.equal(got.status, 200);
  assert.deepEqual(got.body.Item, { pk: { S: "a" }, v: { N: "1" } });
});

test("the vector family survives the transport, control plane and data plane", async () => {
  // The whole point of the unit: an index the control plane advertises must be
  // one the data plane can answer, over the same transport the suite uses.
  const created = await call(
    "DynamoDB_20120810.CreateTable",
    JSON.stringify({
      TableName: "BridgeVec",
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
    }),
  );
  assert.equal(created.status, 200);

  const described = await call(
    "DynamoDB_20120810.DescribeTable",
    JSON.stringify({ TableName: "BridgeVec" }),
  );
  assert.equal(described.status, 200);
  assert.equal(described.body.Table.VectorIndexes[0].IndexName, "vix");
  assert.equal(described.body.Table.VectorIndexes[0].IndexStatus, "ACTIVE");

  for (const [pk, emb] of [
    ["a", ["1", "0", "0"]],
    ["b", ["0", "1", "0"]],
  ]) {
    const put = await call(
      "DynamoDB_20120810.PutItem",
      JSON.stringify({
        TableName: "BridgeVec",
        Item: { pk: { S: pk }, emb: { L: emb.map((n) => ({ N: n })) } },
      }),
    );
    assert.equal(put.status, 200);
  }

  const searched = await call(
    "DynamoDB_20120810.SearchVectors",
    JSON.stringify({
      TableName: "BridgeVec",
      IndexName: "vix",
      SearchVector: [{ N: "1" }, { N: "0" }, { N: "0" }],
      TopK: 1,
    }),
  );
  assert.equal(searched.status, 200, JSON.stringify(searched.body));
  assert.equal(searched.body.SearchResults.length, 1);
  assert.equal(searched.body.SearchResults[0].Item.pk.S, "a");
  // COSINE is a distance, so the self match scores 0.
  assert.equal(searched.body.SearchResults[0].Score, 0);
  // The vector attribute is excluded from the projection by default.
  assert.equal(searched.body.SearchResults[0].Item.emb, undefined);
});

test("a search against a table with no vector index is a validation error, not a skip", async () => {
  const out = await call(
    "DynamoDB_20120810.SearchVectors",
    JSON.stringify({
      TableName: "Bridge",
      IndexName: "nope",
      SearchVector: [{ N: "1" }],
      TopK: 1,
    }),
  );
  assert.notEqual(out.status, 501, "the operation is implemented, so this is not scope");
  assert.equal(out.status, 400);
});

test("an unimplemented operation is a 501 the suite scores as a skip", async () => {
  // The load-bearing case: without this the preview's whole unimplemented
  // surface lands in the failed column instead of being reported as scope.
  const out = await call("DynamoDB_20120810.UpdateTimeToLive");
  assert.equal(out.status, 501);
  assert.match(
    out.body.message,
    /unknown operation|not implemented|unsupported operation|is not supported/i,
    "message must match the conformance suite's isUnsupportedFault regex",
  );
});

test("a stream-enabled CreateTable is refused before the table exists", async () => {
  // The refusal must come before anything is created. A refusal after the
  // create would half-apply: the SDK retries the error, the retry finds the
  // table, and the caller sees ResourceInUseException where the real answer
  // is a capability gap - exactly how the conformance suite's streams probe
  // used to land in the failed column.
  const spec = {
    TableName: "Streamed",
    KeySchema: [{ AttributeName: "pk", KeyType: "HASH" }],
    AttributeDefinitions: [{ AttributeName: "pk", AttributeType: "S" }],
    BillingMode: "PAY_PER_REQUEST",
  };
  const refused = await call(
    "DynamoDB_20120810.CreateTable",
    JSON.stringify({
      ...spec,
      StreamSpecification: { StreamEnabled: true, StreamViewType: "NEW_AND_OLD_IMAGES" },
    }),
  );
  assert.equal(refused.status, 501);
  assert.equal(refused.body.__type, "com.dynoxide.wasm#UnsupportedOperation");
  assert.match(
    refused.body.message,
    /unknown operation|not implemented|unsupported operation|is not supported/i,
    "message must match the conformance suite's isUnsupportedFault regex",
  );

  // A retry of the refused request gets the same answer, and the same name
  // without the stream succeeds: nothing was created by the refusal.
  const retried = await call(
    "DynamoDB_20120810.CreateTable",
    JSON.stringify({
      ...spec,
      StreamSpecification: { StreamEnabled: true, StreamViewType: "NEW_AND_OLD_IMAGES" },
    }),
  );
  assert.equal(retried.status, 501);
  const plain = await call("DynamoDB_20120810.CreateTable", JSON.stringify(spec));
  assert.equal(plain.status, 200);
  await call("DynamoDB_20120810.DeleteTable", JSON.stringify({ TableName: spec.TableName }));
});

test("a tagged CreateTable is refused whole, not created untagged", async () => {
  const spec = {
    TableName: "Tagged",
    KeySchema: [{ AttributeName: "pk", KeyType: "HASH" }],
    AttributeDefinitions: [{ AttributeName: "pk", AttributeType: "S" }],
    BillingMode: "PAY_PER_REQUEST",
  };
  const refused = await call(
    "DynamoDB_20120810.CreateTable",
    JSON.stringify({ ...spec, Tags: [{ Key: "team", Value: "data" }] }),
  );
  assert.equal(refused.status, 501);
  assert.equal(refused.body.__type, "com.dynoxide.wasm#UnsupportedOperation");
  const plain = await call("DynamoDB_20120810.CreateTable", JSON.stringify(spec));
  assert.equal(plain.status, 200);
  await call("DynamoDB_20120810.DeleteTable", JSON.stringify({ TableName: spec.TableName }));
});

test("PartiQL round-trips a statement and its result through the transport", async () => {
  // Its own table, so this runs standalone rather than only after the test
  // that happens to create "Bridge".
  const created = await call(
    "DynamoDB_20120810.CreateTable",
    JSON.stringify({
      TableName: "BridgePartiql",
      KeySchema: [{ AttributeName: "pk", KeyType: "HASH" }],
      AttributeDefinitions: [{ AttributeName: "pk", AttributeType: "S" }],
      BillingMode: "PAY_PER_REQUEST",
    }),
  );
  assert.equal(created.status, 200);

  // The Rust tests cover the statement grammar; this covers the statement and
  // its parameters surviving HTTP, the browser and the Worker intact.
  const inserted = await call(
    "DynamoDB_20120810.ExecuteStatement",
    JSON.stringify({
      Statement: `INSERT INTO "BridgePartiql" VALUE {'pk': ?, 'note': ?}`,
      Parameters: [{ S: "partiql" }, { S: "through the bridge" }],
    }),
  );
  assert.equal(inserted.status, 200);

  const selected = await call(
    "DynamoDB_20120810.ExecuteStatement",
    JSON.stringify({
      Statement: `SELECT * FROM "BridgePartiql" WHERE pk = ?`,
      Parameters: [{ S: "partiql" }],
    }),
  );
  assert.equal(selected.status, 200);
  assert.deepEqual(selected.body.Items, [
    { pk: { S: "partiql" }, note: { S: "through the bridge" } },
  ]);
});

test("a malformed PartiQL statement is a ValidationException, not a skip", async () => {
  const out = await call(
    "DynamoDB_20120810.ExecuteStatement",
    JSON.stringify({ Statement: "NOT A STATEMENT" }),
  );
  assert.equal(out.status, 400);
  assert.match(out.body.__type, /ValidationException$/);
  assert.match(out.body.message, /^Statement wasn't well formed, can't be processed: /);
});

test("a malformed body is a bare SerializationException", async () => {
  const out = await call("DynamoDB_20120810.ListTables", "not json");
  assert.equal(out.status, 400);
  assert.deepEqual(out.body, {
    __type: "com.amazon.coral.service#SerializationException",
  });
});

test("a missing target is a bare UnknownOperationException", async () => {
  const out = await call(null, "{}");
  assert.equal(out.status, 400);
  assert.deepEqual(out.body, {
    __type: "com.amazon.coral.service#UnknownOperationException",
  });
});

test("a real API error keeps its own envelope and status", async () => {
  // Must not be mistaken for an unimplemented operation: this is the engine
  // answering correctly, and the suite has to score it as a pass or a fail
  // rather than a skip.
  const out = await call(
    "DynamoDB_20120810.DescribeTable",
    JSON.stringify({ TableName: "Absent" }),
  );
  assert.equal(out.status, 400);
  assert.match(out.body.__type, /ResourceNotFoundException/);
});

test("the engine starts empty, so shared-table setup cannot collide", async () => {
  // The suite creates its shared tables in a beforeAll and never resets target
  // state, so a session carrying tables over from a previous run would fail it.
  // The table created above proves the session is live; what matters is that it
  // came from this run, not a previous one.
  const out = await call("DynamoDB_20120810.ListTables");
  assert.equal(out.status, 200);
  assert.deepEqual(
    [...out.body.TableNames].sort(),
    ["Bridge", "BridgePartiql", "BridgeVec"],
    "only this run's tables should be present",
  );
});

test("an unsigned request is rejected, as it is on the native server", async () => {
  // Both HTTP surfaces validate auth material through the same code and verify
  // signatures on neither, so the wasm endpoint must not be more permissive
  // than `dynoxide serve`.
  const out = await call("DynamoDB_20120810.ListTables", "{}", { signed: false });
  assert.equal(out.status, 400);
  assert.match(out.body.__type, /MissingAuthenticationTokenException/);
});

test("the target is resolved before auth, matching DynamoDB", async () => {
  const out = await call("DynamoDB_20120810.NoSuchOp", "{}", { signed: false });
  assert.deepEqual(out.body, {
    __type: "com.amazon.coral.service#UnknownOperationException",
  });
});
