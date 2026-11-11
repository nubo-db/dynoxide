"use strict";

const test = require("node:test");
const assert = require("node:assert");
const fs = require("fs");
const os = require("os");
const path = require("path");
const http = require("http");
const { spawn, spawnSync } = require("child_process");
const { launch } = require("../lib/server");

const FAKE = path.join(__dirname, "..", "test-fixtures", "fake-server.js");
const FIXTURES = path.join(__dirname, "..", "test-fixtures");
const unix = process.platform !== "win32";

const tmp = () => fs.mkdtempSync(path.join(os.tmpdir(), "dynoxide-start-"));

// Starts the Node fake through the internal seam, with short grace periods.
function fake(mode, extra = {}) {
  const { env, options, ...seam } = extra;
  return launch(
    { binary: FAKE, ...options },
    { command: [process.execPath], env: { FAKE_MODE: mode, ...env }, graceMs: 3000, ...seam }
  );
}

function alive(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

function answers(port) {
  return new Promise((resolve) => {
    const req = http.get({ host: "127.0.0.1", port, path: "/", timeout: 1000 }, (res) => {
      res.resume();
      resolve(true);
    });
    req.on("error", () => resolve(false));
    req.on("timeout", () => {
      req.destroy();
      resolve(false);
    });
  });
}

async function waitUntil(check, ms) {
  const deadline = Date.now() + ms;
  while (Date.now() < deadline) {
    if (await check()) return true;
    await new Promise((r) => setTimeout(r, 100));
  }
  return check();
}

// The Node fake

test("a ready line gives the endpoint and port, and stop closes stdin", async () => {
  const dir = tmp();
  const marker = path.join(dir, "marker");
  const server = await fake("ready", { env: { FAKE_MARKER: marker } });
  assert.match(server.endpoint, /^http:\/\/127\.0\.0\.1:\d+$/);
  assert.ok(server.port > 0);
  assert.strictEqual(server.endpoint, `http://127.0.0.1:${server.port}`);
  assert.strictEqual(server.host, "127.0.0.1");
  assert.strictEqual(server.version, "1.7.0");
  assert.strictEqual(server.binarySource, "option");
  assert.strictEqual(server.region, "us-east-1");
  assert.deepStrictEqual(server.credentials, { accessKeyId: "dynoxide", secretAccessKey: "dynoxide" });
  assert.ok(await answers(server.port));
  assert.deepStrictEqual(await server.stop(), { code: 0, signal: null });
  assert.strictEqual(fs.readFileSync(marker, "utf8"), "stdin");
});

test("exiting 127 before ready is reported as couldn't run it, with stderr", async () => {
  await assert.rejects(fake("exit-127"), (err) => {
    assert.strictEqual(err.code, "DYNOXIDE_EXITED");
    assert.match(err.message, /status 127/);
    assert.match(err.message, /couldn't run it/);
    assert.match(err.message, /cannot execute/);
    return true;
  });
});

test("a stdout that ends mid-line counts as exiting before ready", async () => {
  await assert.rejects(fake("partial-then-exit"), { code: "DYNOXIDE_EXITED" });
});

test("a first line that isn't JSON is an invalid ready line, and the child is stopped", async () => {
  const dir = tmp();
  const record = path.join(dir, "record");
  await assert.rejects(fake("garbage", { env: { FAKE_PID_FILE: record } }), (err) => {
    assert.strictEqual(err.code, "DYNOXIDE_BAD_READY_LINE");
    assert.match(err.message, /not json/);
    return true;
  });
  assert.strictEqual(alive(Number(fs.readFileSync(record, "utf8"))), false);
});

test("a JSON line whose event isn't ready is an invalid ready line", async () => {
  await assert.rejects(fake("not-ready"), { code: "DYNOXIDE_BAD_READY_LINE" });
});

test("a ready line without a numeric port is an invalid ready line", async () => {
  await assert.rejects(fake("no-port"), { code: "DYNOXIDE_BAD_READY_LINE" });
});

test("a supplied binary that's too old is refused before it's started as a server", async () => {
  const record = path.join(tmp(), "record");
  await assert.rejects(fake("ready", { env: { FAKE_VERSION: "1.6.0", FAKE_RECORD: record } }), {
    code: "DYNOXIDE_TOO_OLD",
  });
  assert.deepStrictEqual(JSON.parse(fs.readFileSync(record, "utf8")).args, ["--version"]);
});

test("the platform package's binary isn't version-checked", async () => {
  const server = await fake("ready", {
    env: { FAKE_VERSION: "1.6.0" },
    resolve: () => ({ path: FAKE, source: "package" }),
  });
  assert.strictEqual(server.binarySource, "package");
  await server.stop();
});

test("a binary that can't be spawned is its own error", async () => {
  const missing = path.join(tmp(), "no-such-dynoxide");
  await assert.rejects(
    launch({}, { resolve: () => ({ path: missing, source: "package" }) }),
    { code: "DYNOXIDE_SPAWN_FAILED" }
  );
});

test("port 0 in the ready line is its own error", async () => {
  await assert.rejects(fake("port-zero"), { code: "DYNOXIDE_PORT_ZERO" });
});

test("no ready line within the timeout fails, with stderr, and the child is stopped", async () => {
  const dir = tmp();
  const record = path.join(dir, "record");
  const started = Date.now();
  await assert.rejects(
    fake("never-ready", { env: { FAKE_PID_FILE: record }, options: { readyTimeoutMs: 700 } }),
    (err) => {
      assert.strictEqual(err.code, "DYNOXIDE_TIMEOUT");
      assert.match(err.message, /0\.7 seconds/);
      assert.match(err.message, /still thinking/);
      return true;
    }
  );
  assert.ok(Date.now() - started >= 700);
  assert.strictEqual(alive(Number(fs.readFileSync(record, "utf8"))), false);
});

test("a server that ignores stdin is stopped with SIGTERM", { skip: !unix && "SIGTERM is a kill on Windows" }, async () => {
  const marker = path.join(tmp(), "marker");
  const server = await fake("ignore-stdin", { env: { FAKE_MARKER: marker }, graceMs: 500 });
  await server.stop();
  assert.strictEqual(fs.readFileSync(marker, "utf8"), "sigterm");
});

test("a server that ignores stdin and SIGTERM is killed within the grace periods", { timeout: 10000 }, async () => {
  const server = await fake("ignore-term", { graceMs: 500 });
  const started = Date.now();
  const status = await server.stop();
  const took = Date.now() - started;
  if (unix) assert.strictEqual(status.signal, "SIGKILL");
  assert.ok(took >= 500, `stopped after ${took} ms, before the first grace period ended`);
  assert.ok(took < 3000, `took ${took} ms`);
  assert.strictEqual(await answers(server.port), false);
});

function flushed(port) {
  return new Promise((resolve, reject) => {
    http.get({ host: "127.0.0.1", port, path: "/" }, (res) => {
      let text = "";
      res.on("data", (c) => (text += c));
      res.on("end", () => resolve(JSON.parse(text)));
    }).on("error", reject);
  });
}

test("a server that floods stdout and stderr keeps being read, and stops", async () => {
  const server = await fake("flood");
  await new Promise((r) => setTimeout(r, 300));
  const before = await flushed(server.port);
  await new Promise((r) => setTimeout(r, 300));
  const after = await flushed(server.port);
  // A pipe holds about 64 KiB; past that, only a reader keeps the count rising.
  for (const key of ["out", "err"]) {
    assert.ok(after[key] > 1024 * 1024, `${key}: only ${after[key]} bytes read`);
    assert.ok(after[key] > before[key], `${key} stopped being read`);
  }
  assert.strictEqual((await server.stop()).code, 0);
});

test("DYNOXIDE_ENCRYPTION_KEY reaches neither the version check nor the server", async () => {
  const record = path.join(tmp(), "record");
  const versionRecord = path.join(tmp(), "version-record");
  process.env.DYNOXIDE_ENCRYPTION_KEY = "secret";
  try {
    const server = await fake("ready", { env: { FAKE_RECORD: record, FAKE_VERSION_RECORD: versionRecord } });
    await server.stop();
  } finally {
    delete process.env.DYNOXIDE_ENCRYPTION_KEY;
  }
  assert.deepStrictEqual(JSON.parse(fs.readFileSync(record, "utf8")).keyed, []);
  assert.deepStrictEqual(JSON.parse(fs.readFileSync(versionRecord, "utf8")).keyed, []);
});

test("the arguments are the contract's, with --db-path and --schema as single arguments", async () => {
  const record = path.join(tmp(), "record");
  const server = await fake("ready", {
    env: { FAKE_RECORD: record },
    options: { dbPath: "-odd name.db", schema: "-schema.json" },
  });
  await server.stop();
  assert.deepStrictEqual(JSON.parse(fs.readFileSync(record, "utf8")).args, [
    "serve",
    "--port",
    "0",
    "--ready-json",
    "--exit-with-parent",
    "--db-path=-odd name.db",
    "--schema=-schema.json",
  ]);
});

test("stop twice returns the same promise and status", async () => {
  const server = await fake("ready");
  const first = server.stop();
  assert.strictEqual(server.stop(), first);
  assert.deepStrictEqual(await first, await server.stop());
});

test("stop after the server died by itself resolves with that status", async () => {
  const record = path.join(tmp(), "record");
  const server = await fake("ready", { env: { FAKE_PID_FILE: record } });
  process.kill(Number(fs.readFileSync(record, "utf8")), "SIGKILL");
  await waitUntil(async () => !(await answers(server.port)), 3000);
  await new Promise((r) => setTimeout(r, 200));
  const status = await server.stop();
  if (unix) assert.deepStrictEqual(status, { code: null, signal: "SIGKILL" });
  else assert.notStrictEqual(status.code, 0);
});

test(
  "stop works through await using's dispose method",
  { skip: typeof Symbol.asyncDispose !== "symbol" && "this Node has no Symbol.asyncDispose" },
  async () => {
  const server = await fake("ready");
  await server[Symbol.asyncDispose]();
  assert.strictEqual(await answers(server.port), false);
  }
);

test("twelve servers share one exit listener", async () => {
  const before = process.listenerCount("exit");
  const servers = await Promise.all(Array.from({ length: 12 }, () => fake("ready")));
  assert.ok(process.listenerCount("exit") - before <= 1);
  await Promise.all(servers.map((s) => s.stop()));
});

test("a script that awaits stop exits as soon as the server has stopped", () => {
  const started = Date.now();
  const run = spawnSync(process.execPath, [path.join(FIXTURES, "stop-then-exit.js"), FAKE], {
    encoding: "utf8",
    timeout: 15000,
  });
  const took = Date.now() - started;
  assert.strictEqual(run.status, 0, run.stderr);
  assert.deepStrictEqual(JSON.parse(run.stdout), { code: 0, signal: null });
  // The default grace period is 5 seconds; a leftover timer would hold Node that long.
  assert.ok(took < 4000, `the script took ${took} ms to exit`);
});

test("a script that never calls stop exits on its own, and the server stops", async () => {
  const run = spawnSync(process.execPath, [path.join(FIXTURES, "forget-stop.js"), FAKE], {
    encoding: "utf8",
    timeout: 10000,
  });
  assert.strictEqual(run.status, 0, run.stderr);
  const port = Number(run.stdout.trim());
  assert.ok(port > 0, run.stdout);
  assert.ok(await waitUntil(async () => !(await answers(port)), 3000), "the server outlived the script");
});

// The real binary, when CI provides one

const real = process.env.DYNOXIDE_BINARY;
const realTest = (name, fn) => test(name, { skip: !real && "DYNOXIDE_BINARY isn't set" }, fn);

function dynamo(endpoint, target, body) {
  return new Promise((resolve, reject) => {
    const data = JSON.stringify(body);
    const req = http.request(
      endpoint,
      {
        method: "POST",
        headers: {
          "content-type": "application/x-amz-json-1.0",
          "x-amz-target": `DynamoDB_20120810.${target}`,
          "x-amz-date": "20260101T000000Z",
          authorization:
            "AWS4-HMAC-SHA256 Credential=dynoxide/20260101/us-east-1/dynamodb/aws4_request, SignedHeaders=host, Signature=0",
          "content-length": Buffer.byteLength(data),
        },
      },
      (res) => {
        let text = "";
        res.on("data", (c) => (text += c));
        res.on("end", () => (res.statusCode === 200 ? resolve(JSON.parse(text)) : reject(new Error(text))));
      }
    );
    req.on("error", reject);
    req.end(data);
  });
}

realTest("the real binary starts, answers ListTables and stops", async () => {
  const { start } = require("..");
  const server = await start();
  assert.strictEqual(server.binarySource, "env");
  assert.deepStrictEqual(await dynamo(server.endpoint, "ListTables", {}), { TableNames: [] });
  assert.strictEqual((await server.stop()).code, 0);
});

realTest("a --db-path database survives a restart", async () => {
  const { start } = require("..");
  const dbPath = path.join(tmp(), "data.db");
  const first = await start({ dbPath });
  await dynamo(first.endpoint, "CreateTable", {
    TableName: "kept",
    KeySchema: [{ AttributeName: "pk", KeyType: "HASH" }],
    AttributeDefinitions: [{ AttributeName: "pk", AttributeType: "S" }],
    BillingMode: "PAY_PER_REQUEST",
  });
  await dynamo(first.endpoint, "PutItem", { TableName: "kept", Item: { pk: { S: "kept" } } });
  await first.stop();

  const second = await start({ dbPath });
  const got = await dynamo(second.endpoint, "GetItem", { TableName: "kept", Key: { pk: { S: "kept" } } });
  await second.stop();
  assert.deepStrictEqual(got.Item, { pk: { S: "kept" } });
});

// The README's AWS SDK example. CI installs the SDK into test-fixtures and sets
// DYNOXIDE_EXTRA_CHECKS, which makes a missing SDK a failure rather than a skip.
function sdkInstalled() {
  try {
    require.resolve("@aws-sdk/client-dynamodb", { paths: [FIXTURES] });
    return true;
  } catch {
    return false;
  }
}
const readmeSkip = !real
  ? "DYNOXIDE_BINARY isn't set"
  : !sdkInstalled() && !process.env.DYNOXIDE_EXTRA_CHECKS && "the AWS SDK isn't installed in test-fixtures";

test("the README's example is the code the next test runs", () => {
  const readme = fs.readFileSync(path.join(__dirname, "..", "README.md"), "utf8");
  const example = fs.readFileSync(path.join(FIXTURES, "readme-example.mjs"), "utf8").trimEnd();
  assert.ok(readme.includes("```js\n" + example + "\n```"), "the README's start() example and test-fixtures/readme-example.mjs differ");
});

test("the README example runs as written", { skip: readmeSkip }, () => {
  const run = spawnSync(process.execPath, [path.join(FIXTURES, "readme-example.mjs")], {
    encoding: "utf8",
    timeout: 20000,
  });
  assert.strictEqual(run.status, 0, run.stderr);
  assert.strictEqual(run.stdout.trim(), "[]");
});

// Starts a fixture that starts a server, then kills the fixture outright.
async function hardKill(fixture, args = []) {
  const child = spawn(process.execPath, [path.join(FIXTURES, fixture), ...args], {
    stdio: ["ignore", "pipe", "inherit"],
    env: process.env,
  });
  const kill = () => {
    if (unix) child.kill("SIGKILL");
    else spawnSync("taskkill", ["/F", "/PID", String(child.pid)]);
  };
  try {
    const line = await new Promise((resolve, reject) => {
      let out = "";
      child.stdout.on("data", (c) => {
        out += c;
        if (out.includes("\n")) resolve(out.split("\n")[0]);
      });
      child.on("exit", (code) => reject(new Error(`the fixture exited ${code} before starting a server`)));
    });
    const { port, pid } = JSON.parse(line);
    assert.ok(await answers(port));
    return { port, pid };
  } finally {
    kill();
  }
}

realTest("the server stops when its runner is killed outright", async () => {
  const { port } = await hardKill("hard-kill-child.js");
  assert.ok(await waitUntil(async () => !(await answers(port)), 3000), "the server outlived its runner");
});

realTest("the hard-kill check sees a server that does outlive its runner", async () => {
  const { port, pid } = await hardKill("spawn-without-exit-flag.js");
  try {
    await new Promise((r) => setTimeout(r, 3000));
    assert.ok(await answers(port), "a server without --exit-with-parent should have survived");
  } finally {
    try {
      process.kill(pid, "SIGKILL");
    } catch {}
  }
});

// On Windows the job object ends the server abruptly rather than through
// stdin, so only Linux and macOS promise a clean shutdown after a hard kill.
test(
  "a write survives the runner being killed outright",
  { skip: (!real && "DYNOXIDE_BINARY isn't set") || (!unix && "Windows ends the server without a clean shutdown") },
  async () => {
  const { start } = require("..");
  const dbPath = path.join(tmp(), "data.db");
  const { port } = await hardKill("hard-kill-child.js", [dbPath]);
  assert.ok(await waitUntil(async () => !(await answers(port)), 3000), "the server outlived its runner");
  const server = await start({ dbPath });
  const got = await dynamo(server.endpoint, "GetItem", { TableName: "kept", Key: { pk: { S: "after-kill" } } });
  await server.stop();
  assert.deepStrictEqual(got.Item, { pk: { S: "after-kill" } });
  }
);
