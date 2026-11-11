"use strict";

const test = require("node:test");
const assert = require("node:assert");
const path = require("path");
const { resolveBinary, checkVersion } = require("../lib/binary");
const { findBinary } = require("../lib/platform");

const FAKE = path.join(__dirname, "..", "test-fixtures", "fake-server.js");
const viaNode = [process.execPath];

test("the binary option wins over DYNOXIDE_BINARY", () => {
  const found = resolveBinary({ binary: FAKE, env: { DYNOXIDE_BINARY: "/elsewhere" } });
  assert.deepStrictEqual(found, { path: FAKE, source: "option" });
});

test("DYNOXIDE_BINARY is used when there's no option", () => {
  const found = resolveBinary({ env: { DYNOXIDE_BINARY: FAKE } });
  assert.deepStrictEqual(found, { path: FAKE, source: "env" });
});

test("an empty DYNOXIDE_BINARY counts as unset", () => {
  assert.throws(
    () => resolveBinary({ env: { DYNOXIDE_BINARY: "" }, platform: "plan9", arch: "x64" }),
    /does not have a prebuilt binary for plan9-x64/
  );
});

test("a missing option path fails rather than falling through", () => {
  assert.throws(
    () => resolveBinary({ binary: "/no/such/dynoxide", env: { DYNOXIDE_BINARY: FAKE } }),
    /\/no\/such\/dynoxide, from the binary option, doesn't exist/
  );
});

test("a missing DYNOXIDE_BINARY path fails", () => {
  assert.throws(() => resolveBinary({ env: { DYNOXIDE_BINARY: "/no/such/dynoxide" } }), /from DYNOXIDE_BINARY, doesn't exist/);
});

test("an unsupported platform throws instead of exiting", () => {
  assert.throws(() => findBinary("plan9", "x64"), /does not have a prebuilt binary for plan9-x64/);
});

test("a missing platform package throws instead of exiting", () => {
  // The repo has no @dynoxide/* packages installed.
  assert.throws(() => findBinary("linux", "x64"), /The platform package @dynoxide\/linux-x64 is not installed/);
});

for (const version of ["1.7.0", "1.7.0-rc.1", "1.8.0", "2.0.0"]) {
  test(`version ${version} is accepted`, async () => {
    assert.strictEqual(await checkVersion(FAKE, { command: viaNode, env: { FAKE_VERSION: version } }), version);
  });
}

for (const version of ["1.6.0", "1.6.9-rc.1", "0.9.0"]) {
  test(`version ${version} is refused`, async () => {
    await assert.rejects(
      checkVersion(FAKE, { command: viaNode, env: { FAKE_VERSION: version } }),
      new RegExp(`found ${version.replace(/\./g, "\\.")}, and start\\(\\) needs 1\\.7\\.0 or later`)
    );
  });
}

test("unparseable --version output is its own error", async () => {
  await assert.rejects(checkVersion(FAKE, { command: viaNode, env: { FAKE_MODE: "version-garbage" } }), /didn't report a version/);
});

test("a failing --version is its own error", async () => {
  await assert.rejects(checkVersion(FAKE, { command: viaNode, env: { FAKE_MODE: "version-fail" } }), /exited with status 3/);
});

test("a hanging --version times out", async () => {
  await assert.rejects(
    checkVersion(FAKE, { command: viaNode, env: { FAKE_MODE: "version-hang" }, timeoutMs: 500 }),
    /didn't answer --version within 0\.5 seconds/
  );
});

test("the CLI still prints the guidance and exits 1 when the platform package is missing", () => {
  const { spawnSync } = require("child_process");
  const cli = path.join(__dirname, "..", "bin", "dynoxide");
  const run = spawnSync(process.execPath, [cli, "--version"], { encoding: "utf8" });
  assert.strictEqual(run.status, 1);
  assert.match(run.stderr, /is not installed|does not have a prebuilt binary/);
  assert.match(run.stderr, /brew install nubo-db\/tap\/dynoxide|cargo install dynoxide-rs/);
});
