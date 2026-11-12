"use strict";

const test = require("node:test");
const assert = require("node:assert");
const path = require("path");
const { resolveBinary, checkVersion } = require("../lib/binary");
const { findBinary } = require("../lib/platform");

const FAKE = path.join(__dirname, "..", "test-fixtures", "fake-server.js");
const viaNode = [process.execPath];

test("the binary option wins over DYNOXIDE_BINARY", () => {
  const found = resolveBinary({ binary: FAKE }, { env: { DYNOXIDE_BINARY: "/elsewhere" } });
  assert.deepStrictEqual(found, { path: FAKE, source: "option" });
});

test("DYNOXIDE_BINARY is used when there's no option", () => {
  const found = resolveBinary({}, { env: { DYNOXIDE_BINARY: FAKE } });
  assert.deepStrictEqual(found, { path: FAKE, source: "env" });
});

test("an empty DYNOXIDE_BINARY counts as unset", () => {
  assert.throws(
    () => resolveBinary({}, { env: { DYNOXIDE_BINARY: "" }, platform: "plan9", arch: "x64" }),
    /does not have a prebuilt binary for plan9-x64/
  );
});

test("a relative path is made absolute, so it runs the file that was checked", () => {
  const relative = path.relative(process.cwd(), FAKE);
  assert.deepStrictEqual(resolveBinary({ binary: relative }), { path: FAKE, source: "option" });
});

test("a directory or a non-string binary is refused with a code", () => {
  assert.throws(() => resolveBinary({ binary: __dirname }), { code: "DYNOXIDE_BINARY_MISSING", message: /isn't a file/ });
  assert.throws(() => resolveBinary({ binary: 42 }), { code: "DYNOXIDE_BINARY_MISSING", message: /given as a string/ });
});

test("a bare command name is explained as needing a path", () => {
  assert.throws(() => resolveBinary({}, { env: { DYNOXIDE_BINARY: "dynoxide-nowhere" } }), {
    code: "DYNOXIDE_BINARY_MISSING",
    message: /takes a path, not a command name/,
  });
});

test("a missing option path fails rather than falling through", () => {
  assert.throws(
    () => resolveBinary({ binary: "/no/such/dynoxide" }, { env: { DYNOXIDE_BINARY: FAKE } }),
    /no.such.dynoxide, from the binary option, isn't a file/
  );
});

test("a missing DYNOXIDE_BINARY path fails", () => {
  assert.throws(() => resolveBinary({}, { env: { DYNOXIDE_BINARY: "/no/such/dynoxide" } }), {
    code: "DYNOXIDE_BINARY_MISSING",
    message: /from DYNOXIDE_BINARY, isn't a file/,
  });
});

test("an unsupported platform throws instead of exiting", () => {
  assert.throws(() => findBinary("plan9", "x64"), {
    code: "DYNOXIDE_UNSUPPORTED_PLATFORM",
    message: /does not have a prebuilt binary for plan9-x64/,
  });
});

test("a missing platform package throws instead of exiting", () => {
  // The repo has no @dynoxide/* packages installed.
  assert.throws(() => findBinary("linux", "x64"), {
    code: "DYNOXIDE_PACKAGE_MISSING",
    message: /The platform package @dynoxide\/linux-x64 is not installed/,
  });
});

for (const version of ["1.7.0", "1.7.0-rc.1", "1.8.0", "2.0.0"]) {
  test(`version ${version} is accepted`, async () => {
    assert.strictEqual(await checkVersion(FAKE, { command: viaNode, env: { FAKE_VERSION: version } }), version);
  });
}

function escapeRegExp(text) {
  return text.replace(/[\\^$.*+?()[\]{}|]/g, "\\$&");
}

for (const version of ["1.6.0", "1.6.9-rc.1", "0.9.0"]) {
  test(`version ${version} is refused`, async () => {
    await assert.rejects(
      checkVersion(FAKE, { command: viaNode, env: { FAKE_VERSION: version } }),
      {
        code: "DYNOXIDE_TOO_OLD",
        message: new RegExp(`found ${escapeRegExp(version)}, and start\\(\\) needs 1\\.7\\.0 or later`),
      }
    );
  });
}

test("unparseable --version output is its own error", async () => {
  await assert.rejects(checkVersion(FAKE, { command: viaNode, env: { FAKE_MODE: "version-garbage" } }), { code: "DYNOXIDE_VERSION_CHECK_FAILED", message: /didn't report a version/ });
});

test("a failing --version is its own error", async () => {
  await assert.rejects(checkVersion(FAKE, { command: viaNode, env: { FAKE_MODE: "version-fail" } }), { code: "DYNOXIDE_VERSION_CHECK_FAILED", message: /exited with status 3/ });
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

test("the typed error codes are exactly the ones the library throws", () => {
  const fs = require("fs");
  const lib = path.join(__dirname, "..", "lib");
  const thrown = new Set();
  for (const file of ["platform.js", "binary.js", "server.js"]) {
    const source = fs.readFileSync(path.join(lib, file), "utf8");
    for (const m of source.matchAll(/coded\(\s*"(DYNOXIDE_[A-Z_]+)"/g)) thrown.add(m[1]);
  }
  const typed = new Set(
    [...fs.readFileSync(path.join(lib, "index.d.ts"), "utf8").matchAll(/\| "(DYNOXIDE_[A-Z_]+)"/g)].map((m) => m[1])
  );
  const readme = fs.readFileSync(path.join(__dirname, "..", "README.md"), "utf8");
  const documented = new Set([...readme.matchAll(/^\| `(DYNOXIDE_[A-Z_]+)` \|/gm)].map((m) => m[1]));
  assert.ok(thrown.size >= 10, `found only ${thrown.size} codes in lib/`);
  assert.deepStrictEqual([...typed].sort(), [...thrown].sort());
  assert.deepStrictEqual([...documented].sort(), [...thrown].sort());
});
