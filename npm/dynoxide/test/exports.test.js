"use strict";

// Packs the package, installs the tarball into a project outside it, and
// checks what that project can import. seek-oss/vitest-dynoxide resolves
// dynoxide/bin/dynoxide, so the exports map must keep that path.

const test = require("node:test");
const { before, after } = test;
const assert = require("node:assert");
const fs = require("fs");
const os = require("os");
const path = require("path");
const { execFileSync, spawnSync } = require("child_process");

const PACKAGE = path.join(__dirname, "..");
const FIXTURES = path.join(PACKAGE, "test-fixtures");

// npm's CLI script, run through node: Node won't spawn npm.cmd on Windows
// without a shell.
function npmCli() {
  const dir = path.dirname(process.execPath);
  const candidates = [
    process.env.npm_execpath,
    path.join(dir, "node_modules", "npm", "bin", "npm-cli.js"),
    path.join(dir, "..", "lib", "node_modules", "npm", "bin", "npm-cli.js"),
  ];
  const found = candidates.find((file) => file && file.endsWith(".js") && fs.existsSync(file));
  if (!found) throw new Error(`Can't find npm-cli.js next to ${process.execPath}`);
  return found;
}

const npm = (args, cwd) =>
  execFileSync(process.execPath, [npmCli(), ...args], { cwd, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] });

let project;

before(() => {
  project = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "dynoxide-exports-")));
  const packed = JSON.parse(npm(["pack", "--json", "--pack-destination", project], PACKAGE));
  fs.writeFileSync(path.join(project, "package.json"), JSON.stringify({ name: "consumer", private: true }));
  npm(
    ["install", "--offline", "--omit=optional", "--no-audit", "--no-fund", "--no-package-lock", path.join(project, packed[0].filename)],
    project
  );
});

after(() => {
  if (project) fs.rmSync(project, { recursive: true, force: true });
});

// Runs code in the consumer project, as CommonJS or as an ES module.
function inProject(code, type = "commonjs") {
  const run = spawnSync(process.execPath, ["--input-type=" + type, "-e", code], {
    cwd: project,
    encoding: "utf8",
  });
  return { status: run.status, out: run.stdout.trim(), err: run.stderr };
}

test("dynoxide/bin/dynoxide resolves through require.resolve", () => {
  const run = inProject('console.log(require.resolve("dynoxide/bin/dynoxide"))');
  assert.strictEqual(run.status, 0, run.err);
  assert.strictEqual(path.relative(project, run.out), path.join("node_modules", "dynoxide", "bin", "dynoxide"));
});

test("dynoxide/bin/dynoxide resolves through import.meta.resolve", () => {
  const run = inProject('console.log(import.meta.resolve("dynoxide/bin/dynoxide"))', "module");
  assert.strictEqual(run.status, 0, run.err);
  assert.match(run.out, /^file:.*\/node_modules\/dynoxide\/bin\/dynoxide$/);
});

test("dynoxide/package.json resolves both ways", () => {
  const cjs = inProject('console.log(require("dynoxide/package.json").name)');
  assert.strictEqual(cjs.out, "dynoxide", cjs.err);
  const esm = inProject('console.log(import.meta.resolve("dynoxide/package.json"))', "module");
  assert.match(esm.out, /\/node_modules\/dynoxide\/package\.json$/, esm.err);
});

test("start can be required and imported", () => {
  const cjs = inProject('console.log(typeof require("dynoxide").start)');
  assert.strictEqual(cjs.out, "function", cjs.err);
  const esm = inProject('import { start } from "dynoxide"; console.log(typeof start)', "module");
  assert.strictEqual(esm.out, "function", esm.err);
});

test("a path outside the exports map is refused", () => {
  const run = inProject('require("dynoxide/lib/platform.js")');
  assert.notStrictEqual(run.status, 0);
  assert.match(run.err, /ERR_PACKAGE_PATH_NOT_EXPORTED/);
});

test("the packed package ships the entry point and its types", () => {
  for (const file of ["lib/index.js", "lib/index.d.ts", "lib/server.js", "lib/binary.js", "bin/dynoxide"]) {
    assert.ok(fs.existsSync(path.join(project, "node_modules", "dynoxide", file)), `${file} wasn't packed`);
  }
  assert.ok(!fs.existsSync(path.join(project, "node_modules", "dynoxide", "test")), "the tests were packed");
});

// The type checks need TypeScript, which CI installs into test-fixtures.
// DYNOXIDE_EXTRA_CHECKS makes a missing install a failure rather than a skip.
function tsc() {
  try {
    return require.resolve("typescript/bin/tsc", { paths: [FIXTURES] });
  } catch {
    return null;
  }
}
const typesSkip = !tsc() && !process.env.DYNOXIDE_EXTRA_CHECKS && "TypeScript isn't installed in test-fixtures";

for (const fixture of ["modern", "older-lib"]) {
  test(`the types compile for the ${fixture} fixture`, { skip: typesSkip }, () => {
    const dir = path.join(project, "types-" + fixture);
    fs.cpSync(path.join(FIXTURES, "types-check", fixture), dir, { recursive: true });
    const run = spawnSync(process.execPath, [tsc(), "-p", dir], { encoding: "utf8" });
    assert.strictEqual(run.status, 0, run.stdout + run.stderr);
  });
}
