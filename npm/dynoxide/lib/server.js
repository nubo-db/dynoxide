"use strict";

const { spawn } = require("child_process");
const { resolveBinary, checkVersion } = require("./binary");

const READY_TIMEOUT_MS = 10000;
const GRACE_MS = 5000;
const STDERR_LINES = 20;
const STDERR_BYTES = 64 * 1024;

// Every running server, so one exit listener can close all their stdin pipes.
// A server whose stdin closes shuts itself down (--exit-with-parent).
const live = new Set();
let exitListener = false;

function watchExit() {
  if (exitListener) return;
  exitListener = true;
  process.on("exit", () => {
    for (const child of live) child.stdin.destroy();
  });
}

// Starts a server. `seam` is for the tests: a command prefix to run a fake
// binary through Node, extra environment for it, and shorter grace periods.
async function launch(options = {}, seam = {}) {
  const found = resolveBinary(options);
  const command = seam.command || [];
  if (found.source !== "package") {
    await checkVersion(found.path, { command, env: seam.env });
  }

  const args = ["serve", "--port", "0", "--ready-json", "--exit-with-parent"];
  if (options.dbPath != null) args.push(`--db-path=${options.dbPath}`);
  if (options.schema != null) args.push(`--schema=${options.schema}`);

  const [cmd, ...prefix] = [...command, found.path];
  const child = spawn(cmd, [...prefix, ...args], {
    stdio: ["pipe", "pipe", "pipe"],
    env: { ...withoutKey(process.env), ...seam.env },
    windowsHide: true,
  });

  const process_ = watch(child, seam.graceMs || GRACE_MS);
  let ready;
  try {
    ready = await readyLine(child, process_, options.readyTimeoutMs || READY_TIMEOUT_MS);
  } catch (err) {
    await process_.stop();
    err.stderr = process_.stderrTail();
    err.message += tailText(err.stderr);
    throw err;
  }

  live.add(child);
  watchExit();
  child.once("exit", () => live.delete(child));

  // A forgotten stop() mustn't keep Node running. If Node exits, its end of
  // the stdin pipe closes and the server stops itself.
  child.unref();
  for (const stream of [child.stdin, child.stdout, child.stderr]) stream.unref && stream.unref();

  const stop = () => {
    live.delete(child);
    return process_.stop();
  };
  const server = {
    endpoint: ready.endpoint,
    host: ready.host,
    port: ready.port,
    version: ready.version,
    binarySource: found.source,
    region: "us-east-1",
    credentials: { accessKeyId: "dynoxide", secretAccessKey: "dynoxide" },
    stop,
  };
  if (typeof Symbol.asyncDispose === "symbol") {
    Object.defineProperty(server, Symbol.asyncDispose, { value: () => stop().then(() => undefined) });
  }
  return server;
}

function withoutKey(env) {
  const copy = {};
  for (const [name, value] of Object.entries(env)) {
    const match =
      process.platform === "win32"
        ? name.toUpperCase() === "DYNOXIDE_ENCRYPTION_KEY"
        : name === "DYNOXIDE_ENCRYPTION_KEY";
    if (!match) copy[name] = value;
  }
  return copy;
}

// Tracks one child: its first exit status, a bounded stderr tail, and the
// contract's stop procedure (close stdin, SIGTERM, kill), run once.
function watch(child, graceMs) {
  let status = null;
  let spawnError = null;
  const exited = new Promise((resolve) => {
    child.once("exit", (code, signal) => {
      status = status || { code, signal };
      resolve(status);
    });
    child.once("error", (err) => {
      spawnError = err;
      // A child that never started has no exit event to wait for.
      if (child.pid === undefined) {
        status = { code: null, signal: null };
        resolve(status);
      }
    });
  });

  let stderr = "";
  child.stderr.on("data", (chunk) => {
    stderr += chunk;
    if (stderr.length > STDERR_BYTES) stderr = stderr.slice(-STDERR_BYTES);
  });
  child.stdin.on("error", () => {});

  const within = (ms) =>
    Promise.race([exited, new Promise((resolve) => setTimeout(resolve, ms))]).then(() => status !== null);

  let stopping = null;
  const stop = () => {
    if (stopping) return stopping;
    stopping = (async () => {
      child.ref();
      if (status) return status;
      child.stdin.end();
      if (await within(graceMs)) return status;
      if (process.platform !== "win32") {
        child.kill("SIGTERM");
        if (await within(graceMs)) return status;
      }
      child.kill("SIGKILL");
      return exited;
    })();
    return stopping;
  };

  return {
    exited,
    stop,
    spawnError: () => spawnError,
    stderrTail: () => stderr.split(/\r?\n/).filter(Boolean).slice(-STDERR_LINES),
  };
}

// Resolves with the parsed ready line, or rejects with one of the contract's
// startup errors. Output after the first line keeps being read and dropped,
// so a chatty server can't fill the pipe.
function readyLine(child, process_, timeoutMs) {
  return new Promise((resolve, reject) => {
    let buffer = "";
    let done = false;
    const finish = (fn, value) => {
      if (done) return;
      done = true;
      clearTimeout(timer);
      fn(value);
    };

    const timer = setTimeout(
      () => finish(reject, failure("DYNOXIDE_TIMEOUT", `Dynoxide wasn't ready within ${timeoutMs / 1000} seconds.`)),
      timeoutMs
    );

    child.stdout.on("data", (chunk) => {
      if (done) return;
      buffer += chunk;
      const end = buffer.indexOf("\n");
      if (end === -1) return;
      const line = buffer.slice(0, end).trim();
      buffer = "";
      let ready;
      try {
        ready = JSON.parse(line);
      } catch {
        ready = null;
      }
      if (!ready || typeof ready !== "object" || ready.event !== "ready" || typeof ready.endpoint !== "string") {
        finish(reject, failure("DYNOXIDE_BAD_READY_LINE", `Dynoxide's first line of output wasn't a ready line: ${line.slice(0, 200)}`));
      } else if (!ready.port) {
        finish(reject, failure("DYNOXIDE_PORT_ZERO", "Dynoxide's ready line reported port 0, which isn't a port it can be reached on."));
      } else {
        finish(resolve, ready);
      }
    });

    // 'close' rather than 'exit': it comes after the last of stdout, so a
    // ready line written just before exiting is still seen.
    child.once("close", (code, signal) => {
      const err = process_.spawnError();
      if (err) {
        finish(reject, failure("DYNOXIDE_SPAWN_FAILED", `Couldn't run Dynoxide: ${err.message}`));
        return;
      }
      const how = signal ? `was killed by ${signal}` : `exited with status ${code}`;
      const hint =
        code === 126 || code === 127
          ? " That status means the system couldn't run it: check it's a Dynoxide executable for this platform."
          : "";
      finish(reject, failure("DYNOXIDE_EXITED", `Dynoxide ${how} before it was ready.${hint}`));
    });
  });
}

function failure(code, message) {
  const err = new Error(message);
  err.code = code;
  return err;
}

function tailText(lines) {
  return lines.length ? `\n\nLast lines of stderr:\n${lines.join("\n")}` : "";
}

module.exports = { launch };
