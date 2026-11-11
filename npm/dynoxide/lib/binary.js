"use strict";

const fs = require("fs");
const { spawn } = require("child_process");
const { findBinary } = require("./platform");

// The first release that speaks the launcher contract (--ready-json and
// --exit-with-parent). A pre-release of it counts.
const MINIMUM = [1, 7, 0];

// Finds the binary in the contract's order: the binary option, then
// DYNOXIDE_BINARY, then the platform package. A path that was asked for but
// doesn't exist is an error rather than a reason to try the next step.
function resolveBinary(options = {}) {
  const env = options.env || process.env;
  if (options.binary) {
    return { path: existing(options.binary, "the binary option"), source: "option" };
  }
  if (env.DYNOXIDE_BINARY) {
    return { path: existing(env.DYNOXIDE_BINARY, "DYNOXIDE_BINARY"), source: "env" };
  }
  return { path: findBinary(options.platform, options.arch), source: "package" };
}

function existing(file, from) {
  if (!fs.existsSync(file)) {
    throw new Error(
      `The dynoxide binary at ${file}, from ${from}, doesn't exist. ` +
        `Point it at a dynoxide executable, or leave it unset to use the one npm installed.`
    );
  }
  return file;
}

// Runs `<binary> --version` and checks the result is 1.7.0 or later.
// Resolves with the version string. `command` is a prefix for running the
// binary through an interpreter (the tests run fakes through Node).
function checkVersion(file, options = {}) {
  const prefix = options.command || [];
  const timeoutMs = options.timeoutMs || 5000;
  const [cmd, ...args] = [...prefix, file, "--version"];

  return new Promise((resolve, reject) => {
    let child;
    try {
      child = spawn(cmd, args, {
        env: { ...process.env, ...options.env },
        stdio: ["ignore", "pipe", "pipe"],
        windowsHide: true,
      });
    } catch (err) {
      reject(new Error(`Couldn't run the dynoxide binary at ${file}: ${err.message}`));
      return;
    }

    let stdout = "";
    let stderr = "";
    let settled = false;
    const finish = (fn, value) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      fn(value);
    };

    const timer = setTimeout(() => {
      child.kill("SIGKILL");
      finish(
        reject,
        new Error(`The dynoxide binary at ${file} didn't answer --version within ${timeoutMs / 1000} seconds.`)
      );
    }, timeoutMs);

    child.stdout.on("data", (chunk) => (stdout += chunk));
    child.stderr.on("data", (chunk) => (stderr += chunk));
    child.on("error", (err) =>
      finish(reject, new Error(`Couldn't run the dynoxide binary at ${file}: ${err.message}`))
    );
    child.on("close", (code, signal) => {
      if (code !== 0) {
        const how = signal ? `was killed by ${signal}` : `exited with status ${code}`;
        const tail = stderr.trim() ? `\n${stderr.trim()}` : "";
        finish(reject, new Error(`The dynoxide binary at ${file} ${how} when asked for --version.${tail}`));
        return;
      }
      const match = /^dynoxide (\d+)\.(\d+)\.(\d+)(-[0-9A-Za-z.-]+)?\s*$/m.exec(stdout);
      if (!match) {
        finish(
          reject,
          new Error(`The binary at ${file} didn't report a version (it printed: ${JSON.stringify(stdout.trim())}).`)
        );
        return;
      }
      const version = match.slice(1, 4).join(".") + (match[4] || "");
      if (!atLeast(match.slice(1, 4).map(Number), MINIMUM)) {
        finish(
          reject,
          new Error(
            `The dynoxide binary at ${file} is too old: found ${version}, and start() needs ${MINIMUM.join(".")} or later.`
          )
        );
        return;
      }
      finish(resolve, version);
    });
  });
}

function atLeast(version, minimum) {
  for (let i = 0; i < 3; i++) {
    if (version[i] !== minimum[i]) return version[i] > minimum[i];
  }
  return true;
}

module.exports = { resolveBinary, checkVersion };
