"use strict";

// A stand-in for the dynoxide binary, run through Node so it works on every
// platform. FAKE_MODE picks the behaviour; FAKE_RECORD, if set, is a file that
// gets the arguments and whether DYNOXIDE_ENCRYPTION_KEY reached it;
// FAKE_MARKER, if set, is written with how the fake was stopped.

const fs = require("fs");
const http = require("http");

const args = process.argv.slice(2);
const mode = process.env.FAKE_MODE || "ready";
const marker = process.env.FAKE_MARKER;
const mark = (how) => marker && fs.writeFileSync(marker, how);

if (process.env.FAKE_PID_FILE) fs.writeFileSync(process.env.FAKE_PID_FILE, String(process.pid));

if (process.env.FAKE_RECORD) {
  const keyed = Object.keys(process.env).filter((k) => k.toUpperCase() === "DYNOXIDE_ENCRYPTION_KEY");
  fs.writeFileSync(process.env.FAKE_RECORD, JSON.stringify({ args, keyed }));
}

if (args[0] === "--version") {
  switch (mode) {
    case "version-hang":
      setInterval(() => {}, 1000);
      break;
    case "version-fail":
      process.stderr.write("broken\n");
      process.exit(3);
      break;
    case "version-garbage":
      process.stdout.write("something else\n");
      break;
    default:
      process.stdout.write(`dynoxide ${process.env.FAKE_VERSION || "1.7.0"}\n`);
  }
  return;
}

const ready = (port, extra = {}) =>
  process.stdout.write(
    JSON.stringify({
      event: "ready",
      schema: 1,
      version: "1.7.0",
      endpoint: `http://127.0.0.1:${port}`,
      host: "127.0.0.1",
      port,
      pid: process.pid,
      ...extra,
    }) + "\n"
  );

const untilEndOfInput = (onEnd) => {
  process.stdin.on("data", () => {});
  process.stdin.on("end", onEnd);
};

switch (mode) {
  case "exit-127":
    process.stderr.write("cannot execute\n");
    process.exit(127);
    break;
  case "garbage":
    process.stdout.write("not json\n");
    setInterval(() => {}, 1000);
    break;
  case "not-ready":
    process.stdout.write(JSON.stringify({ event: "starting" }) + "\n");
    setInterval(() => {}, 1000);
    break;
  case "port-zero":
    ready(0);
    setInterval(() => {}, 1000);
    break;
  case "partial-then-exit":
    process.stdout.write('{"event":"rea', () => process.exit(1));
    break;
  case "never-ready":
    process.stderr.write("still thinking\n");
    setInterval(() => {}, 1000);
    break;
  default: {
    // ready, ignore-stdin, ignore-term, flood
    // In flood mode the server answers with how many bytes of each stream the
    // reader has taken. Writes wait for their callback, so a reader that stops
    // reading stops the count, as it would stop a real server's writes.
    const flushed = { out: 0, err: 0 };
    const server = http.createServer((req, res) => res.end(JSON.stringify(flushed)));
    server.listen(0, "127.0.0.1", () => {
      ready(server.address().port);
      if (mode === "flood") {
        const line = "x".repeat(1024) + "\n";
        const pump = (stream, key) => stream.write(line, () => {
          flushed[key] += line.length;
          pump(stream, key);
        });
        pump(process.stdout, "out");
        pump(process.stderr, "err");
      }
    });
    if (mode !== "ignore-stdin" && mode !== "ignore-term") {
      untilEndOfInput(() => {
        mark("stdin");
        process.exit(0);
      });
    }
    process.on("SIGTERM", () => {
      if (mode === "ignore-term") return;
      mark("sigterm");
      process.exit(0);
    });
  }
}
