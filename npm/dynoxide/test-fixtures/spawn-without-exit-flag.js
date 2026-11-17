"use strict";

// The negative control for the hard-kill test: a server started without
// --exit-with-parent, which nothing stops when this script is killed. On
// Windows it's detached, because libuv puts every other child in a job object
// that Windows kills along with this script.
const { spawn } = require("child_process");

const child = spawn(process.env.DYNOXIDE_BINARY, ["serve", "--port", "0", "--ready-json"], {
  stdio: ["ignore", "pipe", "ignore"],
  detached: process.platform === "win32",
});
let out = "";
child.stdout.on("data", (c) => {
  out += c;
  if (!out.includes("\n")) return;
  const ready = JSON.parse(out.split("\n")[0]);
  console.log(JSON.stringify({ port: ready.port, pid: child.pid }));
});
setInterval(() => {}, 1000);
