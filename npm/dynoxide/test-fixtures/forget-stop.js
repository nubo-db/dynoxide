"use strict";

// Starts a server through the fake and never stops it. The script must still
// exit, and the server with it.
const { launch } = require("../lib/server");

launch({ binary: process.argv[2] }, { command: [process.execPath], env: { FAKE_MODE: "ready" } }).then((server) => {
  console.log(server.port);
});
