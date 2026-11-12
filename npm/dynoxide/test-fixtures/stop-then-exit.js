"use strict";

// Starts a server through the fake with the default grace periods, stops it,
// and should then exit straight away: nothing stop() leaves behind may hold
// Node open.
const { launch } = require("../lib/server");

launch({ binary: process.argv[2] }, { command: [process.execPath], env: { FAKE_MODE: "ready" } })
  .then((server) => server.stop())
  .then((status) => console.log(JSON.stringify(status)));
