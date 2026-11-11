"use strict";

// Starts a real server with start(), prints its port, and waits to be killed.
const { start } = require("..");

start().then((server) => {
  console.log(JSON.stringify({ port: server.port }));
  setInterval(() => {}, 1000);
});
