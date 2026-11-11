"use strict";

const { launch } = require("./server");

// Starts a Dynoxide server for tests. See the README for the options.
function start(options) {
  return launch(options);
}

module.exports = { start };
