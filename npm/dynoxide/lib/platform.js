"use strict";

const fs = require("fs");
const path = require("path");

// Derive platform lookup from the canonical platforms.json (single source of truth).
// Each entry becomes a key of "os-cpu" with the npm package name and binary name.
const platformsData = require("../platforms.json");
const PLATFORMS = {};
for (const entry of platformsData) {
  const key = entry.os + "-" + entry.cpu;
  PLATFORMS[key] = { package: entry.npm_package, binary: entry.binary_name };
}

function resolvePackage(packageName) {
  try {
    const packageDir = path.dirname(
      require.resolve(packageName + "/package.json")
    );
    return packageDir;
  } catch {
    return null;
  }
}

// Returns the platform package's binary, or throws with the guidance the CLI
// prints. A library can catch this; nothing here exits the process.
function findBinary(platform = process.platform, arch = process.arch) {
  const key = platform + "-" + arch;
  const entry = PLATFORMS[key];

  if (!entry) {
    const supported = Object.keys(PLATFORMS).join(", ");
    throw new Error(
      `Dynoxide does not have a prebuilt binary for ${platform}-${arch}.\n` +
        `Supported platforms: ${supported}\n\n` +
        `You can install Dynoxide from source with: cargo install dynoxide-rs`
    );
  }

  const dir = resolvePackage(entry.package);
  if (dir) {
    const binaryPath = path.join(dir, entry.binary);
    if (fs.existsSync(binaryPath)) return binaryPath;
  }

  throw new Error(
    `The platform package ${entry.package} is not installed.\n\n` +
      `This usually means npm was run with --no-optional or --ignore-optional.\n` +
      `Try reinstalling: npm install dynoxide\n\n` +
      `If the problem persists, install Dynoxide directly:\n` +
      `  brew install nubo-db/tap/dynoxide\n` +
      `  cargo install dynoxide-rs`
  );
}

// The CLI's lookup: prints the guidance and exits on failure.
function getBinaryPath() {
  try {
    return findBinary();
  } catch (err) {
    console.error(err.message);
    process.exit(1);
  }
}

module.exports = { findBinary, getBinaryPath };
