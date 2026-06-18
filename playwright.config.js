import { defineConfig, devices } from "@playwright/test";

// Drives the shipped wasm bundle (dist/) in a real browser against real
// wa-sqlite and OPFS - the path the conformance suite does not cover. Run with
// `npm run test:browser` after building the bundle (npm run build:wasm[:dev]).
const PORT = Number(process.env.HARNESS_PORT || 8099);

export default defineConfig({
  testDir: "tests/browser",
  testMatch: "**/*.spec.js",
  // OPFS state is shared per origin, so the specs run serially against unique
  // database names rather than racing on the same directories.
  fullyParallel: false,
  workers: 1,
  timeout: 30_000,
  // localhost is a secure context, so OPFS sync access handles are available
  // without COOP/COEP - the same posture a real static host gets over HTTPS.
  use: {
    baseURL: `http://127.0.0.1:${PORT}`,
  },
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
  webServer: {
    command: `node js/test-support/static-server.mjs ${PORT}`,
    url: `http://127.0.0.1:${PORT}/harness/engine-harness.html`,
    reuseExistingServer: !process.env.CI,
    timeout: 30_000,
  },
});
