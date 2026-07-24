/**
 * DynamoDB HTTP front for the wasm engine.
 *
 * The wasm engine runs in a browser Worker against SQLite-wasm and OPFS, so it
 * has no wire protocol of its own. This puts one in front: it drives the
 * shipping `dist/` bundle in a headless Chromium it manages itself, and serves
 * the DynamoDB JSON-1.0 protocol on a local port. A client sees an endpoint and
 * cannot tell it from the native build - which is the point, because it lets
 * the conformance suite treat the wasm engine as an ordinary target with no
 * target-specific code path.
 *
 * The engine owns the wire envelope (see `dispatch_http` in src/wasm_api.rs).
 * This file resolves nothing about the protocol: it reads the request, lifts out
 * the target, body and auth material, and writes back whatever status and body
 * come out. Anything protocol-shaped that appears here is a bug, because it
 * would be a second implementation that can drift from the native server.
 *
 * Not a production server. It exists to put the shipping wasm artefact under
 * test over a real socket. Auth material is validated exactly as the native
 * server validates it and signatures are verified on neither, so this is no
 * more of a gate than `dynoxide serve` is; there is no TLS, and no concurrency
 * beyond what one browser page provides.
 *
 * Self-contained by design: it installs its own browser on first run, so a
 * caller needs the endpoint and nothing else. The only prerequisite is a built
 * bundle.
 *
 * Usage:
 *   npm run build:wasm            # dist/ must exist first
 *   npm run wasm:serve            # port 8003
 *   npm run wasm:serve -- --port 9000
 */
import { createServer } from "node:http";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { access } from "node:fs/promises";

const repoRoot = join(dirname(fileURLToPath(import.meta.url)), "..");

/** Port the DynamoDB endpoint listens on. 8003 is the free one: 8000 is taken
 *  by DynamoDB Local and ExtendDB, 8001 by the native dynoxide build, 8002 by
 *  Dynalite, and 4566 by LocalStack, Floci and Ministack. */
const DEFAULT_PORT = 8003;

/** Port the internal static server uses to serve /js/ and /dist/ to the page.
 *  Separate from the public port and not something a client ever touches. */
const DEFAULT_ASSET_PORT = 8004;

function parseArgs(argv) {
  const args = { port: DEFAULT_PORT, assetPort: DEFAULT_ASSET_PORT, headed: false };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === "--port") args.port = Number(argv[++i]);
    else if (arg === "--asset-port") args.assetPort = Number(argv[++i]);
    else if (arg === "--headed") args.headed = true;
    else if (arg === "--help" || arg === "-h") args.help = true;
    else throw new Error(`unknown argument: ${arg}`);
  }
  if (!Number.isInteger(args.port) || args.port < 1 || args.port > 65535) {
    throw new Error(`--port must be a valid port, got ${args.port}`);
  }
  return args;
}

const USAGE = `dynoxide wasm HTTP bridge

  --port <n>        DynamoDB endpoint port (default ${DEFAULT_PORT})
  --asset-port <n>  internal static-server port (default ${DEFAULT_ASSET_PORT})
  --headed          run the browser headed, for debugging
  --help            this message
`;

/**
 * The page the engine runs in. Built here rather than committed under harness/
 * so the bridge cannot be broken by an edit to a file that looks like it only
 * serves the browser specs.
 *
 * `ephemeral: true` forces an in-memory session. That is what gives each server
 * start a clean database, which the conformance suite requires: it creates
 * shared tables in a `beforeAll` and never resets target state, so a leftover
 * OPFS table from a previous run would collide. It also means this path does
 * not exercise OPFS persistence - the browser specs in tests/browser cover
 * that, and this covers the DynamoDB surface.
 */
const PAGE = `<!doctype html>
<html lang="en"><head><meta charset="utf-8" /><title>dynoxide wasm bridge</title></head>
<body><script type="module">
  import { EngineClient } from "/js/engine-client.js";
  const client = new EngineClient({
    workerUrl: "/dist/dynoxide-worker.js",
    ephemeral: true,
  });
  globalThis.__bridge = {
    ready: client.ready().then((d) => d),
    dispatch: (target, body, auth) => client.dispatchHttp(target, body, auth),
  };
</script></body></html>`;

async function assertBuilt() {
  try {
    await access(join(repoRoot, "dist", "dynoxide-worker.js"));
  } catch {
    throw new Error(
      "dist/dynoxide-worker.js is missing. Build the engine first: npm run build:wasm",
    );
  }
}

/** Serve the repo root plus the bridge page, on the internal asset port. */
function startAssetServer(port) {
  const child = spawn(
    process.execPath,
    [join(repoRoot, "js", "test-support", "static-server.mjs"), String(port)],
    { cwd: repoRoot, stdio: ["ignore", "pipe", "inherit"] },
  );
  return child;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    process.stdout.write(USAGE);
    return;
  }
  await assertBuilt();

  const assets = startAssetServer(args.assetPort);
  // Anything that fails from here on must take the asset server down with it.
  // Left running it holds its port, and the next start dies on EADDRINUSE
  // pointing at a port the operator never chose.
  const cleanups = [() => assets.kill()];
  const cleanup = async () => {
    for (const fn of cleanups.reverse()) {
      try {
        await fn();
      } catch {
        // Best effort: one failing teardown must not strand the rest.
      }
    }
    cleanups.length = 0;
  };
  process.on("exit", () => assets.kill());

  try {
    // The static server prints its listening line once bound; wait for it
    // rather than sleeping, so a slow start does not race the first page load.
    // If it dies instead (its port taken, say), fail on that rather than
    // waiting on output that will never come.
    await Promise.race([
      once(assets.stdout, "data"),
      once(assets, "exit").then(([code]) => {
        throw new Error(
          `static server exited with code ${code}; is --asset-port ${args.assetPort} already in use?`,
        );
      }),
    ]);
    await serve(args, cleanups);
  } catch (e) {
    await cleanup();
    throw e;
  }

  const shutdown = async () => {
    await cleanup();
    process.exit(0);
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

/**
 * Launch Chromium, installing it first if this machine has not got it.
 *
 * The server is meant to be self-contained: whoever runs it should need an
 * endpoint and nothing else, with the browser being this server's problem
 * rather than theirs. Without this a first run dies on Playwright's "Executable
 * doesn't exist" and pushes a browser-install step onto every caller, including
 * CI.
 */
async function launchBrowser(chromium, headless) {
  try {
    return await chromium.launch({ headless });
  } catch (e) {
    if (!/Executable doesn't exist|playwright install/i.test(e?.message ?? "")) {
      throw e;
    }
    console.log("chromium is missing, installing it (first run only)");
    const install = spawn("npx", ["playwright", "install", "chromium"], {
      cwd: repoRoot,
      stdio: "inherit",
    });
    const [code] = await once(install, "exit");
    if (code !== 0) {
      throw new Error(
        `could not install chromium (exit ${code}). Install it by hand: npx playwright install chromium`,
      );
    }
    return chromium.launch({ headless });
  }
}

/** Launch the browser, boot the engine, and serve until shut down. */
async function serve(args, cleanups) {
  const { chromium } = await import("@playwright/test");

  const browser = await launchBrowser(chromium, !args.headed);
  cleanups.push(() => browser.close());
  const page = await browser.newPage();
  page.on("pageerror", (e) => console.error(`[page] ${e.message}`));

  // The page is served from the asset origin so localhost counts as a secure
  // context and the module imports resolve; setContent on about:blank would
  // give neither.
  await page.route("**/__bridge", (route) =>
    route.fulfill({ status: 200, contentType: "text/html; charset=utf-8", body: PAGE }),
  );
  await page.goto(`http://127.0.0.1:${args.assetPort}/__bridge`);

  const descriptor = await page.evaluate(() => globalThis.__bridge.ready);
  console.log(
    `engine ready: contract ${descriptor.contractVersion}, ` +
      `persistence ${descriptor.persistenceMode}, ${descriptor.capabilities.length} ops`,
  );

  const server = createServer(async (req, res) => {
    try {
      const chunks = [];
      for await (const chunk of req) chunks.push(chunk);
      const body = Buffer.concat(chunks).toString("utf8");
      const target = req.headers["x-amz-target"] ?? null;
      // Auth is validated (never signature-verified) in the engine, by the same
      // code the native server uses, so lift the material out and pass it on
      // rather than deciding anything about it here.
      const auth = {
        authorization: req.headers["authorization"] ?? null,
        query: (req.url ?? "").split("?")[1] ?? "",
        hasDateHeader:
          req.headers["x-amz-date"] != null || req.headers["date"] != null,
      };

      // Everything protocol-shaped happens in the engine. This is the whole of
      // the bridge's decision-making.
      const { status, body: responseBody } = await page.evaluate(
        ([t, b, a]) => globalThis.__bridge.dispatch(t, b, a),
        [target, body, auth],
      );

      res.writeHead(status, {
        "content-type": "application/x-amz-json-1.0",
        "content-length": Buffer.byteLength(responseBody),
      });
      res.end(responseBody);
    } catch (e) {
      // A failure here is the bridge or the browser falling over, not a
      // DynamoDB error. Say so in a shape a client can read rather than
      // hanging the socket, and make it loud in the log.
      console.error(`[bridge] ${e?.message ?? e}`);
      const envelope = JSON.stringify({
        __type: "InternalServerError",
        message: `wasm bridge failure: ${e?.message ?? e}`,
      });
      if (!res.headersSent) {
        res.writeHead(500, { "content-type": "application/x-amz-json-1.0" });
      }
      res.end(envelope);
    }
  });

  cleanups.push(() => server.close());
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(args.port, "127.0.0.1", resolve);
  });
  console.log(`dynoxide wasm engine on http://127.0.0.1:${args.port}`);
}

main().catch((e) => {
  console.error(e.message ?? e);
  process.exit(1);
});
