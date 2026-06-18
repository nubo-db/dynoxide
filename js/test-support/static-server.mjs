/**
 * Minimal static file server for the browser engine harness.
 *
 * Serves the repo root so /js/, /dist/ and /harness/ resolve, and sets
 * application/wasm so the engine's .wasm load through instantiateStreaming
 * rather than the slower arrayBuffer fallback - the same content type a real
 * static host must serve (see docs/wasm.md). Used by playwright.config.js as
 * the test webServer; not a production server.
 */
import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import { extname, join, normalize, sep } from "node:path";

const root = process.cwd();
const port = Number(process.argv[2] || 8081);

const TYPES = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".mjs": "text/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".wasm": "application/wasm",
};

const server = createServer(async (req, res) => {
  try {
    const { pathname } = new URL(req.url, "http://localhost");
    const path = join(root, normalize(decodeURIComponent(pathname)));
    // Confine to the repo root: reject anything that escapes it.
    if (path !== root && !path.startsWith(root + sep)) {
      res.writeHead(403).end("forbidden");
      return;
    }
    const body = await readFile(path);
    res.writeHead(200, {
      "content-type": TYPES[extname(path)] || "application/octet-stream",
    });
    res.end(body);
  } catch {
    res.writeHead(404).end("not found");
  }
});

server.listen(port, "127.0.0.1", () => {
  console.log(`harness server on http://127.0.0.1:${port}`);
});
