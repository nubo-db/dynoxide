#!/usr/bin/env bash
#
# Build the wasm-sqlite backend into a self-contained dist/ that drops onto any
# static host. Produces three files: the bundled Web Worker plus the two .wasm
# assets (kept separate, not base64-inlined, so they cache independently of the
# JS bundle).
#
# Prerequisites: a wasm32 target (`rustup target add wasm32-unknown-unknown`),
# wasm-pack, and `npm install` (pulls wa-sqlite and esbuild).
#
# Usage:
#   scripts/build-wasm.sh            # release build (wasm-opt, optimised)
#   scripts/build-wasm.sh --dev      # fast unoptimised build
#
set -euo pipefail
cd "$(dirname "$0")/.."

profile="release"
if [ "${1:-}" = "--dev" ]; then
  profile="dev"
fi

# 1. Compile the wasm-bindgen artefact into pkg/. The wasm-harness feature
#    provides the Worker's current RPC entry points (the smoke/index/error
#    verification ops); a production op-level API slots into the same Worker
#    without changing this pipeline.
wasm-pack build "--$profile" --target web --out-dir pkg -- \
  --no-default-features --features wasm-harness

# 2. Bundle the Worker into one ES module. esbuild follows the chain
#    (worker -> wasm-bindgen glue -> the inlined bridge -> wa-sqlite) and
#    resolves wa-sqlite's bare specifiers from node_modules. The two
#    `new URL("*.wasm", import.meta.url)` references stay as runtime URLs that
#    resolve next to the bundle, so the .wasm files ship as siblings.
rm -rf dist
npx esbuild js/dynoxide-worker.js \
  --bundle \
  --format=esm \
  --outfile=dist/dynoxide-worker.js

# 3. Copy the two .wasm next to the bundle. wa-sqlite.wasm is the synchronous
#    (non-async) build, paired with the Worker-only AccessHandlePoolVFS; it is
#    about half the size of the Asyncify async build.
cp pkg/dynoxide_bg.wasm dist/
cp node_modules/wa-sqlite/dist/wa-sqlite.wasm dist/

echo
echo "dist/ ready ($profile):"
for f in dist/*; do
  printf '  %-26s %8d bytes\n' "$(basename "$f")" "$(wc -c < "$f")"
done
