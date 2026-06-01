#!/usr/bin/env bash
#
# Build the wasm-sqlite backend into a self-contained dist/ that drops onto any
# static host. Produces the bundled Web Worker, the two .wasm assets (kept
# separate, not base64-inlined, so they cache independently of the JS bundle),
# and a manifest.json stamping the engine and contract versions.
#
# Prerequisites: a wasm32 target (`rustup target add wasm32-unknown-unknown`),
# wasm-pack, and `npm install` (pulls wa-sqlite and esbuild).
#
# Usage:
#   scripts/build-wasm.sh            # release build (wasm-opt, optimised)
#   scripts/build-wasm.sh --dev      # fast unoptimised build
#   scripts/build-wasm.sh --harness  # also include the smoke-test ops
#
# The default ships the operation-level `execute` API (the `wasm-sqlite`
# feature). `--harness` swaps in `wasm-harness`, which adds the smoke/index/
# error verification ops the harness page can drive; those stay out of the
# shipping bundle otherwise.
set -euo pipefail
cd "$(dirname "$0")/.."

profile="release"
feature="wasm-sqlite"
for arg in "$@"; do
  case "$arg" in
    --dev) profile="dev" ;;
    --harness) feature="wasm-harness" ;;
    *) echo "unknown option: $arg" >&2; exit 2 ;;
  esac
done

# 1. Compile the wasm-bindgen artefact into pkg/. The default `wasm-sqlite`
#    feature exposes the operation-level engine API (open/execute/capabilities/
#    contract_version); `--harness` adds the smoke ops on top.
wasm-pack build "--$profile" --target web --out-dir pkg -- \
  --no-default-features --features "$feature"

# 2. Bundle the Worker into one ES module. esbuild follows the chain
#    (worker -> wasm-bindgen glue -> the inlined bridge -> wa-sqlite) and
#    resolves wa-sqlite's bare specifiers from node_modules. The two
#    `new URL("*.wasm", import.meta.url)` references stay as runtime URLs that
#    resolve next to the bundle, so the .wasm files ship as siblings.
#    The smoke ops are present only in `--harness` builds, so in the default
#    `wasm-sqlite` build esbuild correctly sees them as undefined; that is by
#    design (the Worker guards them at runtime), so silence that one warning.
rm -rf dist
npx esbuild js/dynoxide-worker.js \
  --bundle \
  --format=esm \
  --log-override:import-is-undefined=silent \
  --outfile=dist/dynoxide-worker.js

# 3. Copy the two .wasm next to the bundle. wa-sqlite.wasm is the synchronous
#    (non-async) build, paired with the Worker-only AccessHandlePoolVFS; it is
#    about half the size of the Asyncify async build.
cp pkg/dynoxide_bg.wasm dist/
cp node_modules/wa-sqlite/dist/wa-sqlite.wasm dist/

# 4. Stamp a manifest so a consumer can pin and verify what it embeds. The
#    engine version is the crate version; the contract version is the envelope
#    shape the client validates on boot (see src/wasm_api.rs CONTRACT_VERSION).
engine_version="$(grep -m1 '^version' Cargo.toml | cut -d'"' -f2)"
contract_version="$(grep -m1 'CONTRACT_VERSION: u32' src/wasm_api.rs | sed -E 's/.*= *([0-9]+).*/\1/')"
cat > dist/manifest.json <<JSON
{
  "engineVersion": "$engine_version",
  "contractVersion": $contract_version,
  "feature": "$feature",
  "files": ["dynoxide-worker.js", "dynoxide_bg.wasm", "wa-sqlite.wasm"]
}
JSON

echo
echo "dist/ ready ($profile, $feature):"
for f in dist/*; do
  printf '  %-26s %8d bytes\n' "$(basename "$f")" "$(wc -c < "$f")"
done
