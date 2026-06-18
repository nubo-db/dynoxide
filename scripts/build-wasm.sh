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

# 1b. wasm-bindgen copies the bridge (referenced via #[wasm_bindgen(module =
#     "/js/wa-sqlite-bridge.js")]) into pkg/snippets/<hash>/js/ but does not
#     follow its local imports - only the bare wa-sqlite specifiers, which
#     esbuild later resolves from node_modules. Copy the shared fnv1a helper the
#     bridge imports alongside it so that relative import resolves at bundle time.
shopt -s nullglob
snippet_js_dirs=(pkg/snippets/*/js)
shopt -u nullglob
if [ ${#snippet_js_dirs[@]} -eq 0 ]; then
  echo "error: no pkg/snippets/*/js directory found - did wasm-pack run, or did" >&2
  echo "       wasm-bindgen change how it emits the bridge module snippet?" >&2
  exit 1
fi
for snippet_js in "${snippet_js_dirs[@]}"; do
  cp js/fnv1a.js "$snippet_js/"
done

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

# 5. Assemble the publishable engine package. It ships the runtime assets (the
#    Worker, the two .wasm and the manifest) beside the EngineClient, so a
#    consumer depends on the package rather than this repo's source.
#    engine-client.js is the package entry; it resolves the Worker next to
#    itself and the Worker resolves its .wasm next to the Worker - the same
#    import.meta.url chain dist/ relies on, so dropping the package's files in
#    one place works from any path or origin. package.json and README.md are the
#    only checked-in files here; everything else is copied in and gitignored.
#    Skipped for --harness, which is a smoke build and is never shipped.
if [ "$feature" = "wasm-sqlite" ]; then
  pkg="npm/dynoxide-engine"

  # The JS client and Rust engine each bake in a CONTRACT_VERSION; they must ship
  # equal (the client validates against the engine on boot). Shared with CI so a
  # drift fails before publish, not in a consumer at runtime.
  scripts/check-contract-version.sh

  cp dist/dynoxide-worker.js dist/dynoxide_bg.wasm dist/wa-sqlite.wasm dist/manifest.json "$pkg/"
  cp js/engine-client.js js/engine-client.d.ts js/dynoxide-worker.d.ts "$pkg/"
  cp LICENSE-MIT LICENSE-APACHE "$pkg/"

  echo
  echo "engine package $pkg/ (npm pack-ready):"
  for f in engine-client.js engine-client.d.ts dynoxide-worker.js dynoxide-worker.d.ts dynoxide_bg.wasm wa-sqlite.wasm manifest.json; do
    printf '  %-26s %8d bytes\n' "$f" "$(wc -c < "$pkg/$f")"
  done
fi
