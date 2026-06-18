#!/usr/bin/env bash
#
# Fail if the JS client's CONTRACT_VERSION and the Rust engine's disagree. The
# two are hand-written constants that must ship equal - the client validates its
# version against the engine on boot - so catch drift in CI, not at publish.
# Run standalone (CI) or from build-wasm.sh; needs no wasm build.
set -euo pipefail
cd "$(dirname "$0")/.."

rust=$(grep -m1 'CONTRACT_VERSION: u32' src/wasm_api.rs | sed -E 's/.*= *([0-9]+).*/\1/')
js=$(grep -m1 'export const CONTRACT_VERSION' js/engine-client.js | sed -E 's/[^0-9]*([0-9]+).*/\1/')

# Fail loudly if either constant could not be read, so a moved or reformatted
# declaration never lets an empty-vs-empty comparison pass silently.
[ -n "$rust" ] || { echo "error: CONTRACT_VERSION not found in src/wasm_api.rs" >&2; exit 1; }
[ -n "$js" ] || { echo "error: CONTRACT_VERSION not found in js/engine-client.js" >&2; exit 1; }

if [ "$js" != "$rust" ]; then
  echo "error: CONTRACT_VERSION drift - js/engine-client.js=$js, src/wasm_api.rs=$rust" >&2
  exit 1
fi
echo "CONTRACT_VERSION ok ($rust)"
