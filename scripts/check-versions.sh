#!/usr/bin/env bash
# Checks every version-bearing source in the tree against the stream it belongs
# to, and fails naming the file that disagrees.
#
# Two streams (see docs/versioning.md): the product version in VERSION covers
# everything installable, and the crate version in Cargo.toml covers
# dynoxide-rs alone. Nearly every release mistake in this area is one file left
# behind, so the point of this script is to be runnable in ordinary CI rather
# than only at tag time, when the tag has already been pushed.
#
# Usage:
#   scripts/check-versions.sh                 # sources agree with each other
#   scripts/check-versions.sh 1.2.0           # ...and match this product version
#   scripts/check-versions.sh 1.2.0 --built   # ...and so do the built artefacts
set -euo pipefail

EXPECTED="${1:-}"
CHECK_BUILT=false
for arg in "$@"; do [ "$arg" = "--built" ] && CHECK_BUILT=true; done

fail() { echo "::error::$*" >&2; FAILED=1; }
FAILED=0

read_version_file() {
  sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' VERSION | head -1
}

PRODUCT="$(read_version_file)"
CRATE="$(grep -m1 '^version = ' Cargo.toml | cut -d'"' -f2)"

[ -n "$PRODUCT" ] || fail "VERSION is empty"
[ -n "$CRATE" ] || fail "could not read the crate version from Cargo.toml"

SEMVER='^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$'
printf '%s' "$PRODUCT" | grep -qE "$SEMVER" || fail "product version '$PRODUCT' is not semver"
printf '%s' "$CRATE"   | grep -qE "$SEMVER" || fail "crate version '$CRATE' is not semver"

if [ -n "$EXPECTED" ] && [ "$EXPECTED" != "--built" ] && [ "$PRODUCT" != "$EXPECTED" ]; then
  fail "VERSION ($PRODUCT) does not match the expected product version ($EXPECTED)"
fi

# Product-stream sources. Each of these is published under the product number,
# and each has been wrong in the past because nothing compared it.
check_json() {
  local file="$1" query="$2" label="$3" want="$4"
  [ -f "$file" ] || { fail "$file is missing"; return; }
  local got
  got="$(node -p "JSON.parse(require('fs').readFileSync('$file','utf8'))$query" 2>/dev/null || echo "")"
  [ "$got" = "$want" ] || fail "$label in $file is '$got', expected '$want'"
}

check_json mcpb/manifest.json '.version' "bundle version" "$PRODUCT"
check_json npm/wasm-engine/package.json '.version' "package version" "$PRODUCT"

# The crate lockfile has to agree with the crate, or a publish goes out against
# a stale lock. benchmarks/Cargo.lock is an unpublished dev package and is not
# part of this.
LOCKED="$(awk -v RS='' '/name = "dynoxide-rs"/ {print}' Cargo.lock | grep -m1 '^version = ' | cut -d'"' -f2)"
[ "$LOCKED" = "$CRATE" ] || fail "Cargo.lock records dynoxide-rs $LOCKED, Cargo.toml says $CRATE"

if [ "$CHECK_BUILT" = true ]; then
  # Built artefacts. These are what a user actually sees, and they are the
  # surfaces that silently reported the crate version before the split.
  BIN="${DYNOXIDE_BIN:-target/release/dynoxide}"
  if [ -x "$BIN" ]; then
    for flag in --version -V; do
      got="$("$BIN" "$flag" | tr -d '\r')"
      [ "$got" = "dynoxide $PRODUCT" ] || fail "$BIN $flag printed '$got', expected 'dynoxide $PRODUCT'"
    done
  else
    fail "no executable at $BIN to check (set DYNOXIDE_BIN)"
  fi

  for manifest in dist/manifest.json npm/wasm-engine/manifest.json; do
    if [ -f "$manifest" ]; then
      check_json "$manifest" '.engineVersion' "engineVersion" "$PRODUCT"
    fi
  done
fi

if [ "$FAILED" -ne 0 ]; then
  echo "version check failed (product $PRODUCT, crate $CRATE)" >&2
  exit 1
fi
echo "versions agree (product $PRODUCT, crate $CRATE)"
