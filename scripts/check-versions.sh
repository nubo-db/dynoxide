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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXPECTED="${1:-}"
CHECK_BUILT=false
for arg in "$@"; do [ "$arg" = "--built" ] && CHECK_BUILT=true; done

fail() { echo "::error::$*" >&2; FAILED=1; }
FAILED=0

read_version_file() {
  sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' VERSION | head -1
}

[ -f VERSION ] || { fail "VERSION is missing"; exit 1; }
PRODUCT="$(read_version_file)"
# `|| true` because under pipefail a Cargo.toml with no version line would
# otherwise end the script here, silently, before the message below.
CRATE="$(grep -m1 '^version = ' Cargo.toml | cut -d'"' -f2 || true)"

[ -n "$PRODUCT" ] || fail "VERSION is empty"
[ -n "$CRATE" ] || fail "could not read the crate version from Cargo.toml"

# The shape rule is shared with the release workflow and build.rs, so a
# version that passes here is one every later stage accepts. The regex this
# replaced took `1.2.0-rc..1`, which npm refuses once the GitHub Release is
# already public.
if [ -n "$PRODUCT" ]; then
  "$SCRIPT_DIR/is-semver.sh" "$PRODUCT" || fail "product version '$PRODUCT' in VERSION is not semver"
fi
if [ -n "$CRATE" ]; then
  "$SCRIPT_DIR/is-semver.sh" "$CRATE" || fail "crate version '$CRATE' in Cargo.toml is not semver"
fi

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

# The Python package carries the engine version it was built for, and its own
# version in PEP 440 form. A post-release such as 1.8.0.post1 is stamped by
# the publishing workflow and never committed, so the committed value is the
# plain mapping. A prerelease with no PEP 440 form doesn't go to PyPI, so
# only the engine version is checked for one.
PY_VERSION_FILE=python/src/dynoxide/_version.py
if [ -f "$PY_VERSION_FILE" ]; then
  py_field() { sed -n "s/^$1 = \"\\(.*\\)\"\$/\\1/p" "$PY_VERSION_FILE" | head -1; }
  PY_ENGINE="$(py_field ENGINE_VERSION)"
  PY_PACKAGE="$(py_field __version__)"
  [ "$PY_ENGINE" = "$PRODUCT" ] || fail "ENGINE_VERSION in $PY_VERSION_FILE is '$PY_ENGINE', expected '$PRODUCT'"
  if [ -n "$PRODUCT" ] && WANT_PY="$("$SCRIPT_DIR/pypi-version.sh" "$PRODUCT" 2>/dev/null)"; then
    [ "$PY_PACKAGE" = "$WANT_PY" ] || fail "__version__ in $PY_VERSION_FILE is '$PY_PACKAGE', expected '$WANT_PY'"
  else
    echo "note: $PRODUCT has no PyPI form, so __version__ in $PY_VERSION_FILE isn't checked" >&2
  fi
else
  fail "$PY_VERSION_FILE is missing"
fi

# The crate lockfile has to agree with the crate, or a publish goes out against
# a stale lock. benchmarks/Cargo.lock is an unpublished dev package and is not
# part of this.
LOCKED="$(awk -v RS='' '/name = "dynoxide-rs"/ {print}' Cargo.lock | grep -m1 '^version = ' | cut -d'"' -f2 || true)"
[ "$LOCKED" = "$CRATE" ] || fail "Cargo.lock records dynoxide-rs '$LOCKED', Cargo.toml says $CRATE"

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
