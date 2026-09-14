#!/usr/bin/env bash
# Exercises check-versions.sh against a fixture tree.
#
# The check it covers is the gate between a tag and a release, and until this
# test existed it had only ever been seen to pass. A gate like that can be
# matching nothing. So each file the check reads is broken in turn, and the
# check has to exit 1 naming that file; a clean tree has to exit 0; and the
# version shapes npm rejects, which the check once accepted, have to be
# refused before anything is published.
set -euo pipefail

SCRIPTS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHECK="$SCRIPTS/check-versions.sh"
FIXTURE=$(mktemp -d)
trap 'rm -rf "$FIXTURE"' EXIT

failures=0
checked=0

# Writes a tree in which every source agrees: the product version in VERSION,
# the bundle manifest and the engine package; the crate version in Cargo.toml
# and Cargo.lock. The lockfile carries a second package so the check has to
# find the right block rather than the first one.
fixture() {
  local product=${1:-1.2.0} crate=${2:-2.0.0}
  rm -rf "${FIXTURE:?}"/* "${FIXTURE:?}"/.[!.]* 2>/dev/null || true
  mkdir -p "$FIXTURE/mcpb" "$FIXTURE/npm/wasm-engine" "$FIXTURE/dist"
  echo "$product" > "$FIXTURE/VERSION"
  printf '[package]\nname = "dynoxide-rs"\nversion = "%s"\n' "$crate" > "$FIXTURE/Cargo.toml"
  printf '[[package]]\nname = "aardvark"\nversion = "9.9.9"\n\n[[package]]\nname = "dynoxide-rs"\nversion = "%s"\ndependencies = [\n "aardvark",\n]\n' "$crate" > "$FIXTURE/Cargo.lock"
  printf '{"name": "dynoxide", "version": "%s"}\n' "$product" > "$FIXTURE/mcpb/manifest.json"
  printf '{"name": "@dynoxide/wasm-engine", "version": "%s"}\n' "$product" > "$FIXTURE/npm/wasm-engine/package.json"
}

# A stand-in for the built binary, reporting whatever version it is told to.
stub_binary() {
  printf '#!/bin/sh\necho "dynoxide %s"\n' "$1" > "$FIXTURE/dynoxide"
  chmod +x "$FIXTURE/dynoxide"
}

# expect <exit> <text the output must name, or ""> <case name> [check args...]
expect() {
  local want=$1 needle=$2 name=$3
  shift 3
  checked=$((checked + 1))
  local got=0 out
  out=$(cd "$FIXTURE" && DYNOXIDE_BIN="$FIXTURE/dynoxide" bash "$CHECK" "$@" 2>&1) || got=$?
  if [ "$got" -ne "$want" ]; then
    echo "FAIL  - $name: wanted exit $want, got $got"
    echo "$out" | sed 's/^/        /'
    failures=$((failures + 1))
  elif [ -n "$needle" ] && ! grep -qF -- "$needle" <<< "$out"; then
    echo "FAIL  - $name: exit $got but the output does not name '$needle'"
    echo "$out" | sed 's/^/        /'
    failures=$((failures + 1))
  else
    echo "ok    - $name (exit $got)"
  fi
}

fixture
expect 0 "versions agree" "a clean tree passes"

fixture
expect 0 "versions agree" "a clean tree passes against the expected product version" 1.2.0

fixture
expect 1 "VERSION (1.2.0) does not match" "a tag that disagrees with VERSION is caught" 1.3.0

# Each source broken in turn. The failure has to name the file, because the
# point of the check is telling the release engineer what to fix.
fixture
echo "1.2.1" > "$FIXTURE/VERSION"
expect 1 "mcpb/manifest.json" "a VERSION bump the bundle manifest missed is caught"

fixture
printf '{"name": "dynoxide", "version": "1.1.0"}\n' > "$FIXTURE/mcpb/manifest.json"
expect 1 "mcpb/manifest.json" "a stale bundle manifest is caught"

fixture
printf '{"name": "@dynoxide/wasm-engine", "version": "1.1.0"}\n' > "$FIXTURE/npm/wasm-engine/package.json"
expect 1 "npm/wasm-engine/package.json" "a stale engine package is caught"

fixture
printf '[package]\nname = "dynoxide-rs"\nversion = "2.0.1"\n' > "$FIXTURE/Cargo.toml"
expect 1 "Cargo.lock records dynoxide-rs '2.0.0', Cargo.toml says 2.0.1" "a crate bump the lockfile missed is caught"

fixture
printf '[[package]]\nname = "dynoxide-rs"\nversion = "1.9.0"\n' > "$FIXTURE/Cargo.lock"
expect 1 "Cargo.lock" "a stale lockfile is caught"

fixture
printf '[[package]]\nname = "aardvark"\nversion = "9.9.9"\n' > "$FIXTURE/Cargo.lock"
expect 1 "Cargo.lock records dynoxide-rs ''" "a lockfile that has lost the crate is caught"

fixture
rm "$FIXTURE/mcpb/manifest.json"
expect 1 "mcpb/manifest.json is missing" "a vanished source fails rather than reading clean"

fixture
: > "$FIXTURE/VERSION"
expect 1 "VERSION is empty" "an empty VERSION fails"

fixture
rm "$FIXTURE/VERSION"
expect 1 "VERSION is missing" "a missing VERSION fails rather than reading clean"

fixture
printf '[package]\nname = "dynoxide-rs"\n' > "$FIXTURE/Cargo.toml"
expect 1 "could not read the crate version" "a Cargo.toml with no version fails"

# The shapes npm rejects. Each is written to every product source, so the
# only thing wrong with the tree is the shape itself.
for bad in "1.2.0-rc..1" "1.2.0-" "01.2.0" "1.2.0-rc.01" "1.2.0+build.5" "v1.2.0"; do
  fixture "$bad"
  expect 1 "'$bad' in VERSION is not semver" "product version $bad is refused"
done

fixture 1.2.0 "2.0.0-rc..1"
expect 1 "'2.0.0-rc..1' in Cargo.toml is not semver" "crate version 2.0.0-rc..1 is refused"

for good in "1.2.0-rc.1" "1.2.0-beta" "1.2.0-alpha.1-x" "1.10.0"; do
  fixture "$good"
  expect 0 "versions agree" "product version $good is accepted" "$good"
done

# Built artefacts, which only the release path asks for.
fixture
stub_binary 1.2.0
expect 0 "versions agree" "a built binary reporting the product version passes" 1.2.0 --built

fixture
stub_binary 2.0.0
expect 1 "printed 'dynoxide 2.0.0', expected 'dynoxide 1.2.0'" "a built binary reporting the crate version is caught" 1.2.0 --built

fixture
expect 1 "no executable" "a missing binary fails rather than reading clean" 1.2.0 --built

fixture
stub_binary 1.2.0
printf '{"engineVersion": "1.1.0"}\n' > "$FIXTURE/dist/manifest.json"
expect 1 "dist/manifest.json" "a stale engine manifest is caught" 1.2.0 --built

echo "$checked cases checked, $failures failed"
[ "$failures" -eq 0 ]
