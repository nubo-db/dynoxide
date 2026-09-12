#!/usr/bin/env bash
# Exercises check-stale-docs.sh against a fixture tree.
#
# The check it covers exists because an earlier version of it could only pass:
# the pattern it searched for stopped matching once the project moved past
# 1.0.0, and a missing input read as a clean result. So proving the detection
# still fires matters as much as proving the guards do, and both directions are
# asserted below: a real stale pin has to exit 1, and a clean tree has to exit 0.
set -euo pipefail

CHECK="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/check-stale-docs.sh"
FIXTURE=$(mktemp -d)
trap 'chmod -R u+rwX "$FIXTURE" 2>/dev/null || true; rm -rf "$FIXTURE"' EXIT

failures=0
checked=0

# The product and crate versions are parameters, because the check derives its
# stale pattern from them with one loop for earlier majors and another for
# earlier minors of the current major. A fixture pinned to one release shape
# only ever exercises one of those loops, so a regression in the other stays
# green here and reaches a release.
fixture() {
  local product=${1:-1.2.0} crate=${2:-2.0.0}
  local minor=${product%.*}
  rm -rf "${FIXTURE:?}"/*
  mkdir -p "$FIXTURE/docs" "$FIXTURE/npm/dynoxide" "$FIXTURE/npm/wasm-engine"
  echo "$product" > "$FIXTURE/VERSION"
  printf '[package]\nname = "dynoxide-rs"\nversion = "%s"\n' "$crate" > "$FIXTURE/Cargo.toml"
  echo "Run dynoxide:$minor to start." > "$FIXTURE/README.md"
  echo "dynoxide-rs = \"${crate%.*}\"" > "$FIXTURE/README-crate.md"
  echo "Pull dynoxide:$product from the registry." > "$FIXTURE/docs/index.md"
  echo "npm install dynoxide@$product" > "$FIXTURE/npm/dynoxide/README.md"
  echo "The engine ships as $product." > "$FIXTURE/npm/wasm-engine/README.md"
}

expect() {
  local want=$1 name=$2
  checked=$((checked + 1))
  local got=0
  (cd "$FIXTURE" && bash "$CHECK" >/dev/null 2>&1) || got=$?
  if [ "$got" -eq "$want" ]; then
    echo "ok    - $name (exit $got)"
  else
    echo "FAIL  - $name: wanted exit $want, got $got"
    failures=$((failures + 1))
  fi
}

fixture
expect 0 "a clean tree passes"

fixture
echo "Older images used dynoxide:1.1 instead." >> "$FIXTURE/docs/index.md"
expect 1 "a stale product pin is caught"

fixture
echo 'dynoxide-rs = "1.9"' >> "$FIXTURE/docs/index.md"
expect 1 "a stale crate pin is caught"

# An earlier major, which the current-release fixture above never reaches: at
# 1.2.0 the pattern has no earlier-major branch to get wrong.
fixture 2.3.0 3.0.0
echo "Older images used dynoxide:1.9 instead." >> "$FIXTURE/docs/index.md"
expect 1 "a stale pin from an earlier major is caught"

fixture 2.3.0 3.0.0
echo "Older images used dynoxide:2.1 instead." >> "$FIXTURE/docs/index.md"
expect 1 "a stale pin from an earlier minor of this major is caught"

fixture 2.3.0 3.0.0
expect 0 "a clean tree at a later release passes"

fixture
rm "$FIXTURE/npm/wasm-engine/README.md"
expect 2 "a vanished input fails rather than reading clean"

fixture
rm "$FIXTURE/VERSION"
expect 1 "an unreadable VERSION fails"

# chmod cannot take a file away from root, so this pair only means something
# as an ordinary user. CI runs as one; a root container does not.
if [ "$(id -u)" -ne 0 ]; then
  fixture
  chmod 000 "$FIXTURE/docs/index.md"
  expect 2 "an input that cannot be read fails rather than reading clean"

  fixture
  chmod 000 "$FIXTURE/docs"
  expect 2 "a directory that cannot be read fails rather than reading clean"
else
  echo "skip  - unreadable-input cases (running as root)"
fi

echo "$checked cases checked, $failures failed"
[ "$failures" -eq 0 ]
