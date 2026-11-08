#!/usr/bin/env bash
# Exercises ci-changed-paths.sh, the gate that lets docs-only pull requests
# skip the build and test jobs.
#
# A gate like this fails quietly in the direction that matters: if it calls a
# code change "docs", the tests never run and nothing goes red. So most cases
# here are code changes that must still run the suite, including the ones that
# look like docs at a glance.
set -euo pipefail

SCRIPTS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GATE="$SCRIPTS/ci-changed-paths.sh"

failures=0
checked=0

# expect <docker> <code> <case name> <changed files, one per line>
expect() {
  local want_docker=$1 want_code=$2 name=$3 files=$4
  checked=$((checked + 1))
  local out
  out=$(printf '%s' "$files" | bash "$GATE")
  local want
  want=$(printf 'docker=%s\ncode=%s' "$want_docker" "$want_code")
  if [ "$out" != "$want" ]; then
    echo "FAIL  - $name: wanted $(tr '\n' ' ' <<< "$want"), got $(tr '\n' ' ' <<< "$out")"
    failures=$((failures + 1))
  else
    echo "ok    - $name ($(tr '\n' ' ' <<< "$out"))"
  fi
}

# Docs only: the build and test jobs skip.
expect false false "docs page" "docs/import.md"
expect false false "nested docs page" "docs/rfcs/0001-example.md"
expect false false "contributor docs" $'CONTRIBUTING.md\nAGENTS.md\nCHANGELOG.md'
expect false false "issue form and PR template" $'.github/ISSUE_TEMPLATE/community-integration.yml\n.github/PULL_REQUEST_TEMPLATE.md'
expect true false "README also runs the Docker recipe" "README.md"
expect false false "deleted docs page" "docs/old-page.md"

# Anything else runs the suite.
expect false true "source file" "src/lib.rs"
expect false true "docs plus one source file" $'docs/import.md\nsrc/lib.rs'
expect false true "rename out of code (both paths listed)" $'src/legacy.rs\ndocs/legacy.md'
expect false true "packaged crate readme" "README-crate.md"
expect false true "npm package readme" "npm/dynoxide/README.md"
expect false true "benchmarks readme" "benchmarks/README.md"
# The Docker filter anchors only the start of each path, as it always has, so
# README.mdx also runs the smoke test. That errs towards running it.
expect true true "lookalike top-level file" "README.mdx"
expect false true "backup of a docs file" "CHANGELOG.md.bak"
expect false true "docs path one level down" "src/docs/notes.md"
expect true true "workflow file" ".github/workflows/ci.yml"
expect true true "manifest" "Cargo.toml"
expect false true "empty list" ""

# benchmark-regression.yml skips on its own copy of the docs list. An entry
# there that this gate would treat as code would skip the benchmarks on a code
# change, so every entry has to come out as docs here. A narrower copy only
# costs runner time, so that direction isn't checked.
BENCH="$SCRIPTS/../.github/workflows/benchmark-regression.yml"
entries=$(sed -n '/paths-ignore:/,/^[^ ]/p' "$BENCH" | sed -n 's/^ *- "\(.*\)"$/\1/p')
if [ -z "$entries" ]; then
  echo "FAIL  - benchmark paths-ignore: found no entries in $BENCH"
  failures=$((failures + 1))
fi
while IFS= read -r entry; do
  [ -n "$entry" ] || continue
  checked=$((checked + 1))
  sample=${entry%\*\*}
  [ "$sample" = "$entry" ] || sample="${sample}example.md"
  if printf '%s\n' "$sample" | bash "$GATE" | grep -qx 'code=false'; then
    echo "ok    - benchmark skip list entry is docs: $entry"
  else
    echo "FAIL  - benchmark skip list entry is not docs to the gate: $entry"
    failures=$((failures + 1))
  fi
done <<< "$entries"

echo
echo "$checked cases, $failures failed"
[ "$failures" -eq 0 ]
