#!/usr/bin/env bash
# Fails when a stale version pin or leftover preview wording reaches the docs.
#
# The stale pattern is derived from the version in Cargo.toml rather than
# written out, because a literal range stops matching the moment the project
# moves past it: the first version of this script looked for `0.1[0-9]`, which
# covered nothing once 1.0.0 shipped, leaving a check that could only pass.
#
# Excluded on purpose:
#   docs/versioning.md   states the policy, so it names version shapes.
#   docs/rfcs, docs/adr  historical records; an old version in them is the point.
#   "## Upgrading to X"  upgrade notes name the release they are about.
set -euo pipefail

CURRENT=$(grep -m1 -oE '^version = "[0-9]+\.[0-9]+\.[0-9]+"' Cargo.toml \
  | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' || true)
if [ -z "$CURRENT" ]; then
  echo "::error::could not read the package version from Cargo.toml" >&2
  exit 1
fi
MAJOR=${CURRENT%%.*}
MINOR=${CURRENT#*.}; MINOR=${MINOR%%.*}

# Every release line below the current one: earlier majors whole, then the
# earlier minors of this major. At 1.0.0 that is `0.x`; at 2.3.0 it is `0.x`,
# `1.x`, `2.0`, `2.1` and `2.2`.
stale=()
for ((m = 0; m < MAJOR; m++)); do stale+=("${m}\\.[0-9]+"); done
for ((n = 0; n < MINOR; n++)); do stale+=("${MAJOR}\\.${n}"); done
STALE_ALT=$(IFS='|'; echo "${stale[*]}")

# Only where a version is actually being pinned: a `v`-prefixed tag, or a
# number attached to the package name (`dynoxide:0.13.0`, `dynoxide@0.13`,
# `dynoxide-rs = "0.13"`). Matching bare dotted numbers instead catches
# `--host 0.0.0.0` and benchmark figures like `0.2ms`, which is noise rather
# than a finding.
# The `[^0-9.]` before each alternative matters: without it the `0.0` inside a
# current `dynoxide:1.0.0` reads as a stale `0.x` pin.
if [ -n "$STALE_ALT" ]; then
  PATTERN="\\bpreview\\b"
  PATTERN="${PATTERN}|[vV](${STALE_ALT})"
  PATTERN="${PATTERN}|[\"'^](${STALE_ALT})"
  PATTERN="${PATTERN}|dynoxide[^[:space:]]{0,14}[^0-9.[:space:]](${STALE_ALT})"
else
  PATTERN='\bpreview\b'
fi

ALLOW='## Upgrading to |is a breaking release'

hits=$(grep -rniE "$PATTERN" \
        README.md docs/ npm/dynoxide/README.md npm/wasm-engine/README.md \
        --exclude-dir=rfcs --exclude-dir=adr \
        2>/dev/null \
      | grep -v '^docs/versioning\.md:' \
      | grep -vE "$ALLOW" || true)

if [ -n "$hits" ]; then
  echo "Stale version pin or preview wording (current version is ${CURRENT}):"
  echo "$hits"
  exit 1
fi
echo "docs clean (checked against ${CURRENT})"
