#!/usr/bin/env bash
# Fails when a stale version pin or leftover preview wording reaches the docs.
#
# The stale pattern is derived from the current version rather than written
# out, because a literal range stops matching the moment the project moves
# past it: the first version of this script looked for `0.1[0-9]`, which
# covered nothing once 1.0.0 shipped, leaving a check that could only pass.
#
# Two streams, so two passes. Nearly everything these docs pin is the product
# (image tags, npm pins, Action pins), which comes from VERSION. The crate is
# pinned separately as `dynoxide-rs = "..."` and tracks Cargo.toml. Deriving
# both from Cargo.toml would report every correct product pin as stale the
# moment the crate moved ahead of it.
#
# Excluded on purpose:
#   docs/versioning.md   states the policy, so it names version shapes.
#   docs/rfcs, docs/adr  historical records; an old version in them is the point.
#   "## Upgrading to X"  upgrade notes name the release they are about.
set -euo pipefail

CURRENT=$(tr -d '[:space:]' < VERSION || true)
if [ -z "$CURRENT" ]; then
  echo "::error::could not read the product version from VERSION" >&2
  exit 1
fi
CRATE=$(grep -m1 -oE '^version = "[0-9]+\.[0-9]+\.[0-9]+"' Cargo.toml \
  | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' || true)
if [ -z "$CRATE" ]; then
  echo "::error::could not read the crate version from Cargo.toml" >&2
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
  # A tilde range needs a trailing boundary: without it `~0.2ms` in a benchmark
  # table reads as a stale `0.x` pin, while `~1.1.0` is a real one.
  PATTERN="${PATTERN}|~(${STALE_ALT})[^0-9a-z]"
  PATTERN="${PATTERN}|dynoxide[^[:space:]]{0,14}[^0-9.[:space:]](${STALE_ALT})"
else
  PATTERN='\bpreview\b'
fi

ALLOW='## Upgrading to |is a breaking release'

# A crate pin is not a product pin. `dynoxide-rs = "2.0"` is correct while the
# product sits at 1.2, so those lines are held out of the product pass and
# checked against Cargo.toml below.
CRATE_PIN='dynoxide-rs'

hits=$(grep -rniE "$PATTERN" \
        README.md README-crate.md docs/ npm/dynoxide/README.md npm/wasm-engine/README.md \
        --exclude-dir=rfcs --exclude-dir=adr \
        2>/dev/null \
      | grep -v '^docs/versioning\.md:' \
      | grep -vE "${CRATE_PIN}[^\"]*\"[\^~]?[0-9]" \
      | grep -vE "$ALLOW" || true)

if [ -n "$hits" ]; then
  echo "Stale product version pin or preview wording (product version is ${CURRENT}):"
  echo "$hits"
  exit 1
fi

# Crate pins: a quoted version attached to the crate name, as in
# `dynoxide-rs = "2.0"` or `dynoxide-rs = { version = "2.0" }`. The quotes
# matter: prose naming the crate beside an unrelated number, such as the MSRV,
# is not a pin.
CRATE_MM="${CRATE%.*}"
CRATE_MM_RE="${CRATE_MM//./\\.}"
crate_hits=$(grep -rniE "${CRATE_PIN}[^\"]*\"[\^~]?[0-9]+\\.[0-9]+" \
        README.md README-crate.md docs/ npm/dynoxide/README.md npm/wasm-engine/README.md \
        --exclude-dir=rfcs --exclude-dir=adr \
        2>/dev/null \
      | grep -v '^docs/versioning\.md:' \
      | grep -vE "$ALLOW" \
      | grep -vE "${CRATE_PIN}[^\"]*\"[\^~]?${CRATE_MM_RE}([^0-9]|$)" || true)

if [ -n "$crate_hits" ]; then
  echo "Crate pin does not name the current crate version (crate is ${CRATE}):"
  echo "$crate_hits"
  exit 1
fi

echo "docs clean (product ${CURRENT}, crate ${CRATE})"
