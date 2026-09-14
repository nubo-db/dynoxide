#!/usr/bin/env bash
# Exits 0 when its argument is a version this project can release, and 1 with
# a reason on stderr otherwise.
#
# The shape is semver 2.0.0 without build metadata: MAJOR.MINOR.PATCH with no
# leading zeros, then an optional prerelease of dot-separated identifiers, each
# non-empty, drawn from [0-9A-Za-z-], and with no leading zero when purely
# numeric. Build metadata (`1.2.0+build.5`) is refused on purpose: the version
# becomes a git tag, a container image tag, which cannot hold a `+`, and an npm
# version, which drops it, so two releases would differ in name only.
#
# The rule used to be a looser regex in three places, and all three accepted
# `1.2.0-rc..1`, which npm rejects only after the GitHub Release exists. Every
# check of a version's shape calls this, and build.rs applies the same rule
# when it compiles the product version in.
#
# Usage:
#   scripts/is-semver.sh 1.2.0-rc.1
set -euo pipefail

VERSION="${1:-}"

NUM='(0|[1-9][0-9]*)'
IDENT='(0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)'
SEMVER="^${NUM}\\.${NUM}\\.${NUM}(-${IDENT}(\\.${IDENT})*)?$"

if printf '%s' "$VERSION" | grep -qE "$SEMVER"; then
  exit 0
fi

echo "'$VERSION' is not semver: expected MAJOR.MINOR.PATCH with no leading zeros, then an optional -prerelease of dot-separated non-empty identifiers, and no build metadata" >&2
exit 1
