#!/usr/bin/env bash
# Prints the npm dist-tag a version publishes under: `next` for a prerelease,
# `latest` for anything else.
#
# npm refuses a bare publish of a prerelease ("You must specify a tag using
# --tag") and does not default one, and publishing a prerelease to `latest`
# would hand it to every `npm install dynoxide`. The rule lived in release.yml,
# npm.yml and npm/scripts/publish.sh at once, which is three places for it to
# drift; it lives here and they call this.
#
# The version is checked first, because the rule is only sound on a real
# version: `1.2.0-` has a hyphen and is not a prerelease of anything.
#
# Usage:
#   scripts/npm-dist-tag.sh 1.2.0        # latest
#   scripts/npm-dist-tag.sh 1.2.0-rc.1   # next
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VERSION="${1:-}"

if [ -z "$VERSION" ]; then
  echo "usage: $0 <version>" >&2
  exit 1
fi
"$SCRIPT_DIR/is-semver.sh" "$VERSION"

case "$VERSION" in
  *-*) echo next ;;
  *)   echo latest ;;
esac
