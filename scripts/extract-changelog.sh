#!/usr/bin/env bash
# Extract the changelog entry for a given version from CHANGELOG.md.
#
# Usage: extract-changelog.sh <version> [changelog-path]
#
# The version should be without the "v" prefix (e.g., "0.9.6").
# The changelog path defaults to CHANGELOG.md in the current directory.
#
# Outputs the changelog section to stdout. Exits non-zero if no entry is found.

set -euo pipefail

if [ $# -lt 1 ] || [ $# -gt 2 ]; then
  echo "Usage: $0 <version> [changelog-path]" >&2
  echo "  version: version string without v prefix (e.g., 0.9.6)" >&2
  echo "  changelog-path: path to CHANGELOG.md (default: CHANGELOG.md)" >&2
  exit 1
fi

VERSION="$1"
CHANGELOG_PATH="${2:-CHANGELOG.md}"

if [ ! -f "$CHANGELOG_PATH" ]; then
  echo "Error: changelog file not found: $CHANGELOG_PATH" >&2
  exit 1
fi

# Extract content between this version's header and the next version header
ENTRY=$(awk -v ver="$VERSION" '
  /^## \[/ {
    if (found) exit
    if (index($0, "[" ver "]")) found=1
    next
  }
  found { print }
' "$CHANGELOG_PATH")

if [ -z "$ENTRY" ]; then
  echo "Error: no changelog entry found for version $VERSION in $CHANGELOG_PATH" >&2
  exit 1
fi

printf '%s\n' "$ENTRY"
