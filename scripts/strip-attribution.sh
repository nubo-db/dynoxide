#!/usr/bin/env bash
# Strip AI attribution lines from a file in-place.
#
# Removes lines containing "Co-Authored-By:" or "Generated with Claude Code".
#
# Usage: strip-attribution.sh <file>

set -euo pipefail

if [ $# -ne 1 ]; then
  echo "Usage: $0 <file>" >&2
  echo "  file: path to the file to strip attribution from (modified in-place)" >&2
  exit 1
fi

FILE="$1"

if [ ! -f "$FILE" ]; then
  echo "Error: file not found: $FILE" >&2
  exit 1
fi

sed -i '/Co-Authored-By:/d; /Generated with Claude Code/d' "$FILE"
