#!/usr/bin/env bash
# Prints the PyPI (PEP 440) version for a product version, and exits 1 with a
# reason when there isn't one.
#
#   1.8.0            -> 1.8.0
#   1.8.0 1          -> 1.8.0.post1   (a fix to the Python package alone)
#   1.9.0-rc.2       -> 1.9.0rc2
#   1.9.0-beta.1     -> 1.9.0b1
#   1.9.0-alpha      -> 1.9.0a0
#
# Other prerelease shapes have no PEP 440 form, so they don't go to PyPI. A
# post number applies only to a release: a release candidate that needs a fix
# gets the next candidate instead.
#
# Usage:
#   scripts/pypi-version.sh <version> [post]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VERSION="${1:-}"
POST="${2:-}"

if [ -z "$VERSION" ]; then
  echo "usage: $0 <version> [post]" >&2
  exit 1
fi
"$SCRIPT_DIR/is-semver.sh" "$VERSION"

if [ -n "$POST" ] && ! [[ $POST =~ ^[1-9][0-9]*$ ]]; then
  echo "post number '$POST' must be a whole number from 1, with no leading zero" >&2
  exit 1
fi

BASE="${VERSION%%-*}"
if [ "$BASE" = "$VERSION" ]; then
  echo "$VERSION${POST:+.post$POST}"
  exit 0
fi

if [ -n "$POST" ]; then
  echo "'$VERSION' is a prerelease, so it can't have a post-release; cut the next prerelease instead" >&2
  exit 1
fi

PRE="${VERSION#*-}"
if [[ $PRE =~ ^(alpha|beta|rc)(\.(0|[1-9][0-9]*))?$ ]]; then
  case "${BASH_REMATCH[1]}" in
    alpha) LETTER=a ;;
    beta) LETTER=b ;;
    rc) LETTER=rc ;;
  esac
  echo "$BASE$LETTER${BASH_REMATCH[3]:-0}"
  exit 0
fi

echo "'$VERSION' has no PyPI form: a prerelease must be alpha, beta or rc, optionally followed by .N" >&2
exit 1
