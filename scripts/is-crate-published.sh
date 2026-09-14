#!/usr/bin/env bash
# Asks crates.io whether a crate version exists. Prints `true` or `false` and
# exits 0 when it got an answer; exits 1 when it could not find out.
#
# Only a 200 or a 404 is an answer. Treating a rate limit or a network failure
# as "not published" would attempt to republish an existing version, fail the
# job, and strand the product release with no npm, container, Homebrew or
# registry update. So anything else is retried, and refused if it persists.
#
# The release path and the crate recovery workflow both need this, and their
# two copies had already drifted: one retried, the other did not.
#
# Usage:
#   scripts/is-crate-published.sh dynoxide-rs 2.0.0
#
# CURL, CRATES_API and RETRY_DELAY exist so the test can stand in for crates.io.
set -euo pipefail

CRATE="${1:-}"
VERSION="${2:-}"
if [ -z "$CRATE" ] || [ -z "$VERSION" ]; then
  echo "usage: $0 <crate> <version>" >&2
  exit 1
fi

CURL="${CURL:-curl}"
RETRY_DELAY="${RETRY_DELAY:-5}"
URL="${CRATES_API:-https://crates.io/api/v1/crates}/$CRATE/$VERSION"

CODE="000"
for attempt in 1 2 3 4 5; do
  CODE=$("$CURL" -sS -o /dev/null -w '%{http_code}' \
           -H 'User-Agent: dynoxide-release' "$URL" || echo "000")
  case "$CODE" in
    200) echo true;  exit 0 ;;
    404) echo false; exit 0 ;;
  esac
  echo "crates.io answered $CODE for $CRATE $VERSION (attempt $attempt); retrying" >&2
  sleep $((attempt * RETRY_DELAY))
done

echo "could not determine whether $CRATE $VERSION is published (last status $CODE)" >&2
exit 1
