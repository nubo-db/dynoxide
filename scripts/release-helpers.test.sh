#!/usr/bin/env bash
# Exercises the small scripts the release workflows share: is-semver.sh,
# npm-dist-tag.sh, is-crate-published.sh and check-release-binary-version.sh.
#
# Each of these replaced a copy of the same logic in two or three workflow
# files, and workflow steps only run at tag time, when a mistake is already
# public. So every one is shown to fail on a real violation here, and to stay
# quiet on a clean case, on every PR. The two that talk to the network take a
# stand-in through an environment variable rather than being skipped.
set -euo pipefail

SCRIPTS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FIXTURE=$(mktemp -d)
trap 'rm -rf "$FIXTURE"' EXIT

failures=0
checked=0

# expect <exit> <stdout must equal this, or ""> <case name> <command...>
expect() {
  local want=$1 stdout=$2 name=$3
  shift 3
  checked=$((checked + 1))
  local got=0 out
  out=$("$@" 2>/dev/null) || got=$?
  if [ "$got" -ne "$want" ]; then
    echo "FAIL  - $name: wanted exit $want, got $got"
    failures=$((failures + 1))
  elif [ -n "$stdout" ] && [ "$out" != "$stdout" ]; then
    echo "FAIL  - $name: exit $got but printed '$out', wanted '$stdout'"
    failures=$((failures + 1))
  else
    echo "ok    - $name (exit $got)"
  fi
}

# --- is-semver.sh -----------------------------------------------------------

SEMVER="$SCRIPTS/is-semver.sh"
for good in "1.2.0" "1.2.0-rc.1" "1.2.0-beta" "0.0.0" "1.2.0-0" "1.2.0-alpha-1.x-y.0"; do
  expect 0 "" "is-semver accepts $good" "$SEMVER" "$good"
done
for bad in "1.2.0-rc..1" "1.2.0-" "01.2.0" "1.2.0-rc.01" "1.2" "1.2.0.1" "v1.2.0" "1.2.0+build.5" "1.2.0-rc.1+build.5" "1.2.0 " ""; do
  expect 1 "" "is-semver refuses '$bad'" "$SEMVER" "$bad"
done

# --- npm-dist-tag.sh --------------------------------------------------------

DIST_TAG="$SCRIPTS/npm-dist-tag.sh"
expect 0 "latest" "a release publishes to latest" "$DIST_TAG" 1.2.0
expect 0 "next" "a prerelease publishes to next" "$DIST_TAG" 1.2.0-rc.1
expect 0 "next" "a hyphenated prerelease publishes to next" "$DIST_TAG" 1.2.0-beta-2
expect 1 "" "a malformed version has no dist-tag" "$DIST_TAG" "1.2.0-"
expect 1 "" "an empty version has no dist-tag" "$DIST_TAG" ""

# --- is-crate-published.sh --------------------------------------------------
#
# A stand-in for curl that answers with the next status code in a file, so a
# retry sequence can be scripted. An empty line means "curl itself failed".

PROBE="$SCRIPTS/is-crate-published.sh"
STUB="$FIXTURE/curl"
CODES="$FIXTURE/codes"
cat > "$STUB" <<'EOF'
#!/usr/bin/env bash
code=$(head -1 "$STUB_CODES")
tail -n +2 "$STUB_CODES" > "$STUB_CODES.next" && mv "$STUB_CODES.next" "$STUB_CODES"
[ -n "$code" ] || exit 7
printf '%s' "$code"
EOF
chmod +x "$STUB"
export CURL="$STUB" STUB_CODES="$CODES" RETRY_DELAY=0

printf '200\n' > "$CODES"
expect 0 "true" "a 200 means published" "$PROBE" dynoxide-rs 2.0.0

printf '404\n' > "$CODES"
expect 0 "false" "a 404 means not published" "$PROBE" dynoxide-rs 2.0.0

printf '500\n503\n404\n' > "$CODES"
expect 0 "false" "a transient error is retried until an answer arrives" "$PROBE" dynoxide-rs 2.0.0

printf '429\n429\n429\n429\n429\n' > "$CODES"
expect 1 "" "a persistent rate limit is refused rather than read as unpublished" "$PROBE" dynoxide-rs 2.0.0

printf '\n\n\n\n\n' > "$CODES"
expect 1 "" "a curl that cannot connect is refused rather than read as unpublished" "$PROBE" dynoxide-rs 2.0.0

printf '200\n' > "$CODES"
expect 1 "" "a missing version argument is refused" "$PROBE" dynoxide-rs

unset CURL STUB_CODES RETRY_DELAY

# --- check-release-binary-version.sh ----------------------------------------

BINCHECK="$SCRIPTS/check-release-binary-version.sh"
stub_binary() {
  printf '#!/bin/sh\nprintf "%s"\n' "$1" > "$FIXTURE/dynoxide"
  chmod +x "$FIXTURE/dynoxide"
}

stub_binary 'dynoxide 1.2.0\n'
expect 0 "" "a binary reporting the release version passes" "$BINCHECK" 1.2.0 "$FIXTURE/dynoxide"

stub_binary 'dynoxide 1.2.0\r\n'
expect 0 "" "a Windows line ending does not fail the comparison" "$BINCHECK" 1.2.0 "$FIXTURE/dynoxide"

stub_binary 'dynoxide 2.0.0\n'
expect 1 "" "a binary reporting the crate version is caught" "$BINCHECK" 1.2.0 "$FIXTURE/dynoxide"

stub_binary 'dynoxide 1.2.0-rc.1\n'
expect 1 "" "a binary reporting a different prerelease is caught" "$BINCHECK" 1.2.0 "$FIXTURE/dynoxide"

expect 1 "" "a missing binary fails rather than reading clean" "$BINCHECK" 1.2.0 "$FIXTURE/absent"

# The fetch path, against a local archive served over file:// so the layout
# the release publishes (a tarball holding `dynoxide` at its root) is what is
# unpacked, without touching GitHub.
ARCHIVE_ROOT="$FIXTURE/releases/download/v1.2.0"
mkdir -p "$ARCHIVE_ROOT" "$FIXTURE/pack"
stub_binary 'dynoxide 1.2.0\n'
cp "$FIXTURE/dynoxide" "$FIXTURE/pack/dynoxide"
tar czf "$ARCHIVE_ROOT/dynoxide-x86_64-unknown-linux-musl.tar.gz" -C "$FIXTURE/pack" dynoxide
expect 0 "" "a fetched archive whose binary reports the version passes" \
  env RELEASE_BASE="file://$FIXTURE/releases/download" "$BINCHECK" 1.2.0

# The same 1.2.0 binary, published under a 1.2.1 tag: the archive downloads
# fine, so the only thing the check can fail on is what the binary says.
mkdir -p "$FIXTURE/releases/download/v1.2.1"
cp "$ARCHIVE_ROOT/dynoxide-x86_64-unknown-linux-musl.tar.gz" "$FIXTURE/releases/download/v1.2.1/"
expect 1 "" "a fetched archive whose binary reports another version is caught" \
  env RELEASE_BASE="file://$FIXTURE/releases/download" "$BINCHECK" 1.2.1

expect 1 "" "an archive that cannot be fetched fails rather than reading clean" \
  env RELEASE_BASE="file://$FIXTURE/releases/download" "$BINCHECK" 9.9.9

echo "$checked cases checked, $failures failed"
[ "$failures" -eq 0 ]
