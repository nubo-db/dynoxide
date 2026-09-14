#!/usr/bin/env bash
# Checks that a released binary reports the version a formula, package or tag
# is about to declare for it.
#
# The Homebrew formula carries a test block that does this, but nothing ran
# it: Homebrew only executes it on `brew test`, so a formula whose binary
# reported the wrong version reached the tap unchallenged. This does what that
# block does, against the same archive the formula points at, before the tap
# moves. The release path and the Homebrew recovery workflow both call it.
#
# Usage:
#   scripts/check-release-binary-version.sh 1.2.0             # fetch the release archive and probe it
#   scripts/check-release-binary-version.sh 1.2.0 ./dynoxide  # probe this binary instead
#
# RELEASE_BASE exists so the test can point the fetch somewhere local.
set -euo pipefail

VERSION="${1:-}"
BIN="${2:-}"
if [ -z "$VERSION" ]; then
  echo "usage: $0 <version> [binary-path]" >&2
  exit 1
fi

if [ -z "$BIN" ]; then
  WORK=$(mktemp -d)
  trap 'rm -rf "$WORK"' EXIT
  ARCHIVE="${RELEASE_BASE:-https://github.com/nubo-db/dynoxide/releases/download}/v${VERSION}/dynoxide-x86_64-unknown-linux-musl.tar.gz"
  if ! curl -fsSL "$ARCHIVE" -o "$WORK/probe.tar.gz"; then
    echo "::error::could not fetch $ARCHIVE, so the released binary was not checked" >&2
    exit 1
  fi
  tar xzf "$WORK/probe.tar.gz" -C "$WORK"
  BIN="$WORK/dynoxide"
fi

if [ ! -x "$BIN" ]; then
  echo "::error::no executable at $BIN to check" >&2
  exit 1
fi

actual="$("$BIN" --version | tr -d '\r')"
echo "released binary reports: $actual"
if [ "$actual" != "dynoxide $VERSION" ]; then
  echo "::error::released binary reports '$actual', expected 'dynoxide $VERSION'" >&2
  exit 1
fi
