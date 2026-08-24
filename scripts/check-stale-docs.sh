#!/usr/bin/env bash
# Fails when a stale version pin or leftover preview language reaches the docs.
#
# The allowlist is upgrade history: sections describing what changed in an older
# release name that release on purpose, and must survive. Without the allowlist
# this check can never pass, and a check that can never pass gets ignored.
set -euo pipefail

ALLOW='## Upgrading to |is a breaking release'

hits=$(grep -rnE '0\.1[0-9]|preview' \
        README.md docs/ npm/dynoxide/README.md npm/wasm-engine/README.md \
        2>/dev/null \
      | grep -v 'docs/versioning.md' \
      | grep -vE "$ALLOW" || true)

if [ -n "$hits" ]; then
  echo "Stale version pin or preview language:"
  echo "$hits"
  exit 1
fi
echo "docs clean"
