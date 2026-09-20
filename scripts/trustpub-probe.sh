#!/usr/bin/env bash
# Confirms crates.io Trusted Publishing is wired up, without ever minting a
# publish token. Exits 0 when every workflow named on the command line is
# registered against this repository, 1 otherwise.
#
# The trick is that crates.io matches an exchange on the workflow filename,
# and the preflight workflow deliberately has no config of its own. So the
# exchange is refused, and the refusal is the useful part: crates.io only
# reaches the filename check after the repository and the owner ID have
# already matched, and the message it returns names every workflow filename
# that IS configured. A refusal listing release.yml and publish-crate.yml
# therefore proves both publish paths are registered, against the right
# repository, without a token ever existing.
#
# Giving this workflow a config of its own would be the obvious way to probe,
# and it is the wrong one: the preflight runs on workflow_dispatch with no
# environment gate, so a config would let anyone who can dispatch it mint a
# real publish token. If one is ever added by mistake the exchange succeeds,
# and this script revokes the token it was handed and fails.
#
# What it cannot see: the environment on each config. crates.io checks the
# environment only for configs that survived the filename match, and none do
# here. A wrong environment still surfaces at release time, not before.
#
# Usage:
#   scripts/trustpub-probe.sh release.yml publish-crate.yml
#
# CURL, TRUSTPUB_API and OIDC_TOKEN exist so the test can stand in for both
# GitHub's OIDC endpoint and crates.io.
set -euo pipefail

if [ "$#" -eq 0 ]; then
  echo "usage: $0 <workflow filename> [workflow filename...]" >&2
  exit 1
fi
EXPECTED=("$@")

CURL="${CURL:-curl}"
API="${TRUSTPUB_API:-https://crates.io/api/v1/trusted_publishing/tokens}"
UA='dynoxide-release-preflight'

# GitHub mints the JWT; audience is the registry host, as the official action
# derives it. OIDC_TOKEN short-circuits this for the test.
jwt="${OIDC_TOKEN:-}"
if [ -z "$jwt" ]; then
  if [ -z "${ACTIONS_ID_TOKEN_REQUEST_URL:-}" ] || [ -z "${ACTIONS_ID_TOKEN_REQUEST_TOKEN:-}" ]; then
    echo "::error::no OIDC request URL or token in the environment. Does this job have 'id-token: write'?" >&2
    exit 1
  fi
  jwt=$("$CURL" -sS -H "Authorization: bearer $ACTIONS_ID_TOKEN_REQUEST_TOKEN" \
          "${ACTIONS_ID_TOKEN_REQUEST_URL}&audience=crates.io" \
        | sed -n 's/.*"value":"\([^"]*\)".*/\1/p')
  if [ -z "$jwt" ]; then
    echo "::error::GitHub returned no OIDC token" >&2
    exit 1
  fi
fi

body=$("$CURL" -sS -X POST "$API" \
         -H 'Content-Type: application/json' \
         -H "User-Agent: $UA" \
         -w $'\n%{http_code}' \
         -d "{\"jwt\": \"$jwt\"}" || true)
status="${body##*$'\n'}"
body="${body%$'\n'*}"

# A detail string if the body is crates.io's error envelope, the raw body if
# it is anything else. jq is on every GitHub runner; the fallback keeps the
# script usable without it rather than failing on a missing tool.
detail=$(printf '%s' "$body" | jq -r '.errors[0].detail // empty' 2>/dev/null || true)
[ -n "$detail" ] || detail="$body"

case "$status" in
  200)
    # Someone gave this workflow a config. Hand the token straight back.
    token=$(printf '%s' "$body" | jq -r '.token // empty' 2>/dev/null || true)
    if [ -n "$token" ]; then
      "$CURL" -sS -X DELETE "$API" \
        -H "Authorization: Bearer $token" \
        -H "User-Agent: $UA" >/dev/null 2>&1 || true
      echo "revoked the token crates.io issued" >&2
    fi
    echo "::error::crates.io issued a publish token to this workflow, so a Trusted Publishing config names it. Remove that config: this workflow is dispatchable without an environment gate." >&2
    exit 1
    ;;
  400)
    : # the expected path; classified below
    ;;
  *)
    echo "::error::crates.io answered $status, which this probe cannot interpret: $detail" >&2
    exit 1
    ;;
esac

case "$detail" in
  *"does not match the workflow filename"*)
    : # reached the filename check, so repository and owner already matched
    ;;
  *"No Trusted Publishing config found"*)
    echo "::error::crates.io has no Trusted Publishing config for this repository at all: $detail" >&2
    exit 1
    ;;
  *"does not match the repository owner ID"*)
    echo "::error::the Trusted Publishing config is stale and must be recreated: $detail" >&2
    exit 1
    ;;
  *)
    echo "::error::crates.io refused the exchange for a reason this probe does not recognise: $detail" >&2
    exit 1
    ;;
esac

echo "crates.io reports: $detail"

# Everything after "Expected workflow filenames:" is the configured set. Fail
# if that list is empty rather than reporting a pass against nothing.
configured="${detail##*Expected workflow filenames: }"
if [ "$configured" = "$detail" ] || [ -z "$configured" ]; then
  echo "::error::crates.io named no configured workflow filenames: $detail" >&2
  exit 1
fi

missing=()
for workflow in "${EXPECTED[@]}"; do
  case "$configured" in
    *"\`$workflow\`"*) echo "  $workflow is registered" ;;
    *) missing+=("$workflow") ;;
  esac
done

if [ "${#missing[@]}" -ne 0 ]; then
  echo "::error::not registered on crates.io: ${missing[*]}. Configured: $configured" >&2
  exit 1
fi

echo "${#EXPECTED[@]} workflow(s) checked, all registered on crates.io"
