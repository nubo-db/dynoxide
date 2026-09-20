#!/usr/bin/env bash
# Exercises trustpub-probe.sh against a stand-in crates.io.
#
# The probe passes by reading a refusal, which is an easy thing to get wrong
# in the direction that always says yes: match the wrong substring, or treat
# an empty list of configured workflows as a pass, and it reports success
# whatever crates.io said. So every answer crates.io can give is fed in here,
# including a success, and the probe has to reach the right verdict for each.
set -euo pipefail

SCRIPTS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROBE="$SCRIPTS/trustpub-probe.sh"
FIXTURE=$(mktemp -d)
trap 'rm -rf "$FIXTURE"' EXIT

failures=0
checked=0

# A curl stand-in. It ignores the request and replays a canned status and body
# from the fixture, appending the status the way `curl -w` would. A DELETE is
# recorded rather than answered, so the revoke path can be asserted on.
stub_curl() {
  local status=$1 body=$2
  printf '%s' "$status" > "$FIXTURE/status"
  printf '%s' "$body" > "$FIXTURE/body"
  : > "$FIXTURE/deleted"
  cat > "$FIXTURE/curl" <<'STUB'
#!/usr/bin/env bash
for arg in "$@"; do
  if [ "$arg" = "DELETE" ]; then echo yes > "$FIXTURE_DIR/deleted"; exit 0; fi
done
printf '%s\n%s' "$(cat "$FIXTURE_DIR/body")" "$(cat "$FIXTURE_DIR/status")"
STUB
  chmod +x "$FIXTURE/curl"
}

err() { printf '{"errors":[{"detail":"%s"}]}' "$1"; }

# expect <exit> <text the output must name, or ""> <case name>
expect() {
  local want=$1 needle=$2 name=$3
  shift 3
  checked=$((checked + 1))
  local out rc=0
  out=$(FIXTURE_DIR="$FIXTURE" CURL="$FIXTURE/curl" OIDC_TOKEN=stub-jwt \
        TRUSTPUB_API=https://example.invalid/tokens \
        "$PROBE" release.yml publish-crate.yml 2>&1) || rc=$?
  if [ "$rc" -ne "$want" ]; then
    echo "FAIL [$name]: expected exit $want, got $rc"
    echo "$out" | sed 's/^/    /'
    failures=$((failures + 1))
    return
  fi
  if [ -n "$needle" ] && ! printf '%s' "$out" | grep -qF "$needle"; then
    echo "FAIL [$name]: output did not mention '$needle'"
    echo "$out" | sed 's/^/    /'
    failures=$((failures + 1))
    return
  fi
  echo "ok   [$name]"
}

# The happy path: refused on the filename, and both workflows are listed.
stub_curl 400 "$(err 'The Trusted Publishing config for repository `nubo-db/dynoxide` does not match the workflow filename `release-preflight.yml` in the JWT. Expected workflow filenames: `release.yml`, `publish-crate.yml`')"
expect 0 "all registered" "both workflows registered"

# One of the two missing has to fail, or the probe is not reading the list.
stub_curl 400 "$(err 'The Trusted Publishing config for repository `nubo-db/dynoxide` does not match the workflow filename `release-preflight.yml` in the JWT. Expected workflow filenames: `release.yml`')"
expect 1 "publish-crate.yml" "one workflow missing"

# A near-miss name must not satisfy the match.
stub_curl 400 "$(err 'The Trusted Publishing config for repository `nubo-db/dynoxide` does not match the workflow filename `release-preflight.yml` in the JWT. Expected workflow filenames: `release.yml.bak`, `publish-crate.yml`')"
expect 1 "release.yml" "near-miss filename is not a match"

# An empty list is the failure this test exists for: it must not read as a pass.
stub_curl 400 "$(err 'The Trusted Publishing config for repository `nubo-db/dynoxide` does not match the workflow filename `release-preflight.yml` in the JWT. Expected workflow filenames: ')"
expect 1 "named no configured workflow" "empty list is not a pass"

# The crate is not configured at all.
stub_curl 400 "$(err 'No Trusted Publishing config found for repository `nubo-db/dynoxide`.')"
expect 1 "no Trusted Publishing config for this repository" "crate not configured"

# A config that predates a rename of the org.
stub_curl 400 "$(err 'The Trusted Publishing config for repository `nubo-db/dynoxide` does not match the repository owner ID (123) in the JWT. Expected owner IDs: 456.')"
expect 1 "stale and must be recreated" "stale owner id"

# An unrecognised refusal must fail rather than be read as a pass.
stub_curl 400 "$(err 'Something new crates.io started saying.')"
expect 1 "does not recognise" "unknown refusal"

# A status the probe has no reading for.
stub_curl 500 'upstream exploded'
expect 1 "answered 500" "server error"

# A success means this workflow has a config it should not have. The token has
# to be handed back as well as the check failing.
stub_curl 200 '{"token":"ct_secret"}'
expect 1 "Remove that config" "success is a finding, not a pass"
checked=$((checked + 1))
if [ -s "$FIXTURE/deleted" ]; then
  echo "ok   [token revoked on unexpected success]"
else
  echo "FAIL [token revoked on unexpected success]: no DELETE was issued"
  failures=$((failures + 1))
fi

echo
if [ "$failures" -ne 0 ]; then
  echo "$checked checked, $failures failed"
  exit 1
fi
echo "$checked checked, all passed"
