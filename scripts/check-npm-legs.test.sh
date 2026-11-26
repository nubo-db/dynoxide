#!/usr/bin/env bash
# Exercises check-npm-legs.sh, which fails a matrix whose legs didn't all run
# their tests cleanly. Most cases here must fail: a check that passes them
# would let a leg that never ran look green.
set -euo pipefail

SCRIPTS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHECK="$SCRIPTS/check-npm-legs.sh"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

failures=0
checked=0

# leg <name> <contents>: writes a leg record under the default prefix.
leg() {
  mkdir -p "$WORK/$case/npm-leg-$1"
  printf '%b' "$2" > "$WORK/$case/npm-leg-$1/leg.txt"
}

# expect <pass|fail> <case name> <leg names...>, against the records written
# for this case.
expect() {
  local want=$1 name=$2
  shift 2
  checked=$((checked + 1))
  local got=pass
  bash "$CHECK" "$WORK/$case" "$@" > /dev/null 2>&1 || got=fail
  if [ "$got" != "$want" ]; then
    echo "FAIL  - $name: wanted $want, got $got"
    failures=$((failures + 1))
  else
    echo "ok    - $name ($got)"
  fi
}

case=clean
leg Linux 'expected_skips=0\n# tests 12\n# fail 0\n# skipped 0\n'
expect pass "a leg that ran cleanly" Linux

case=crlf
leg Windows 'expected_skips=4\r\n# tests 111\r\n# fail 0\r\n# skipped 4\r\n'
expect pass "a Windows leg written with CRLF" Windows

case=missing
leg Linux 'expected_skips=0\n# tests 12\n# fail 0\n# skipped 0\n'
expect fail "a leg with no record" Linux macOS

case=failed
leg Linux 'expected_skips=0\n# tests 12\n# fail 1\n# skipped 0\n'
expect fail "a leg with a failed test" Linux

case=skips
leg Linux 'expected_skips=0\n# tests 12\n# fail 0\n# skipped 2\n'
expect fail "a leg with skips it didn't expect" Linux

case=none
leg Linux 'expected_skips=0\n# tests 0\n# fail 0\n# skipped 0\n'
expect fail "a leg that ran no tests" Linux

case=empty
leg Linux ''
expect fail "an empty record" Linux

case=prefix
mkdir -p "$WORK/$case/python-leg-Linux-3.15"
printf 'expected_skips=0\n# tests 111\n# fail 0\n# skipped 0\n' > "$WORK/$case/python-leg-Linux-3.15/leg.txt"
checked=$((checked + 1))
if LEG_PREFIX=python-leg- bash "$CHECK" "$WORK/$case" Linux-3.15 > /dev/null 2>&1; then
  echo "ok    - a leg under LEG_PREFIX (pass)"
else
  echo "FAIL  - a leg under LEG_PREFIX: wanted pass, got fail"
  failures=$((failures + 1))
fi

echo "$checked cases checked, $failures failed"
[ "$failures" -eq 0 ]
