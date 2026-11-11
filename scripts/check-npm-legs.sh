#!/usr/bin/env bash
# Checks that every leg of the npm start() job ran its tests, with none failed
# and exactly the skips that leg expects. A leg that never ran leaves no
# record, which is the case a green matrix can hide.
#
# Usage: check-npm-legs.sh <legs directory> <leg name>...
# Each leg leaves <legs directory>/npm-leg-<name>/leg.txt holding its expected
# skip count and the summary lines of its TAP output.
set -euo pipefail

dir=$1
shift
status=0
for leg in "$@"; do
  file="$dir/npm-leg-$leg/leg.txt"
  if [ ! -f "$file" ]; then
    echo "::error::the $leg leg didn't run"
    status=1
    continue
  fi
  field() { sed -n "s/^# $1 //p" "$file"; }
  tests=$(field tests)
  fail=$(field fail)
  skipped=$(field skipped)
  want=$(sed -n 's/^expected_skips=//p' "$file")
  echo "$leg: $tests tests, $fail failed, $skipped skipped (expected $want)"
  if [ -z "$tests" ] || [ "$tests" -lt 1 ] || [ "$fail" != 0 ] || [ "$skipped" != "$want" ]; then
    echo "::error::the $leg leg didn't run cleanly"
    status=1
  fi
done
exit $status
