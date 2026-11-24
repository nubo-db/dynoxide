#!/usr/bin/env bash
# Checks that every leg of the npm start() job ran its tests, with none failed
# and exactly the skips that leg expects. A leg that never ran leaves no
# record, which is the case a green matrix can hide. The Python launcher's job
# uses it too, with LEG_PREFIX=python-leg-.
#
# Usage: check-npm-legs.sh <legs directory> <leg name>...
# Each leg leaves <legs directory>/<prefix><name>/leg.txt holding its expected
# skip count and TAP-style summary lines (# tests, # fail, # skipped). The
# prefix is LEG_PREFIX, npm-leg- by default.
set -euo pipefail

dir=$1
shift
status=0
for leg in "$@"; do
  file="$dir/${LEG_PREFIX:-npm-leg-}$leg/leg.txt"
  if [ ! -f "$file" ]; then
    echo "::error::the $leg leg didn't run"
    status=1
    continue
  fi
  # A Windows leg can write CRLF, and a stray \r would fail every compare.
  field() { tr -d '\r' < "$file" | sed -n "s/^$1//p"; }
  tests=$(field '# tests ')
  fail=$(field '# fail ')
  skipped=$(field '# skipped ')
  want=$(field 'expected_skips=')
  echo "$leg: $tests tests, $fail failed, $skipped skipped (expected $want)"
  if [ -z "$tests" ] || [ "$tests" -lt 1 ] || [ "$fail" != 0 ] || [ "$skipped" != "$want" ]; then
    echo "::error::the $leg leg didn't run cleanly"
    status=1
  fi
done
exit $status
