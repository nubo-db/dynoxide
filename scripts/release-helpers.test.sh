#!/usr/bin/env bash
# Exercises the small scripts the release workflows share: is-semver.sh,
# is-prerelease.sh, npm-dist-tag.sh, pypi-version.sh, is-crate-published.sh,
# crate-publish-decision.sh, check-release-binary-version.sh and
# install-mcp-publisher.sh.
#
# Each of these replaced a copy of the same logic in two or three workflow
# files, and workflow steps only run at tag time, when a mistake is already
# public. So every one is shown to fail on a real violation here, and to stay
# quiet on a clean case, on every PR. The ones that talk to the network take a
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
# The cases live in semver-cases.txt, which tests/product_version.rs reads as
# well, so the shell rule and the Rust rule in build/semver.rs answer to one
# list. A line the file cannot be read from is a failure, not a skip.
semver_cases=0
while IFS= read -r line; do
  case "$line" in
    ''|'#'*) continue ;;
    ok:*)  expect 0 "" "is-semver accepts ${line#ok:}" "$SEMVER" "${line#ok:}" ;;
    bad:*) expect 1 "" "is-semver refuses '${line#bad:}'" "$SEMVER" "${line#bad:}" ;;
    *) echo "FAIL  - semver-cases.txt has an unreadable line: '$line'"; failures=$((failures + 1)); checked=$((checked + 1)) ;;
  esac
  semver_cases=$((semver_cases + 1))
done < "$SCRIPTS/semver-cases.txt"
if [ "$semver_cases" -lt 10 ]; then
  echo "FAIL  - semver-cases.txt holds only $semver_cases cases; the list was not read"
  failures=$((failures + 1)); checked=$((checked + 1))
fi

# A workflow_dispatch input can hold a newline, and a rule that matched line by
# line would accept a value whose first line is a version. These cannot live in
# semver-cases.txt, which is read a line at a time.
expect 1 "" "is-semver refuses a version followed by another line" "$SEMVER" $'1.2.0\nnot a version'
expect 1 "" "is-semver refuses a version preceded by another line" "$SEMVER" $'not a version\n1.2.0'
expect 1 "" "is-semver refuses a version with a trailing newline" "$SEMVER" $'1.2.0\n'

# --- is-prerelease.sh -------------------------------------------------------
#
# The Homebrew, MCP registry and site jobs are skipped on `true`, so a release
# read as a prerelease would silently leave the tap behind. Both directions
# are pinned, along with the shapes that are not versions at all.

PRERELEASE="$SCRIPTS/is-prerelease.sh"
expect 0 "false" "a release is not a prerelease" "$PRERELEASE" 1.2.0
expect 0 "false" "a release with multi-digit parts is not a prerelease" "$PRERELEASE" 10.20.30
expect 0 "true" "an rc is a prerelease" "$PRERELEASE" 1.2.0-rc.1
expect 0 "true" "a hyphen inside the identifier is a prerelease" "$PRERELEASE" 1.2.0-beta-2
expect 0 "true" "a numeric prerelease identifier is a prerelease" "$PRERELEASE" 1.2.0-0
expect 1 "" "a trailing hyphen is not a version" "$PRERELEASE" "1.2.0-"
expect 1 "" "a tag is not a version" "$PRERELEASE" v1.2.0-rc.1
expect 1 "" "an empty version is refused" "$PRERELEASE" ""
expect 1 "" "a release followed by an rc on the next line is not a version" "$PRERELEASE" $'1.2.0\n1.2.0-rc.1'

# --- npm-dist-tag.sh --------------------------------------------------------

DIST_TAG="$SCRIPTS/npm-dist-tag.sh"
expect 0 "latest" "a release publishes to latest" "$DIST_TAG" 1.2.0
expect 0 "next" "a prerelease publishes to next" "$DIST_TAG" 1.2.0-rc.1
expect 0 "next" "a hyphenated prerelease publishes to next" "$DIST_TAG" 1.2.0-beta-2
expect 1 "" "a malformed version has no dist-tag" "$DIST_TAG" "1.2.0-"
expect 1 "" "an empty version has no dist-tag" "$DIST_TAG" ""

# --- pypi-version.sh --------------------------------------------------------

PYPI="$SCRIPTS/pypi-version.sh"
expect 0 "1.2.0" "a release keeps its version on PyPI" "$PYPI" 1.2.0
expect 0 "1.2.0.post1" "a post number makes a post-release" "$PYPI" 1.2.0 1
expect 0 "1.2.0.post12" "a two-digit post number" "$PYPI" 1.2.0 12
expect 0 "1.2.0rc2" "a release candidate becomes rcN" "$PYPI" 1.2.0-rc.2
expect 0 "1.2.0b1" "a beta becomes bN" "$PYPI" 1.2.0-beta.1
expect 0 "1.2.0a0" "an unnumbered alpha becomes a0" "$PYPI" 1.2.0-alpha
expect 1 "" "a release candidate can't have a post number" "$PYPI" 1.2.0-rc.1 1
expect 1 "" "a post number of 0 is refused" "$PYPI" 1.2.0 0
expect 1 "" "a post number with a leading zero is refused" "$PYPI" 1.2.0 01
expect 1 "" "a post number that isn't a number is refused" "$PYPI" 1.2.0 "1; echo"
expect 1 "" "a prerelease PEP 440 can't express is refused" "$PYPI" 1.2.0-alpha.1-x
expect 1 "" "a hyphenated prerelease is refused" "$PYPI" 1.2.0-beta-2
expect 1 "" "a tag rather than a version is refused" "$PYPI" v1.2.0
expect 1 "" "a malformed version is refused" "$PYPI" 1.2.0-rc..1
expect 1 "" "an empty version is refused" "$PYPI" ""

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

# --- crate-publish-decision.sh ----------------------------------------------
#
# The whole truth table, since release.yml and publish-crate.yml both act on
# it and a crate version can never be replaced once it is wrong. Arguments are
# the crate, its version and the product version. The same curl stand-in
# answers for crates.io.

DECIDE="$SCRIPTS/crate-publish-decision.sh"

printf '404\n' > "$CODES"
expect 0 "publish" "a release publishes an unpublished crate" "$DECIDE" dynoxide-rs 2.1.0 1.3.0
printf '200\n' > "$CODES"
expect 0 "already-published" "a release skips a published crate" "$DECIDE" dynoxide-rs 2.1.0 1.3.0
printf '404\n' > "$CODES"
expect 0 "publish" "a release publishes an unpublished prerelease crate" "$DECIDE" dynoxide-rs 2.1.0-rc.1 1.3.0
printf '200\n' > "$CODES"
expect 0 "already-published" "a release skips a published prerelease crate" "$DECIDE" dynoxide-rs 2.1.0-rc.1 1.3.0
printf '404\n' > "$CODES"
expect 0 "stable-crate-on-prerelease" "a prerelease never publishes a stable crate" "$DECIDE" dynoxide-rs 2.1.0 1.3.0-rc.1
printf '404\n' > "$CODES"
expect 0 "publish" "a prerelease publishes an unpublished prerelease crate" "$DECIDE" dynoxide-rs 2.1.0-rc.1 1.3.0-rc.1
printf '200\n' > "$CODES"
expect 0 "already-published" "a prerelease skips a published prerelease crate" "$DECIDE" dynoxide-rs 2.1.0-rc.1 1.3.0-rc.1
printf '429\n429\n429\n429\n429\n' > "$CODES"
expect 1 "" "a release refuses to guess when crates.io will not answer" "$DECIDE" dynoxide-rs 2.1.0 1.3.0

# A prerelease with a stable crate is decided before crates.io is asked, so an
# outage there cannot hold back npm and the containers for an rc. The stand-in
# could not connect if it were called, and its answers must still be unread.
printf '\n\n\n\n\n' > "$CODES"
expect 0 "stable-crate-on-prerelease" "a prerelease with a stable crate needs no answer from crates.io" "$DECIDE" dynoxide-rs 2.1.0 1.3.0-rc.1
checked=$((checked + 1))
if [ "$(wc -l < "$CODES" | tr -d ' ')" -ne 5 ]; then
  echo "FAIL  - crates.io was asked about a stable crate on a prerelease"
  failures=$((failures + 1))
else
  echo "ok    - crates.io was not asked about a stable crate on a prerelease"
fi

printf '404\n' > "$CODES"
expect 1 "" "a tag is not a product version" "$DECIDE" dynoxide-rs 2.1.0 v1.3.0
expect 1 "" "a malformed crate version is refused" "$DECIDE" dynoxide-rs 2.1.0-rc..1 1.3.0
expect 1 "" "a missing product version is refused" "$DECIDE" dynoxide-rs 2.1.0

unset CURL STUB_CODES RETRY_DELAY

# --- release.yml prerelease gates -------------------------------------------
#
# The scripts above decide what a prerelease is, but release.yml decides what
# happens to one, and a gate wired the wrong way round would hand an rc to the
# tap or hold a stable release back with every job still green. Workflows only
# run at tag time, so their gates are read here instead: the stable-only jobs
# must compare validate's prerelease output with exactly what is-prerelease.sh
# prints for a release, and the GitHub Release must take its prerelease flag
# from the same output.
#
# A small reader rather than a YAML library, since nothing installs one on the
# runner: jobs sit at two spaces under `jobs:`, their keys at four.

check_gates() {
  python3 - "$@" <<'PY'
import re, sys

path, stable = sys.argv[1], sys.argv[2]
lines = open(path).read().splitlines()
problems, checks = [], 0

def expr(value):
    value = value.strip()
    m = re.fullmatch(r"\$\{\{(.*)\}\}", value)
    return re.sub(r"\s+", " ", (m.group(1) if m else value).strip())

jobs, current = {}, None
if "jobs:" not in lines:
    sys.exit(f"{path}: no top-level jobs: key")
for line in lines[lines.index("jobs:") + 1:]:
    if line and not line.startswith((" ", "#")):
        break
    m = re.fullmatch(r"  ([A-Za-z0-9_-]+):\s*", line)
    if m:
        current = m.group(1)
        jobs[current] = []
    elif current:
        jobs[current].append(line)

def keys(job, key):
    found = [re.fullmatch(rf"    {key}:\s*(.*)", l) for l in jobs.get(job, [])]
    return [f.group(1) for f in found if f]

def check(ok, message):
    global checks
    checks += 1
    if not ok:
        problems.append(message)

validate = jobs.get("validate", [])
check(any(re.fullmatch(r"\s+prerelease:\s*\$\{\{\s*steps\.resolve\.outputs\.prerelease\s*\}\}\s*", l) for l in validate),
      "validate does not declare a prerelease output from steps.resolve")
check(any("scripts/is-prerelease.sh" in l for l in validate),
      "validate does not run scripts/is-prerelease.sh")

gate = f"needs.validate.outputs.prerelease == '{stable}'"
for job in ("publish-homebrew", "publish-mcp-registry", "notify-site"):
    if job not in jobs:
        check(False, f"{job}: job not found")
        continue
    ifs = keys(job, "if")
    check(len(ifs) == 1 and expr(ifs[0]) == gate,
          f"{job}: if is {ifs or 'missing'}, wanted {gate}")
    check(any(re.search(r"\bvalidate\b", n) for n in keys(job, "needs")),
          f"{job}: does not need validate")

release = jobs.get("github-release", [])
step, inside = [], False
for line in release:
    if re.match(r"      - ", line):
        inside = re.fullmatch(r"      - name:\s*Create Release\s*", line) is not None
        continue
    if inside:
        step.append(line)
flags = [m.group(1) for m in (re.fullmatch(r"          prerelease:\s*(.*)", l) for l in step) if m]
flag = f"needs.validate.outputs.prerelease != '{stable}'"
check(len(flags) == 1 and expr(flags[0]) == flag,
      f"github-release Create Release: prerelease is {flags or 'missing'}, wanted {flag}")

for p in problems:
    print(p)
print(f"{checks} gate checks on {path}, {len(problems)} wrong")
sys.exit(1 if problems else 0)
PY
}

RELEASE_YML="$SCRIPTS/../.github/workflows/release.yml"
STABLE=$("$PRERELEASE" 1.3.0)
expect 0 "" "release.yml gates the stable-only jobs and the Release flag on the prerelease answer" check_gates "$RELEASE_YML" "$STABLE"

# Each gate is broken on a copy and the reader has to notice. perl swaps the
# nth match only, so each case breaks one job and leaves the others intact. A
# swap that matched nothing leaves the copy clean, and the case then fails.
nth() {
  N="$1" FIND="$2" REPL="$3" perl -0pe \
    '$n = 0; s/\Q$ENV{FIND}\E/++$n == $ENV{N} ? $ENV{REPL} : $&/ge' \
    "$RELEASE_YML" > "$FIXTURE/release.yml"
}
GATE="    if: needs.validate.outputs.prerelease == 'false'"

nth 1 "$GATE
" ""
expect 1 "" "a stable-only job with its gate removed is caught" check_gates "$FIXTURE/release.yml" "$STABLE"
nth 2 "== 'false'" "== 'true'"
expect 1 "" "a stable-only job with its gate inverted is caught" check_gates "$FIXTURE/release.yml" "$STABLE"
nth 3 "== 'false'" "!= 'true'"
expect 1 "" "a gate that fails open on a missing answer is caught" check_gates "$FIXTURE/release.yml" "$STABLE"
nth 1 "prerelease: \${{ needs.validate.outputs.prerelease != 'false' }}" "prerelease: false"
expect 1 "" "a GitHub Release flag that ignores the answer is caught" check_gates "$FIXTURE/release.yml" "$STABLE"
nth 1 "      prerelease: \${{ steps.resolve.outputs.prerelease }}
" ""
expect 1 "" "a validate job without the prerelease output is caught" check_gates "$FIXTURE/release.yml" "$STABLE"
expect 1 "" "gates compared with a value is-prerelease.sh does not print are caught" check_gates "$RELEASE_YML" "no"

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

# --- install-mcp-publisher.sh -----------------------------------------------
#
# Only the refusals can be shown offline, since an install that passes needs
# the real release bytes. The stand-in download is a real tarball holding an
# `mcp-publisher`, so a script that unpacked before checking the digest would
# leave one behind and fail here even though it still exited non-zero.

INSTALL="$SCRIPTS/install-mcp-publisher.sh"
PUBLISHER_VERSION=$(sed -n 's/^VERSION="\(.*\)"$/\1/p' "$INSTALL")
PUBLISHER_ASSET=$(sed -n 's/^ASSET="\(.*\)"$/\1/p' "$INSTALL")

# install_refuses <stderr must contain this> <case name> <download base>
install_refuses() {
  local want=$1 name=$2 base=$3 dir got=0 err
  checked=$((checked + 1))
  dir=$(mktemp -d "$FIXTURE/install.XXXXXX")
  err=$(cd "$dir" && MCP_PUBLISHER_BASE="$base" "$INSTALL" 2>&1 >/dev/null) || got=$?
  if [ "$got" -eq 0 ]; then
    echo "FAIL  - $name: wanted a refusal, got exit 0"
    failures=$((failures + 1))
  elif [[ "$err" != *"$want"* ]]; then
    echo "FAIL  - $name: refused, but not because '$want': $err"
    failures=$((failures + 1))
  elif [ -e "$dir/mcp-publisher" ]; then
    echo "FAIL  - $name: refused, but left an mcp-publisher behind"
    failures=$((failures + 1))
  else
    echo "ok    - $name (exit $got)"
  fi
}

if [ -z "$PUBLISHER_VERSION" ] || [ -z "$PUBLISHER_ASSET" ]; then
  echo "FAIL  - could not read VERSION and ASSET from install-mcp-publisher.sh"
  failures=$((failures + 1)); checked=$((checked + 1))
elif [ "$(uname -s)" = "Linux" ] && [ "$(uname -m)" = "x86_64" ]; then
  mkdir -p "$FIXTURE/mcp/v$PUBLISHER_VERSION" "$FIXTURE/mcp-pack"
  printf '#!/bin/sh\necho not the real mcp-publisher\n' > "$FIXTURE/mcp-pack/mcp-publisher"
  chmod +x "$FIXTURE/mcp-pack/mcp-publisher"
  tar czf "$FIXTURE/mcp/v$PUBLISHER_VERSION/$PUBLISHER_ASSET" -C "$FIXTURE/mcp-pack" mcp-publisher
  install_refuses "does not match its pinned SHA-256" \
    "an mcp-publisher download that does not match the pinned digest is refused" \
    "file://$FIXTURE/mcp"
  install_refuses "could not download" \
    "an mcp-publisher download that cannot be fetched is refused" \
    "file://$FIXTURE/absent"
else
  # The pin is the linux amd64 build, so elsewhere the guard is what refuses.
  install_refuses "is the linux amd64 build" \
    "mcp-publisher is refused on a host other than linux amd64" \
    "file://$FIXTURE/mcp"
fi

echo "$checked cases checked, $failures failed"
[ "$failures" -eq 0 ]
