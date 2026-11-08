#!/usr/bin/env bash
# Decides which CI jobs a pull request needs, from its changed files.
#
# Reads one path per line on stdin (the output of
# `git diff --name-only --no-renames <base> HEAD`) and prints two lines in
# GITHUB_OUTPUT form:
#
#   docker=true|false  the Docker MCP smoke test should run
#   code=true|false    the build and test jobs should run
#
# `code` is false only when every changed file is documentation. The list below
# is an allowlist of docs, not a denylist of code, so a file nobody thought
# about runs the full suite. Entries match whole paths: top-level files exactly,
# directories by prefix. README-crate.md is the crate's packaged readme and the
# READMEs under npm/ ship in packages, so neither is on the list.
#
# An empty list gives code=true. The caller handles a missing or unreachable
# diff base itself, by not calling this and running everything.
#
# benchmark-regression.yml carries a copy of DOCS_ONLY as a paths-ignore
# filter. Change both together.
set -euo pipefail

DOCS_ONLY='^(docs/|\.github/ISSUE_TEMPLATE/|\.github/PULL_REQUEST_TEMPLATE\.md$|(README|CONTRIBUTING|AGENTS|CHANGELOG|SECURITY|CODE_OF_CONDUCT)\.md$)'

# Paths that can break the documented MCP-in-Docker recipe. src/main.rs carries
# the serve and mcp flag plumbing and Cargo.{toml,lock} the feature set and rmcp
# version, so a src/mcp-only gate would be too narrow.
DOCKER='^(Dockerfile|Cargo\.(toml|lock)|src/main\.rs|src/mcp/|README\.md|\.github/workflows/ci\.yml)'

changed=$(grep -v '^$' || true)

if [ -n "$changed" ] && grep -qE "$DOCKER" <<< "$changed"; then
  echo "docker=true"
else
  echo "docker=false"
fi

# Count the files that are not docs rather than branching on grep's exit
# status. Exit 1 only means grep selected nothing, which here means every file
# is docs; any other failure stops the script, so the step goes red rather
# than printing code=false from an empty count.
not_docs=$(grep -cvE "$DOCS_ONLY" <<< "$changed") || [ $? -eq 1 ]
if [ -z "$changed" ] || [ "$not_docs" -gt 0 ]; then
  echo "code=true"
else
  echo "code=false"
fi
