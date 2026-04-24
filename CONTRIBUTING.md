# Contributing to Dynoxide

Thanks for considering a contribution. Dynoxide is a DynamoDB-compatible
engine in Rust backed by SQLite, and behaves like AWS DynamoDB by
design. That guides most of what follows.

If you are using an AI coding tool (Codex, Cursor, Aider, Claude Code,
or similar), please also read [AGENTS.md](AGENTS.md); it covers the
same ground in a form those tools pick up automatically.

## Before you start

- Open a GitHub issue describing the change if it is more than a small
  bug fix or obvious cleanup. A short paragraph is enough. This catches
  direction questions before a PR round-trips.
- The storage layer (`src/storage.rs` and related) is scheduled for a
  significant refactor before 1.0: the `Database` type will become
  generic over a `StorageBackend` trait. Please open an issue before
  submitting changes in this area so we can advise whether your change
  fits the current architecture, the planned one, or is best deferred.
- If you want to add a new dependency, open an issue so we can weigh
  binary size, build time, and licence.

## The compatibility principle

Dynoxide exists to behave like AWS DynamoDB. If a PR would make
Dynoxide's observable behaviour diverge from real DynamoDB, the PR
description needs to say so explicitly and cite the AWS documentation
or behaviour note that motivates the change. Cleaner ergonomics or
"nicer" API shapes are not a reason on their own.

## Local setup

- Rust 2024 edition, MSRV 1.85 (both declared in `Cargo.toml`). Any
  recent stable toolchain that satisfies the MSRV works.
- `cargo build` to compile, `cargo test` to run the test suite.
- `cargo fmt --check` and `cargo clippy -- -D warnings` must pass
  before a PR is merge-ready; CI enforces both.

## Tests

- New behaviour ships with tests. Bug fixes ship with a regression
  test that fails before the fix.
- Tests live in `tests/` and as inline `#[cfg(test)]` modules.
- The external conformance suite
  (<https://github.com/nubo-db/dynamodb-conformance>) runs in CI for
  release candidates. Running it locally is optional but useful when
  you change request or response shapes.

## Benchmarks

The PR benchmark check has two layers. Criterion wall-clock numbers
run on shared CI runners and are advisory; they can swing by up to
roughly 3x on a noisy neighbour. iai-callgrind runs on the same PR
and is deterministic and blocking. If Criterion looks red but
iai-callgrind is clean, re-run the workflow before worrying.
`AGENTS.md` has the short version of this.

## Commit style

Short subject, lower-case, imperative where possible. A Conventional
Commits-style prefix (`feat:`, `fix:`, `docs:`, `refactor:`, `chore:`,
`ci:`) is preferred when one fits but is not a gate; a plain short
subject is fine if no prefix fits. Bodies are welcome for anything
non-obvious.

## AI-assisted contributions

AI tools are welcome. If an AI tool drafted or materially shaped the
change, say so in the PR description. A single line is enough and
the bar is low: "drafted by Cursor, I ran the tests" or "Copilot
autocomplete on the glue code" or "hand-written, Claude Code
reviewed it" all work. The goal is that a maintainer knows what
level of human review to apply, not that contributors match a
specific phrasing.

## Where to ask

GitHub Issues: <https://github.com/nubo-db/dynoxide/issues>.
Discussions are not currently enabled.
