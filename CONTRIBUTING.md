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
- The storage layer is mid-refactor. A `StorageBackend` trait now lives
  in `src/storage_backend/` and the native rusqlite-backed `Storage`
  implements it. Nothing dynamic dispatches the trait yet; `Database`
  and the action handlers still operate against `Storage` directly. The
  `Storage::conn()` and `Storage::conn_mut()` escape hatches are not on
  the trait and folding them in is the next pass. Please open an issue
  before submitting changes to `src/storage.rs`, `src/storage_backend/`,
  or call sites that use `conn()` directly so we can advise whether your
  change fits the current shape or is better held until the next pass.
- If you want to add a new dependency, open an issue so we can weigh
  binary size, build time, and licence.

## When you need an RFC or an ADR

Bigger changes get a short [RFC](docs/rfcs/) before they are built:
anything touching the DynamoDB wire contract, the `StorageBackend`
trait, SigV4 auth, the on-disk format, or the CLI and config surface,
plus large new features. It is a lightweight process, and it overlaps
with the "open an issue first" note above. [ADRs](docs/adr/) record the
decisions that come out of it. Bug fixes, refactors that keep behaviour
the same, docs, and tests need neither.

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
