# AGENTS.md

Guidance for AI coding tools (Codex, Cursor, Aider, Claude Code, and
others) contributing to Dynoxide. Humans are welcome to read it too;
`CONTRIBUTING.md` covers the same ground in prose.

## What Dynoxide is

A DynamoDB-compatible engine written in Rust, backed by SQLite. It runs
as an HTTP server, as an MCP server for coding agents, or embeds
directly into Rust and iOS applications as a library. Compatibility
with AWS DynamoDB's observable behaviour is the headline goal.

## Ground rules for contributions

1. **Compatibility first.** Dynoxide exists to behave like DynamoDB. A
   change that diverges from AWS DynamoDB's observable behaviour needs
   an explicit justification in the PR description and a link to the
   AWS doc or behaviour note that motivates it. "Cleaner API" is not a
   reason.
2. **Discuss before coding for anything non-trivial.** Open a GitHub
   issue describing the change before writing it. Small bug fixes and
   obvious cleanups are fine without a prior issue; anything that adds
   a feature, changes public behaviour, or touches more than a handful
   of files is worth a short issue first.
3. **No new dependencies without discussion.** Open an issue so we can
   weigh binary size, build time, and licence.
4. **Disclose AI assistance.** If an AI tool drafted or materially
   shaped the change, note it in the PR description. A single line
   is enough; the bar is "tell us, any level", not "match a specific
   phrasing". Examples:
   - "Drafted by Cursor; I reviewed and ran the tests."
   - "Copilot autocomplete on the glue code, otherwise hand-written."
   - "Hand-written; Claude Code reviewed it and flagged two edits I
     took."
   This keeps maintainer review calibrated; it is not a gate.

## Rust conventions

- Edition: 2024 (declared in `Cargo.toml`).
- MSRV: 1.85 (declared as `rust-version` in `Cargo.toml`). Do not use
  features that raise it.
- Formatting: `cargo fmt --check` must pass.
- Linting: `cargo clippy -- -D warnings` must pass. Warnings are
  errors; fix them rather than silencing.
- Testing: `cargo test` on the default feature set. Other feature
  combinations are exercised in CI; if you change feature gates,
  mirror what `.github/workflows/ci.yml` runs.

## Testing expectations

- New behaviour ships with tests in `tests/` or inline module tests.
- Bug fixes ship with a regression test that fails before the fix.
- `cargo test` on default features is what most contributors run
  locally. CI goes further and runs a feature matrix across four
  configurations:
  - default (all default features)
  - `--no-default-features --features native-sqlite --lib`
  - `--no-default-features --features encryption --lib`
  - `--no-default-features --features encryption,http-server`
  A separate feature-guard job checks that deliberately incompatible
  feature combinations fail to compile (for example
  `native-sqlite + encryption`). If you change feature gates or
  anything cross-cutting, running the relevant matrix leg locally
  saves a CI round-trip. `.github/workflows/ci.yml` is the
  authoritative list.
- The external conformance suite lives at
  <https://github.com/nubo-db/dynamodb-conformance>. It is run in CI
  for release candidates; you do not need to run it locally for every
  PR, but it is a useful check when changing request or response
  shapes.

## Benchmarks

The benchmark PR check has two layers and they do not carry equal
weight:

- **Criterion (wall-clock micro-benchmarks).** Advisory. These run on
  shared GitHub Actions runners and can vary by up to roughly 3x
  between runs because of noisy neighbours. A red Criterion row on
  your PR is not by itself a blocker; re-run the workflow if the
  result looks noisy.
- **iai-callgrind (instruction-count benchmarks).** Deterministic and
  blocking. If iai-callgrind flags a regression it means the code
  path does measurably more work; investigate before merging.

In short: trust iai-callgrind, treat Criterion as a smoke signal.

## Commit style

Short subject (ideally imperative, "add X" / "fix Y"), lower-case. A
Conventional Commits-style prefix is common and preferred when one
fits: `feat:`, `fix:`, `docs:`, `refactor:`, `chore:`, `ci:`. You
will also see `release:` and `merge:` on release-pipeline commits;
leave those to the maintainer.

Prefixes are a convention, not a gate. If a prefix genuinely does not
fit, a plain short subject is fine. Bodies are welcome when context
helps a reviewer but are not required.

## Storage layer boundary

The storage layer (`src/storage.rs` and related) is scheduled for a
significant refactor before 1.0: the `Database` type will become
generic over a `StorageBackend` trait. Please open an issue before
submitting changes in this area; we can advise whether your change
fits the current architecture, the planned one, or is best deferred.

## Where to discuss

- GitHub Issues: <https://github.com/nubo-db/dynoxide/issues>

Discussions are not currently enabled. If that changes, this section
will be updated.
