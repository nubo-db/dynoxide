# RFCs

Substantial changes are discussed in an RFC before they are built. An RFC is a short proposal opened as a pull request; the maintainer accepts or declines it on that PR. [ADRs](../adr/) are the companion: an RFC proposes a change and gathers input, an ADR records a decision once it is made.

## When to open an RFC

Open one before starting work that changes any of:

- the DynamoDB wire contract: response shapes, error envelopes, or new API operations
- the `StorageBackend` trait, or what a storage backend is expected to do
- SigV4 authentication or the authorisation model
- the on-disk format, schema, or migration behaviour
- public CLI flags or the configuration file format
- a large new feature or subsystem

You do not need one for bug fixes, dependency bumps, internal refactors that keep behaviour the same, documentation, or tests. If you are not sure, open an issue describing the change and ask - that is where the "open an issue before touching the storage layer" note in [CONTRIBUTING.md](../../CONTRIBUTING.md) already points.

## How it works

1. Copy `0000-template.md` to `docs/rfcs/0000-short-title.md`, keep the `0000` for now, and fill it in.
2. Open a pull request. The PR is the discussion thread.
3. The maintainer either accepts it (merged with the next number assigned) or declines it (merged with `Status: Rejected`, so the reasoning is kept for anyone who wonders later). Implementation PRs reference the RFC by number.

This is deliberately light: no fixed comment period and no vote. dynoxide has a single maintainer, and the process exists to think a change through and leave a record, not to add ceremony.

## Index

<!-- Accepted RFCs are listed here as they land. -->
