# Versioning

Dynoxide has two version streams.

The **product version** covers everything you install: the `dynoxide` CLI and
its platform binaries, the npm packages, the browser engine, the container
images, the MCPB bundle, the MCP registry entry, the GitHub Action and the
Homebrew formula. It lives in the `VERSION` file at the repository root, and
the release tag carries it. `dynoxide --version`, the `x-dynoxide-version` and
`Server` headers and the MCP server info all report it.

The **crate version** covers `dynoxide-rs` on crates.io, and nothing else. It
lives in `Cargo.toml`.

They were one number until 1.1.0. The split happened because a change to a
public Rust type forced a major on every artefact whose users never touch the
Rust API, and stranding everyone on `^1.x` and `dynoxide:1` to report a change
they cannot observe is a poor trade. The current mapping is product **1.2.0**,
crate **2.0.0**.

This page says what each number promises. It is not a claim that conformance is
finished.

## Four contracts, two streams

| Contract | Surface | Stream |
|---|---|---|
| Rust API | Public types, traits, signatures, feature flags, MSRV | Crate |
| CLI and wire | `dynoxide` argv and exit codes, HTTP responses, error strings, the `x-dynoxide-version` and `Server` headers, container entrypoint and port, the `GET /` health response and the startup line | Product |
| Browser JS API | The Worker client API and message protocol | Product |
| Engine behaviour | How the engine answers, across every surface above | Both |

Engine behaviour sits in both because both ship the same engine. A behaviour
change moves whichever streams publish it, which is usually both.

The MCP surface is deliberately absent. The data model is context for an agent,
not something the engine enforces, so it carries no version promise. Its
`serverInfo.version` reports the product version because that is what the user
installed.

MSRV is the exception inside the Rust row. It is named there because the crate
number describes it, not because every move in it breaks a consumer: raising it
ships as a minor. Where the two tables could be read against each other, the one
below is the authority.

## What forces what

"Breaking" is doing work in the first row. Adding a public type or function is
additive and ships as a minor; changing or removing one is not.

| Change | Stream | Version |
|---|---|---|
| Rust public type or signature change, breaking | Crate | major |
| Rust public type or function added | Crate | minor |
| CLI flag removed or renamed | Product | major |
| HTTP response shape or error string change | Product | major |
| Worker message protocol or client API change | Product | major |
| Container entrypoint or exposed port change | Product | major |
| `GET /` health status or body change | Product | major |
| Startup line text, or the stream it goes to | Product | major |
| Engine behaviour change | Both | major, unless it is a capture-backed conformance fix |
| Conformance fix, capture-backed | Both | minor |
| New DynamoDB operation | Both | minor |
| New feature flag | Crate | minor |
| MSRV raise | Crate | minor |
| Dependency bump with no surface change | Both | patch |

A release may move one stream and not the other. A Rust-only break publishes a
new crate against an unchanged product; a CLI-only change publishes a new
product against an unchanged crate.

## What a container waits on

Two behaviours are promises rather than descriptions, because tooling outside
this repository waits on them to decide Dynoxide is up.

`GET /` answers `200` with the body `healthy: dynamodb.us-east-1.amazonaws.com `,
trailing space included. The region in it comes from a constant rather than the
address the server bound to, so it does not move with `--host` or `--port`.
Every other path answers `404`, so a probe aimed at the wrong one fails instead
of passing by accident.

The startup line reads `Dynoxide listening on http://<host>:<port>` and goes to
**stderr**. Tracing output goes to stdout, so a log-based wait pointed at the
wrong stream waits for ever.

`tests/container_contract.rs` holds both. Worth knowing if you change either:
the container's own `HEALTHCHECK` reads the status line and never the body, so
without those tests nothing would notice the body change.

## The storage backend seam

`storage_backend::StorageBackend` is public, because `Database<S>` is generic
over it and the wasm dispatch functions take it as a bound. It is also sealed:
only Dynoxide's own backends implement it.

That is deliberate. The trait is the seam between the engine and its SQLite
backends, and its method set tracks what the engine can do rather than a
stable interface. Vector index support added seven methods to it in one
release. Were it open, each of those would have been a break, and an engine
growing its own storage layer would reach a new major every few months.

So adding a method to it is not a break, and the trait keeps pace with the
engine inside a crate major. You can name it and use it as a bound; you cannot implement
it. If third-party backends ever earn their place, unsealing is a minor.

## Conformance fixes are the exception

Dynoxide exists to behave like DynamoDB. Where it does not, that is a bug, and
fixing it changes how the engine answers. Under the rule above every such fix
would be a major, and a project correcting its own divergences would reach
8.0.0 inside a year, stranding a cohort of users at each one.

So conformance fixes ship as **minors**, on one condition: there must be a
recorded observation of the AWS behaviour being matched. A capture naming
region, date and request, a conformance suite result, or an equivalent
artefact. No recorded observation, no exception, and the change takes a major.

That condition is load-bearing. Without it, "this matches AWS more closely" is a
claim Dynoxide makes about its own change, and any behaviour change at all could
be filed under it.

Occasionally a conformance fix is disruptive enough that shipping it as a minor
is indefensible, and it takes a major instead. When that happens the release
note names the consumer behaviour that could not be carried across, and why
listing it as a behaviour change was not enough.

## What `^1.0.0` gives you, and what it does not

`^1.0.0` accepts every 1.x release. It does not freeze behaviour.

Behaviour moves inside 1.x, because conformance fixes ship as minors. A test
passing against 1.2.0 can fail against 1.3.0 if 1.3.0 corrected a divergence
that test had come to depend on. That is the deliberate trade of this policy:
for an emulator, an answer closer to DynamoDB is usually the one you wanted, and
the alternative is a new major every few weeks.

If you would rather behaviour held still:

- **npm:** `~1.2.0` accepts patches only.
- **Containers:** `dynoxide:1.2` is pinned to the minor. `dynoxide:1` floats
  across the whole major line and does receive conformance fixes.

## Reading the two numbers

The number in `dynoxide --version` is the product version. So is the image tag,
the npm version and the Action tag. If you are reporting a bug, that is the
number to give.

`cargo install dynoxide-rs --version X` selects the **crate** version, and the
binary it produces still reports the product version. They are different
numbers on purpose, and `cargo install dynoxide-rs --version 1.2.0` will not
find the product release. The crates.io badge in the README shows the crate.

A product release does not always mean a new crate, and a crate release does
not always mean new binaries.
- **Cargo:** `=2.0.0`, or rely on your lockfile. That pins the crate, which is a different number from the product version the binary reports.

`npm install dynoxide` writes a caret range for you. If you want the narrower range,
ask for it explicitly.

## The browser engine

`@dynoxide/wasm-engine` is versioned with the other installable artefacts and covered by the
same rules.

It is a scored target in the conformance suite. Current results are published
alongside every other target and are not quoted here, because a figure written
into this page is stale by the next release. Two exclusions are structural, and
worth knowing before you rely on it:

- **Unimplemented operations.** `TransactWriteItems`, streams, tags and TTL are
  not implemented. The suite skips them rather than failing them, so they do not
  appear in the pass figure.
- **Persistence.** The suite's browser shim opens the engine ephemerally, so the
  OPFS-backed storage path the package actually ships with is not exercised by
  conformance runs.

`manifest.json` carries a `contractVersion` alongside the engine version. It
stamps the message-envelope shape rather than the engine, moves on its own
schedule, and the client validates it on boot.

## The platform packages

`@dynoxide/darwin-arm64` and its siblings carry no contract. The `dynoxide`
wrapper pins them to exact versions and resolves them internally. Depend on
`dynoxide`; never on a platform package directly.

## Where behaviour changes are recorded

`docs/compatibility-summary.md` carries a cumulative list of behaviour changes
by version. That is the file to read when something answers differently after an
upgrade, and it is the one this page commits to keeping current.

Release notes also carry a Behaviour Changes section covering what moved in that
release. That is how releases are written, rather than a promise about every
future one.
