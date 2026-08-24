# Versioning

Every Dynoxide artifact carries the same version number: the crate, the npm CLI
wrapper and its platform binaries, the browser engine, the container images and
the MCP registry entry. One tag produces one release across all of them.

This page says what that number promises. It is not a claim that conformance is
finished.

## One number, four contracts

| Contract | Surface |
|---|---|
| Rust API | Public types, traits, signatures, feature flags, MSRV |
| CLI and wire | `dynoxide` argv and exit codes, HTTP responses, error strings, the `x-dynoxide-version` header, container entrypoint and port |
| Browser JS API | The Worker client API and message protocol |
| Engine behaviour | How the engine answers, across every surface above |

A break in any one of them forces a major on the shared number.

That includes a case worth stating plainly because you will meet it: **a change
to the Rust API bumps the CLI's major**, even though a CLI user never touches
the Rust API. They share a number, so they share its increments. If that cost
becomes routine, the answer is to move the crate onto its own version stream,
and this is the reason it would happen.

## What forces what

| Change | Version |
|---|---|
| Rust public type or signature change | major |
| CLI flag removed or renamed | major |
| HTTP response shape or error string change | major |
| Worker message protocol or client API change | major |
| Container entrypoint or exposed port change | major |
| Engine behaviour change | major, unless it is a capture-backed conformance fix |
| Conformance fix, capture-backed | minor |
| New DynamoDB operation | minor |
| New feature flag | minor |
| MSRV raise | minor |
| Dependency bump with no surface change | patch |

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

- **npm:** `~1.0.0` accepts patches only.
- **Containers:** `dynoxide:1.0` is pinned to the minor. `dynoxide:1` floats
  across the whole major line and does receive conformance fixes.
- **Cargo:** `=1.0.0`, or rely on your lockfile.

`npm install dynoxide` writes `^1.0.0` for you. If you want the narrower range,
ask for it explicitly.

## The browser engine

`@dynoxide/wasm-engine` is versioned with everything else and covered by the
same rules.

It is a scored target in the conformance suite and passes tier 1 in full. Two
exclusions are worth knowing before you rely on it:

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
