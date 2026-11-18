# Releasing Dynoxide

This page is for readers evaluating Dynoxide, contributing a PR, or verifying
a downloaded artefact. If you are cutting a release, the operational runbook
is tracked separately.

## Cadence

A release is cut when a fix or feature is ready to ship, with no fixed
schedule. Versioning follows [SemVer](https://semver.org).

## How a release ships

1. Benchmark numbers shown in the README are refreshed by an automated
   workflow that opens a "docs: update benchmark numbers" PR. The maintainer
   reviews the numbers and merges.
2. The product version bump in `VERSION`, a crate version bump in
   `Cargo.toml` if the Rust API changed, and a new `CHANGELOG.md` entry are
   committed to `main`. The two versions move independently: see
   `docs/versioning.md`.
3. A `v*` tag is pushed on that commit.
4. The tag push triggers the release pipeline, which:
   - validates the tag against `VERSION` and `CHANGELOG.md`
   - checks the crate version is coherent
   - builds binaries for five targets in parallel
   - creates a GitHub Release with artefacts and checksums
   - pauses for one-click maintainer approval
   - publishes to crates.io, updates the Homebrew tap, publishes the npm
     packages and pushes the Docker image.

The approval gate sits between the GitHub Release and the registry publishes,
so a release can still be aborted before anything reaches crates.io, Homebrew,
npm, or any container registry.

The gate doesn't cover the GitHub Release itself. The release is created
before the pause, and its artefacts can be downloaded as soon as it exists,
which is how the composite action resolves `version: latest`. Aborting at the
gate leaves the GitHub Release in place; delete it or mark it a draft if that
matters.

A prerelease tag such as `vX.Y.Z-rc.1` runs the same pipeline, but only
reaches people who ask for it. Its GitHub Release is flagged as a prerelease,
so `version: latest` keeps resolving to the last stable release. npm publishes
it to `next`, and the image gets its exact tag and no other. A stable crate
version is not published from it. The Homebrew tap, the MCP registry and the
site are left alone. `docs/versioning.md` lists what every surface does with
one.

## Verifying a release

- **sha256sums.txt** is attached to every GitHub Release. Download it
  alongside any archive and verify with `sha256sum -c sha256sums.txt`.
- **npm packages** are published with OIDC-backed provenance attestations.
  Provenance on npmjs.com confirms the package was built from this
  repository at the expected tag via GitHub Actions.
- **crates.io** is published by the same pipeline under the same tag, so
  `docs.rs`, the GitHub Release binaries, and the npm packages all
  correspond to a single commit. The crates.io step is skipped when the crate
  version is already published.
- **Docker images** carry SLSA provenance and SBOM attestations on the
  GHCR canonical (`ghcr.io/nubo-db/dynoxide`). Verify with
  `gh attestation verify oci://ghcr.io/nubo-db/dynoxide:<version> --owner nubo-db`.
  Docker Hub and ECR Public mirrors hold the same image manifest and blobs
  but not the OCI referrers, so attestation verification needs the GHCR
  canonical.

## Release history

[`CHANGELOG.md`](../CHANGELOG.md) lists every released version and what
changed. The GitHub
[Releases page](https://github.com/nubo-db/dynoxide/releases) mirrors it
with downloadable artefacts and checksums.

## Contributor PRs and release timing

PRs are not blocked by release timing. Anything merged to `main` before a
tag is pushed ships in that release; anything merged after waits for the
next one. There are no feature-freeze windows.

## Workflow reference

Public workflows a contributor may see when opening a PR:

| Workflow | Trigger | What it does |
|----------|---------|--------------|
| `ci.yml` | Pull request, manual | Build, test, and lint. Required for merge. |
| `benchmark-regression.yml` | Pull request | Runs Criterion and iai-callgrind benchmarks and posts a comparison comment. Wall-clock numbers are advisory because shared runners are noisy. iai-callgrind instruction counts are deterministic but informational: the job has no stored baseline, so it does not fail on a regression. |
| `release.yml` | `v*` tag push | Primary release pipeline. Runs only on releases; contributors should not see it fire on PRs. |

Additional workflows in `.github/workflows/` (`benchmark-refresh.yml`,
`release-preflight.yml`, `test-build.yml`, `publish-crate.yml`,
`homebrew.yml`, `npm.yml`, `docker.yml`, `mcp.yml`) support release operations
and recovery. They are maintainer workflows and are not exercised by PR
traffic.

## Maintainer runbook

Step-by-step release execution, secret management, environment
configuration, and recovery procedures are kept in an internal runbook, and
this page doesn't repeat them.
