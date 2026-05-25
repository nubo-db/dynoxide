# Releasing Dynoxide

This page is for readers evaluating Dynoxide, contributing a PR, or verifying
a downloaded artefact. If you are cutting a release, the operational runbook
is tracked separately.

## Cadence

Releases cut as needed when a fix or feature is ready to ship. No fixed
schedule. Versioning follows [SemVer](https://semver.org).

## How a release ships

1. Benchmark numbers shown in the README are refreshed by an automated
   workflow that opens a "docs: update benchmark numbers" PR. The maintainer
   reviews the numbers and merges.
2. The version bump in `Cargo.toml` and a new `CHANGELOG.md` entry are
   committed to `main`.
3. A `v*` tag is pushed on that commit.
4. The tag push triggers the release pipeline: validate the tag against
   `Cargo.toml` and `CHANGELOG.md`, build binaries for five targets in
   parallel, create a GitHub Release with artefacts and checksums, pause for
   one-click maintainer approval, then publish to crates.io, update the
   Homebrew tap, publish the npm packages, and push the Docker image.

The approval gate between the GitHub Release and any external publish means
a release can still be aborted after the artefacts are built but before
anything reaches crates.io, Homebrew, npm, or any container registry.

## Verifying a release

- **sha256sums.txt** is attached to every GitHub Release. Download it
  alongside any archive and verify with `sha256sum -c sha256sums.txt`.
- **npm packages** are published with OIDC-backed provenance attestations.
  Provenance on npmjs.com confirms the package was built from this
  repository at the expected tag via GitHub Actions.
- **crates.io** is published by the same pipeline under the same tag, so
  `docs.rs`, the GitHub Release binaries, and the npm packages all
  correspond to a single commit.
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
| `benchmark-regression.yml` | Pull request | Runs Criterion and iai-callgrind benchmarks and posts a comparison comment. Wall-clock numbers are advisory because shared runners are noisy; iai-callgrind instruction-count regressions block the PR. |
| `release.yml` | `v*` tag push | Primary release pipeline. Runs only on releases; contributors should not see it fire on PRs. |

Additional workflows in `.github/workflows/` (`benchmark-refresh.yml`,
`release-preflight.yml`, `test-build.yml`, `publish-crate.yml`,
`homebrew.yml`, `npm.yml`, `docker.yml`) support release operations and
recovery. They are maintainer workflows and are not exercised by PR
traffic.

## Maintainer runbook

Step-by-step release execution, secret management, environment
configuration, and recovery procedures are tracked in an internal runbook.
This page intentionally does not duplicate that content.
