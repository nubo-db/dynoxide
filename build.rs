//! Makes the product version available to the compiled code.
//!
//! The crate version in `Cargo.toml` and the product version in `VERSION` are
//! separate streams: a break in the Rust API moves the crate without charging
//! a major to the CLI, the containers, the browser engine or the npm package,
//! whose users never touch that API. See `docs/versioning.md`.
//!
//! Every surface that reports what dynoxide *is* to a user reads the value
//! emitted here, and only Cargo publication and docs.rs read the crate
//! version. Deriving from git is deliberately avoided so that a source
//! archive, a vendored copy and `cargo install` all report the same thing as
//! a release build.

use std::path::Path;

fn main() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("VERSION");
    println!("cargo:rerun-if-changed={}", path.display());

    let raw = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "cannot read the product version from {}: {e}. \
             The file ships with the crate and has no fallback, because a \
             guessed version is worse than a failed build",
            path.display()
        )
    });
    let version = raw.trim();

    // The same rule as scripts/is-semver.sh, so a version that builds is one
    // the release pipeline will accept. The looser check this replaced let
    // `1.2.0-rc..1` through, which npm rejects only after the GitHub Release
    // exists. A prerelease is allowed, because the release workflow accepts
    // prerelease tags and the npm publish path has a channel for them.
    if !is_semver(version) {
        panic!(
            "the product version in {} is {version:?}, which is not \
             MAJOR.MINOR.PATCH with no leading zeros and an optional \
             -prerelease of dot-separated non-empty identifiers",
            path.display()
        );
    }

    println!("cargo:rustc-env=DYNOXIDE_PRODUCT_VERSION={version}");
}

/// Semver 2.0.0 without build metadata: a numeric core with no leading zeros,
/// then an optional prerelease of dot-separated identifiers, each non-empty,
/// drawn from `[0-9A-Za-z-]`, and with no leading zero when purely numeric.
/// Build metadata is refused because the version becomes a git tag, a
/// container image tag, which cannot hold a `+`, and an npm version, which
/// drops it.
fn is_semver(version: &str) -> bool {
    let (core, pre) = match version.split_once('-') {
        Some((core, pre)) => (core, Some(pre)),
        None => (version, None),
    };
    let core_ok = core.split('.').count() == 3 && core.split('.').all(is_numeric_identifier);
    let pre_ok = pre.is_none_or(|pre| pre.split('.').all(is_prerelease_identifier));
    core_ok && pre_ok
}

/// `0`, or digits with no leading zero.
fn is_numeric_identifier(part: &str) -> bool {
    !part.is_empty()
        && part.bytes().all(|b| b.is_ascii_digit())
        && (part == "0" || !part.starts_with('0'))
}

/// A non-empty run of `[0-9A-Za-z-]`; when it is all digits, no leading zero.
fn is_prerelease_identifier(part: &str) -> bool {
    if part.is_empty() || !part.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-') {
        return false;
    }
    if part.bytes().all(|b| b.is_ascii_digit()) {
        is_numeric_identifier(part)
    } else {
        true
    }
}
