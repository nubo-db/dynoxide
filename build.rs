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

    // Not a full semver parse: enough to catch an empty file, a stray editor
    // newline turned into whitespace, or a `v` prefix copied from a git tag.
    // A prerelease suffix is allowed, because the release workflow accepts
    // prerelease tags and the npm publish path has a channel for them.
    let (core, pre) = match version.split_once('-') {
        Some((core, pre)) => (core, Some(pre)),
        None => (version, None),
    };
    let core_ok = core.split('.').count() == 3
        && core
            .split('.')
            .all(|part| !part.is_empty() && part.chars().all(|c| c.is_ascii_digit()));
    let pre_ok = pre.is_none_or(|pre| {
        !pre.is_empty()
            && pre
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '-')
    });
    if !core_ok || !pre_ok {
        panic!(
            "the product version in {} is {version:?}, which is not \
             MAJOR.MINOR.PATCH with an optional -prerelease suffix",
            path.display()
        );
    }

    println!("cargo:rustc-env=DYNOXIDE_PRODUCT_VERSION={version}");
}
