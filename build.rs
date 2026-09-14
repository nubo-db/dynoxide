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
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let path = root.join("VERSION");
    println!("cargo:rerun-if-changed={}", path.display());
    println!(
        "cargo:rerun-if-changed={}",
        root.join("build").join("semver.rs").display()
    );

    let raw = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "cannot read the product version from {}: {e}. \
             The file ships with the crate and has no fallback, because a \
             guessed version is worse than a failed build",
            path.display()
        )
    });
    let version = raw.trim();

    // The same rule as scripts/is-semver.sh, from build/semver.rs, so a
    // version that builds is one the release pipeline will accept. The
    // looser check this replaced let `1.2.0-rc..1` through, which npm
    // rejects only after the GitHub Release exists. A prerelease is allowed,
    // because the release workflow accepts prerelease tags and the npm
    // publish path has a channel for them.
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

// Shared with tests/product_version.rs, which runs it against the cases
// scripts/release-helpers.test.sh runs against scripts/is-semver.sh.
include!("build/semver.rs");
