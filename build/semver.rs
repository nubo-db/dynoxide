// The product version rule, in Rust. This file is `include!`d by build.rs,
// which cannot shell out to scripts/is-semver.sh (a build script runs on
// Windows, on docs.rs and under `cargo install` from crates.io, with no bash
// in reach), and by tests/product_version.rs, which runs it against the cases
// scripts/release-helpers.test.sh runs against the shell script. It has no
// crate dependencies, so both can take it as it is. Change the rule here and
// in scripts/is-semver.sh together, and add the case to both test files.

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
