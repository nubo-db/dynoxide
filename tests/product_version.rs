//! The product version rule that build.rs compiles in, run against the same
//! versions scripts/release-helpers.test.sh runs against scripts/is-semver.sh.
//!
//! build.rs has no test target of its own, so the rule lives in
//! build/semver.rs and is included here as well. The two case lists below
//! are copied from the shell test; a version added to one belongs in the
//! other, so the Rust rule and the shell rule cannot drift apart unnoticed.

include!("../build/semver.rs");

/// The `good` list in scripts/release-helpers.test.sh.
const ACCEPTED: &[&str] = &[
    "1.2.0",
    "1.2.0-rc.1",
    "1.2.0-beta",
    "0.0.0",
    "1.2.0-0",
    "1.2.0-alpha-1.x-y.0",
];

/// The `bad` list in scripts/release-helpers.test.sh.
const REFUSED: &[&str] = &[
    "1.2.0-rc..1",
    "1.2.0-",
    "01.2.0",
    "1.2.0-rc.01",
    "1.2",
    "1.2.0.1",
    "v1.2.0",
    "1.2.0+build.5",
    "1.2.0-rc.1+build.5",
    "1.2.0 ",
    "",
];

#[test]
fn accepts_every_version_the_shell_rule_accepts() {
    let wrongly_refused: Vec<&str> = ACCEPTED.iter().copied().filter(|v| !is_semver(v)).collect();
    assert!(
        wrongly_refused.is_empty(),
        "refused, but scripts/is-semver.sh accepts: {wrongly_refused:?}"
    );
}

#[test]
fn refuses_every_version_the_shell_rule_refuses() {
    let wrongly_accepted: Vec<&str> = REFUSED.iter().copied().filter(|v| is_semver(v)).collect();
    assert!(
        wrongly_accepted.is_empty(),
        "accepted, but scripts/is-semver.sh refuses: {wrongly_accepted:?}"
    );
}

#[test]
fn the_version_file_passes_the_rule_and_is_what_the_crate_reports() {
    let raw = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/VERSION"))
        .expect("VERSION ships with the crate");
    let version = raw.trim();
    assert!(is_semver(version), "VERSION holds {version:?}");
    assert_eq!(dynoxide::PRODUCT_VERSION, version);
}
