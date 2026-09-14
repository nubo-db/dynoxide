//! The product version rule that build.rs compiles in, run against the same
//! versions scripts/release-helpers.test.sh runs against scripts/is-semver.sh.
//!
//! build.rs has no test target of its own, so the rule lives in
//! build/semver.rs and is included here as well. Both tests read their cases
//! from scripts/semver-cases.txt, so the Rust rule and the shell rule cannot
//! drift apart unnoticed: a version added to the list is checked by both.

include!("../build/semver.rs");

const CASES: &str = include_str!("../scripts/semver-cases.txt");

/// The versions marked `kind` in the case file. A line that is neither a
/// comment nor `ok:` nor `bad:` is a broken file, not a missing case.
fn cases(kind: &str) -> Vec<&'static str> {
    let mut out = Vec::new();
    for line in CASES.lines() {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (k, version) = line
            .split_once(':')
            .unwrap_or_else(|| panic!("scripts/semver-cases.txt has an unreadable line: {line:?}"));
        assert!(
            k == "ok" || k == "bad",
            "scripts/semver-cases.txt has an unreadable line: {line:?}"
        );
        if k == kind {
            out.push(version);
        }
    }
    assert!(
        out.len() >= 5,
        "scripts/semver-cases.txt holds only {} {kind} cases; the list was not read",
        out.len()
    );
    out
}

#[test]
fn accepts_every_version_the_shell_rule_accepts() {
    let wrongly_refused: Vec<&str> = cases("ok").into_iter().filter(|v| !is_semver(v)).collect();
    assert!(
        wrongly_refused.is_empty(),
        "refused, but scripts/is-semver.sh accepts: {wrongly_refused:?}"
    );
}

#[test]
fn refuses_every_version_the_shell_rule_refuses() {
    let wrongly_accepted: Vec<&str> = cases("bad").into_iter().filter(|v| is_semver(v)).collect();
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
