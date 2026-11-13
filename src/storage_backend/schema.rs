//! The schema versions a database records, and what opening one needs.
//!
//! Both backends record the version in `_config` and read it back on open.
//! Which of the open-time migrations a recorded version calls for is decided
//! here, in one pure function, so the decision is the same on both backends
//! and can be tested without a database.

use super::sql_builders::SCHEMA_VERSION;

/// The version that first keys a multi-attribute GSI on every key attribute.
/// A database recorded below it may hold such an index keyed on the first
/// attribute of each kind, and its multi-attribute GSIs are rebuilt on open.
pub const MULTI_ATTRIBUTE_KEYS: u32 = 10;

/// The version that first stores the items of a table with a vector index at
/// their billed size. A database recorded below it holds number-rule sizes
/// for those items, and they are recomputed on open.
pub const VECTOR_BILLED_SIZES: u32 = 11;

/// The version that first holds a hash prefix for every item. A native
/// database recorded below it may hold items written before schema version 6
/// with an empty `hash_prefix`, which no parallel scan segment returns, and
/// those prefixes are filled in on open.
pub const HASH_PREFIXES: u32 = 12;

// The version this build writes is the version its last migration records.
const _: () = assert!(SCHEMA_VERSION == HASH_PREFIXES);

/// What opening a database at a recorded schema version needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenMigrations {
    /// Rebuild every multi-attribute GSI from its base table.
    pub rebuild_multi_attribute_gsis: bool,
    /// Recompute the stored item sizes of every table with a vector index.
    pub recompute_vector_sizes: bool,
    /// Fill in the empty `hash_prefix` of every item that has one.
    pub fill_hash_prefixes: bool,
    /// The file is open read-only. Only the hash prefix fill looks at this:
    /// with nothing to fill, it is skipped and the recorded version left as
    /// it is, so a file that needs no change still opens; with rows to fill,
    /// the open is refused. The steps before it are unaffected.
    pub read_only: bool,
    /// The version a newer build than this one recorded, when one did.
    /// Nothing is migrated, and the open refuses the file.
    pub newer_than_this_build: Option<u32>,
}

/// The open-time migrations a database recorded at `recorded` needs.
///
/// `None` is a database with no recorded version: one the wasm backend wrote
/// before it began recording a version, or one this open is creating. Every
/// migration runs for it; on a new database there is nothing for any of them
/// to do.
pub fn open_migrations(recorded: Option<u32>) -> OpenMigrations {
    match recorded {
        None => OpenMigrations {
            rebuild_multi_attribute_gsis: true,
            recompute_vector_sizes: true,
            fill_hash_prefixes: true,
            read_only: false,
            newer_than_this_build: None,
        },
        Some(version) => OpenMigrations {
            rebuild_multi_attribute_gsis: version < MULTI_ATTRIBUTE_KEYS,
            recompute_vector_sizes: version < VECTOR_BILLED_SIZES,
            fill_hash_prefixes: version < HASH_PREFIXES,
            read_only: false,
            newer_than_this_build: (version > SCHEMA_VERSION).then_some(version),
        },
    }
}

/// Refuse a database a newer build wrote: one whose recorded version is past
/// [`SCHEMA_VERSION`]. This build can't know what that build changed, so it
/// neither reads nor migrates such a file. Both backends check this before an
/// open writes anything, so a refused file is left as it was found.
pub fn refuse_newer(recorded: Option<u32>) -> Result<(), String> {
    match recorded {
        Some(version) if version > SCHEMA_VERSION => Err(newer_than_this_build(version)),
        _ => Ok(()),
    }
}

/// Refuse a version past [`SCHEMA_VERSION`] that a migration step finds once it
/// holds the write lock: a newer build wrote the file after this open began
/// bringing it up to date. Unlike [`refuse_newer`], the steps this open had
/// already finished are kept.
pub fn refuse_newer_mid_migration(recorded: Option<u32>) -> Result<(), String> {
    match recorded {
        Some(version) if version > SCHEMA_VERSION => Err(format!(
            "this database was written by a newer version of Dynoxide (schema version \
             {version}; this build reads up to schema version {SCHEMA_VERSION}) while \
             this build was bringing it up to date. Upgrade Dynoxide to open it. The \
             steps this build had finished are kept, and it ran no others."
        )),
        _ => Ok(()),
    }
}

/// What an open says of a database open read-only that holds items without a
/// hash prefix: filling them in is a write.
pub fn read_only_needs_hash_prefixes() -> String {
    format!(
        "this database holds items written before Dynoxide 0.9.5 that need their \
         hash prefix filled in before a parallel scan can return them, and it is \
         open read-only. Open it writable once so this build can bring it up to \
         schema version {HASH_PREFIXES}. The file has not been changed."
    )
}

/// Read a recorded schema version as [`parse_recorded`] does and refuse one a
/// newer build wrote, as [`refuse_newer`] does: the version an open may go on
/// with.
pub fn accept_recorded(value: Option<&str>) -> Result<Option<u32>, String> {
    let recorded = parse_recorded(value)?;
    refuse_newer(recorded)?;
    Ok(recorded)
}

/// What an open says of a database a newer build wrote at `version`, given
/// as it was recorded.
pub fn newer_than_this_build(version: impl std::fmt::Display) -> String {
    format!(
        "this database was written by a newer version of Dynoxide (schema version \
         {version}; this build reads up to schema version {SCHEMA_VERSION}). Upgrade \
         Dynoxide to open it. The file has not been changed."
    )
}

/// Read a recorded schema version: `Ok(None)` when none is recorded, and the
/// version when the value is digits that fit one. Digits past that were
/// written by a newer build, and anything else by no build at all, so both are
/// refused rather than read as no version, which would run every migration on
/// a file this build doesn't understand.
pub fn parse_recorded(value: Option<&str>) -> Result<Option<u32>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    let digits = value.trim();
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(unreadable_version(value));
    }
    digits
        .parse()
        .map(Some)
        .map_err(|_| newer_than_this_build(digits))
}

/// What an open says of a recorded schema version that isn't one.
fn unreadable_version(value: &str) -> String {
    format!(
        "this database records a schema version Dynoxide can't read ({value:?}). It was \
         not written by any build of Dynoxide. The file has not been changed."
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const EVERY: OpenMigrations = OpenMigrations {
        rebuild_multi_attribute_gsis: true,
        recompute_vector_sizes: true,
        fill_hash_prefixes: true,
        read_only: false,
        newer_than_this_build: None,
    };

    #[test]
    fn an_unversioned_database_runs_every_migration() {
        assert_eq!(open_migrations(None), EVERY);
    }

    #[test]
    fn a_database_before_multi_attribute_keys_runs_every_migration() {
        for version in 1..MULTI_ATTRIBUTE_KEYS {
            assert_eq!(open_migrations(Some(version)), EVERY, "version {version}");
        }
    }

    #[test]
    fn a_database_at_multi_attribute_keys_recomputes_sizes_and_fills_prefixes() {
        assert_eq!(
            open_migrations(Some(MULTI_ATTRIBUTE_KEYS)),
            OpenMigrations {
                rebuild_multi_attribute_gsis: false,
                recompute_vector_sizes: true,
                fill_hash_prefixes: true,
                read_only: false,
                newer_than_this_build: None,
            }
        );
    }

    #[test]
    fn a_database_at_vector_billed_sizes_only_fills_prefixes() {
        assert_eq!(
            open_migrations(Some(VECTOR_BILLED_SIZES)),
            OpenMigrations {
                rebuild_multi_attribute_gsis: false,
                recompute_vector_sizes: false,
                fill_hash_prefixes: true,
                read_only: false,
                newer_than_this_build: None,
            }
        );
    }

    #[test]
    fn a_database_at_this_version_runs_nothing() {
        assert_eq!(
            open_migrations(Some(SCHEMA_VERSION)),
            OpenMigrations {
                rebuild_multi_attribute_gsis: false,
                recompute_vector_sizes: false,
                fill_hash_prefixes: false,
                read_only: false,
                newer_than_this_build: None,
            }
        );
    }

    #[test]
    fn a_database_from_a_newer_build_runs_nothing_and_says_so() {
        assert_eq!(
            open_migrations(Some(SCHEMA_VERSION + 1)),
            OpenMigrations {
                rebuild_multi_attribute_gsis: false,
                recompute_vector_sizes: false,
                fill_hash_prefixes: false,
                read_only: false,
                newer_than_this_build: Some(SCHEMA_VERSION + 1),
            }
        );
    }

    #[test]
    fn only_a_version_past_this_one_is_refused() {
        for recorded in [None, Some(1), Some(SCHEMA_VERSION)] {
            assert_eq!(refuse_newer(recorded), Ok(()), "{recorded:?}");
        }
        let message = refuse_newer(Some(SCHEMA_VERSION + 1)).unwrap_err();
        assert_eq!(
            message,
            format!(
                "this database was written by a newer version of Dynoxide (schema version \
                 {}; this build reads up to schema version {SCHEMA_VERSION}). Upgrade \
                 Dynoxide to open it. The file has not been changed.",
                SCHEMA_VERSION + 1
            )
        );
    }

    #[test]
    fn a_recorded_version_reads_only_as_digits() {
        assert_eq!(parse_recorded(None), Ok(None));
        assert_eq!(parse_recorded(Some("11")), Ok(Some(11)));
        assert_eq!(parse_recorded(Some(" 11 ")), Ok(Some(11)));
        assert_eq!(parse_recorded(Some("4294967295")), Ok(Some(u32::MAX)));
    }

    #[test]
    fn digits_past_the_version_type_are_a_newer_build() {
        for value in ["4294967296", "99999999999"] {
            assert_eq!(
                parse_recorded(Some(value)),
                Err(newer_than_this_build(value)),
                "{value}"
            );
        }
        assert!(newer_than_this_build("99999999999").contains("schema version 99999999999;"));
    }

    #[test]
    fn a_recorded_version_that_is_not_a_number_is_refused() {
        for value in ["eleven", "-1", "+11", "", "1.5", "1 1"] {
            let message = parse_recorded(Some(value)).unwrap_err();
            assert_eq!(
                message,
                format!(
                    "this database records a schema version Dynoxide can't read ({value:?}). \
                     It was not written by any build of Dynoxide. The file has not been changed."
                ),
                "{value}"
            );
        }
    }
}
