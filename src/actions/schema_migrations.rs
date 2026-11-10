//! The migrations a database needs when it is opened, run the same way on
//! both backends.

use std::future::Future;

use super::helpers::with_write_transaction;
use crate::errors::{DynoxideError, Result};
use crate::storage_backend::StorageBackend;
use crate::storage_backend::schema::{self, OpenMigrations};

/// Bring a database an earlier build wrote up to date, as `plan` says: rebuild
/// its multi-attribute GSIs, then recompute the stored sizes of its vector
/// tables. A database a newer build wrote is refused.
///
/// Each step is one write transaction that records the step's version as it
/// commits, so an open that fails part way leaves the database as the last
/// finished step left it, and the next open carries on from there.
pub(crate) async fn run_open_migrations<S: StorageBackend>(
    storage: &S,
    plan: OpenMigrations,
) -> Result<()> {
    // Both opens refuse such a file before they write to it; this keeps a
    // caller that skipped that check from running on it.
    if let Some(version) = plan.newer_than_this_build {
        return Err(DynoxideError::InternalServerError(
            schema::newer_than_this_build(version),
        ));
    }
    if plan.rebuild_multi_attribute_gsis {
        migrate_step(storage, schema::MULTI_ATTRIBUTE_KEYS, || async {
            super::gsi::rebuild_multi_attribute_gsis(storage)
                .await
                .map(|_| ())
        })
        .await?;
    }
    if plan.recompute_vector_sizes {
        migrate_step(storage, schema::VECTOR_BILLED_SIZES, || async {
            super::vector_index::recompute_vector_table_sizes(storage)
                .await
                .map(|_| ())
        })
        .await?;
    }
    Ok(())
}

/// Run `step` and record `version`, in one write transaction.
///
/// The recorded version is read again once the transaction holds the write
/// lock (`BEGIN IMMEDIATE` on both backends), so a second process opening the
/// same file can't run the step alongside this one: it waits for the lock, or
/// fails to get it, and then finds the step done. A version a newer build
/// recorded in the meantime is refused, not taken as the step being done. A
/// failure part way through rolls back the step and leaves the old version
/// recorded, so the next open runs it again from the start.
async fn migrate_step<S, F, Fut>(storage: &S, version: u32, step: F) -> Result<()>
where
    S: StorageBackend,
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<()>>,
{
    with_write_transaction(storage, async {
        let recorded = storage.recorded_schema_version().await?;
        schema::refuse_newer_mid_migration(recorded).map_err(DynoxideError::InternalServerError)?;
        if recorded.is_some_and(|recorded| recorded >= version) {
            return Ok(());
        }
        step().await?;
        storage.record_schema_version(version).await?;
        Ok(())
    })
    .await
}
