use crate::actions::gsi;
use crate::actions::helpers;
use crate::actions::lsi;
use crate::errors::{DynoxideError, Result};
use crate::storage::{Storage, escape_table_name};
use crate::types::item_size;
use crate::{ImportOptions, ImportResult};
use rusqlite::params;
use std::time::SystemTime;

/// Execute a bulk import of items into a table.
///
/// All items are inserted in a single transaction. If any item fails,
/// the entire import is rolled back.
pub fn execute(
    storage: &Storage,
    table_name: &str,
    items: Vec<crate::types::Item>,
    options: &ImportOptions,
) -> Result<ImportResult> {
    execute_inner(storage, table_name, items, options, false)
}

/// Bulk import with option to skip GSI deletes.
///
/// When `skip_gsi_deletes` is true, the DELETE-before-INSERT on GSI tables
/// is skipped entirely. The caller must guarantee there are no pre-existing
/// rows whose GSI keys could become stale (i.e., fresh database).
pub fn execute_skip_gsi_deletes(
    storage: &Storage,
    table_name: &str,
    items: Vec<crate::types::Item>,
    options: &ImportOptions,
) -> Result<ImportResult> {
    execute_inner(storage, table_name, items, options, true)
}

fn execute_inner(
    storage: &Storage,
    table_name: &str,
    items: Vec<crate::types::Item>,
    options: &ImportOptions,
    skip_gsi_deletes: bool,
) -> Result<ImportResult> {
    // 1. Require table exists
    let meta = helpers::require_table(storage, table_name)?;
    let key_schema = helpers::parse_key_schema(&meta)?;

    // 2. Empty vec: no-op
    let item_count = items.len();
    if items.is_empty() {
        return Ok(ImportResult {
            items_imported: 0,
            bytes_imported: 0,
        });
    }

    // 3. Parse GSI and LSI definitions once (outside the loop)
    let gsi_defs = gsi::parse_gsi_defs(&meta)?;
    let lsi_defs = lsi::parse_lsi_defs(&meta)?;

    // 4. Begin transaction
    storage.begin_transaction()?;

    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs_f64();
    let cached_at_val: Option<f64> = if options.set_cached_at {
        Some(now)
    } else {
        None
    };

    // Pre-fetch the next stream sequence number once (fix: O(n) → O(1))
    let mut next_seq = if options.record_streams && meta.stream_enabled {
        storage.next_stream_sequence_number(table_name)?
    } else {
        0
    };

    let mut total_bytes: usize = 0;

    let result = (|| -> Result<()> {
        // Prepare the INSERT statement once outside the loop
        let escaped = escape_table_name(table_name);
        let sql = format!(
            "INSERT OR REPLACE INTO \"{escaped}\" (pk, sk, item_json, item_size, cached_at, hash_prefix) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)"
        );
        let mut insert_stmt = storage.conn().prepare_cached(&sql)?;

        for mut item in items {
            // 5. Validate required keys and extract pk/sk
            helpers::validate_item_keys(&item, &key_schema, &meta)?;
            crate::validation::validate_item_attribute_values(&item)?;

            // Deduplicate sets (in-place, no clone needed)
            crate::validation::normalize_item_sets(&mut item);

            let (pk, sk) = helpers::extract_key_strings(&item, &key_schema)?;

            // 6. Serialize and calculate size
            let item_json = serde_json::to_string(&item).map_err(|e| {
                DynoxideError::InternalServerError(format!("JSON serialization failed: {e}"))
            })?;
            let size = item_size(&item);
            total_bytes += size;

            // Compute hash prefix for parallel scan ordering
            let hash_prefix = item
                .get(&key_schema.partition_key)
                .map(crate::storage::compute_hash_prefix)
                .unwrap_or_default();

            // 7. INSERT OR REPLACE into base table
            insert_stmt.execute(params![
                pk,
                sk,
                item_json,
                size as i64,
                cached_at_val,
                hash_prefix
            ])?;

            // 8. Maintain GSI tables (sparse: skip items missing GSI pk)
            for gsi_def in &gsi_defs {
                // Delete any existing GSI entry for this base table key
                // (skipped on fresh import — no stale entries to clean up)
                if !skip_gsi_deletes {
                    storage.delete_gsi_item(table_name, &gsi_def.index_name, &pk, &sk)?;
                }

                // If item has GSI pk attribute, insert into GSI
                if let Some(gsi_pk_val) = item.get(&gsi_def.pk_attr) {
                    let gsi_pk = gsi_pk_val.to_key_string().unwrap_or_default();
                    let gsi_sk = gsi_def
                        .sk_attr
                        .as_ref()
                        .and_then(|sk_attr| item.get(sk_attr))
                        .and_then(|v| v.to_key_string())
                        .unwrap_or_default();

                    // For ALL projection, reuse the base table JSON directly
                    // (avoids cloning the item HashMap and re-serializing)
                    let projected_json =
                        if gsi_def.projection_type == crate::types::ProjectionType::ALL {
                            item_json.clone()
                        } else {
                            let projected = gsi::build_index_item(
                                &item,
                                gsi_def,
                                &key_schema.partition_key,
                                key_schema.sort_key.as_deref(),
                            );
                            serde_json::to_string(&projected).map_err(|e| {
                                DynoxideError::InternalServerError(format!(
                                    "GSI JSON serialization failed: {e}"
                                ))
                            })?
                        };

                    storage.insert_gsi_item(
                        table_name,
                        &gsi_def.index_name,
                        &gsi_pk,
                        &gsi_sk,
                        &pk,
                        &sk,
                        &projected_json,
                    )?;
                }
            }

            // 8b. Maintain LSI tables (sparse: skip items missing LSI sk)
            for lsi_def in &lsi_defs {
                // Delete any existing LSI entry for this base table key
                if !skip_gsi_deletes {
                    storage.delete_lsi_item(table_name, &lsi_def.index_name, &pk, &sk)?;
                }

                // If item has LSI sk attribute, insert into LSI
                if let Some(ref lsi_sk_attr) = lsi_def.sk_attr {
                    if let Some(lsi_sk_val) = item.get(lsi_sk_attr) {
                        let lsi_pk = pk.clone();
                        let lsi_sk = lsi_sk_val.to_key_string().unwrap_or_default();

                        // For ALL projection, reuse the base table JSON directly
                        let projected_json =
                            if lsi_def.projection_type == crate::types::ProjectionType::ALL {
                                item_json.clone()
                            } else {
                                let projected = gsi::build_index_item(
                                    &item,
                                    lsi_def,
                                    &key_schema.partition_key,
                                    key_schema.sort_key.as_deref(),
                                );
                                serde_json::to_string(&projected).map_err(|e| {
                                    DynoxideError::InternalServerError(format!(
                                        "LSI JSON serialization failed: {e}"
                                    ))
                                })?
                            };

                        storage.insert_lsi_item(
                            table_name,
                            &lsi_def.index_name,
                            &lsi_pk,
                            &lsi_sk,
                            &pk,
                            &sk,
                            &projected_json,
                        )?;
                    }
                }
            }

            // 9. Optional stream recording
            if options.record_streams && meta.stream_enabled {
                let seq = next_seq;
                next_seq += 1;
                let shard_id = format!("shardId-{table_name}-000000");
                let keys_json = {
                    let mut keys = std::collections::HashMap::new();
                    if let Some(v) = item.get(&key_schema.partition_key) {
                        keys.insert(key_schema.partition_key.clone(), v.clone());
                    }
                    if let Some(ref sk_name) = key_schema.sort_key {
                        if let Some(v) = item.get(sk_name) {
                            keys.insert(sk_name.clone(), v.clone());
                        }
                    }
                    serde_json::to_string(&keys).unwrap_or_default()
                };
                let now_epoch = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as i64;
                storage.insert_stream_record(
                    table_name,
                    "INSERT",
                    &keys_json,
                    Some(&item_json),
                    None,
                    &seq.to_string(),
                    &shard_id,
                    now_epoch,
                )?;
            }
        }
        Ok(())
    })();

    // 10. Commit or rollback
    match result {
        Ok(()) => {
            storage.commit()?;
            Ok(ImportResult {
                items_imported: item_count,
                bytes_imported: total_bytes,
            })
        }
        Err(e) => {
            let _ = storage.rollback();
            Err(e)
        }
    }
}
