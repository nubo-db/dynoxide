//! LSI maintenance helpers.
//!
//! Handles keeping LSI tables in sync with base table writes.

use super::gsi::IndexWrite;
use crate::errors::{DynoxideError, Result};
use crate::storage::TableMetadata;
use crate::storage_backend::{IndexWriteOp, StorageBackend};
use crate::types::{Item, KeyType, LocalSecondaryIndex};
use std::collections::HashMap;

/// Type alias: LSI definitions reuse the shared IndexDef from gsi.
pub type LsiDef = super::gsi::IndexDef;

/// Convert a single LocalSecondaryIndex to an LsiDef.
pub fn lsi_to_def(lsi: &LocalSecondaryIndex) -> Result<LsiDef> {
    let pk_attr = lsi
        .key_schema
        .iter()
        .find(|k| k.key_type == KeyType::HASH)
        .map(|k| k.attribute_name.clone())
        .ok_or_else(|| DynoxideError::InternalServerError("LSI missing HASH key".to_string()))?;

    let sk_attr = lsi
        .key_schema
        .iter()
        .find(|k| k.key_type == KeyType::RANGE)
        .map(|k| k.attribute_name.clone());

    Ok(LsiDef {
        index_name: lsi.index_name.clone(),
        pk_attr,
        sk_attr,
        projection_type: lsi.projection.projection_type.clone().unwrap_or_default(),
        non_key_attributes: lsi.projection.non_key_attributes.clone(),
    })
}

/// Parse LSI definitions from table metadata.
pub fn parse_lsi_defs(meta: &TableMetadata) -> Result<Vec<LsiDef>> {
    let lsis: Vec<LocalSecondaryIndex> = match meta.lsi_definitions.as_ref() {
        Some(json) => serde_json::from_str(json)
            .map_err(|e| DynoxideError::InternalServerError(format!("Bad LSI JSON: {e}")))?,
        None => return Ok(Vec::new()),
    };

    lsis.iter().map(lsi_to_def).collect()
}

/// Parse key attribute names for an LSI.
pub fn parse_lsi_key_schema(
    meta: &TableMetadata,
    index_name: &str,
) -> Result<(String, Option<String>)> {
    let lsi_defs = parse_lsi_defs(meta)?;
    let lsi = lsi_defs
        .into_iter()
        .find(|l| l.index_name == index_name)
        .ok_or_else(|| {
            DynoxideError::ValidationException(format!(
                "The table does not have the specified index: {index_name}"
            ))
        })?;
    Ok((lsi.pk_attr, lsi.sk_attr))
}

/// Update all LSI tables after an item write (put/update).
/// Handles both insert and update cases.
///
/// Returns a map of LSI name to write capacity units consumed, on the same terms
/// as the GSI fan-out: an index the write leaves untouched is absent from the map
/// rather than present and zeroed. `capacity_mode` gates the sizing on the same
/// terms too, so see [`super::gsi::maintain_gsis_after_write`].
pub async fn maintain_lsis_after_write<S: StorageBackend>(
    storage: &S,
    meta: &TableMetadata,
    target: &IndexWrite<'_>,
    old_item: Option<&Item>,
    item: &Item,
    capacity_mode: Option<&str>,
) -> Result<HashMap<String, f64>> {
    let want_capacity = crate::types::capacity_wanted(capacity_mode);
    let lsi_defs = parse_lsi_defs(meta)?;
    let mut lsi_units: HashMap<String, f64> = HashMap::new();
    let mut ops: Vec<IndexWriteOp> = Vec::new();

    for lsi in &lsi_defs {
        // First, remove any existing LSI entry for this base table key
        ops.push(IndexWriteOp::DeleteLsi {
            table_name: target.table_name.to_string(),
            index_name: lsi.index_name.clone(),
            base_pk: target.pk.to_string(),
            base_sk: target.sk.to_string(),
        });

        // Insert only when the item belongs in this index (sparse): an LSI shares
        // the table partition key, so membership rests on a present, scalar sort key.
        // Built once here and handed to the capacity calculation below, which
        // would otherwise project the same item again.
        let entry = super::index_capacity::entry_for(item, lsi, target.pk_attr, target.sk_attr);
        if let Some(ref entry) = entry {
            let (lsi_pk, lsi_sk) = entry.key.clone();
            let item_json = serde_json::to_string(&entry.projected)
                .map_err(|e| DynoxideError::InternalServerError(e.to_string()))?;

            ops.push(IndexWriteOp::InsertLsi {
                table_name: target.table_name.to_string(),
                index_name: lsi.index_name.clone(),
                pk: lsi_pk,
                sk: lsi_sk,
                base_pk: target.pk.to_string(),
                base_sk: target.sk.to_string(),
                item_json,
            });
        }

        if want_capacity
            && let Some(units) = super::index_capacity::index_write_units_for(
                old_item,
                entry,
                lsi,
                target.pk_attr,
                target.sk_attr,
            )
        {
            lsi_units.insert(lsi.index_name.clone(), units);
        }
    }

    storage.apply_index_writes(&ops).await?;
    Ok(lsi_units)
}

/// Remove an item from all LSI tables after a delete.
///
/// Returns a map of LSI name to write capacity units consumed, charging only the
/// indexes the deleted item was a member of. `capacity_mode` gates the sizing as
/// it does on [`super::gsi::maintain_gsis_after_delete`].
pub async fn maintain_lsis_after_delete<S: StorageBackend>(
    storage: &S,
    meta: &TableMetadata,
    target: &IndexWrite<'_>,
    old_item: Option<&Item>,
    capacity_mode: Option<&str>,
) -> Result<HashMap<String, f64>> {
    let want_capacity = crate::types::capacity_wanted(capacity_mode);
    let lsi_defs = parse_lsi_defs(meta)?;
    let mut lsi_units: HashMap<String, f64> = HashMap::new();
    let mut ops: Vec<IndexWriteOp> = Vec::new();

    for lsi in &lsi_defs {
        ops.push(IndexWriteOp::DeleteLsi {
            table_name: target.table_name.to_string(),
            index_name: lsi.index_name.clone(),
            base_pk: target.pk.to_string(),
            base_sk: target.sk.to_string(),
        });

        if want_capacity
            && let Some(units) = super::index_capacity::index_write_units(
                old_item,
                None,
                lsi,
                target.pk_attr,
                target.sk_attr,
            )
        {
            lsi_units.insert(lsi.index_name.clone(), units);
        }
    }

    storage.apply_index_writes(&ops).await?;
    Ok(lsi_units)
}
