//! LSI maintenance helpers.
//!
//! Handles keeping LSI tables in sync with base table writes.

use crate::errors::{DynoxideError, Result};
use crate::storage::TableMetadata;
use crate::storage_backend::{IndexWriteOp, StorageBackend};
use crate::types::{Item, KeyType, LocalSecondaryIndex};

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
#[allow(clippy::too_many_arguments)]
pub async fn maintain_lsis_after_write<S: StorageBackend>(
    storage: &S,
    table_name: &str,
    meta: &TableMetadata,
    table_pk_str: &str,
    table_sk_str: &str,
    item: &Item,
    table_pk_attr: &str,
    table_sk_attr: Option<&str>,
) -> Result<()> {
    let lsi_defs = parse_lsi_defs(meta)?;
    let mut ops: Vec<IndexWriteOp> = Vec::new();

    for lsi in &lsi_defs {
        // First, remove any existing LSI entry for this base table key
        ops.push(IndexWriteOp::DeleteLsi {
            table_name: table_name.to_string(),
            index_name: lsi.index_name.clone(),
            base_pk: table_pk_str.to_string(),
            base_sk: table_sk_str.to_string(),
        });

        // Insert only when the item belongs in this index (sparse): an LSI shares
        // the table partition key, so membership rests on a present, scalar sort key.
        if let Some((lsi_pk, lsi_sk)) = lsi.index_key_strings(item) {
            let projected = super::gsi::build_index_item(item, lsi, table_pk_attr, table_sk_attr);
            let item_json = serde_json::to_string(&projected)
                .map_err(|e| DynoxideError::InternalServerError(e.to_string()))?;

            ops.push(IndexWriteOp::InsertLsi {
                table_name: table_name.to_string(),
                index_name: lsi.index_name.clone(),
                pk: lsi_pk,
                sk: lsi_sk,
                base_pk: table_pk_str.to_string(),
                base_sk: table_sk_str.to_string(),
                item_json,
            });
        }
    }

    storage.apply_index_writes(&ops).await?;
    Ok(())
}

/// Remove an item from all LSI tables after a delete.
pub async fn maintain_lsis_after_delete<S: StorageBackend>(
    storage: &S,
    table_name: &str,
    meta: &TableMetadata,
    table_pk_str: &str,
    table_sk_str: &str,
) -> Result<()> {
    let lsi_defs = parse_lsi_defs(meta)?;
    let mut ops: Vec<IndexWriteOp> = Vec::new();

    for lsi in &lsi_defs {
        ops.push(IndexWriteOp::DeleteLsi {
            table_name: table_name.to_string(),
            index_name: lsi.index_name.clone(),
            base_pk: table_pk_str.to_string(),
            base_sk: table_sk_str.to_string(),
        });
    }

    storage.apply_index_writes(&ops).await?;
    Ok(())
}
