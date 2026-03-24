//! LSI maintenance helpers.
//!
//! Handles keeping LSI tables in sync with base table writes.

use crate::errors::{DynoxideError, Result};
use crate::storage::{Storage, TableMetadata};
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
pub fn maintain_lsis_after_write(
    storage: &Storage,
    table_name: &str,
    meta: &TableMetadata,
    table_pk_str: &str,
    table_sk_str: &str,
    item: &Item,
    table_pk_attr: &str,
    table_sk_attr: Option<&str>,
) -> Result<()> {
    let lsi_defs = parse_lsi_defs(meta)?;

    for lsi in &lsi_defs {
        // First, remove any existing LSI entry for this base table key
        storage.delete_lsi_item(table_name, &lsi.index_name, table_pk_str, table_sk_str)?;

        // LSI pk is always the same as the table pk. Only insert if item
        // has the LSI sort key attribute (sparse index behaviour).
        if let Some(ref lsi_sk_attr) = lsi.sk_attr {
            if let Some(lsi_sk_val) = item.get(lsi_sk_attr) {
                let lsi_pk = table_pk_str.to_string();
                let lsi_sk = lsi_sk_val.to_key_string().unwrap_or_default();

                let projected =
                    super::gsi::build_index_item(item, lsi, table_pk_attr, table_sk_attr);
                let item_json = serde_json::to_string(&projected)
                    .map_err(|e| DynoxideError::InternalServerError(e.to_string()))?;

                storage.insert_lsi_item(
                    table_name,
                    &lsi.index_name,
                    &lsi_pk,
                    &lsi_sk,
                    table_pk_str,
                    table_sk_str,
                    &item_json,
                )?;
            }
        }
    }

    Ok(())
}

/// Remove an item from all LSI tables after a delete.
pub fn maintain_lsis_after_delete(
    storage: &Storage,
    table_name: &str,
    meta: &TableMetadata,
    table_pk_str: &str,
    table_sk_str: &str,
) -> Result<()> {
    let lsi_defs = parse_lsi_defs(meta)?;

    for lsi in &lsi_defs {
        storage.delete_lsi_item(table_name, &lsi.index_name, table_pk_str, table_sk_str)?;
    }

    Ok(())
}
