//! GSI maintenance helpers.
//!
//! Handles keeping GSI tables in sync with base table writes.

use crate::errors::{DynoxideError, Result};
use crate::storage::TableMetadata;
use crate::storage_backend::{IndexWriteOp, StorageBackend};
use crate::types::{GlobalSecondaryIndex, Item, KeyType, ProjectionType};
use std::collections::HashMap;

/// Parsed index definition for convenient access. Used for both GSI and LSI,
/// since the projected-item logic is identical.
pub struct IndexDef {
    pub index_name: String,
    pub pk_attr: String,
    pub sk_attr: Option<String>,
    pub projection_type: ProjectionType,
    pub non_key_attributes: Option<Vec<String>>,
}

/// Type alias retained for backward compatibility.
pub type GsiDef = IndexDef;

/// Convert a single GlobalSecondaryIndex to a GsiDef.
pub fn gsi_to_def(gsi: &GlobalSecondaryIndex) -> Result<GsiDef> {
    let pk_attr = gsi
        .key_schema
        .iter()
        .find(|k| k.key_type == KeyType::HASH)
        .map(|k| k.attribute_name.clone())
        .ok_or_else(|| DynoxideError::InternalServerError("GSI missing HASH key".to_string()))?;

    let sk_attr = gsi
        .key_schema
        .iter()
        .find(|k| k.key_type == KeyType::RANGE)
        .map(|k| k.attribute_name.clone());

    Ok(GsiDef {
        index_name: gsi.index_name.clone(),
        pk_attr,
        sk_attr,
        projection_type: gsi.projection.projection_type.clone().unwrap_or_default(),
        non_key_attributes: gsi.projection.non_key_attributes.clone(),
    })
}

/// Parse GSI definitions from table metadata.
pub fn parse_gsi_defs(meta: &TableMetadata) -> Result<Vec<GsiDef>> {
    let gsis: Vec<GlobalSecondaryIndex> = match meta.gsi_definitions.as_ref() {
        Some(json) => serde_json::from_str(json)
            .map_err(|e| DynoxideError::InternalServerError(format!("Bad GSI JSON: {e}")))?,
        None => return Ok(Vec::new()),
    };

    gsis.iter().map(gsi_to_def).collect()
}

/// Build the projected item_json for an index (GSI or LSI) based on projection type.
pub fn build_index_item(
    item: &Item,
    index: &IndexDef,
    table_pk: &str,
    table_sk: Option<&str>,
) -> Item {
    match index.projection_type {
        ProjectionType::ALL => item.clone(),
        ProjectionType::KEYS_ONLY => {
            let mut projected = HashMap::new();
            // Table keys
            if let Some(v) = item.get(table_pk) {
                projected.insert(table_pk.to_string(), v.clone());
            }
            if let Some(sk) = table_sk {
                if let Some(v) = item.get(sk) {
                    projected.insert(sk.to_string(), v.clone());
                }
            }
            // Index keys
            if let Some(v) = item.get(&index.pk_attr) {
                projected.insert(index.pk_attr.clone(), v.clone());
            }
            if let Some(ref sk) = index.sk_attr {
                if let Some(v) = item.get(sk) {
                    projected.insert(sk.clone(), v.clone());
                }
            }
            projected
        }
        ProjectionType::INCLUDE => {
            let mut projected = HashMap::new();
            // Table keys
            if let Some(v) = item.get(table_pk) {
                projected.insert(table_pk.to_string(), v.clone());
            }
            if let Some(sk) = table_sk {
                if let Some(v) = item.get(sk) {
                    projected.insert(sk.to_string(), v.clone());
                }
            }
            // Index keys
            if let Some(v) = item.get(&index.pk_attr) {
                projected.insert(index.pk_attr.clone(), v.clone());
            }
            if let Some(ref sk) = index.sk_attr {
                if let Some(v) = item.get(sk) {
                    projected.insert(sk.clone(), v.clone());
                }
            }
            // Non-key attributes
            if let Some(ref attrs) = index.non_key_attributes {
                for attr in attrs {
                    if let Some(v) = item.get(attr) {
                        projected.insert(attr.clone(), v.clone());
                    }
                }
            }
            projected
        }
    }
}

/// Update all GSI tables after an item write (put/update).
/// Handles both insert and update cases.
/// Returns a map of GSI name to write capacity units consumed.
#[allow(clippy::too_many_arguments)]
pub async fn maintain_gsis_after_write<S: StorageBackend>(
    storage: &S,
    table_name: &str,
    meta: &TableMetadata,
    table_pk_str: &str,
    table_sk_str: &str,
    item: &Item,
    table_pk_attr: &str,
    table_sk_attr: Option<&str>,
) -> Result<HashMap<String, f64>> {
    let gsi_defs = parse_gsi_defs(meta)?;
    let mut gsi_units: HashMap<String, f64> = HashMap::new();
    let mut ops: Vec<IndexWriteOp> = Vec::new();

    for gsi in &gsi_defs {
        // First, remove any existing GSI entry for this base table key
        ops.push(IndexWriteOp::DeleteGsi {
            table_name: table_name.to_string(),
            index_name: gsi.index_name.clone(),
            table_pk: table_pk_str.to_string(),
            table_sk: table_sk_str.to_string(),
        });

        // If the item has the GSI pk attribute, insert into GSI
        if let Some(gsi_pk_val) = item.get(&gsi.pk_attr) {
            let gsi_pk = gsi_pk_val.to_key_string().unwrap_or_default();
            let gsi_sk = gsi
                .sk_attr
                .as_ref()
                .and_then(|sk| item.get(sk))
                .and_then(|v| v.to_key_string())
                .unwrap_or_default();

            let projected = build_index_item(item, gsi, table_pk_attr, table_sk_attr);
            let projected_size = crate::types::item_size(&projected);
            let item_json = serde_json::to_string(&projected)
                .map_err(|e| DynoxideError::InternalServerError(e.to_string()))?;

            ops.push(IndexWriteOp::InsertGsi {
                table_name: table_name.to_string(),
                index_name: gsi.index_name.clone(),
                gsi_pk,
                gsi_sk,
                table_pk: table_pk_str.to_string(),
                table_sk: table_sk_str.to_string(),
                item_json,
            });

            gsi_units.insert(
                gsi.index_name.clone(),
                crate::types::write_capacity_units(projected_size),
            );
        }
    }

    // One batched fan-out call: the per-op loop crossed the wasm bridge once per
    // index operation; this hands the whole list over in a single crossing. The
    // default impl replays it per-item, so native order and behaviour are
    // unchanged.
    storage.apply_index_writes(&ops).await?;
    Ok(gsi_units)
}

/// Remove an item from all GSI tables after a delete.
/// Returns a map of GSI name to write capacity units consumed.
pub async fn maintain_gsis_after_delete<S: StorageBackend>(
    storage: &S,
    table_name: &str,
    meta: &TableMetadata,
    table_pk_str: &str,
    table_sk_str: &str,
) -> Result<HashMap<String, f64>> {
    let gsi_defs = parse_gsi_defs(meta)?;
    let mut gsi_units: HashMap<String, f64> = HashMap::new();
    let mut ops: Vec<IndexWriteOp> = Vec::new();

    for gsi in &gsi_defs {
        ops.push(IndexWriteOp::DeleteGsi {
            table_name: table_name.to_string(),
            index_name: gsi.index_name.clone(),
            table_pk: table_pk_str.to_string(),
            table_sk: table_sk_str.to_string(),
        });
        // Delete operations consume 1 WCU minimum per GSI affected
        gsi_units.insert(gsi.index_name.clone(), 1.0);
    }

    storage.apply_index_writes(&ops).await?;
    Ok(gsi_units)
}

/// Parse key attribute names for a GSI.
pub fn parse_gsi_key_schema(
    meta: &TableMetadata,
    index_name: &str,
) -> Result<(String, Option<String>)> {
    let gsi_defs = parse_gsi_defs(meta)?;
    let gsi = gsi_defs
        .into_iter()
        .find(|g| g.index_name == index_name)
        .ok_or_else(|| {
            DynoxideError::ValidationException(format!(
                "The table does not have the specified index: {index_name}"
            ))
        })?;
    Ok((gsi.pk_attr, gsi.sk_attr))
}
