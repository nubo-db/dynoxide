//! GSI maintenance helpers.
//!
//! Handles keeping GSI tables in sync with base table writes.

use crate::errors::{DynoxideError, Result};
use crate::storage::TableMetadata;
use crate::storage_backend::{IndexWriteOp, StorageBackend};
use crate::types::{GlobalSecondaryIndex, Item, KeyType, ProjectionType};
use std::collections::HashMap;

/// Where a write lands in the base table, how the table's own keys are named,
/// what the item looked like before it, and whether the caller wants capacity.
/// Shared by the GSI, LSI and vector fan-out, which all need the key strings to
/// address index rows and the key attribute names to rebuild the projected entry
/// on either side of the write.
///
/// The old image and the capacity mode live here rather than as arguments
/// because all three fan-outs must be given the same pair. Passed separately
/// they were repeated at every call site, and a site with two plausible old
/// images in scope could hand one fan-out the wrong one and still compile:
/// `update_item` holds both the pre-mutation copy and the genuinely-absent
/// image a create-through-update needs, and swapping them turns an insert
/// charge into a change charge. Built once per site, the three fan-outs cannot
/// disagree.
pub struct IndexWrite<'a> {
    pub table_name: &'a str,
    /// The base table partition key, as the string index rows are addressed by.
    pub pk: &'a str,
    /// The base table sort key, empty for a table without one.
    pub sk: &'a str,
    pub pk_attr: &'a str,
    pub sk_attr: Option<&'a str>,
    /// The item as it stood before this write, or `None` where no row existed.
    /// Absence has to be genuine: a key-only stand-in reads as a change rather
    /// than an insert.
    pub old_item: Option<&'a Item>,
    /// The caller's `ReturnConsumedCapacity`, deciding whether the fan-outs
    /// size their work at all.
    pub capacity_mode: Option<&'a str>,
}

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

impl IndexDef {
    /// Whether an entry in this index carries `attr`.
    ///
    /// The index keys and the base table keys are always carried, whatever the
    /// projection says, because an entry cannot point back at its item without
    /// them. `build_index_item` projects by this rule and the PartiQL read path
    /// rejects by it, so it lives here rather than in either of them.
    pub fn projects(&self, attr: &str, table_pk: &str, table_sk: Option<&str>) -> bool {
        if attr == self.pk_attr
            || self.sk_attr.as_deref() == Some(attr)
            || attr == table_pk
            || table_sk == Some(attr)
        {
            return true;
        }
        match self.projection_type {
            ProjectionType::ALL => true,
            ProjectionType::KEYS_ONLY => false,
            ProjectionType::INCLUDE => self
                .non_key_attributes
                .as_ref()
                .is_some_and(|names| names.iter().any(|n| n == attr)),
        }
    }

    /// The `(pk, sk)` key strings for this item's index entry, or `None` if the
    /// item is excluded. Sparse-index behaviour: an item missing the partition
    /// key, or the sort key when one is defined, or holding a non-scalar where a
    /// key is expected, is not projected into the index.
    pub fn index_key_strings(&self, item: &Item) -> Option<(String, String)> {
        let pk = item.get(&self.pk_attr)?.to_key_string()?;
        let sk = match self.sk_attr {
            Some(ref sk_attr) => item.get(sk_attr)?.to_key_string()?,
            None => String::new(),
        };
        Some((pk, sk))
    }
}

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
    if meta.gsi_definitions.is_some() {
        crate::bench_counters::record(&crate::bench_counters::INDEX_DEFS_PARSES);
    }
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
    crate::bench_counters::record(&crate::bench_counters::INDEX_ENTRIES_BUILT);
    if index.projection_type == ProjectionType::ALL {
        return item.clone();
    }
    item.iter()
        .filter(|(name, _)| index.projects(name, table_pk, table_sk))
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect()
}

/// Update all GSI tables after an item write (put/update).
/// Handles both insert and update cases.
///
/// Returns a map of GSI name to write capacity units consumed. An index whose
/// stored view the write leaves untouched is absent from the map rather than
/// present and zeroed, so the response omits its arm entirely. `target.old_item` is the
/// item as it stood before the write, or `None` when there was no item.
///
/// `capacity_mode` is the caller's `ReturnConsumedCapacity`, forwarded rather
/// than interpreted, so a call site is right by passing a field it already
/// holds. The map comes back empty when it asks for nothing, and sizing an index
/// costs a projection of the old image per index that nothing else wants.
pub async fn maintain_gsis_after_write<S: StorageBackend>(
    storage: &S,
    meta: &TableMetadata,
    target: &IndexWrite<'_>,
    item: &Item,
) -> Result<HashMap<String, f64>> {
    let gsi_defs = parse_gsi_defs(meta)?;
    maintain_gsis_after_write_with_defs(storage, &gsi_defs, target, item).await
}

/// Defs-accepting form of [`maintain_gsis_after_write`], for callers that
/// parse the definitions once per batch (BatchWriteItem).
pub async fn maintain_gsis_after_write_with_defs<S: StorageBackend>(
    storage: &S,
    gsi_defs: &[GsiDef],
    target: &IndexWrite<'_>,
    item: &Item,
) -> Result<HashMap<String, f64>> {
    let want_capacity = crate::types::capacity_wanted(target.capacity_mode);
    let mut gsi_units: HashMap<String, f64> = HashMap::new();
    let mut ops: Vec<IndexWriteOp> = Vec::new();

    for gsi in gsi_defs {
        // First, remove any existing GSI entry for this base table key
        ops.push(IndexWriteOp::DeleteGsi {
            table_name: target.table_name.to_string(),
            index_name: gsi.index_name.clone(),
            table_pk: target.pk.to_string(),
            table_sk: target.sk.to_string(),
        });

        // Insert only when the item belongs in this index (sparse). The entry
        // is built once here and handed to the capacity calculation below,
        // which would otherwise project the same item again.
        let entry = super::index_capacity::entry_for(item, gsi, target.pk_attr, target.sk_attr);
        if let Some(ref entry) = entry {
            let (gsi_pk, gsi_sk) = entry.key.clone();
            let item_json = serde_json::to_string(&entry.projected)
                .map_err(|e| DynoxideError::InternalServerError(e.to_string()))?;

            ops.push(IndexWriteOp::InsertGsi {
                table_name: target.table_name.to_string(),
                index_name: gsi.index_name.clone(),
                gsi_pk,
                gsi_sk,
                table_pk: target.pk.to_string(),
                table_sk: target.sk.to_string(),
                item_json,
            });
        }

        // Capacity is the change to what the index stores, so it is charged even
        // when the item leaves the index and no insert is queued above.
        if want_capacity
            && let Some(units) = super::index_capacity::index_write_units_for(
                target.old_item,
                entry,
                gsi,
                target.pk_attr,
                target.sk_attr,
            )
        {
            gsi_units.insert(gsi.index_name.clone(), units);
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
///
/// Returns a map of GSI name to write capacity units consumed. Only an index the
/// deleted item was actually a member of is charged, sized on the entry it held.
/// `target.old_item` is the deleted item, or `None` when the delete removed nothing.
///
/// `capacity_mode` is the caller's `ReturnConsumedCapacity`, as on
/// [`maintain_gsis_after_write`]. A delete queues its ops from the target key
/// alone, so sizing is the only thing that projects the deleted item at all.
pub async fn maintain_gsis_after_delete<S: StorageBackend>(
    storage: &S,
    meta: &TableMetadata,
    target: &IndexWrite<'_>,
) -> Result<HashMap<String, f64>> {
    let gsi_defs = parse_gsi_defs(meta)?;
    maintain_gsis_after_delete_with_defs(storage, &gsi_defs, target).await
}

/// Defs-accepting form of [`maintain_gsis_after_delete`], for callers that
/// parse the definitions once per batch (BatchWriteItem).
pub async fn maintain_gsis_after_delete_with_defs<S: StorageBackend>(
    storage: &S,
    gsi_defs: &[GsiDef],
    target: &IndexWrite<'_>,
) -> Result<HashMap<String, f64>> {
    let want_capacity = crate::types::capacity_wanted(target.capacity_mode);
    let mut gsi_units: HashMap<String, f64> = HashMap::new();
    let mut ops: Vec<IndexWriteOp> = Vec::new();

    for gsi in gsi_defs {
        ops.push(IndexWriteOp::DeleteGsi {
            table_name: target.table_name.to_string(),
            index_name: gsi.index_name.clone(),
            table_pk: target.pk.to_string(),
            table_sk: target.sk.to_string(),
        });

        if want_capacity
            && let Some(units) = super::index_capacity::index_write_units(
                target.old_item,
                None,
                gsi,
                target.pk_attr,
                target.sk_attr,
            )
        {
            gsi_units.insert(gsi.index_name.clone(), units);
        }
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
