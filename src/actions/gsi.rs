//! GSI maintenance helpers.
//!
//! Handles keeping GSI tables in sync with base table writes.

use crate::errors::{DynoxideError, Result};
use crate::storage::TableMetadata;
use crate::storage_backend::{IndexWriteOp, StorageBackend};
use crate::types::{AttributeValue, GlobalSecondaryIndex, Item, KeyType, ProjectionType};
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
///
/// A GSI key can have up to four partition key and four sort key attributes.
/// `pk_attr` and `sk_attr` name the first of each, which is the whole key for
/// an LSI and for a single-attribute GSI; `hash_attrs` and `range_attrs` list
/// every one in key-schema order.
pub struct IndexDef {
    pub index_name: String,
    pub pk_attr: String,
    pub sk_attr: Option<String>,
    pub hash_attrs: Vec<String>,
    pub range_attrs: Vec<String>,
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
        if self.is_key_attr(attr) || attr == table_pk || table_sk == Some(attr) {
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

    /// Whether `attr` is one of this index's key attributes.
    pub fn is_key_attr(&self, attr: &str) -> bool {
        self.hash_attrs.iter().any(|a| a == attr) || self.range_attrs.iter().any(|a| a == attr)
    }

    /// The `(pk, sk)` key strings for this item's index entry, or `None` if the
    /// item is excluded. Sparse-index behaviour: an item missing any key
    /// attribute, or holding a non-scalar where a key is expected, is not
    /// projected into the index.
    pub fn index_key_strings(&self, item: &Item) -> Option<(String, String)> {
        let values = |attrs: &[String]| -> Option<Vec<&AttributeValue>> {
            attrs.iter().map(|a| item.get(a)).collect()
        };
        let pk = composite_key_string(&values(&self.hash_attrs)?)?;
        let sk = if self.range_attrs.is_empty() {
            String::new()
        } else {
            composite_key_string(&values(&self.range_attrs)?)?
        };
        Some((pk, sk))
    }
}

/// Closes each attribute's part of a multi-attribute key string. It sorts
/// below anything an escaped part can hold at the same position, so a shorter
/// value sorts ahead of a longer one it is a prefix of, as it does on its own.
pub(crate) const KEY_PART_END: &str = "\u{1}\u{1}";

/// The key string an index row is stored and compared by.
///
/// One attribute keeps the plain key string, so single-attribute indexes are
/// stored exactly as before. With more than one, each attribute's key string is
/// escaped and closed by [`KEY_PART_END`], which keeps byte order equal to
/// comparing the attributes one after another: DynamoDB sorts a multi-attribute
/// sort key by its first attribute, then its second, and so on. `None` when a
/// value has no key form.
pub fn composite_key_string(values: &[&AttributeValue]) -> Option<String> {
    if let [only] = values {
        return only.to_key_string();
    }
    let mut out = String::new();
    for value in values {
        push_key_part(&mut out, &value.to_key_string()?);
    }
    Some(out)
}

/// Append one attribute's escaped key string and its terminator.
pub(crate) fn push_key_part(out: &mut String, key_string: &str) {
    push_escaped_key(out, key_string);
    out.push_str(KEY_PART_END);
}

/// Append a key string with the two characters at or below the terminator
/// escaped. `\u{0}` and `\u{1}` become `\u{1}\u{2}` and `\u{1}\u{3}`, which
/// sort above the terminator and below every other character, so the escaped
/// form orders exactly as the original does.
pub(crate) fn push_escaped_key(out: &mut String, key_string: &str) {
    for c in key_string.chars() {
        match c {
            '\u{0}' => out.push_str("\u{1}\u{2}"),
            '\u{1}' => out.push_str("\u{1}\u{3}"),
            c => out.push(c),
        }
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
    let (hash_attrs, range_attrs) = key_attrs_by_type(&gsi.key_schema);

    Ok(GsiDef {
        index_name: gsi.index_name.clone(),
        pk_attr,
        sk_attr,
        hash_attrs,
        range_attrs,
        projection_type: gsi.projection.projection_type.clone().unwrap_or_default(),
        non_key_attributes: gsi.projection.non_key_attributes.clone(),
    })
}

/// A key schema's partition key and sort key attributes, each in key-schema
/// order.
pub fn key_attrs_by_type(
    key_schema: &[crate::types::KeySchemaElement],
) -> (Vec<String>, Vec<String>) {
    let of = |key_type: KeyType| {
        key_schema
            .iter()
            .filter(|k| k.key_type == key_type)
            .map(|k| k.attribute_name.clone())
            .collect()
    };
    (of(KeyType::HASH), of(KeyType::RANGE))
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

/// Every partition key and sort key attribute of a GSI, in key-schema order.
pub fn parse_gsi_key_attrs(
    meta: &TableMetadata,
    index_name: &str,
) -> Result<(Vec<String>, Vec<String>)> {
    let gsi = parse_gsi_defs(meta)?
        .into_iter()
        .find(|g| g.index_name == index_name)
        .ok_or_else(|| {
            DynoxideError::ValidationException(format!(
                "The table does not have the specified index: {index_name}"
            ))
        })?;
    Ok((gsi.hash_attrs, gsi.range_attrs))
}

/// Rebuild every GSI whose key has more than one partition key or sort key
/// attribute from its base table, returning how many were rebuilt.
///
/// Earlier versions accepted such an index but keyed its rows on the first
/// attribute of each kind, so items sorted on the first sort key attribute
/// alone and an item missing a later key attribute was indexed. Both backends
/// run this when they open a database recorded before
/// [`MULTI_ATTRIBUTE_KEYS`](crate::storage_backend::schema::MULTI_ATTRIBUTE_KEYS),
/// through [`run_open_migrations`](super::schema_migrations::run_open_migrations).
/// Single-attribute indexes are stored the same way as before and are left
/// alone.
pub(crate) async fn rebuild_multi_attribute_gsis<S: StorageBackend>(storage: &S) -> Result<usize> {
    let mut rebuilt = 0;
    for table_name in storage.list_table_names().await? {
        let Some(meta) = storage.get_table_metadata(&table_name).await? else {
            continue;
        };
        // A listed table an interrupted CreateTable or DeleteTable left
        // without its data table has no items to rebuild from.
        if !storage.data_table_exists(&table_name).await? {
            continue;
        }
        let key_schema = super::helpers::parse_key_schema(&meta)?;
        for def in parse_gsi_defs(&meta)? {
            if def.hash_attrs.len() <= 1 && def.range_attrs.len() <= 1 {
                continue;
            }
            storage.drop_gsi_table(&table_name, &def.index_name).await?;
            storage
                .create_gsi_table(&table_name, &def.index_name)
                .await?;
            backfill_gsi(storage, &table_name, &key_schema, &def).await?;
            rebuilt += 1;
        }
    }
    Ok(rebuilt)
}

/// Parse key attribute names for a GSI: the first partition key and sort key
/// attribute. See [`parse_gsi_key_attrs`] for every attribute of a
/// multi-attribute key.
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

/// Backfill existing items into a newly created GSI, processing in batches.
pub(crate) async fn backfill_gsi<S: StorageBackend>(
    storage: &S,
    table_name: &str,
    key_schema: &super::helpers::KeySchema,
    gsi_def: &GsiDef,
) -> Result<()> {
    const BATCH_SIZE: usize = 1000;
    let mut last_pk: Option<String> = None;
    let mut last_sk: Option<String> = None;

    loop {
        let items = storage
            .scan_items(
                table_name,
                &crate::storage::ScanParams {
                    limit: Some(BATCH_SIZE),
                    exclusive_start_pk: last_pk.as_deref(),
                    exclusive_start_sk: last_sk.as_deref(),
                    ..Default::default()
                },
            )
            .await?;

        if items.is_empty() {
            break;
        }

        let mut rows: Vec<crate::storage_backend::GsiItemRow> = Vec::new();
        for (pk, sk, item_json) in &items {
            let item: crate::types::Item = serde_json::from_str(item_json)
                .map_err(|e| DynoxideError::InternalServerError(format!("Bad item JSON: {e}")))?;

            // Backfill only the items that belong in this index (sparse).
            if let Some((gsi_pk, gsi_sk)) = gsi_def.index_key_strings(&item) {
                let projected = build_index_item(
                    &item,
                    gsi_def,
                    &key_schema.partition_key,
                    key_schema.sort_key.as_deref(),
                );
                let projected_json = serde_json::to_string(&projected)
                    .map_err(|e| DynoxideError::InternalServerError(e.to_string()))?;

                rows.push(crate::storage_backend::GsiItemRow {
                    gsi_pk,
                    gsi_sk,
                    table_pk: pk.clone(),
                    table_sk: sk.clone(),
                    item_json: projected_json,
                });
            }
        }

        storage
            .insert_gsi_items(table_name, &gsi_def.index_name, &rows)
            .await?;

        let last = &items[items.len() - 1];
        last_pk = Some(last.0.clone());
        last_sk = Some(last.1.clone());

        if items.len() < BATCH_SIZE {
            break;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(parts: &[&str]) -> String {
        let values: Vec<AttributeValue> = parts
            .iter()
            .map(|p| AttributeValue::S(p.to_string()))
            .collect();
        let refs: Vec<&AttributeValue> = values.iter().collect();
        composite_key_string(&refs).unwrap()
    }

    #[test]
    fn a_composite_key_orders_part_by_part() {
        // Ascending by the first part, then the second. A value sorts ahead of
        // a longer one it is a prefix of, whatever comes after it, and the two
        // characters the encoding escapes keep their place among the others.
        let ascending = [
            key(&["a", "z"]),
            key(&["a\u{0}", "a"]),
            key(&["a\u{1}", "a"]),
            key(&["a\u{2}", "a"]),
            key(&["ab", "a"]),
            key(&["ab", "b"]),
            key(&["b", "a"]),
        ];
        for pair in ascending.windows(2) {
            assert!(
                pair[0] < pair[1],
                "{:?} should sort before {:?}",
                pair[0],
                pair[1]
            );
        }
    }

    #[test]
    fn a_single_attribute_key_is_stored_as_before() {
        let n = AttributeValue::N("-2.5".into());
        assert_eq!(composite_key_string(&[&n]), n.to_key_string());
    }
}
