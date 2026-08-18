//! Vector index maintenance helpers.
//!
//! Keeps the per-index shadow tables in sync with base table writes,
//! mirroring the GSI/LSI pattern in `gsi.rs`: write validation before the
//! base write, then a delete-then-insert fan-out per index through
//! [`StorageBackend::apply_index_writes`]. Row derivation is shared with the
//! UpdateTable backfill path, so live writes and backfill agree on validity
//! by construction; the asymmetry between them is that backfill sparse-skips
//! exactly the shapes a live write rejects (captured from real DynamoDB,
//! eu-west-2 and us-east-1, 2026-08-12), while the live write silently
//! de-indexes only items missing the vector or SearchSchema HASH attribute.

use crate::actions::gsi::IndexWrite;
use crate::errors::{DynoxideError, Result};
use crate::storage::TableMetadata;
use crate::storage_backend::{IndexWriteOp, StorageBackend, VectorItemRow};
use crate::types::{
    AttributeDefinition, AttributeValue, Item, ScalarAttributeType, VectorIndex, VectorValueError,
};
use std::collections::HashMap;

/// Parse vector index definitions from table metadata.
pub fn parse_vector_defs(meta: &TableMetadata) -> Result<Vec<VectorIndex>> {
    match meta.vector_index_definitions.as_ref() {
        Some(json) => serde_json::from_str(json)
            .map_err(|e| DynoxideError::InternalServerError(format!("Bad vector index JSON: {e}"))),
        None => Ok(Vec::new()),
    }
}

/// The SearchSchema HASH attribute of an index, when the schema declares one.
/// Shared with the SearchVectors handler, which scopes candidate loads and
/// the mandatory-condition check on the same attribute the write path keys.
pub(crate) fn hash_attr(vix: &VectorIndex) -> Option<&str> {
    vix.search_schema.as_ref().and_then(|schema| {
        schema
            .iter()
            .find(|e| e.search_schema_element_type == "HASH")
            .map(|e| e.attribute_name.as_str())
    })
}

/// Parse attribute definitions from table metadata.
pub(crate) fn parse_attr_defs(meta: &TableMetadata) -> Result<Vec<AttributeDefinition>> {
    serde_json::from_str(&meta.attribute_definitions).map_err(|e| {
        DynoxideError::InternalServerError(format!("Bad attribute definitions JSON: {e}"))
    })
}

/// The wire letter of a declared scalar attribute type. Shared with the
/// SearchVectors handler's type-mismatch rejection.
pub(crate) fn scalar_type_str(t: &ScalarAttributeType) -> &'static str {
    match t {
        ScalarAttributeType::S => "S",
        ScalarAttributeType::N => "N",
        ScalarAttributeType::B => "B",
    }
}

/// The declared `AttributeDefinitions` entry for `attr` when the present
/// value's type does not match it: `Some(def)` on a mismatch, `None` when the
/// attribute is undeclared or the type matches. Shared by the write validator
/// (which rejects the mismatch) and the backfill row derivation (which skips
/// it), so the two paths agree on what counts as a mismatch by construction.
fn mismatched_attr_def<'a>(
    attr_defs: &'a [AttributeDefinition],
    attr: &str,
    val: &AttributeValue,
) -> Option<&'a AttributeDefinition> {
    let def = attr_defs.iter().find(|d| d.attribute_name == attr)?;
    let type_matches = match def.attribute_type {
        ScalarAttributeType::S => matches!(val, AttributeValue::S(_)),
        ScalarAttributeType::N => matches!(val, AttributeValue::N(_)),
        ScalarAttributeType::B => matches!(val, AttributeValue::B(_)),
    };
    if type_matches { None } else { Some(def) }
}

/// The captured `ValidationException` message for an invalid vector value.
/// All four shapes captured from real DynamoDB: wrong size, wrong element
/// type, and not-a-list in eu-west-2 on 2026-08-11; out-of-range in eu-west-2
/// and us-east-1 on 2026-08-12. Punctuation is preserved exactly as captured:
/// a full stop after `invalid` (not the usual colon), no stop before
/// `IndexName` on the size and not-a-list forms, a stop before it on the
/// element-type and out-of-range forms.
fn vector_value_message(err: &VectorValueError, attr: &str, dims: u32, index: &str) -> String {
    match err {
        VectorValueError::NotAList => format!(
            "One or more parameter values were invalid. Invalid type for parameter {attr}, \
             Expected: 32-bit floating point number list IndexName: {index}"
        ),
        VectorValueError::WrongDimensions { actual } => format!(
            "One or more parameter values were invalid. Invalid size for parameter {attr}, \
             Expected: {dims}, Actual: {actual} IndexName: {index}"
        ),
        VectorValueError::ElementNotANumber { position, actual } => format!(
            "One or more parameter values were invalid. Invalid type for parameter \
             {attr}[{position}], Expected: 32-bit floating point number, Actual: {actual}. \
             IndexName: {index}"
        ),
        VectorValueError::ElementOutOfRange { position, value } => format!(
            "One or more parameter values were invalid. Invalid value for parameter \
             {attr}[{position}], Value: {value} is outside valid range \
             [-3.4028235E38, 3.4028235E38]. IndexName: {index}"
        ),
    }
}

/// Reject any present-but-invalid vector index attribute on a put-shaped
/// write: the vector attribute itself, and the SearchSchema HASH attribute.
/// Absent attributes are fine (the item is silently de-indexed instead).
/// Called from `validate_item_keys` beside the classic index-key checks, so
/// the rejection fires before the base-table write.
pub fn validate_vector_write_attributes(item: &Item, meta: &TableMetadata) -> Result<()> {
    run_vector_write_validation(item, meta, None)
}

/// Like [`validate_vector_write_attributes`], but only checks attributes this
/// update changed against `before`, so an unrelated update never re-rejects a
/// pre-existing invalid value (for example an item the backfill sparse-skipped).
/// Mirrors the classic `validate_updated_index_keys` semantics; whether real
/// AWS re-rejects untouched values on update is uncaptured.
pub fn validate_updated_vector_attributes(
    before: &Item,
    after: &Item,
    meta: &TableMetadata,
) -> Result<()> {
    run_vector_write_validation(after, meta, Some(before))
}

fn run_vector_write_validation(
    item: &Item,
    meta: &TableMetadata,
    before: Option<&Item>,
) -> Result<()> {
    let mut vixs = parse_vector_defs(meta)?;
    if vixs.is_empty() {
        return Ok(());
    }
    let attr_defs = parse_attr_defs(meta)?;

    // Which index reports first when several are violated is uncaptured;
    // mirror the classic index-key validation's alphabetical tie-break so the
    // choice is at least deterministic. Within one index the vector attribute
    // is checked first, then the SearchSchema elements in declaration order
    // (also uncaptured).
    vixs.sort_by(|a, b| a.index_name.cmp(&b.index_name));

    let changed = |attr: &str, val: &AttributeValue| -> bool {
        match before {
            Some(b) => b.get(attr) != Some(val),
            None => true,
        }
    };

    for vix in &vixs {
        let vector_attr = vix.vector_attribute.attribute_name.as_str();
        if let Some(val) = item.get(vector_attr) {
            if changed(vector_attr, val) {
                if let Err(e) = crate::types::check_vector_f32_values(val, vix.dimensions) {
                    return Err(DynoxideError::ValidationException(vector_value_message(
                        &e,
                        vector_attr,
                        vix.dimensions,
                        &vix.index_name,
                    )));
                }
            }
        }

        let Some(schema) = vix.search_schema.as_ref() else {
            continue;
        };
        for elem in schema {
            let attr = elem.attribute_name.as_str();
            let Some(val) = item.get(attr) else {
                // Absent attributes are never an error: a missing HASH value
                // silently de-indexes the item, a missing filter value is
                // simply omitted from the filter set.
                continue;
            };
            if !changed(attr, val) {
                continue;
            }
            // A value whose type differs from the declared AttributeDefinitions
            // type rejects with the vector family's own wording, for HASH and
            // INLINE_FILTER elements alike (captured eu-west-2 and us-east-1:
            // the HASH form on 2026-08-12, the INLINE_FILTER form in the same
            // format on 2026-08-13; full stop after `invalid`).
            if let Some(def) = mismatched_attr_def(&attr_defs, attr, val) {
                return Err(DynoxideError::ValidationException(format!(
                    "One or more parameter values were invalid. Attribute '{attr}' type \
                     mismatch. Expected: {}, Actual: {}. IndexName: {}",
                    scalar_type_str(&def.attribute_type),
                    val.type_name(),
                    vix.index_name
                )));
            }
            if elem.search_schema_element_type != "HASH" {
                continue;
            }
            // An empty-string or empty-binary HASH value rejects with the
            // classic secondary-index message, IndexName/IndexKey suffix
            // included (empty string captured eu-west-2, 2026-08-11; empty
            // binary captured eu-west-2 and us-east-1, 2026-08-13). The
            // routable class matches the classic index keys, so the
            // transaction loops hoist it the same way. The update-expression
            // form drops the suffix, mirroring the classic update-path
            // wording (the empty-string form captured byte-identical in
            // eu-west-2 and us-east-1, 2026-08-13; the binary form follows
            // the same precedent).
            let empty_kind = match val {
                AttributeValue::S(s) if s.is_empty() => Some("string"),
                AttributeValue::B(b) if b.is_empty() => Some("binary"),
                _ => None,
            };
            if let Some(kind) = empty_kind {
                let msg = if before.is_some() {
                    format!(
                        "One or more parameter values are not valid. The update expression \
                         attempted to update a secondary index key to a value that is not \
                         supported. The AttributeValue for a key attribute cannot contain an \
                         empty {kind} value."
                    )
                } else {
                    format!(
                        "One or more parameter values are not valid. A value specified for a \
                         secondary index key is not supported. The AttributeValue for a key \
                         attribute cannot contain an empty {kind} value. IndexName: {}, \
                         IndexKey: {attr}",
                        vix.index_name
                    )
                };
                return Err(DynoxideError::KeyEmptyValueValidation(msg));
            }
        }
    }
    Ok(())
}

/// Derive the shadow-table row for one base item, or `None` when the item
/// does not belong in the index (sparse): an invalid vector value (missing
/// attribute, wrong type, wrong element count, non-numeric or out-of-f32-range
/// element) produces no row, and so does a SearchSchema HASH attribute that is
/// missing, non-scalar, of a type other than the declared AttributeDefinitions
/// type, or an empty string or binary value, and so does an INLINE_FILTER
/// value of a type other than its declared one. The skip set matches exactly
/// what a live write rejects: the HASH type mismatch was captured as
/// write-rejected in eu-west-2 and us-east-1 on 2026-08-12, the empty string
/// in eu-west-2 on 2026-08-11, and the empty binary and INLINE_FILTER type
/// mismatch in both regions on 2026-08-13.
///
/// Shared by the UpdateTable backfill and the live-write maintenance below;
/// on the live path everything rejectable was already rejected by
/// [`validate_vector_write_attributes`], so only the missing-attribute shapes
/// still fall through to `None` there.
pub fn vector_index_row(
    item: &Item,
    vix: &VectorIndex,
    pk_attr: &str,
    sk_attr: Option<&str>,
    attr_defs: &[AttributeDefinition],
    table_pk: &str,
    table_sk: &str,
) -> Result<Option<VectorItemRow>> {
    let Some(value) = item.get(&vix.vector_attribute.attribute_name) else {
        return Ok(None);
    };
    let Some(values) = crate::types::vector_f32_values(value, vix.dimensions) else {
        return Ok(None);
    };

    // A HASH-schema index scopes every search to one partition value, so an
    // item without the HASH attribute (or with a non-scalar there) is
    // unreachable and gets no row: the sparse-index pattern.
    let hash_value = match hash_attr(vix) {
        Some(attr) => {
            let Some(hash_val) = item.get(attr) else {
                return Ok(None);
            };
            // A value whose type differs from the declared AttributeDefinitions
            // type, or an empty string or binary value, is rejected by a live
            // write once the index exists, so backfill skips the same shapes.
            if mismatched_attr_def(attr_defs, attr, hash_val).is_some() {
                return Ok(None);
            }
            if matches!(hash_val, AttributeValue::S(s) if s.is_empty())
                || matches!(hash_val, AttributeValue::B(b) if b.is_empty())
            {
                return Ok(None);
            }
            match hash_val.to_key_string() {
                Some(s) => s,
                None => return Ok(None),
            }
        }
        None => String::new(),
    };

    // INLINE_FILTER attribute values present on the item, as a wire-shaped
    // JSON object. Absent filter attributes stay absent; the item is still
    // indexed.
    let mut filter_map = serde_json::Map::new();
    if let Some(ref schema) = vix.search_schema {
        for elem in schema {
            if elem.search_schema_element_type == "INLINE_FILTER" {
                if let Some(v) = item.get(&elem.attribute_name) {
                    // A filter value whose type differs from the declared
                    // AttributeDefinitions type is rejected by a live write
                    // (captured eu-west-2 and us-east-1, 2026-08-13), so
                    // backfill skips the whole row, matching the HASH
                    // mismatch treatment above.
                    if mismatched_attr_def(attr_defs, &elem.attribute_name, v).is_some() {
                        return Ok(None);
                    }
                    filter_map.insert(
                        elem.attribute_name.clone(),
                        serde_json::to_value(v)
                            .map_err(|e| DynoxideError::InternalServerError(e.to_string()))?,
                    );
                }
            }
        }
    }
    let filter_json = serde_json::to_string(&serde_json::Value::Object(filter_map))
        .map_err(|e| DynoxideError::InternalServerError(e.to_string()))?;

    // The index copy holds the f32-truncated vector; the base item keeps full
    // precision (captured from real DynamoDB, eu-west-2, 2026-08-11).
    let f32_value = AttributeValue::L(
        values
            .iter()
            .map(|v| AttributeValue::N(crate::types::f32_number_string(*v)))
            .collect(),
    );
    let projected = build_vector_index_item(item, vix, pk_attr, sk_attr, &f32_value);
    let item_json = serde_json::to_string(&projected)
        .map_err(|e| DynoxideError::InternalServerError(e.to_string()))?;
    let vector_json = serde_json::to_string(&values)
        .map_err(|e| DynoxideError::InternalServerError(e.to_string()))?;

    Ok(Some(VectorItemRow {
        table_pk: table_pk.to_string(),
        table_sk: table_sk.to_string(),
        hash_value,
        vector_json,
        filter_json,
        item_json,
    }))
}

/// Build the projected item copy for a vector index row. The vector attribute
/// appears only where the projection carries it (ALL, or INCLUDE naming it),
/// always as the f32 copy; SearchSchema attributes play the role GSI index
/// keys do in KEYS_ONLY and INCLUDE projections.
fn build_vector_index_item(
    item: &Item,
    vix: &VectorIndex,
    pk_attr: &str,
    sk_attr: Option<&str>,
    f32_value: &AttributeValue,
) -> Item {
    use crate::types::ProjectionType;

    let vector_attr = vix.vector_attribute.attribute_name.as_str();
    let projection_type = vix.projection.projection_type.clone().unwrap_or_default();

    match projection_type {
        ProjectionType::ALL => {
            let mut projected = item.clone();
            projected.insert(vector_attr.to_string(), f32_value.clone());
            projected
        }
        ProjectionType::KEYS_ONLY | ProjectionType::INCLUDE => {
            let mut projected = Item::new();
            // Table keys
            if let Some(v) = item.get(pk_attr) {
                projected.insert(pk_attr.to_string(), v.clone());
            }
            if let Some(sk) = sk_attr {
                if let Some(v) = item.get(sk) {
                    projected.insert(sk.to_string(), v.clone());
                }
            }
            // SearchSchema attributes
            if let Some(ref schema) = vix.search_schema {
                for elem in schema {
                    if let Some(v) = item.get(&elem.attribute_name) {
                        projected.insert(elem.attribute_name.clone(), v.clone());
                    }
                }
            }
            // Non-key attributes (INCLUDE only), the vector attribute as its
            // f32 copy when named
            if projection_type == ProjectionType::INCLUDE {
                if let Some(ref attrs) = vix.projection.non_key_attributes {
                    for attr in attrs {
                        if attr == vector_attr {
                            projected.insert(attr.clone(), f32_value.clone());
                        } else if let Some(v) = item.get(attr) {
                            projected.insert(attr.clone(), v.clone());
                        }
                    }
                }
            }
            projected
        }
    }
}

/// The billable size of one shadow-table row: the stored vector, the filter
/// values, the projected item copy, and the keys addressing it.
fn vector_row_bytes(row: &VectorItemRow) -> usize {
    row.table_pk.len()
        + row.table_sk.len()
        + row.hash_value.len()
        + row.vector_json.len()
        + row.filter_json.len()
        + row.item_json.len()
}

/// Whether two derived rows hold the same stored view, so an overwrite that
/// produced them is free.
///
/// The projected copy and the filter values are compared as parsed items, not
/// as the JSON they serialise to. `Item` is a `HashMap`, so two equal items can
/// serialise to different strings, and the sets inside one are unordered on
/// DynamoDB's terms while their backing `Vec` is not. The classic fan-out
/// answers the same question with `index_capacity::unchanged`, and this asks
/// there so the two families cannot disagree about what a change is.
///
/// `hash_value` and `vector_json` are compared directly: one is a key string
/// and the other an ordered array of f32s, both deterministic.
fn vector_rows_agree(a: &VectorItemRow, b: &VectorItemRow) -> bool {
    if a.hash_value != b.hash_value || a.vector_json != b.vector_json {
        return false;
    }
    json_items_agree(&a.filter_json, &b.filter_json) && json_items_agree(&a.item_json, &b.item_json)
}

/// Compare two serialised items structurally. Unparseable JSON falls back to a
/// byte comparison, which cannot happen for rows this module derived.
fn json_items_agree(a: &str, b: &str) -> bool {
    match (
        serde_json::from_str::<Item>(a),
        serde_json::from_str::<Item>(b),
    ) {
        (Ok(x), Ok(y)) => super::index_capacity::unchanged(&x, &y),
        _ => a == b,
    }
}

/// Bytes charged against one vector index for a write, or `None` when the
/// index's stored view does not change.
///
/// The rules mirror the classic index replication table: an entry appearing or
/// disappearing costs its own size, an entry changing costs the larger of the
/// two images, and an identical overwrite costs nothing. `None` is distinct
/// from `Some(0.0)`, so an untouched index is absent from the response rather
/// than present and zeroed. Vector figures are bytes against a 1KB floor
/// rather than KB-rounded units.
///
/// The classic table has a sixth row, the key move that costs both sides
/// because it is a delete from one position and an insert into another. A
/// vector shadow row is addressed by the base table key, which an update
/// cannot change, so the row is always replaced in place and that row is
/// unreachable here. Changing the SearchSchema HASH value rewrites a column,
/// not the row's address.
fn vector_write_bytes(old: Option<&VectorItemRow>, new: Option<&VectorItemRow>) -> Option<f64> {
    match (old, new) {
        (None, None) => None,
        (None, Some(row)) | (Some(row), None) => {
            Some(crate::types::vector_request_bytes(vector_row_bytes(row)))
        }
        (Some(before), Some(after)) if vector_rows_agree(before, after) => None,
        (Some(before), Some(after)) => Some(crate::types::vector_request_bytes(
            vector_row_bytes(before).max(vector_row_bytes(after)),
        )),
    }
}

/// Update all vector shadow tables after an item write (put/update),
/// mirroring `maintain_gsis_after_write`: remove any existing row for the
/// base key, then insert the derived row when the item belongs in the index.
/// An item missing the vector attribute or the SearchSchema HASH attribute
/// derives no row, so the delete alone silently de-indexes it.
///
/// Returns a map of index name to the bytes that index's replication cost, on
/// the same absent-not-zero terms as the classic fan-out. `capacity_mode` is
/// the caller's `ReturnConsumedCapacity`, forwarded rather than interpreted;
/// sizing costs a derivation of the old image per index that nothing else
/// wants, so the map comes back empty when nobody asked.
pub async fn maintain_vector_indexes_after_write<S: StorageBackend>(
    storage: &S,
    meta: &TableMetadata,
    target: &IndexWrite<'_>,
    old_item: Option<&Item>,
    item: &Item,
    capacity_mode: Option<&str>,
) -> Result<HashMap<String, f64>> {
    let vixs = parse_vector_defs(meta)?;
    if vixs.is_empty() {
        return Ok(HashMap::new());
    }
    let attr_defs = parse_attr_defs(meta)?;
    maintain_vector_indexes_after_write_with_defs(
        storage,
        &vixs,
        &attr_defs,
        target,
        old_item,
        item,
        capacity_mode,
        false,
    )
    .await
}

/// Defs-accepting form of [`maintain_vector_indexes_after_write`], for callers
/// that parse the definitions once per batch (BatchWriteItem, ImportItems).
/// `skip_deletes` serves the fresh-import fast path, where the caller
/// guarantees no existing shadow row for the base key; every other caller
/// passes `false` so any stale row is removed before the insert.
#[allow(clippy::too_many_arguments)]
pub async fn maintain_vector_indexes_after_write_with_defs<S: StorageBackend>(
    storage: &S,
    vixs: &[VectorIndex],
    attr_defs: &[AttributeDefinition],
    target: &IndexWrite<'_>,
    old_item: Option<&Item>,
    item: &Item,
    capacity_mode: Option<&str>,
    skip_deletes: bool,
) -> Result<HashMap<String, f64>> {
    if vixs.is_empty() {
        return Ok(HashMap::new());
    }
    let want_capacity = crate::types::capacity_wanted(capacity_mode);
    let mut vector_bytes: HashMap<String, f64> = HashMap::new();
    let mut ops: Vec<IndexWriteOp> = Vec::new();

    for vix in vixs {
        if !skip_deletes {
            ops.push(IndexWriteOp::DeleteVector {
                table_name: target.table_name.to_string(),
                index_name: vix.index_name.clone(),
                table_pk: target.pk.to_string(),
                table_sk: target.sk.to_string(),
            });
        }

        // Built once here and handed to the sizing below, which would
        // otherwise derive the same row again.
        let row = vector_index_row(
            item,
            vix,
            target.pk_attr,
            target.sk_attr,
            attr_defs,
            target.pk,
            target.sk,
        )?;
        if let Some(ref row) = row {
            ops.push(IndexWriteOp::InsertVector {
                table_name: target.table_name.to_string(),
                index_name: vix.index_name.clone(),
                row: Box::new(row.clone()),
            });
        }

        if want_capacity {
            let previous = match old_item {
                Some(old) => vector_index_row(
                    old,
                    vix,
                    target.pk_attr,
                    target.sk_attr,
                    attr_defs,
                    target.pk,
                    target.sk,
                )?,
                None => None,
            };
            if let Some(bytes) = vector_write_bytes(previous.as_ref(), row.as_ref()) {
                vector_bytes.insert(vix.index_name.clone(), bytes);
            }
        }
    }

    storage.apply_index_writes(&ops).await?;
    Ok(vector_bytes)
}

/// Remove an item from all vector shadow tables after a delete, mirroring
/// `maintain_gsis_after_delete`. Also called by the TTL reaper.
///
/// Charges only the indexes the deleted item was a member of, sized on the row
/// it held, as on [`maintain_vector_indexes_after_write`].
pub async fn maintain_vector_indexes_after_delete<S: StorageBackend>(
    storage: &S,
    meta: &TableMetadata,
    target: &IndexWrite<'_>,
    old_item: Option<&Item>,
    capacity_mode: Option<&str>,
) -> Result<HashMap<String, f64>> {
    let vixs = parse_vector_defs(meta)?;
    if vixs.is_empty() {
        return Ok(HashMap::new());
    }
    let attr_defs = parse_attr_defs(meta)?;
    maintain_vector_indexes_after_delete_with_defs(
        storage,
        &vixs,
        &attr_defs,
        target,
        old_item,
        capacity_mode,
    )
    .await
}

/// Defs-accepting form of [`maintain_vector_indexes_after_delete`], for
/// callers that parse the definitions once per batch (BatchWriteItem).
pub async fn maintain_vector_indexes_after_delete_with_defs<S: StorageBackend>(
    storage: &S,
    vixs: &[VectorIndex],
    attr_defs: &[AttributeDefinition],
    target: &IndexWrite<'_>,
    old_item: Option<&Item>,
    capacity_mode: Option<&str>,
) -> Result<HashMap<String, f64>> {
    if vixs.is_empty() {
        return Ok(HashMap::new());
    }
    let want_capacity = crate::types::capacity_wanted(capacity_mode);
    let mut vector_bytes: HashMap<String, f64> = HashMap::new();
    let mut ops: Vec<IndexWriteOp> = Vec::new();

    for vix in vixs {
        ops.push(IndexWriteOp::DeleteVector {
            table_name: target.table_name.to_string(),
            index_name: vix.index_name.clone(),
            table_pk: target.pk.to_string(),
            table_sk: target.sk.to_string(),
        });

        if want_capacity
            && let Some(old) = old_item
            && let Some(row) = vector_index_row(
                old,
                vix,
                target.pk_attr,
                target.sk_attr,
                attr_defs,
                target.pk,
                target.sk,
            )?
            && let Some(bytes) = vector_write_bytes(Some(&row), None)
        {
            vector_bytes.insert(vix.index_name.clone(), bytes);
        }
    }

    storage.apply_index_writes(&ops).await?;
    Ok(vector_bytes)
}
