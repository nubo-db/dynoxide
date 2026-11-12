use crate::errors::{DynoxideError, Result};
use crate::storage::TableMetadata;
use crate::storage_backend::StorageBackend;
use crate::types::{
    AttributeDefinition, AttributeValue, KeySchemaElement, KeyType, ScalarAttributeType,
};
use crate::validation::ClassifiedValidationError;
use std::collections::HashMap;
use std::future::Future;

/// Run `body` inside a single backend write transaction.
///
/// Commits on success; if the commit itself fails, rolls back and surfaces the
/// failure rather than leaving the connection stuck mid-transaction (a leftover
/// open transaction makes the next `begin_transaction` fail). On any error from
/// `body`, rolls back; if that rollback also fails, surfaces a combined error.
/// A rollback that fails only because SQLite has already ended the transaction
/// itself (it rolls the whole transaction back on a full disk, among other
/// errors) adds nothing, and the error that stopped the write is returned as
/// it is.
/// A commit that fails for the same reason, after a body that got past the
/// error, is reported as a write that was not committed, naming what SQLite
/// kept and what it rolled back.
///
/// This is the single place the write-transaction lifecycle policy lives, so
/// every write path handles a failed commit or rollback identically.
pub async fn with_write_transaction<S, T, F>(storage: &S, body: F) -> Result<T>
where
    S: StorageBackend,
    F: Future<Output = Result<T>>,
{
    storage.begin_transaction().await?;
    match body.await {
        Ok(value) => match storage.commit().await {
            Ok(()) => Ok(value),
            // SQLite ended the transaction itself after an earlier error the
            // body got past, so there is nothing to commit or roll back.
            Err(commit_err) if found_no_transaction(&commit_err) => {
                Err(DynoxideError::InternalServerError(
                    "the write was not committed: SQLite ended its transaction after an \
                     earlier error, so what ran before that error was rolled back, and \
                     anything run after it was applied on its own"
                        .to_string(),
                ))
            }
            Err(commit_err) => match storage.rollback().await {
                Err(rb_err) if !found_no_transaction(&rb_err) => {
                    Err(DynoxideError::InternalServerError(format!(
                        "commit failed ({commit_err}) and rollback also failed ({rb_err})"
                    )))
                }
                _ => Err(commit_err.into()),
            },
        },
        Err(e) => match storage.rollback().await {
            Err(rb_err) if !found_no_transaction(&rb_err) => {
                Err(DynoxideError::InternalServerError(format!(
                    "operation failed ({e}) and rollback also failed ({rb_err})"
                )))
            }
            _ => Err(e),
        },
    }
}

/// Whether a commit or rollback failed only because no transaction was
/// active: SQLite's `cannot commit - no transaction is active` or `cannot
/// rollback - ...`, which both backends pass on in SQLite's own words.
fn found_no_transaction(err: &crate::storage_backend::BackendError) -> bool {
    matches!(
        err,
        crate::storage_backend::BackendError::Other(message)
            if message.contains("no transaction is active")
    )
}

/// Parsed key schema for convenient access.
pub struct KeySchema {
    pub partition_key: String,
    pub partition_key_type: ScalarAttributeType,
    pub sort_key: Option<String>,
    pub sort_key_type: Option<ScalarAttributeType>,
}

/// Require that a table exists, returning its metadata.
pub async fn require_table<S: StorageBackend>(
    storage: &S,
    table_name: &str,
) -> Result<TableMetadata> {
    storage
        .get_table_metadata(table_name)
        .await?
        .ok_or_else(|| {
            DynoxideError::ResourceNotFoundException(format!(
                "Requested resource not found: Table: {table_name} not found"
            ))
        })
}

/// Like `require_table`, but uses the shorter error message format that DynamoDB
/// uses for item-level operations (PutItem, GetItem, DeleteItem, UpdateItem,
/// Query, Scan, BatchGetItem, BatchWriteItem).
pub async fn require_table_for_item_op<S: StorageBackend>(
    storage: &S,
    table_name: &str,
) -> Result<TableMetadata> {
    storage
        .get_table_metadata(table_name)
        .await?
        .ok_or_else(|| {
            DynoxideError::ResourceNotFoundException("Requested resource not found".to_string())
        })
}

/// Parse key schema and attribute definitions from table metadata.
pub fn parse_key_schema(meta: &TableMetadata) -> Result<KeySchema> {
    crate::bench_counters::record(&crate::bench_counters::KEY_SCHEMA_PARSES);
    let key_schema: Vec<KeySchemaElement> = serde_json::from_str(&meta.key_schema)
        .map_err(|e| DynoxideError::InternalServerError(format!("Bad key schema JSON: {e}")))?;
    let attr_defs: Vec<AttributeDefinition> = serde_json::from_str(&meta.attribute_definitions)
        .map_err(|e| {
            DynoxideError::InternalServerError(format!("Bad attribute definitions JSON: {e}"))
        })?;

    let pk_elem = key_schema
        .iter()
        .find(|k| k.key_type == KeyType::HASH)
        .ok_or_else(|| DynoxideError::InternalServerError("No HASH key in schema".to_string()))?;

    let pk_type = attr_defs
        .iter()
        .find(|d| d.attribute_name == pk_elem.attribute_name)
        .map(|d| d.attribute_type.clone())
        .unwrap_or(ScalarAttributeType::S);

    let sk_elem = key_schema.iter().find(|k| k.key_type == KeyType::RANGE);

    let (sort_key, sort_key_type) = if let Some(sk) = sk_elem {
        let sk_type = attr_defs
            .iter()
            .find(|d| d.attribute_name == sk.attribute_name)
            .map(|d| d.attribute_type.clone())
            .unwrap_or(ScalarAttributeType::S);
        (Some(sk.attribute_name.clone()), Some(sk_type))
    } else {
        (None, None)
    };

    Ok(KeySchema {
        partition_key: pk_elem.attribute_name.clone(),
        partition_key_type: pk_type,
        sort_key,
        sort_key_type,
    })
}

/// Wording for a wrong-type or non-scalar *table* key. The put-shaped paths
/// (PutItem, TransactWriteItems, ImportItems, PartiQL) report the specific
/// "Type mismatch for key" message; BatchWriteItem collapses both cases to the
/// generic schema error, matching real DynamoDB (#97).
#[derive(Clone, Copy)]
enum TableKeyTypeError {
    TypeMismatch,
    SchemaMismatch,
}

/// Validate that an item has the required key attributes with correct types.
pub fn validate_item_keys(
    item: &HashMap<String, AttributeValue>,
    schema: &KeySchema,
    meta: &TableMetadata,
) -> Result<()> {
    validate_item_keys_inner(item, schema, meta, TableKeyTypeError::TypeMismatch)?;
    validate_vector_write_attributes_after_keys(item, meta)
}

/// Like [`validate_item_keys`], but a wrong-type or non-scalar table key yields
/// the generic "The provided key element does not match the schema" error
/// rather than PutItem's "Type mismatch for key" wording. BatchWriteItem put
/// requests use this: real DynamoDB collapses both cases in a batch (#97). The
/// empty-string table-key message and the index-key messages are unchanged.
pub fn validate_item_keys_for_batch(
    item: &HashMap<String, AttributeValue>,
    schema: &KeySchema,
    meta: &TableMetadata,
) -> Result<()> {
    validate_item_keys_inner(item, schema, meta, TableKeyTypeError::SchemaMismatch)?;
    validate_vector_write_attributes_after_keys(item, meta)
}

/// Like [`validate_item_keys`], but skips the vector index write validation.
/// ImportItems uses this: an import is backfill-shaped, so a vector index
/// value a live write would reject is sparse-skipped by the row derivation
/// (base item imported, no shadow row) rather than failing the import. The
/// classic table-key and GSI/LSI index-key checks are exactly those of
/// [`validate_item_keys`].
pub fn validate_item_keys_for_import(
    item: &HashMap<String, AttributeValue>,
    schema: &KeySchema,
    meta: &TableMetadata,
) -> Result<()> {
    validate_item_keys_inner(item, schema, meta, TableKeyTypeError::TypeMismatch)
}

/// Reject any present-but-invalid vector index attribute (the vector value,
/// or a SearchSchema HASH or INLINE_FILTER value). Runs after the classic
/// index-key checks; where real AWS orders the vector family relative to the
/// classic one is uncaptured, so it follows the family it borrows its shape
/// from.
fn validate_vector_write_attributes_after_keys(
    item: &HashMap<String, AttributeValue>,
    meta: &TableMetadata,
) -> Result<()> {
    super::vector_index::validate_vector_write_attributes(item, meta)
}

fn validate_item_keys_inner(
    item: &HashMap<String, AttributeValue>,
    schema: &KeySchema,
    meta: &TableMetadata,
    type_error: TableKeyTypeError,
) -> Result<()> {
    // Partition key must be present
    let pk_val = item.get(&schema.partition_key).ok_or_else(|| {
        DynoxideError::ValidationException(format!(
            "One or more parameter values were invalid: Missing the key {} in the item",
            schema.partition_key
        ))
    })?;

    validate_key_type(
        pk_val,
        &schema.partition_key,
        &schema.partition_key_type,
        type_error,
    )?;

    // Validate hash key size (max 2048 bytes)
    let pk_size = key_attribute_size(pk_val);
    if pk_size > 2048 {
        return Err(DynoxideError::ValidationException(
            HASH_KEY_TOO_LARGE.to_string(),
        ));
    }

    // Sort key must be present if table has one
    if let Some(ref sk_name) = schema.sort_key {
        let sk_val = item.get(sk_name).ok_or_else(|| {
            DynoxideError::ValidationException(format!(
                "One or more parameter values were invalid: Missing the key {sk_name} in the item"
            ))
        })?;

        if let Some(ref sk_type) = schema.sort_key_type {
            validate_key_type(sk_val, sk_name, sk_type, type_error)?;
        }

        // Validate range key size (max 1024 bytes)
        let sk_size = key_attribute_size(sk_val);
        if sk_size > 1024 {
            return Err(DynoxideError::ValidationException(
                RANGE_KEY_TOO_LARGE.to_string(),
            ));
        }
    }

    // Reject any present-but-invalid GSI/LSI key attribute. Runs after the
    // table-key checks so a bad table key is reported first. The vector
    // family's write validation is layered on by the public wrappers above,
    // so the import path can keep these classic checks while letting its row
    // derivation sparse-skip invalid vector shapes.
    validate_index_key_attributes(item, meta)?;

    Ok(())
}

/// A secondary-index key attribute and its declared scalar type.
struct IndexKeyEntry {
    index_name: String,
    attr: String,
    declared: ScalarAttributeType,
}

/// How a present index key value is invalid for its declared type.
enum IndexKeyViolation {
    TypeMismatch {
        actual: &'static str,
        expected: ScalarAttributeType,
    },
    EmptyString,
    EmptyBinary,
}

impl IndexKeyViolation {
    /// The exact DynamoDB `ValidationException` message for this violation.
    fn message(&self, attr: &str, index: &str) -> String {
        match self {
            IndexKeyViolation::TypeMismatch { actual, expected } => format!(
                "One or more parameter values were invalid: Type mismatch for Index Key \
                 {attr} Expected: {} Actual: {actual} IndexName: {index}",
                scalar_type_str(expected)
            ),
            IndexKeyViolation::EmptyString => format!(
                "One or more parameter values are not valid. A value specified for a secondary \
                 index key is not supported. The AttributeValue for a key attribute cannot \
                 contain an empty string value. IndexName: {index}, IndexKey: {attr}"
            ),
            IndexKeyViolation::EmptyBinary => format!(
                "One or more parameter values are not valid. A value specified for a secondary \
                 index key is not supported. The AttributeValue for a key attribute cannot \
                 contain an empty binary value. IndexName: {index}, IndexKey: {attr}"
            ),
        }
    }
}

fn scalar_type_str(t: &ScalarAttributeType) -> &'static str {
    match t {
        ScalarAttributeType::S => "S",
        ScalarAttributeType::N => "N",
        ScalarAttributeType::B => "B",
    }
}

/// Every GSI/LSI key attribute with its declared scalar type. Types come from
/// `AttributeDefinitions`, joined by name like `parse_key_schema`; a missing
/// definition is an internal error (an index key is always defined).
fn index_key_entries(meta: &TableMetadata) -> Result<Vec<IndexKeyEntry>> {
    let attr_defs: Vec<AttributeDefinition> = serde_json::from_str(&meta.attribute_definitions)
        .map_err(|e| {
            DynoxideError::InternalServerError(format!("Bad attribute definitions JSON: {e}"))
        })?;
    let declared = |attr: &str| -> Result<ScalarAttributeType> {
        attr_defs
            .iter()
            .find(|d| d.attribute_name == attr)
            .map(|d| d.attribute_type.clone())
            .ok_or_else(|| {
                DynoxideError::InternalServerError(format!(
                    "Index key attribute {attr} missing from AttributeDefinitions"
                ))
            })
    };

    let mut entries = Vec::new();
    // Every attribute of a multi-attribute GSI key is checked, in key-schema
    // order, partition key attributes first.
    for gsi in super::gsi::parse_gsi_defs(meta)? {
        for attr in gsi.hash_attrs.iter().chain(&gsi.range_attrs) {
            entries.push(IndexKeyEntry {
                declared: declared(attr)?,
                attr: attr.clone(),
                index_name: gsi.index_name.clone(),
            });
        }
    }
    // An LSI shares the table partition key (validated as a table key), so only its
    // sort key is index-specific.
    for lsi in super::lsi::parse_lsi_defs(meta)? {
        if let Some(sk) = lsi.sk_attr {
            entries.push(IndexKeyEntry {
                declared: declared(&sk)?,
                attr: sk,
                index_name: lsi.index_name,
            });
        }
    }
    Ok(entries)
}

/// Classify a present index key value against its declared type, or `None` if valid.
fn check_index_key_value(
    val: &AttributeValue,
    declared: &ScalarAttributeType,
) -> Option<IndexKeyViolation> {
    match (val, declared) {
        (AttributeValue::S(s), ScalarAttributeType::S) => {
            s.is_empty().then_some(IndexKeyViolation::EmptyString)
        }
        (AttributeValue::N(_), ScalarAttributeType::N) => None,
        (AttributeValue::B(b), ScalarAttributeType::B) => {
            b.is_empty().then_some(IndexKeyViolation::EmptyBinary)
        }
        _ => Some(IndexKeyViolation::TypeMismatch {
            actual: val.type_name(),
            expected: declared.clone(),
        }),
    }
}

/// AWS's message for an empty-string secondary-index key set by an update expression.
/// Unlike the put form, it carries no `IndexName`/`IndexKey` suffix.
const UPDATE_EMPTY_STRING_INDEX_KEY_MSG: &str = "One or more parameter values are not valid. \
     The update expression attempted to update a secondary index key to a value that is not \
     supported. The AttributeValue for a key attribute cannot contain an empty string value.";

/// AWS's message for an empty-binary secondary-index key set by an update expression.
/// The empty-binary analogue of `UPDATE_EMPTY_STRING_INDEX_KEY_MSG` (real AWS, captured).
const UPDATE_EMPTY_BINARY_INDEX_KEY_MSG: &str = "One or more parameter values are not valid. \
     The update expression attempted to update a secondary index key to a value that is not \
     supported. The AttributeValue for a key attribute cannot contain an empty binary value.";

/// Reject the first invalid index key present in `item`, naming the
/// alphabetically-first offending index as DynamoDB does. With `before` set, only
/// attributes whose value changed are checked.
fn run_index_key_validation(
    item: &HashMap<String, AttributeValue>,
    meta: &TableMetadata,
    before: Option<&HashMap<String, AttributeValue>>,
) -> Result<()> {
    let entries = index_key_entries(meta)?;
    let mut violations: Vec<(String, String, IndexKeyViolation)> = Vec::new();
    for entry in &entries {
        let Some(val) = item.get(&entry.attr) else {
            continue; // absent -> sparse, not an error
        };
        if let Some(before) = before
            && before
                .get(&entry.attr)
                .is_some_and(|old| super::index_capacity::value_unchanged(old, val))
        {
            // Unchanged by this update. Sets compare as sets: a row stored
            // before sets were sorted holds the same set in another order.
            continue;
        }
        if let Some(violation) = check_index_key_value(val, &entry.declared) {
            violations.push((entry.index_name.clone(), entry.attr.clone(), violation));
        }
    }
    let Some((index, attr, violation)) = violations.into_iter().min_by(|a, b| a.0.cmp(&b.0)) else {
        return Ok(());
    };
    match violation {
        // Both empty-value index keys carry the routable class so they hoist to a
        // top-level ValidationException inside a transaction (real AWS, captured),
        // and use the distinct update-path message when set by an update expression.
        IndexKeyViolation::EmptyString => {
            let msg = if before.is_some() {
                UPDATE_EMPTY_STRING_INDEX_KEY_MSG.to_string()
            } else {
                IndexKeyViolation::EmptyString.message(&attr, &index)
            };
            Err(DynoxideError::KeyEmptyValueValidation(msg))
        }
        IndexKeyViolation::EmptyBinary => {
            let msg = if before.is_some() {
                UPDATE_EMPTY_BINARY_INDEX_KEY_MSG.to_string()
            } else {
                IndexKeyViolation::EmptyBinary.message(&attr, &index)
            };
            Err(DynoxideError::KeyEmptyValueValidation(msg))
        }
        // TypeMismatch only: a wrong-type index key stays a plain ValidationException
        // (a ValidationError cancellation reason inside a transaction).
        other => Err(DynoxideError::ValidationException(
            other.message(&attr, &index),
        )),
    }
}

/// Reject any present-but-invalid GSI/LSI key attribute. Absent attributes are
/// fine (sparse index). Used by the put-shaped write paths via `validate_item_keys`.
pub fn validate_index_key_attributes(
    item: &HashMap<String, AttributeValue>,
    meta: &TableMetadata,
) -> Result<()> {
    run_index_key_validation(item, meta, None)
}

/// Like [`validate_index_key_attributes`], but only checks index keys this update
/// changed against `before`, so an unrelated update never re-rejects an untouched
/// value. Used by the update-shaped paths.
pub fn validate_updated_index_keys(
    before: &HashMap<String, AttributeValue>,
    after: &HashMap<String, AttributeValue>,
    meta: &TableMetadata,
) -> Result<()> {
    run_index_key_validation(after, meta, Some(before))?;
    // The vector family gets the same changed-values-only treatment, so an
    // unrelated update never re-rejects a pre-existing invalid vector value
    // (for example an item the index backfill sparse-skipped).
    super::vector_index::validate_updated_vector_attributes(before, after, meta)
}

/// Validate that a Key map has exactly the key attributes (for GetItem/DeleteItem/UpdateItem).
///
/// DynamoDB returns a single generic error for all key schema mismatches:
/// wrong attribute count, missing key, wrong type, extra attributes, etc.
pub fn validate_key_only(key: &HashMap<String, AttributeValue>, schema: &KeySchema) -> Result<()> {
    let expected_count = if schema.sort_key.is_some() { 2 } else { 1 };

    // Wrong number of key attributes
    if key.len() != expected_count {
        return Err(DynoxideError::ValidationException(
            "The provided key element does not match the schema".to_string(),
        ));
    }

    // Partition key must be present and have correct type
    let pk_val = key.get(&schema.partition_key).ok_or_else(|| {
        DynoxideError::ValidationException(
            "The provided key element does not match the schema".to_string(),
        )
    })?;
    validate_key_type_for_key_op(pk_val, &schema.partition_key, &schema.partition_key_type)?;

    // Validate hash key size (max 2048 bytes)
    let pk_size = key_attribute_size(pk_val);
    if pk_size > 2048 {
        return Err(DynoxideError::ValidationException(
            HASH_KEY_TOO_LARGE.to_string(),
        ));
    }

    // Sort key must be present and have correct type (if table has one)
    if let Some(ref sk_name) = schema.sort_key {
        let sk_val = key.get(sk_name).ok_or_else(|| {
            DynoxideError::ValidationException(
                "The provided key element does not match the schema".to_string(),
            )
        })?;
        if let Some(ref sk_type) = schema.sort_key_type {
            validate_key_type_for_key_op(sk_val, sk_name, sk_type)?;
        }

        // Validate range key size (max 1024 bytes)
        let sk_size = key_attribute_size(sk_val);
        if sk_size > 1024 {
            return Err(DynoxideError::ValidationException(
                RANGE_KEY_TOO_LARGE.to_string(),
            ));
        }
    }

    Ok(())
}

/// Validate a key value matches its expected type. `type_error` selects the
/// wording for a wrong-type or non-scalar value (see [`TableKeyTypeError`]);
/// the empty-string and empty-binary messages are the same either way.
fn validate_key_type(
    val: &AttributeValue,
    attr_name: &str,
    expected: &ScalarAttributeType,
    type_error: TableKeyTypeError,
) -> Result<()> {
    let matches = match (val, expected) {
        (AttributeValue::S(s), ScalarAttributeType::S) => {
            if s.is_empty() {
                return Err(DynoxideError::KeyEmptyValueValidation(format!(
                    "One or more parameter values are not valid. The AttributeValue for a key \
                     attribute cannot contain an empty string value. Key: {attr_name}"
                )));
            }
            true
        }
        (AttributeValue::N(_), ScalarAttributeType::N) => true,
        (AttributeValue::B(b), ScalarAttributeType::B) => {
            if b.is_empty() {
                // Empty-binary is the routable empty-value class too, so it hoists to a
                // top-level ValidationException inside a transaction (real AWS, captured).
                return Err(DynoxideError::KeyEmptyValueValidation(format!(
                    "One or more parameter values are not valid. The AttributeValue for a key \
                     attribute cannot contain an empty binary value. Key: {attr_name}"
                )));
            }
            true
        }
        _ => false,
    };

    if !matches {
        return Err(match type_error {
            TableKeyTypeError::TypeMismatch => {
                let actual_type = match val {
                    AttributeValue::S(_) => "S",
                    AttributeValue::N(_) => "N",
                    AttributeValue::B(_) => "B",
                    AttributeValue::SS(_) => "SS",
                    AttributeValue::NS(_) => "NS",
                    AttributeValue::BS(_) => "BS",
                    AttributeValue::BOOL(_) => "BOOL",
                    AttributeValue::NULL(_) => "NULL",
                    AttributeValue::L(_) => "L",
                    AttributeValue::M(_) => "M",
                };
                DynoxideError::ValidationException(format!(
                    "One or more parameter values were invalid: Type mismatch for key \
                     {attr_name} expected: {expected:?} actual: {actual_type}"
                ))
            }
            TableKeyTypeError::SchemaMismatch => DynoxideError::ValidationException(
                "The provided key element does not match the schema".to_string(),
            ),
        });
    }

    Ok(())
}

/// Validate a key value matches its expected type for key-based operations
/// (GetItem, DeleteItem, UpdateItem).
///
/// Unlike `validate_key_type` (used by PutItem), this returns the generic
/// "does not match the schema" error for type mismatches. An empty-string or
/// empty-binary key value emits `KeyEmptyValueValidation` with the same
/// "are not valid." wording as `validate_key_type`, so both hoist to a
/// top-level `ValidationException` inside a transaction.
fn validate_key_type_for_key_op(
    val: &AttributeValue,
    attr_name: &str,
    expected: &ScalarAttributeType,
) -> Result<()> {
    let matches = match (val, expected) {
        (AttributeValue::S(s), ScalarAttributeType::S) => {
            if s.is_empty() {
                // Carry the routable KeyEmptyValueValidation class so the transact-write
                // loop hoists this to a top-level ValidationException (matching real AWS),
                // while single-action and batch callers surface the same top-level error
                // through the shared error mapping. Mirrors validate_key_type's item-key arm.
                return Err(DynoxideError::KeyEmptyValueValidation(format!(
                    "One or more parameter values are not valid. The AttributeValue for a key \
                     attribute cannot contain an empty string value. Key: {attr_name}"
                )));
            }
            true
        }
        (AttributeValue::N(_), ScalarAttributeType::N) => true,
        (AttributeValue::B(b), ScalarAttributeType::B) => {
            if b.is_empty() {
                // Empty-binary mirrors empty-string (real AWS, captured): the same
                // "are not valid." wording and the routable empty-value class, so it
                // hoists to a top-level ValidationException inside a transaction.
                return Err(DynoxideError::KeyEmptyValueValidation(format!(
                    "One or more parameter values are not valid. The AttributeValue for a key \
                     attribute cannot contain an empty binary value. Key: {attr_name}"
                )));
            }
            true
        }
        _ => false,
    };

    if !matches {
        return Err(DynoxideError::ValidationException(
            "The provided key element does not match the schema".to_string(),
        ));
    }

    Ok(())
}

/// DynamoDB's refusal of a partition key value over 2,048 bytes, spelled as
/// it spells it (no space before the figure).
pub(crate) const HASH_KEY_TOO_LARGE: &str = "One or more parameter values were invalid: \
     Size of hashkey has exceeded the maximum size limit of2048 bytes";

/// DynamoDB's refusal of a sort key value over 1,024 bytes.
pub(crate) const RANGE_KEY_TOO_LARGE: &str = "One or more parameter values were invalid: \
     Aggregated size of all range keys has exceeded the size limit of 1024 bytes";

/// The start of DynamoDB's refusal of an update that sets or removes a key
/// attribute, which goes on to name the attribute: see
/// [`key_attribute_update_refusal`].
pub(crate) const KEY_ATTRIBUTE_UPDATE: &str =
    "One or more parameter values were invalid: Cannot update attribute ";

/// DynamoDB's refusal of an update that sets or removes the key attribute
/// `attribute`, on UpdateItem and a PartiQL UPDATE alike.
pub(crate) fn key_attribute_update_refusal(attribute: &str) -> DynoxideError {
    DynoxideError::ValidationException(format!(
        "{KEY_ATTRIBUTE_UPDATE}{attribute}. This attribute is part of the key"
    ))
}

/// The refusal for a key value larger than a key can be, if it is: over
/// 2,048 bytes for a partition key, over 1,024 for a sort key.
pub(crate) fn key_size_refusal(value: &AttributeValue, sort_key: bool) -> Option<DynoxideError> {
    let (limit, message) = if sort_key {
        (1024, RANGE_KEY_TOO_LARGE)
    } else {
        (2048, HASH_KEY_TOO_LARGE)
    };
    (key_attribute_size(value) > limit)
        .then(|| DynoxideError::ValidationException(message.to_string()))
}

/// Return the byte size of a key attribute value for size limit checks.
pub(crate) fn key_attribute_size(val: &AttributeValue) -> usize {
    match val {
        AttributeValue::S(s) => s.len(),
        AttributeValue::N(n) => n.len(),
        AttributeValue::B(b) => b.len(),
        _ => 0,
    }
}

// ---------------------------------------------------------------------------
// Expression parameter validation
// ---------------------------------------------------------------------------

/// Parameters describing which expression and non-expression parameters are present.
pub struct ExpressionParamContext<'a> {
    /// Names of non-expression parameters that are present (e.g. "Expected", "AttributeUpdates", "AttributesToGet")
    pub non_expression_params: Vec<&'a str>,
    /// Names of expression parameters that are present (e.g. "ConditionExpression", "UpdateExpression", "ProjectionExpression")
    pub expression_params: Vec<&'a str>,
    /// The expression parameters that can use ExpressionAttributeValues, where
    /// only some of the operation's can: without one of them, values are
    /// refused even beside another expression (a Query's or Scan's
    /// ProjectionExpression, captured eu-west-2 and us-east-1, 2026-11-11).
    /// Empty where any expression will do.
    pub value_expression_params: &'a [&'a str],
    pub expression_attribute_names: &'a Option<HashMap<String, String>>,
    pub expression_attribute_values: &'a Option<HashMap<String, AttributeValue>>,
    /// Raw JSON for ExpressionAttributeValues: when present, used for key
    /// validation and parsed with key-specific error messages.
    pub expression_attribute_values_raw: &'a Option<serde_json::Value>,
    /// The validation layer the operation runs on, which decides how a bad
    /// number or a duplicated set member in a value is worded.
    pub layer: crate::validation::ValueLayer,
}

/// Refuse a placeholder whose name holds a character outside ASCII before the
/// expression is read: DynamoDB reports the map's key, not the expression
/// (`:vé`, `#n€`, an emoji or a no-break space; captured on UpdateItem in
/// eu-west-2 and us-east-1, 2026-10-09, inside the request-validation
/// envelope). The family is enveloped like the others here, so operations that
/// don't wrap it propagate the plain message.
fn reject_non_ascii_placeholder(
    map: &str,
    key: &str,
) -> std::result::Result<(), ClassifiedValidationError> {
    if key.is_ascii() {
        return Ok(());
    }
    Err(ClassifiedValidationError::enveloped(format!(
        "{map} contains invalid key: Syntax error; key: \"{key}\""
    )))
}

/// Validate expression vs non-expression parameter conflicts and basic
/// ExpressionAttributeNames/Values key format.
///
/// This performs the pre-checks that DynamoDB runs before table existence and
/// key schema validation.
///
/// Returns the parsed `ExpressionAttributeValues` if raw JSON was provided
/// and successfully parsed, so the caller can store it on the request.
///
/// The error is classified: the families DynamoDB wraps in the
/// request-validation envelope on PutItem, UpdateItem and DeleteItem (mixing
/// expression and non-expression parameters, ExpressionAttributeValues or
/// ExpressionAttributeNames without an expression, either map empty, and empty
/// or duplicate sets inside ExpressionAttributeValues) are marked as
/// enveloped. Callers other than those three operations propagate with `?`,
/// which converts back to the plain error unchanged.
pub fn validate_expression_params(
    ctx: &ExpressionParamContext<'_>,
) -> std::result::Result<Option<HashMap<String, AttributeValue>>, ClassifiedValidationError> {
    let has_expressions = !ctx.expression_params.is_empty();
    let has_non_expressions = !ctx.non_expression_params.is_empty();
    let has_names = ctx.expression_attribute_names.is_some();
    // EAV is present if either parsed or raw is Some
    let has_values =
        ctx.expression_attribute_values.is_some() || ctx.expression_attribute_values_raw.is_some();

    // Check expression/non-expression conflict
    if has_expressions && has_non_expressions {
        let non_expr = ctx.non_expression_params.join(", ");
        let expr = ctx.expression_params.join(", ");
        return Err(ClassifiedValidationError::enveloped(format!(
            "Can not use both expression and non-expression parameters in the same request: \
             Non-expression parameters: {{{non_expr}}} Expression parameters: {{{expr}}}"
        )));
    }

    // ExpressionAttributeNames without any expression
    if has_names && !has_expressions {
        return Err(ClassifiedValidationError::enveloped(
            "ExpressionAttributeNames can only be specified when using expressions",
        ));
    }

    // ExpressionAttributeValues without an expression that can use them. The
    // message is the same on every operation, with nothing after it (Query and
    // Scan captured eu-west-2 and us-east-1, 2026-11-11).
    let can_use_values = if ctx.value_expression_params.is_empty() {
        has_expressions
    } else {
        ctx.expression_params
            .iter()
            .any(|p| ctx.value_expression_params.contains(p))
    };
    if has_values && !can_use_values {
        return Err(ClassifiedValidationError::enveloped(
            "ExpressionAttributeValues can only be specified when using expressions",
        ));
    }

    // Empty ExpressionAttributeNames
    if let Some(names) = ctx.expression_attribute_names {
        if names.is_empty() {
            return Err(ClassifiedValidationError::enveloped(
                "ExpressionAttributeNames must not be empty",
            ));
        }
        // Validate key format (must start with #)
        for key in names.keys() {
            if !key.starts_with('#') {
                return Err(ClassifiedValidationError::bare(format!(
                    "ExpressionAttributeNames contains invalid key: Syntax error; key: \"{key}\""
                )));
            }
            reject_non_ascii_placeholder("ExpressionAttributeNames", key)?;
        }
        // An empty name, or one given as null, is refused here on the older
        // layer, which doesn't check it ahead of everything else (see
        // `empty_names::names_checked_early`).
        if let Some(placeholder) = crate::empty_names::empty_placeholder(Some(names)) {
            return Err(ClassifiedValidationError::enveloped(
                crate::empty_names::placeholder_message(placeholder),
            ));
        }
    }

    // ExpressionAttributeValues: prefer raw JSON if available (for key-specific errors)
    if let Some(raw_val) = ctx.expression_attribute_values_raw {
        if let Some(obj) = raw_val.as_object() {
            if obj.is_empty() {
                return Err(ClassifiedValidationError::enveloped(
                    "ExpressionAttributeValues must not be empty",
                ));
            }
            // Validate key format (must start with :)
            for key in obj.keys() {
                if !key.starts_with(':') {
                    return Err(ClassifiedValidationError::bare(format!(
                        "ExpressionAttributeValues contains invalid key: Syntax error; key: \"{key}\""
                    )));
                }
                reject_non_ascii_placeholder("ExpressionAttributeValues", key)?;
            }
            // Parse and validate values with key context
            let parsed =
                parse_expression_attribute_values_raw(raw_val, ctx.layer).map_err(|e| match e {
                    DynoxideError::EnvelopedValidation(msg) => {
                        ClassifiedValidationError::enveloped(msg)
                    }
                    other => other.into(),
                })?;
            // Validate parsed values (NULL:false, empty sets, duplicates, numbers)
            for (key, value) in &parsed {
                validate_expression_attribute_value(key, value, ctx.layer)?;
            }
            return Ok(Some(parsed));
        }
    } else if let Some(values) = ctx.expression_attribute_values {
        // Pre-parsed EAV (e.g. from MCP/library usage)
        if values.is_empty() {
            return Err(ClassifiedValidationError::enveloped(
                "ExpressionAttributeValues must not be empty",
            ));
        }
        for key in values.keys() {
            if !key.starts_with(':') {
                return Err(ClassifiedValidationError::bare(format!(
                    "ExpressionAttributeValues contains invalid key: Syntax error; key: \"{key}\""
                )));
            }
            reject_non_ascii_placeholder("ExpressionAttributeValues", key)?;
        }
        for (key, value) in values {
            validate_expression_attribute_value(key, value, ctx.layer)?;
        }
    }

    Ok(None)
}

/// Validate a single ExpressionAttributeValues entry.
///
/// On the current layer a duplicated set member or a number it can't store is
/// refused in that layer's words with no `ExpressionAttributeValues contains
/// invalid value` wrapper, marked for the envelope; the older layer keeps the
/// wrapper naming the key (eu-west-2, us-east-1, eu-west-1 and us-west-2,
/// 2026-10-09).
fn validate_expression_attribute_value(
    key: &str,
    value: &AttributeValue,
    layer: crate::validation::ValueLayer,
) -> std::result::Result<(), ClassifiedValidationError> {
    use crate::validation::ValueLayer;
    // Over-deep expression values are rejected up front, before evaluation, with the
    // nesting message real DynamoDB uses (no "contains invalid value" wrapper).
    // PutItem, UpdateItem and DeleteItem wrap it in the envelope (us-east-1 and
    // eu-west-2, 2026-10-09); the reads send it bare.
    // From 63 levels a second check refuses the value naming its key, bare
    // (us-east-1 and eu-west-2, 2026-10-09).
    if crate::validation::validate_nesting_depth(value).is_err() {
        if crate::validation::nesting_depth(value) > crate::validation::SECOND_NESTING_LIMIT {
            return Err(ClassifiedValidationError::bare(format!(
                "ExpressionAttributeValues contains invalid value: {} for key {key}",
                crate::validation::NESTING_LIMIT_MESSAGE
            )));
        }
        return Err(ClassifiedValidationError::enveloped(
            crate::validation::NESTING_LIMIT_MESSAGE,
        ));
    }
    // Check for empty/unsupported datatypes
    match value {
        // Unreachable from PutItem and UpdateItem, whose execute paths reject
        // NULL(false) first with the captured message (no per-key wrapper),
        // tagged for the envelope; this arm serves the other operations'
        // pre-parsed in-process EAV paths.
        AttributeValue::NULL(b) if !b => {
            return Err(ClassifiedValidationError::bare(
                crate::serde_errors::named_value_refusal(crate::serde_errors::NULL_NOT_TRUE, key),
            ));
        }
        AttributeValue::SS(set) if set.is_empty() => {
            return Err(ClassifiedValidationError::enveloped(format!(
                "ExpressionAttributeValues contains invalid value: \
                 One or more parameter values were invalid: \
                 An string set  may not be empty for key {key}"
            )));
        }
        AttributeValue::NS(set) if set.is_empty() => {
            return Err(ClassifiedValidationError::enveloped(format!(
                "ExpressionAttributeValues contains invalid value: \
                 One or more parameter values were invalid: \
                 An number set  may not be empty for key {key}"
            )));
        }
        AttributeValue::BS(set) if set.is_empty() => {
            return Err(ClassifiedValidationError::enveloped(format!(
                "ExpressionAttributeValues contains invalid value: \
                 One or more parameter values were invalid: \
                 Binary sets should not be empty for key {key}"
            )));
        }
        AttributeValue::SS(_) | AttributeValue::BS(_) => {
            if let Some(message) = crate::validation::duplicate_set_message(value, layer) {
                return Err(ClassifiedValidationError::enveloped(match layer {
                    ValueLayer::Current => message,
                    ValueLayer::Older => format!(
                        "ExpressionAttributeValues contains invalid value: {message} for key {key}"
                    ),
                }));
            }
        }
        AttributeValue::N(_) | AttributeValue::NS(_) => {
            let numbers: Vec<&String> = match value {
                AttributeValue::N(n) => vec![n],
                AttributeValue::NS(set) => set.iter().collect(),
                _ => Vec::new(),
            };
            for n in numbers {
                layer.check_number(n).map_err(|e| match layer {
                    ValueLayer::Current => layer.classify_number_error(e),
                    ValueLayer::Older => {
                        let inner = match &e {
                            DynoxideError::ValidationException(m) => m.clone(),
                            _ => e.to_string(),
                        };
                        ClassifiedValidationError::bare(format!(
                            "ExpressionAttributeValues contains invalid value: {inner} for key {key}"
                        ))
                    }
                })?;
            }
            if let Some(message) = crate::validation::duplicate_set_message(value, layer) {
                return Err(ClassifiedValidationError::enveloped(match layer {
                    ValueLayer::Current => message,
                    ValueLayer::Older => format!(
                        "ExpressionAttributeValues contains invalid value: {message} for key {key}"
                    ),
                }));
            }
        }
        _ => {}
    }
    Ok(())
}

/// Reject `{NULL: false}` anywhere in an attribute-value map, recursing
/// through lists and maps.
///
/// Matches the request-deserialisation rejection so the in-process API agrees
/// with HTTP; confirmed against real DynamoDB (eu-west-2). Only PutItem and
/// UpdateItem call this, for the captured positions (item body, Key,
/// ExpressionAttributeValues and AttributeUpdates); batch, transact and import
/// behaviour is unchanged. The error carries the wire-invisible envelope tag,
/// unwrapped at the operation boundary.
pub fn validate_no_null_false(values: &HashMap<String, AttributeValue>) -> Result<()> {
    for value in values.values() {
        validate_no_null_false_value(value)?;
    }
    Ok(())
}

/// Single-value form of [`validate_no_null_false`], for callers holding a bare
/// `AttributeValue` rather than a map (the legacy AttributeUpdates entries).
pub(crate) fn validate_no_null_false_value(value: &AttributeValue) -> Result<()> {
    match value {
        AttributeValue::NULL(b) if !b => Err(DynoxideError::EnvelopedValidation(
            crate::serde_errors::NULL_NOT_TRUE.to_string(),
        )),
        AttributeValue::L(list) => {
            for v in list {
                validate_no_null_false_value(v)?;
            }
            Ok(())
        }
        AttributeValue::M(map) => {
            for v in map.values() {
                validate_no_null_false_value(v)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// Parse raw EAV JSON into a `HashMap<String, AttributeValue>` with key-specific
/// error messages.
///
/// This function handles the case where individual attribute values may have
/// validation errors (invalid numbers, multiple datatypes, unsupported types)
/// and wraps them with the `ExpressionAttributeValues contains invalid value: ... for key :x`
/// format that DynamoDB uses.
pub fn parse_expression_attribute_values_raw(
    raw: &serde_json::Value,
    layer: crate::validation::ValueLayer,
) -> Result<HashMap<String, AttributeValue>> {
    let obj = raw.as_object().ok_or_else(|| {
        DynoxideError::SerializationException(
            "Start of structure or map found where not expected".to_string(),
        )
    })?;

    let mut result = HashMap::new();
    for (key, value) in obj {
        match crate::types::with_value_layer(layer, || {
            serde_json::from_value::<AttributeValue>(value.clone())
        }) {
            Ok(av) => {
                result.insert(key.clone(), av);
            }
            Err(e) => {
                use crate::serde_errors::{
                    EMPTY_VALUE, MORE_THAN_ONE_TYPE, ValueRefusal, named_value_refusal,
                    value_refusal,
                };
                let msg = e.to_string();
                // A malformed value fails the request as it is read, before
                // any key is named.
                if let Some(error) = crate::serde_errors::marked_serialization_error(&msg) {
                    return Err(error);
                }
                return Err(match value_refusal(&msg) {
                    // A JSON null is named by key on every operation (captured
                    // eu-west-2 and us-east-1, 2026-11-11).
                    ValueRefusal::Null(inner) => {
                        DynoxideError::ValidationException(named_value_refusal(inner, key))
                    }
                    ValueRefusal::Empty => {
                        DynoxideError::ValidationException(named_value_refusal(EMPTY_VALUE, key))
                    }
                    ValueRefusal::MoreThanOneType => DynoxideError::ValidationException(
                        named_value_refusal(MORE_THAN_ONE_TYPE, key),
                    ),
                    // Only a value with more than one member is refused for an
                    // empty set as it is read, on the older layer, which names
                    // the key (captured eu-west-2 and us-east-1, 2026-10-10).
                    ValueRefusal::EmptySet(inner) => {
                        DynoxideError::ValidationException(named_value_refusal(inner, key))
                    }
                    // The current layer refuses it with no wrapper, in the
                    // envelope on DeleteItem; the older layer names the key.
                    ValueRefusal::Number(inner) => match layer {
                        crate::validation::ValueLayer::Current => {
                            DynoxideError::EnvelopedValidation(inner.to_string())
                        }
                        crate::validation::ValueLayer::Older => {
                            DynoxideError::ValidationException(named_value_refusal(inner, key))
                        }
                    },
                    // A NULL refused as it was parsed (a string that reads as
                    // false, or false beside another member) is named by key,
                    // as an empty value is (captured eu-west-2 and us-east-1,
                    // 2026-10-10, on UpdateItem, DeleteItem and Query).
                    ValueRefusal::ParsedNull(inner) => {
                        DynoxideError::ValidationException(named_value_refusal(inner, key))
                    }
                    // A rejected {"NULL": false} is refused with the bare
                    // validation message, without the per-key wrapper DynamoDB
                    // uses for empty or malformed values (eu-west-2). It is the
                    // request-validation family DeleteItem wraps in the
                    // envelope (eu-west-2 and us-east-1, 2026-10-09), so it is
                    // tagged; a read that reports it bare strips the tag.
                    ValueRefusal::RequestNull(inner) => {
                        DynoxideError::EnvelopedValidation(inner.to_string())
                    }
                    ValueRefusal::Other => DynoxideError::SerializationException(msg),
                });
            }
        }
    }
    Ok(result)
}

/// Read the legacy `Expected` map of PutItem, UpdateItem or DeleteItem.
///
/// A value in it that DynamoDB refuses (`{"NULL": false}`, a number it can't
/// read or store) is refused here, tagged for the request-validation envelope,
/// as DynamoDB refuses it (eu-west-2 and us-east-1, 2026-10-09). A map that
/// doesn't read as conditions is refused too, classified as a request body
/// would be, and never dropped: dropping it ran the write with no condition
/// at all. An empty attribute value gets DynamoDB's bare "Supplied
/// AttributeValue is empty" (eu-west-2 and us-east-1, 2026-10-10).
pub(crate) fn read_expected(
    expected: &serde_json::Value,
) -> Result<Option<HashMap<String, ExpectedCondition>>> {
    // A null given for a whole condition is refused in words of its own
    // (captured eu-west-2 and us-east-1, 2026-11-11).
    if let Some(name) = expected.as_object().and_then(|conditions| {
        conditions
            .iter()
            .find_map(|(name, condition)| condition.is_null().then_some(name))
    }) {
        return Err(DynoxideError::ValidationException(format!(
            "One or more parameter values were invalid: \
             ExpectedAttributeValue must not be null for Attribute: {name}"
        )));
    }
    match serde_json::from_value::<HashMap<String, ExpectedCondition>>(expected.clone()) {
        Ok(map) => {
            // Each value is checked as an item's would be: empty and duplicated
            // sets and numbers, in the envelope (eu-west-2, us-east-1,
            // eu-west-1 and us-west-2, 2026-10-09).
            let mut names: Vec<&String> = map.keys().collect();
            names.sort();
            for name in names {
                let condition = &map[name];
                let values = condition
                    .value
                    .iter()
                    .chain(condition.attribute_value_list.iter().flatten());
                for value in values {
                    crate::validation::validate_value_classified(
                        value,
                        crate::validation::ValueLayer::Current,
                    )
                    .map_err(ClassifiedValidationError::into_tagged)?;
                }
            }
            Ok(Some(map))
        }
        Err(e) => {
            let msg = e.to_string();
            let marked = msg.starts_with(crate::serde_errors::REQUEST_VALIDATION_MARKER)
                || msg.starts_with(crate::serde_errors::VALIDATION_MARKER);
            if marked {
                Err(DynoxideError::EnvelopedValidation(
                    crate::serde_errors::clean_serde_message(&msg).to_string(),
                ))
            } else {
                // Classified as the request body's own errors are, in every
                // build, so a library caller gets the same words as the wire.
                let refused =
                    crate::serde_errors::deserialize::<HashMap<String, ExpectedCondition>>(
                        &expected.to_string(),
                    )
                    .err()
                    .unwrap_or_else(|| {
                        DynoxideError::ValidationException(
                            crate::serde_errors::clean_serde_message(&msg).to_string(),
                        )
                    });
                Err(refused)
            }
        }
    }
}

/// Validate that AttributesToGet has no duplicate values.
pub fn validate_attributes_to_get_no_duplicates(attrs: &[String]) -> Result<()> {
    use std::collections::HashSet;
    let mut seen = HashSet::new();
    for attr in attrs {
        if !seen.insert(attr) {
            return Err(DynoxideError::ValidationException(format!(
                "One or more parameter values were invalid: \
                 Duplicate value in attribute name: {attr}"
            )));
        }
    }
    Ok(())
}

/// Parse a table name from a DynamoDB ARN.
///
/// Expects format: `arn:aws:dynamodb:<region>:<account>:table/<TableName>`
pub fn parse_table_name_from_arn(arn: &str) -> Result<&str> {
    // Empty ARN = missing parameter
    if arn.is_empty() {
        return Err(DynoxideError::ValidationException(
            "Invalid TableArn".to_string(),
        ));
    }

    let remainder = match arn.strip_prefix("arn:aws:dynamodb:") {
        Some(r) => r,
        None => {
            return Err(DynoxideError::ValidationException(format!(
                "Invalid TableArn: Invalid ResourceArn provided as input {}",
                arn
            )));
        }
    };

    // Validate region:account:table/ structure
    let parts: Vec<&str> = remainder.splitn(3, ':').collect();
    if parts.len() < 3 {
        return Err(DynoxideError::ValidationException(format!(
            "Invalid TableArn: Invalid ResourceArn provided as input {}",
            arn
        )));
    }

    let resource = parts[2];
    let table_name = match resource
        .strip_prefix("table/")
        .and_then(|s| s.split('/').next())
    {
        Some(name) if !name.is_empty() => name,
        _ => {
            return Err(DynoxideError::ValidationException(format!(
                "Invalid TableArn: Invalid ResourceArn provided as input {}",
                arn
            )));
        }
    };

    // Validate table name (3-255 chars, alphanumeric + _.-)
    if table_name.len() < 3
        || table_name.len() > 255
        || !table_name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
    {
        return Err(DynoxideError::ValidationException(format!(
            "Invalid TableArn: Invalid ResourceArn provided as input {}",
            arn
        )));
    }

    Ok(table_name)
}

/// Build `ItemCollectionMetrics` if requested and the table has LSIs.
///
/// Returns `None` if the request did not ask for SIZE, or if the table has no LSIs.
pub async fn build_item_collection_metrics<S: StorageBackend>(
    storage: &S,
    meta: &TableMetadata,
    table_name: &str,
    pk_str: &str,
    pk_attr: &str,
    pk_value: &AttributeValue,
    return_item_collection_metrics: &Option<String>,
) -> Result<Option<crate::types::ItemCollectionMetrics>> {
    let requested = matches!(return_item_collection_metrics.as_deref(), Some("SIZE"));
    if !requested || meta.lsi_definitions.is_none() {
        return Ok(None);
    }

    let mut partition_bytes = storage.get_partition_size(table_name, pk_str).await?;

    // Include LSI table sizes - DynamoDB's 10GB item collection limit applies
    // to the aggregate across base table and all LSIs.
    if let Some(ref lsi_json) = meta.lsi_definitions
        && let Ok(lsis) = serde_json::from_str::<Vec<crate::types::LocalSecondaryIndex>>(lsi_json)
    {
        for lsi in &lsis {
            let lsi_size = storage
                .get_lsi_partition_size(table_name, &lsi.index_name, pk_str)
                .await?;
            partition_bytes += lsi_size;
        }
    }

    let mut key_map = HashMap::new();
    key_map.insert(pk_attr.to_string(), pk_value.clone());

    Ok(Some(crate::types::ItemCollectionMetrics {
        item_collection_key: key_map,
        size_estimate_range_gb: size_estimate_range_gb(partition_bytes),
    }))
}

/// The `SizeEstimateRangeGB` DynamoDB reports for an item collection of
/// `bytes`: a lower and an upper bound in whole gigabytes, the size rounded
/// down and one more. A small collection reads `[0.0, 1.0]` (captured
/// eu-west-2 and us-east-1, 2026-10-09).
fn size_estimate_range_gb(bytes: i64) -> Vec<f64> {
    let lower = (bytes as f64 / (1024.0 * 1024.0 * 1024.0)).floor();
    vec![lower, lower + 1.0]
}

/// Convert legacy `AttributesToGet` list to a `ProjectionExpr`.
///
/// Each attribute name becomes a single-element path.
pub fn attributes_to_get_to_projection(
    attrs: &[String],
) -> crate::expressions::projection::ProjectionExpr {
    let paths = attrs
        .iter()
        .map(|name| vec![crate::expressions::PathElement::Attribute(name.clone())])
        .collect();
    crate::expressions::projection::ProjectionExpr { paths }
}

/// Convert legacy `Expected` parameter to a `ConditionExpression` string
/// with the `ExpressionAttributeNames` and `ExpressionAttributeValues` it
/// uses. Each attribute is named through a placeholder by its position,
/// `#expected_0`, never by its own name, which may hold a character the
/// expression reader stops at (`-`, `.`, anything outside ASCII).
pub fn convert_expected_to_condition(
    expected: &HashMap<String, ExpectedCondition>,
    conditional_operator: Option<&str>,
) -> Result<ConvertedCondition> {
    let joiner = match conditional_operator {
        Some(op) if op.eq_ignore_ascii_case("OR") => " OR ",
        _ => " AND ",
    };
    let mut parts = Vec::new();
    let mut values = HashMap::new();
    let mut names = HashMap::new();
    let mut val_idx = 0u32;

    // In name order, so the placeholders and the expression are the same
    // for the same request.
    let mut ordered: Vec<(&String, &ExpectedCondition)> = expected.iter().collect();
    ordered.sort_by(|a, b| a.0.cmp(b.0));
    for (index, (attr_name, cond)) in ordered.into_iter().enumerate() {
        let name_ref = format!("#expected_{index}");
        names.insert(name_ref.clone(), attr_name.clone());
        // Validate conflicting options
        if cond.exists.is_some() && cond.comparison_operator.is_some() {
            return Err(DynoxideError::ValidationException(format!(
                "One or more parameter values were invalid: Exists and ComparisonOperator cannot be used together for Attribute: {attr_name}"
            )));
        }

        if let Some(ref comp_op) = cond.comparison_operator {
            let comp_upper = comp_op.to_uppercase();

            // Helper: get the single comparison value from either AttributeValueList
            // (first element) or Value. DynamoDB allows both with ComparisonOperator.
            let single_val = || -> Option<crate::types::AttributeValue> {
                if let Some(ref avl) = cond.attribute_value_list
                    && avl.len() == 1
                {
                    return Some(avl[0].clone());
                }
                cond.value.clone()
            };

            match comp_upper.as_str() {
                "NULL" => {
                    parts.push(format!("attribute_not_exists({name_ref})"));
                }
                "NOT_NULL" => {
                    parts.push(format!("attribute_exists({name_ref})"));
                }
                "EQ" => {
                    if let Some(val) = single_val() {
                        let val_name = format!(":expected_v{val_idx}");
                        val_idx += 1;
                        parts.push(format!("{name_ref} = {val_name}"));
                        values.insert(val_name, val);
                    } else {
                        return Err(DynoxideError::ValidationException(format!(
                            "One or more parameter values were invalid: Value or AttributeValueList must be used with ComparisonOperator: EQ for Attribute: {attr_name}"
                        )));
                    }
                }
                "NE" => {
                    if let Some(val) = single_val() {
                        let val_name = format!(":expected_v{val_idx}");
                        val_idx += 1;
                        parts.push(format!("{name_ref} <> {val_name}"));
                        values.insert(val_name, val);
                    } else {
                        return Err(DynoxideError::ValidationException(format!(
                            "One or more parameter values were invalid: Value or AttributeValueList must be used with ComparisonOperator: NE for Attribute: {attr_name}"
                        )));
                    }
                }
                "LE" => {
                    if let Some(val) = single_val() {
                        let val_name = format!(":expected_v{val_idx}");
                        val_idx += 1;
                        parts.push(format!("{name_ref} <= {val_name}"));
                        values.insert(val_name, val);
                    } else {
                        return Err(DynoxideError::ValidationException(format!(
                            "One or more parameter values were invalid: Value or AttributeValueList must be used with ComparisonOperator: LE for Attribute: {attr_name}"
                        )));
                    }
                }
                "LT" => {
                    if let Some(val) = single_val() {
                        let val_name = format!(":expected_v{val_idx}");
                        val_idx += 1;
                        parts.push(format!("{name_ref} < {val_name}"));
                        values.insert(val_name, val);
                    } else {
                        return Err(DynoxideError::ValidationException(format!(
                            "One or more parameter values were invalid: Value or AttributeValueList must be used with ComparisonOperator: LT for Attribute: {attr_name}"
                        )));
                    }
                }
                "GE" => {
                    if let Some(val) = single_val() {
                        let val_name = format!(":expected_v{val_idx}");
                        val_idx += 1;
                        parts.push(format!("{name_ref} >= {val_name}"));
                        values.insert(val_name, val);
                    } else {
                        return Err(DynoxideError::ValidationException(format!(
                            "One or more parameter values were invalid: Value or AttributeValueList must be used with ComparisonOperator: GE for Attribute: {attr_name}"
                        )));
                    }
                }
                "GT" => {
                    if let Some(val) = single_val() {
                        let val_name = format!(":expected_v{val_idx}");
                        val_idx += 1;
                        parts.push(format!("{name_ref} > {val_name}"));
                        values.insert(val_name, val);
                    } else {
                        return Err(DynoxideError::ValidationException(format!(
                            "One or more parameter values were invalid: Value or AttributeValueList must be used with ComparisonOperator: GT for Attribute: {attr_name}"
                        )));
                    }
                }
                "BETWEEN" => {
                    let avl = cond.attribute_value_list.as_ref().filter(|l| l.len() == 2);
                    if let Some(list) = avl {
                        let v1 = format!(":expected_v{val_idx}");
                        val_idx += 1;
                        let v2 = format!(":expected_v{val_idx}");
                        val_idx += 1;
                        parts.push(format!("{name_ref} BETWEEN {v1} AND {v2}"));
                        values.insert(v1, list[0].clone());
                        values.insert(v2, list[1].clone());
                    } else {
                        return Err(DynoxideError::ValidationException(format!(
                            "One or more parameter values were invalid: Value or AttributeValueList must be used with ComparisonOperator: BETWEEN for Attribute: {attr_name}"
                        )));
                    }
                }
                "BEGINS_WITH" => {
                    if let Some(val) = single_val() {
                        let val_name = format!(":expected_v{val_idx}");
                        val_idx += 1;
                        parts.push(format!("begins_with({name_ref}, {val_name})"));
                        values.insert(val_name, val);
                    } else {
                        return Err(DynoxideError::ValidationException(format!(
                            "One or more parameter values were invalid: Value or AttributeValueList must be used with ComparisonOperator: BEGINS_WITH for Attribute: {attr_name}"
                        )));
                    }
                }
                "CONTAINS" => {
                    if let Some(val) = single_val() {
                        let val_name = format!(":expected_v{val_idx}");
                        val_idx += 1;
                        parts.push(format!("contains({name_ref}, {val_name})"));
                        values.insert(val_name, val);
                    } else {
                        return Err(DynoxideError::ValidationException(format!(
                            "One or more parameter values were invalid: Value or AttributeValueList must be used with ComparisonOperator: CONTAINS for Attribute: {attr_name}"
                        )));
                    }
                }
                "NOT_CONTAINS" => {
                    if let Some(val) = single_val() {
                        let val_name = format!(":expected_v{val_idx}");
                        val_idx += 1;
                        parts.push(format!("NOT contains({name_ref}, {val_name})"));
                        values.insert(val_name, val);
                    } else {
                        return Err(DynoxideError::ValidationException(format!(
                            "One or more parameter values were invalid: Value or AttributeValueList must be used with ComparisonOperator: NOT_CONTAINS for Attribute: {attr_name}"
                        )));
                    }
                }
                "IN" => {
                    let avl = cond.attribute_value_list.as_ref().filter(|l| !l.is_empty());
                    if let Some(list) = avl {
                        let val_names: Vec<String> = list
                            .iter()
                            .map(|v| {
                                let name = format!(":expected_v{val_idx}");
                                val_idx += 1;
                                values.insert(name.clone(), v.clone());
                                name
                            })
                            .collect();
                        parts.push(format!("{name_ref} IN ({})", val_names.join(", ")));
                    } else {
                        return Err(DynoxideError::ValidationException(format!(
                            "One or more parameter values were invalid: Value or AttributeValueList must be used with ComparisonOperator: IN for Attribute: {attr_name}"
                        )));
                    }
                }
                _ => {
                    return Err(DynoxideError::ValidationException(format!(
                        "One or more parameter values were invalid: Value or AttributeValueList must be used with ComparisonOperator: {comp_upper} for Attribute: {attr_name}"
                    )));
                }
            }
        } else if let Some(false) = cond.exists {
            // { Exists: false } => attribute_not_exists
            parts.push(format!("attribute_not_exists({name_ref})"));
        } else if let Some(ref value) = cond.value {
            // { Value: ... } (with Exists: true or Exists omitted) => attr = value
            let val_name = format!(":expected_v{val_idx}");
            val_idx += 1;
            parts.push(format!("{name_ref} = {val_name}"));
            values.insert(val_name, value.clone());
        } else {
            // { Exists: true } without Value => attribute_exists
            parts.push(format!("attribute_exists({name_ref})"));
        }
    }

    Ok(ConvertedCondition {
        expression: parts.join(joiner),
        attribute_values: values,
        attribute_names: names,
    })
}

/// Legacy `Expected` condition for a single attribute.
#[derive(Debug, Clone, Default, serde::Deserialize)]
pub struct ExpectedCondition {
    #[serde(rename = "Value", default)]
    pub value: Option<crate::types::AttributeValue>,
    #[serde(
        rename = "Exists",
        default,
        deserialize_with = "crate::types::deserialize_lenient_flag"
    )]
    pub exists: Option<bool>,
    #[serde(rename = "ComparisonOperator", default)]
    pub comparison_operator: Option<String>,
    #[serde(rename = "AttributeValueList", default)]
    pub attribute_value_list: Option<Vec<crate::types::AttributeValue>>,
}

/// Validate `Expected` conditions without converting them.
///
/// Checks for structural issues like conflicting parameters (ComparisonOperator + Exists,
/// AttributeValueList + Value, etc.) which DynamoDB validates BEFORE checking table existence.
pub fn validate_expected_conditions(expected: &HashMap<String, ExpectedCondition>) -> Result<()> {
    for (attr_name, cond) in expected {
        let has_value = cond.value.is_some();
        let has_exists = cond.exists.is_some();
        let has_comp_op = cond.comparison_operator.is_some();
        let has_avl = cond.attribute_value_list.is_some();

        // Exists and ComparisonOperator cannot be used together
        if has_exists && has_comp_op {
            return Err(DynoxideError::ValidationException(format!(
                "One or more parameter values were invalid: \
                 Exists and ComparisonOperator cannot be used together for Attribute: {attr_name}"
            )));
        }

        // Value and AttributeValueList cannot be used together
        // (checked before AVL-without-ComparisonOperator, matching DynamoDB ordering)
        if has_value && has_avl {
            return Err(DynoxideError::ValidationException(format!(
                "One or more parameter values were invalid: \
                 Value and AttributeValueList cannot be used together for Attribute: {attr_name}"
            )));
        }

        // AttributeValueList can only be used with ComparisonOperator
        if has_avl && !has_comp_op {
            return Err(DynoxideError::ValidationException(format!(
                "One or more parameter values were invalid: \
                 AttributeValueList can only be used with a ComparisonOperator for Attribute: {attr_name}"
            )));
        }

        // ComparisonOperator argument count validation
        if has_comp_op {
            let op = cond.comparison_operator.as_deref().unwrap_or("");
            let arg_count = if has_avl {
                cond.attribute_value_list.as_ref().map_or(0, |l| l.len())
            } else if has_value {
                1
            } else {
                0
            };

            match op {
                "NULL" | "NOT_NULL" => {
                    if arg_count > 0 {
                        return Err(DynoxideError::ValidationException(format!(
                            "One or more parameter values were invalid: \
                             Invalid number of argument(s) for the {op} ComparisonOperator"
                        )));
                    }
                }
                "EQ" | "NE" | "LE" | "LT" | "GE" | "GT" | "CONTAINS" | "NOT_CONTAINS"
                | "BEGINS_WITH" => {
                    if arg_count != 1 {
                        // An AttributeValueList that is there but empty is a
                        // wrong count, not a missing value (EQ captured
                        // eu-west-2 and us-east-1, 2026-10-10).
                        if arg_count == 0 && !has_avl {
                            return Err(DynoxideError::ValidationException(format!(
                                "One or more parameter values were invalid: \
                                 Value or AttributeValueList must be used with ComparisonOperator: {op} for Attribute: {attr_name}"
                            )));
                        }
                        return Err(DynoxideError::ValidationException(format!(
                            "One or more parameter values were invalid: \
                             Invalid number of argument(s) for the {op} ComparisonOperator"
                        )));
                    }
                }
                "BETWEEN" => {
                    if arg_count != 2 {
                        if arg_count == 0 && !has_value {
                            return Err(DynoxideError::ValidationException(format!(
                                "One or more parameter values were invalid: \
                                 Value or AttributeValueList must be used with ComparisonOperator: {op} for Attribute: {attr_name}"
                            )));
                        }
                        return Err(DynoxideError::ValidationException(format!(
                            "One or more parameter values were invalid: \
                             Invalid number of argument(s) for the {op} ComparisonOperator"
                        )));
                    }
                }
                "IN" => {
                    if arg_count == 0 && !has_value {
                        return Err(DynoxideError::ValidationException(format!(
                            "One or more parameter values were invalid: \
                             Value or AttributeValueList must be used with ComparisonOperator: {op} for Attribute: {attr_name}"
                        )));
                    }
                }
                _ => {
                    // An operator DynamoDB doesn't have is refused as such,
                    // ahead of its arguments (captured eu-west-2 and
                    // us-east-1, 2026-10-10).
                    return Err(DynoxideError::ValidationException(
                        INVALID_COMPARISON_OPERATOR.to_string(),
                    ));
                }
            }

            // IN takes only a string, number or binary value, and names the
            // first value in its list of any other type, ahead of the list's
            // types disagreeing (captured eu-west-2 and us-east-1, 2026-10-10).
            if op == "IN"
                && let Some(ref avl) = cond.attribute_value_list
            {
                for value in avl {
                    validate_comparison_type_compat(op, value.type_name())?;
                }
            }

            // AttributeValueList values must be of the same type
            if let Some(ref avl) = cond.attribute_value_list
                && avl.len() > 1
            {
                let first_type = std::mem::discriminant(&avl[0]);
                if avl
                    .iter()
                    .skip(1)
                    .any(|v| std::mem::discriminant(v) != first_type)
                {
                    return Err(DynoxideError::ValidationException(
                        "One or more parameter values were invalid: \
                             AttributeValues inside AttributeValueList must be of same type"
                            .to_string(),
                    ));
                }
            }

            // BETWEEN order validation
            if op == "BETWEEN"
                && arg_count == 2
                && let Some(ref avl) = cond.attribute_value_list
                && compare_attribute_values_for_between(&avl[0], &avl[1])
                    == std::cmp::Ordering::Greater
            {
                return Err(DynoxideError::ValidationException(
                            "The BETWEEN condition was provided a range where the lower bound is greater than the upper bound"
                                .to_string(),
                        ));
            }
        }

        // Exists=true requires Value
        if has_exists && cond.exists == Some(true) && !has_value {
            return Err(DynoxideError::ValidationException(format!(
                "One or more parameter values were invalid: \
                 Value must be provided when Exists is true for Attribute: {attr_name}"
            )));
        }

        // Exists=false cannot have Value
        if has_exists && cond.exists == Some(false) && has_value {
            return Err(DynoxideError::ValidationException(format!(
                "One or more parameter values were invalid: \
                 Value cannot be used when Exists is false for Attribute: {attr_name}"
            )));
        }

        // No value and no exists
        if !has_value && !has_exists && !has_comp_op && !has_avl {
            return Err(DynoxideError::ValidationException(format!(
                "One or more parameter values were invalid: \
                 Value must be provided when Exists is null for Attribute: {attr_name}"
            )));
        }
    }
    Ok(())
}

/// Compare two attribute values for BETWEEN order validation.
/// Returns Ordering::Greater if `lower` > `upper`.
fn compare_attribute_values_for_between(
    lower: &crate::types::AttributeValue,
    upper: &crate::types::AttributeValue,
) -> std::cmp::Ordering {
    use crate::types::AttributeValue;
    match (lower, upper) {
        (AttributeValue::S(a), AttributeValue::S(b)) => a.cmp(b),
        (AttributeValue::N(a), AttributeValue::N(b)) => {
            // Compare numbers as f64 for ordering
            let a_f = a.parse::<f64>().unwrap_or(0.0);
            let b_f = b.parse::<f64>().unwrap_or(0.0);
            a_f.partial_cmp(&b_f).unwrap_or(std::cmp::Ordering::Equal)
        }
        (AttributeValue::B(a), AttributeValue::B(b)) => a.cmp(b),
        _ => std::cmp::Ordering::Equal,
    }
}

/// Result of converting a legacy condition parameter to expression form.
pub struct ConvertedCondition {
    pub expression: String,
    pub attribute_values: HashMap<String, crate::types::AttributeValue>,
    pub attribute_names: HashMap<String, String>,
}

/// The expressions a read carries: Query, Scan, GetItem and BatchGetItem.
#[derive(Default, Clone, Copy)]
pub(crate) struct ReadExpressions<'a> {
    pub filter: Option<&'a str>,
    pub key_condition: Option<&'a str>,
    pub projection: Option<&'a str>,
}

/// The checks a read makes on its expressions and their attributes before the
/// table is looked up, in DynamoDB's order: a value the FilterExpression uses
/// but nobody defined, then one the KeyConditionExpression uses, then the
/// ProjectionExpression (its syntax, a name nobody defined in written order,
/// overlapping paths), and only then a name and then a value no expression
/// uses (captured eu-west-2 and us-east-1, 2026-11-11). The FilterExpression's
/// and KeyConditionExpression's syntax and names are checked by the caller
/// before this, so a fault there is not reported again. Returns the parsed
/// ProjectionExpression for the read to apply.
pub(crate) fn validate_read_expressions(
    names: &Option<HashMap<String, String>>,
    values: &Option<HashMap<String, AttributeValue>>,
    expressions: ReadExpressions<'_>,
) -> Result<Option<crate::expressions::projection::ProjectionExpr>> {
    use crate::expressions::{condition, key_condition, projection};
    let tracker = crate::expressions::TrackedExpressionAttributes::new(names, values);
    let undefined_value = |expression: &str| {
        tracker.first_undefined_value().map(|value| {
            DynoxideError::ValidationException(format!(
                "Invalid {expression}: An expression attribute value used in expression \
                 is not defined; attribute value: {value}"
            ))
        })
    };
    if let Some(Ok(filter)) = expressions
        .filter
        .filter(|fe| !fe.is_empty())
        .map(condition::parse)
    {
        tracker.track_condition_expr(&filter);
        if let Some(err) = undefined_value("FilterExpression") {
            return Err(err);
        }
    }
    if let Some(Ok(key_condition)) = expressions
        .key_condition
        .filter(|kce| !kce.is_empty())
        .map(|kce| key_condition::parse(kce, &tracker))
    {
        tracker.track_key_condition(&key_condition);
        if let Some(err) = undefined_value("KeyConditionExpression") {
            return Err(err);
        }
    }
    let projection = match expressions.projection.filter(|pe| !pe.is_empty()) {
        Some(expr) => {
            let parsed = projection::parse(expr).map_err(DynoxideError::ValidationException)?;
            tracker.track_projection_expr(&parsed);
            projection::validate(&parsed, &tracker).map_err(DynoxideError::ValidationException)?;
            Some(parsed)
        }
        None => None,
    };
    tracker.check_unused()?;
    Ok(projection)
}

/// The request's own `ExpressionAttributeNames` and `ExpressionAttributeValues`,
/// which a legacy condition's generated placeholders are merged into and so
/// must never share a key with.
#[derive(Clone, Copy)]
pub struct CallerPlaceholders<'a> {
    pub names: Option<&'a HashMap<String, String>>,
    pub values: Option<&'a HashMap<String, AttributeValue>>,
}

impl CallerPlaceholders<'_> {
    /// A prefix for generated placeholders, `#<prefix>_<n>` and
    /// `:<prefix>_v<n>`, that none of the caller's keys starts with, so no
    /// generated key can be one of them: `base`, or `base` followed by the
    /// first number that is free.
    fn free_prefix(&self, base: &str) -> String {
        let taken = |prefix: &str| {
            let name_start = format!("#{prefix}_");
            let value_start = format!(":{prefix}_");
            self.names
                .is_some_and(|names| names.keys().any(|k| k.starts_with(&name_start)))
                || self
                    .values
                    .is_some_and(|values| values.keys().any(|k| k.starts_with(&value_start)))
        };
        if !taken(base) {
            return base.to_string();
        }
        (1u32..)
            .map(|n| format!("{base}{n}"))
            .find(|prefix| !taken(prefix))
            .expect("a finite set of keys leaves some prefix free")
    }
}

/// Convert legacy `KeyConditions` to a KeyConditionExpression + ExpressionAttributeValues.
///
/// If `partition_key_name` is provided, the partition key condition is placed
/// first in the expression so that the key condition parser identifies it
/// correctly (important when both hash and range use EQ). The generated
/// placeholders never share a key with `caller`'s, which a FilterExpression or
/// ProjectionExpression beside the conditions may use.
pub fn convert_key_conditions(
    key_conditions: &HashMap<String, KeyCondition>,
    partition_key_name: Option<&str>,
    caller: CallerPlaceholders<'_>,
) -> Result<ConvertedCondition> {
    let prefix = caller.free_prefix("kc");
    let mut parts = Vec::new();
    let mut values = HashMap::new();
    let mut names = HashMap::new();
    let mut val_idx = 0u32;

    // Ensure deterministic ordering: partition key first, then remaining keys sorted.
    let mut ordered_keys: Vec<&String> = key_conditions.keys().collect();
    ordered_keys.sort();
    if let Some(pk) = partition_key_name
        && let Some(pos) = ordered_keys.iter().position(|k| k.as_str() == pk)
    {
        let removed = ordered_keys.remove(pos);
        ordered_keys.insert(0, removed);
    }

    // Each attribute is named through a placeholder by its position, never by
    // its own name, which may hold a character the expression reader stops at
    // (`-`, `.`, anything outside ASCII).
    for (index, attr_name) in ordered_keys.into_iter().enumerate() {
        let cond = &key_conditions[attr_name];
        let name_ref = format!("#{prefix}_{index}");
        names.insert(name_ref.clone(), attr_name.clone());

        // Operators are matched as written: DynamoDB refuses `eq`.
        let op = cond.comparison_operator.as_str();
        let count = match op {
            "EQ" | "LE" | "LT" | "GE" | "GT" | "BEGINS_WITH" => 1,
            "BETWEEN" => 2,
            "NE" | "IN" | "NULL" | "NOT_NULL" | "CONTAINS" | "NOT_CONTAINS" => {
                return Err(DynoxideError::ValidationException(format!(
                    "Unsupported KeyConditions ComparisonOperator: {op}"
                )));
            }
            _ => {
                return Err(DynoxideError::ValidationException(
                    INVALID_COMPARISON_OPERATOR.to_string(),
                ));
            }
        };
        let refs = legacy_value_refs(
            cond.attribute_value_list.as_deref(),
            op,
            count..=count,
            &prefix,
            &mut val_idx,
            &mut values,
        )?;
        parts.push(legacy_condition_text(op, &name_ref, &refs));
    }

    Ok(ConvertedCondition {
        expression: parts.join(" AND "),
        attribute_values: values,
        attribute_names: names,
    })
}

/// Legacy `KeyConditions` entry.
#[derive(Debug, Clone, Default, serde::Deserialize)]
pub struct KeyCondition {
    #[serde(rename = "ComparisonOperator", default)]
    pub comparison_operator: String,
    #[serde(rename = "AttributeValueList", default)]
    pub attribute_value_list: Option<Vec<crate::types::AttributeValue>>,
}

/// Legacy `QueryFilter` / `ScanFilter` condition entry.
#[derive(Debug, Clone, Default, serde::Deserialize)]
pub struct FilterCondition {
    #[serde(rename = "ComparisonOperator", default)]
    pub comparison_operator: String,
    #[serde(rename = "AttributeValueList", default)]
    pub attribute_value_list: Option<Vec<crate::types::AttributeValue>>,
}

/// Convert legacy `QueryFilter` / `ScanFilter` to a FilterExpression + values + names.
/// The generated placeholders never share a key with `caller`'s.
pub fn convert_filter_conditions(
    conditions: &HashMap<String, FilterCondition>,
    conditional_operator: Option<&str>,
    caller: CallerPlaceholders<'_>,
) -> Result<ConvertedCondition> {
    let prefix = caller.free_prefix("qf");
    let joiner = match conditional_operator {
        Some(op) if op.eq_ignore_ascii_case("OR") => " OR ",
        _ => " AND ",
    };
    let mut parts = Vec::new();
    let mut values = HashMap::new();
    let mut names = HashMap::new();
    let mut val_idx = 0u32;

    // In name order, each attribute named through a placeholder by its
    // position, never by its own name, which may hold a character the
    // expression reader stops at (`-`, `.`, anything outside ASCII).
    let mut ordered: Vec<(&String, &FilterCondition)> = conditions.iter().collect();
    ordered.sort_by(|a, b| a.0.cmp(b.0));
    for (index, (attr_name, cond)) in ordered.into_iter().enumerate() {
        let name_ref = format!("#{prefix}_{index}");
        names.insert(name_ref.clone(), attr_name.clone());

        // Operators are matched as written: DynamoDB refuses `eq`. An unknown
        // one is refused rather than skipped, which ran the read without it.
        let op = cond.comparison_operator.as_str();
        let counts = match op {
            "NULL" | "NOT_NULL" => 0..=0,
            "EQ" | "NE" | "LE" | "LT" | "GE" | "GT" | "BEGINS_WITH" | "CONTAINS"
            | "NOT_CONTAINS" => 1..=1,
            "BETWEEN" => 2..=2,
            "IN" => 1..=usize::MAX,
            _ => {
                return Err(DynoxideError::ValidationException(
                    INVALID_COMPARISON_OPERATOR.to_string(),
                ));
            }
        };
        let refs = legacy_value_refs(
            cond.attribute_value_list.as_deref(),
            op,
            counts,
            &prefix,
            &mut val_idx,
            &mut values,
        )?;
        parts.push(legacy_condition_text(op, &name_ref, &refs));
    }

    Ok(ConvertedCondition {
        expression: parts.join(joiner),
        attribute_values: values,
        attribute_names: names,
    })
}

/// Name each value of a legacy condition as an expression value,
/// `:<prefix>_v<n>`, adding it to `values`. A list the operator can't take is
/// refused with the argument count message, never dropped; the request's
/// validation has already refused it, so this only guards against a caller
/// that skipped it.
fn legacy_value_refs(
    list: Option<&[crate::types::AttributeValue]>,
    op: &str,
    counts: std::ops::RangeInclusive<usize>,
    prefix: &str,
    val_idx: &mut u32,
    values: &mut HashMap<String, crate::types::AttributeValue>,
) -> Result<Vec<String>> {
    let list = list.unwrap_or_default();
    if !counts.contains(&list.len()) {
        return Err(DynoxideError::ValidationException(format!(
            "One or more parameter values were invalid: Invalid number of argument(s) for the {op} ComparisonOperator"
        )));
    }
    Ok(list
        .iter()
        .map(|value| {
            let name = format!(":{prefix}_v{val_idx}");
            *val_idx += 1;
            values.insert(name.clone(), value.clone());
            name
        })
        .collect())
}

/// A legacy condition as expression text over `name` and its value names,
/// for an operator already checked to be one DynamoDB has.
fn legacy_condition_text(op: &str, name: &str, refs: &[String]) -> String {
    let comparison = |symbol: &str| format!("{name} {symbol} {}", refs[0]);
    match op {
        "NULL" => format!("attribute_not_exists({name})"),
        "NOT_NULL" => format!("attribute_exists({name})"),
        "EQ" => comparison("="),
        "NE" => comparison("<>"),
        "LE" => comparison("<="),
        "LT" => comparison("<"),
        "GE" => comparison(">="),
        "GT" => comparison(">"),
        "BETWEEN" => format!("{name} BETWEEN {} AND {}", refs[0], refs[1]),
        "BEGINS_WITH" => format!("begins_with({name}, {})", refs[0]),
        "CONTAINS" => format!("contains({name}, {})", refs[0]),
        "NOT_CONTAINS" => format!("NOT contains({name}, {})", refs[0]),
        // IN, the only operator left.
        _ => format!("{name} IN ({})", refs.join(", ")),
    }
}

/// Extract pk and sk strings from an item's key attributes.
pub fn extract_key_strings(
    item: &HashMap<String, AttributeValue>,
    schema: &KeySchema,
) -> Result<(String, String)> {
    let pk_val = item
        .get(&schema.partition_key)
        .ok_or_else(|| DynoxideError::InternalServerError("Missing partition key".to_string()))?;

    let pk = pk_val.to_key_string().ok_or_else(|| {
        DynoxideError::InternalServerError("Cannot convert partition key to string".to_string())
    })?;

    let sk = if let Some(ref sk_name) = schema.sort_key {
        let sk_val = item
            .get(sk_name)
            .ok_or_else(|| DynoxideError::InternalServerError("Missing sort key".to_string()))?;
        sk_val.to_key_string().ok_or_else(|| {
            DynoxideError::InternalServerError("Cannot convert sort key to string".to_string())
        })?
    } else {
        String::new()
    };

    Ok((pk, sk))
}

/// Read a Query's or Scan's `ExpressionAttributeValues` with `read`, after
/// its starting key. DynamoDB checks `ExclusiveStartKey` ahead of the
/// expression values, whichever is written first (captured eu-west-2 and
/// us-east-1, 2026-11-11, on Query; Scan is taken to read them the same way),
/// so when a value is refused, the starting key's own fault, read as
/// `read_start_key` reads it, is answered instead. Otherwise the starting key
/// is read later, after the request's other checks. The error is a raw serde
/// message, marker and all.
pub(crate) fn read_values_after_start_key(
    values: Option<crate::types::HeldValues>,
    read: impl FnOnce(
        Vec<(String, serde_json::Value)>,
    ) -> std::result::Result<HashMap<String, AttributeValue>, String>,
    start_key: Option<&serde_json::Value>,
    read_start_key: impl FnOnce(&serde_json::Value) -> Result<HashMap<String, AttributeValue>>,
) -> std::result::Result<Option<HashMap<String, AttributeValue>>, String> {
    let Some(values) = values else {
        return Ok(None);
    };
    match values.read(read) {
        Ok(values) => Ok(Some(values)),
        Err(refusal) => match start_key.map(read_start_key) {
            Some(Err(error)) => Err(crate::serde_errors::marked_message(&error).unwrap_or(refusal)),
            _ => Err(refusal),
        },
    }
}

/// Whether a Query's or Scan's `ExclusiveStartKey` holds a JSON null where a
/// value belongs, which hands the request to the older validation layer:
/// beside a `Limit` of 0 it answers `Value '0' at 'limit'`, where a starting
/// key without one, or with a bad number, answers `Value at 'Limit'`
/// (captured eu-west-2 and us-east-1, 2026-11-11). A null type member, as in
/// `{"S": null}`, is such a null too, as it is wherever the request's layer
/// is decided (captured eu-west-2 and us-east-1, 2026-11-12).
pub(crate) fn start_key_holds_null(start_key: Option<&serde_json::Value>) -> bool {
    start_key.is_some_and(crate::types::attribute_map_holds_null)
}

/// Parse an `ExclusiveStartKey` from a raw `serde_json::Value`.
///
/// Validation errors are formatted to match DynamoDB's exact error messages:
/// - Empty/unsupported AV and multi-type: prefixed with "The provided starting key is invalid: "
/// - NULL:false, empty SS, empty BS: prefixed with "The provided starting key is invalid: One or more..."
/// - Empty NS, duplicate SS, duplicate BS: "One or more parameter values were invalid: ..." (no ESK prefix)
/// - Number errors: raw error (no prefix)
pub fn parse_exclusive_start_key(
    value: &serde_json::Value,
    layer: crate::validation::ValueLayer,
) -> Result<HashMap<String, AttributeValue>> {
    // A null among the key's values puts the request on the older layer, so
    // the key's other values are read by that layer's rules, as an item's
    // are beside a null.
    let layer = if start_key_holds_null(Some(value)) {
        crate::validation::ValueLayer::Older
    } else {
        layer
    };
    // First, try to deserialise: this catches multi-type and number errors
    let parsed = match crate::types::with_value_layer(layer, || {
        serde_json::from_value::<crate::types::AttributeMap>(value.clone())
    }) {
        Ok(map) => map.0,
        Err(e) => {
            let msg = e.to_string();
            // A malformed value fails the request as it is read, as it does
            // in an expression value (not captured for a starting key).
            if let Some(error) = crate::serde_errors::marked_serialization_error(&msg) {
                return Err(error);
            }
            // Strip the serde position suffix and any internal marker
            let inner = crate::serde_errors::clean_serde_message(&msg);

            if inner.starts_with(crate::serde_errors::NULL_VALUE) {
                // A null member is named, inside the starting-key prefix
                // (captured eu-west-2 and us-east-1, 2026-11-11).
                return Err(DynoxideError::ValidationException(format!(
                    "The provided starting key is invalid: {inner}"
                )));
            } else if inner.contains("empty AttributeValue")
                || (inner.contains("Supplied AttributeValue") && inner.contains("empty"))
            {
                return Err(DynoxideError::ValidationException(
                    "The provided starting key is invalid: \
                     Supplied AttributeValue is empty, must contain exactly one of the supported datatypes"
                        .to_string(),
                ));
            } else if inner.contains("more than one datatypes") {
                return Err(DynoxideError::ValidationException(
                    "The provided starting key is invalid: \
                     Supplied AttributeValue has more than one datatypes set, \
                     must contain exactly one of the supported datatypes"
                        .to_string(),
                ));
            } else if inner.contains("cannot be converted to a numeric value")
                || inner.contains("significant digits")
                || inner.contains("Number overflow")
                || inner.contains("Number underflow")
            {
                // Number errors: raw message, no prefix
                return Err(DynoxideError::ValidationException(inner.to_string()));
            } else {
                return Err(DynoxideError::ValidationException(format!(
                    "The provided starting key is invalid: {inner}"
                )));
            }
        }
    };

    // Post-parse validation for invalid values
    for av in parsed.values() {
        match av {
            // These get "The provided starting key is invalid: One or more..." prefix
            AttributeValue::NULL(b) if !b => {
                return Err(DynoxideError::ValidationException(format!(
                    "The provided starting key is invalid: {}",
                    crate::serde_errors::NULL_NOT_TRUE
                )));
            }
            AttributeValue::SS(set) if set.is_empty() => {
                return Err(DynoxideError::ValidationException(
                    "The provided starting key is invalid: \
                     One or more parameter values were invalid: \
                     An string set  may not be empty"
                        .to_string(),
                ));
            }
            AttributeValue::BS(set) if set.is_empty() => {
                return Err(DynoxideError::ValidationException(
                    "The provided starting key is invalid: \
                     One or more parameter values were invalid: \
                     Binary sets should not be empty"
                        .to_string(),
                ));
            }
            // These do NOT get the "provided starting key" prefix
            AttributeValue::NS(set) if set.is_empty() => {
                return Err(DynoxideError::ValidationException(
                    "One or more parameter values were invalid: \
                     An number set  may not be empty"
                        .to_string(),
                ));
            }
            AttributeValue::SS(_) | AttributeValue::BS(_) => {
                if let Some(message) = crate::validation::duplicate_set_message(av, layer) {
                    return Err(DynoxideError::ValidationException(message));
                }
            }
            AttributeValue::N(n) => {
                layer.check_number(n)?;
            }
            AttributeValue::NS(set) => {
                for n in set {
                    layer.check_number(n)?;
                }
                if let Some(message) = crate::validation::duplicate_set_message(av, layer) {
                    return Err(DynoxideError::ValidationException(message));
                }
            }
            _ => {}
        }
    }

    Ok(parsed)
}

/// Validate ESK count and index key types (stages 1 and 2 of ESK validation).
///
/// Stage 1: Count check - error uses `count_mismatch_msg`.
/// Stage 2: Index key type check (if index) - missing attr returns
/// "The provided starting key is invalid", wrong type returns
/// "The provided key element does not match the schema".
///
/// Call `validate_esk_table_keys` separately afterward.
pub fn validate_esk_count_and_index_keys(
    esk: &HashMap<String, AttributeValue>,
    meta: &TableMetadata,
    index_name: Option<&str>,
    count_mismatch_msg: &str,
    name_index_on_type_mismatch: bool,
) -> Result<()> {
    let table_key_schema: Vec<KeySchemaElement> = serde_json::from_str(&meta.key_schema)
        .map_err(|e| DynoxideError::InternalServerError(format!("Bad key schema JSON: {e}")))?;
    let attr_defs: Vec<AttributeDefinition> = serde_json::from_str(&meta.attribute_definitions)
        .map_err(|e| {
            DynoxideError::InternalServerError(format!("Bad attribute definitions JSON: {e}"))
        })?;

    // Build expected key name list: table keys + index keys (deduplicated)
    let mut expected_names: Vec<String> = table_key_schema
        .iter()
        .map(|k| k.attribute_name.clone())
        .collect();

    let index_key_schema = if let Some(idx) = index_name {
        get_index_key_schema(meta, idx)?
    } else {
        Vec::new()
    };

    for k in &index_key_schema {
        if !expected_names.contains(&k.attribute_name) {
            expected_names.push(k.attribute_name.clone());
        }
    }

    // A Scan on the newer validation layer checks every starting key attribute
    // against the attribute definitions ahead of the count, and on a table
    // with indexes reports a mismatch as an index key's, naming the index
    // asked for or else the first the table has (captured eu-west-2 and
    // eu-central-1, 20 October 2026). A table with no index answers in the
    // short form from the table key check.
    if name_index_on_type_mismatch {
        let index = match index_name {
            Some(index) => Some(index.to_string()),
            None => super::gsi::parse_gsi_defs(meta)?
                .into_iter()
                .chain(super::lsi::parse_lsi_defs(meta)?)
                .next()
                .map(|def| def.index_name),
        };
        if let Some(index) = index {
            for def in &attr_defs {
                if let Some(val) = esk.get(&def.attribute_name)
                    && !attr_value_matches_scalar_type(val, &def.attribute_type)
                {
                    return Err(DynoxideError::ValidationException(format!(
                        "One or more parameter values were invalid: Type mismatch for Index \
                         Key {} Expected: {:?} Actual: {} IndexName: {index}",
                        def.attribute_name,
                        def.attribute_type,
                        val.type_name()
                    )));
                }
            }
        }
    }

    // Stage 1: count check
    if esk.len() != expected_names.len() {
        return Err(DynoxideError::ValidationException(
            count_mismatch_msg.to_string(),
        ));
    }

    // Stage 2: index key check (only for index queries/scans)
    if index_name.is_some() {
        for key_elem in &index_key_schema {
            let attr = &key_elem.attribute_name;
            let val = match esk.get(attr) {
                Some(v) => v,
                None => {
                    return Err(DynoxideError::ValidationException(
                        "The provided starting key is invalid".to_string(),
                    ));
                }
            };
            if let Some(def) = attr_defs.iter().find(|d| d.attribute_name == *attr)
                && !attr_value_matches_scalar_type(val, &def.attribute_type)
            {
                return Err(DynoxideError::ValidationException(
                    "The provided key element does not match the schema".to_string(),
                ));
            }
        }
    }

    Ok(())
}

/// Validate ESK table key types (stage 3 of ESK validation).
///
/// Extracts the table key attributes from the ESK and validates their count
/// and types. Errors are prefixed with "The provided starting key is invalid: ".
pub fn validate_esk_table_keys(
    esk: &HashMap<String, AttributeValue>,
    meta: &TableMetadata,
) -> Result<()> {
    let table_key_schema: Vec<KeySchemaElement> = serde_json::from_str(&meta.key_schema)
        .map_err(|e| DynoxideError::InternalServerError(format!("Bad key schema JSON: {e}")))?;
    let attr_defs: Vec<AttributeDefinition> = serde_json::from_str(&meta.attribute_definitions)
        .map_err(|e| {
            DynoxideError::InternalServerError(format!("Bad attribute definitions JSON: {e}"))
        })?;

    let table_key_count = table_key_schema.len();
    let mut table_key_subset: HashMap<String, &AttributeValue> = HashMap::new();
    for key_elem in &table_key_schema {
        if let Some(v) = esk.get(&key_elem.attribute_name) {
            table_key_subset.insert(key_elem.attribute_name.clone(), v);
        }
    }

    // Check count of table key subset
    if table_key_subset.len() != table_key_count {
        return Err(DynoxideError::ValidationException(
            "The provided starting key is invalid: \
             The provided key element does not match the schema"
                .to_string(),
        ));
    }

    // Check types of table key attributes
    for key_elem in &table_key_schema {
        if let Some(val) = table_key_subset.get(&key_elem.attribute_name)
            && let Some(def) = attr_defs
                .iter()
                .find(|d| d.attribute_name == key_elem.attribute_name)
            && !attr_value_matches_scalar_type(val, &def.attribute_type)
        {
            return Err(DynoxideError::ValidationException(
                "The provided starting key is invalid: \
                         The provided key element does not match the schema"
                    .to_string(),
            ));
        }
    }

    Ok(())
}

/// Get the key schema elements for a GSI or LSI by index name.
fn get_index_key_schema(meta: &TableMetadata, index_name: &str) -> Result<Vec<KeySchemaElement>> {
    // Check LSIs first
    if let Some(ref lsi_json) = meta.lsi_definitions
        && let Ok(lsis) = serde_json::from_str::<Vec<crate::types::LocalSecondaryIndex>>(lsi_json)
    {
        for lsi in &lsis {
            if lsi.index_name == index_name {
                return Ok(lsi.key_schema.clone());
            }
        }
    }
    // Check GSIs
    if let Some(ref gsi_json) = meta.gsi_definitions
        && let Ok(gsis) = serde_json::from_str::<Vec<crate::types::GlobalSecondaryIndex>>(gsi_json)
    {
        for gsi in &gsis {
            if gsi.index_name == index_name {
                return Ok(gsi.key_schema.clone());
            }
        }
    }
    // Index not found: the caller should already have validated this
    Ok(Vec::new())
}

/// Check whether an `AttributeValue` matches a `ScalarAttributeType`.
pub(crate) fn attr_value_matches_scalar_type(
    val: &AttributeValue,
    expected: &ScalarAttributeType,
) -> bool {
    matches!(
        (val, expected),
        (AttributeValue::S(_), ScalarAttributeType::S)
            | (AttributeValue::N(_), ScalarAttributeType::N)
            | (AttributeValue::B(_), ScalarAttributeType::B)
    )
}

/// DynamoDB's refusal of a legacy condition's `ComparisonOperator` that isn't
/// one of its own. Operators are case-sensitive, so `eq` is refused too.
/// Captured on ScanFilter and QueryFilter in eu-west-2, ap-southeast-2 and
/// eu-central-1 (2026-10-10), and on Expected in eu-west-2 and us-east-1.
/// us-east-1 answers a ScanFilter's with the member's enum constraint instead,
/// `1 validation error detected: Value 'FOO' at
/// 'scanFilter.a.member.comparisonOperator' failed to satisfy constraint:
/// Member must satisfy enum value set: [IN, NULL, BETWEEN, ...]`; the majority
/// is followed.
pub(crate) const INVALID_COMPARISON_OPERATOR: &str =
    "One or more parameter values were invalid: Invalid comparison operator";

/// The member error for every condition in a legacy `QueryFilter`,
/// `ScanFilter` or `KeyConditions` map with no `ComparisonOperator`, or a
/// JSON null one, under `member` (`scanFilter`, `queryFilter`,
/// `keyConditions`): `Value null at 'scanFilter.a.member.comparisonOperator'
/// failed to satisfy constraint: Member must not be null` (ScanFilter captured
/// in four regions, 2026-10-10; the other two inferred). An operator of
/// another JSON type is left to the request's reader, which refuses it.
pub(crate) fn missing_comparison_operators(
    conditions: Option<&serde_json::Value>,
    member: &str,
) -> Vec<String> {
    let Some(conditions) = conditions.and_then(|v| v.as_object()) else {
        return Vec::new();
    };
    conditions
        .iter()
        .filter(|(_, condition)| {
            condition.as_object().is_some_and(|condition| {
                condition
                    .get("ComparisonOperator")
                    .is_none_or(serde_json::Value::is_null)
            })
        })
        .map(|(name, _)| {
            format!(
                "Value null at '{member}.{name}.member.comparisonOperator' failed to satisfy \
                 constraint: Member must not be null"
            )
        })
        .collect()
}

/// Validate argument counts and type compatibility for each `ComparisonOperator`
/// in a raw `QueryFilter`, `ScanFilter`, or `KeyConditions` JSON map, named by
/// `member` as its errors name it (`scanFilter`, `queryFilter`,
/// `keyConditions`).
///
/// A missing operator is a member error the request's reader reports with the
/// request's others; it is refused here too for a request built in code. An
/// operator DynamoDB doesn't have is refused, never skipped: skipping it ran
/// the read without that condition.
///
/// DynamoDB validates these BEFORE ExclusiveStartKey and expression syntax.
pub fn validate_filter_condition_args(
    value: Option<&serde_json::Value>,
    member: &str,
) -> Result<()> {
    if let Some(message) =
        crate::validation::format_validation_errors(&missing_comparison_operators(value, member))
    {
        return Err(DynoxideError::ValidationException(message));
    }
    let val = match value {
        Some(v) if v.is_object() => v,
        _ => return Ok(()),
    };

    let obj = val.as_object().unwrap();
    for (_attr_name, cond_val) in obj {
        let cond_obj = match cond_val.as_object() {
            Some(o) => o,
            None => continue,
        };

        // Not a string: the request's reader refuses it.
        let comp_op = match cond_obj.get("ComparisonOperator").and_then(|v| v.as_str()) {
            Some(op) => op,
            None => continue,
        };

        let avl = cond_obj
            .get("AttributeValueList")
            .and_then(|v| v.as_array());
        let avl_len = avl.map_or(0, |a| a.len());

        // Check argument count
        let (min, max) = match comp_op {
            "NULL" | "NOT_NULL" => (0, 0),
            "EQ" | "NE" | "LE" | "LT" | "GE" | "GT" | "CONTAINS" | "NOT_CONTAINS"
            | "BEGINS_WITH" => (1, 1),
            "BETWEEN" => (2, 2),
            "IN" => (1, usize::MAX),
            _ => {
                return Err(DynoxideError::ValidationException(
                    INVALID_COMPARISON_OPERATOR.to_string(),
                ));
            }
        };

        if avl_len < min || avl_len > max {
            return Err(DynoxideError::ValidationException(format!(
                "One or more parameter values were invalid: Invalid number of argument(s) for the {} ComparisonOperator",
                comp_op
            )));
        }

        // Type compatibility validation. IN checks every value in its list,
        // naming the first whose type it can't take (captured eu-west-2 and
        // us-east-1, 2026-10-10); the other operators check their first.
        if let Some(arr) = avl
            && !arr.is_empty()
        {
            let checked = if comp_op == "IN" { &arr[..] } else { &arr[..1] };
            for value in checked {
                if let Some(type_name) = attr_value_type_name_from_json(value) {
                    validate_comparison_type_compat(comp_op, type_name)?;
                }
            }
        }
    }

    Ok(())
}

/// Extract the DynamoDB type name (S, N, B, BOOL, NULL, SS, NS, BS, M, L)
/// from a raw JSON AttributeValue object. A member set to JSON null counts as
/// absent, as it does when the value is read.
fn attr_value_type_name_from_json(val: &serde_json::Value) -> Option<&str> {
    val.as_object().and_then(|obj| {
        ["S", "N", "B", "BOOL", "NULL", "SS", "NS", "BS", "M", "L"]
            .into_iter()
            .find(|key| obj.get(*key).is_some_and(|member| !member.is_null()))
    })
}

/// Validate that a ComparisonOperator is compatible with the given AttributeValue type.
fn validate_comparison_type_compat(comp_op: &str, type_name: &str) -> Result<()> {
    let valid = match comp_op {
        "LT" | "LE" | "GT" | "GE" | "IN" => matches!(type_name, "S" | "N" | "B"),
        "BETWEEN" => matches!(type_name, "S" | "N" | "B"),
        "BEGINS_WITH" => matches!(type_name, "S" | "B"),
        "CONTAINS" | "NOT_CONTAINS" => matches!(type_name, "S" | "N" | "B" | "BOOL" | "NULL"),
        // EQ, NE, NULL, NOT_NULL accept all types
        _ => true,
    };

    if !valid {
        return Err(DynoxideError::ValidationException(format!(
            "One or more parameter values were invalid: ComparisonOperator {} is not valid for {} AttributeValue type",
            comp_op, type_name
        )));
    }

    Ok(())
}

/// Validate the attribute values inside a raw `QueryFilter`, `ScanFilter`, or
/// `KeyConditions` JSON value *before* converting to expressions.
///
/// This catches empty AttributeValues, invalid numbers, duplicate sets, etc.
/// inside the filter condition's `AttributeValueList`, matching DynamoDB's
/// validation order (filters are validated before ExclusiveStartKey).
pub fn validate_filter_conditions_raw(
    value: Option<&serde_json::Value>,
    _param_name: &str,

    layer: crate::validation::ValueLayer,
) -> Result<()> {
    let val = match value {
        Some(v) if v.is_object() => v,
        _ => return Ok(()),
    };

    let obj = val.as_object().unwrap();
    // A KeyConditions, QueryFilter or ScanFilter entry given as null is
    // refused before the table is looked up (captured eu-west-2 and
    // us-east-1, 2026-11-11).
    if let Some(name) = obj
        .iter()
        .find_map(|(name, condition)| condition.is_null().then_some(name))
    {
        return Err(DynoxideError::ValidationException(format!(
            "The filter condition for an attribute cannot be null: {name}"
        )));
    }
    for (_attr_name, cond_val) in obj {
        let cond_obj = match cond_val.as_object() {
            Some(o) => o,
            None => continue,
        };

        // Validate AttributeValueList entries
        if let Some(avl) = cond_obj.get("AttributeValueList")
            && let Some(arr) = avl.as_array()
        {
            for av_val in arr {
                // Try to deserialize each AttributeValue to trigger validation
                match crate::types::with_value_layer(layer, || {
                    serde_json::from_value::<AttributeValue>(av_val.clone())
                }) {
                    // A value that doesn't read is refused, never skipped:
                    // skipping it dropped the condition and ran the read
                    // without it.
                    Err(e) => return Err(legacy_value_refusal(&e.to_string(), av_val, layer)),
                    Ok(av) => {
                        // Validate parsed value (NULL:false, empty sets, duplicates, numbers)
                        validate_filter_attribute_value(&av, layer)?;
                    }
                }
            }
        }
    }

    Ok(())
}

/// The error a value in a legacy condition that doesn't read is refused with,
/// bare, as DynamoDB refuses it in a QueryFilter, ScanFilter or KeyConditions:
/// a BOOL or NULL that doesn't read as a boolean fails the whole request as
/// malformed, and a NULL that reads as false is refused ahead of the
/// condition's operator checks (captured eu-west-2 and us-east-1, 2026-10-10).
/// Anything else is classified as the request body's own values are.
fn legacy_value_refusal(
    msg: &str,
    value: &serde_json::Value,
    layer: crate::validation::ValueLayer,
) -> DynoxideError {
    use crate::serde_errors::{EMPTY_VALUE, MORE_THAN_ONE_TYPE, ValueRefusal, value_refusal};
    if let Some(error) = crate::serde_errors::marked_serialization_error(msg) {
        return error;
    }
    let refusal = |message: &str| DynoxideError::ValidationException(message.to_string());
    match value_refusal(msg) {
        ValueRefusal::Empty => refusal(EMPTY_VALUE),
        ValueRefusal::MoreThanOneType => refusal(MORE_THAN_ONE_TYPE),
        ValueRefusal::Null(inner)
        | ValueRefusal::EmptySet(inner)
        | ValueRefusal::Number(inner)
        | ValueRefusal::ParsedNull(inner)
        | ValueRefusal::RequestNull(inner) => refusal(inner),
        ValueRefusal::Other => match crate::types::with_value_layer(layer, || {
            crate::serde_errors::deserialize::<AttributeValue>(&value.to_string())
        }) {
            Err(DynoxideError::EnvelopedValidation(message)) => {
                DynoxideError::ValidationException(message)
            }
            Err(error) => error,
            Ok(_) => refusal(crate::serde_errors::clean_serde_message(msg)),
        },
    }
}

/// Read a legacy condition map (`QueryFilter`, `ScanFilter`, `KeyConditions`)
/// into its conditions, refusing one that doesn't read in the words the
/// request body's own errors take. It is never dropped: dropping it ran the
/// read with no condition at all.
pub(crate) fn read_legacy_conditions<T: serde::de::DeserializeOwned>(
    conditions: &serde_json::Value,
    layer: crate::validation::ValueLayer,
) -> Result<T> {
    crate::types::with_value_layer(layer, || {
        crate::serde_errors::deserialize::<T>(&conditions.to_string())
    })
    .map_err(|e| match e {
        DynoxideError::EnvelopedValidation(message) => DynoxideError::ValidationException(message),
        other => other,
    })
}

/// Validate a single attribute value inside a filter condition (QueryFilter,
/// ScanFilter, KeyConditions). Error messages use the
/// "One or more parameter values were invalid: " prefix.
fn validate_filter_attribute_value(
    value: &AttributeValue,
    layer: crate::validation::ValueLayer,
) -> Result<()> {
    match value {
        AttributeValue::NULL(b) if !b => Err(DynoxideError::ValidationException(
            crate::serde_errors::NULL_NOT_TRUE.to_string(),
        )),
        AttributeValue::SS(set) if set.is_empty() => Err(DynoxideError::ValidationException(
            "One or more parameter values were invalid: An string set  may not be empty"
                .to_string(),
        )),
        AttributeValue::NS(set) if set.is_empty() => Err(DynoxideError::ValidationException(
            "One or more parameter values were invalid: An number set  may not be empty"
                .to_string(),
        )),
        AttributeValue::BS(set) if set.is_empty() => Err(DynoxideError::ValidationException(
            "One or more parameter values were invalid: Binary sets should not be empty"
                .to_string(),
        )),
        AttributeValue::SS(_) | AttributeValue::BS(_) => {
            match crate::validation::duplicate_set_message(value, layer) {
                Some(message) => Err(DynoxideError::ValidationException(message)),
                None => Ok(()),
            }
        }
        AttributeValue::N(n) => layer.check_number(n),
        AttributeValue::NS(set) => {
            for n in set {
                layer.check_number(n)?;
            }
            match crate::validation::duplicate_set_message(value, layer) {
                Some(message) => Err(DynoxideError::ValidationException(message)),
                None => Ok(()),
            }
        }
        _ => Ok(()),
    }
}

/// An integer member as DynamoDB reads it: a 32-bit signed integer that
/// saturates at either end, truncating a fraction. A value that saturated, was
/// written with a fraction or exponent, or was a token JSON doesn't allow (see
/// `number_tokens::read_integer`) is flagged, because the member is
/// then checked by the older layer, which reports the value as read (captured
/// eu-west-2 and eu-central-1, 20 October 2026). `0.0` is flagged though it
/// loses nothing: Query reports it as read, as it does `0.5` (captured
/// eu-west-2 and us-east-1, 6 November 2026).
#[derive(Debug, Clone, Copy)]
pub(crate) struct Int32 {
    pub(crate) value: i64,
    pub(crate) coerced: bool,
}

pub(crate) fn deserialize_int32<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<Int32>, D::Error> {
    use serde::de::Error as _;
    // Read as text first: serde_json refuses a number past f64's range, such
    // as `1e400`, which DynamoDB reads and saturates (captured eu-west-2 and
    // us-east-1, 6 November 2026).
    let Some(raw) = <Option<Box<serde_json::value::RawValue>> as serde::Deserialize>::deserialize(
        deserializer,
    )?
    else {
        return Ok(None);
    };
    // A malformed token is read by DynamoDB's integer rule or refused as
    // `Bad number format` (see `number_tokens::integer_text`).
    let crate::number_tokens::Integer {
        value: wide,
        lenient,
    } = match crate::number_tokens::read_integer(raw.get()) {
        Ok(integer) => integer,
        Err(crate::number_tokens::IntegerFault::BadNumber) => {
            return Err(crate::number_tokens::bad_number());
        }
        // Anything other than a number keeps serde's own wording, which
        // the caller maps as it did when the member was read as a number.
        Err(crate::number_tokens::IntegerFault::NotANumber(e)) => {
            return Err(D::Error::custom(e));
        }
    };
    let clamped = wide.clamp(i128::from(i32::MIN), i128::from(i32::MAX));
    Ok(Some(Int32 {
        value: clamped as i64,
        coerced: lenient || clamped != wide,
    }))
}

/// Select against the projection members and the index, as Query and Scan
/// both check it on the newer validation layer, in the
/// `1 validation error detected:` envelope: `SPECIFIC_ATTRIBUTES` needs
/// `AttributesToGet` or `ProjectionExpression`, any other value refuses both,
/// and `ALL_PROJECTED_ATTRIBUTES` needs an index (captured eu-west-2,
/// eu-central-1 and us-east-1 for Query, eu-west-2 and eu-central-1 for Scan,
/// 20 October 2026). The message keeps "Querying" on a Scan too.
pub(crate) fn select_projection_rejection(
    select: Option<&str>,
    attributes_to_get: bool,
    projection_expression: bool,
    index: bool,
) -> Option<String> {
    let select = select?;
    let projected = if attributes_to_get {
        Some("AttributesToGet")
    } else if projection_expression {
        Some("ProjectionExpression")
    } else {
        None
    };
    let msg = match (select, projected) {
        ("SPECIFIC_ATTRIBUTES", None) => "Must specify the AttributesToGet or \
             ProjectionExpression when choosing to get SPECIFIC_ATTRIBUTES"
            .to_string(),
        ("SPECIFIC_ATTRIBUTES", Some(_)) => return None,
        (select, Some(member)) => {
            // COUNT is phrased "only the Count"; other values use the literal.
            let target = if select == "COUNT" {
                "only the Count"
            } else {
                select
            };
            format!("Cannot specify the {member} when choosing to get {target}")
        }
        ("ALL_PROJECTED_ATTRIBUTES", None) if !index => {
            "ALL_PROJECTED_ATTRIBUTES can be used only when Querying using an IndexName".to_string()
        }
        (_, None) => return None,
    };
    Some(crate::validation::envelope_message(&msg))
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_commit_or_rollback_that_found_no_transaction_is_told_apart() {
        use crate::storage_backend::BackendError;
        let found_none =
            |message: &str| super::found_no_transaction(&BackendError::Other(message.to_string()));
        // As the native backend and the wasm one pass SQLite's message on.
        assert!(found_none("cannot rollback - no transaction is active"));
        assert!(found_none(
            "sqlite-wasm: SQLITE_ERROR: sqlite3 result code 1: cannot rollback - no transaction is active"
        ));
        assert!(found_none("cannot commit - no transaction is active"));
        assert!(!found_none("disk I/O error"));
        assert!(!super::found_no_transaction(&BackendError::Locked));
    }

    /// SQLite ends a transaction itself after some errors (a full disk, an
    /// I/O error). A write that carries on regardless reaches a commit with no
    /// transaction to commit, and says so rather than passing on SQLite's
    /// bare commit error.
    #[cfg(any(feature = "native-sqlite", feature = "_has-encryption"))]
    #[test]
    fn a_commit_sqlite_had_already_ended_is_named() {
        let storage = crate::storage::Storage::memory().unwrap();
        storage
            .conn()
            .execute_batch("CREATE TABLE w (x TEXT)")
            .unwrap();
        let result = pollster::block_on(super::with_write_transaction(&storage, async {
            let conn = storage.conn();
            conn.execute_batch(
                "INSERT INTO w VALUES ('before'); ROLLBACK; INSERT INTO w VALUES ('after');",
            )
            .unwrap();
            Ok(())
        }));
        match result {
            Err(crate::DynoxideError::InternalServerError(message)) => assert_eq!(
                message,
                "the write was not committed: SQLite ended its transaction after an earlier \
                 error, so what ran before that error was rolled back, and anything run after \
                 it was applied on its own"
            ),
            other => panic!("expected the ended-transaction error, got {other:?}"),
        }
        let rows: Vec<String> = {
            let mut stmt = storage.conn().prepare("SELECT x FROM w").unwrap();
            stmt.query_map([], |r| r.get(0))
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap()
        };
        // The statement after the transaction ended ran on its own.
        assert_eq!(rows, ["after"]);
    }

    #[test]
    fn size_estimate_range_is_whole_gigabytes_around_the_size() {
        const GB: i64 = 1024 * 1024 * 1024;
        assert_eq!(super::size_estimate_range_gb(0), vec![0.0, 1.0]);
        assert_eq!(super::size_estimate_range_gb(1_000), vec![0.0, 1.0]);
        assert_eq!(super::size_estimate_range_gb(GB - 1), vec![0.0, 1.0]);
        assert_eq!(super::size_estimate_range_gb(GB), vec![1.0, 2.0]);
        assert_eq!(super::size_estimate_range_gb(GB * 3 / 2), vec![1.0, 2.0]);
        assert_eq!(super::size_estimate_range_gb(10 * GB), vec![10.0, 11.0]);
    }

    /// A legacy `Expected` that doesn't read as conditions is refused in
    /// DynamoDB's words in every build, a library-only one included: the
    /// classification no longer depends on the server features.
    #[test]
    fn an_unreadable_expected_is_refused_in_dynamodbs_words() {
        let err = super::read_expected(&serde_json::json!({"guard": {"Value": {}}})).unwrap_err();
        match err {
            crate::DynoxideError::ValidationException(message) => assert_eq!(
                message,
                "Supplied AttributeValue is empty, must contain exactly one of the supported datatypes"
            ),
            other => panic!("expected a ValidationException, got {other:?}"),
        }
    }
}
