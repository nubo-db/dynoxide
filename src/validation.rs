use crate::errors::{DynoxideError, Result};
use crate::types::{
    AttributeDefinition, AttributeValue, GlobalSecondaryIndex, Item, KeySchemaElement, KeyType,
    ScalarAttributeType,
};

/// Per-operation context for table-name validation.
///
/// AWS DynamoDB applies different constraints to `tableName` depending on the
/// operation. CreateTable enforces a minimum of 3 characters and a regex
/// pattern. Read/write operations (PutItem, GetItem, Query, Scan, UpdateItem,
/// DeleteItem, BatchGet/Write, TransactGet/Write) only enforce a minimum of 1
/// character; the regex pattern only fires on a non-empty invalid name.
#[derive(Copy, Clone, Debug)]
pub enum TableNameContext {
    /// CreateTable: regex pattern + minimum length 3.
    CreateTable,
    /// PutItem and friends: minimum length 1, regex pattern only on non-empty input.
    ReadWrite,
}

/// Validate a DynamoDB table name for a read/write operation.
///
/// Equivalent to `table_name_constraint_errors(Some(name), TableNameContext::ReadWrite)`
/// followed by formatting into the multi-error envelope. CreateTable callers must use
/// `table_name_constraint_errors` directly with `TableNameContext::CreateTable` because
/// CreateTable's full validation produces additional errors that need to be folded into
/// a single envelope.
pub fn validate_table_name(name: &str) -> Result<()> {
    let errors = table_name_constraint_errors(Some(name), TableNameContext::ReadWrite);
    if errors.is_empty() {
        return Ok(());
    }
    let count = errors.len();
    let msg = format!(
        "{count} validation error{} detected: {}",
        if count == 1 { "" } else { "s" },
        errors.join("; ")
    );
    Err(DynoxideError::ValidationException(msg))
}

/// Collect table-name constraint errors for the multi-error validation format.
///
/// Returns a (possibly empty) list of error strings. If `table_name` is `None`,
/// a "must not be null" error is emitted. If it is present but invalid, pattern
/// and/or length errors are emitted, gated by `context`.
pub fn table_name_constraint_errors(
    table_name: Option<&str>,
    context: TableNameContext,
) -> Vec<String> {
    let mut errors = Vec::new();
    match table_name {
        None => {
            errors.push(
                "Value null at 'tableName' failed to satisfy constraint: \
                 Member must not be null"
                    .to_string(),
            );
        }
        Some(name) => match context {
            TableNameContext::CreateTable => {
                if name.is_empty()
                    || !name
                        .chars()
                        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
                {
                    errors.push(format!(
                        "Value '{}' at 'tableName' failed to satisfy constraint: \
                         Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+",
                        name
                    ));
                }
                if name.len() < 3 {
                    errors.push(format!(
                        "Value '{}' at 'tableName' failed to satisfy constraint: \
                         Member must have length greater than or equal to 3",
                        name
                    ));
                }
                if name.len() > 255 {
                    errors.push(format!(
                        "Value '{}' at 'tableName' failed to satisfy constraint: \
                         Member must have length less than or equal to 255",
                        name
                    ));
                }
            }
            TableNameContext::ReadWrite => {
                if name.is_empty() {
                    errors.push(
                        "Value '' at 'tableName' failed to satisfy constraint: \
                         Member must have length greater than or equal to 1"
                            .to_string(),
                    );
                } else if !name
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
                {
                    errors.push(format!(
                        "Value '{}' at 'tableName' failed to satisfy constraint: \
                         Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+",
                        name
                    ));
                }
                if name.len() > 255 {
                    errors.push(format!(
                        "Value '{}' at 'tableName' failed to satisfy constraint: \
                         Member must have length less than or equal to 255",
                        name
                    ));
                }
            }
        },
    }
    errors
}

/// Format a list of constraint validation errors into the DynamoDB multi-error format.
///
/// Returns `Some(message)` if there are errors, `None` if empty.
pub fn format_validation_errors(errors: &[String]) -> Option<String> {
    if errors.is_empty() {
        return None;
    }
    if let [single] = errors {
        return Some(envelope_message(single));
    }
    Some(format!(
        "{} validation errors detected: {}",
        errors.len(),
        errors.join("; ")
    ))
}

/// The single-error form of the request-validation envelope. This and
/// `format_validation_errors` are the only producers of the prefix wording.
pub(crate) fn envelope_message(msg: &str) -> String {
    format!("1 validation error detected: {msg}")
}

/// Wrap an `EnvelopedValidation` error in the `1 validation error detected: `
/// envelope, converting it to a plain `ValidationException`. Every other error
/// passes through unchanged.
///
/// PutItem and UpdateItem apply this at their operation boundary.
pub(crate) fn envelope_request_validation(err: DynoxideError) -> DynoxideError {
    match err {
        DynoxideError::EnvelopedValidation(msg) => {
            DynoxideError::ValidationException(envelope_message(&msg))
        }
        other => other,
    }
}

/// Convert the wire-invisible `EnvelopedValidation` tag back to a plain,
/// unenveloped `ValidationException`. Every other error passes through
/// unchanged.
///
/// Operations other than PutItem and UpdateItem report the request-validation
/// family bare, and the tag must never reach the wire.
pub(crate) fn strip_request_validation_tag(err: DynoxideError) -> DynoxideError {
    match err {
        DynoxideError::EnvelopedValidation(msg) => DynoxideError::ValidationException(msg),
        other => other,
    }
}

/// Promote a plain `ValidationException` to the wire-invisible
/// `EnvelopedValidation` tag. Inverse of `strip_request_validation_tag`, for
/// call sites that tag an error raised by a shared helper.
pub(crate) fn tag_request_validation(err: DynoxideError) -> DynoxideError {
    match err {
        DynoxideError::ValidationException(msg) => DynoxideError::EnvelopedValidation(msg),
        other => other,
    }
}

/// Resolve the wire-invisible `EnvelopedValidation` tag for a named operation:
/// PutItem and UpdateItem wrap the request-validation family in the
/// `1 validation error detected: ` envelope, every other operation reports it
/// bare. Either way the tag never reaches the wire. Enveloping is idempotent
/// for untagged errors, so applying this to an already-enveloped action error
/// is safe.
///
/// Single owner of the operation split; the HTTP and wasm dispatch seams both
/// route through here.
pub(crate) fn resolve_request_validation_tag(operation: &str, err: DynoxideError) -> DynoxideError {
    match operation {
        "PutItem" | "UpdateItem" => envelope_request_validation(err),
        _ => strip_request_validation_tag(err),
    }
}

/// A validation failure from a shared validator, classified so PutItem and
/// UpdateItem can tell which families DynamoDB wraps in the
/// `1 validation error detected: ` envelope.
///
/// Every other caller converts it straight back to the original error via the
/// `From` impl, so plain `?` propagation keeps their behaviour byte-identical.
#[derive(Debug)]
pub(crate) enum ClassifiedValidationError {
    /// A family DynamoDB reports bare, or an error adopted from an
    /// unclassified helper.
    Bare(DynoxideError),
    /// A family DynamoDB wraps in the request-validation envelope on PutItem
    /// and UpdateItem.
    Enveloped(String),
}

impl ClassifiedValidationError {
    /// A family DynamoDB reports bare.
    pub(crate) fn bare(message: impl Into<String>) -> Self {
        Self::Bare(DynoxideError::ValidationException(message.into()))
    }

    /// A family DynamoDB wraps in the request-validation envelope on PutItem
    /// and UpdateItem.
    pub(crate) fn enveloped(message: impl Into<String>) -> Self {
        Self::Enveloped(message.into())
    }

    /// Convert for a PutItem/UpdateItem call site: enveloped families become
    /// the wire-invisible `EnvelopedValidation` tag (unwrapped at the
    /// operation boundary), everything else passes through unchanged.
    pub(crate) fn into_tagged(self) -> DynoxideError {
        match self {
            Self::Enveloped(msg) => DynoxideError::EnvelopedValidation(msg),
            Self::Bare(err) => err,
        }
    }
}

impl From<ClassifiedValidationError> for DynoxideError {
    fn from(e: ClassifiedValidationError) -> Self {
        match e {
            ClassifiedValidationError::Enveloped(msg) => DynoxideError::ValidationException(msg),
            ClassifiedValidationError::Bare(err) => err,
        }
    }
}

impl From<DynoxideError> for ClassifiedValidationError {
    /// Errors adopted from unclassified helpers stay bare; only call sites
    /// that know a family is enveloped construct the enveloped form.
    fn from(error: DynoxideError) -> Self {
        Self::Bare(error)
    }
}

/// Validate key schema: exactly one HASH key, optionally one RANGE key.
///
/// DynamoDB validates positionally: the first element must be HASH and, if a
/// second element is present, it must be RANGE.
pub fn validate_key_schema(key_schema: &[KeySchemaElement]) -> Result<()> {
    if key_schema.is_empty() || key_schema.len() > 2 {
        return Err(DynoxideError::ValidationException(
            "1 validation error detected: Value null at 'keySchema' failed to satisfy constraint: \
             Member must have length less than or equal to 2"
                .to_string(),
        ));
    }

    // First element must be HASH.
    if key_schema[0].key_type != KeyType::HASH {
        return Err(DynoxideError::ValidationException(
            "Invalid KeySchema: The first KeySchemaElement is not a HASH key type".to_string(),
        ));
    }

    // Check for duplicate attribute names (before type check, matching DynamoDB ordering).
    if key_schema.len() == 2 && key_schema[0].attribute_name == key_schema[1].attribute_name {
        return Err(DynoxideError::ValidationException(
            "Both the Hash Key and the Range Key element in the KeySchema have the same name"
                .to_string(),
        ));
    }

    // Second element, if present, must be RANGE.
    if key_schema.len() == 2 && key_schema[1].key_type != KeyType::RANGE {
        return Err(DynoxideError::ValidationException(
            "Invalid KeySchema: The second KeySchemaElement is not a RANGE key type".to_string(),
        ));
    }

    Ok(())
}

/// Validate attribute definitions: types must be S, N, or B.
pub fn validate_attribute_definitions(defs: &[AttributeDefinition]) -> Result<()> {
    if defs.is_empty() {
        return Err(DynoxideError::ValidationException(
            "1 validation error detected: Value null at 'attributeDefinitions' failed to satisfy \
             constraint: Member must have length greater than or equal to 1"
                .to_string(),
        ));
    }

    for def in defs {
        match def.attribute_type {
            ScalarAttributeType::S | ScalarAttributeType::N | ScalarAttributeType::B => {}
        }
    }

    Ok(())
}

/// Validate that all key schema attributes are defined in attribute definitions.
pub fn validate_key_attributes_in_definitions(
    key_schema: &[KeySchemaElement],
    definitions: &[AttributeDefinition],
) -> Result<()> {
    for key_elem in key_schema {
        let found = definitions
            .iter()
            .any(|def| def.attribute_name == key_elem.attribute_name);
        if !found {
            return Err(DynoxideError::ValidationException(format!(
                "One or more parameter values were invalid: Some index key attributes are not \
                 defined in AttributeDefinitions. Keys: [{}], AttributeDefinitions: [{}]",
                key_elem.attribute_name,
                definitions
                    .iter()
                    .map(|d| d.attribute_name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }
    }

    Ok(())
}

/// Validate a Global Secondary Index definition.
///
/// `request_definitions` must be the AttributeDefinitions declared in the
/// current request, not the table's merged stored set: DynamoDB requires a new
/// index's key attributes to be (re)declared in the request itself.
pub fn validate_gsi(
    gsi: &GlobalSecondaryIndex,
    request_definitions: &[AttributeDefinition],
) -> Result<()> {
    // Validate index name length
    if gsi.index_name.len() < 3 || gsi.index_name.len() > 255 {
        return Err(DynoxideError::ValidationException(format!(
            "1 validation error detected: Value '{}' at 'globalSecondaryIndexes.1.member.indexName' \
             failed to satisfy constraint: Member must have length greater than or equal to 3",
            gsi.index_name
        )));
    }

    // Validate index name character set (same as table names)
    if !gsi
        .index_name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
    {
        return Err(DynoxideError::ValidationException(format!(
            "1 validation error detected: Value '{}' at 'globalSecondaryIndexes.1.member.indexName' \
             failed to satisfy constraint: Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+",
            gsi.index_name
        )));
    }

    // Validate key schema
    validate_key_schema(&gsi.key_schema)?;

    // Validate projection
    validate_projection(&gsi.projection, &gsi.index_name)?;

    // Validate GSI key attributes exist in definitions
    validate_key_attributes_in_definitions(&gsi.key_schema, request_definitions)?;

    Ok(())
}

/// Validate a Projection (for GSI or LSI).
///
/// DynamoDB checks:
/// 1. ProjectionType must be present (not null)
/// 2. If NonKeyAttributes is specified, ProjectionType must be INCLUDE
pub fn validate_projection(projection: &crate::types::Projection, _index_name: &str) -> Result<()> {
    match &projection.projection_type {
        None => {
            return Err(DynoxideError::ValidationException(
                "One or more parameter values were invalid: Unknown ProjectionType: null"
                    .to_string(),
            ));
        }
        Some(pt) => {
            if let Some(ref nka) = projection.non_key_attributes {
                // NonKeyAttributes is present; check ProjectionType compatibility
                match pt {
                    crate::types::ProjectionType::ALL => {
                        return Err(DynoxideError::ValidationException(
                            "One or more parameter values were invalid: \
                             ProjectionType is ALL, but NonKeyAttributes is specified"
                                .to_string(),
                        ));
                    }
                    crate::types::ProjectionType::KEYS_ONLY => {
                        return Err(DynoxideError::ValidationException(
                            "One or more parameter values were invalid: \
                             ProjectionType is KEYS_ONLY, but NonKeyAttributes is specified"
                                .to_string(),
                        ));
                    }
                    crate::types::ProjectionType::INCLUDE => {
                        // NonKeyAttributes with INCLUDE is valid, but must not be empty
                        if nka.is_empty() {
                            return Err(DynoxideError::ValidationException(
                                "One or more parameter values were invalid: \
                                 NonKeyAttributes must not be empty"
                                    .to_string(),
                            ));
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

/// Extract the partition key name from a key schema.
pub fn partition_key_name(key_schema: &[KeySchemaElement]) -> Option<&str> {
    key_schema
        .iter()
        .find(|k| k.key_type == KeyType::HASH)
        .map(|k| k.attribute_name.as_str())
}

/// DynamoDB caps document nesting at 32 levels, counting the top-level attribute
/// value as level 1. Values are validated 0-indexed (top-level = depth 0), so the
/// deepest permitted leaf sits at depth 31 and a value reaching depth 32 is rejected.
const MAX_NESTING_DEPTH: usize = 32;

/// Real DynamoDB's verbatim message when document nesting exceeds the limit. Shared
/// by the stored-item and ExpressionAttributeValue checks so both match AWS.
const NESTING_LIMIT_MESSAGE: &str = "Nesting Levels have exceeded supported limits: Attributes in the item have nested levels beyond supported limit";

/// Validate all attribute values in an item.
///
/// Rejects:
/// - Empty sets (`{"SS": []}`, `{"NS": []}`, `{"BS": []}`)
/// - Numbers that violate DynamoDB's precision/range constraints
/// - Nesting deeper than 32 levels
///
/// Validation is recursive: invalid values nested inside L (list) or M (map) are also rejected.
///
/// **Note:** Empty strings (`{"S": ""}`) and empty binary values (`{"B": ""}`) are
/// permitted in non-key attributes since DynamoDB's 2020 update. Key attributes
/// are validated separately in `helpers::validate_key_type`.
///
/// **Important:** This must only be called on items being persisted, NOT on
/// `ExpressionAttributeValues` (which may legitimately contain empty strings for comparisons).
pub fn validate_item_attribute_values(item: &Item) -> crate::Result<()> {
    validate_item_attribute_values_classified(item).map_err(Into::into)
}

/// Classified form of [`validate_item_attribute_values`]: the set families
/// (empty and duplicate SS/NS/BS) are marked as enveloped so PutItem and
/// UpdateItem can wrap them in the request-validation envelope; number and
/// nesting errors stay bare. Every other caller uses the public wrapper,
/// which converts back to the plain error unchanged.
pub(crate) fn validate_item_attribute_values_classified(
    item: &Item,
) -> std::result::Result<(), ClassifiedValidationError> {
    for value in item.values() {
        validate_attribute_value(value, 0)?;
    }
    Ok(())
}

fn validate_attribute_value(
    value: &AttributeValue,
    depth: usize,
) -> std::result::Result<(), ClassifiedValidationError> {
    if depth >= MAX_NESTING_DEPTH {
        return Err(ClassifiedValidationError::bare(NESTING_LIMIT_MESSAGE));
    }
    match value {
        AttributeValue::SS(set) if set.is_empty() => Err(ClassifiedValidationError::enveloped(
            "One or more parameter values were invalid: An string set  may not be empty",
        )),
        AttributeValue::NS(set) if set.is_empty() => Err(ClassifiedValidationError::enveloped(
            "One or more parameter values were invalid: An number set  may not be empty",
        )),
        AttributeValue::BS(set) if set.is_empty() => Err(ClassifiedValidationError::enveloped(
            "One or more parameter values were invalid: Binary sets should not be empty",
        )),
        AttributeValue::SS(set) if !set.is_empty() => {
            let mut seen = std::collections::HashSet::new();
            for s in set {
                if !seen.insert(s.clone()) {
                    let display: Vec<&str> = set.iter().map(|s| s.as_str()).collect();
                    return Err(ClassifiedValidationError::enveloped(format!(
                        "One or more parameter values were invalid: Input collection [{}] contains duplicates.",
                        display.join(", ")
                    )));
                }
            }
            Ok(())
        }
        AttributeValue::BS(set) if !set.is_empty() => {
            let mut seen = std::collections::HashSet::new();
            for b in set {
                if !seen.insert(b.clone()) {
                    use base64::Engine;
                    let display: Vec<String> = set
                        .iter()
                        .map(|s| base64::engine::general_purpose::STANDARD.encode(s))
                        .collect();
                    return Err(ClassifiedValidationError::enveloped(format!(
                        "One or more parameter values were invalid: Input collection [{}]of type BS contains duplicates.",
                        display.join(", ")
                    )));
                }
            }
            Ok(())
        }
        AttributeValue::NS(set) if !set.is_empty() => {
            for n in set {
                crate::types::validate_dynamo_number(n)?;
            }
            // Check for numeric duplicates
            let mut seen = std::collections::HashSet::new();
            for n in set {
                let normalized = crate::types::normalize_dynamo_number(n);
                if !seen.insert(normalized) {
                    return Err(ClassifiedValidationError::enveloped(
                        "Input collection contains duplicates",
                    ));
                }
            }
            Ok(())
        }
        AttributeValue::N(n) => {
            crate::types::validate_dynamo_number(n)?;
            Ok(())
        }
        AttributeValue::L(list) => {
            for v in list {
                validate_attribute_value(v, depth + 1)?;
            }
            Ok(())
        }
        AttributeValue::M(map) => {
            for v in map.values() {
                validate_attribute_value(v, depth + 1)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// Validate that a single `ExpressionAttributeValue` does not nest deeper than
/// DynamoDB allows.
///
/// Real DynamoDB rejects expression values whose document nesting exceeds 32 levels
/// up front, before the expression is evaluated, raising the same bare nesting
/// `ValidationException` it raises for over-deep stored items (no "ExpressionAttributeValues
/// contains invalid value" wrapper). Only the nesting depth is checked here; empty
/// strings and other shapes that are legal in comparisons are left untouched.
pub fn validate_nesting_depth(value: &AttributeValue) -> Result<()> {
    check_nesting_depth(value, 0)
}

fn check_nesting_depth(value: &AttributeValue, depth: usize) -> Result<()> {
    if depth >= MAX_NESTING_DEPTH {
        return Err(DynoxideError::ValidationException(
            NESTING_LIMIT_MESSAGE.to_string(),
        ));
    }
    match value {
        AttributeValue::L(list) => list
            .iter()
            .try_for_each(|v| check_nesting_depth(v, depth + 1)),
        AttributeValue::M(map) => map
            .values()
            .try_for_each(|v| check_nesting_depth(v, depth + 1)),
        _ => Ok(()),
    }
}

/// Validate Key attribute values before table-level checks.
///
/// This validates the attribute values in a Key map for:
/// - Invalid/empty numbers
/// - Empty sets, duplicate sets
/// - Multiple datatypes
///
/// These errors are returned with "One or more parameter values were invalid: " prefix.
pub fn validate_key_attribute_values(key: &Item) -> Result<()> {
    for value in key.values() {
        validate_key_attr_value(value)?;
    }
    Ok(())
}

fn validate_key_attr_value(value: &AttributeValue) -> Result<()> {
    match value {
        AttributeValue::SS(set) if set.is_empty() => {
            return Err(DynoxideError::ValidationException(
                "One or more parameter values were invalid: An string set  may not be empty"
                    .to_string(),
            ));
        }
        AttributeValue::NS(set) if set.is_empty() => {
            return Err(DynoxideError::ValidationException(
                "One or more parameter values were invalid: An number set  may not be empty"
                    .to_string(),
            ));
        }
        AttributeValue::BS(set) if set.is_empty() => {
            return Err(DynoxideError::ValidationException(
                "One or more parameter values were invalid: Binary sets should not be empty"
                    .to_string(),
            ));
        }
        AttributeValue::SS(set) => {
            // Check for duplicates
            let mut seen = std::collections::HashSet::new();
            for s in set {
                if !seen.insert(s.clone()) {
                    let display: Vec<&str> = set.iter().map(|s| s.as_str()).collect();
                    return Err(DynoxideError::ValidationException(format!(
                        "One or more parameter values were invalid: \
                         Input collection [{}] contains duplicates.",
                        display.join(", ")
                    )));
                }
            }
        }
        AttributeValue::NS(set) if !set.is_empty() => {
            // Validate numbers and check for duplicates
            for n in set {
                crate::types::validate_dynamo_number(n)?;
            }
            let mut seen = std::collections::HashSet::new();
            for n in set {
                let normalized = crate::types::normalize_dynamo_number(n);
                if !seen.insert(normalized) {
                    return Err(DynoxideError::ValidationException(
                        "Input collection contains duplicates".to_string(),
                    ));
                }
            }
        }
        AttributeValue::BS(set) => {
            // Check for duplicates
            let mut seen = std::collections::HashSet::new();
            for b in set {
                if !seen.insert(b.clone()) {
                    use base64::Engine;
                    let display: Vec<String> = set
                        .iter()
                        .map(|s| base64::engine::general_purpose::STANDARD.encode(s))
                        .collect();
                    return Err(DynoxideError::ValidationException(format!(
                        "One or more parameter values were invalid: \
                         Input collection [{}]of type BS contains duplicates.",
                        display.join(", ")
                    )));
                }
            }
        }
        AttributeValue::N(n) => {
            crate::types::validate_dynamo_number(n)?;
        }
        _ => {}
    }
    Ok(())
}

/// Normalize sets within an item by deduplicating them in-place.
///
/// - SS: deduplicates by string value
/// - NS: deduplicates by numeric value (e.g., "1.0" and "1" are the same)
/// - BS: deduplicates by byte content
///
/// Recursively normalizes sets inside L (list) and M (map) values.
pub fn normalize_item_sets(item: &mut Item) {
    for value in item.values_mut() {
        normalize_attribute_sets(value);
    }
}

fn normalize_attribute_sets(value: &mut AttributeValue) {
    match value {
        AttributeValue::N(n) => {
            *n = crate::types::normalize_dynamo_number(n);
        }
        AttributeValue::SS(set) => {
            let mut seen = std::collections::HashSet::new();
            set.retain(|s| seen.insert(s.clone()));
        }
        AttributeValue::NS(set) => {
            let mut seen = std::collections::HashSet::new();
            set.retain(|n| seen.insert(normalize_number_for_dedup(n)));
            // Normalize each number in the set
            for n in set.iter_mut() {
                *n = crate::types::normalize_dynamo_number(n);
            }
        }
        AttributeValue::BS(set) => {
            let mut seen = std::collections::HashSet::new();
            set.retain(|b| seen.insert(b.clone()));
        }
        AttributeValue::L(list) => {
            for v in list.iter_mut() {
                normalize_attribute_sets(v);
            }
        }
        AttributeValue::M(map) => {
            for v in map.values_mut() {
                normalize_attribute_sets(v);
            }
        }
        _ => {}
    }
}

/// Produce a canonical string for a DynamoDB number for deduplication purposes.
/// Strips leading/trailing zeros and normalizes to a canonical form so that
/// "1.0", "1", "1.00", "01" all map to the same string.
fn normalize_number_for_dedup(n: &str) -> String {
    let trimmed = n.trim();
    let negative = trimmed.starts_with('-');
    let abs_str = if negative { &trimmed[1..] } else { trimmed };

    let (digits, exponent) = crate::types::parse_number_parts(abs_str);

    if digits.is_empty() {
        return "0".to_string();
    }

    let mantissa: String = digits.iter().map(|&d| (b'0' + d) as char).collect();
    let sign = if negative { "-" } else { "" };
    format!("{sign}{mantissa}E{exponent}")
}

/// Validate a Local Secondary Index definition.
pub fn validate_lsi(
    lsi: &crate::types::LocalSecondaryIndex,
    table_key_schema: &[KeySchemaElement],
    all_definitions: &[AttributeDefinition],
) -> Result<()> {
    // Validate index name length
    if lsi.index_name.len() < 3 || lsi.index_name.len() > 255 {
        return Err(DynoxideError::ValidationException(format!(
            "1 validation error detected: Value '{}' at 'localSecondaryIndexes.1.member.indexName' \
             failed to satisfy constraint: Member must have length greater than or equal to 3",
            lsi.index_name
        )));
    }

    // Validate index name character set
    if !lsi
        .index_name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
    {
        return Err(DynoxideError::ValidationException(format!(
            "1 validation error detected: Value '{}' at 'localSecondaryIndexes.1.member.indexName' \
             failed to satisfy constraint: Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+",
            lsi.index_name
        )));
    }

    // Validate key schema
    validate_key_schema(&lsi.key_schema)?;

    // Validate projection (DynamoDB checks this before hash key / sort key checks)
    validate_projection(&lsi.projection, &lsi.index_name)?;

    // LSI must have a RANGE key (sort key)
    let lsi_pk = lsi
        .key_schema
        .iter()
        .find(|k| k.key_type == KeyType::HASH)
        .map(|k| k.attribute_name.as_str());
    let lsi_sk = lsi
        .key_schema
        .iter()
        .find(|k| k.key_type == KeyType::RANGE)
        .map(|k| k.attribute_name.as_str());

    let table_pk = partition_key_name(table_key_schema);
    let table_sk = sort_key_name(table_key_schema);

    // LSI partition key MUST match table partition key
    if lsi_pk != table_pk {
        return Err(DynoxideError::ValidationException(
            "One or more parameter values were invalid: Table KeySchema: The AttributeValue for a key attribute for the table must match the AttributeValue definition".to_string(),
        ));
    }

    // LSI sort key must be different from table sort key
    if lsi_sk.is_some() && lsi_sk == table_sk {
        return Err(DynoxideError::ValidationException(
            "One or more parameter values were invalid: Index KeySchema: The index KeySchema must not be the same as the table KeySchema".to_string(),
        ));
    }

    // LSI sort key must be in AttributeDefinitions
    validate_key_attributes_in_definitions(&lsi.key_schema, all_definitions)?;

    Ok(())
}

/// Extract the sort key name from a key schema (if present).
pub fn sort_key_name(key_schema: &[KeySchemaElement]) -> Option<&str> {
    key_schema
        .iter()
        .find(|k| k.key_type == KeyType::RANGE)
        .map(|k| k.attribute_name.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash_key(name: &str) -> KeySchemaElement {
        KeySchemaElement {
            attribute_name: name.to_string(),
            key_type: KeyType::HASH,
        }
    }

    fn range_key(name: &str) -> KeySchemaElement {
        KeySchemaElement {
            attribute_name: name.to_string(),
            key_type: KeyType::RANGE,
        }
    }

    fn attr_def(name: &str, attr_type: ScalarAttributeType) -> AttributeDefinition {
        AttributeDefinition {
            attribute_name: name.to_string(),
            attribute_type: attr_type,
        }
    }

    #[test]
    fn test_valid_table_name() {
        assert!(validate_table_name("MyTable").is_ok());
        assert!(validate_table_name("my-table.v2").is_ok());
        assert!(validate_table_name("a_b").is_ok());
    }

    #[test]
    fn test_short_table_name_accepted_for_read_write() {
        // ReadWrite context (the default for validate_table_name) only enforces min length 1,
        // matching AWS's per-operation rules. CreateTable's min-length-3 lives behind
        // table_name_constraint_errors with TableNameContext::CreateTable.
        assert!(validate_table_name("ab").is_ok());
        assert!(validate_table_name("a").is_ok());
    }

    #[test]
    fn test_empty_table_name_rejected_for_read_write() {
        let err = validate_table_name("").unwrap_err().to_string();
        assert!(err.contains("Member must have length greater than or equal to 1"));
        assert!(!err.contains("greater than or equal to 3"));
    }

    #[test]
    fn test_invalid_table_name_bad_chars() {
        assert!(validate_table_name("my table").is_err());
        assert!(validate_table_name("my@table").is_err());
    }

    #[test]
    fn test_create_table_context_keeps_min_length_3() {
        let errs = table_name_constraint_errors(Some("ab"), TableNameContext::CreateTable);
        assert!(
            errs.iter()
                .any(|e| e.contains("Member must have length greater than or equal to 3"))
        );
    }

    #[test]
    fn test_valid_key_schema() {
        let schema = vec![hash_key("pk")];
        assert!(validate_key_schema(&schema).is_ok());

        let schema = vec![hash_key("pk"), range_key("sk")];
        assert!(validate_key_schema(&schema).is_ok());
    }

    #[test]
    fn test_invalid_key_schema_empty() {
        assert!(validate_key_schema(&[]).is_err());
    }

    #[test]
    fn test_invalid_key_schema_no_hash() {
        let schema = vec![range_key("sk")];
        assert!(validate_key_schema(&schema).is_err());
    }

    #[test]
    fn test_valid_key_attributes_in_definitions() {
        let schema = vec![hash_key("pk"), range_key("sk")];
        let defs = vec![
            attr_def("pk", ScalarAttributeType::S),
            attr_def("sk", ScalarAttributeType::N),
        ];
        assert!(validate_key_attributes_in_definitions(&schema, &defs).is_ok());
    }

    #[test]
    fn test_missing_key_attribute_in_definitions() {
        let schema = vec![hash_key("pk"), range_key("sk")];
        let defs = vec![attr_def("pk", ScalarAttributeType::S)];
        assert!(validate_key_attributes_in_definitions(&schema, &defs).is_err());
    }

    #[test]
    fn test_partition_key_name() {
        let schema = vec![hash_key("pk"), range_key("sk")];
        assert_eq!(partition_key_name(&schema), Some("pk"));
    }

    #[test]
    fn test_sort_key_name() {
        let schema = vec![hash_key("pk"), range_key("sk")];
        assert_eq!(sort_key_name(&schema), Some("sk"));

        let schema = vec![hash_key("pk")];
        assert_eq!(sort_key_name(&schema), None);
    }

    #[test]
    fn test_envelope_request_validation_wraps_tagged_error() {
        let msg = "Value '' at 'expressionAttributeNames' failed to satisfy constraint: \
                   Map value must satisfy constraint";
        let err = envelope_request_validation(DynoxideError::EnvelopedValidation(msg.to_string()));
        match err {
            DynoxideError::ValidationException(m) => {
                assert_eq!(m, format!("1 validation error detected: {msg}"));
            }
            other => panic!("expected ValidationException, got {other:?}"),
        }
    }

    #[test]
    fn test_envelope_request_validation_passes_other_errors_through() {
        let plain = envelope_request_validation(DynoxideError::ValidationException("msg".into()));
        assert!(matches!(
            &plain,
            DynoxideError::ValidationException(m) if m == "msg"
        ));

        let key_empty =
            envelope_request_validation(DynoxideError::KeyEmptyValueValidation("msg".into()));
        assert!(matches!(
            &key_empty,
            DynoxideError::KeyEmptyValueValidation(m) if m == "msg"
        ));

        let not_found =
            envelope_request_validation(DynoxideError::ResourceNotFoundException("msg".into()));
        assert!(matches!(
            &not_found,
            DynoxideError::ResourceNotFoundException(m) if m == "msg"
        ));
    }

    #[test]
    fn test_strip_request_validation_tag_untags_without_envelope() {
        let err = strip_request_validation_tag(DynoxideError::EnvelopedValidation("msg".into()));
        assert!(matches!(
            &err,
            DynoxideError::ValidationException(m) if m == "msg"
        ));
    }

    #[test]
    fn test_strip_request_validation_tag_passes_other_errors_through() {
        let plain = strip_request_validation_tag(DynoxideError::ValidationException("msg".into()));
        assert!(matches!(
            &plain,
            DynoxideError::ValidationException(m) if m == "msg"
        ));

        let not_found =
            strip_request_validation_tag(DynoxideError::ResourceNotFoundException("msg".into()));
        assert!(matches!(
            &not_found,
            DynoxideError::ResourceNotFoundException(m) if m == "msg"
        ));
    }
}
