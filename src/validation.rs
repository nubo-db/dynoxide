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
    let prefix = format!(
        "{} validation error{} detected: ",
        errors.len(),
        if errors.len() == 1 { "" } else { "s" }
    );
    Some(format!("{}{}", prefix, errors.join("; ")))
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
pub fn validate_gsi(
    gsi: &GlobalSecondaryIndex,
    all_definitions: &[AttributeDefinition],
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
    validate_key_attributes_in_definitions(&gsi.key_schema, all_definitions)?;

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

/// Maximum nesting depth for item attribute values (DynamoDB's limit).
const MAX_NESTING_DEPTH: usize = 32;

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
pub fn validate_item_attribute_values(item: &Item) -> Result<()> {
    for value in item.values() {
        validate_attribute_value(value, 0)?;
    }
    Ok(())
}

fn validate_attribute_value(value: &AttributeValue, depth: usize) -> Result<()> {
    if depth > MAX_NESTING_DEPTH {
        return Err(DynoxideError::ValidationException(
            "Nesting level exceeds limit of 32".to_string(),
        ));
    }
    match value {
        AttributeValue::NULL(b) if !b => Err(DynoxideError::ValidationException(
            "One or more parameter values were invalid: \
             Null attribute value types must have the value of true"
                .to_string(),
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
        AttributeValue::SS(set) if !set.is_empty() => {
            let mut seen = std::collections::HashSet::new();
            for s in set {
                if !seen.insert(s.clone()) {
                    let display: Vec<&str> = set.iter().map(|s| s.as_str()).collect();
                    return Err(DynoxideError::ValidationException(format!(
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
                    return Err(DynoxideError::ValidationException(format!(
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
                    return Err(DynoxideError::ValidationException(
                        "Input collection contains duplicates".to_string(),
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

/// Validate Key attribute values before table-level checks.
///
/// This validates the attribute values in a Key map for:
/// - Invalid/empty numbers
/// - Empty sets, duplicate sets
/// - NULL attribute with non-true value
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
        AttributeValue::NULL(b) if !b => {
            return Err(DynoxideError::ValidationException(
                "One or more parameter values were invalid: \
                 Null attribute value types must have the value of true"
                    .to_string(),
            ));
        }
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
}
