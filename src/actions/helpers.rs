use crate::errors::{DynoxideError, Result};
use crate::storage::{Storage, TableMetadata};
use crate::types::{
    AttributeDefinition, AttributeValue, KeySchemaElement, KeyType, ScalarAttributeType,
};
use std::collections::HashMap;

/// Parsed key schema for convenient access.
pub struct KeySchema {
    pub partition_key: String,
    pub partition_key_type: ScalarAttributeType,
    pub sort_key: Option<String>,
    pub sort_key_type: Option<ScalarAttributeType>,
}

/// Require that a table exists, returning its metadata.
pub fn require_table(storage: &Storage, table_name: &str) -> Result<TableMetadata> {
    storage.get_table_metadata(table_name)?.ok_or_else(|| {
        DynoxideError::ResourceNotFoundException(format!(
            "Requested resource not found: Table: {table_name} not found"
        ))
    })
}

/// Like `require_table`, but uses the shorter error message format that DynamoDB
/// uses for item-level operations (PutItem, GetItem, DeleteItem, UpdateItem,
/// Query, Scan, BatchGetItem, BatchWriteItem).
pub fn require_table_for_item_op(storage: &Storage, table_name: &str) -> Result<TableMetadata> {
    storage.get_table_metadata(table_name)?.ok_or_else(|| {
        DynoxideError::ResourceNotFoundException("Requested resource not found".to_string())
    })
}

/// Parse key schema and attribute definitions from table metadata.
pub fn parse_key_schema(meta: &TableMetadata) -> Result<KeySchema> {
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

/// Validate that an item has the required key attributes with correct types.
pub fn validate_item_keys(
    item: &HashMap<String, AttributeValue>,
    schema: &KeySchema,
    _meta: &TableMetadata,
) -> Result<()> {
    // Partition key must be present
    let pk_val = item.get(&schema.partition_key).ok_or_else(|| {
        DynoxideError::ValidationException(format!(
            "One or more parameter values were invalid: Missing the key {} in the item",
            schema.partition_key
        ))
    })?;

    validate_key_type(pk_val, &schema.partition_key, &schema.partition_key_type)?;

    // Validate hash key size (max 2048 bytes)
    let pk_size = key_attribute_size(pk_val);
    if pk_size > 2048 {
        return Err(DynoxideError::ValidationException(
            "One or more parameter values were invalid: \
             Size of hashkey has exceeded the maximum size limit of2048 bytes"
                .to_string(),
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
            validate_key_type(sk_val, sk_name, sk_type)?;
        }

        // Validate range key size (max 1024 bytes)
        let sk_size = key_attribute_size(sk_val);
        if sk_size > 1024 {
            return Err(DynoxideError::ValidationException(
                "One or more parameter values were invalid: \
                 Aggregated size of all range keys has exceeded the size limit of 1024 bytes"
                    .to_string(),
            ));
        }
    }

    Ok(())
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
            "One or more parameter values were invalid: \
             Size of hashkey has exceeded the maximum size limit of2048 bytes"
                .to_string(),
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
                "One or more parameter values were invalid: \
                 Aggregated size of all range keys has exceeded the size limit of 1024 bytes"
                    .to_string(),
            ));
        }
    }

    Ok(())
}

/// Validate a key value matches its expected type.
fn validate_key_type(
    val: &AttributeValue,
    attr_name: &str,
    expected: &ScalarAttributeType,
) -> Result<()> {
    let matches = match (val, expected) {
        (AttributeValue::S(s), ScalarAttributeType::S) => {
            if s.is_empty() {
                return Err(DynoxideError::ValidationException(format!(
                    "One or more parameter values are not valid. The AttributeValue for a key \
                     attribute cannot contain an empty string value. Key: {attr_name}"
                )));
            }
            true
        }
        (AttributeValue::N(_), ScalarAttributeType::N) => true,
        (AttributeValue::B(b), ScalarAttributeType::B) => {
            if b.is_empty() {
                return Err(DynoxideError::ValidationException(format!(
                    "One or more parameter values are not valid. The AttributeValue for a key \
                     attribute cannot contain an empty binary value. Key: {attr_name}"
                )));
            }
            true
        }
        _ => false,
    };

    if !matches {
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
        return Err(DynoxideError::ValidationException(format!(
            "One or more parameter values were invalid: Type mismatch for key \
             {attr_name} expected: {expected:?} actual: {actual_type}"
        )));
    }

    Ok(())
}

/// Validate a key value matches its expected type for key-based operations
/// (GetItem, DeleteItem, UpdateItem).
///
/// Unlike `validate_key_type` (used by PutItem), this returns the generic
/// "does not match the schema" error for type mismatches, and uses the
/// "were invalid:" prefix for empty key values.
fn validate_key_type_for_key_op(
    val: &AttributeValue,
    attr_name: &str,
    expected: &ScalarAttributeType,
) -> Result<()> {
    let matches = match (val, expected) {
        (AttributeValue::S(s), ScalarAttributeType::S) => {
            if s.is_empty() {
                return Err(DynoxideError::ValidationException(format!(
                    "One or more parameter values were invalid: \
                     The AttributeValue for a key attribute cannot contain an \
                     empty string value. Key: {attr_name}"
                )));
            }
            true
        }
        (AttributeValue::N(_), ScalarAttributeType::N) => true,
        (AttributeValue::B(b), ScalarAttributeType::B) => {
            if b.is_empty() {
                return Err(DynoxideError::ValidationException(format!(
                    "One or more parameter values were invalid: \
                     The AttributeValue for a key attribute cannot contain an \
                     empty binary value. Key: {attr_name}"
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

/// Return the byte size of a key attribute value for size limit checks.
fn key_attribute_size(val: &AttributeValue) -> usize {
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
    /// All expression parameter names that could carry expressions
    /// (used to build the "is null" message for ExpressionAttributeValues)
    pub all_expression_param_names: Vec<&'a str>,
    pub expression_attribute_names: &'a Option<HashMap<String, String>>,
    pub expression_attribute_values: &'a Option<HashMap<String, AttributeValue>>,
    /// Raw JSON for ExpressionAttributeValues — when present, used for key
    /// validation and parsed with key-specific error messages.
    pub expression_attribute_values_raw: &'a Option<serde_json::Value>,
}

/// Validate expression vs non-expression parameter conflicts and basic
/// ExpressionAttributeNames/Values key format.
///
/// This performs the pre-checks that DynamoDB runs before table existence and
/// key schema validation.
///
/// Returns the parsed `ExpressionAttributeValues` if raw JSON was provided
/// and successfully parsed, so the caller can store it on the request.
pub fn validate_expression_params(
    ctx: &ExpressionParamContext<'_>,
) -> Result<Option<HashMap<String, AttributeValue>>> {
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
        return Err(DynoxideError::ValidationException(format!(
            "Can not use both expression and non-expression parameters in the same request: \
             Non-expression parameters: {{{non_expr}}} Expression parameters: {{{expr}}}"
        )));
    }

    // ExpressionAttributeNames without any expression
    if has_names && !has_expressions {
        return Err(DynoxideError::ValidationException(
            "ExpressionAttributeNames can only be specified when using expressions".to_string(),
        ));
    }

    // ExpressionAttributeValues without any expression
    if has_values && !has_expressions {
        let null_parts: Vec<String> = ctx
            .all_expression_param_names
            .iter()
            .map(|n| format!("{n} is null"))
            .collect();
        let suffix = if null_parts.is_empty() {
            String::new()
        } else {
            format!(": {}", null_parts.join(" and "))
        };
        return Err(DynoxideError::ValidationException(format!(
            "ExpressionAttributeValues can only be specified when using expressions{suffix}"
        )));
    }

    // Empty ExpressionAttributeNames
    if let Some(names) = ctx.expression_attribute_names {
        if names.is_empty() {
            return Err(DynoxideError::ValidationException(
                "ExpressionAttributeNames must not be empty".to_string(),
            ));
        }
        // Validate key format (must start with #)
        for key in names.keys() {
            if !key.starts_with('#') {
                return Err(DynoxideError::ValidationException(format!(
                    "ExpressionAttributeNames contains invalid key: Syntax error; key: \"{key}\""
                )));
            }
        }
    }

    // ExpressionAttributeValues — prefer raw JSON if available (for key-specific errors)
    if let Some(raw_val) = ctx.expression_attribute_values_raw {
        if let Some(obj) = raw_val.as_object() {
            if obj.is_empty() {
                return Err(DynoxideError::ValidationException(
                    "ExpressionAttributeValues must not be empty".to_string(),
                ));
            }
            // Validate key format (must start with :)
            for key in obj.keys() {
                if !key.starts_with(':') {
                    return Err(DynoxideError::ValidationException(format!(
                        "ExpressionAttributeValues contains invalid key: Syntax error; key: \"{key}\""
                    )));
                }
            }
            // Parse and validate values with key context
            let parsed = parse_expression_attribute_values_raw(raw_val)?;
            // Validate parsed values (NULL:false, empty sets, duplicates, numbers)
            for (key, value) in &parsed {
                validate_expression_attribute_value(key, value)?;
            }
            return Ok(Some(parsed));
        }
    } else if let Some(values) = ctx.expression_attribute_values {
        // Pre-parsed EAV (e.g. from MCP/library usage)
        if values.is_empty() {
            return Err(DynoxideError::ValidationException(
                "ExpressionAttributeValues must not be empty".to_string(),
            ));
        }
        for key in values.keys() {
            if !key.starts_with(':') {
                return Err(DynoxideError::ValidationException(format!(
                    "ExpressionAttributeValues contains invalid key: Syntax error; key: \"{key}\""
                )));
            }
        }
        for (key, value) in values {
            validate_expression_attribute_value(key, value)?;
        }
    }

    Ok(None)
}

/// Validate a single ExpressionAttributeValues entry.
fn validate_expression_attribute_value(key: &str, value: &AttributeValue) -> Result<()> {
    // Check for empty/unsupported datatypes
    match value {
        AttributeValue::NULL(b) if !b => {
            return Err(DynoxideError::ValidationException(format!(
                "ExpressionAttributeValues contains invalid value: \
                 One or more parameter values were invalid: \
                 Null attribute value types must have the value of true for key {key}"
            )));
        }
        AttributeValue::SS(set) if set.is_empty() => {
            return Err(DynoxideError::ValidationException(format!(
                "ExpressionAttributeValues contains invalid value: \
                 One or more parameter values were invalid: \
                 An string set  may not be empty for key {key}"
            )));
        }
        AttributeValue::NS(set) if set.is_empty() => {
            return Err(DynoxideError::ValidationException(format!(
                "ExpressionAttributeValues contains invalid value: \
                 One or more parameter values were invalid: \
                 An number set  may not be empty for key {key}"
            )));
        }
        AttributeValue::BS(set) if set.is_empty() => {
            return Err(DynoxideError::ValidationException(format!(
                "ExpressionAttributeValues contains invalid value: \
                 One or more parameter values were invalid: \
                 Binary sets should not be empty for key {key}"
            )));
        }
        AttributeValue::SS(set) => {
            let mut seen = std::collections::HashSet::new();
            for s in set {
                if !seen.insert(s.clone()) {
                    let display: Vec<&str> = set.iter().map(|s| s.as_str()).collect();
                    return Err(DynoxideError::ValidationException(format!(
                        "ExpressionAttributeValues contains invalid value: \
                         One or more parameter values were invalid: \
                         Input collection [{}] contains duplicates. for key {key}",
                        display.join(", ")
                    )));
                }
            }
        }
        AttributeValue::BS(set) => {
            let mut seen = std::collections::HashSet::new();
            for b in set {
                if !seen.insert(b.clone()) {
                    use base64::Engine;
                    let display: Vec<String> = set
                        .iter()
                        .map(|s| base64::engine::general_purpose::STANDARD.encode(s))
                        .collect();
                    return Err(DynoxideError::ValidationException(format!(
                        "ExpressionAttributeValues contains invalid value: \
                         One or more parameter values were invalid: \
                         Input collection [{}]of type BS contains duplicates. for key {key}",
                        display.join(", ")
                    )));
                }
            }
        }
        AttributeValue::N(n) => {
            crate::types::validate_dynamo_number(n).map_err(|e| {
                let inner = match &e {
                    DynoxideError::ValidationException(m) => m.clone(),
                    _ => e.to_string(),
                };
                DynoxideError::ValidationException(format!(
                    "ExpressionAttributeValues contains invalid value: {inner} for key {key}"
                ))
            })?;
        }
        AttributeValue::NS(set) if !set.is_empty() => {
            for n in set {
                crate::types::validate_dynamo_number(n).map_err(|e| {
                    let inner = match &e {
                        DynoxideError::ValidationException(m) => m.clone(),
                        _ => e.to_string(),
                    };
                    DynoxideError::ValidationException(format!(
                        "ExpressionAttributeValues contains invalid value: {inner} for key {key}"
                    ))
                })?;
            }
            // Check for duplicates
            let mut seen = std::collections::HashSet::new();
            for n in set {
                let normalized = crate::types::normalize_dynamo_number(n);
                if !seen.insert(normalized) {
                    return Err(DynoxideError::ValidationException(format!(
                        "ExpressionAttributeValues contains invalid value: \
                         Input collection contains duplicates for key {key}"
                    )));
                }
            }
        }
        _ => {}
    }
    Ok(())
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
) -> Result<HashMap<String, AttributeValue>> {
    let obj = raw.as_object().ok_or_else(|| {
        DynoxideError::SerializationException(
            "Start of structure or map found where not expected".to_string(),
        )
    })?;

    let mut result = HashMap::new();
    for (key, value) in obj {
        match serde_json::from_value::<AttributeValue>(value.clone()) {
            Ok(av) => {
                result.insert(key.clone(), av);
            }
            Err(e) => {
                let msg = e.to_string();
                // Extract the core error message (strip serde position suffix)
                let clean = if let Some(idx) = msg.rfind(" at line ") {
                    let suffix = &msg[idx..];
                    if suffix.contains("column") {
                        &msg[..idx]
                    } else {
                        &msg
                    }
                } else {
                    &msg
                };
                // Strip VALIDATION: prefix if present
                let inner = clean.strip_prefix("VALIDATION:").unwrap_or(clean);

                if inner.contains("empty AttributeValue")
                    || (inner.contains("Supplied AttributeValue") && inner.contains("empty"))
                {
                    return Err(DynoxideError::ValidationException(format!(
                        "ExpressionAttributeValues contains invalid value: \
                         Supplied AttributeValue is empty, must contain exactly one of the \
                         supported datatypes for key {key}"
                    )));
                } else if inner.contains("more than one datatypes") {
                    return Err(DynoxideError::ValidationException(format!(
                        "ExpressionAttributeValues contains invalid value: \
                         Supplied AttributeValue has more than one datatypes set, \
                         must contain exactly one of the supported datatypes for key {key}"
                    )));
                } else if inner.contains("cannot be converted to a numeric value")
                    || inner.contains("significant digits")
                    || inner.contains("Number overflow")
                    || inner.contains("Number underflow")
                {
                    return Err(DynoxideError::ValidationException(format!(
                        "ExpressionAttributeValues contains invalid value: \
                         {inner} for key {key}"
                    )));
                } else {
                    return Err(DynoxideError::SerializationException(msg));
                }
            }
        }
    }
    Ok(result)
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
pub fn build_item_collection_metrics(
    storage: &Storage,
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

    let mut partition_bytes = storage.get_partition_size(table_name, pk_str)?;

    // Include LSI table sizes — DynamoDB's 10GB item collection limit applies
    // to the aggregate across base table and all LSIs.
    if let Some(ref lsi_json) = meta.lsi_definitions {
        if let Ok(lsis) = serde_json::from_str::<Vec<crate::types::LocalSecondaryIndex>>(lsi_json) {
            for lsi in &lsis {
                let lsi_size =
                    storage.get_lsi_partition_size(table_name, &lsi.index_name, pk_str)?;
                partition_bytes += lsi_size;
            }
        }
    }

    let size_gb = partition_bytes as f64 / (1024.0 * 1024.0 * 1024.0);

    let mut key_map = HashMap::new();
    key_map.insert(pk_attr.to_string(), pk_value.clone());

    Ok(Some(crate::types::ItemCollectionMetrics {
        item_collection_key: key_map,
        size_estimate_range_gb: vec![size_gb, size_gb],
    }))
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
/// and matching `ExpressionAttributeValues`.
///
/// Returns `(condition_expression, expression_attribute_values)` or an error.
pub fn convert_expected_to_condition(
    expected: &HashMap<String, ExpectedCondition>,
    conditional_operator: Option<&str>,
) -> Result<(String, HashMap<String, crate::types::AttributeValue>)> {
    let joiner = match conditional_operator {
        Some(op) if op.eq_ignore_ascii_case("OR") => " OR ",
        _ => " AND ",
    };
    let mut parts = Vec::new();
    let mut values = HashMap::new();
    let mut val_idx = 0u32;

    for (attr_name, cond) in expected {
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
                if let Some(ref avl) = cond.attribute_value_list {
                    if avl.len() == 1 {
                        return Some(avl[0].clone());
                    }
                }
                cond.value.clone()
            };

            match comp_upper.as_str() {
                "NULL" => {
                    parts.push(format!("attribute_not_exists(#expected_{attr_name})"));
                }
                "NOT_NULL" => {
                    parts.push(format!("attribute_exists(#expected_{attr_name})"));
                }
                "EQ" => {
                    if let Some(val) = single_val() {
                        let val_name = format!(":expected_v{val_idx}");
                        val_idx += 1;
                        parts.push(format!("#expected_{attr_name} = {val_name}"));
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
                        parts.push(format!("#expected_{attr_name} <> {val_name}"));
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
                        parts.push(format!("#expected_{attr_name} <= {val_name}"));
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
                        parts.push(format!("#expected_{attr_name} < {val_name}"));
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
                        parts.push(format!("#expected_{attr_name} >= {val_name}"));
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
                        parts.push(format!("#expected_{attr_name} > {val_name}"));
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
                        parts.push(format!("#expected_{attr_name} BETWEEN {v1} AND {v2}"));
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
                        parts.push(format!("begins_with(#expected_{attr_name}, {val_name})"));
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
                        parts.push(format!("contains(#expected_{attr_name}, {val_name})"));
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
                        parts.push(format!("NOT contains(#expected_{attr_name}, {val_name})"));
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
                        parts.push(format!(
                            "#expected_{attr_name} IN ({})",
                            val_names.join(", ")
                        ));
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
            parts.push(format!("attribute_not_exists(#expected_{attr_name})"));
        } else if let Some(ref value) = cond.value {
            // { Value: ... } (with Exists: true or Exists omitted) => attr = value
            let val_name = format!(":expected_v{val_idx}");
            val_idx += 1;
            parts.push(format!("#expected_{attr_name} = {val_name}"));
            values.insert(val_name, value.clone());
        } else {
            // { Exists: true } without Value => attribute_exists
            parts.push(format!("attribute_exists(#expected_{attr_name})"));
        }
    }

    if parts.is_empty() {
        return Ok((String::new(), values));
    }

    // Sort parts for deterministic output (attribute names are unordered in HashMap)
    parts.sort();
    Ok((parts.join(joiner), values))
}

/// Build ExpressionAttributeNames for the Expected conversion.
/// Maps `#expected_<attr>` to `<attr>` for each attribute in the Expected map.
pub fn expected_attr_names(
    expected: &HashMap<String, ExpectedCondition>,
) -> HashMap<String, String> {
    expected
        .keys()
        .map(|attr| (format!("#expected_{attr}"), attr.clone()))
        .collect()
}

/// Legacy `Expected` condition for a single attribute.
#[derive(Debug, Clone, Default, serde::Deserialize)]
pub struct ExpectedCondition {
    #[serde(rename = "Value", default)]
    pub value: Option<crate::types::AttributeValue>,
    #[serde(rename = "Exists", default)]
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
                        if arg_count == 0 {
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
                    if arg_count == 0 && !has_value {
                        return Err(DynoxideError::ValidationException(format!(
                            "One or more parameter values were invalid: \
                             Value or AttributeValueList must be used with ComparisonOperator: {op} for Attribute: {attr_name}"
                        )));
                    }
                }
            }

            // AttributeValueList values must be of the same type
            if let Some(ref avl) = cond.attribute_value_list {
                if avl.len() > 1 {
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
            }

            // BETWEEN order validation
            if op == "BETWEEN" && arg_count == 2 {
                if let Some(ref avl) = cond.attribute_value_list {
                    if compare_attribute_values_for_between(&avl[0], &avl[1])
                        == std::cmp::Ordering::Greater
                    {
                        return Err(DynoxideError::ValidationException(
                            "The BETWEEN condition was provided a range where the lower bound is greater than the upper bound"
                                .to_string(),
                        ));
                    }
                }
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

/// Convert legacy `KeyConditions` to a KeyConditionExpression + ExpressionAttributeValues.
///
/// If `partition_key_name` is provided, the partition key condition is placed
/// first in the expression so that the key condition parser identifies it
/// correctly (important when both hash and range use EQ).
pub fn convert_key_conditions(
    key_conditions: &HashMap<String, KeyCondition>,
    partition_key_name: Option<&str>,
) -> Result<ConvertedCondition> {
    let mut parts = Vec::new();
    let mut values = HashMap::new();
    let mut names = HashMap::new();
    let mut val_idx = 0u32;

    // Ensure deterministic ordering: partition key first, then remaining keys sorted.
    let mut ordered_keys: Vec<&String> = key_conditions.keys().collect();
    ordered_keys.sort();
    if let Some(pk) = partition_key_name {
        if let Some(pos) = ordered_keys.iter().position(|k| k.as_str() == pk) {
            let removed = ordered_keys.remove(pos);
            ordered_keys.insert(0, removed);
        }
    }

    for attr_name in ordered_keys {
        let cond = &key_conditions[attr_name];
        let name_ref = format!("#kc_{attr_name}");
        names.insert(name_ref.clone(), attr_name.clone());

        let comp_op = cond.comparison_operator.to_uppercase();
        match comp_op.as_str() {
            "EQ" => {
                if let Some(ref list) = cond.attribute_value_list {
                    if list.len() == 1 {
                        let val_name = format!(":kc_v{val_idx}");
                        val_idx += 1;
                        parts.push(format!("{name_ref} = {val_name}"));
                        values.insert(val_name, list[0].clone());
                    }
                }
            }
            "LE" => {
                if let Some(ref list) = cond.attribute_value_list {
                    if list.len() == 1 {
                        let val_name = format!(":kc_v{val_idx}");
                        val_idx += 1;
                        parts.push(format!("{name_ref} <= {val_name}"));
                        values.insert(val_name, list[0].clone());
                    }
                }
            }
            "LT" => {
                if let Some(ref list) = cond.attribute_value_list {
                    if list.len() == 1 {
                        let val_name = format!(":kc_v{val_idx}");
                        val_idx += 1;
                        parts.push(format!("{name_ref} < {val_name}"));
                        values.insert(val_name, list[0].clone());
                    }
                }
            }
            "GE" => {
                if let Some(ref list) = cond.attribute_value_list {
                    if list.len() == 1 {
                        let val_name = format!(":kc_v{val_idx}");
                        val_idx += 1;
                        parts.push(format!("{name_ref} >= {val_name}"));
                        values.insert(val_name, list[0].clone());
                    }
                }
            }
            "GT" => {
                if let Some(ref list) = cond.attribute_value_list {
                    if list.len() == 1 {
                        let val_name = format!(":kc_v{val_idx}");
                        val_idx += 1;
                        parts.push(format!("{name_ref} > {val_name}"));
                        values.insert(val_name, list[0].clone());
                    }
                }
            }
            "BETWEEN" => {
                if let Some(ref list) = cond.attribute_value_list {
                    if list.len() == 2 {
                        let v1 = format!(":kc_v{val_idx}");
                        val_idx += 1;
                        let v2 = format!(":kc_v{val_idx}");
                        val_idx += 1;
                        parts.push(format!("{name_ref} BETWEEN {v1} AND {v2}"));
                        values.insert(v1, list[0].clone());
                        values.insert(v2, list[1].clone());
                    }
                }
            }
            "BEGINS_WITH" => {
                if let Some(ref list) = cond.attribute_value_list {
                    if list.len() == 1 {
                        let val_name = format!(":kc_v{val_idx}");
                        val_idx += 1;
                        parts.push(format!("begins_with({name_ref}, {val_name})"));
                        values.insert(val_name, list[0].clone());
                    }
                }
            }
            _ => {
                return Err(DynoxideError::ValidationException(format!(
                    "Unsupported KeyConditions ComparisonOperator: {comp_op}"
                )));
            }
        }
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
pub fn convert_filter_conditions(
    conditions: &HashMap<String, FilterCondition>,
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

    for (attr_name, cond) in conditions {
        let name_ref = format!("#qf_{attr_name}");
        names.insert(name_ref.clone(), attr_name.clone());

        let comp_op = cond.comparison_operator.to_uppercase();
        match comp_op.as_str() {
            "NULL" => {
                parts.push(format!("attribute_not_exists({name_ref})"));
            }
            "NOT_NULL" => {
                parts.push(format!("attribute_exists({name_ref})"));
            }
            "EQ" | "NE" | "LE" | "LT" | "GE" | "GT" => {
                let op_str = match comp_op.as_str() {
                    "EQ" => "=",
                    "NE" => "<>",
                    "LE" => "<=",
                    "LT" => "<",
                    "GE" => ">=",
                    "GT" => ">",
                    _ => unreachable!(),
                };
                if let Some(ref list) = cond.attribute_value_list {
                    if list.len() == 1 {
                        let val_name = format!(":qf_v{val_idx}");
                        val_idx += 1;
                        parts.push(format!("{name_ref} {op_str} {val_name}"));
                        values.insert(val_name, list[0].clone());
                    }
                }
            }
            "BETWEEN" => {
                if let Some(ref list) = cond.attribute_value_list {
                    if list.len() == 2 {
                        let v1 = format!(":qf_v{val_idx}");
                        val_idx += 1;
                        let v2 = format!(":qf_v{val_idx}");
                        val_idx += 1;
                        parts.push(format!("{name_ref} BETWEEN {v1} AND {v2}"));
                        values.insert(v1, list[0].clone());
                        values.insert(v2, list[1].clone());
                    }
                }
            }
            "BEGINS_WITH" => {
                if let Some(ref list) = cond.attribute_value_list {
                    if list.len() == 1 {
                        let val_name = format!(":qf_v{val_idx}");
                        val_idx += 1;
                        parts.push(format!("begins_with({name_ref}, {val_name})"));
                        values.insert(val_name, list[0].clone());
                    }
                }
            }
            "CONTAINS" => {
                if let Some(ref list) = cond.attribute_value_list {
                    if list.len() == 1 {
                        let val_name = format!(":qf_v{val_idx}");
                        val_idx += 1;
                        parts.push(format!("contains({name_ref}, {val_name})"));
                        values.insert(val_name, list[0].clone());
                    }
                }
            }
            "NOT_CONTAINS" => {
                if let Some(ref list) = cond.attribute_value_list {
                    if list.len() == 1 {
                        let val_name = format!(":qf_v{val_idx}");
                        val_idx += 1;
                        parts.push(format!("NOT contains({name_ref}, {val_name})"));
                        values.insert(val_name, list[0].clone());
                    }
                }
            }
            "IN" => {
                if let Some(ref list) = cond.attribute_value_list {
                    if !list.is_empty() {
                        let val_names: Vec<String> = list
                            .iter()
                            .map(|v| {
                                let name = format!(":qf_v{val_idx}");
                                val_idx += 1;
                                values.insert(name.clone(), v.clone());
                                name
                            })
                            .collect();
                        parts.push(format!("{name_ref} IN ({})", val_names.join(", ")));
                    }
                }
            }
            _ => {}
        }
    }

    parts.sort();
    Ok(ConvertedCondition {
        expression: parts.join(joiner),
        attribute_values: values,
        attribute_names: names,
    })
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

/// Parse an `ExclusiveStartKey` from a raw `serde_json::Value`.
///
/// Validation errors are formatted to match DynamoDB's exact error messages:
/// - Empty/unsupported AV and multi-type: prefixed with "The provided starting key is invalid: "
/// - NULL:false, empty SS, empty BS: prefixed with "The provided starting key is invalid: One or more..."
/// - Empty NS, duplicate SS, duplicate BS: "One or more parameter values were invalid: ..." (no ESK prefix)
/// - Number errors: raw error (no prefix)
pub fn parse_exclusive_start_key(
    value: &serde_json::Value,
) -> Result<HashMap<String, AttributeValue>> {
    // First, try to deserialise — this catches multi-type and number errors
    let parsed = match serde_json::from_value::<HashMap<String, AttributeValue>>(value.clone()) {
        Ok(map) => map,
        Err(e) => {
            let msg = e.to_string();
            // Strip serde position suffix
            let clean = if let Some(idx) = msg.rfind(" at line ") {
                let suffix = &msg[idx..];
                if suffix.contains("column") {
                    &msg[..idx]
                } else {
                    &msg
                }
            } else {
                &msg
            };
            let inner = clean.strip_prefix("VALIDATION:").unwrap_or(clean);

            if inner.contains("empty AttributeValue")
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
                return Err(DynoxideError::ValidationException(
                    "The provided starting key is invalid: \
                     One or more parameter values were invalid: \
                     Null attribute value types must have the value of true"
                        .to_string(),
                ));
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
            AttributeValue::SS(set) => {
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
            AttributeValue::BS(set) => {
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
            AttributeValue::NS(set) if !set.is_empty() => {
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
            _ => {}
        }
    }

    Ok(parsed)
}

/// Validate ESK count and index key types (stages 1 and 2 of ESK validation).
///
/// Stage 1: Count check — error uses `count_mismatch_msg`.
/// Stage 2: Index key type check (if index) — missing attr returns
/// "The provided starting key is invalid", wrong type returns
/// "The provided key element does not match the schema".
///
/// Call `validate_esk_table_keys` separately afterward.
pub fn validate_esk_count_and_index_keys(
    esk: &HashMap<String, AttributeValue>,
    meta: &TableMetadata,
    index_name: Option<&str>,
    count_mismatch_msg: &str,
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
            if let Some(def) = attr_defs.iter().find(|d| d.attribute_name == *attr) {
                if !attr_value_matches_scalar_type(val, &def.attribute_type) {
                    return Err(DynoxideError::ValidationException(
                        "The provided key element does not match the schema".to_string(),
                    ));
                }
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
        if let Some(val) = table_key_subset.get(&key_elem.attribute_name) {
            if let Some(def) = attr_defs
                .iter()
                .find(|d| d.attribute_name == key_elem.attribute_name)
            {
                if !attr_value_matches_scalar_type(val, &def.attribute_type) {
                    return Err(DynoxideError::ValidationException(
                        "The provided starting key is invalid: \
                         The provided key element does not match the schema"
                            .to_string(),
                    ));
                }
            }
        }
    }

    Ok(())
}

/// Get the key schema elements for a GSI or LSI by index name.
fn get_index_key_schema(meta: &TableMetadata, index_name: &str) -> Result<Vec<KeySchemaElement>> {
    // Check LSIs first
    if let Some(ref lsi_json) = meta.lsi_definitions {
        if let Ok(lsis) = serde_json::from_str::<Vec<crate::types::LocalSecondaryIndex>>(lsi_json) {
            for lsi in &lsis {
                if lsi.index_name == index_name {
                    return Ok(lsi.key_schema.clone());
                }
            }
        }
    }
    // Check GSIs
    if let Some(ref gsi_json) = meta.gsi_definitions {
        if let Ok(gsis) = serde_json::from_str::<Vec<crate::types::GlobalSecondaryIndex>>(gsi_json)
        {
            for gsi in &gsis {
                if gsi.index_name == index_name {
                    return Ok(gsi.key_schema.clone());
                }
            }
        }
    }
    // Index not found — the caller should already have validated this
    Ok(Vec::new())
}

/// Check whether an `AttributeValue` matches a `ScalarAttributeType`.
fn attr_value_matches_scalar_type(val: &AttributeValue, expected: &ScalarAttributeType) -> bool {
    matches!(
        (val, expected),
        (AttributeValue::S(_), ScalarAttributeType::S)
            | (AttributeValue::N(_), ScalarAttributeType::N)
            | (AttributeValue::B(_), ScalarAttributeType::B)
    )
}

/// Validate argument counts and type compatibility for each `ComparisonOperator`
/// in a raw `QueryFilter`, `ScanFilter`, or `KeyConditions` JSON map.
///
/// DynamoDB validates these BEFORE ExclusiveStartKey and expression syntax.
pub fn validate_filter_condition_args(value: Option<&serde_json::Value>) -> Result<()> {
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
            _ => continue,
        };

        if avl_len < min || avl_len > max {
            return Err(DynoxideError::ValidationException(format!(
                "One or more parameter values were invalid: Invalid number of argument(s) for the {} ComparisonOperator",
                comp_op
            )));
        }

        // Type compatibility validation
        if let Some(arr) = avl {
            if !arr.is_empty() {
                let first_type = attr_value_type_name_from_json(&arr[0]);
                if let Some(type_name) = first_type {
                    validate_comparison_type_compat(comp_op, type_name)?;
                }
            }
        }
    }

    Ok(())
}

/// Extract the DynamoDB type name (S, N, B, BOOL, NULL, SS, NS, BS, M, L)
/// from a raw JSON AttributeValue object.
fn attr_value_type_name_from_json(val: &serde_json::Value) -> Option<&str> {
    val.as_object().and_then(|obj| {
        for key in &["S", "N", "B", "BOOL", "NULL", "SS", "NS", "BS", "M", "L"] {
            if obj.contains_key(*key) {
                return Some(*key);
            }
        }
        None
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
) -> Result<()> {
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

        // Validate AttributeValueList entries
        if let Some(avl) = cond_obj.get("AttributeValueList") {
            if let Some(arr) = avl.as_array() {
                for av_val in arr {
                    // Try to deserialize each AttributeValue to trigger validation
                    match serde_json::from_value::<AttributeValue>(av_val.clone()) {
                        Err(e) => {
                            let msg = e.to_string();
                            // Strip serde position suffix
                            let clean = if let Some(idx) = msg.rfind(" at line ") {
                                let suffix = &msg[idx..];
                                if suffix.contains("column") {
                                    &msg[..idx]
                                } else {
                                    &msg
                                }
                            } else {
                                &msg
                            };
                            let inner = clean.strip_prefix("VALIDATION:").unwrap_or(clean);

                            if inner.contains("empty AttributeValue")
                                || (inner.contains("Supplied AttributeValue")
                                    && inner.contains("empty"))
                            {
                                return Err(DynoxideError::ValidationException(
                                    "Supplied AttributeValue is empty, must contain exactly one of the supported datatypes".to_string()
                                ));
                            } else if inner.contains("more than one datatypes") {
                                return Err(DynoxideError::ValidationException(
                                    "Supplied AttributeValue has more than one datatypes set, \
                                     must contain exactly one of the supported datatypes"
                                        .to_string(),
                                ));
                            } else if inner.contains("cannot be converted to a numeric value")
                                || inner.contains("significant digits")
                                || inner.contains("Number overflow")
                                || inner.contains("Number underflow")
                            {
                                return Err(DynoxideError::ValidationException(inner.to_string()));
                            }
                            // Let other errors pass through for now
                        }
                        Ok(av) => {
                            // Validate parsed value (NULL:false, empty sets, duplicates, numbers)
                            validate_filter_attribute_value(&av)?;
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

/// Validate a single attribute value inside a filter condition (QueryFilter,
/// ScanFilter, KeyConditions). Error messages use the
/// "One or more parameter values were invalid: " prefix.
fn validate_filter_attribute_value(value: &AttributeValue) -> Result<()> {
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
        AttributeValue::SS(set) => {
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
            Ok(())
        }
        AttributeValue::BS(set) => {
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
            Ok(())
        }
        AttributeValue::N(n) => {
            crate::types::validate_dynamo_number(n)?;
            Ok(())
        }
        AttributeValue::NS(set) if !set.is_empty() => {
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
            Ok(())
        }
        _ => Ok(()),
    }
}
