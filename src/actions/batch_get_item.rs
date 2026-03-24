use crate::actions::helpers;
use crate::errors::{DynoxideError, Result};
use crate::expressions;
use crate::storage::Storage;
use crate::types::{AttributeValue, Item};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Default, Deserialize)]
pub struct BatchGetItemRequest {
    #[serde(rename = "RequestItems")]
    pub request_items: HashMap<String, KeysAndAttributes>,
    #[serde(rename = "ReturnConsumedCapacity", default)]
    pub return_consumed_capacity: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
pub struct KeysAndAttributes {
    #[serde(rename = "Keys")]
    pub keys: Vec<HashMap<String, AttributeValue>>,
    #[serde(rename = "ProjectionExpression", default)]
    pub projection_expression: Option<String>,
    #[serde(rename = "ExpressionAttributeNames", default)]
    pub expression_attribute_names: Option<HashMap<String, String>>,
    #[serde(rename = "ConsistentRead", default)]
    pub consistent_read: Option<bool>,
    #[serde(rename = "AttributesToGet", default)]
    pub attributes_to_get: Option<Vec<String>>,
}

#[derive(Debug, Default, Serialize)]
pub struct BatchGetItemResponse {
    #[serde(rename = "Responses")]
    pub responses: HashMap<String, Vec<Item>>,
    #[serde(rename = "UnprocessedKeys")]
    pub unprocessed_keys: HashMap<String, serde_json::Value>,
    #[serde(rename = "ConsumedCapacity", skip_serializing_if = "Option::is_none")]
    pub consumed_capacity: Option<Vec<crate::types::ConsumedCapacity>>,
}

pub fn execute(storage: &Storage, request: BatchGetItemRequest) -> Result<BatchGetItemResponse> {
    // Validate RequestItems is not empty
    if request.request_items.is_empty() {
        return Err(DynoxideError::ValidationException(
            "1 validation error detected: Value '{}' at 'requestItems' failed to satisfy constraint: Member must have length greater than or equal to 1".to_string(),
        ));
    }

    // Validate each table entry has at least one key
    for (table_name, ka) in &request.request_items {
        if ka.keys.is_empty() {
            return Err(DynoxideError::ValidationException(format!(
                "1 validation error detected: Value at 'requestItems.{table_name}.member.keys' failed to satisfy constraint: Member must have length greater than or equal to 1"
            )));
        }
    }

    // Validate table name format for all tables before checking existence
    for table_name in request.request_items.keys() {
        crate::validation::validate_table_name(table_name)?;
    }

    // Validate total key count
    let total_keys: usize = request.request_items.values().map(|ka| ka.keys.len()).sum();
    if total_keys > 100 {
        return Err(DynoxideError::ValidationException(
            "Too many items requested for the BatchGetItem call".to_string(),
        ));
    }

    // --- Pre-table validations ---
    // DynamoDB validates expression attributes, key values, projections, and duplicates
    // BEFORE checking table existence. Perform these checks first.
    for keys_and_attrs in request.request_items.values() {
        // Check AttributesToGet + expression conflict
        let has_attributes_to_get = keys_and_attrs.attributes_to_get.is_some();
        let has_projection_expr = keys_and_attrs.projection_expression.is_some();
        let has_expr_attr_names = keys_and_attrs.expression_attribute_names.is_some();

        if has_attributes_to_get && has_projection_expr {
            return Err(DynoxideError::ValidationException(
                "Can not use both expression and non-expression parameters in the same request: Non-expression parameters: {AttributesToGet} Expression parameters: {ProjectionExpression}".to_string(),
            ));
        }

        // ExpressionAttributeNames without expression
        if has_expr_attr_names && !has_projection_expr {
            return Err(DynoxideError::ValidationException(
                "ExpressionAttributeNames can only be specified when using expressions".to_string(),
            ));
        }

        // Empty ExpressionAttributeNames
        if let Some(ref ean) = keys_and_attrs.expression_attribute_names {
            if ean.is_empty() {
                return Err(DynoxideError::ValidationException(
                    "ExpressionAttributeNames must not be empty".to_string(),
                ));
            }
            // Invalid EAN keys (must start with #)
            for key in ean.keys() {
                if !key.starts_with('#') {
                    return Err(DynoxideError::ValidationException(format!(
                        "ExpressionAttributeNames contains invalid key: Syntax error; key: \"{key}\""
                    )));
                }
            }
        }

        // Empty ProjectionExpression
        if let Some(ref pe) = keys_and_attrs.projection_expression {
            if pe.is_empty() {
                return Err(DynoxideError::ValidationException(
                    "Invalid ProjectionExpression: The expression can not be empty;".to_string(),
                ));
            }
        }

        // Duplicate AttributesToGet check (must come before duplicate keys check)
        if let Some(ref atg) = keys_and_attrs.attributes_to_get {
            let mut seen = std::collections::HashSet::new();
            for attr in atg {
                if !seen.insert(attr.as_str()) {
                    return Err(DynoxideError::ValidationException(format!(
                        "One or more parameter values were invalid: Duplicate value in attribute name: {attr}"
                    )));
                }
            }
        }

        // Validate key attribute values (empty attrs, invalid numbers, etc.)
        for key in &keys_and_attrs.keys {
            crate::validation::validate_item_attribute_values(key)?;
        }

        // Duplicate keys check
        if keys_and_attrs.keys.len() > 1 {
            let serialised: Vec<String> = keys_and_attrs
                .keys
                .iter()
                .map(|k| {
                    let mut pairs: Vec<_> = k.iter().map(|(k, v)| format!("{k}={v:?}")).collect();
                    pairs.sort();
                    pairs.join(",")
                })
                .collect();
            let mut seen = std::collections::HashSet::new();
            for s in &serialised {
                if !seen.insert(s) {
                    return Err(DynoxideError::ValidationException(
                        "Provided list of item keys contains duplicates".to_string(),
                    ));
                }
            }
        }
    }

    const MAX_RESPONSE_SIZE: usize = 16 * 1024 * 1024; // 16MB

    let mut responses: HashMap<String, Vec<Item>> = HashMap::new();
    let mut unprocessed_keys: HashMap<String, serde_json::Value> = HashMap::new();
    let mut cumulative_size: usize = 0;
    let mut size_limit_reached = false;
    // Track per-key RCU for ConsumedCapacity (uses full item size, not projected)
    let mut table_rcu: HashMap<String, f64> = HashMap::new();

    for (table_name, keys_and_attrs) in &request.request_items {
        let meta = helpers::require_table_for_item_op(storage, table_name)?;
        let key_schema = helpers::parse_key_schema(&meta)?;

        // Parse projection if present; also handle legacy AttributesToGet
        let projection = if let Some(ref expr) = keys_and_attrs.projection_expression {
            Some(expressions::projection::parse(expr).map_err(DynoxideError::ValidationException)?)
        } else {
            keys_and_attrs
                .attributes_to_get
                .as_ref()
                .map(|attrs| crate::actions::helpers::attributes_to_get_to_projection(attrs))
        };

        let tracker = crate::expressions::TrackedExpressionAttributes::new(
            &keys_and_attrs.expression_attribute_names,
            &None, // BatchGetItem has no ExpressionAttributeValues
        );

        // Pre-register projection expression references
        if let Some(ref proj) = projection {
            tracker.track_projection_expr(proj);
        }

        // BatchGetItem does NOT automatically include key attributes in projections.
        let key_attrs = Vec::new();

        let consistent = keys_and_attrs.consistent_read.unwrap_or(false);
        let mut table_items = Vec::new();
        let mut remaining_keys: Vec<HashMap<String, AttributeValue>> = Vec::new();
        let mut per_table_rcu: f64 = 0.0;

        for key in &keys_and_attrs.keys {
            if size_limit_reached {
                remaining_keys.push(key.clone());
                continue;
            }

            helpers::validate_key_only(key, &key_schema)?;
            let (pk, sk) = helpers::extract_key_strings(key, &key_schema)?;

            if let Some(item_json) = storage.get_item(table_name, &pk, &sk)? {
                let item: Item = serde_json::from_str(&item_json).map_err(|e| {
                    DynoxideError::InternalServerError(format!("Bad item JSON: {e}"))
                })?;

                // Use full item size for both capacity and response limit
                let item_size = crate::types::item_size(&item);

                if cumulative_size + item_size > MAX_RESPONSE_SIZE {
                    size_limit_reached = true;
                    remaining_keys.push(key.clone());
                    continue;
                }

                cumulative_size += item_size;

                // RCU is based on full item size, not projected size
                per_table_rcu +=
                    crate::types::read_capacity_units_with_consistency(item_size, consistent);

                let result_item = if let Some(ref proj) = projection {
                    expressions::projection::apply(&item, proj, &tracker, &key_attrs)
                        .map_err(DynoxideError::ValidationException)?
                } else {
                    item
                };

                table_items.push(result_item);
            } else {
                // DynamoDB charges for the read attempt even if the item is not found
                per_table_rcu += crate::types::read_capacity_units_with_consistency(0, consistent);
            }
        }

        // Check for unused expression attribute names
        tracker.check_unused()?;

        table_rcu.insert(table_name.clone(), per_table_rcu);
        responses.insert(table_name.clone(), table_items);

        if !remaining_keys.is_empty() {
            let mut unprocessed = serde_json::json!({
                "Keys": remaining_keys,
            });
            // Preserve original request settings so the caller can retry
            // without losing projection or consistency configuration.
            if let Some(ref pe) = keys_and_attrs.projection_expression {
                unprocessed["ProjectionExpression"] = serde_json::json!(pe);
            }
            if let Some(ref ean) = keys_and_attrs.expression_attribute_names {
                unprocessed["ExpressionAttributeNames"] = serde_json::json!(ean);
            }
            if let Some(cr) = keys_and_attrs.consistent_read {
                unprocessed["ConsistentRead"] = serde_json::json!(cr);
            }
            unprocessed_keys.insert(table_name.clone(), unprocessed);
        }
    }

    // Build consumed capacity per table
    let consumed_capacity = if matches!(
        request.return_consumed_capacity.as_deref(),
        Some("TOTAL") | Some("INDEXES")
    ) {
        let mut caps = Vec::new();
        for table_name in request.request_items.keys() {
            let total_rcu = table_rcu.get(table_name).copied().unwrap_or(0.0);
            if let Some(cc) = crate::types::consumed_capacity(
                table_name,
                total_rcu,
                &request.return_consumed_capacity,
            ) {
                caps.push(cc);
            }
        }
        Some(caps)
    } else {
        None
    };

    Ok(BatchGetItemResponse {
        responses,
        unprocessed_keys,
        consumed_capacity,
    })
}
