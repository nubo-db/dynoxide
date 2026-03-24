use crate::actions::helpers;
use crate::errors::{DynoxideError, Result};
use crate::storage::Storage;
use crate::types::{self, AttributeValue, Item};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Internal deserialization struct that uses Options to detect missing fields.
#[derive(Debug, Default, Deserialize)]
struct PutItemRequestRaw {
    #[serde(rename = "TableName", default)]
    table_name: Option<String>,
    #[serde(rename = "Item", default)]
    item: Option<HashMap<String, AttributeValue>>,
    #[serde(rename = "ReturnValues", default)]
    return_values: Option<String>,
    #[serde(rename = "ConditionExpression", default)]
    condition_expression: Option<String>,
    #[serde(rename = "ExpressionAttributeNames", default)]
    expression_attribute_names: Option<HashMap<String, String>>,
    #[serde(rename = "ExpressionAttributeValues", default)]
    expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    #[serde(rename = "ReturnConsumedCapacity", default)]
    return_consumed_capacity: Option<String>,
    #[serde(rename = "ReturnValuesOnConditionCheckFailure", default)]
    return_values_on_condition_check_failure: Option<String>,
    #[serde(rename = "ReturnItemCollectionMetrics", default)]
    return_item_collection_metrics: Option<String>,
    #[serde(rename = "Expected", default)]
    expected: Option<serde_json::Value>,
    #[serde(rename = "ConditionalOperator", default)]
    conditional_operator: Option<String>,
}

#[derive(Debug, Default)]
pub struct PutItemRequest {
    pub table_name: String,
    pub item: HashMap<String, AttributeValue>,
    pub return_values: Option<String>,
    pub condition_expression: Option<String>,
    pub expression_attribute_names: Option<HashMap<String, String>>,
    pub expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    pub return_consumed_capacity: Option<String>,
    pub return_values_on_condition_check_failure: Option<String>,
    pub return_item_collection_metrics: Option<String>,
    pub expected: Option<serde_json::Value>,
    pub conditional_operator: Option<String>,
}

impl<'de> serde::Deserialize<'de> for PutItemRequest {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let raw = PutItemRequestRaw::deserialize(deserializer)?;

        use crate::validation::{format_validation_errors, table_name_constraint_errors};

        // Collect constraint validation errors (DynamoDB checks all at once)
        let mut errors = Vec::new();
        let table_name_opt = raw.table_name.as_deref();

        errors.extend(table_name_constraint_errors(table_name_opt));
        let table_name = raw.table_name.unwrap_or_default();

        // Item constraint validation
        if raw.item.is_none() {
            errors.push(
                "Value null at 'item' failed to satisfy constraint: \
                 Member must not be null"
                    .to_string(),
            );
        }

        // ReturnConsumedCapacity enum validation
        if let Some(ref rcc) = raw.return_consumed_capacity {
            if !["INDEXES", "TOTAL", "NONE"].contains(&rcc.as_str()) {
                errors.push(format!(
                    "Value '{}' at 'returnConsumedCapacity' failed to satisfy constraint: \
                     Member must satisfy enum value set: [INDEXES, TOTAL, NONE]",
                    rcc
                ));
            }
        }

        // ReturnValues enum validation
        if let Some(ref rv) = raw.return_values {
            if !["ALL_NEW", "UPDATED_OLD", "ALL_OLD", "NONE", "UPDATED_NEW"].contains(&rv.as_str())
            {
                errors.push(format!(
                    "Value '{}' at 'returnValues' failed to satisfy constraint: \
                     Member must satisfy enum value set: \
                     [ALL_NEW, UPDATED_OLD, ALL_OLD, NONE, UPDATED_NEW]",
                    rv
                ));
            }
        }

        // ReturnItemCollectionMetrics enum validation
        if let Some(ref ricm) = raw.return_item_collection_metrics {
            if !["SIZE", "NONE"].contains(&ricm.as_str()) {
                errors.push(format!(
                    "Value '{}' at 'returnItemCollectionMetrics' failed to satisfy constraint: \
                     Member must satisfy enum value set: [SIZE, NONE]",
                    ricm
                ));
            }
        }

        if let Some(msg) = format_validation_errors(&errors) {
            return Err(serde::de::Error::custom(format!("VALIDATION:{}", msg)));
        }

        Ok(PutItemRequest {
            table_name,
            item: raw.item.unwrap_or_default(),
            return_values: raw.return_values,
            condition_expression: raw.condition_expression,
            expression_attribute_names: raw.expression_attribute_names,
            expression_attribute_values: raw.expression_attribute_values,
            return_consumed_capacity: raw.return_consumed_capacity,
            return_values_on_condition_check_failure: raw.return_values_on_condition_check_failure,
            return_item_collection_metrics: raw.return_item_collection_metrics,
            expected: raw.expected,
            conditional_operator: raw.conditional_operator,
        })
    }
}

#[derive(Debug, Default, Serialize)]
pub struct PutItemResponse {
    #[serde(rename = "Attributes", skip_serializing_if = "Option::is_none")]
    pub attributes: Option<HashMap<String, AttributeValue>>,
    #[serde(rename = "ConsumedCapacity", skip_serializing_if = "Option::is_none")]
    pub consumed_capacity: Option<types::ConsumedCapacity>,
    #[serde(
        rename = "ItemCollectionMetrics",
        skip_serializing_if = "Option::is_none"
    )]
    pub item_collection_metrics: Option<crate::types::ItemCollectionMetrics>,
}

pub fn execute(storage: &Storage, mut request: PutItemRequest) -> Result<PutItemResponse> {
    // Validate table name format before checking existence (DynamoDB validates input first)
    crate::validation::validate_table_name(&request.table_name)?;

    // Validate attribute values (empty strings, empty sets, number precision)
    // DynamoDB validates item values before expression parameter checks for PutItem.
    crate::validation::validate_item_attribute_values(&request.item)?;

    // Validate expression/non-expression parameter conflicts
    {
        let mut non_expr = Vec::new();
        let mut expr_params = Vec::new();
        if request.expected.is_some() {
            non_expr.push("Expected");
        }
        if request.condition_expression.is_some() {
            expr_params.push("ConditionExpression");
        }
        let ctx = helpers::ExpressionParamContext {
            non_expression_params: non_expr,
            expression_params: expr_params,
            all_expression_param_names: vec!["ConditionExpression"],
            expression_attribute_names: &request.expression_attribute_names,
            expression_attribute_values: &request.expression_attribute_values,
            expression_attribute_values_raw: &None,
        };
        helpers::validate_expression_params(&ctx)?; // Raw EAV not used for PutItem HTTP path
    }

    // Validate empty ConditionExpression
    if let Some(ref ce) = request.condition_expression {
        if ce.is_empty() {
            return Err(DynoxideError::ValidationException(
                "Invalid ConditionExpression: The expression can not be empty;".to_string(),
            ));
        }
    }

    // Statically validate ConditionExpression (syntax + BETWEEN bounds, etc.) before table lookup
    if let Some(ref ce) = request.condition_expression {
        let parsed = crate::expressions::condition::parse(ce).map_err(|e| {
            DynoxideError::ValidationException(format!("Invalid ConditionExpression: {e}"))
        })?;
        crate::expressions::condition::validate_static(
            &parsed,
            &request.expression_attribute_values,
        )
        .map_err(DynoxideError::ValidationException)?;
    }

    // Validate ReturnValues parameter (PutItem only supports NONE and ALL_OLD)
    if let Some(ref rv) = request.return_values {
        let rv_upper = rv.to_uppercase();
        if rv_upper != "NONE" && rv_upper != "ALL_OLD" {
            return Err(DynoxideError::ValidationException(
                "ReturnValues can only be ALL_OLD or NONE".to_string(),
            ));
        }
    }

    // Validate legacy Expected parameter BEFORE checking table existence
    // (DynamoDB validates request parameters before checking table)
    if request.condition_expression.is_none() {
        if let Some(ref expected_val) = request.expected {
            if let Ok(expected) = serde_json::from_value::<
                HashMap<String, helpers::ExpectedCondition>,
            >(expected_val.clone())
            {
                // Validate Expected conditions (ComparisonOperator, Value, Exists conflicts)
                helpers::validate_expected_conditions(&expected)?;
            }
        }
    }

    // Validate item size BEFORE checking table existence
    // (DynamoDB validates item size before checking if table exists)
    let size = types::item_size(&request.item);
    if size > types::MAX_ITEM_SIZE {
        return Err(DynoxideError::ValidationException(
            "Item size has exceeded the maximum allowed size".to_string(),
        ));
    }

    let meta = helpers::require_table_for_item_op(storage, &request.table_name)?;
    let key_schema = helpers::parse_key_schema(&meta)?;

    // Convert legacy Expected parameter to ConditionExpression if no expression is set
    if request.condition_expression.is_none() {
        if let Some(ref expected_val) = request.expected {
            if let Ok(expected) = serde_json::from_value::<
                HashMap<String, helpers::ExpectedCondition>,
            >(expected_val.clone())
            {
                if !expected.is_empty() {
                    let (cond_expr, values) = helpers::convert_expected_to_condition(
                        &expected,
                        request.conditional_operator.as_deref(),
                    )?;
                    if !cond_expr.is_empty() {
                        let names = helpers::expected_attr_names(&expected);
                        request.condition_expression = Some(cond_expr);
                        let expr_values = request
                            .expression_attribute_values
                            .get_or_insert_with(HashMap::new);
                        expr_values.extend(values);
                        let expr_names = request
                            .expression_attribute_names
                            .get_or_insert_with(HashMap::new);
                        expr_names.extend(names);
                    }
                }
            }
        }
    }

    // Validate key attributes present and correct types
    helpers::validate_item_keys(&request.item, &key_schema, &meta)?;

    // Normalize sets (deduplication)
    crate::validation::normalize_item_sets(&mut request.item);

    // Extract key values
    let (pk, sk) = helpers::extract_key_strings(&request.item, &key_schema)?;

    // Check for unused expression attribute names/values
    let tracker = crate::expressions::TrackedExpressionAttributes::new(
        &request.expression_attribute_names,
        &request.expression_attribute_values,
    );

    // Pre-register all expression references statically so check_unused sees
    // every :value and #name, even those in short-circuited AND/OR branches.
    if let Some(ref cond_expr) = request.condition_expression {
        if let Ok(parsed) = crate::expressions::condition::parse(cond_expr) {
            tracker.track_condition_expr(&parsed);
        }
    }

    // Wrap condition check + write in a transaction to prevent TOCTOU races
    let has_condition = request.condition_expression.is_some();
    if has_condition {
        storage.begin_transaction()?;
    }

    let conditional_result = (|| -> Result<(Option<String>, String)> {
        // Evaluate ConditionExpression against existing item (if any)
        let old_json = if request.condition_expression.is_some() {
            let existing_json = storage.get_item(&request.table_name, &pk, &sk)?;
            let existing_item: HashMap<String, AttributeValue> = existing_json
                .as_ref()
                .and_then(|j| serde_json::from_str(j).ok())
                .unwrap_or_default();

            if let Some(ref cond_expr) = request.condition_expression {
                let parsed = crate::expressions::condition::parse(cond_expr)
                    .map_err(DynoxideError::ValidationException)?;
                let result =
                    crate::expressions::condition::evaluate(&parsed, &existing_item, &tracker)
                        .map_err(DynoxideError::ValidationException)?;
                if !result {
                    let return_item = if request.return_values_on_condition_check_failure.as_deref()
                        == Some("ALL_OLD")
                        && !existing_item.is_empty()
                    {
                        Some(existing_item.clone())
                    } else {
                        None
                    };
                    return Err(DynoxideError::ConditionalCheckFailedException(
                        "The conditional request failed".to_string(),
                        return_item,
                    ));
                }
            }
            existing_json
        } else {
            None
        };

        // Check for unused expression attribute names/values
        tracker.check_unused()?;

        // Serialize item
        let item_json = serde_json::to_string(&request.item)
            .map_err(|e| DynoxideError::InternalServerError(e.to_string()))?;

        // Compute hash prefix for parallel scan ordering
        let hash_prefix = request
            .item
            .get(&key_schema.partition_key)
            .map(crate::storage::compute_hash_prefix)
            .unwrap_or_default();

        // Store item (returns old item if it existed)
        // If we already fetched old_json for condition check, use put_item but ignore its return
        let old_json = if old_json.is_some() {
            storage.put_item_with_hash(
                &request.table_name,
                &pk,
                &sk,
                &item_json,
                size,
                &hash_prefix,
            )?;
            old_json
        } else {
            storage.put_item_with_hash(
                &request.table_name,
                &pk,
                &sk,
                &item_json,
                size,
                &hash_prefix,
            )?
        };

        Ok((old_json, item_json))
    })();

    // Handle transaction commit/rollback
    if has_condition {
        match conditional_result {
            Ok(_) => storage.commit()?,
            Err(ref _e) => {
                let _ = storage.rollback();
            }
        }
    }

    let (old_json, _item_json) = conditional_result?;

    // Maintain GSI tables
    let gsi_units = super::gsi::maintain_gsis_after_write(
        storage,
        &request.table_name,
        &meta,
        &pk,
        &sk,
        &request.item,
        &key_schema.partition_key,
        key_schema.sort_key.as_deref(),
    )?;

    // Maintain LSI tables
    super::lsi::maintain_lsis_after_write(
        storage,
        &request.table_name,
        &meta,
        &pk,
        &sk,
        &request.item,
        &key_schema.partition_key,
        key_schema.sort_key.as_deref(),
    )?;

    // Record stream event
    let old_item_for_stream: Option<Item> =
        old_json.as_ref().and_then(|j| serde_json::from_str(j).ok());
    crate::streams::record_stream_event(
        storage,
        &meta,
        old_item_for_stream.as_ref(),
        Some(&request.item),
    )?;

    // Handle ReturnValues
    let return_old = request
        .return_values
        .as_deref()
        .unwrap_or("NONE")
        .eq_ignore_ascii_case("ALL_OLD");

    let attributes = if return_old {
        old_json
            .as_ref()
            .and_then(|json| serde_json::from_str::<Item>(json).ok())
    } else {
        None
    };

    // Build item collection metrics (only for tables with LSIs)
    let pk_value = request.item.get(&key_schema.partition_key).cloned();
    let item_collection_metrics = helpers::build_item_collection_metrics(
        storage,
        &meta,
        &request.table_name,
        &pk,
        &key_schema.partition_key,
        pk_value
            .as_ref()
            .unwrap_or(&AttributeValue::S(String::new())),
        &request.return_item_collection_metrics,
    )?;

    let consumed_capacity = types::consumed_capacity_with_indexes(
        &request.table_name,
        types::write_capacity_units(size),
        &gsi_units,
        &request.return_consumed_capacity,
    );

    Ok(PutItemResponse {
        attributes,
        consumed_capacity,
        item_collection_metrics,
    })
}
