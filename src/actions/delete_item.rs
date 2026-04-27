use crate::actions::helpers;
use crate::errors::{DynoxideError, Result};
use crate::storage::Storage;
use crate::types::{self, AttributeValue, Item};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Internal deserialization struct for detecting missing fields.
#[derive(Debug, Default, Deserialize)]
struct DeleteItemRequestRaw {
    #[serde(rename = "TableName", default)]
    table_name: Option<String>,
    #[serde(rename = "Key", default)]
    key: Option<HashMap<String, AttributeValue>>,
    #[serde(rename = "ReturnValues", default)]
    return_values: Option<String>,
    #[serde(rename = "ConditionExpression", default)]
    condition_expression: Option<String>,
    #[serde(rename = "ExpressionAttributeNames", default)]
    expression_attribute_names: Option<HashMap<String, String>>,
    #[serde(rename = "ExpressionAttributeValues", default)]
    expression_attribute_values_raw: Option<serde_json::Value>,
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
pub struct DeleteItemRequest {
    pub table_name: String,
    pub key: HashMap<String, AttributeValue>,
    pub return_values: Option<String>,
    pub condition_expression: Option<String>,
    pub expression_attribute_names: Option<HashMap<String, String>>,
    pub expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    pub return_consumed_capacity: Option<String>,
    pub return_values_on_condition_check_failure: Option<String>,
    pub return_item_collection_metrics: Option<String>,
    pub expected: Option<serde_json::Value>,
    pub conditional_operator: Option<String>,
    #[doc(hidden)]
    pub expression_attribute_values_raw: Option<serde_json::Value>,
}

impl<'de> serde::Deserialize<'de> for DeleteItemRequest {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let raw = DeleteItemRequestRaw::deserialize(deserializer)?;
        use crate::validation::{
            format_validation_errors, table_name_constraint_errors, TableNameContext,
        };

        let mut errors = Vec::new();

        // Table name constraints first (DynamoDB ordering for DeleteItem)
        errors.extend(table_name_constraint_errors(
            raw.table_name.as_deref(),
            TableNameContext::ReadWrite,
        ));
        let table_name = raw.table_name.unwrap_or_default();

        // Key constraint
        if raw.key.is_none() {
            errors.push(
                "Value null at 'key' failed to satisfy constraint: \
                 Member must not be null"
                    .to_string(),
            );
        }

        // ReturnConsumedCapacity enum
        if let Some(ref rcc) = raw.return_consumed_capacity {
            if !["INDEXES", "TOTAL", "NONE"].contains(&rcc.as_str()) {
                errors.push(format!(
                    "Value '{}' at 'returnConsumedCapacity' failed to satisfy constraint: \
                     Member must satisfy enum value set: [INDEXES, TOTAL, NONE]",
                    rcc
                ));
            }
        }

        // ReturnValues enum
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

        // ReturnItemCollectionMetrics enum
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

        Ok(DeleteItemRequest {
            table_name,
            key: raw.key.unwrap_or_default(),
            return_values: raw.return_values,
            condition_expression: raw.condition_expression,
            expression_attribute_names: raw.expression_attribute_names,
            expression_attribute_values: None,
            return_consumed_capacity: raw.return_consumed_capacity,
            return_values_on_condition_check_failure: raw.return_values_on_condition_check_failure,
            return_item_collection_metrics: raw.return_item_collection_metrics,
            expected: raw.expected,
            conditional_operator: raw.conditional_operator,
            expression_attribute_values_raw: raw.expression_attribute_values_raw,
        })
    }
}

#[derive(Debug, Default, Serialize)]
pub struct DeleteItemResponse {
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

pub fn execute(storage: &Storage, mut request: DeleteItemRequest) -> Result<DeleteItemResponse> {
    // Validate table name format before checking existence (DynamoDB validates input first)
    crate::validation::validate_table_name(&request.table_name)?;

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
            expression_attribute_values_raw: &request.expression_attribute_values_raw,
        };
        if let Some(parsed_eav) = helpers::validate_expression_params(&ctx)? {
            request.expression_attribute_values = Some(parsed_eav);
        }
    }

    // Validate key attribute values (unsupported datatypes, invalid numbers)
    crate::validation::validate_key_attribute_values(&request.key)?;

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

    // Validate legacy Expected parameter BEFORE checking table existence
    if request.condition_expression.is_none() {
        if let Some(ref expected_val) = request.expected {
            if let Ok(expected) = serde_json::from_value::<
                HashMap<String, helpers::ExpectedCondition>,
            >(expected_val.clone())
            {
                helpers::validate_expected_conditions(&expected)?;
            }
        }
    }

    // Convert legacy Expected parameter to ConditionExpression if no expression is set
    // (validation already done above, this is the actual conversion)
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

    let meta = helpers::require_table_for_item_op(storage, &request.table_name)?;
    let key_schema = helpers::parse_key_schema(&meta)?;

    // Validate ReturnValues parameter (DeleteItem only supports NONE and ALL_OLD)
    if let Some(ref rv) = request.return_values {
        let rv_upper = rv.to_uppercase();
        if rv_upper != "NONE" && rv_upper != "ALL_OLD" {
            return Err(DynoxideError::ValidationException(format!(
                "1 validation error detected: Value '{rv}' at 'returnValues' failed to satisfy constraint: \
                 Member must satisfy enum value set: [ALL_OLD, NONE]"
            )));
        }
    }

    // Validate key
    helpers::validate_key_only(&request.key, &key_schema)?;

    // Extract key values
    let (pk, sk) = helpers::extract_key_strings(&request.key, &key_schema)?;

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

    // Wrap condition check + delete in a transaction to prevent TOCTOU races
    let has_condition = request.condition_expression.is_some();
    if has_condition {
        storage.begin_transaction()?;
    }

    let conditional_result = (|| -> Result<Option<String>> {
        // Evaluate ConditionExpression against existing item
        if let Some(ref cond_expr) = request.condition_expression {
            let existing_json = storage.get_item(&request.table_name, &pk, &sk)?;
            let existing_item: HashMap<String, AttributeValue> = existing_json
                .as_ref()
                .and_then(|j| serde_json::from_str(j).ok())
                .unwrap_or_default();

            let parsed = crate::expressions::condition::parse(cond_expr)
                .map_err(DynoxideError::ValidationException)?;
            let result = crate::expressions::condition::evaluate(&parsed, &existing_item, &tracker)
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

        // Check for unused expression attribute names/values
        tracker.check_unused()?;

        // Delete item (returns old item_json)
        storage.delete_item(&request.table_name, &pk, &sk)
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

    let old_json = conditional_result?;

    // Maintain GSI tables
    let gsi_units =
        super::gsi::maintain_gsis_after_delete(storage, &request.table_name, &meta, &pk, &sk)?;

    // Maintain LSI tables
    super::lsi::maintain_lsis_after_delete(storage, &request.table_name, &meta, &pk, &sk)?;

    // Record stream event
    let old_item_for_stream: Option<Item> =
        old_json.as_ref().and_then(|j| serde_json::from_str(j).ok());
    if old_item_for_stream.is_some() {
        crate::streams::record_stream_event(storage, &meta, old_item_for_stream.as_ref(), None)?;
    }

    // Handle ReturnValues
    let return_old = request
        .return_values
        .as_deref()
        .unwrap_or("NONE")
        .eq_ignore_ascii_case("ALL_OLD");

    let attributes = if return_old {
        old_json.and_then(|json| serde_json::from_str::<Item>(&json).ok())
    } else {
        None
    };

    // Build item collection metrics (only for tables with LSIs)
    let pk_value = request.key.get(&key_schema.partition_key).cloned();
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

    // Calculate consumed capacity from old item size (write for delete)
    let old_size = old_item_for_stream
        .as_ref()
        .map(types::item_size)
        .unwrap_or(0);
    let consumed_capacity = types::consumed_capacity_with_indexes(
        &request.table_name,
        types::write_capacity_units(old_size),
        &gsi_units,
        &request.return_consumed_capacity,
    );

    Ok(DeleteItemResponse {
        attributes,
        consumed_capacity,
        item_collection_metrics,
    })
}
