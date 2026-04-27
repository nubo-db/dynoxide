use crate::actions::helpers;
use crate::errors::{DynoxideError, Result};
use crate::storage::Storage;
use crate::types::{self, AttributeValue};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Internal result from the transactional update work closure.
struct UpdateWorkResult {
    existing_json: Option<String>,
    old_item: HashMap<String, AttributeValue>,
    item: HashMap<String, AttributeValue>,
    item_json: String,
    size: usize,
}

/// Internal deserialization struct for detecting missing fields.
#[derive(Debug, Default, Deserialize)]
struct UpdateItemRequestRaw {
    #[serde(rename = "TableName", default)]
    table_name: Option<String>,
    #[serde(rename = "Key", default)]
    key: Option<HashMap<String, AttributeValue>>,
    #[serde(rename = "UpdateExpression", default)]
    update_expression: Option<String>,
    #[serde(rename = "ConditionExpression", default)]
    condition_expression: Option<String>,
    #[serde(rename = "ExpressionAttributeNames", default)]
    expression_attribute_names: Option<HashMap<String, String>>,
    #[serde(rename = "ExpressionAttributeValues", default)]
    expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    #[serde(rename = "ReturnValues", default)]
    return_values: Option<String>,
    #[serde(rename = "ReturnConsumedCapacity", default)]
    return_consumed_capacity: Option<String>,
    #[serde(rename = "ReturnValuesOnConditionCheckFailure", default)]
    return_values_on_condition_check_failure: Option<String>,
    #[serde(rename = "ReturnItemCollectionMetrics", default)]
    return_item_collection_metrics: Option<String>,
    #[serde(rename = "AttributeUpdates", default)]
    attribute_updates: Option<HashMap<String, AttributeValueUpdate>>,
    #[serde(rename = "Expected", default)]
    expected: Option<serde_json::Value>,
    #[serde(rename = "ConditionalOperator", default)]
    conditional_operator: Option<String>,
}

#[derive(Debug, Default)]
pub struct UpdateItemRequest {
    pub table_name: String,
    pub key: HashMap<String, AttributeValue>,
    pub update_expression: Option<String>,
    pub condition_expression: Option<String>,
    pub expression_attribute_names: Option<HashMap<String, String>>,
    pub expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    pub return_values: Option<String>,
    pub return_consumed_capacity: Option<String>,
    pub return_values_on_condition_check_failure: Option<String>,
    pub return_item_collection_metrics: Option<String>,
    pub attribute_updates: Option<HashMap<String, AttributeValueUpdate>>,
    pub expected: Option<serde_json::Value>,
    pub conditional_operator: Option<String>,
}

impl<'de> serde::Deserialize<'de> for UpdateItemRequest {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let raw = UpdateItemRequestRaw::deserialize(deserializer)?;
        use crate::validation::{
            format_validation_errors, table_name_constraint_errors, TableNameContext,
        };

        let mut errors = Vec::new();

        // Table name constraints
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

        Ok(UpdateItemRequest {
            table_name,
            key: raw.key.unwrap_or_default(),
            update_expression: raw.update_expression,
            condition_expression: raw.condition_expression,
            expression_attribute_names: raw.expression_attribute_names,
            expression_attribute_values: raw.expression_attribute_values,
            return_values: raw.return_values,
            return_consumed_capacity: raw.return_consumed_capacity,
            return_values_on_condition_check_failure: raw.return_values_on_condition_check_failure,
            return_item_collection_metrics: raw.return_item_collection_metrics,
            attribute_updates: raw.attribute_updates,
            expected: raw.expected,
            conditional_operator: raw.conditional_operator,
        })
    }
}

/// Legacy `AttributeUpdates` entry — one per attribute being modified.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct AttributeValueUpdate {
    #[serde(rename = "Action", default = "default_put_action")]
    pub action: String,
    #[serde(rename = "Value", default)]
    pub value: Option<AttributeValue>,
}

fn default_put_action() -> String {
    "PUT".to_string()
}

#[derive(Debug, Default, Serialize)]
pub struct UpdateItemResponse {
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

/// Apply the `Invalid UpdateExpression:` prefix to a sub-error message at the
/// UpdateItem dispatch boundary. AWS DynamoDB tags the missing-EAV error
/// (and similar UpdateExpression-scoped errors) with this prefix; the prefix
/// must not leak into ConditionExpression contexts that share the same
/// underlying validators in `crate::expressions::mod`. Idempotent so that
/// errors which already carry the prefix (e.g. parser-level syntax errors)
/// are not double-wrapped.
fn wrap_invalid_update_expression(err: String) -> String {
    if err.starts_with("Invalid UpdateExpression:") {
        err
    } else {
        format!("Invalid UpdateExpression: {err}")
    }
}

pub fn execute(storage: &Storage, mut request: UpdateItemRequest) -> Result<UpdateItemResponse> {
    // Validate table name format before checking existence (DynamoDB validates input first)
    crate::validation::validate_table_name(&request.table_name)?;

    // Validate expression/non-expression parameter conflicts BEFORE Expected conversion
    {
        let mut non_expr = Vec::new();
        let mut expr_params = Vec::new();
        if request.attribute_updates.is_some() {
            non_expr.push("AttributeUpdates");
        }
        if request.expected.is_some() {
            non_expr.push("Expected");
        }
        if request.update_expression.is_some() {
            expr_params.push("UpdateExpression");
        }
        if request.condition_expression.is_some() {
            expr_params.push("ConditionExpression");
        }
        let no_raw_eav: Option<serde_json::Value> = None;
        let ctx = helpers::ExpressionParamContext {
            non_expression_params: non_expr,
            expression_params: expr_params,
            all_expression_param_names: vec!["UpdateExpression", "ConditionExpression"],
            expression_attribute_names: &request.expression_attribute_names,
            expression_attribute_values: &request.expression_attribute_values,
            expression_attribute_values_raw: &no_raw_eav,
        };
        helpers::validate_expression_params(&ctx)?;
    }

    // Validate key attribute values (unsupported datatypes, invalid numbers)
    crate::validation::validate_key_attribute_values(&request.key)?;

    // Validate legacy AttributeUpdates parameters
    if request.update_expression.is_none() {
        if let Some(ref updates) = request.attribute_updates {
            for (attr_name, update) in updates {
                let action = update.action.to_uppercase();
                if update.value.is_none() && action != "DELETE" {
                    return Err(DynoxideError::ValidationException(
                        "One or more parameter values were invalid: \
                         Only DELETE action is allowed when no attribute value is specified"
                            .to_string(),
                    ));
                }
                if action == "DELETE" {
                    if let Some(ref val) = update.value {
                        let type_name = match val {
                            AttributeValue::SS(_)
                            | AttributeValue::NS(_)
                            | AttributeValue::BS(_) => None,
                            _ => Some(val.type_name()),
                        };
                        if let Some(tn) = type_name {
                            return Err(DynoxideError::ValidationException(format!(
                                "One or more parameter values were invalid: \
                                 DELETE action with value is not supported for the type {tn}"
                            )));
                        }
                    }
                }
                if action == "ADD" {
                    if let Some(ref val) = update.value {
                        let allowed = matches!(
                            val,
                            AttributeValue::N(_)
                                | AttributeValue::SS(_)
                                | AttributeValue::NS(_)
                                | AttributeValue::BS(_)
                                | AttributeValue::L(_)
                        );
                        if !allowed {
                            let tn = val.type_name();
                            return Err(DynoxideError::ValidationException(format!(
                                "One or more parameter values were invalid: \
                                 ADD action is not supported for the type {tn}"
                            )));
                        }
                    }
                }
                let _ = attr_name; // suppress unused warning
            }
        }
    }

    // Validate legacy Expected parameter
    if request.condition_expression.is_none() && request.update_expression.is_none() {
        if let Some(ref expected_val) = request.expected {
            if let Ok(expected) = serde_json::from_value::<
                HashMap<String, helpers::ExpectedCondition>,
            >(expected_val.clone())
            {
                helpers::validate_expected_conditions(&expected)?;
            }
        }
    }

    // Validate empty UpdateExpression
    if let Some(ref ue) = request.update_expression {
        if ue.is_empty() {
            return Err(DynoxideError::ValidationException(
                "Invalid UpdateExpression: The expression can not be empty;".to_string(),
            ));
        }
    }

    // Validate empty ConditionExpression
    if let Some(ref ce) = request.condition_expression {
        if ce.is_empty() {
            return Err(DynoxideError::ValidationException(
                "Invalid ConditionExpression: The expression can not be empty;".to_string(),
            ));
        }
    }

    // Pre-validate UpdateExpression syntax BEFORE table lookup.
    // DynamoDB validates expression syntax, reserved keywords, undefined attribute
    // names/values, overlapping paths, etc. before checking table existence.
    if let Some(ref ue) = request.update_expression {
        let parsed =
            crate::expressions::update::parse(ue).map_err(DynoxideError::ValidationException)?;

        // Track all attribute name/value references statically (without evaluating)
        let tracker = crate::expressions::TrackedExpressionAttributes::new(
            &request.expression_attribute_names,
            &request.expression_attribute_values,
        );
        crate::expressions::update::track_references(&parsed, &tracker)
            .map_err(|e| DynoxideError::ValidationException(wrap_invalid_update_expression(e)))?;

        // Also walk the ConditionExpression to track its attribute usage
        if let Some(ref ce) = request.condition_expression {
            if let Ok(cond_parsed) = crate::expressions::condition::parse(ce) {
                crate::expressions::condition::track_references(&cond_parsed, &tracker)
                    .map_err(DynoxideError::ValidationException)?;
            }
        }

        // Check for unused expression attribute names/values
        tracker.check_unused()?;
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

    let meta = helpers::require_table_for_item_op(storage, &request.table_name)?;
    let key_schema = helpers::parse_key_schema(&meta)?;

    // Validate ReturnValues parameter
    if let Some(ref rv) = request.return_values {
        let rv_upper = rv.to_uppercase();
        if !["NONE", "ALL_OLD", "ALL_NEW", "UPDATED_OLD", "UPDATED_NEW"]
            .contains(&rv_upper.as_str())
        {
            return Err(DynoxideError::ValidationException(format!(
                "1 validation error detected: Value '{rv}' at 'returnValues' failed to satisfy constraint: \
                 Member must satisfy enum value set: [ALL_NEW, ALL_OLD, NONE, UPDATED_NEW, UPDATED_OLD]"
            )));
        }
    }

    // Validate key
    helpers::validate_key_only(&request.key, &key_schema)?;

    // Extract key values
    let (pk, sk) = helpers::extract_key_strings(&request.key, &key_schema)?;

    // Collect the set of attribute names affected by the legacy AttributeUpdates
    // parameter, used later for UPDATED_OLD / UPDATED_NEW extraction.
    let legacy_attr_names: Option<Vec<String>> = request
        .attribute_updates
        .as_ref()
        .map(|updates| updates.keys().cloned().collect());

    // Wrap condition check + write in a transaction to prevent TOCTOU races
    let has_condition = request.condition_expression.is_some();
    if has_condition {
        storage.begin_transaction()?;
    }

    // Execution tracker — tracking disabled because unused-reference validation was
    // already done statically by Tracker 1 (pre-validation block above). This tracker
    // only needs name/value resolution, not usage tracking.
    let tracker = crate::expressions::TrackedExpressionAttributes::without_tracking(
        &request.expression_attribute_names,
        &request.expression_attribute_values,
    );

    // Execute the condition check + update + write atomically within a transaction.
    // The closure captures everything from get_item through put_item.
    let transactional_work = || -> Result<UpdateWorkResult> {
        // Fetch existing item (or create empty one for upsert)
        let existing_json = storage.get_item(&request.table_name, &pk, &sk)?;
        let existing_item: HashMap<String, AttributeValue> = existing_json
            .as_ref()
            .and_then(|j| serde_json::from_str(j).ok())
            .unwrap_or_default();

        // Evaluate ConditionExpression against the original existing item BEFORE
        // populating key attributes for upsert. Otherwise attribute_exists(PK)
        // would always pass because the key was pre-populated.
        if let Some(ref cond_expr) = request.condition_expression {
            let parsed = crate::expressions::condition::parse(cond_expr)
                .map_err(DynoxideError::ValidationException)?;
            let result = crate::expressions::condition::evaluate(&parsed, &existing_item, &tracker)
                .map_err(DynoxideError::ValidationException)?;
            if !result {
                let return_item = if request.return_values_on_condition_check_failure.as_deref()
                    == Some("ALL_OLD")
                    && existing_json.is_some()
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

        // Build mutable item for the update expression.
        // If item doesn't exist, populate key attributes for upsert.
        let mut item = existing_item;
        if existing_json.is_none() {
            for (k, v) in &request.key {
                item.insert(k.clone(), v.clone());
            }
        }

        // Save old item for ReturnValues
        let old_item = item.clone();

        // Apply UpdateExpression
        if let Some(ref update_expr) = request.update_expression {
            let parsed = crate::expressions::update::parse(update_expr)
                .map_err(DynoxideError::ValidationException)?;

            // Validate: cannot modify key attributes with SET
            // (key validation uses the free function, not tracked)
            for action in &parsed.set_actions {
                validate_not_key_attr(
                    action.path.first(),
                    &key_schema,
                    &request.expression_attribute_names,
                )?;
            }

            // Validate: cannot REMOVE key attributes
            for path in &parsed.remove_actions {
                validate_not_key_attr(
                    path.first(),
                    &key_schema,
                    &request.expression_attribute_names,
                )?;
            }

            // Validate: cannot ADD to key attributes
            for action in &parsed.add_actions {
                validate_not_key_attr(
                    action.path.first(),
                    &key_schema,
                    &request.expression_attribute_names,
                )?;
            }

            // Validate: cannot DELETE from key attributes
            for action in &parsed.delete_actions {
                validate_not_key_attr(
                    action.path.first(),
                    &key_schema,
                    &request.expression_attribute_names,
                )?;
            }

            crate::expressions::update::apply(&mut item, &parsed, &tracker)
                .map_err(DynoxideError::ValidationException)?;
        }

        // Apply legacy AttributeUpdates (if no UpdateExpression was provided)
        if request.update_expression.is_none() {
            if let Some(ref updates) = request.attribute_updates {
                apply_attribute_updates(&mut item, updates, &key_schema)?;
            }
        }

        // Note: unused expression attribute validation already done in pre-validation
        // block (Tracker 1). Not repeated here — runtime evaluation may skip branches
        // (e.g., if_not_exists short-circuits) which would cause false positives.

        // Validate attribute values after update expression applied
        crate::validation::validate_item_attribute_values(&item)?;
        crate::validation::normalize_item_sets(&mut item);

        // Validate updated item size
        let size = types::item_size(&item);
        if size > types::MAX_ITEM_SIZE {
            return Err(DynoxideError::ValidationException(
                "Item size to update has exceeded the maximum allowed size".to_string(),
            ));
        }

        // Serialize and store
        let item_json = serde_json::to_string(&item)
            .map_err(|e| DynoxideError::InternalServerError(e.to_string()))?;
        let hash_prefix = request
            .key
            .get(&key_schema.partition_key)
            .map(crate::storage::compute_hash_prefix)
            .unwrap_or_default();
        storage.put_item_with_hash(
            &request.table_name,
            &pk,
            &sk,
            &item_json,
            size,
            &hash_prefix,
        )?;

        Ok(UpdateWorkResult {
            existing_json,
            old_item,
            item,
            item_json,
            size,
        })
    };

    let result = transactional_work();

    // Commit or rollback the condition+write transaction
    if has_condition {
        match result {
            Ok(_) => storage.commit()?,
            Err(ref _e) => {
                let _ = storage.rollback();
            }
        }
    }

    let UpdateWorkResult {
        existing_json,
        old_item,
        item,
        item_json,
        size,
    } = result?;

    // Maintain GSI tables
    let gsi_units = super::gsi::maintain_gsis_after_write(
        storage,
        &request.table_name,
        &meta,
        &pk,
        &sk,
        &item,
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
        &item,
        &key_schema.partition_key,
        key_schema.sort_key.as_deref(),
    )?;

    // Record stream event
    let old_for_stream = if existing_json.is_some() {
        Some(&old_item)
    } else {
        None
    };
    crate::streams::record_stream_event(storage, &meta, old_for_stream, Some(&item))?;

    // Handle ReturnValues
    let return_values = request.return_values.as_deref().unwrap_or("NONE");
    let attributes = match return_values.to_uppercase().as_str() {
        "ALL_OLD" => Some(old_item),
        "ALL_NEW" => Some(item),
        "UPDATED_OLD" => {
            if let Some(ref update_expr) = request.update_expression {
                // Expression-based: extract only the attributes targeted by the expression.
                let parsed = crate::expressions::update::parse(update_expr)
                    .map_err(DynoxideError::ValidationException)?;
                Some(extract_updated_attrs(
                    &old_item,
                    &parsed,
                    &request.expression_attribute_names,
                ))
            } else {
                // Legacy AttributeUpdates: extract the named attributes from the old item.
                legacy_attr_names
                    .as_ref()
                    .map(|names| extract_named_attrs(&old_item, names))
            }
        }
        "UPDATED_NEW" => {
            if let Some(ref update_expr) = request.update_expression {
                // Expression-based: extract only the attributes targeted by the expression.
                let parsed = crate::expressions::update::parse(update_expr)
                    .map_err(DynoxideError::ValidationException)?;
                let new_item: HashMap<String, AttributeValue> = serde_json::from_str(&item_json)
                    .map_err(|e| DynoxideError::InternalServerError(e.to_string()))?;
                Some(extract_updated_attrs(
                    &new_item,
                    &parsed,
                    &request.expression_attribute_names,
                ))
            } else {
                // Legacy AttributeUpdates: extract the named attributes from the new item.
                legacy_attr_names.as_ref().map(|names| {
                    let new_item: HashMap<String, AttributeValue> =
                        serde_json::from_str(&item_json).unwrap_or_default();
                    extract_named_attrs(&new_item, names)
                })
            }
        }
        _ => None, // "NONE" or default
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

    let consumed_capacity = types::consumed_capacity_with_indexes(
        &request.table_name,
        types::write_capacity_units(size),
        &gsi_units,
        &request.return_consumed_capacity,
    );

    Ok(UpdateItemResponse {
        attributes,
        consumed_capacity,
        item_collection_metrics,
    })
}

/// Apply legacy `AttributeUpdates` to the item, mutating it in place.
///
/// Each entry maps an attribute name to an action:
/// - `PUT` (default): set the attribute to the given value
/// - `ADD`: add a number or union a set
/// - `DELETE`: remove the attribute, or remove elements from a set
fn apply_attribute_updates(
    item: &mut HashMap<String, AttributeValue>,
    updates: &HashMap<String, AttributeValueUpdate>,
    key_schema: &helpers::KeySchema,
) -> Result<()> {
    for (attr_name, update) in updates {
        // Cannot modify key attributes
        if attr_name == &key_schema.partition_key
            || key_schema
                .sort_key
                .as_ref()
                .is_some_and(|sk| sk == attr_name)
        {
            return Err(DynoxideError::ValidationException(format!(
                "One or more parameter values were invalid: \
                 Cannot update attribute {attr_name}. This attribute is part of the key"
            )));
        }

        let action = update.action.to_uppercase();
        match action.as_str() {
            "PUT" => {
                if let Some(ref value) = update.value {
                    item.insert(attr_name.clone(), value.clone());
                }
            }
            "ADD" => {
                if let Some(ref add_val) = update.value {
                    let path = vec![crate::expressions::PathElement::Attribute(
                        attr_name.clone(),
                    )];
                    crate::expressions::update::apply_add_public(item, &path, add_val)
                        .map_err(DynoxideError::ValidationException)?;
                }
            }
            "DELETE" => {
                if let Some(ref del_val) = update.value {
                    // DELETE with a value: remove elements from a set
                    let path = vec![crate::expressions::PathElement::Attribute(
                        attr_name.clone(),
                    )];
                    crate::expressions::update::apply_delete_public(item, &path, del_val)
                        .map_err(DynoxideError::ValidationException)?;
                } else {
                    // DELETE without a value: remove the attribute entirely
                    item.remove(attr_name);
                }
            }
            _ => {
                return Err(DynoxideError::ValidationException(format!(
                    "1 validation error detected: Value '{action}' at 'attributeUpdates.{attr_name}.member.action' \
                     failed to satisfy constraint: Member must satisfy enum value set: [ADD, PUT, DELETE]"
                )));
            }
        }
    }
    Ok(())
}

/// Extract only the attributes that were affected by the update expression.
fn extract_updated_attrs(
    item: &HashMap<String, AttributeValue>,
    expr: &crate::expressions::update::UpdateExpr,
    attr_names: &Option<HashMap<String, String>>,
) -> HashMap<String, AttributeValue> {
    let mut result = HashMap::new();

    // SET actions
    for action in &expr.set_actions {
        if let Some(name) = get_top_level_name(&action.path, attr_names) {
            if let Some(val) = item.get(&name) {
                result.insert(name, val.clone());
            }
        }
    }

    // REMOVE actions
    for path in &expr.remove_actions {
        if let Some(name) = get_top_level_name(path, attr_names) {
            if let Some(val) = item.get(&name) {
                result.insert(name, val.clone());
            }
        }
    }

    // ADD actions
    for action in &expr.add_actions {
        if let Some(name) = get_top_level_name(&action.path, attr_names) {
            if let Some(val) = item.get(&name) {
                result.insert(name, val.clone());
            }
        }
    }

    // DELETE actions
    for action in &expr.delete_actions {
        if let Some(name) = get_top_level_name(&action.path, attr_names) {
            if let Some(val) = item.get(&name) {
                result.insert(name, val.clone());
            }
        }
    }

    result
}

/// Extract named attributes from an item (used for legacy AttributeUpdates ReturnValues).
fn extract_named_attrs(
    item: &HashMap<String, AttributeValue>,
    attr_names: &[String],
) -> HashMap<String, AttributeValue> {
    let mut result = HashMap::new();
    for name in attr_names {
        if let Some(val) = item.get(name) {
            result.insert(name.clone(), val.clone());
        }
    }
    result
}

fn get_top_level_name(
    path: &[crate::expressions::PathElement],
    attr_names: &Option<HashMap<String, String>>,
) -> Option<String> {
    match path.first() {
        Some(crate::expressions::PathElement::Attribute(name)) => {
            if name.starts_with('#') {
                crate::expressions::resolve_name(name, attr_names).ok()
            } else {
                Some(name.clone())
            }
        }
        _ => None,
    }
}

/// Validate that a path element does not target a key attribute.
fn validate_not_key_attr(
    first_element: Option<&crate::expressions::PathElement>,
    key_schema: &helpers::KeySchema,
    expression_attribute_names: &Option<HashMap<String, String>>,
) -> crate::errors::Result<()> {
    if let Some(crate::expressions::PathElement::Attribute(name)) = first_element {
        let resolved_name = if name.starts_with('#') {
            crate::expressions::resolve_name(name, expression_attribute_names)
                .map_err(DynoxideError::ValidationException)?
        } else {
            name.clone()
        };
        if resolved_name == key_schema.partition_key
            || key_schema
                .sort_key
                .as_ref()
                .is_some_and(|sk| sk == &resolved_name)
        {
            return Err(DynoxideError::ValidationException(format!(
                "One or more parameter values were invalid: Cannot update attribute {resolved_name}. This attribute is part of the key"
            )));
        }
    }
    Ok(())
}
