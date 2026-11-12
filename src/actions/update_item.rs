use crate::actions::helpers;
use crate::errors::{DynoxideError, Result};
use crate::storage_backend::StorageBackend;
use crate::types::{self, AttributeValue};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Internal result from the transactional update work closure.
struct UpdateWorkResult {
    old_item: HashMap<String, AttributeValue>,
    item: HashMap<String, AttributeValue>,
    item_json: String,
    size: usize,
    /// Size of the item as it genuinely stood beforehand, or `None` when the
    /// update was an upsert that created it. Distinct from `old_item`, which
    /// carries injected key attributes in that case.
    old_size: Option<usize>,
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
    #[serde(
        rename = "ExpressionAttributeValues",
        default,
        deserialize_with = "crate::types::deserialize_named_null_refusals"
    )]
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

/// UpdateItem's first invalid enum member, or `None` if all are valid, and
/// whether DynamoDB wraps it in the `1 validation error detected: ` envelope.
/// UpdateItem reports one, where PutItem reports them all. In the newer
/// validation layer (five of seven regions, 2026-10-09) the order is
/// ReturnValues, ReturnValuesOnConditionCheckFailure,
/// ReturnItemCollectionMetrics, ReturnConsumedCapacity, and the second and
/// fourth come back without the envelope.
fn first_invalid_member_enum(
    return_values: Option<&str>,
    return_values_on_condition_check_failure: Option<&str>,
    return_item_collection_metrics: Option<&str>,
    return_consumed_capacity: Option<&str>,
) -> Option<(String, bool)> {
    if let Some(rv) = return_values
        && !crate::validation::RETURN_VALUES.contains(&rv)
    {
        return Some((crate::validation::RETURN_VALUES_REJECTION.to_string(), true));
    }
    if let Some(msg) = crate::validation::return_values_on_condition_check_failure_rejection(
        return_values_on_condition_check_failure,
        "returnValuesOnConditionCheckFailure",
    ) {
        return Some((msg, false));
    }
    if let Some(msg) =
        crate::validation::return_item_collection_metrics_rejection(return_item_collection_metrics)
    {
        return Some((msg, true));
    }
    if let Some(msg) =
        crate::validation::return_consumed_capacity_rejection(return_consumed_capacity)
    {
        return Some((msg, false));
    }
    None
}

impl<'de> serde::Deserialize<'de> for UpdateItemRequest {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let raw = UpdateItemRequestRaw::deserialize(deserializer)?;
        use crate::validation::format_validation_errors;

        // A legacy condition value that can't be read (a boolean that doesn't
        // read as one, or a member of the wrong JSON type) makes the whole
        // request malformed, ahead of everything else.
        if let Some(message) = crate::types::malformed_condition_value(raw.expected.as_ref()) {
            return Err(serde::de::Error::custom(message));
        }

        // A missing key hands the request to the older member-validation
        // layer, including an empty table name and ConditionalOperator (#262).
        if raw.key.is_none() {
            let mut errors: Vec<String> = Vec::new();
            if let Some(msg) = crate::validation::return_consumed_capacity_rejection(
                raw.return_consumed_capacity.as_deref(),
            ) {
                errors.push(msg);
            }
            if raw.table_name.as_deref().is_none_or(str::is_empty) {
                errors.extend(crate::validation::table_name_constraint_errors(
                    raw.table_name.as_deref(),
                    crate::validation::TableNameContext::ReadWrite,
                ));
            }
            if let Some(rv) = raw.return_values.as_deref()
                && !crate::validation::RETURN_VALUES.contains(&rv)
            {
                errors.push(crate::validation::return_values_value_rejection(rv));
            }
            if let Some(msg) = crate::validation::return_values_on_condition_check_failure_rejection(
                raw.return_values_on_condition_check_failure.as_deref(),
                "returnValuesOnConditionCheckFailure",
            ) {
                errors.push(msg);
            }
            if let Some(msg) = crate::validation::return_item_collection_metrics_rejection(
                raw.return_item_collection_metrics.as_deref(),
            ) {
                errors.push(msg);
            }
            errors.extend(crate::validation::conditional_operator_rejection(
                raw.conditional_operator.as_deref(),
            ));
            errors.push(
                "Value null at 'key' failed to satisfy constraint: Member must not be null"
                    .to_string(),
            );
            if let Some(msg) = format_validation_errors(&errors) {
                return Err(serde::de::Error::custom(format!("VALIDATION:{}", msg)));
            }
        }

        // A missing table name is joined with an invalid ReturnValues, the
        // name's error first (captured eu-west-2 and us-east-1, 2026-10-09).
        // The other enums beside a missing name are uncaptured.
        if raw.table_name.is_none()
            && let Some(rv) = raw.return_values.as_deref()
            && !crate::validation::RETURN_VALUES.contains(&rv)
        {
            let mut errors = crate::validation::update_item_table_name_errors(None);
            errors.push(crate::validation::return_values_value_rejection(rv));
            if let Some(msg) = format_validation_errors(&errors) {
                return Err(serde::de::Error::custom(format!("VALIDATION:{}", msg)));
            }
        }

        // UpdateItem reports a table name error on its own, before the key and
        // the enums (captured 2026-10-09, five of seven regions).
        let table_name_errors =
            crate::validation::update_item_table_name_errors(raw.table_name.as_deref());
        if let Some(msg) = format_validation_errors(&table_name_errors) {
            return Err(serde::de::Error::custom(format!("VALIDATION:{}", msg)));
        }
        let table_name = raw.table_name.unwrap_or_default();

        let mut errors = Vec::new();

        // Key constraint
        if raw.key.is_none() {
            errors.push(
                "Value null at 'key' failed to satisfy constraint: \
                 Member must not be null"
                    .to_string(),
            );
        }

        // UpdateItem stops at the first invalid enum and reports one error,
        // where PutItem aggregates all of them. One reported without the
        // envelope stands alone; beside a missing key it is left for later,
        // since that pairing is uncaptured.
        match first_invalid_member_enum(
            raw.return_values.as_deref(),
            raw.return_values_on_condition_check_failure.as_deref(),
            raw.return_item_collection_metrics.as_deref(),
            raw.return_consumed_capacity.as_deref(),
        ) {
            Some((msg, true)) => errors.push(msg),
            Some((msg, false)) if errors.is_empty() => {
                return Err(serde::de::Error::custom(format!("VALIDATION:{msg}")));
            }
            _ => {}
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

/// Legacy `AttributeUpdates` entry: one per attribute being modified.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct AttributeValueUpdate {
    #[serde(rename = "Action", default = "default_put_action")]
    pub action: String,
    #[serde(rename = "Value", default)]
    pub value: Option<AttributeValue>,
}

/// DynamoDB's refusal of an `AttributeUpdates` action other than `ADD`,
/// `DELETE` or `PUT`, written exactly. Tagged for the envelope, which adds
/// `1 validation error detected: `.
const ATTRIBUTE_UPDATE_ACTION_REJECTION: &str =
    "Member must satisfy enum value set: [ADD, DELETE, PUT]' for Action";

fn default_put_action() -> String {
    "PUT".to_string()
}

#[derive(Debug, Default, Serialize)]
pub struct UpdateItemResponse {
    #[serde(rename = "Attributes", skip_serializing_if = "Option::is_none")]
    // Number sets in numeric order here, as DynamoDB returns them;
    // pinned by set_order's every_response_item_field test.
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

pub async fn execute<S: StorageBackend>(
    storage: &S,
    request: UpdateItemRequest,
) -> Result<UpdateItemResponse> {
    // Apply the request-validation envelope exactly once at the operation
    // boundary, so every early return inside is covered and the tagged
    // variant never escapes. A request read on the older layer never gets it.
    let layer = crate::validation::single_write_layer(request.expected.as_ref());
    execute_inner(storage, request, layer)
        .await
        .map_err(|e| crate::validation::resolve_for_layer(layer, e))
}

async fn execute_inner<S: StorageBackend>(
    storage: &S,
    mut request: UpdateItemRequest,
    layer: crate::validation::ValueLayer,
) -> Result<UpdateItemResponse> {
    // Validate table name format before checking existence (DynamoDB validates input first)
    if let Some(msg) = crate::validation::format_validation_errors(
        &crate::validation::update_item_table_name_errors(Some(&request.table_name)),
    ) {
        return Err(DynoxideError::ValidationException(msg));
    }

    // An empty attribute name is refused as the request is read, ahead of every
    // other value check, the expressions and the table lookup (captured in ten
    // regions).
    let updates_empty = request.attribute_updates.as_ref().is_some_and(|updates| {
        updates.iter().any(|(name, update)| {
            name.is_empty()
                || update
                    .value
                    .as_ref()
                    .is_some_and(crate::empty_names::value_has_empty_name)
        })
    });
    crate::empty_names::check_single_item_write(
        &request.key,
        updates_empty,
        request.expected.as_ref(),
        request.expression_attribute_names.as_ref(),
        request.expression_attribute_values.as_ref(),
        None,
    )?;

    // Reject {NULL: false} up front. The HTTP path rejects it during request
    // deserialisation; this keeps the in-process API in agreement (eu-west-2).
    helpers::validate_no_null_false(&request.key)?;
    if let Some(ref values) = request.expression_attribute_values {
        helpers::validate_no_null_false(values)?;
    }
    if let Some(ref updates) = request.attribute_updates {
        for update in updates.values() {
            if let Some(ref value) = update.value {
                helpers::validate_no_null_false_value(value)?;
            }
        }
    }

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
        if request.conditional_operator.is_some() {
            non_expr.push("ConditionalOperator");
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
            // DynamoDB names no expression after its "can only be specified
            // when using expressions" message on the single-item writes
            // (eu-west-2 and us-east-1, 2026-10-09).
            value_expression_params: &[],
            expression_attribute_names: &request.expression_attribute_names,
            expression_attribute_values: &request.expression_attribute_values,
            expression_attribute_values_raw: &no_raw_eav,
            layer,
        };
        // Enveloped families are tagged for the request-validation envelope.
        helpers::validate_expression_params(&ctx)
            .map_err(crate::validation::ClassifiedValidationError::into_tagged)?;
    }

    // ConditionalOperator only joins legacy Expected conditions, so it is
    // refused without two of them, whatever its value (captured 2026-10-09).
    if let Some(msg) = crate::validation::conditional_operator_misuse(
        request.conditional_operator.as_deref(),
        crate::validation::legacy_condition_count(request.expected.as_ref()),
    ) {
        return Err(DynoxideError::EnvelopedValidation(msg));
    }
    // A key, AttributeUpdates value or Expected value nested too deep is
    // refused, inside the envelope, before the key is matched to the schema or
    // the table looked up.
    crate::value_nesting::check_key_enveloped(&request.key)?;
    crate::value_nesting::check_attribute_updates(
        request
            .attribute_updates
            .iter()
            .flat_map(|updates| updates.values())
            .map(|update| update.value.as_ref()),
    )?;
    crate::value_nesting::check_expected(request.expected.as_ref())?;

    // Validate key attribute values (unsupported datatypes, invalid numbers).
    // A duplicated set member or a number it can't store comes back in the
    // envelope (eu-west-2, us-east-1, eu-west-1 and us-west-2, 2026-10-09).
    crate::validation::validate_key_values(&request.key, layer)
        .map_err(crate::validation::ClassifiedValidationError::into_tagged)?;

    // A legacy AttributeUpdates value with a repeated set member is refused in
    // the envelope, in the current layer's wording (same capture).
    if let Some(ref updates) = request.attribute_updates {
        let mut names: Vec<&String> = updates.keys().collect();
        names.sort();
        for name in names {
            if let Some(value) = updates[name].value.as_ref()
                && let Some(message) = crate::validation::find_duplicate_set(value, layer)
            {
                return Err(DynoxideError::EnvelopedValidation(message));
            }
        }
    }

    // Validate legacy AttributeUpdates parameters
    if request.update_expression.is_none()
        && let Some(ref updates) = request.attribute_updates
    {
        // The action is matched exactly: `put` or `Delete` is refused before
        // the table is looked up (eu-west-2 and us-east-1, 2026-10-10). The
        // stray quote is DynamoDB's own. Every action is checked before any
        // other update's own checks, so the answer doesn't depend on the
        // order the map happens to iterate in.
        if updates
            .values()
            .any(|update| !["ADD", "DELETE", "PUT"].contains(&update.action.as_str()))
        {
            return Err(DynoxideError::EnvelopedValidation(
                ATTRIBUTE_UPDATE_ACTION_REJECTION.to_string(),
            ));
        }
        for update in updates.values() {
            let action = update.action.as_str();
            if update.value.is_none() && action != "DELETE" {
                return Err(DynoxideError::ValidationException(
                    "One or more parameter values were invalid: \
                         Only DELETE action is allowed when no attribute value is specified"
                        .to_string(),
                ));
            }
            if action == "DELETE"
                && let Some(ref val) = update.value
            {
                let type_name = match val {
                    AttributeValue::SS(_) | AttributeValue::NS(_) | AttributeValue::BS(_) => None,
                    _ => Some(val.type_name()),
                };
                if let Some(tn) = type_name {
                    return Err(DynoxideError::ValidationException(format!(
                        "One or more parameter values were invalid: \
                                 DELETE action with value is not supported for the type {tn}"
                    )));
                }
            }
            if action == "ADD"
                && let Some(ref val) = update.value
            {
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
    }

    // Read once and used twice: the validation immediately below and the
    // conversion further down, either side of the expression checks between,
    // none of which touch `expected` or `condition_expression`.
    //
    // The two uses do not share a condition. Validation additionally requires
    // no UpdateExpression, conversion does not, so that guard stays on the
    // validation rather than moving up here.
    let expected: Option<HashMap<String, helpers::ExpectedCondition>> =
        match request.expected.as_ref() {
            Some(v) if request.condition_expression.is_none() => helpers::read_expected(v)?,
            _ => None,
        };

    // Validate legacy Expected parameter. Every family comes back in the
    // envelope (eu-west-2 and us-east-1, 2026-10-09).
    if request.update_expression.is_none()
        && let Some(ref expected) = expected
    {
        helpers::validate_expected_conditions(expected)
            .map_err(crate::validation::tag_for_envelope)?;
    }

    // Validate empty ConditionExpression, inside the envelope (eu-west-2 and
    // us-east-1, 2026-10-09). It is reported ahead of an empty
    // UpdateExpression (eu-west-2 and us-east-1, 2026-10-09).
    if let Some(ref ce) = request.condition_expression
        && ce.is_empty()
    {
        return Err(DynoxideError::EnvelopedValidation(
            "Invalid ConditionExpression: The expression can not be empty;".to_string(),
        ));
    }

    // Validate empty UpdateExpression, inside the envelope too (same capture).
    if let Some(ref ue) = request.update_expression
        && ue.is_empty()
    {
        return Err(DynoxideError::EnvelopedValidation(
            "Invalid UpdateExpression: The expression can not be empty;".to_string(),
        ));
    }

    // Pre-validate UpdateExpression syntax BEFORE table lookup.
    // DynamoDB validates expression syntax, reserved keywords, undefined attribute
    // names/values, overlapping paths, etc. before checking table existence.
    // Parse-time errors here are families DynamoDB wraps in the
    // request-validation envelope, so they are tagged; the unused-attribute
    // check propagates bare, matching PutItem.
    if let Some(ref ue) = request.update_expression {
        let parsed =
            crate::expressions::update::parse(ue).map_err(DynoxideError::EnvelopedValidation)?;

        // Track all attribute name/value references statically (without evaluating)
        let tracker = crate::expressions::TrackedExpressionAttributes::new(
            &request.expression_attribute_names,
            &request.expression_attribute_values,
        );
        crate::expressions::update::track_references(&parsed, &tracker)
            .map_err(|e| DynoxideError::EnvelopedValidation(wrap_invalid_update_expression(e)))?;

        // Also walk the ConditionExpression to track its attribute usage. One
        // that doesn't parse is reported here, ahead of any unused value
        // (us-east-1 and eu-west-2, 2026-10-09).
        if let Some(ref ce) = request.condition_expression {
            let cond_parsed = crate::expressions::condition::parse(ce).map_err(|e| {
                DynoxideError::EnvelopedValidation(format!("Invalid ConditionExpression: {e}"))
            })?;
            crate::expressions::condition::track_references(&cond_parsed, &tracker).map_err(
                |e| DynoxideError::EnvelopedValidation(format!("Invalid ConditionExpression: {e}")),
            )?;
        }

        // Check for unused expression attribute names/values. Enveloped, and
        // ahead of the table lookup (us-east-1 and eu-west-2, 2026-10-09).
        tracker
            .check_unused()
            .map_err(crate::validation::tag_for_envelope)?;
    }

    // Statically validate ConditionExpression (syntax + BETWEEN bounds, etc.)
    // before table lookup. Parse-time, so tagged like the block above.
    if let Some(ref ce) = request.condition_expression {
        let parsed = crate::expressions::condition::parse(ce).map_err(|e| {
            DynoxideError::EnvelopedValidation(format!("Invalid ConditionExpression: {e}"))
        })?;
        crate::expressions::condition::validate_static(
            &parsed,
            &request.expression_attribute_values,
        )
        .map_err(DynoxideError::EnvelopedValidation)?;
        crate::expressions::condition::validate_operand_semantics(
            &parsed,
            &request.expression_attribute_names,
            &request.expression_attribute_values,
        )
        .map_err(|e| {
            DynoxideError::EnvelopedValidation(format!("Invalid ConditionExpression: {e}"))
        })?;
    }

    // Convert legacy Expected parameter to ConditionExpression if no expression is set
    if let Some(ref expected) = expected
        && !expected.is_empty()
    {
        let converted = helpers::convert_expected_to_condition(
            expected,
            request.conditional_operator.as_deref(),
        )?;
        if !converted.expression.is_empty() {
            request.condition_expression = Some(converted.expression);
            let expr_values = request
                .expression_attribute_values
                .get_or_insert_with(HashMap::new);
            expr_values.extend(converted.attribute_values);
            let expr_names = request
                .expression_attribute_names
                .get_or_insert_with(HashMap::new);
            expr_names.extend(converted.attribute_names);
        }
    }

    let meta = helpers::require_table_for_item_op(storage, &request.table_name).await?;
    let key_schema = helpers::parse_key_schema(&meta)?;

    // Validate ReturnValues parameter. The constraint text is tagged without
    // the envelope prefix; the operation boundary applies it, so the prefix
    // has a single producer. It is the same text first_invalid_return_enum
    // gives, so the in-process message matches the request deserialiser.
    if let Some(ref rv) = request.return_values {
        let rv_upper = rv.to_uppercase();
        if !["NONE", "ALL_OLD", "ALL_NEW", "UPDATED_OLD", "UPDATED_NEW"]
            .contains(&rv_upper.as_str())
        {
            return Err(DynoxideError::EnvelopedValidation(
                crate::validation::RETURN_VALUES_REJECTION.to_string(),
            ));
        }
    }

    // Validate key
    helpers::validate_key_only(&request.key, &key_schema)?;

    // Extract key values
    // TODO: validation must precede this call -- if reaching this line, caller has already validated keys.
    let (pk, sk) = helpers::extract_key_strings(&request.key, &key_schema)?;

    // Collect the set of attribute names affected by the legacy AttributeUpdates
    // parameter, used later for UPDATED_OLD / UPDATED_NEW extraction.
    let legacy_attr_names: Option<Vec<String>> = request
        .attribute_updates
        .as_ref()
        .map(|updates| updates.keys().cloned().collect());

    // Execution tracker: tracking disabled because unused-reference validation was
    // already done statically by Tracker 1 (pre-validation block above). This tracker
    // only needs name/value resolution, not usage tracking.
    let tracker = crate::expressions::TrackedExpressionAttributes::without_tracking(
        &request.expression_attribute_names,
        &request.expression_attribute_values,
    );

    // Hold the request to the 400KB limit before the item is read. DynamoDB
    // charges a standalone update for what its request carries, worked out from
    // the statement alone, so the charge needs nothing from the stored item and
    // is refused ahead of the condition, a SET on a key attribute and anything
    // else that only shows once the update is applied. Request validation above
    // still answers first. Captured against eu-west-2 on 9 October 2026; see
    // `UpdateExpr::request_charge` for the costs. The legacy AttributeUpdates
    // parameter carries no charge of its own.
    if let Some(ref update_expr) = request.update_expression {
        let parsed = crate::expressions::update::parse(update_expr)
            .map_err(DynoxideError::ValidationException)?;
        let charge = parsed
            .request_charge(&tracker)
            .map_err(DynoxideError::ValidationException)?;
        if charge > types::MAX_ITEM_SIZE {
            return Err(DynoxideError::ValidationException(
                "Item size to update has exceeded the maximum allowed size".to_string(),
            ));
        }
    }

    // Wrap the condition check, base write and the GSI/LSI fan-out in a single
    // transaction so a mid-fan-out failure rolls the whole update back, leaving
    // no torn index. Unconditional because the atomicity guarantee applies to
    // every single-item write. The block captures everything from get_item
    // through the GSI/LSI fan-out and stream record.
    let (
        UpdateWorkResult {
            old_item,
            item,
            item_json,
            size,
            old_size,
        },
        gsi_units,
        lsi_units,
        vector_bytes,
    ) = helpers::with_write_transaction(storage, async {
        // Fetch existing item (or create empty one for upsert)
        let existing_json = storage.get_item(&request.table_name, &pk, &sk).await?;
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
        if request.update_expression.is_none()
            && let Some(ref updates) = request.attribute_updates
        {
            apply_attribute_updates(&mut item, updates, &key_schema)?;
        }

        // Note: unused expression attribute validation already done in pre-validation
        // block (Tracker 1). Not repeated here: runtime evaluation may skip branches
        // (e.g., if_not_exists short-circuits) which would cause false positives.

        // An UpdateExpression that leaves the item nested too deep (a value
        // written far down an existing path) gets the short nesting message,
        // bare. Captured us-east-1 and eu-west-2, 2026-10-09.
        if request.update_expression.is_some() {
            crate::validation::validate_updated_item_nesting(&item)?;
        }

        // Validate attribute values after update expression applied. This
        // checks the post-update item, not the request, so its errors stay
        // bare: plain `?` converts through the untagged From impl. Do not
        // route it through into_tagged even though the messages match the
        // tagged request-time families byte for byte.
        crate::validation::validate_item_attribute_values(&item)?;
        crate::validation::normalize_item_sets(&mut item);

        // Reject an index key set to an invalid value, before the fan-out so the
        // error surfaces. Checks only what this update changed (see the helper).
        helpers::validate_updated_index_keys(&old_item, &item, &meta)?;

        // The finished item is held to the limit as well as the request charge
        // checked before the transaction. The charge binds while the statement
        // carries more than it adds to the item; once the stored item holds
        // enough besides, the item's own size does.
        if types::item_size(&item) > types::MAX_ITEM_SIZE {
            return Err(DynoxideError::ValidationException(
                "Item size to update has exceeded the maximum allowed size".to_string(),
            ));
        }

        // What the row records and the write is charged: the billed size, with
        // a vector attribute at 4 bytes per dimension. The limit above sizes it
        // by its numbers, as DynamoDB does.
        let vector_defs = super::vector_index::parse_vector_defs(&meta)?;
        let size = types::billed_item_size(&item, &vector_defs);

        // Serialize and store
        let item_json = serde_json::to_string(&item)
            .map_err(|e| DynoxideError::InternalServerError(e.to_string()))?;
        let hash_prefix = request
            .key
            .get(&key_schema.partition_key)
            .map(crate::storage::compute_hash_prefix)
            .unwrap_or_default();
        storage
            .put_item_with_hash(
                &request.table_name,
                &pk,
                &sk,
                &item_json,
                size,
                &hash_prefix,
            )
            .await?;

        // `old_item` is cloned after the key attributes are injected for the
        // upsert case, so on a create-through-update it is a `{pk, sk}` image
        // rather than an absent one. The index delta has to see genuine absence,
        // so gate on whether a row was actually read back.
        let prior_image = existing_json.is_some().then_some(&old_item);
        let old_size = prior_image.map(|old| types::billed_item_size(old, &vector_defs));

        let target = super::gsi::IndexWrite {
            table_name: &request.table_name,
            pk: &pk,
            sk: &sk,
            pk_attr: &key_schema.partition_key,
            sk_attr: key_schema.sort_key.as_deref(),
            old_item: prior_image,
            capacity_mode: request.return_consumed_capacity.as_deref(),
        };

        // Maintain GSI tables (inside the transaction)
        let gsi_units =
            super::gsi::maintain_gsis_after_write(storage, &meta, &target, &item).await?;

        // Maintain LSI tables (inside the transaction)
        let lsi_units =
            super::lsi::maintain_lsis_after_write(storage, &meta, &target, &item).await?;

        // Maintain vector index shadow tables (inside the transaction). A
        // REMOVE of the vector attribute derives no row, so this de-indexes
        // the item; restoring the attribute re-indexes it.
        let vector_bytes = super::vector_index::maintain_vector_indexes_after_write(
            storage, &meta, &target, &item,
        )
        .await?;

        // Record stream event (inside the transaction)
        crate::streams::record_stream_event(storage, &meta, prior_image, Some(&item)).await?;

        Ok((
            UpdateWorkResult {
                old_item,
                item,
                item_json,
                size,
                old_size,
            },
            gsi_units,
            lsi_units,
            vector_bytes,
        ))
    })
    .await?;

    // Handle ReturnValues
    let return_values = request.return_values.as_deref().unwrap_or("NONE");
    let attributes = match return_values.to_uppercase().as_str() {
        // An update that creates its item has no old image, even though
        // `old_item` already holds the key attributes it was seeded with.
        "ALL_OLD" => old_size.is_some().then_some(old_item),
        "ALL_NEW" => Some(item),
        "UPDATED_OLD" => {
            if let Some(ref update_expr) = request.update_expression {
                // Expression-based: extract only the attributes targeted by the expression.
                let parsed = crate::expressions::update::parse(update_expr)
                    .map_err(DynoxideError::ValidationException)?;
                omit_if_empty(extract_updated_attrs(
                    &old_item,
                    &parsed,
                    &request.expression_attribute_names,
                ))
            } else {
                // Legacy AttributeUpdates: extract the named attributes from the old item.
                legacy_attr_names
                    .as_ref()
                    .map(|names| extract_named_attrs(&old_item, names))
                    .and_then(omit_if_empty)
            }
        }
        "UPDATED_NEW" => {
            if let Some(ref update_expr) = request.update_expression {
                // Expression-based: extract only the attributes targeted by the expression.
                let parsed = crate::expressions::update::parse(update_expr)
                    .map_err(DynoxideError::ValidationException)?;
                let new_item: HashMap<String, AttributeValue> = serde_json::from_str(&item_json)
                    .map_err(|e| DynoxideError::InternalServerError(e.to_string()))?;
                omit_if_empty(extract_updated_attrs(
                    &new_item,
                    &parsed,
                    &request.expression_attribute_names,
                ))
            } else {
                // Legacy AttributeUpdates: extract the named attributes from the new item.
                legacy_attr_names
                    .as_ref()
                    .map(|names| {
                        let new_item: HashMap<String, AttributeValue> =
                            serde_json::from_str(&item_json).unwrap_or_default();
                        extract_named_attrs(&new_item, names)
                    })
                    .and_then(omit_if_empty)
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
    )
    .await?;

    let consumed_capacity = types::consumed_capacity_with_vector_indexes(
        &request.table_name,
        types::table_write_capacity_units(old_size, Some(size)),
        &gsi_units,
        &lsi_units,
        &vector_bytes,
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
            return Err(helpers::key_attribute_update_refusal(attr_name));
        }

        match update.action.as_str() {
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
                return Err(DynoxideError::EnvelopedValidation(
                    ATTRIBUTE_UPDATE_ACTION_REJECTION.to_string(),
                ));
            }
        }
    }
    Ok(())
}

/// Extract only the attributes that were affected by the update expression,
/// at full path granularity.
///
/// For a nested target like `SET parent.child = :v` this returns only the
/// changed fragment (`{parent: {M: {child}}}`), not the whole `parent` map,
/// matching how AWS scopes `UPDATED_NEW` / `UPDATED_OLD`. A path that no longer
/// resolves in `item` (a removed attribute under `UPDATED_NEW`) contributes
/// nothing, so a REMOVE-only update yields an empty map.
fn extract_updated_attrs(
    item: &HashMap<String, AttributeValue>,
    expr: &crate::expressions::update::UpdateExpr,
    attr_names: &Option<HashMap<String, String>>,
) -> HashMap<String, AttributeValue> {
    use crate::expressions::{PathElement, resolve_path, resolve_path_elements};

    let no_values: Option<HashMap<String, AttributeValue>> = None;
    let tracker =
        crate::expressions::TrackedExpressionAttributes::without_tracking(attr_names, &no_values);

    // Collect every target path across all clauses, in clause order.
    let mut paths: Vec<&[PathElement]> = Vec::new();
    paths.extend(expr.set_actions.iter().map(|a| a.path.as_slice()));
    paths.extend(expr.remove_actions.iter().map(|p| p.as_slice()));
    paths.extend(expr.add_actions.iter().map(|a| a.path.as_slice()));
    paths.extend(expr.delete_actions.iter().map(|a| a.path.as_slice()));

    let mut result = HashMap::new();
    for path in paths {
        let Ok(resolved) = resolve_path_elements(path, &tracker) else {
            continue;
        };

        // `insert_at_path` rebuilds a list from index 0, so it can't represent a
        // target that dives through a list index (`list[2]`) as a pruned
        // fragment without mislocating the element and dropping its siblings.
        // For those, fall back to returning the whole top-level attribute, the
        // coarse-but-correct shape. Pure attribute paths get the granular
        // fragment AWS scopes `UPDATED_NEW` / `UPDATED_OLD` to.
        if resolved.iter().any(|e| matches!(e, PathElement::Index(_))) {
            if let Some(PathElement::Attribute(top)) = resolved.first()
                && let Some(val) = item.get(top)
            {
                result.insert(top.clone(), val.clone());
            }
            continue;
        }

        if let Some(val) = resolve_path(item, &resolved) {
            crate::expressions::projection::insert_at_path(&mut result, &resolved, val);
        }
    }

    result
}

/// Collapse an empty projection to `None` so `Attributes` is omitted entirely.
///
/// AWS omits `Attributes` from a `UPDATED_NEW` / `UPDATED_OLD` response when
/// nothing was projected: for example a REMOVE-only update under `UPDATED_NEW`,
/// where no attribute was set to a new value. Returning `Some({})` instead would
/// serialise an empty `Attributes` map, which AWS never does.
fn omit_if_empty(map: HashMap<String, AttributeValue>) -> Option<HashMap<String, AttributeValue>> {
    if map.is_empty() { None } else { Some(map) }
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
            return Err(helpers::key_attribute_update_refusal(&resolved_name));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::actions::{create_table, put_item, update_item};
    use crate::storage::Storage;
    use crate::storage_backend::StorageBackend;

    #[test]
    fn update_item_stops_at_first_invalid_enum() {
        // eu-west-2: UpdateItem reports one error for the first invalid enum
        // (ReturnValues first), unlike PutItem which aggregates them.
        let err = serde_json::from_value::<super::UpdateItemRequest>(serde_json::json!({
            "TableName": "_conformance_valid_table_name",
            "Key": {"pk": {"S": "test"}},
            "ReturnValues": "INVALID",
            "ReturnConsumedCapacity": "INVALID"
        }))
        .unwrap_err()
        .to_string();
        assert!(err.contains("1 validation error detected"), "got: {err}");
        assert!(err.contains("enum value set"), "got: {err}");
        // The generic form names neither the value nor the member (captured
        // 2026-10-08): the enum list is what identifies ReturnValues.
        assert!(
            err.contains("[ALL_OLD, UPDATED_OLD, ALL_NEW, UPDATE_NEW, NONE]"),
            "got: {err}"
        );
        assert!(!err.contains("returnConsumedCapacity"), "got: {err}");
    }

    #[test]
    fn update_item_empty_table_name_reports_only_table_name() {
        let err = serde_json::from_value::<super::UpdateItemRequest>(serde_json::json!({
            "TableName": "",
            "Key": {}
        }))
        .unwrap_err()
        .to_string();
        assert!(err.contains("1 validation error detected"), "got: {err}");
        assert!(err.to_lowercase().contains("tablename"), "got: {err}");
    }

    /// An update and its GSI fan-out succeed or fail as one unit: a mid-fan-out
    /// failure leaves the item at its pre-update value.
    #[test]
    fn update_item_rolls_back_base_write_when_gsi_fan_out_fails() {
        let storage = Storage::memory().unwrap();

        let create = serde_json::from_value(serde_json::json!({
            "TableName": "Orders",
            "KeySchema": [{"AttributeName": "UserId", "KeyType": "HASH"}],
            "AttributeDefinitions": [
                {"AttributeName": "UserId", "AttributeType": "S"},
                {"AttributeName": "Status", "AttributeType": "S"},
                {"AttributeName": "Priority", "AttributeType": "S"}
            ],
            "GlobalSecondaryIndexes": [
                {"IndexName": "StatusIndex", "KeySchema": [{"AttributeName": "Status", "KeyType": "HASH"}], "Projection": {"ProjectionType": "ALL"}},
                {"IndexName": "PriorityIndex", "KeySchema": [{"AttributeName": "Priority", "KeyType": "HASH"}], "Projection": {"ProjectionType": "ALL"}}
            ]
        }))
        .unwrap();
        pollster::block_on(create_table::execute(&storage, create)).unwrap();

        let put = serde_json::from_value(serde_json::json!({
            "TableName": "Orders",
            "Item": {"UserId": {"S": "u1"}, "Status": {"S": "SHIPPED"}, "Priority": {"S": "HIGH"}, "Note": {"S": "before"}}
        }))
        .unwrap();
        pollster::block_on(put_item::execute(&storage, put)).unwrap();

        // Break the second GSI's fan-out by dropping its physical table.
        storage.drop_gsi_table("Orders", "PriorityIndex").unwrap();

        let update = serde_json::from_value(serde_json::json!({
            "TableName": "Orders",
            "Key": {"UserId": {"S": "u1"}},
            "UpdateExpression": "SET Note = :n",
            "ExpressionAttributeValues": {":n": {"S": "after"}}
        }))
        .unwrap();
        let res = pollster::block_on(update_item::execute(&storage, update));
        assert!(
            res.is_err(),
            "a mid-fan-out failure must surface as an error"
        );

        // The base write must roll back: the item is still present at its
        // pre-update value.
        let rows = pollster::block_on(<Storage as StorageBackend>::scan_items(
            &storage,
            "Orders",
            &Default::default(),
        ))
        .unwrap();
        assert_eq!(
            rows.len(),
            1,
            "the item must still be present after rollback"
        );
        let raw = &rows[0].2;
        assert!(
            raw.contains("\"before\"") && !raw.contains("\"after\""),
            "update must roll back when fan-out fails, leaving the original value: {raw}"
        );
    }
}
