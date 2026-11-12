use crate::actions::helpers;
use crate::errors::{DynoxideError, Result};
use crate::storage_backend::StorageBackend;
use crate::types::{self, AttributeValue, Item};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Internal deserialization struct for detecting missing fields.
#[derive(Debug, Default, Deserialize)]
struct DeleteItemRequestRaw {
    #[serde(rename = "TableName", default)]
    table_name: Option<String>,
    #[serde(
        rename = "Key",
        default,
        deserialize_with = "crate::types::deserialize_attribute_map"
    )]
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

use crate::validation::RETURN_VALUES_NOT_HONOURED;

/// Whether `return_values` is one DeleteItem refuses, for a request built in
/// Rust: anything but NONE and ALL_OLD, compared without regard to case, as
/// the check always was. A decoded request has had a value outside the enum
/// reported by the enum check already.
fn return_values_not_honoured(return_values: Option<&str>) -> bool {
    return_values.is_some_and(|rv| {
        let rv = rv.to_uppercase();
        rv != "NONE" && rv != "ALL_OLD"
    })
}

impl<'de> serde::Deserialize<'de> for DeleteItemRequest {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let raw = DeleteItemRequestRaw::deserialize(deserializer)?;
        use crate::validation::format_validation_errors;

        // A legacy condition value that can't be read (a boolean that doesn't
        // read as one, or a member of the wrong JSON type) makes the whole
        // request malformed, ahead of everything else.
        if let Some(message) = crate::types::malformed_condition_value(raw.expected.as_ref()) {
            return Err(serde::de::Error::custom(message));
        }

        // An empty name is the newer validation layer's own check and is
        // reported on its own; any other name error leads the member errors,
        // one per name (captured 2026-10-09, five of seven regions).
        if raw.table_name.as_deref() == Some("") {
            return Err(serde::de::Error::custom(format!(
                "VALIDATION:{}",
                crate::validation::envelope_message(crate::validation::EMPTY_TABLE_NAME_REJECTION)
            )));
        }
        let mut errors = crate::validation::item_table_name_errors(raw.table_name.as_deref());
        let table_name = raw.table_name.unwrap_or_default();

        // A missing key hands the request to the older layer, which lists a
        // ConditionalOperator outside its enum just ahead of the key, as
        // UpdateItem does (captured eu-west-2 and us-east-1, 2026-11-11).
        if raw.key.is_none() {
            errors.extend(crate::validation::conditional_operator_rejection(
                raw.conditional_operator.as_deref(),
            ));
            errors.push(
                "Value null at 'key' failed to satisfy constraint: \
                 Member must not be null"
                    .to_string(),
            );
        }

        // ReturnConsumedCapacity enum
        if let Some(msg) = crate::validation::return_consumed_capacity_rejection(
            raw.return_consumed_capacity.as_deref(),
        ) {
            errors.push(msg);
        }

        // ReturnValues enum
        if let Some(ref rv) = raw.return_values
            && !crate::validation::RETURN_VALUES.contains(&rv.as_str())
        {
            errors.push(crate::validation::RETURN_VALUES_REJECTION.to_string());
        }

        // ReturnItemCollectionMetrics enum
        if let Some(ref ricm) = raw.return_item_collection_metrics
            && !["SIZE", "NONE"].contains(&ricm.as_str())
        {
            errors.push(format!(
                "Value '{}' at 'returnItemCollectionMetrics' failed to satisfy constraint: \
                     Member must satisfy enum value set: [SIZE, NONE]",
                ricm
            ));
        }

        // ReturnValuesOnConditionCheckFailure enum, after the others
        // (captured 2026-10-09).
        if let Some(msg) = crate::validation::return_values_on_condition_check_failure_rejection(
            raw.return_values_on_condition_check_failure.as_deref(),
            "returnValuesOnConditionCheckFailure",
        ) {
            errors.push(msg);
        }

        // DeleteItem honours only NONE and ALL_OLD. Another value the enum
        // allows is a member error too, joined after the others, and, with a
        // key to go with it, ahead of every expression check (captured
        // eu-west-2 and us-east-1, 2026-10-09; a missing key is reported
        // alone).
        if raw.key.is_some()
            && raw
                .return_values
                .as_deref()
                .is_some_and(|rv| ["ALL_NEW", "UPDATED_OLD", "UPDATED_NEW"].contains(&rv))
        {
            errors.push(RETURN_VALUES_NOT_HONOURED.to_string());
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

pub async fn execute<S: StorageBackend>(
    storage: &S,
    request: DeleteItemRequest,
) -> Result<DeleteItemResponse> {
    // Apply the request-validation envelope exactly once at the operation
    // boundary, as PutItem and UpdateItem do, so every early return inside is
    // covered and the tagged variant never escapes.
    // A request read on the older layer never gets the envelope.
    let layer = crate::validation::single_write_layer(request.expected.as_ref());
    execute_inner(storage, request, layer)
        .await
        .map_err(|e| crate::validation::resolve_for_layer(layer, e))
}

async fn execute_inner<S: StorageBackend>(
    storage: &S,
    mut request: DeleteItemRequest,
    layer: crate::validation::ValueLayer,
) -> Result<DeleteItemResponse> {
    // Validate table name format before checking existence (DynamoDB validates input first)
    crate::validation::validate_item_table_name(&request.table_name)?;

    // A ReturnValues DeleteItem does not honour is a request member error, so
    // it is refused ahead of the expressions, as a decoded request is.
    if return_values_not_honoured(request.return_values.as_deref()) {
        return Err(DynoxideError::EnvelopedValidation(
            RETURN_VALUES_NOT_HONOURED.to_string(),
        ));
    }

    // An empty attribute name is refused as the request is read, ahead of every
    // other value check, the condition and the table lookup (captured in ten
    // regions). The values may still be raw JSON at this point.
    crate::empty_names::check_single_item_write(
        &request.key,
        false,
        request.expected.as_ref(),
        request.expression_attribute_names.as_ref(),
        request.expression_attribute_values.as_ref(),
        request.expression_attribute_values_raw.as_ref(),
    )?;

    // Validate expression/non-expression parameter conflicts
    {
        let mut non_expr = Vec::new();
        let mut expr_params = Vec::new();
        if request.expected.is_some() {
            non_expr.push("Expected");
        }
        if request.conditional_operator.is_some() {
            non_expr.push("ConditionalOperator");
        }
        if request.condition_expression.is_some() {
            expr_params.push("ConditionExpression");
        }
        let ctx = helpers::ExpressionParamContext {
            non_expression_params: non_expr,
            expression_params: expr_params,
            // DynamoDB names no expression after its "can only be specified
            // when using expressions" message on the single-item writes
            // (eu-west-2 and us-east-1, 2026-10-09).
            value_expression_params: &[],
            expression_attribute_names: &request.expression_attribute_names,
            expression_attribute_values: &request.expression_attribute_values,
            expression_attribute_values_raw: &request.expression_attribute_values_raw,
            layer,
        };
        // DeleteItem wraps the request-validation families in the envelope, as
        // PutItem and UpdateItem do: the nesting rejection, mixing expression
        // and non-expression parameters, either map without an expression and
        // either map empty (us-east-1 and eu-west-2, 2026-10-09).
        if let Some(parsed_eav) = helpers::validate_expression_params(&ctx)
            .map_err(crate::validation::ClassifiedValidationError::into_tagged)?
        {
            request.expression_attribute_values = Some(parsed_eav);
        }
    }

    // ConditionalOperator only joins legacy Expected conditions, so it is
    // refused without two of them, whatever its value. DeleteItem reports the
    // request-validation family without the tag, so the envelope is added here.
    if let Some(msg) = crate::validation::conditional_operator_misuse(
        request.conditional_operator.as_deref(),
        crate::validation::legacy_condition_count(request.expected.as_ref()),
    ) {
        return Err(DynoxideError::ValidationException(
            crate::validation::envelope_message(&msg),
        ));
    }

    // A key or Expected value nested too deep is refused, inside the envelope,
    // before the key is matched to the schema or the table looked up.
    crate::value_nesting::check_key_enveloped(&request.key)?;
    crate::value_nesting::check_expected(request.expected.as_ref())?;

    // Validate key attribute values (unsupported datatypes, invalid numbers).
    // A duplicated set member or a number it can't store comes back in the
    // envelope (eu-west-2, us-east-1, eu-west-1 and us-west-2, 2026-10-09).
    crate::validation::validate_key_values(&request.key, layer)
        .map_err(crate::validation::ClassifiedValidationError::into_tagged)?;

    // Validate empty ConditionExpression, inside the envelope and before the
    // table lookup (eu-west-2 and us-east-1, 2026-10-09).
    if let Some(ref ce) = request.condition_expression
        && ce.is_empty()
    {
        return Err(DynoxideError::EnvelopedValidation(
            "Invalid ConditionExpression: The expression can not be empty;".to_string(),
        ));
    }

    // Statically validate ConditionExpression (syntax + BETWEEN bounds, etc.)
    // before table lookup. Every error here comes back in the request-validation
    // envelope, as on PutItem and UpdateItem: syntax, size, redundant
    // parentheses, reserved words, BETWEEN bounds, operand types, undefined and
    // unused names and values (us-east-1 and eu-west-2, 2026-10-09).
    if let Some(ref ce) = request.condition_expression {
        let parsed = crate::expressions::condition::parse(ce).map_err(|e| {
            DynoxideError::ValidationException(crate::validation::envelope_message(&format!(
                "Invalid ConditionExpression: {e}"
            )))
        })?;
        crate::expressions::condition::validate_static(
            &parsed,
            &request.expression_attribute_values,
        )
        .map_err(|e| DynoxideError::ValidationException(crate::validation::envelope_message(&e)))?;
        crate::expressions::condition::validate_operand_semantics(
            &parsed,
            &request.expression_attribute_names,
            &request.expression_attribute_values,
        )
        .map_err(|e| {
            DynoxideError::ValidationException(crate::validation::envelope_message(&format!(
                "Invalid ConditionExpression: {e}"
            )))
        })?;

        // The condition's names and values, from the request alone: one it
        // names but the request doesn't define, then any it doesn't use.
        let tracker = crate::expressions::TrackedExpressionAttributes::new(
            &request.expression_attribute_names,
            &request.expression_attribute_values,
        );
        crate::expressions::condition::track_references(&parsed, &tracker).map_err(|e| {
            DynoxideError::ValidationException(crate::validation::envelope_message(&format!(
                "Invalid ConditionExpression: {e}"
            )))
        })?;
        tracker.check_unused().map_err(|e| match e {
            DynoxideError::ValidationException(msg) => {
                DynoxideError::ValidationException(crate::validation::envelope_message(&msg))
            }
            other => other,
        })?;
    }

    // Validate the legacy Expected parameter BEFORE checking table existence,
    // then convert it to a ConditionExpression. Both steps read the same map,
    // so it is deserialised once rather than once per step.
    let expected = match request.expected.as_ref() {
        Some(v) if request.condition_expression.is_none() => helpers::read_expected(v)?,
        _ => None,
    };
    if let Some(expected) = expected {
        // Every family comes back in the envelope, before the table lookup
        // (eu-west-2 and us-east-1, 2026-10-09).
        helpers::validate_expected_conditions(&expected)
            .map_err(crate::validation::tag_for_envelope)?;

        if !expected.is_empty() {
            let converted = helpers::convert_expected_to_condition(
                &expected,
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
    }

    let meta = helpers::require_table_for_item_op(storage, &request.table_name).await?;
    let key_schema = helpers::parse_key_schema(&meta)?;

    // Validate key
    helpers::validate_key_only(&request.key, &key_schema)?;

    // Extract key values
    // TODO: validation must precede this call -- if reaching this line, caller has already validated keys.
    let (pk, sk) = helpers::extract_key_strings(&request.key, &key_schema)?;

    // Check for unused expression attribute names/values
    let tracker = crate::expressions::TrackedExpressionAttributes::new(
        &request.expression_attribute_names,
        &request.expression_attribute_values,
    );

    // Pre-register all expression references statically so check_unused sees
    // every :value and #name, even those in short-circuited AND/OR branches.
    if let Some(ref cond_expr) = request.condition_expression
        && let Ok(parsed) = crate::expressions::condition::parse(cond_expr)
    {
        tracker.track_condition_expr(&parsed);
    }

    // Wrap the condition check, base delete and the GSI/LSI fan-out in a single
    // transaction so a mid-fan-out failure rolls the whole delete back, leaving
    // no torn index. Unconditional (not just for ConditionExpression) because
    // the atomicity guarantee applies to every single-item write.
    let (old_item, gsi_units, lsi_units, vector_bytes) =
        helpers::with_write_transaction(storage, async {
            // Evaluate ConditionExpression against existing item
            if let Some(ref cond_expr) = request.condition_expression {
                let existing_json = storage.get_item(&request.table_name, &pk, &sk).await?;
                let existing_item: HashMap<String, AttributeValue> = existing_json
                    .as_ref()
                    .and_then(|j| serde_json::from_str(j).ok())
                    .unwrap_or_default();

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

            // Check for unused expression attribute names/values
            tracker.check_unused()?;

            // Delete item (returns old item_json)
            let old_json = storage.delete_item(&request.table_name, &pk, &sk).await?;

            // Parse the old item once, here, for the index capacity delta, the
            // stream record and the response.
            let old_item: Option<Item> =
                old_json.as_ref().and_then(|j| serde_json::from_str(j).ok());

            let target = super::gsi::IndexWrite {
                table_name: &request.table_name,
                pk: &pk,
                sk: &sk,
                pk_attr: &key_schema.partition_key,
                sk_attr: key_schema.sort_key.as_deref(),
                old_item: old_item.as_ref(),
                capacity_mode: request.return_consumed_capacity.as_deref(),
            };

            // Maintain GSI tables (inside the transaction)
            let gsi_units = super::gsi::maintain_gsis_after_delete(storage, &meta, &target).await?;

            // Maintain LSI tables (inside the transaction)
            let lsi_units = super::lsi::maintain_lsis_after_delete(storage, &meta, &target).await?;

            // Maintain vector index shadow tables (inside the transaction)
            let vector_bytes =
                super::vector_index::maintain_vector_indexes_after_delete(storage, &meta, &target)
                    .await?;

            // Record stream event (inside the transaction)
            if old_item.is_some() {
                crate::streams::record_stream_event(storage, &meta, old_item.as_ref(), None)
                    .await?;
            }

            Ok((old_item, gsi_units, lsi_units, vector_bytes))
        })
        .await?;

    // Handle ReturnValues
    let return_old = request
        .return_values
        .as_deref()
        .unwrap_or("NONE")
        .eq_ignore_ascii_case("ALL_OLD");

    let attributes = if return_old { old_item.clone() } else { None };

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

    // Calculate consumed capacity from old item size (write for delete), on the
    // billed size, as every write is.
    let vector_defs = super::vector_index::parse_vector_defs(&meta)?;
    let consumed_capacity = types::consumed_capacity_with_vector_indexes(
        &request.table_name,
        types::table_write_capacity_units(
            old_item
                .as_ref()
                .map(|old| types::billed_item_size(old, &vector_defs)),
            None,
        ),
        &gsi_units,
        &lsi_units,
        &vector_bytes,
        &request.return_consumed_capacity,
    );

    Ok(DeleteItemResponse {
        attributes,
        consumed_capacity,
        item_collection_metrics,
    })
}

#[cfg(test)]
mod tests {
    use crate::actions::{create_table, delete_item, put_item};
    use crate::storage::Storage;
    use crate::storage_backend::StorageBackend;

    fn seed_two_gsi_table_with_item(storage: &Storage) {
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
        pollster::block_on(create_table::execute(storage, create)).unwrap();

        let put = serde_json::from_value(serde_json::json!({
            "TableName": "Orders",
            "Item": {"UserId": {"S": "u1"}, "Status": {"S": "SHIPPED"}, "Priority": {"S": "HIGH"}}
        }))
        .unwrap();
        pollster::block_on(put_item::execute(storage, put)).unwrap();
    }

    /// A delete and its GSI fan-out succeed or fail as one unit: a mid-fan-out
    /// failure leaves the item (and its index entries) in place.
    #[test]
    fn delete_item_rolls_back_base_delete_when_gsi_fan_out_fails() {
        let storage = Storage::memory().unwrap();
        seed_two_gsi_table_with_item(&storage);

        // Break the second GSI's fan-out by dropping its physical table.
        storage.drop_gsi_table("Orders", "PriorityIndex").unwrap();

        let del = serde_json::from_value(serde_json::json!({
            "TableName": "Orders",
            "Key": {"UserId": {"S": "u1"}}
        }))
        .unwrap();
        let res = pollster::block_on(delete_item::execute(&storage, del));
        assert!(
            res.is_err(),
            "a mid-fan-out failure must surface as an error"
        );

        // The base delete must roll back: the item is still present.
        let count =
            pollster::block_on(<Storage as StorageBackend>::count_items(&storage, "Orders"))
                .unwrap();
        assert_eq!(count, 1, "base delete must roll back when fan-out fails");
    }
}
