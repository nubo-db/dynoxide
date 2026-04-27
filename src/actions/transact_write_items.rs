use crate::actions::helpers;
use crate::errors::{CancellationReason, DynoxideError, Result};
use crate::storage::Storage;
use crate::types::{self, AttributeValue, Item};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct TransactWriteItemsRequest {
    #[serde(rename = "TransactItems")]
    pub transact_items: Vec<TransactWriteItem>,
    #[serde(rename = "ClientRequestToken", default)]
    pub client_request_token: Option<String>,
    #[serde(rename = "ReturnConsumedCapacity", default)]
    pub return_consumed_capacity: Option<String>,
    #[serde(rename = "ReturnItemCollectionMetrics", default)]
    pub return_item_collection_metrics: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct TransactWriteItem {
    #[serde(rename = "Put", default)]
    pub put: Option<TransactPut>,
    #[serde(rename = "Update", default)]
    pub update: Option<TransactUpdate>,
    #[serde(rename = "Delete", default)]
    pub delete: Option<TransactDelete>,
    #[serde(rename = "ConditionCheck", default)]
    pub condition_check: Option<TransactConditionCheck>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct TransactPut {
    #[serde(rename = "TableName")]
    pub table_name: String,
    #[serde(rename = "Item")]
    pub item: Item,
    #[serde(rename = "ConditionExpression", default)]
    pub condition_expression: Option<String>,
    #[serde(rename = "ExpressionAttributeNames", default)]
    pub expression_attribute_names: Option<HashMap<String, String>>,
    #[serde(rename = "ExpressionAttributeValues", default)]
    pub expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    #[serde(rename = "ReturnValuesOnConditionCheckFailure", default)]
    pub return_values_on_condition_check_failure: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct TransactUpdate {
    #[serde(rename = "TableName")]
    pub table_name: String,
    #[serde(rename = "Key")]
    pub key: HashMap<String, AttributeValue>,
    #[serde(rename = "UpdateExpression")]
    pub update_expression: String,
    #[serde(rename = "ConditionExpression", default)]
    pub condition_expression: Option<String>,
    #[serde(rename = "ExpressionAttributeNames", default)]
    pub expression_attribute_names: Option<HashMap<String, String>>,
    #[serde(rename = "ExpressionAttributeValues", default)]
    pub expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    #[serde(rename = "ReturnValuesOnConditionCheckFailure", default)]
    pub return_values_on_condition_check_failure: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct TransactDelete {
    #[serde(rename = "TableName")]
    pub table_name: String,
    #[serde(rename = "Key")]
    pub key: HashMap<String, AttributeValue>,
    #[serde(rename = "ConditionExpression", default)]
    pub condition_expression: Option<String>,
    #[serde(rename = "ExpressionAttributeNames", default)]
    pub expression_attribute_names: Option<HashMap<String, String>>,
    #[serde(rename = "ExpressionAttributeValues", default)]
    pub expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    #[serde(rename = "ReturnValuesOnConditionCheckFailure", default)]
    pub return_values_on_condition_check_failure: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct TransactConditionCheck {
    #[serde(rename = "TableName")]
    pub table_name: String,
    #[serde(rename = "Key")]
    pub key: HashMap<String, AttributeValue>,
    #[serde(rename = "ConditionExpression")]
    pub condition_expression: String,
    #[serde(rename = "ExpressionAttributeNames", default)]
    pub expression_attribute_names: Option<HashMap<String, String>>,
    #[serde(rename = "ExpressionAttributeValues", default)]
    pub expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    #[serde(rename = "ReturnValuesOnConditionCheckFailure", default)]
    pub return_values_on_condition_check_failure: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct TransactWriteItemsResponse {
    #[serde(rename = "ConsumedCapacity", skip_serializing_if = "Option::is_none")]
    pub consumed_capacity: Option<Vec<crate::types::ConsumedCapacity>>,
    /// Item collection metrics per table. Currently always `None` — full metrics
    /// computation for transactional writes is deferred to a future release.
    #[serde(
        rename = "ItemCollectionMetrics",
        skip_serializing_if = "Option::is_none"
    )]
    pub item_collection_metrics: Option<HashMap<String, Vec<crate::types::ItemCollectionMetrics>>>,
}

pub fn execute(
    storage: &Storage,
    request: TransactWriteItemsRequest,
) -> Result<TransactWriteItemsResponse> {
    let items = &request.transact_items;

    // Validate: at least 1 action
    if items.is_empty() {
        return Err(DynoxideError::ValidationException(
            "1 validation error detected: Value '[]' at 'transactItems' failed to satisfy constraint: Member must have length greater than or equal to 1".to_string(),
        ));
    }

    // Validate: up to 100 actions.
    // AWS surfaces this as the standard "1 validation error detected" envelope
    // around `Value '[<dump>]' at 'transactItems'`. The conformance suite
    // anchors a regex on the envelope and constraint phrase but leaves the
    // dump body unconstrained.
    if items.len() > 100 {
        let dump = format!("{items:?}");
        return Err(DynoxideError::ValidationException(format!(
            "1 validation error detected: Value '[{dump}]' at 'transactItems' failed to satisfy constraint: Member must have length less than or equal to 100"
        )));
    }

    // Validate: no duplicate item targets
    let mut seen_targets = HashSet::new();
    for item in items {
        let target = get_item_target(storage, item)?;
        if !seen_targets.insert(target) {
            return Err(DynoxideError::ValidationException(
                "Transaction request cannot include multiple operations on one item".to_string(),
            ));
        }
    }

    // Validate: aggregate item size must not exceed 4MB
    let total_size: usize = items.iter().map(|i| get_action_table_and_size(i).1).sum();
    if total_size > 4 * 1024 * 1024 {
        return Err(DynoxideError::ValidationException(
            "Collection size of items exceeded, which can also be caused by the aggregate size of the items in the transaction exceeding the 4MB limit".to_string(),
        ));
    }

    // Begin SQLite transaction
    storage.begin_transaction()?;

    let result = execute_within_transaction(storage, items);

    match result {
        Ok(()) => {
            storage.commit()?;
            // Build consumed capacity per table
            let consumed_capacity = if matches!(
                request.return_consumed_capacity.as_deref(),
                Some("TOTAL") | Some("INDEXES")
            ) {
                let mut table_sizes: HashMap<String, usize> = HashMap::new();
                for item in items {
                    let (table, size) = get_action_table_and_size(item);
                    *table_sizes.entry(table).or_default() += size;
                }
                let caps: Vec<_> = table_sizes
                    .iter()
                    .filter_map(|(table, &size)| {
                        crate::types::consumed_capacity(
                            table,
                            crate::types::write_capacity_units(size),
                            &request.return_consumed_capacity,
                        )
                    })
                    .collect();
                Some(caps)
            } else {
                None
            };
            Ok(TransactWriteItemsResponse {
                consumed_capacity,
                item_collection_metrics: None,
            })
        }
        Err(e) => {
            if let Err(rb_err) = storage.rollback() {
                return Err(DynoxideError::InternalServerError(format!(
                    "Transaction failed ({e}) and rollback also failed ({rb_err})"
                )));
            }
            Err(e)
        }
    }
}

fn execute_within_transaction(storage: &Storage, items: &[TransactWriteItem]) -> Result<()> {
    let mut cancellation_reasons: Vec<CancellationReason> = Vec::with_capacity(items.len());
    let mut has_failure = false;

    for item in items {
        let reason = execute_single_action(storage, item);
        match reason {
            Ok(()) => {
                cancellation_reasons.push(CancellationReason {
                    code: "None".to_string(),
                    message: None,
                    item: None,
                });
            }
            Err(e) => {
                has_failure = true;
                let message = Some(e.to_string());
                let (code, item) = match e {
                    DynoxideError::ConditionalCheckFailedException(_, item) => {
                        ("ConditionalCheckFailed".to_string(), item)
                    }
                    DynoxideError::ValidationException(_) => ("ValidationError".to_string(), None),
                    _ => ("InternalError".to_string(), None),
                };
                cancellation_reasons.push(CancellationReason {
                    code,
                    message,
                    item,
                });
            }
        }
    }

    if has_failure {
        let codes: Vec<&str> = cancellation_reasons
            .iter()
            .map(|r| r.code.as_str())
            .collect();
        let message = format!(
            "Transaction cancelled, please refer cancellation reasons for specific reasons [{}]",
            codes.join(", ")
        );
        return Err(DynoxideError::TransactionCanceledException(
            message,
            cancellation_reasons,
        ));
    }

    Ok(())
}

fn execute_single_action(storage: &Storage, item: &TransactWriteItem) -> Result<()> {
    if let Some(ref put) = item.put {
        execute_put(storage, put)
    } else if let Some(ref update) = item.update {
        execute_update(storage, update)
    } else if let Some(ref delete) = item.delete {
        execute_delete(storage, delete)
    } else if let Some(ref check) = item.condition_check {
        execute_condition_check(storage, check)
    } else {
        Err(DynoxideError::ValidationException(
            "TransactItem must contain exactly one of Put, Update, Delete, or ConditionCheck"
                .to_string(),
        ))
    }
}

fn execute_put(storage: &Storage, put: &TransactPut) -> Result<()> {
    crate::validation::validate_table_name(&put.table_name)?;
    let meta = helpers::require_table_for_item_op(storage, &put.table_name)?;
    let key_schema = helpers::parse_key_schema(&meta)?;

    helpers::validate_item_keys(&put.item, &key_schema, &meta)?;
    crate::validation::validate_item_attribute_values(&put.item)?;

    // Deduplicate sets - need a mutable copy since put is borrowed immutably
    let mut item = put.item.clone();
    crate::validation::normalize_item_sets(&mut item);

    let size = types::item_size(&item);
    if size > types::MAX_ITEM_SIZE {
        return Err(DynoxideError::ValidationException(
            "Item size has exceeded the maximum allowed size".to_string(),
        ));
    }

    let (pk, sk) = helpers::extract_key_strings(&item, &key_schema)?;

    let tracker = crate::expressions::TrackedExpressionAttributes::new(
        &put.expression_attribute_names,
        &put.expression_attribute_values,
    );

    // Pre-register references statically before runtime evaluation
    if let Some(ref cond_expr) = put.condition_expression {
        if let Ok(parsed) = crate::expressions::condition::parse(cond_expr) {
            tracker.track_condition_expr(&parsed);
        }
    }

    // Evaluate condition if present
    if let Some(ref cond_expr) = put.condition_expression {
        let existing_json = storage.get_item(&put.table_name, &pk, &sk)?;
        let existing_item: Item = existing_json
            .as_ref()
            .and_then(|j| serde_json::from_str(j).ok())
            .unwrap_or_default();

        let return_item = if put.return_values_on_condition_check_failure.as_deref()
            == Some("ALL_OLD")
            && !existing_item.is_empty()
        {
            Some(existing_item.clone())
        } else {
            None
        };
        check_condition_tracked(cond_expr, &existing_item, &tracker, return_item)?;
    }

    tracker.check_unused()?;

    let item_json = serde_json::to_string(&item)
        .map_err(|e| DynoxideError::InternalServerError(e.to_string()))?;
    let hash_prefix = item
        .get(&key_schema.partition_key)
        .map(crate::storage::compute_hash_prefix)
        .unwrap_or_default();
    let old_json =
        storage.put_item_with_hash(&put.table_name, &pk, &sk, &item_json, size, &hash_prefix)?;

    let _ = super::gsi::maintain_gsis_after_write(
        storage,
        &put.table_name,
        &meta,
        &pk,
        &sk,
        &item,
        &key_schema.partition_key,
        key_schema.sort_key.as_deref(),
    )?;

    super::lsi::maintain_lsis_after_write(
        storage,
        &put.table_name,
        &meta,
        &pk,
        &sk,
        &item,
        &key_schema.partition_key,
        key_schema.sort_key.as_deref(),
    )?;

    // Record stream event
    let old_item: Option<Item> = old_json.and_then(|j| serde_json::from_str(&j).ok());
    crate::streams::record_stream_event(storage, &meta, old_item.as_ref(), Some(&item))?;

    Ok(())
}

fn execute_update(storage: &Storage, update: &TransactUpdate) -> Result<()> {
    crate::validation::validate_table_name(&update.table_name)?;
    let meta = helpers::require_table_for_item_op(storage, &update.table_name)?;
    let key_schema = helpers::parse_key_schema(&meta)?;

    helpers::validate_key_only(&update.key, &key_schema)?;
    let (pk, sk) = helpers::extract_key_strings(&update.key, &key_schema)?;

    let existing_json = storage.get_item(&update.table_name, &pk, &sk)?;
    let existing_item: Item = existing_json
        .as_ref()
        .and_then(|j| serde_json::from_str(j).ok())
        .unwrap_or_default();

    let tracker = crate::expressions::TrackedExpressionAttributes::new(
        &update.expression_attribute_names,
        &update.expression_attribute_values,
    );

    // Pre-register references statically before runtime evaluation
    if let Some(ref cond_expr) = update.condition_expression {
        if let Ok(parsed) = crate::expressions::condition::parse(cond_expr) {
            tracker.track_condition_expr(&parsed);
        }
    }
    if let Ok(parsed) = crate::expressions::update::parse(&update.update_expression) {
        tracker.track_update_expr(&parsed);
    }

    // Evaluate condition against the original existing item BEFORE populating
    // key attributes for upsert. Otherwise attribute_exists(PK) would always
    // pass because the key was pre-populated.
    if let Some(ref cond_expr) = update.condition_expression {
        let return_item = if update.return_values_on_condition_check_failure.as_deref()
            == Some("ALL_OLD")
            && existing_json.is_some()
        {
            Some(existing_item.clone())
        } else {
            None
        };
        check_condition_tracked(cond_expr, &existing_item, &tracker, return_item)?;
    }

    // Build the mutable item for the update expression.
    // If new item (upsert), populate key attrs.
    let mut item = existing_item;
    if existing_json.is_none() {
        for (k, v) in &update.key {
            item.insert(k.clone(), v.clone());
        }
    }

    // Apply update expression
    let parsed = crate::expressions::update::parse(&update.update_expression)
        .map_err(DynoxideError::ValidationException)?;
    crate::expressions::update::apply(&mut item, &parsed, &tracker)
        .map_err(DynoxideError::ValidationException)?;

    tracker.check_unused()?;

    // Validate attribute values after update expression applied
    crate::validation::validate_item_attribute_values(&item)?;
    crate::validation::normalize_item_sets(&mut item);

    let size = types::item_size(&item);
    if size > types::MAX_ITEM_SIZE {
        return Err(DynoxideError::ValidationException(
            "Item size has exceeded the maximum allowed size".to_string(),
        ));
    }

    // Save old item reference for streams
    let old_for_stream = existing_json.clone();

    let item_json = serde_json::to_string(&item)
        .map_err(|e| DynoxideError::InternalServerError(e.to_string()))?;
    let hash_prefix = update
        .key
        .get(&key_schema.partition_key)
        .map(crate::storage::compute_hash_prefix)
        .unwrap_or_default();
    storage.put_item_with_hash(&update.table_name, &pk, &sk, &item_json, size, &hash_prefix)?;

    let _ = super::gsi::maintain_gsis_after_write(
        storage,
        &update.table_name,
        &meta,
        &pk,
        &sk,
        &item,
        &key_schema.partition_key,
        key_schema.sort_key.as_deref(),
    )?;

    super::lsi::maintain_lsis_after_write(
        storage,
        &update.table_name,
        &meta,
        &pk,
        &sk,
        &item,
        &key_schema.partition_key,
        key_schema.sort_key.as_deref(),
    )?;

    // Record stream event
    let old_item: Option<Item> = old_for_stream.and_then(|j| serde_json::from_str(&j).ok());
    crate::streams::record_stream_event(storage, &meta, old_item.as_ref(), Some(&item))?;

    Ok(())
}

fn execute_delete(storage: &Storage, delete: &TransactDelete) -> Result<()> {
    crate::validation::validate_table_name(&delete.table_name)?;
    let meta = helpers::require_table_for_item_op(storage, &delete.table_name)?;
    let key_schema = helpers::parse_key_schema(&meta)?;

    helpers::validate_key_only(&delete.key, &key_schema)?;
    let (pk, sk) = helpers::extract_key_strings(&delete.key, &key_schema)?;

    let tracker = crate::expressions::TrackedExpressionAttributes::new(
        &delete.expression_attribute_names,
        &delete.expression_attribute_values,
    );

    // Pre-register references statically before runtime evaluation
    if let Some(ref cond_expr) = delete.condition_expression {
        if let Ok(parsed) = crate::expressions::condition::parse(cond_expr) {
            tracker.track_condition_expr(&parsed);
        }
    }

    // Evaluate condition if present
    if let Some(ref cond_expr) = delete.condition_expression {
        let existing_json = storage.get_item(&delete.table_name, &pk, &sk)?;
        let existing_item: Item = existing_json
            .as_ref()
            .and_then(|j| serde_json::from_str(j).ok())
            .unwrap_or_default();

        let return_item = if delete.return_values_on_condition_check_failure.as_deref()
            == Some("ALL_OLD")
            && !existing_item.is_empty()
        {
            Some(existing_item.clone())
        } else {
            None
        };
        check_condition_tracked(cond_expr, &existing_item, &tracker, return_item)?;
    }

    tracker.check_unused()?;

    let old_json = storage.delete_item(&delete.table_name, &pk, &sk)?;
    let _ = super::gsi::maintain_gsis_after_delete(storage, &delete.table_name, &meta, &pk, &sk)?;
    super::lsi::maintain_lsis_after_delete(storage, &delete.table_name, &meta, &pk, &sk)?;

    // Record stream event
    let old_item: Option<Item> = old_json.and_then(|j| serde_json::from_str(&j).ok());
    if old_item.is_some() {
        crate::streams::record_stream_event(storage, &meta, old_item.as_ref(), None)?;
    }

    Ok(())
}

fn execute_condition_check(storage: &Storage, check: &TransactConditionCheck) -> Result<()> {
    crate::validation::validate_table_name(&check.table_name)?;
    let meta = helpers::require_table_for_item_op(storage, &check.table_name)?;
    let key_schema = helpers::parse_key_schema(&meta)?;

    helpers::validate_key_only(&check.key, &key_schema)?;
    let (pk, sk) = helpers::extract_key_strings(&check.key, &key_schema)?;

    let existing_json = storage.get_item(&check.table_name, &pk, &sk)?;
    let existing_item: Item = existing_json
        .as_ref()
        .and_then(|j| serde_json::from_str(j).ok())
        .unwrap_or_default();

    let tracker = crate::expressions::TrackedExpressionAttributes::new(
        &check.expression_attribute_names,
        &check.expression_attribute_values,
    );

    // Pre-register references statically before runtime evaluation
    if let Ok(parsed) = crate::expressions::condition::parse(&check.condition_expression) {
        tracker.track_condition_expr(&parsed);
    }

    let return_item = if check.return_values_on_condition_check_failure.as_deref()
        == Some("ALL_OLD")
        && !existing_item.is_empty()
    {
        Some(existing_item.clone())
    } else {
        None
    };
    check_condition_tracked(
        &check.condition_expression,
        &existing_item,
        &tracker,
        return_item,
    )?;

    tracker.check_unused()?;
    Ok(())
}

fn check_condition_tracked(
    expression: &str,
    item: &Item,
    tracker: &crate::expressions::TrackedExpressionAttributes,
    return_item_on_failure: Option<Item>,
) -> Result<()> {
    let parsed = crate::expressions::condition::parse(expression)
        .map_err(DynoxideError::ValidationException)?;
    let result = crate::expressions::condition::evaluate(&parsed, item, tracker)
        .map_err(DynoxideError::ValidationException)?;
    if !result {
        return Err(DynoxideError::ConditionalCheckFailedException(
            "The conditional request failed".to_string(),
            return_item_on_failure,
        ));
    }
    Ok(())
}

/// Get table name and estimated item size for an action.
///
/// For Put, uses the full item size. For Update, includes both the key size
/// and the expression attribute values size (a better approximation of the
/// request payload contribution). For Delete and ConditionCheck, uses key size.
fn get_action_table_and_size(item: &TransactWriteItem) -> (String, usize) {
    if let Some(ref put) = item.put {
        (put.table_name.clone(), types::item_size(&put.item))
    } else if let Some(ref update) = item.update {
        let key_size = types::item_size(&update.key);
        let eav_size = update
            .expression_attribute_values
            .as_ref()
            .map(|vals| vals.values().map(|v| v.size()).sum::<usize>())
            .unwrap_or(0);
        (update.table_name.clone(), key_size + eav_size)
    } else if let Some(ref delete) = item.delete {
        (delete.table_name.clone(), types::item_size(&delete.key))
    } else if let Some(ref check) = item.condition_check {
        (check.table_name.clone(), types::item_size(&check.key))
    } else {
        (String::new(), 0)
    }
}

/// Get a unique target key (table + pk + sk) for duplicate detection.
fn get_item_target(storage: &Storage, item: &TransactWriteItem) -> Result<String> {
    if let Some(ref put) = item.put {
        crate::validation::validate_table_name(&put.table_name)?;
        let meta = helpers::require_table_for_item_op(storage, &put.table_name)?;
        let key_schema = helpers::parse_key_schema(&meta)?;
        let (pk, sk) = helpers::extract_key_strings(&put.item, &key_schema)?;
        Ok(format!("{}#{}#{}", put.table_name, pk, sk))
    } else if let Some(ref update) = item.update {
        crate::validation::validate_table_name(&update.table_name)?;
        let meta = helpers::require_table_for_item_op(storage, &update.table_name)?;
        let key_schema = helpers::parse_key_schema(&meta)?;
        let (pk, sk) = helpers::extract_key_strings(&update.key, &key_schema)?;
        Ok(format!("{}#{}#{}", update.table_name, pk, sk))
    } else if let Some(ref delete) = item.delete {
        crate::validation::validate_table_name(&delete.table_name)?;
        let meta = helpers::require_table_for_item_op(storage, &delete.table_name)?;
        let key_schema = helpers::parse_key_schema(&meta)?;
        let (pk, sk) = helpers::extract_key_strings(&delete.key, &key_schema)?;
        Ok(format!("{}#{}#{}", delete.table_name, pk, sk))
    } else if let Some(ref check) = item.condition_check {
        crate::validation::validate_table_name(&check.table_name)?;
        let meta = helpers::require_table_for_item_op(storage, &check.table_name)?;
        let key_schema = helpers::parse_key_schema(&meta)?;
        let (pk, sk) = helpers::extract_key_strings(&check.key, &key_schema)?;
        Ok(format!("{}#{}#{}", check.table_name, pk, sk))
    } else {
        Err(DynoxideError::ValidationException(
            "TransactItem must contain exactly one action".to_string(),
        ))
    }
}
