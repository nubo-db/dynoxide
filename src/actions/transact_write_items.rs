use crate::actions::helpers;
use crate::actions::index_capacity::{
    WriteCapacity, aggregate_by_table, per_table_capacity, transactional_read_units,
};
use crate::errors::{CancellationReason, DynoxideError, Result};
use crate::storage_backend::StorageBackend;
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
    /// Item collection metrics per table. Currently always `None`: full metrics
    /// computation for transactional writes is deferred to a future release.
    #[serde(
        rename = "ItemCollectionMetrics",
        skip_serializing_if = "Option::is_none"
    )]
    pub item_collection_metrics: Option<HashMap<String, Vec<crate::types::ItemCollectionMetrics>>>,
}

/// A first-call result together with what a same-token replay needs to bill it.
///
/// The replay is charged against the images each action was sized on, which the
/// request cannot supply: a `Delete` or a `ConditionCheck` carries only a key.
/// Those sizes are internal bookkeeping, so they live here, on the type the
/// idempotency cache holds, rather than on the response type callers see.
#[derive(Debug, Clone, Default)]
pub(crate) struct CachedWrite {
    pub(crate) response: TransactWriteItemsResponse,
    /// Per-action `(table, image size)`.
    pub(crate) replay_sizes: Vec<(String, usize)>,
}

/// Run a transactional write.
///
/// Callers driving idempotency want [`execute_cached`], which also hands back
/// the sizes a replay is billed against.
pub async fn execute<S: StorageBackend>(
    storage: &S,
    request: TransactWriteItemsRequest,
) -> Result<TransactWriteItemsResponse> {
    Ok(execute_cached(storage, request).await?.response)
}

pub(crate) async fn execute_cached<S: StorageBackend>(
    storage: &S,
    request: TransactWriteItemsRequest,
) -> Result<CachedWrite> {
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

    // Validate: no duplicate item targets. A key that can't be stringified (non-scalar
    // or missing) is skipped here and reported by the in-loop validation instead (#95).
    let mut seen_targets = HashSet::new();
    for item in items {
        if let Some(target) = get_item_target(storage, item).await?
            && !seen_targets.insert(target)
        {
            return Err(DynoxideError::ValidationException(
                "Transaction request cannot include multiple operations on one item".to_string(),
            ));
        }
    }

    // Validate: each Put is within the item-size limit. This sits out here rather
    // than inside the action because a put's size is knowable from the request,
    // so DynamoDB answers it as a plain validation error before the transaction
    // runs. An update's size is not: it depends on the stored item, and comes
    // back as a cancellation reason instead. Captured against eu-west-2.
    for item in items {
        if let Some(ref put) = item.put
            && types::item_size(&put.item) > types::MAX_ITEM_SIZE
        {
            return Err(DynoxideError::ValidationException(
                "Item size has exceeded the maximum allowed size".to_string(),
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

    // All actions run inside one SQLite transaction (all-or-nothing).
    let capacity_mode = request.return_consumed_capacity.as_deref();
    let capacity = helpers::with_write_transaction(
        storage,
        execute_within_transaction(storage, items, capacity_mode),
    )
    .await?;

    Ok(CachedWrite {
        response: TransactWriteItemsResponse {
            consumed_capacity: build_write_capacity(&capacity, &request.return_consumed_capacity),
            item_collection_metrics: None,
        },
        replay_sizes: replay_sizes(&capacity),
    })
}

/// The image size each action was charged on, kept for a same-token replay.
///
/// The write is sized on the larger of the two images, and a capture shows the
/// replay charging against that same image, so the choice is made once here
/// rather than twice with a chance of drifting apart.
fn replay_sizes(capacity: &[WriteCapacity]) -> Vec<(String, usize)> {
    capacity
        .iter()
        .map(|record| {
            let larger = record
                .old_size
                .unwrap_or(0)
                .max(record.new_size.unwrap_or(0));
            (record.table_name.clone(), larger)
        })
        .collect()
}

/// Fold the per-action records into one `ConsumedCapacity` per table.
///
/// The transactional factor reaches the base table arm only. Index arms are
/// charged at their single-write cost, which is what a capture against real
/// DynamoDB reports: a transactional put of an indexed item costs table 2 and
/// gsi 1, where the same put outside a transaction costs table 1 and gsi 1.
fn build_write_capacity(
    capacity: &[WriteCapacity],
    mode: &Option<String>,
) -> Option<Vec<crate::types::ConsumedCapacity>> {
    // Checked before aggregating, not just inside the shared builder. Most
    // calls ask for no capacity at all, and aggregation walks every action and
    // allocates a map per table before the builder could discard it.
    if !matches!(mode.as_deref(), Some("TOTAL") | Some("INDEXES")) {
        return None;
    }

    let by_table = aggregate_by_table(capacity, crate::types::TRANSACTIONAL_CAPACITY_FACTOR);
    per_table_capacity(
        &by_table,
        mode,
        crate::types::transactional_write_capacity_with_indexes,
    )
}

/// Build the response for a same-token idempotent replay. The items are
/// identical to the first call (the idempotency hash matched), so capacity is
/// recomputed as a transactional READ against the image sizes the first call
/// recorded rather than re-serving its write numbers, honouring the replay
/// request's own `ReturnConsumedCapacity` mode (the original call's mode does
/// not carry over). The read cost is computed at 4KB read granularity, which
/// diverges from the first-call write magnitude above 1KB. `cached` is what the
/// first call stored: its response, and the image sizes to bill against.
pub(crate) fn replay_response(cached: &CachedWrite, mode: &Option<String>) -> CachedWrite {
    CachedWrite {
        response: TransactWriteItemsResponse {
            consumed_capacity: crate::types::build_transactional_capacity(
                &transactional_read_units(&cached.replay_sizes),
                mode,
                crate::types::transactional_read_capacity,
            ),
            item_collection_metrics: cached.response.item_collection_metrics.clone(),
        },
        replay_sizes: cached.replay_sizes.clone(),
    }
}

/// Run every action, returning what each contributed to `ConsumedCapacity`.
///
/// The records are only meaningful when the whole transaction commits; a
/// cancellation returns an error and reports no capacity at all.
async fn execute_within_transaction<S: StorageBackend>(
    storage: &S,
    items: &[TransactWriteItem],
    capacity_mode: Option<&str>,
) -> Result<Vec<WriteCapacity>> {
    let mut cancellation_reasons: Vec<CancellationReason> = Vec::with_capacity(items.len());
    let mut capacity: Vec<WriteCapacity> = Vec::with_capacity(items.len());
    let mut has_failure = false;

    for item in items {
        let reason = execute_single_action(storage, item, capacity_mode).await;
        match reason {
            Ok(action_capacity) => {
                capacity.push(action_capacity);
                cancellation_reasons.push(CancellationReason {
                    code: "None".to_string(),
                    message: None,
                    item: None,
                });
            }
            Err(e) => {
                // An empty-value key (empty string or empty binary) surfaces top-level:
                // returning here rolls the transaction back. Other errors become
                // cancellation reasons below (#95).
                if matches!(e, DynoxideError::KeyEmptyValueValidation(_)) {
                    return Err(e);
                }
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

    Ok(capacity)
}

async fn execute_single_action<S: StorageBackend>(
    storage: &S,
    item: &TransactWriteItem,
    capacity_mode: Option<&str>,
) -> Result<WriteCapacity> {
    if let Some(ref put) = item.put {
        execute_put(storage, put, capacity_mode).await
    } else if let Some(ref update) = item.update {
        execute_update(storage, update, capacity_mode).await
    } else if let Some(ref delete) = item.delete {
        execute_delete(storage, delete, capacity_mode).await
    } else if let Some(ref check) = item.condition_check {
        execute_condition_check(storage, check).await
    } else {
        Err(DynoxideError::ValidationException(
            "TransactItem must contain exactly one of Put, Update, Delete, or ConditionCheck"
                .to_string(),
        ))
    }
}

/// Reject any ExpressionAttributeValue that nests deeper than DynamoDB allows.
/// TransactWriteItems does not route through the shared expression-param helper, so
/// the per-value nesting check is applied here for each sub-action's values.
fn validate_eav_nesting(values: &Option<HashMap<String, AttributeValue>>) -> Result<()> {
    if let Some(map) = values {
        for value in map.values() {
            crate::validation::validate_nesting_depth(value)?;
        }
    }
    Ok(())
}

async fn execute_put<S: StorageBackend>(
    storage: &S,
    put: &TransactPut,
    capacity_mode: Option<&str>,
) -> Result<WriteCapacity> {
    crate::validation::validate_table_name(&put.table_name)?;
    let meta = helpers::require_table_for_item_op(storage, &put.table_name).await?;
    let key_schema = helpers::parse_key_schema(&meta)?;

    helpers::validate_item_keys(&put.item, &key_schema, &meta)?;
    crate::validation::validate_item_attribute_values(&put.item)?;

    // Deduplicate sets - need a mutable copy since put is borrowed immutably
    let mut item = put.item.clone();
    crate::validation::normalize_item_sets(&mut item);

    // The item-size limit was applied to this put before the transaction opened,
    // and normalising cannot grow the item, so there is nothing to recheck here.
    let size = types::item_size(&item);

    // TODO: validation must precede this call -- if reaching this line, caller has already validated keys.
    let (pk, sk) = helpers::extract_key_strings(&item, &key_schema)?;

    validate_eav_nesting(&put.expression_attribute_values)?;

    let tracker = crate::expressions::TrackedExpressionAttributes::new(
        &put.expression_attribute_names,
        &put.expression_attribute_values,
    );

    // Pre-register references statically before runtime evaluation
    if let Some(ref cond_expr) = put.condition_expression
        && let Ok(parsed) = crate::expressions::condition::parse(cond_expr)
    {
        tracker.track_condition_expr(&parsed);
    }

    // Evaluate condition if present
    if let Some(ref cond_expr) = put.condition_expression {
        let existing_json = storage.get_item(&put.table_name, &pk, &sk).await?;
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
    let old_json = storage
        .put_item_with_hash(&put.table_name, &pk, &sk, &item_json, size, &hash_prefix)
        .await?;

    let old_item: Option<Item> = old_json.and_then(|j| serde_json::from_str(&j).ok());

    let target = super::gsi::IndexWrite {
        table_name: &put.table_name,
        pk: &pk,
        sk: &sk,
        pk_attr: &key_schema.partition_key,
        sk_attr: key_schema.sort_key.as_deref(),
        old_item: old_item.as_ref(),
        capacity_mode,
    };

    let gsi_units = super::gsi::maintain_gsis_after_write(storage, &meta, &target, &item).await?;

    let lsi_units = super::lsi::maintain_lsis_after_write(storage, &meta, &target, &item).await?;

    let vector_bytes =
        super::vector_index::maintain_vector_indexes_after_write(storage, &meta, &target, &item)
            .await?;

    // Record stream event
    crate::streams::record_stream_event(storage, &meta, old_item.as_ref(), Some(&item)).await?;

    Ok(WriteCapacity::new(
        &put.table_name,
        old_item.as_ref().map(types::item_size),
        Some(size),
        gsi_units,
        lsi_units,
    )
    .with_vector_bytes(vector_bytes))
}

async fn execute_update<S: StorageBackend>(
    storage: &S,
    update: &TransactUpdate,
    capacity_mode: Option<&str>,
) -> Result<WriteCapacity> {
    crate::validation::validate_table_name(&update.table_name)?;
    let meta = helpers::require_table_for_item_op(storage, &update.table_name).await?;
    let key_schema = helpers::parse_key_schema(&meta)?;

    helpers::validate_key_only(&update.key, &key_schema)?;
    // TODO: validation must precede this call -- if reaching this line, caller has already validated keys.
    let (pk, sk) = helpers::extract_key_strings(&update.key, &key_schema)?;

    let existing_json = storage.get_item(&update.table_name, &pk, &sk).await?;
    let existing_item: Item = existing_json
        .as_ref()
        .and_then(|j| serde_json::from_str(j).ok())
        .unwrap_or_default();

    validate_eav_nesting(&update.expression_attribute_values)?;

    let tracker = crate::expressions::TrackedExpressionAttributes::new(
        &update.expression_attribute_names,
        &update.expression_attribute_values,
    );

    // Pre-register references statically before runtime evaluation
    if let Some(ref cond_expr) = update.condition_expression
        && let Ok(parsed) = crate::expressions::condition::parse(cond_expr)
    {
        tracker.track_condition_expr(&parsed);
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
    let before_item = item.clone();

    // Apply update expression
    let parsed = crate::expressions::update::parse(&update.update_expression)
        .map_err(DynoxideError::ValidationException)?;
    crate::expressions::update::apply(&mut item, &parsed, &tracker)
        .map_err(DynoxideError::ValidationException)?;

    tracker.check_unused()?;

    // Validate attribute values after update expression applied
    crate::validation::validate_item_attribute_values(&item)?;
    crate::validation::normalize_item_sets(&mut item);

    // Reject an index key this update set to an invalid value (see helpers).
    helpers::validate_updated_index_keys(&before_item, &item, &meta)?;

    // A transacted update is measured flat against the resulting item, unlike the
    // standalone UpdateItem, which takes the key attributes out and charges per
    // action. Both were captured against eu-west-2; the two really do differ.
    let size = types::item_size(&item);
    if size > types::MAX_ITEM_SIZE {
        return Err(DynoxideError::ValidationException(
            "Item size to update has exceeded the maximum allowed size".to_string(),
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
    storage
        .put_item_with_hash(&update.table_name, &pk, &sk, &item_json, size, &hash_prefix)
        .await?;

    let old_item: Option<Item> = old_for_stream.and_then(|j| serde_json::from_str(&j).ok());

    let target = super::gsi::IndexWrite {
        table_name: &update.table_name,
        pk: &pk,
        sk: &sk,
        pk_attr: &key_schema.partition_key,
        sk_attr: key_schema.sort_key.as_deref(),
        old_item: old_item.as_ref(),
        capacity_mode,
    };

    let gsi_units = super::gsi::maintain_gsis_after_write(storage, &meta, &target, &item).await?;

    let lsi_units = super::lsi::maintain_lsis_after_write(storage, &meta, &target, &item).await?;

    let vector_bytes =
        super::vector_index::maintain_vector_indexes_after_write(storage, &meta, &target, &item)
            .await?;

    // Record stream event
    crate::streams::record_stream_event(storage, &meta, old_item.as_ref(), Some(&item)).await?;

    // `old_item` comes from the stored JSON rather than from the assembled item,
    // which matters on an upsert: the assembled item carries the key attributes
    // this function injected, and charging against that would read as a key move
    // rather than an insert.
    Ok(WriteCapacity::new(
        &update.table_name,
        old_item.as_ref().map(types::item_size),
        Some(size),
        gsi_units,
        lsi_units,
    )
    .with_vector_bytes(vector_bytes))
}

async fn execute_delete<S: StorageBackend>(
    storage: &S,
    delete: &TransactDelete,
    capacity_mode: Option<&str>,
) -> Result<WriteCapacity> {
    crate::validation::validate_table_name(&delete.table_name)?;
    let meta = helpers::require_table_for_item_op(storage, &delete.table_name).await?;
    let key_schema = helpers::parse_key_schema(&meta)?;

    helpers::validate_key_only(&delete.key, &key_schema)?;
    // TODO: validation must precede this call -- if reaching this line, caller has already validated keys.
    let (pk, sk) = helpers::extract_key_strings(&delete.key, &key_schema)?;

    validate_eav_nesting(&delete.expression_attribute_values)?;

    let tracker = crate::expressions::TrackedExpressionAttributes::new(
        &delete.expression_attribute_names,
        &delete.expression_attribute_values,
    );

    // Pre-register references statically before runtime evaluation
    if let Some(ref cond_expr) = delete.condition_expression
        && let Ok(parsed) = crate::expressions::condition::parse(cond_expr)
    {
        tracker.track_condition_expr(&parsed);
    }

    // Evaluate condition if present
    if let Some(ref cond_expr) = delete.condition_expression {
        let existing_json = storage.get_item(&delete.table_name, &pk, &sk).await?;
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

    let old_json = storage.delete_item(&delete.table_name, &pk, &sk).await?;
    let old_item: Option<Item> = old_json.and_then(|j| serde_json::from_str(&j).ok());

    let target = super::gsi::IndexWrite {
        table_name: &delete.table_name,
        pk: &pk,
        sk: &sk,
        pk_attr: &key_schema.partition_key,
        sk_attr: key_schema.sort_key.as_deref(),
        old_item: old_item.as_ref(),
        capacity_mode,
    };

    let gsi_units = super::gsi::maintain_gsis_after_delete(storage, &meta, &target).await?;
    let lsi_units = super::lsi::maintain_lsis_after_delete(storage, &meta, &target).await?;
    let vector_bytes =
        super::vector_index::maintain_vector_indexes_after_delete(storage, &meta, &target).await?;

    // Record stream event
    if old_item.is_some() {
        crate::streams::record_stream_event(storage, &meta, old_item.as_ref(), None).await?;
    }

    Ok(WriteCapacity::from_items(
        &delete.table_name,
        old_item.as_ref(),
        None,
        gsi_units,
        lsi_units,
    )
    .with_vector_bytes(vector_bytes))
}

async fn execute_condition_check<S: StorageBackend>(
    storage: &S,
    check: &TransactConditionCheck,
) -> Result<WriteCapacity> {
    crate::validation::validate_table_name(&check.table_name)?;
    let meta = helpers::require_table_for_item_op(storage, &check.table_name).await?;
    let key_schema = helpers::parse_key_schema(&meta)?;

    helpers::validate_key_only(&check.key, &key_schema)?;
    // TODO: validation must precede this call -- if reaching this line, caller has already validated keys.
    let (pk, sk) = helpers::extract_key_strings(&check.key, &key_schema)?;

    let existing_json = storage.get_item(&check.table_name, &pk, &sk).await?;
    let existing_item: Item = existing_json
        .as_ref()
        .and_then(|j| serde_json::from_str(j).ok())
        .unwrap_or_default();

    validate_eav_nesting(&check.expression_attribute_values)?;

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
    // A check writes nothing and touches no index, and is still charged against
    // the image it read.
    Ok(WriteCapacity::condition_check(
        &check.table_name,
        existing_json.is_some().then_some(&existing_item),
    ))
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

/// Compute the dedup target (table + pk + sk) for one action's key source, or `None`
/// when the key can't be stringified (non-scalar or missing). Table name and existence
/// are still validated, so a bad name or missing table surfaces up front.
async fn target_for<S: StorageBackend>(
    storage: &S,
    table_name: &str,
    key_source: &HashMap<String, AttributeValue>,
) -> Result<Option<String>> {
    crate::validation::validate_table_name(table_name)?;
    let meta = helpers::require_table_for_item_op(storage, table_name).await?;
    let key_schema = helpers::parse_key_schema(&meta)?;
    match helpers::extract_key_strings(key_source, &key_schema) {
        Ok((pk, sk)) => Ok(Some(format!("{table_name}#{pk}#{sk}"))),
        Err(_) => Ok(None),
    }
}

/// Get a unique target key (table + pk + sk) for duplicate detection, or `None` when
/// the action's key can't form one (see [`target_for`]).
async fn get_item_target<S: StorageBackend>(
    storage: &S,
    item: &TransactWriteItem,
) -> Result<Option<String>> {
    if let Some(ref put) = item.put {
        target_for(storage, &put.table_name, &put.item).await
    } else if let Some(ref update) = item.update {
        target_for(storage, &update.table_name, &update.key).await
    } else if let Some(ref delete) = item.delete {
        target_for(storage, &delete.table_name, &delete.key).await
    } else if let Some(ref check) = item.condition_check {
        target_for(storage, &check.table_name, &check.key).await
    } else {
        Err(DynoxideError::ValidationException(
            "TransactItem must contain exactly one action".to_string(),
        ))
    }
}
