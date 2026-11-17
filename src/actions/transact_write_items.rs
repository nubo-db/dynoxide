use crate::actions::helpers;
use crate::actions::index_capacity::{WriteCapacity, aggregate_by_table, per_table_capacity};
use crate::errors::{CancellationReason, DynoxideError, Result};
use crate::storage_backend::StorageBackend;
use crate::types::{self, AttributeValue, Item};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Default, Serialize)]
pub struct TransactWriteItemsRequest {
    #[serde(rename = "TransactItems")]
    pub transact_items: Vec<TransactWriteItem>,
    #[serde(rename = "ClientRequestToken")]
    pub client_request_token: Option<String>,
    #[serde(rename = "ReturnConsumedCapacity")]
    pub return_consumed_capacity: Option<String>,
    #[serde(rename = "ReturnItemCollectionMetrics")]
    pub return_item_collection_metrics: Option<String>,
}

impl<'de> Deserialize<'de> for TransactWriteItemsRequest {
    /// Read the request as it arrives on the wire, where a required member
    /// may be left out, and raise the first stage of DynamoDB's member checks
    /// that finds anything ahead of a value refused in an item. The stages
    /// after that run in `check_request`.
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        wire::Request::deserialize(deserializer)?
            .into_request()
            .map_err(serde::de::Error::custom)
    }
}

/// One action of a transaction write.
///
/// This type and the four action types keep a derived `Deserialize` only so
/// the public API stays as it was. Neither the HTTP API nor the MCP tool reads
/// them with it: both read [`TransactWriteItemsRequest`], whose reader runs
/// DynamoDB's member checks in its order.
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
    #[serde(
        rename = "Item",
        deserialize_with = "crate::types::deserialize_required_attribute_map"
    )]
    pub item: Item,
    #[serde(rename = "ConditionExpression", default)]
    pub condition_expression: Option<String>,
    #[serde(
        rename = "ExpressionAttributeNames",
        default,
        deserialize_with = "crate::empty_names::deserialize_names"
    )]
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
    #[serde(
        rename = "Key",
        deserialize_with = "crate::types::deserialize_required_attribute_map"
    )]
    pub key: HashMap<String, AttributeValue>,
    #[serde(rename = "UpdateExpression")]
    pub update_expression: String,
    #[serde(rename = "ConditionExpression", default)]
    pub condition_expression: Option<String>,
    #[serde(
        rename = "ExpressionAttributeNames",
        default,
        deserialize_with = "crate::empty_names::deserialize_names"
    )]
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
    #[serde(
        rename = "Key",
        deserialize_with = "crate::types::deserialize_required_attribute_map"
    )]
    pub key: HashMap<String, AttributeValue>,
    #[serde(rename = "ConditionExpression", default)]
    pub condition_expression: Option<String>,
    #[serde(
        rename = "ExpressionAttributeNames",
        default,
        deserialize_with = "crate::empty_names::deserialize_names"
    )]
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
    #[serde(
        rename = "Key",
        deserialize_with = "crate::types::deserialize_required_attribute_map"
    )]
    pub key: HashMap<String, AttributeValue>,
    #[serde(rename = "ConditionExpression")]
    pub condition_expression: String,
    #[serde(
        rename = "ExpressionAttributeNames",
        default,
        deserialize_with = "crate::empty_names::deserialize_names"
    )]
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
    /// Per-action `(table, image size)`. Only the native `Database` replays a
    /// transaction: the wasm engine refuses `TransactWriteItems`.
    #[cfg(any(feature = "native-sqlite", feature = "_has-encryption"))]
    pub(crate) replay_sizes: Vec<(String, usize)>,
}

/// Run a transactional write.
///
/// Callers driving idempotency want `execute_cached`, which also hands back
/// the sizes a replay is billed against.
pub async fn execute<S: StorageBackend>(
    storage: &S,
    request: TransactWriteItemsRequest,
) -> Result<TransactWriteItemsResponse> {
    Ok(execute_cached(storage, request).await?.response)
}

/// Refuse an empty attribute name in one action: in the item or key (bare),
/// in its expression names, or in its expression values. The values are
/// reported naming their key, as eight of ten regions do; eu-west-2 and
/// eu-north-1 answer the bare message.
fn empty_name_in_write_action(item: &TransactWriteItem) -> Result<()> {
    let (attributes, names, values) = if let Some(ref put) = item.put {
        (
            &put.item,
            put.expression_attribute_names.as_ref(),
            put.expression_attribute_values.as_ref(),
        )
    } else if let Some(ref update) = item.update {
        (
            &update.key,
            update.expression_attribute_names.as_ref(),
            update.expression_attribute_values.as_ref(),
        )
    } else if let Some(ref delete) = item.delete {
        (
            &delete.key,
            delete.expression_attribute_names.as_ref(),
            delete.expression_attribute_values.as_ref(),
        )
    } else if let Some(ref check) = item.condition_check {
        (
            &check.key,
            check.expression_attribute_names.as_ref(),
            check.expression_attribute_values.as_ref(),
        )
    } else {
        return Ok(());
    };
    if crate::empty_names::map_has_empty_name(attributes) {
        return Err(crate::empty_names::bare());
    }
    crate::empty_names::check_names(names)?;
    if let Some(key) = crate::empty_names::value_placeholder(values, None) {
        return Err(DynoxideError::ValidationException(
            crate::empty_names::value_placeholder_message(key),
        ));
    }
    Ok(())
}

/// `TransactItems` holding no action, or more than 100, in the older form
/// DynamoDB words it in, with no list echoed.
const NO_ACTIONS: &str = "Value at 'TransactItems' failed to satisfy constraint: Member must have length greater than or equal to 1";
const TOO_MANY_ACTIONS: &str = "Value at 'TransactItems' failed to satisfy constraint: Member must have length less than or equal to 100";

/// An item that names no action, as DynamoDB words it beside the request's
/// enums.
const EMPTY_ACTION: &str = "Value '' at 'transactItems' failed to satisfy constraint: TransactWriteRequest should contain Delete or Put or Update or ConditionCheck";

/// The second stage of the member checks: the token's length, then the
/// length of `TransactItems`, in the older form and listed together. When
/// either is wrong nothing else is listed beside them, not the request's enums,
/// an empty action or a value refused in an item (captured eu-west-2 and
/// us-east-1, 2026-11-11).
fn length_errors(token: Option<&str>, actions: usize) -> Option<String> {
    let mut errors: Vec<String> = Vec::new();
    errors.extend(crate::older_token_constraint_error(token));
    if actions == 0 {
        errors.push(NO_ACTIONS.to_string());
    } else if actions > 100 {
        errors.push(TOO_MANY_ACTIONS.to_string());
    }
    crate::validation::format_validation_errors(&errors)
}

/// How many of the four actions an item names.
fn action_count(item: &TransactWriteItem) -> usize {
    [
        item.put.is_some(),
        item.update.is_some(),
        item.delete.is_some(),
        item.condition_check.is_some(),
    ]
    .into_iter()
    .filter(|&present| present)
    .count()
}

/// The request's two enums refused, if either is.
fn enum_errors(
    return_consumed_capacity: Option<&str>,
    return_item_collection_metrics: Option<&str>,
) -> Vec<String> {
    [
        crate::validation::return_consumed_capacity_rejection(return_consumed_capacity),
        crate::validation::return_item_collection_metrics_rejection(return_item_collection_metrics),
    ]
    .into_iter()
    .flatten()
    .collect()
}

/// Every action's table name errors, under its position and action type, such
/// as `transactItems.2.member.update.tableName`.
fn table_name_errors(items: &[TransactWriteItem]) -> Vec<String> {
    let mut errors = Vec::new();
    for (position, item) in items.iter().enumerate() {
        let (action, table_name) = if let Some(ref put) = item.put {
            ("put", &put.table_name)
        } else if let Some(ref update) = item.update {
            ("update", &update.table_name)
        } else if let Some(ref delete) = item.delete {
            ("delete", &delete.table_name)
        } else if let Some(ref check) = item.condition_check {
            ("conditionCheck", &check.table_name)
        } else {
            continue;
        };
        errors.extend(crate::validation::member_table_name_errors(
            table_name,
            &format!("transactItems.{}.member.{action}.tableName", position + 1),
        ));
    }
    errors
}

/// The third stage of the member checks. An item naming no action is
/// reported ahead of a bad `ReturnConsumedCapacity` or
/// `ReturnItemCollectionMetrics`, and every refused table name is left out
/// beside it. Otherwise every refused table name is listed, ahead of either
/// enum (captured eu-west-2 and us-east-1, 2026-11-11).
fn enum_stage_errors(
    items: &[TransactWriteItem],
    return_consumed_capacity: Option<&str>,
    return_item_collection_metrics: Option<&str>,
) -> Option<String> {
    let enums = enum_errors(return_consumed_capacity, return_item_collection_metrics);
    let ahead = if items.iter().any(|item| action_count(item) == 0) {
        vec![EMPTY_ACTION.to_string()]
    } else {
        table_name_errors(items)
    };
    crate::validation::format_validation_errors(&[ahead, enums].concat())
}

/// The stages of the member checks a value refused in an item comes after:
/// the token's length and the length of `TransactItems` (see
/// [`length_errors`]), then the request's enums with an item naming no action
/// or the table names (see [`enum_stage_errors`]), then whether any item names
/// more than one action (captured eu-west-2, us-east-1, eu-west-1 and
/// us-west-2, 2026-11-11). The request decoder runs them when it holds such a
/// value, and [`check_request`] runs them first.
fn stages_before_values(request: &TransactWriteItemsRequest) -> Option<String> {
    let items = &request.transact_items;
    length_errors(request.client_request_token.as_deref(), items.len())
        .or_else(|| {
            enum_stage_errors(
                items,
                request.return_consumed_capacity.as_deref(),
                request.return_item_collection_metrics.as_deref(),
            )
        })
        .or_else(|| {
            items.iter().any(|item| action_count(item) > 1).then(|| {
                "TransactItems can only contain one of Check, Put, Update or Delete".to_string()
            })
        })
}

/// The first action, in request order, whose
/// `ReturnValuesOnConditionCheckFailure` DynamoDB refuses. Only that one is
/// reported, however many are wrong (captured eu-west-2 and us-east-1,
/// 2026-11-11).
fn first_mode_error(items: &[TransactWriteItem]) -> Option<String> {
    items.iter().enumerate().find_map(|(position, item)| {
        [
            (
                "put",
                item.put
                    .as_ref()
                    .map(|a| &a.return_values_on_condition_check_failure),
            ),
            (
                "update",
                item.update
                    .as_ref()
                    .map(|a| &a.return_values_on_condition_check_failure),
            ),
            (
                "delete",
                item.delete
                    .as_ref()
                    .map(|a| &a.return_values_on_condition_check_failure),
            ),
            (
                "conditionCheck",
                item.condition_check
                    .as_ref()
                    .map(|a| &a.return_values_on_condition_check_failure),
            ),
        ]
        .into_iter()
        .find_map(|(action, value)| {
            crate::validation::return_values_on_condition_check_failure_rejection(
                value?.as_deref(),
                &format!(
                    "transactItems.{}.member.{action}.returnValuesOnConditionCheckFailure",
                    position + 1
                ),
            )
        })
    })
}

/// The request-level checks, in the order DynamoDB reports them, as
/// eu-west-2 and us-east-1 answered on 2026-11-11.
///
/// A required member left null comes first, but a request with one never
/// gets here: it is refused as it is read. So is a value refused in an item,
/// once the stages [`stages_before_values`] runs have passed. Last comes the
/// first action's `ReturnValuesOnConditionCheckFailure` that is wrong.
///
/// Called at the entry point ahead of the idempotency cache as well as at the
/// start of `execute_cached`. A same-token replay never reaches
/// `execute_cached`, and the token is keyed on the items alone, so a second
/// call may carry a capacity mode the first one never did. Running the same
/// checks in both places keeps their order the same whichever path answers.
pub(crate) fn check_request(request: &TransactWriteItemsRequest) -> Result<()> {
    let items = &request.transact_items;
    let refuse = |msg: String| Err(DynoxideError::ValidationException(msg));

    if let Some(msg) = stages_before_values(request) {
        return refuse(msg);
    }
    if let Some(msg) = first_mode_error(items) {
        return refuse(crate::validation::envelope_message(&msg));
    }
    Ok(())
}

mod wire;

pub(crate) async fn execute_cached<S: StorageBackend>(
    storage: &S,
    request: TransactWriteItemsRequest,
) -> Result<CachedWrite> {
    check_request(&request)?;
    let items = &request.transact_items;

    // An empty attribute name anywhere in an action is a top-level rejection,
    // after the request's own members and ahead of the table lookups and the
    // transaction (captured in ten regions).
    for item in items {
        empty_name_in_write_action(item)?;
    }

    // Validate: each action's request-only checks (value depth, Put item,
    // expression sizes and condition syntax), in request order, before any
    // table is looked up (#223). The request's own members come first, as
    // DynamoDB reports them (captured 2026-10-09).
    for item in items {
        validate_action_request(item)?;
    }

    // A Delete, ConditionCheck or Update key nested too deep fails the whole
    // request at the top level, before any table is looked up.
    for item in items {
        let key = item
            .delete
            .as_ref()
            .map(|d| &d.key)
            .or(item.condition_check.as_ref().map(|c| &c.key))
            .or(item.update.as_ref().map(|u| &u.key));
        if let Some(key) = key {
            crate::value_nesting::check_key(key)?;
        }
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
        #[cfg(any(feature = "native-sqlite", feature = "_has-encryption"))]
        replay_sizes: replay_sizes(&capacity),
    })
}

/// The image size each action was charged on, kept for a same-token replay.
///
/// The write is sized on the larger of the two images, and a capture shows the
/// replay charging against that same image, so the choice is made once here
/// rather than twice with a chance of drifting apart.
#[cfg(any(feature = "native-sqlite", feature = "_has-encryption"))]
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
#[cfg(any(feature = "native-sqlite", feature = "_has-encryption"))]
pub(crate) fn replay_response(cached: &CachedWrite, mode: &Option<String>) -> CachedWrite {
    CachedWrite {
        response: TransactWriteItemsResponse {
            consumed_capacity: crate::types::build_transactional_capacity(
                &crate::actions::index_capacity::transactional_read_units(&cached.replay_sizes),
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
///
/// Every action runs even after one has failed, so the cancellation gives a
/// reason for each. A failure of the storage itself (a full disk, an I/O
/// error, anything not reported as a failed condition or a validation error)
/// stops the transaction there and is returned as it is: SQLite may already
/// have rolled the whole transaction back for it, and an action run after
/// that would commit on its own.
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
                let message = Some(e.to_string());
                let (code, item) = match e {
                    DynoxideError::ConditionalCheckFailedException(_, item) => {
                        ("ConditionalCheckFailed".to_string(), item)
                    }
                    DynoxideError::ValidationException(_) => ("ValidationError".to_string(), None),
                    storage_failure => return Err(storage_failure),
                };
                has_failure = true;
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

/// The checks DynamoDB makes on one action from the request alone, before the
/// transaction looks at any table.
///
/// Each failure is a top-level ValidationException rather than a cancellation
/// reason, so nothing is written even when an earlier action was fine. The
/// actions are taken in request order and the first failure wins, ahead of a
/// missing table, a duplicate target or an empty key value anywhere in the
/// request. Captured in us-east-1, us-east-2, us-west-2, sa-east-1, eu-west-1,
/// eu-central-1, ap-northeast-1 and ap-southeast-2, 2026-10-08, and in
/// us-east-1 again for the order within an action, 2026-10-09.
///
/// Within one action the `ExpressionAttributeValues` depth comes first, then a
/// Put's item depth, its condition size and its item size, or an Update's
/// UpdateExpression size and then its condition size. The condition's syntax
/// comes last; where it sits against the other checks in the same action was
/// not captured, only that it runs up front.
///
/// eu-west-2 and eu-north-1 are part way through a change: on 2026-10-08 and
/// 2026-10-09 they refused an `ExpressionAttributeValues` entry from 32 levels
/// with the bare nesting message, left the size figure off the
/// ConditionExpression message, and put a Put's item depth ahead of its
/// condition size, where us-east-1 reports the size first. Dynoxide follows
/// the majority except on that last point, which keeps PutItem's order.
fn validate_action_request(item: &TransactWriteItem) -> Result<()> {
    // Every action's sets are checked first, empty or with a repeated member,
    // in its item or key and its expression values: ahead of a missing table
    // and a condition's syntax error, as a top-level refusal (us-east-1,
    // eu-west-1 and us-west-2, 2026-10-09), worded on the current layer in
    // every region (captured eu-west-2, us-east-1, eu-west-1 and us-west-2,
    // 2026-11-11).
    validate_action_sets(item)?;
    if let Some(ref put) = item.put {
        validate_expression_value_depth(&put.expression_attribute_values)?;
        for value in put.item.values() {
            crate::validation::validate_nesting_depth(value)?;
        }
        validate_condition_size(put.condition_expression.as_deref())?;
        // A put's size is knowable from the request, unlike an update's, which
        // depends on the stored item and comes back as a cancellation reason.
        if types::item_size(&put.item) > types::MAX_ITEM_SIZE {
            return Err(DynoxideError::ValidationException(
                "Item size has exceeded the maximum allowed size".to_string(),
            ));
        }
        validate_condition_syntax(put.condition_expression.as_deref())?;
        validate_expression_references(
            &put.expression_attribute_names,
            &put.expression_attribute_values,
            put.condition_expression.as_deref(),
            None,
        )?;
    } else if let Some(ref update) = item.update {
        validate_expression_value_depth(&update.expression_attribute_values)?;
        // Unlike the ConditionExpression form, this message carries no size
        // figure, in every region captured.
        if update.update_expression.len() > crate::expressions::MAX_EXPRESSION_BYTES {
            return Err(DynoxideError::ValidationException(
                "Invalid UpdateExpression: Expression size has exceeded the maximum allowed size;"
                    .to_string(),
            ));
        }
        // An empty expression is refused here, ahead of the table lookup,
        // where it used to parse as an update that changes nothing and write
        // the key alone (captured eu-west-2 and us-east-1, 2026-10-09).
        if update.update_expression.is_empty() {
            return Err(DynoxideError::ValidationException(
                "Invalid UpdateExpression: The expression can not be empty;".to_string(),
            ));
        }
        let update_expression = crate::expressions::update::parse(&update.update_expression)
            .map_err(DynoxideError::ValidationException)?;
        validate_condition_size(update.condition_expression.as_deref())?;
        validate_condition_syntax(update.condition_expression.as_deref())?;
        validate_expression_references(
            &update.expression_attribute_names,
            &update.expression_attribute_values,
            update.condition_expression.as_deref(),
            Some(&update_expression),
        )?;
    } else if let Some(ref delete) = item.delete {
        validate_expression_value_depth(&delete.expression_attribute_values)?;
        validate_condition_size(delete.condition_expression.as_deref())?;
        validate_condition_syntax(delete.condition_expression.as_deref())?;
        validate_expression_references(
            &delete.expression_attribute_names,
            &delete.expression_attribute_values,
            delete.condition_expression.as_deref(),
            None,
        )?;
    } else if let Some(ref check) = item.condition_check {
        validate_expression_value_depth(&check.expression_attribute_values)?;
        validate_condition_size(Some(&check.condition_expression))?;
        validate_condition_syntax(Some(&check.condition_expression))?;
        validate_expression_references(
            &check.expression_attribute_names,
            &check.expression_attribute_values,
            Some(&check.condition_expression),
            None,
        )?;
    }
    Ok(())
}

/// The empty or duplicated sets in one action, item or key first and then its
/// expression values, each in key order, worded by the layer the request's
/// values were read on. A refused expression value is reported bare, as
/// DynamoDB reports it in every region (captured eu-west-2, us-east-1,
/// eu-west-1 and us-west-2, 2026-11-11).
fn validate_action_sets(item: &TransactWriteItem) -> Result<()> {
    let (values, values_map) = if let Some(ref put) = item.put {
        (&put.item, &put.expression_attribute_values)
    } else if let Some(ref update) = item.update {
        (&update.key, &update.expression_attribute_values)
    } else if let Some(ref delete) = item.delete {
        (&delete.key, &delete.expression_attribute_values)
    } else if let Some(ref check) = item.condition_check {
        (&check.key, &check.expression_attribute_values)
    } else {
        return Ok(());
    };
    let layer = crate::types::current_value_layer();
    let in_key_order = |map: &HashMap<String, AttributeValue>| {
        let mut names: Vec<&String> = map.keys().collect();
        names.sort();
        names
            .into_iter()
            .find_map(|name| crate::validation::find_set_error(&map[name], layer))
    };
    match in_key_order(values).or_else(|| values_map.as_ref().and_then(in_key_order)) {
        Some(message) => Err(DynoxideError::ValidationException(message)),
        None => Ok(()),
    }
}

/// The names and values an action's expressions refer to, checked from the
/// request alone: a value the condition names but the request doesn't define,
/// then any name or value no expression uses (names first). Both are refused
/// up front, ahead of a missing table, a duplicate target or a bad key, and in
/// request order against the other actions' checks. Captured us-east-1 and
/// eu-west-2, 2026-10-09.
///
/// An action with no expression at all is left alone. us-east-1 accepts a Put
/// that carries ExpressionAttributeValues and no ConditionExpression and
/// writes it; eu-west-2 refuses it ("ExpressionAttributeValues can only be
/// specified when using expressions"). Dynoxide follows us-east-1.
///
/// An UpdateExpression's references are only marked as used here; whether
/// they resolve is still checked as the action runs.
fn validate_expression_references(
    names: &Option<HashMap<String, String>>,
    values: &Option<HashMap<String, AttributeValue>>,
    condition: Option<&str>,
    update: Option<&crate::expressions::update::UpdateExpr>,
) -> Result<()> {
    if condition.is_none() && update.is_none() {
        return Ok(());
    }
    let tracker = crate::expressions::TrackedExpressionAttributes::new(names, values);
    if let Some(update) = update {
        tracker.track_update_expr(update);
        // Overlapping target paths are refused here, before anything is
        // written, without the envelope UpdateItem's carries.
        crate::expressions::update::check_target_overlaps(update, &tracker)
            .map_err(DynoxideError::ValidationException)?;
    }
    if let Some(condition) = condition {
        // Syntax has already been checked, so this parses.
        let parsed = crate::expressions::condition::parse(condition).map_err(|e| {
            DynoxideError::ValidationException(format!("Invalid ConditionExpression: {e}"))
        })?;
        crate::expressions::condition::track_references(&parsed, &tracker).map_err(|e| {
            DynoxideError::ValidationException(format!("Invalid ConditionExpression: {e}"))
        })?;
    }
    tracker.check_unused()
}

/// The deepest `ExpressionAttributeValues` entry a transaction accepts, one past
/// the stored-item limit. A 32-level entry that is only compared is evaluated,
/// and one an Update writes is caught by the stored-item cap on the finished
/// item; from 33 levels the entry is refused up front, used or not.
const MAX_EXPRESSION_VALUE_DEPTH: usize = 32;

fn validate_expression_value_depth(values: &Option<HashMap<String, AttributeValue>>) -> Result<()> {
    let Some(values) = values else {
        return Ok(());
    };
    let mut keys: Vec<&String> = values.keys().collect();
    keys.sort();
    for key in keys {
        if crate::validation::nesting_depth(&values[key]) > MAX_EXPRESSION_VALUE_DEPTH {
            return Err(DynoxideError::ValidationException(format!(
                "ExpressionAttributeValues contains invalid value: Nesting Levels have exceeded \
                 supported limits: Attributes in the item have nested levels beyond supported \
                 limit for key {key}"
            )));
        }
    }
    Ok(())
}

/// An empty or unparseable ConditionExpression is refused up front, where it
/// used to surface as a cancellation reason. Captured for a ConditionCheck in
/// us-east-1 and eu-west-2, 2026-10-09; the other actions take the same member
/// and PutItem, UpdateItem and DeleteItem refuse it the same way.
fn validate_condition_syntax(expression: Option<&str>) -> Result<()> {
    let Some(expr) = expression else {
        return Ok(());
    };
    if expr.is_empty() {
        return Err(DynoxideError::ValidationException(
            "Invalid ConditionExpression: The expression can not be empty;".to_string(),
        ));
    }
    crate::expressions::condition::parse(expr).map_err(|e| {
        DynoxideError::ValidationException(format!("Invalid ConditionExpression: {e}"))
    })?;
    Ok(())
}

fn validate_condition_size(expression: Option<&str>) -> Result<()> {
    if let Some(expr) = expression {
        crate::expressions::check_expression_size_with_figure(expr).map_err(|e| {
            DynoxideError::ValidationException(format!("Invalid ConditionExpression: {e}"))
        })?;
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
    // What the row records and the write is charged is the billed size, with a
    // vector attribute at 4 bytes per dimension.
    let vector_defs = super::vector_index::parse_vector_defs(&meta)?;
    let size = types::billed_item_size(&item, &vector_defs);

    // TODO: validation must precede this call -- if reaching this line, caller has already validated keys.
    let (pk, sk) = helpers::extract_key_strings(&item, &key_schema)?;

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

    // With no ConditionExpression there is nothing to check the values
    // against, and the transaction goes ahead (see validate_expression_references).
    if put.condition_expression.is_some() {
        tracker.check_unused()?;
    }

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
        old_item
            .as_ref()
            .map(|old| types::billed_item_size(old, &vector_defs)),
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

    // A transaction cancels an Update whose finished item is nested too deep
    // with the short form of the nesting message, without the clause about the
    // item's attributes. Captured us-east-1, 2026-10-09.
    crate::validation::validate_updated_item_nesting(&item)?;

    // Validate attribute values after update expression applied
    crate::validation::validate_item_attribute_values(&item)?;
    crate::validation::normalize_item_sets(&mut item);

    // Reject an index key this update set to an invalid value (see helpers).
    helpers::validate_updated_index_keys(&before_item, &item, &meta)?;

    // A transacted update is measured flat against the resulting item, unlike the
    // standalone UpdateItem, which is also charged for what its request carries.
    // The flat measure is what 30 regions applied on 2026-10-08. eu-west-2,
    // eu-north-1 and ap-northeast-2 began charging a transacted update for its
    // request up front that day, mid-rollout; this follows the majority.
    if types::item_size(&item) > types::MAX_ITEM_SIZE {
        return Err(DynoxideError::ValidationException(
            "Item size to update has exceeded the maximum allowed size".to_string(),
        ));
    }
    let vector_defs = super::vector_index::parse_vector_defs(&meta)?;
    let size = types::billed_item_size(&item, &vector_defs);

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
        old_item
            .as_ref()
            .map(|old| types::billed_item_size(old, &vector_defs)),
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

    // With no ConditionExpression there is nothing to check the values
    // against, and the transaction goes ahead (see validate_expression_references).
    if delete.condition_expression.is_some() {
        tracker.check_unused()?;
    }

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

    let vector_defs = super::vector_index::parse_vector_defs(&meta)?;
    Ok(WriteCapacity::new(
        &delete.table_name,
        old_item
            .as_ref()
            .map(|old| types::billed_item_size(old, &vector_defs)),
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
        &super::vector_index::parse_vector_defs(&meta)?,
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The public action types' own reader stops at the first expression value
    /// refused for its number and refuses it bare, as the request reader does
    /// on the current layer (captured eu-west-2, us-east-1, eu-west-1 and
    /// us-west-2, 2026-11-11).
    #[test]
    fn the_derived_read_stops_at_the_first_refused_expression_value() {
        let values = serde_json::json!({":a": {"N": "xyz"}, ":b": {"N": "abc"}});
        let expected = format!(
            "{}The parameter cannot be converted to a numeric value: xyz",
            crate::serde_errors::REQUEST_VALIDATION_MARKER
        );
        let put = serde_json::json!({
            "TableName": "t",
            "Item": {"pk": {"S": "a"}},
            "ConditionExpression": "a = :a AND b = :b",
            "ExpressionAttributeValues": values
        });
        let check = serde_json::json!({
            "TableName": "t",
            "Key": {"pk": {"S": "a"}},
            "ConditionExpression": "a = :a AND b = :b",
            "ExpressionAttributeValues": values
        });
        let mut update = check.clone();
        update["UpdateExpression"] = "SET a = :a, b = :b".into();
        let errors = [
            serde_json::from_value::<TransactPut>(put).err(),
            serde_json::from_value::<TransactUpdate>(update).err(),
            serde_json::from_value::<TransactDelete>(check.clone()).err(),
            serde_json::from_value::<TransactConditionCheck>(check).err(),
        ];
        for error in errors {
            assert_eq!(error.expect("the value is refused").to_string(), expected);
        }
    }
}
