use crate::actions::helpers;
use crate::errors::{CancellationReason, DynoxideError, Result};
use crate::expressions;
use crate::storage_backend::StorageBackend;
use crate::types::{AttributeValue, Item};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Default, Deserialize)]
pub struct TransactGetItemsRequest {
    #[serde(rename = "TransactItems")]
    pub transact_items: Vec<TransactGetItem>,
    #[serde(rename = "ReturnConsumedCapacity", default)]
    pub return_consumed_capacity: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
pub struct TransactGetItem {
    #[serde(rename = "Get")]
    pub get: TransactGet,
}

#[derive(Debug, Default, Deserialize)]
pub struct TransactGet {
    #[serde(rename = "TableName")]
    pub table_name: String,
    #[serde(rename = "Key")]
    pub key: HashMap<String, AttributeValue>,
    #[serde(rename = "ProjectionExpression", default)]
    pub projection_expression: Option<String>,
    #[serde(rename = "ExpressionAttributeNames", default)]
    pub expression_attribute_names: Option<HashMap<String, String>>,
}

#[derive(Debug, Default, Serialize)]
pub struct TransactGetItemsResponse {
    #[serde(rename = "Responses")]
    pub responses: Vec<TransactGetResponse>,
    #[serde(rename = "ConsumedCapacity", skip_serializing_if = "Option::is_none")]
    pub consumed_capacity: Option<Vec<crate::types::ConsumedCapacity>>,
}

#[derive(Debug, Default, Serialize)]
pub struct TransactGetResponse {
    #[serde(rename = "Item", skip_serializing_if = "Option::is_none")]
    pub item: Option<Item>,
}

pub async fn execute<S: StorageBackend>(
    storage: &S,
    request: TransactGetItemsRequest,
) -> Result<TransactGetItemsResponse> {
    // Validate: at least 1 action
    if request.transact_items.is_empty() {
        return Err(DynoxideError::ValidationException(
            "1 validation error detected: Value '[]' at 'transactItems' failed to satisfy constraint: Member must have length greater than or equal to 1".to_string(),
        ));
    }

    // Validate: up to 100 actions.
    // AWS surfaces this as the standard "1 validation error detected" envelope
    // around `Value '[<dump>]' at 'transactItems'`. The conformance suite
    // anchors a regex on the envelope and constraint phrase but leaves the
    // dump body unconstrained.
    if request.transact_items.len() > 100 {
        let dump = format!("{:?}", request.transact_items);
        return Err(DynoxideError::ValidationException(format!(
            "1 validation error detected: Value '[{dump}]' at 'transactItems' failed to satisfy constraint: Member must have length less than or equal to 100"
        )));
    }

    // Per-action validation pass.
    //
    // AWS surfaces per-action validation failures (empty Key, schema mismatch,
    // etc.) through the cancellation channel rather than as a request-level
    // ValidationException, so we collect a CancellationReason for each action
    // up-front. Validation here must run BEFORE any call to
    // helpers::extract_key_strings: that helper returns InternalServerError
    // for a missing partition or sort key, which would leak as HTTP 500
    // instead of a per-action ValidationError. validate_key_only is the
    // ValidationException-returning equivalent.
    let mut reasons: Vec<CancellationReason> = Vec::with_capacity(request.transact_items.len());
    let mut validated_schemas: Vec<Option<helpers::KeySchema>> =
        Vec::with_capacity(request.transact_items.len());
    let mut has_failure = false;

    for transact_item in &request.transact_items {
        let get = &transact_item.get;
        match validate_action(storage, get).await {
            Ok(schema) => {
                reasons.push(CancellationReason {
                    code: "None".to_string(),
                    message: None,
                    item: None,
                });
                validated_schemas.push(Some(schema));
            }
            // Group KeyEmptyValueValidation with ValidationException so an
            // empty-value key stays a per-action ValidationError cancellation
            // reason here. Unlike TransactWriteItems, a transact read surfaces
            // per-action key validation through the cancellation channel rather
            // than as a top-level error (captured AWS behaviour).
            Err(DynoxideError::ValidationException(msg))
            | Err(DynoxideError::KeyEmptyValueValidation(msg)) => {
                has_failure = true;
                reasons.push(CancellationReason {
                    code: "ValidationError".to_string(),
                    message: Some(msg),
                    item: None,
                });
                validated_schemas.push(None);
            }
            Err(DynoxideError::ResourceNotFoundException(msg)) => {
                // Resource-not-found at the request level is the existing AWS
                // behaviour (mirrors transact-get's pre-fix path); preserve it.
                return Err(DynoxideError::ResourceNotFoundException(msg));
            }
            Err(other) => return Err(other),
        }
    }

    if has_failure {
        let codes: Vec<&str> = reasons.iter().map(|r| r.code.as_str()).collect();
        let message = format!(
            "Transaction cancelled, please refer cancellation reasons for specific reasons [{}]",
            codes.join(", ")
        );
        return Err(DynoxideError::TransactionCanceledException(
            message, reasons,
        ));
    }

    // Validate: no duplicate item targets.
    // Safe to call extract_key_strings here because validate_key_only has
    // already passed for every action.
    let mut seen_targets = HashSet::new();
    for (transact_item, schema) in request.transact_items.iter().zip(validated_schemas.iter()) {
        let get = &transact_item.get;
        let key_schema = schema.as_ref().expect("validated above");
        // TODO: validation must precede this call -- if reaching this line, caller has already validated keys.
        let (pk, sk) = helpers::extract_key_strings(&get.key, key_schema)?;
        let target = format!("{}#{}#{}", get.table_name, pk, sk);
        if !seen_targets.insert(target) {
            return Err(DynoxideError::ValidationException(
                "Transaction request cannot include multiple operations on one item".to_string(),
            ));
        }
    }

    let mut responses = Vec::with_capacity(request.transact_items.len());

    for (transact_item, schema) in request.transact_items.iter().zip(validated_schemas.iter()) {
        let get = &transact_item.get;
        let key_schema = schema.as_ref().expect("validated above");

        // TODO: validation must precede this call -- if reaching this line, caller has already validated keys.
        let (pk, sk) = helpers::extract_key_strings(&get.key, key_schema)?;

        let item_json = storage.get_item(&get.table_name, &pk, &sk).await?;

        let item: Option<Item> = item_json.and_then(|j| serde_json::from_str(&j).ok());

        // Apply projection if present
        let tracker = crate::expressions::TrackedExpressionAttributes::new(
            &get.expression_attribute_names,
            &None, // TransactGet has no ExpressionAttributeValues
        );

        let item = if let Some(proj_expr) = &get.projection_expression {
            let projection = expressions::projection::parse(proj_expr)
                .map_err(DynoxideError::ValidationException)?;
            tracker.track_projection_expr(&projection);

            if let Some(item) = item {
                let mut key_attrs = vec![key_schema.partition_key.clone()];
                if let Some(ref sk) = key_schema.sort_key {
                    key_attrs.push(sk.clone());
                }

                // AWS omits `Item` entirely when a ProjectionExpression matches
                // no attribute on an otherwise-present item. `projection::apply`
                // always re-injects the key attributes, so its result is never
                // literally empty. Apply the projection without those keys to
                // see whether any path actually resolved, then return the
                // key-bearing result only when one did.
                let matched = expressions::projection::apply(&item, &projection, &tracker, &[])
                    .map_err(DynoxideError::ValidationException)?;
                if matched.is_empty() {
                    None
                } else {
                    let projected =
                        expressions::projection::apply(&item, &projection, &tracker, &key_attrs)
                            .map_err(DynoxideError::ValidationException)?;
                    Some(projected)
                }
            } else {
                None
            }
        } else {
            item
        };

        tracker.check_unused()?;

        responses.push(TransactGetResponse { item });
    }

    // Build consumed capacity per table
    let consumed_capacity = if matches!(
        request.return_consumed_capacity.as_deref(),
        Some("TOTAL") | Some("INDEXES")
    ) {
        // AWS charges 2 RCU per requested item for a transactional read,
        // including items that turned out to be missing (a missing item has
        // size 0, which still rounds up to 1 RCU before the 2x factor). Round
        // each item up to whole read units first, then double, then sum per
        // table, so a boundary-straddling item is not undercharged.
        let mut table_units: std::collections::HashMap<String, f64> =
            std::collections::HashMap::new();
        for (resp, req_item) in responses.iter().zip(request.transact_items.iter()) {
            let size = resp.item.as_ref().map(crate::types::item_size).unwrap_or(0);
            *table_units
                .entry(req_item.get.table_name.clone())
                .or_default() += crate::types::TRANSACTIONAL_CAPACITY_FACTOR
                * crate::types::read_capacity_units_with_consistency(size, true);
        }
        let caps: Vec<_> = table_units
            .iter()
            .filter_map(|(table, &units)| {
                crate::types::transactional_read_capacity(
                    table,
                    units,
                    &request.return_consumed_capacity,
                )
            })
            .collect();
        Some(caps)
    } else {
        None
    };

    Ok(TransactGetItemsResponse {
        responses,
        consumed_capacity,
    })
}

/// Run the validation that AWS treats as per-action (and therefore reportable
/// through the cancellation channel as ValidationError) for a single
/// TransactGet action: table-name shape, table existence, parsed key schema,
/// and key shape against that schema. Returns the resolved KeySchema so the
/// caller can avoid re-parsing it before extract_key_strings.
async fn validate_action<S: StorageBackend>(
    storage: &S,
    get: &TransactGet,
) -> Result<helpers::KeySchema> {
    crate::validation::validate_table_name(&get.table_name)?;
    let meta = helpers::require_table_for_item_op(storage, &get.table_name).await?;
    let key_schema = helpers::parse_key_schema(&meta)?;
    helpers::validate_key_only(&get.key, &key_schema)?;
    Ok(key_schema)
}
