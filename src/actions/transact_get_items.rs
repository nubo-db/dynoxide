use crate::actions::helpers;
use crate::errors::{CancellationReason, DynoxideError, Result};
use crate::expressions;
use crate::storage_backend::StorageBackend;
use crate::types::{AttributeValue, Item};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Default)]
pub struct TransactGetItemsRequest {
    pub transact_items: Vec<TransactGetItem>,
    pub return_consumed_capacity: Option<String>,
}

/// The request as it arrives, before an item's `Get` is known to be there.
#[derive(Deserialize)]
struct TransactGetItemsRequestRaw {
    #[serde(rename = "TransactItems")]
    transact_items: Vec<TransactGetItemRaw>,
    #[serde(rename = "ReturnConsumedCapacity", default)]
    return_consumed_capacity: Option<String>,
}

#[derive(Deserialize)]
struct TransactGetItemRaw {
    #[serde(rename = "Get", default)]
    get: Option<TransactGet>,
}

impl<'de> Deserialize<'de> for TransactGetItemsRequest {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let raw = TransactGetItemsRequestRaw::deserialize(deserializer)?;

        // An item without its `Get` (absent or null) is a member constraint,
        // reported together with a `ReturnConsumedCapacity` outside the enum
        // and ahead of every other check (captured in five regions,
        // 2026-10-09). Only a request body can leave it out: the public type
        // requires it.
        let mut errors: Vec<String> = raw
            .transact_items
            .iter()
            .enumerate()
            .filter(|(_, item)| item.get.is_none())
            .map(|(position, _)| {
                format!(
                    "Value null at 'transactItems.{}.member.get' failed to satisfy \
                     constraint: Member must not be null",
                    position + 1
                )
            })
            .collect();
        if !errors.is_empty() {
            if let Some(msg) = crate::validation::return_consumed_capacity_rejection(
                raw.return_consumed_capacity.as_deref(),
            ) {
                errors.push(msg);
            }
            if let Some(msg) = crate::validation::format_validation_errors(&errors) {
                return Err(serde::de::Error::custom(format!("VALIDATION:{msg}")));
            }
        }

        Ok(TransactGetItemsRequest {
            transact_items: raw
                .transact_items
                .into_iter()
                .filter_map(|item| item.get)
                .map(|get| TransactGetItem { get })
                .collect(),
            return_consumed_capacity: raw.return_consumed_capacity,
        })
    }
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
    #[serde(
        rename = "Item",
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::set_order::item_in_hash_order"
    )]
    // Number sets in Java hash-set order here, as DynamoDB returns them;
    // pinned by set_order's every_response_item_field test.
    pub item: Option<Item>,
}

/// The action list as DynamoDB echoes it in a constraint message: each
/// action's Java `toString`, every field named, in the form us-east-1,
/// eu-west-1 and us-west-2 gave on 2026-10-10. eu-west-2 echoes no list
/// (`Value at 'TransactItems'`), and some hosts print each attribute value as
/// an object hash; the form most regions give is the one followed. Bounded by
/// [`crate::actions::batch_write_item::ECHO_LIMIT`].
fn items_echo(items: &[TransactGetItem]) -> String {
    let mut out = crate::actions::batch_write_item::BoundedEcho::new();
    let _ = write_items(&mut out, items);
    out.finish()
}

fn write_items(out: &mut dyn std::fmt::Write, items: &[TransactGetItem]) -> std::fmt::Result {
    use crate::actions::batch_write_item::{write_java_item, write_java_names, write_java_text};
    out.write_str("[")?;
    for (i, item) in items.iter().enumerate() {
        if i > 0 {
            out.write_str(", ")?;
        }
        let get = &item.get;
        out.write_str("TransactGetItem(get=Get(key=")?;
        write_java_item(out, &get.key)?;
        write!(out, ", tableName={}, projectionExpression=", get.table_name)?;
        write_java_text(out, get.projection_expression.as_deref())?;
        out.write_str(", expressionAttributeNames=")?;
        write_java_names(out, get.expression_attribute_names.as_ref())?;
        out.write_str(", workloadProfileName=null))")?;
    }
    out.write_str("]")
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

    // Validate: up to 100 actions, echoing the list as DynamoDB does.
    if request.transact_items.len() > 100 {
        return Err(DynoxideError::ValidationException(format!(
            "1 validation error detected: Value '{}' at 'transactItems' failed to satisfy constraint: Member must have length less than or equal to 100",
            items_echo(&request.transact_items)
        )));
    }

    // ReturnConsumedCapacity outside the enum: refused with the message the
    // single-item operations give, after the TransactItems constraints and before
    // anything is read or written (captured eu-west-2, 2026-10-08).
    if let Some(msg) = crate::validation::return_consumed_capacity_rejection(
        request.return_consumed_capacity.as_deref(),
    ) {
        return Err(DynoxideError::ValidationException(
            crate::validation::envelope_message(&msg),
        ));
    }

    // Every Get's table name, reported together as a top-level error under its
    // position, such as `transactItems.2.member.get.tableName`, never as a
    // cancellation reason. Checked after the capacity mode, as us-east-1 and
    // nine more regions do (captured 2026-10-09); eu-west-2 and eu-north-1
    // report the two together.
    let mut name_errors = Vec::new();
    for (position, transact_item) in request.transact_items.iter().enumerate() {
        name_errors.extend(crate::validation::member_table_name_errors(
            &transact_item.get.table_name,
            &format!("transactItems.{}.member.get.tableName", position + 1),
        ));
    }
    if let Some(msg) = crate::validation::format_validation_errors(&name_errors) {
        return Err(DynoxideError::ValidationException(msg));
    }

    // An empty attribute name in a key or the expression names is a top-level
    // rejection, not a cancellation reason, ahead of the table lookups
    // (captured in ten regions).
    for transact_item in &request.transact_items {
        let get = &transact_item.get;
        if crate::empty_names::map_has_empty_name(&get.key) {
            return Err(crate::empty_names::bare());
        }
        crate::empty_names::check_names(get.expression_attribute_names.as_ref())?;
    }

    // A key nested too deep fails the whole request at the top level, before
    // any table is looked up.
    for transact_item in &request.transact_items {
        crate::value_nesting::check_key(&transact_item.get.key)?;
    }

    // So does a key set that is empty or repeats a member, in the older
    // layer's words (us-east-1, eu-west-1 and us-west-2, 2026-10-09;
    // eu-west-2 uses the current layer's).
    for transact_item in &request.transact_items {
        let key = &transact_item.get.key;
        let mut names: Vec<&String> = key.keys().collect();
        names.sort();
        for name in names {
            if let Some(message) =
                crate::validation::find_set_error(&key[name], crate::validation::ValueLayer::Older)
            {
                return Err(DynoxideError::ValidationException(message));
            }
        }
    }

    // ProjectionExpressions are checked before any table is looked up: an
    // empty or whitespace-only one on a Get against a missing table is refused
    // as a ValidationException, not reported as the missing table (captured
    // eu-west-2, 2026-10-10). Each Get's names are checked in the same pass,
    // undefined and unused alike, so between two Gets the earlier one's name
    // error still wins (uncaptured; TransactWriteItems checks its names
    // together per action the same way).
    for transact_item in &request.transact_items {
        let get = &transact_item.get;
        let tracker = crate::expressions::TrackedExpressionAttributes::new(
            &get.expression_attribute_names,
            &None,
        );
        if let Some(proj_expr) = &get.projection_expression {
            if proj_expr.is_empty() {
                return Err(DynoxideError::ValidationException(
                    "Invalid ProjectionExpression: The expression can not be empty;".to_string(),
                ));
            }
            let projection = expressions::projection::parse(proj_expr)
                .map_err(DynoxideError::ValidationException)?;
            tracker.track_projection_expr(&projection);
            expressions::projection::validate(&projection, &tracker)
                .map_err(DynoxideError::ValidationException)?;
        }
        tracker.check_unused()?;
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
    let mut vector_defs: Vec<Vec<crate::types::VectorIndex>> =
        Vec::with_capacity(request.transact_items.len());
    let mut has_failure = false;

    for transact_item in &request.transact_items {
        let get = &transact_item.get;
        match validate_action(storage, get).await {
            Ok((schema, defs)) => {
                reasons.push(CancellationReason {
                    code: "None".to_string(),
                    message: None,
                    item: None,
                });
                validated_schemas.push(Some(schema));
                vector_defs.push(defs);
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
    let mut read_sizes = Vec::with_capacity(request.transact_items.len());

    for ((transact_item, schema), defs) in request
        .transact_items
        .iter()
        .zip(validated_schemas.iter())
        .zip(vector_defs.iter())
    {
        let get = &transact_item.get;
        let key_schema = schema.as_ref().expect("validated above");

        // TODO: validation must precede this call -- if reaching this line, caller has already validated keys.
        let (pk, sk) = helpers::extract_key_strings(&get.key, key_schema)?;

        let item_json = storage.get_item(&get.table_name, &pk, &sk).await?;

        let item: Option<Item> = item_json.and_then(|j| serde_json::from_str(&j).ok());

        // Charged on the whole item's billed size, whatever the projection
        // returns (captured eu-west-2 and us-east-1, 2026-10-23).
        read_sizes.push(
            item.as_ref()
                .map(|item| crate::types::billed_item_size(item, defs))
                .unwrap_or(0),
        );

        // Apply projection if present
        let tracker = crate::expressions::TrackedExpressionAttributes::new(
            &get.expression_attribute_names,
            &None, // TransactGet has no ExpressionAttributeValues
        );

        let item = if let Some(proj_expr) = &get.projection_expression {
            // Already checked before the tables were looked up.
            let projection = expressions::projection::parse(proj_expr)
                .map_err(DynoxideError::ValidationException)?;
            tracker.track_projection_expr(&projection);
            // Reject undefined names and overlapping paths before the read.
            expressions::projection::validate(&projection, &tracker)
                .map_err(DynoxideError::ValidationException)?;

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
        for (&size, req_item) in read_sizes.iter().zip(request.transact_items.iter()) {
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
/// caller can avoid re-parsing it before extract_key_strings, and the table's
/// vector indexes, which the read is billed by.
async fn validate_action<S: StorageBackend>(
    storage: &S,
    get: &TransactGet,
) -> Result<(helpers::KeySchema, Vec<crate::types::VectorIndex>)> {
    crate::validation::validate_table_name(&get.table_name)?;
    let meta = helpers::require_table_for_item_op(storage, &get.table_name).await?;
    let key_schema = helpers::parse_key_schema(&meta)?;
    helpers::validate_key_only(&get.key, &key_schema)?;
    let vector_defs = crate::actions::vector_index::parse_vector_defs(&meta)?;
    Ok((key_schema, vector_defs))
}
