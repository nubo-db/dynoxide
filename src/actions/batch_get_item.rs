use crate::actions::helpers;
use crate::errors::{DynoxideError, Result};
use crate::expressions;
use crate::storage_backend::StorageBackend;
use crate::types::{AttributeValue, Item};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Default, Deserialize)]
#[serde(try_from = "BatchGetItemWire")]
pub struct BatchGetItemRequest {
    #[serde(rename = "RequestItems")]
    pub request_items: HashMap<String, KeysAndAttributes>,
    #[serde(rename = "ReturnConsumedCapacity", default)]
    pub return_consumed_capacity: Option<String>,
}

/// The request as it is read, with every member DynamoDB tells apart when it
/// is missing or null held as an `Option`: `RequestItems`, each table's entry,
/// its `Keys` and each key. A member of the wrong JSON type still fails the
/// read, wherever it is written, ahead of the checks on missing and null
/// members (captured eu-west-2 and us-east-1, 11 and 12 November 2026).
#[derive(Deserialize)]
struct BatchGetItemWire {
    #[serde(rename = "RequestItems", default)]
    request_items: Option<helpers::WrittenTables<Option<KeysAndAttributesWire>>>,
    #[serde(rename = "ReturnConsumedCapacity", default)]
    return_consumed_capacity: Option<String>,
}

/// A table's entry as it is read: [`KeysAndAttributes`] with `Keys` and each
/// key optional.
#[derive(Deserialize)]
struct KeysAndAttributesWire {
    #[serde(rename = "Keys", default)]
    keys: Option<Vec<Option<crate::types::AttributeMap>>>,
    #[serde(rename = "ProjectionExpression", default)]
    projection_expression: Option<String>,
    #[serde(
        rename = "ExpressionAttributeNames",
        default,
        deserialize_with = "crate::empty_names::deserialize_names"
    )]
    expression_attribute_names: Option<HashMap<String, String>>,
    #[serde(
        rename = "ConsistentRead",
        default,
        deserialize_with = "crate::types::deserialize_lenient_flag"
    )]
    consistent_read: Option<bool>,
    #[serde(rename = "AttributesToGet", default)]
    attributes_to_get: Option<Vec<String>>,
}

impl TryFrom<BatchGetItemWire> for BatchGetItemRequest {
    type Error = String;

    /// The checks DynamoDB makes on a missing or null member, in its words
    /// (captured eu-west-2 and us-east-1, 11, 12 and 13 November 2026):
    ///
    /// - A missing or null `RequestItems` is refused alone, ahead of anything
    ///   else.
    /// - A table with no `Keys` is a null member. Its fault is listed with the
    ///   request's other member constraints in the older words, a `Keys` that
    ///   is empty or holds more than 100 keys and a `ReturnConsumedCapacity`
    ///   outside its enum, tables in the order DynamoDB walks the map. A table
    ///   name that breaks its constraints is not listed.
    /// - A null table entry or a null key is refused alone, after a
    ///   `ReturnConsumedCapacity` outside its enum.
    ///
    /// Every other check is made where it is made for a request that passes
    /// these. The refusal keeps its text whole, since a table name may hold
    /// anything.
    fn try_from(wire: BatchGetItemWire) -> std::result::Result<Self, String> {
        let refuse = |message: &str| {
            format!(
                "{}{message}",
                crate::serde_errors::VALIDATION_AS_READ_MARKER
            )
        };
        let Some(entries) = wire.request_items else {
            return Err(refuse(
                "The requestItems parameter is required for BatchGetItem",
            ));
        };
        let tables = entries.walk_order();
        let rcc_rejection = crate::validation::return_consumed_capacity_rejection(
            wire.return_consumed_capacity.as_deref(),
        );

        if tables
            .iter()
            .any(|(_, entry)| entry.as_ref().is_some_and(|ka| ka.keys.is_none()))
        {
            let mut member_errors: Vec<String> = tables
                .iter()
                .filter_map(|(table_name, entry)| {
                    let keys = match entry.as_ref()?.keys.as_ref() {
                        None => {
                            return Some(format!(
                                "Value null at 'requestItems.{table_name}.member.keys' failed \
                                 to satisfy constraint: Member must not be null"
                            ));
                        }
                        Some(keys) => keys,
                    };
                    let bound = if keys.is_empty() {
                        "greater than or equal to 1"
                    } else if keys.len() > 100 {
                        "less than or equal to 100"
                    } else {
                        return None;
                    };
                    Some(format!(
                        "Value '{}' at 'requestItems.{table_name}.member.keys' failed to \
                         satisfy constraint: Member must have length {bound}",
                        keys_echo(keys)
                    ))
                })
                .chain(rcc_rejection.clone())
                .collect();
            // DynamoDB lists ten member faults at most.
            member_errors.truncate(10);
            if let Some(message) = crate::validation::format_validation_errors(&member_errors) {
                return Err(refuse(&message));
            }
        }

        for (table_name, entry) in &tables {
            let refusal = match entry {
                None => format!(
                    "The list of keys in RequestItems for BatchGetItem is required: \
                     {table_name} has empty list"
                ),
                Some(ka) if ka.keys.iter().flatten().any(Option::is_none) => format!(
                    "The list of keys in RequestItems for BatchGetItem may not contain null or \
                     empty values: {table_name} has one or more empty keys"
                ),
                Some(_) => continue,
            };
            return Err(refuse(&match rcc_rejection {
                Some(rejection) => crate::validation::envelope_message(&rejection),
                None => refusal,
            }));
        }

        // Every entry and key is present by now.
        let request_items = entries
            .0
            .into_iter()
            .filter_map(|(table_name, entry)| {
                let ka = entry?;
                let keys = ka.keys?.into_iter().flatten().map(|key| key.0).collect();
                Some((
                    table_name,
                    KeysAndAttributes {
                        keys,
                        projection_expression: ka.projection_expression,
                        expression_attribute_names: ka.expression_attribute_names,
                        consistent_read: ka.consistent_read,
                        attributes_to_get: ka.attributes_to_get,
                    },
                ))
            })
            .collect();
        Ok(BatchGetItemRequest {
            request_items,
            return_consumed_capacity: wire.return_consumed_capacity,
        })
    }
}

/// A table's `Keys` as the Java model prints them in a constraint message:
/// each key's attribute map, a null key as `null` (two tables, one with 101
/// keys, captured us-east-1, 13 November 2026). Bounded by
/// [`crate::actions::batch_write_item::ECHO_LIMIT`].
fn keys_echo(keys: &[Option<crate::types::AttributeMap>]) -> String {
    use std::fmt::Write;
    let mut out = crate::actions::batch_write_item::BoundedEcho::new();
    let _ = (|| {
        out.write_str("[")?;
        for (i, key) in keys.iter().enumerate() {
            if i > 0 {
                out.write_str(", ")?;
            }
            match key {
                Some(key) => crate::actions::batch_write_item::write_java_item(&mut out, &key.0)?,
                None => out.write_str("null")?,
            }
        }
        out.write_str("]")
    })();
    out.finish()
}

#[derive(Debug, Default, Deserialize)]
pub struct KeysAndAttributes {
    #[serde(
        rename = "Keys",
        deserialize_with = "crate::types::deserialize_attribute_maps"
    )]
    pub keys: Vec<HashMap<String, AttributeValue>>,
    #[serde(rename = "ProjectionExpression", default)]
    pub projection_expression: Option<String>,
    #[serde(
        rename = "ExpressionAttributeNames",
        default,
        deserialize_with = "crate::empty_names::deserialize_names"
    )]
    pub expression_attribute_names: Option<HashMap<String, String>>,
    #[serde(
        rename = "ConsistentRead",
        default,
        deserialize_with = "crate::types::deserialize_lenient_flag"
    )]
    pub consistent_read: Option<bool>,
    #[serde(rename = "AttributesToGet", default)]
    pub attributes_to_get: Option<Vec<String>>,
}

#[derive(Debug, Default, Serialize)]
pub struct BatchGetItemResponse {
    #[serde(rename = "Responses")]
    // Number sets in numeric order here, as DynamoDB returns them;
    // pinned by set_order's every_response_item_field test.
    pub responses: HashMap<String, Vec<Item>>,
    #[serde(rename = "UnprocessedKeys")]
    pub unprocessed_keys: HashMap<String, serde_json::Value>,
    #[serde(rename = "ConsumedCapacity", skip_serializing_if = "Option::is_none")]
    pub consumed_capacity: Option<Vec<crate::types::ConsumedCapacity>>,
}

pub async fn execute<S: StorageBackend>(
    storage: &S,
    request: BatchGetItemRequest,
) -> Result<BatchGetItemResponse> {
    // Validate RequestItems is not empty.
    // AWS used to answer the empty-map case with a bespoke parameter-required
    // sentence. It has since moved BatchGetItem onto the standard validation
    // envelope, and as of the 2026-09-02 sweep 32 of the 33 regions observed
    // answer this message; only me-central-1 still returns the old sentence.
    // BatchWriteItem has not followed, so the sibling operation deliberately
    // keeps its bespoke sentence rather than matching this one.
    if request.request_items.is_empty() {
        return Err(DynoxideError::ValidationException(
            "1 validation error detected: Value at 'RequestItems' failed to satisfy constraint: Member must have length greater than or equal to 1".to_string(),
        ));
    }

    // A table entry with no keys is a fault of its own, one for each such
    // table up to ten, checked ahead of the table names and reported alone,
    // even beside an unknown capacity mode (captured in twelve regions,
    // 2026-10-09, and eleven tables in eu-west-2 and us-east-1, 2026-11-13).
    // The order is BatchWriteItem's for its lists' lengths: eleven tables
    // came back in no order that could be reproduced.
    let mut empty_keys: Vec<String> =
        helpers::length_fault_order(request.request_items.keys().map(String::as_str))
            .into_iter()
            .filter(|table_name| request.request_items[*table_name].keys.is_empty())
            .map(|table_name| {
                format!(
                    "Value at 'RequestItems.{table_name}.member.Keys' failed to satisfy \
                     constraint: Member must have length greater than or equal to 1"
                )
            })
            .collect();
    // DynamoDB lists ten member faults at most.
    empty_keys.truncate(10);
    if let Some(message) = crate::validation::format_validation_errors(&empty_keys) {
        return Err(DynoxideError::ValidationException(message));
    }

    // Validate total key count.
    // AWS surfaces this as the standard "1 validation error detected" envelope
    // with the field path pinned to one of the request's tables. The conformance
    // suite exercises a single-table case; for multi-table requests we pick the
    // table with the largest Keys array (and, on ties, fall through to whichever
    // HashMap iteration yields first) so the field path lines up with the table
    // that pushed the total over.
    //
    // A table name that breaks its constraints is reported first, whatever the
    // total: the map-key error wins over the key count (captured eu-west-2 and
    // us-east-1, 2026-10-09), and is raised below.
    let total_keys: usize = request.request_items.values().map(|ka| ka.keys.len()).sum();
    let all_names_valid = request
        .request_items
        .keys()
        .all(|name| crate::validation::member_table_name_is_valid(name));
    if total_keys > 100 && all_names_valid {
        let table_name = request
            .request_items
            .iter()
            .max_by_key(|(_, ka)| ka.keys.len())
            .map(|(name, _)| name.as_str())
            .unwrap_or("");
        return Err(DynoxideError::ValidationException(format!(
            "1 validation error detected: Value at 'RequestItems.{table_name}.member.Keys' failed to satisfy constraint: Member must have length less than or equal to 100"
        )));
    }

    // A table name that breaks its constraints is a constraint on the
    // RequestItems map's keys, reported once for the map with the map echoed,
    // and together with a ReturnConsumedCapacity outside the enum, before
    // anything is read (captured us-east-1, eu-west-2 and ten more regions,
    // 2026-10-08 and 2026-10-09).
    let mut member_errors = Vec::new();
    if !all_names_valid {
        member_errors.push(format!(
            "Value '{}' at 'requestItems' failed to satisfy constraint: {}",
            request_items_echo(&request.request_items),
            crate::validation::REQUEST_ITEMS_KEY_CONSTRAINT
        ));
    }
    if let Some(msg) = crate::validation::return_consumed_capacity_rejection(
        request.return_consumed_capacity.as_deref(),
    ) {
        member_errors.push(msg);
    }
    if let Some(msg) = crate::validation::format_validation_errors(&member_errors) {
        return Err(DynoxideError::ValidationException(msg));
    }

    // --- Pre-table validations ---
    // DynamoDB validates expression attributes, key values, projections, and duplicates
    // BEFORE checking table existence. Perform these checks first.

    // An empty attribute name in a key or AttributesToGet is refused bare,
    // before anything else here (captured in ten regions).
    let mut tables: Vec<&String> = request.request_items.keys().collect();
    tables.sort_unstable();
    for table in &tables {
        let ka = &request.request_items[*table];
        if ka.keys.iter().any(crate::empty_names::map_has_empty_name)
            || crate::empty_names::names_include_empty(ka.attributes_to_get.as_ref())
        {
            return Err(crate::empty_names::bare());
        }
    }

    // Reject a request that pairs an expression projection with a non-expression
    // AttributesToGet, whether in the same block or across blocks. AWS rejects the
    // request as a whole even when each block is internally consistent.
    // Order-independent over the request map. It comes ahead of an empty
    // expression name, as on GetItem (captured eu-west-2 and us-east-1,
    // 2026-11-12).
    let any_projection = request
        .request_items
        .values()
        .any(|ka| ka.projection_expression.is_some());
    let any_attributes_to_get = request
        .request_items
        .values()
        .any(|ka| ka.attributes_to_get.is_some());
    if let Some(message) = helpers::mixed_parameters_refusal(
        if any_attributes_to_get {
            &["AttributesToGet"]
        } else {
            &[]
        },
        if any_projection {
            &["ProjectionExpression"]
        } else {
            &[]
        },
    ) {
        return Err(DynoxideError::ValidationException(message));
    }

    // An empty expression name, refused bare (captured in ten regions).
    for table in tables {
        crate::empty_names::check_names_early(
            request.request_items[table]
                .expression_attribute_names
                .as_ref(),
        )?;
    }

    // Each table's ProjectionExpression, checked through before any table is
    // looked up (captured eu-west-2 and us-east-1, 2026-11-11).
    let mut projections: HashMap<&String, expressions::projection::ProjectionExpr> = HashMap::new();
    for (table_name, keys_and_attrs) in &request.request_items {
        let has_projection_expr = keys_and_attrs.projection_expression.is_some();
        let has_expr_attr_names = keys_and_attrs.expression_attribute_names.is_some();

        // ExpressionAttributeNames without expression
        if has_expr_attr_names && !has_projection_expr {
            return Err(DynoxideError::ValidationException(
                "ExpressionAttributeNames can only be specified when using expressions".to_string(),
            ));
        }

        // Empty ExpressionAttributeNames
        if let Some(ref ean) = keys_and_attrs.expression_attribute_names {
            if ean.is_empty() {
                return Err(DynoxideError::ValidationException(
                    "ExpressionAttributeNames must not be empty".to_string(),
                ));
            }
            // Invalid EAN keys (must start with #)
            for key in ean.keys() {
                if !key.starts_with('#') {
                    return Err(DynoxideError::ValidationException(format!(
                        "ExpressionAttributeNames contains invalid key: Syntax error; key: \"{key}\""
                    )));
                }
            }
            // An empty name left to here, on the older layer (see
            // `empty_names::names_checked_early`).
            crate::empty_names::check_names(Some(ean))?;
        }

        // Empty ProjectionExpression
        if let Some(ref pe) = keys_and_attrs.projection_expression
            && pe.is_empty()
        {
            return Err(DynoxideError::ValidationException(
                "Invalid ProjectionExpression: The expression can not be empty;".to_string(),
            ));
        }

        // The projection is checked through ahead of the keys: a fault in it
        // comes before a bad key value or a repeated key (captured eu-west-2
        // and us-east-1, 2026-11-11).
        if let Some(parsed) = helpers::validate_read_expressions(
            &keys_and_attrs.expression_attribute_names,
            &None, // BatchGetItem has no ExpressionAttributeValues
            helpers::ReadExpressions {
                projection: keys_and_attrs.projection_expression.as_deref(),
                ..Default::default()
            },
        )? {
            projections.insert(table_name, parsed);
        }

        // Duplicate AttributesToGet check (must come before duplicate keys check)
        if let Some(ref atg) = keys_and_attrs.attributes_to_get {
            let mut seen = std::collections::HashSet::new();
            for attr in atg {
                if !seen.insert(attr.as_str()) {
                    return Err(DynoxideError::ValidationException(format!(
                        "One or more parameter values were invalid: Duplicate value in attribute name: {attr}"
                    )));
                }
            }
        }

        // Validate key attribute values (empty attrs, invalid numbers, etc.)
        for key in &keys_and_attrs.keys {
            // A key's values are checked as an item's are (depth included),
            // in the current layer's words (2026-10-09).
            crate::validation::validate_item_attribute_values_classified(
                key,
                crate::validation::ValueLayer::Current,
            )?;
        }

        // Duplicate keys check
        if keys_and_attrs.keys.len() > 1 {
            let serialised: Vec<String> = keys_and_attrs
                .keys
                .iter()
                .map(|k| {
                    let mut pairs: Vec<_> = k.iter().map(|(k, v)| format!("{k}={v:?}")).collect();
                    pairs.sort();
                    pairs.join(",")
                })
                .collect();
            let mut seen = std::collections::HashSet::new();
            for s in &serialised {
                if !seen.insert(s) {
                    return Err(DynoxideError::ValidationException(
                        "Provided list of item keys contains duplicates".to_string(),
                    ));
                }
            }
        }
    }

    const MAX_RESPONSE_SIZE: usize = 16 * 1024 * 1024; // 16MB

    let mut responses: HashMap<String, Vec<Item>> = HashMap::new();
    let mut unprocessed_keys: HashMap<String, serde_json::Value> = HashMap::new();
    let mut cumulative_size: usize = 0;
    let mut size_limit_reached = false;
    // Track per-key RCU for ConsumedCapacity (uses full item size, not projected)
    let mut table_rcu: HashMap<String, f64> = HashMap::new();

    for (table_name, keys_and_attrs) in &request.request_items {
        let meta = helpers::require_table_for_item_op(storage, table_name).await?;
        let key_schema = helpers::parse_key_schema(&meta)?;
        let vector_defs = crate::actions::vector_index::parse_vector_defs(&meta)?;

        // The projection checked before the lookups, or the legacy
        // AttributesToGet.
        let projection = projections.remove(table_name).or_else(|| {
            keys_and_attrs
                .attributes_to_get
                .as_ref()
                .map(|attrs| crate::actions::helpers::attributes_to_get_to_projection(attrs))
        });

        // Resolves the projection's names; they were checked before the lookups.
        let tracker = crate::expressions::TrackedExpressionAttributes::without_tracking(
            &keys_and_attrs.expression_attribute_names,
            &None, // BatchGetItem has no ExpressionAttributeValues
        );

        // BatchGetItem does NOT automatically include key attributes in projections.
        let key_attrs = Vec::new();

        let consistent = keys_and_attrs.consistent_read.unwrap_or(false);
        let mut table_items = Vec::new();
        let mut remaining_keys: Vec<HashMap<String, AttributeValue>> = Vec::new();
        let mut per_table_rcu: f64 = 0.0;

        for key in &keys_and_attrs.keys {
            if size_limit_reached {
                remaining_keys.push(key.clone());
                continue;
            }

            helpers::validate_key_only(key, &key_schema)?;
            // TODO: validation must precede this call -- if reaching this line, caller has already validated keys.
            let (pk, sk) = helpers::extract_key_strings(key, &key_schema)?;

            if let Some(item_json) = storage.get_item(table_name, &pk, &sk).await? {
                let item: Item = serde_json::from_str(&item_json).map_err(|e| {
                    DynoxideError::InternalServerError(format!("Bad item JSON: {e}"))
                })?;

                // Use full item size for both capacity and response limit
                let item_size = crate::types::item_size(&item);

                if cumulative_size + item_size > MAX_RESPONSE_SIZE {
                    size_limit_reached = true;
                    remaining_keys.push(key.clone());
                    continue;
                }

                cumulative_size += item_size;

                // RCU is based on the full item's billed size, not projected
                // size: a vector is charged at four bytes per dimension.
                let billed = crate::types::billed_item_size(&item, &vector_defs);
                per_table_rcu +=
                    crate::types::read_capacity_units_with_consistency(billed, consistent);

                let result_item = if let Some(ref proj) = projection {
                    expressions::projection::apply(&item, proj, &tracker, &key_attrs)
                        .map_err(DynoxideError::ValidationException)?
                } else {
                    item
                };

                table_items.push(result_item);
            } else {
                // DynamoDB charges for the read attempt even if the item is not found
                per_table_rcu += crate::types::read_capacity_units_with_consistency(0, consistent);
            }
        }

        table_rcu.insert(table_name.clone(), per_table_rcu);
        responses.insert(table_name.clone(), table_items);

        if !remaining_keys.is_empty() {
            let mut unprocessed = serde_json::json!({
                "Keys": remaining_keys,
            });
            // Preserve original request settings so the caller can retry
            // without losing projection or consistency configuration.
            if let Some(ref pe) = keys_and_attrs.projection_expression {
                unprocessed["ProjectionExpression"] = serde_json::json!(pe);
            }
            if let Some(ref ean) = keys_and_attrs.expression_attribute_names {
                unprocessed["ExpressionAttributeNames"] = serde_json::json!(ean);
            }
            if let Some(cr) = keys_and_attrs.consistent_read {
                unprocessed["ConsistentRead"] = serde_json::json!(cr);
            }
            unprocessed_keys.insert(table_name.clone(), unprocessed);
        }
    }

    // Build consumed capacity per table
    let consumed_capacity = if matches!(
        request.return_consumed_capacity.as_deref(),
        Some("TOTAL") | Some("INDEXES")
    ) {
        let mut caps = Vec::new();
        for table_name in request.request_items.keys() {
            let total_rcu = table_rcu.get(table_name).copied().unwrap_or(0.0);
            if let Some(cc) = crate::types::consumed_capacity(
                table_name,
                total_rcu,
                &request.return_consumed_capacity,
            ) {
                caps.push(cc);
            }
        }
        Some(caps)
    } else {
        None
    };

    Ok(BatchGetItemResponse {
        responses,
        unprocessed_keys,
        consumed_capacity,
    })
}

/// The `RequestItems` map as DynamoDB echoes it in a constraint message: JSON,
/// with the tables, each table's members and every attribute map in order of
/// name. Two tables came back in name order in half the regions captured and
/// in the other order in the rest, so the order is DynamoDB's own map order;
/// name order is the deterministic choice. Cut at
/// [`crate::actions::batch_write_item::ECHO_LIMIT`] like the other echoes; this
/// one is JSON, no larger than the request, so it is cut once built.
fn request_items_echo(request_items: &HashMap<String, KeysAndAttributes>) -> String {
    let mut tables: Vec<(&String, &KeysAndAttributes)> = request_items.iter().collect();
    tables.sort_by(|a, b| a.0.cmp(b.0));
    let mut echo = serde_json::Map::new();
    for (table_name, ka) in tables {
        let mut members = serde_json::Map::new();
        if let Some(ref attributes) = ka.attributes_to_get {
            members.insert("AttributesToGet".to_string(), serde_json::json!(attributes));
        }
        if let Some(consistent_read) = ka.consistent_read {
            members.insert(
                "ConsistentRead".to_string(),
                serde_json::json!(consistent_read),
            );
        }
        if let Some(ref names) = ka.expression_attribute_names {
            members.insert(
                "ExpressionAttributeNames".to_string(),
                crate::sorted_json(serde_json::json!(names)),
            );
        }
        let keys = ka
            .keys
            .iter()
            .map(|key| crate::sorted_json(serde_json::to_value(key).unwrap_or_default()))
            .collect();
        members.insert("Keys".to_string(), serde_json::Value::Array(keys));
        if let Some(ref projection) = ka.projection_expression {
            members.insert(
                "ProjectionExpression".to_string(),
                serde_json::json!(projection),
            );
        }
        echo.insert(table_name.clone(), serde_json::Value::Object(members));
    }
    // Sorted as a whole too: the tables arrive in hash-map order, which
    // serde_json keeps when its `preserve_order` feature is on.
    crate::actions::batch_write_item::bound_echo(
        crate::sorted_json(serde_json::Value::Object(echo)).to_string(),
    )
}
