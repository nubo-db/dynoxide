use crate::actions::helpers;
use crate::errors::{DynoxideError, Result};
use crate::storage_backend::StorageBackend;
use crate::types::{self, AttributeValue, Item};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Default, Deserialize)]
#[serde(try_from = "BatchWriteItemWire")]
pub struct BatchWriteItemRequest {
    #[serde(rename = "RequestItems")]
    pub request_items: HashMap<String, Vec<WriteRequest>>,
    #[serde(rename = "ReturnConsumedCapacity", default)]
    pub return_consumed_capacity: Option<String>,
    #[serde(rename = "ReturnItemCollectionMetrics", default)]
    pub return_item_collection_metrics: Option<String>,
}

/// The request as it is read, with every member DynamoDB tells apart when it
/// is missing or null held as an `Option`: `RequestItems`, each table's list
/// of write requests, each write request, and each put's `Item` and delete's
/// `Key`. A member of the wrong JSON type still fails the read, wherever it is
/// written, ahead of the checks on missing and null members (captured
/// eu-west-2 and us-east-1, 11 and 12 November 2026).
#[derive(Deserialize)]
struct BatchWriteItemWire {
    #[serde(rename = "RequestItems", default)]
    request_items: Option<helpers::WrittenTables<Option<Vec<Option<WriteRequestWire>>>>>,
    #[serde(rename = "ReturnConsumedCapacity", default)]
    return_consumed_capacity: Option<String>,
    #[serde(rename = "ReturnItemCollectionMetrics", default)]
    return_item_collection_metrics: Option<String>,
}

/// A write request as it is read. A null `PutRequest` or `DeleteRequest` is
/// read as absent, as DynamoDB reads it (captured eu-west-2 and us-east-1,
/// 13 November 2026).
#[derive(Deserialize)]
struct WriteRequestWire {
    #[serde(rename = "PutRequest", default)]
    put_request: Option<PutRequestWire>,
    #[serde(rename = "DeleteRequest", default)]
    delete_request: Option<DeleteRequestWire>,
}

/// A put as it is read: [`PutRequest`] with its `Item` optional.
#[derive(Deserialize)]
struct PutRequestWire {
    #[serde(rename = "Item", default)]
    item: Option<types::AttributeMap>,
}

/// A delete as it is read: [`DeleteRequest`] with its `Key` optional.
#[derive(Deserialize)]
struct DeleteRequestWire {
    #[serde(rename = "Key", default)]
    key: Option<types::AttributeMap>,
}

impl TryFrom<BatchWriteItemWire> for BatchWriteItemRequest {
    type Error = String;

    /// The checks DynamoDB makes on a missing or null member, in its words
    /// (captured eu-west-2 and us-east-1, 11, 12 and 13 November 2026):
    ///
    /// - A missing or null `RequestItems` is refused alone, ahead of anything
    ///   else.
    /// - A put with no `Item` or a delete with no `Key` is a null member. Its
    ///   fault is listed with the request's other member constraints in the
    ///   older words: the request lists' lengths, with the request echoed in
    ///   its Java model's `toString` (both regions still answer this way here,
    ///   though eu-west-2 words a list's length differently on its own, see
    ///   [`check_request`]), then the null members, tables in the order
    ///   DynamoDB walks the map, then `ReturnConsumedCapacity` and
    ///   `ReturnItemCollectionMetrics` outside their enums. A table name that
    ///   breaks its constraints is not listed.
    /// - A null list for a table or a null write request is refused alone,
    ///   after those enum faults.
    ///
    /// Every other check is made where it is made for a request that passes
    /// these. The refusal keeps its text whole, since a table name may hold
    /// anything.
    fn try_from(wire: BatchWriteItemWire) -> std::result::Result<Self, String> {
        let refuse = |message: &str| {
            format!(
                "{}{message}",
                crate::serde_errors::VALIDATION_AS_READ_MARKER
            )
        };
        let Some(entries) = wire.request_items else {
            return Err(refuse(
                "The requestItems parameter is required for BatchWriteItem",
            ));
        };
        let tables = entries.walk_order();
        let enum_faults: Vec<String> = crate::validation::return_consumed_capacity_rejection(
            wire.return_consumed_capacity.as_deref(),
        )
        .into_iter()
        .chain(crate::validation::return_item_collection_metrics_rejection(
            wire.return_item_collection_metrics.as_deref(),
        ))
        .collect();

        let mut null_members = Vec::new();
        for (table_name, list) in &tables {
            let requests = list.iter().flatten().enumerate();
            for (i, request) in requests.filter_map(|(i, r)| Some((i + 1, r.as_ref()?))) {
                if request
                    .put_request
                    .as_ref()
                    .is_some_and(|p| p.item.is_none())
                {
                    null_members.push(format!(
                        "Value null at 'requestItems.{table_name}.member.{i}.member.putRequest.item' \
                         failed to satisfy constraint: Member must not be null"
                    ));
                }
                if request
                    .delete_request
                    .as_ref()
                    .is_some_and(|d| d.key.is_none())
                {
                    null_members.push(format!(
                        "Value null at 'requestItems.{table_name}.member.{i}.member.deleteRequest.key' \
                         failed to satisfy constraint: Member must not be null"
                    ));
                }
            }
        }
        if !null_members.is_empty() {
            let lists = || tables.iter().filter_map(|(_, list)| list.as_ref());
            let total_requests: usize = lists().map(Vec::len).sum();
            let mut member_errors = Vec::new();
            if total_requests > 25 || lists().any(Vec::is_empty) {
                member_errors.push(format!(
                    "Value '{}' at 'requestItems' failed to satisfy constraint: {MAP_VALUE_CONSTRAINT}",
                    wire_request_items_echo(&entries)
                ));
            }
            member_errors.extend(null_members);
            member_errors.extend(enum_faults.iter().cloned());
            // DynamoDB lists ten member faults at most.
            member_errors.truncate(10);
            if let Some(message) = crate::validation::format_validation_errors(&member_errors) {
                return Err(refuse(&message));
            }
        }

        for (table_name, list) in &tables {
            let refusal = match list {
                None => format!(
                    "The batch write request list for a table cannot be null or empty: {table_name}"
                ),
                Some(requests) if requests.iter().any(Option::is_none) => {
                    "The batch write request list for a table cannot be null or empty".to_string()
                }
                Some(_) => continue,
            };
            return Err(refuse(
                &crate::validation::format_validation_errors(&enum_faults).unwrap_or(refusal),
            ));
        }

        // Every list, write request, item and key is present by now.
        let request_items = entries
            .0
            .into_iter()
            .filter_map(|(table_name, list)| {
                let requests = list?
                    .into_iter()
                    .flatten()
                    .map(|request| WriteRequest {
                        put_request: request
                            .put_request
                            .and_then(|put| put.item)
                            .map(|item| PutRequest { item: item.0 }),
                        delete_request: request
                            .delete_request
                            .and_then(|delete| delete.key)
                            .map(|key| DeleteRequest { key: key.0 }),
                    })
                    .collect();
                Some((table_name, requests))
            })
            .collect();
        Ok(BatchWriteItemRequest {
            request_items,
            return_consumed_capacity: wire.return_consumed_capacity,
            return_item_collection_metrics: wire.return_item_collection_metrics,
        })
    }
}

/// The constraint on each of `RequestItems`' lists, as DynamoDB words it.
const MAP_VALUE_CONSTRAINT: &str = "Map value must satisfy constraint: \
     [Member must have length less than or equal to 25, \
     Member must have length greater than or equal to 1]";

#[derive(Debug, Default, Deserialize)]
pub struct WriteRequest {
    #[serde(rename = "PutRequest", default)]
    pub put_request: Option<PutRequest>,
    #[serde(rename = "DeleteRequest", default)]
    pub delete_request: Option<DeleteRequest>,
}

#[derive(Debug, Default, Deserialize)]
pub struct PutRequest {
    #[serde(
        rename = "Item",
        deserialize_with = "crate::types::deserialize_required_attribute_map"
    )]
    pub item: Item,
}

#[derive(Debug, Default, Deserialize)]
pub struct DeleteRequest {
    #[serde(
        rename = "Key",
        deserialize_with = "crate::types::deserialize_required_attribute_map"
    )]
    pub key: HashMap<String, AttributeValue>,
}

#[derive(Debug, Default, Serialize)]
pub struct BatchWriteItemResponse {
    #[serde(rename = "UnprocessedItems")]
    pub unprocessed_items: HashMap<String, serde_json::Value>,
    #[serde(rename = "ConsumedCapacity", skip_serializing_if = "Option::is_none")]
    pub consumed_capacity: Option<Vec<crate::types::ConsumedCapacity>>,
    #[serde(
        rename = "ItemCollectionMetrics",
        skip_serializing_if = "Option::is_none"
    )]
    pub item_collection_metrics: Option<HashMap<String, Vec<crate::types::ItemCollectionMetrics>>>,
}

/// Fold one item's per-index units into the running per-table totals. A batch
/// reports the sum across its items, and an index no item in the batch touched
/// stays absent rather than appearing as a zero.
fn accumulate_index_units(
    totals: &mut HashMap<String, HashMap<String, f64>>,
    table_name: &str,
    units: &HashMap<String, f64>,
) {
    if units.is_empty() {
        return;
    }
    let table_entry = totals.entry(table_name.to_string()).or_default();
    for (index_name, units) in units {
        *table_entry.entry(index_name.clone()).or_insert(0.0) += units;
    }
}

/// The request-level checks, in the order eu-west-2 reports them: an empty
/// `RequestItems`; then each table's request list that is empty or holds more
/// than 25, alone; then a table name that breaks its constraints together with
/// ReturnConsumedCapacity and ReturnItemCollectionMetrics outside their enums.
///
/// Also run on the request-parsing path for a body nested too deep to decode,
/// so a member error there is still reported ahead of the nesting error.
pub(crate) fn check_request(request: &BatchWriteItemRequest) -> Result<()> {
    // An empty RequestItems is a length error in the standard envelope, as
    // BatchGetItem answers it. DynamoDB's regions differed on this when
    // captured (11 and 12 November 2026), and Dynoxide follows eu-west-2.
    if request.request_items.is_empty() {
        return Err(DynoxideError::ValidationException(
            "1 validation error detected: Value at 'RequestItems' failed to satisfy constraint: \
             Member must have length greater than or equal to 1"
                .to_string(),
        ));
    }

    let enum_faults = crate::validation::return_consumed_capacity_rejection(
        request.return_consumed_capacity.as_deref(),
    )
    .into_iter()
    .chain(crate::validation::return_item_collection_metrics_rejection(
        request.return_item_collection_metrics.as_deref(),
    ));

    // A table's request list that is empty or holds more than 25 is a fault
    // of its own, one for each such table, with no echo, reported ahead of and
    // without the enum faults or a table name that breaks its constraints
    // (captured eu-west-2, 13 November 2026). Two tables came back in the
    // reverse of the order DynamoDB walks the map. us-east-1 still gave the
    // older form, one fault for the whole map with the request echoed in its
    // Java model's `toString`, listed with the enum faults.
    let mut names: Vec<&str> = request.request_items.keys().map(String::as_str).collect();
    names.sort_unstable();
    let length_faults: Vec<String> = crate::validation::java_hash_order(&names, true)
        .into_iter()
        .rev()
        .filter_map(|table_name| {
            let requests = request.request_items[table_name].len();
            let bound = if requests == 0 {
                "greater than or equal to 1"
            } else if requests > 25 {
                "less than or equal to 25"
            } else {
                return None;
            };
            Some(format!(
                "Value at 'RequestItems.{table_name}.member' failed to satisfy constraint: \
                 Member must have length {bound}"
            ))
        })
        .collect();
    if let Some(msg) = crate::validation::format_validation_errors(&length_faults) {
        return Err(DynoxideError::ValidationException(msg));
    }

    // More than 25 requests across tables that each hold 25 or fewer has not
    // been captured since eu-west-2 changed its words, so it keeps the older
    // form, listed with the enum faults.
    let total_requests: usize = request.request_items.values().map(Vec::len).sum();
    if total_requests > 25 {
        let member_errors: Vec<String> = std::iter::once(format!(
            "Value '{}' at 'requestItems' failed to satisfy constraint: {MAP_VALUE_CONSTRAINT}",
            request_items_echo(&request.request_items)
        ))
        .chain(enum_faults)
        .collect();
        return Err(DynoxideError::ValidationException(
            crate::validation::format_validation_errors(&member_errors).unwrap_or_default(),
        ));
    }

    // A table name that breaks its constraints is a constraint on the
    // RequestItems map's keys, reported once for the map with the request
    // echoed as JSON, ahead of the enum faults and listed with them (captured
    // eu-west-2, 12 and 13 November 2026).
    let all_names_valid = request
        .request_items
        .keys()
        .all(|name| crate::validation::member_table_name_is_valid(name));
    let member_errors: Vec<String> = (!all_names_valid)
        .then(|| {
            format!(
                "Value '{}' at 'requestItems' failed to satisfy constraint: {}",
                request_items_json_echo(&request.request_items),
                crate::validation::REQUEST_ITEMS_KEY_CONSTRAINT
            )
        })
        .into_iter()
        .chain(enum_faults)
        .collect();
    if let Some(msg) = crate::validation::format_validation_errors(&member_errors) {
        return Err(DynoxideError::ValidationException(msg));
    }

    Ok(())
}

/// The `RequestItems` map echoed as JSON, as eu-west-2 echoes it for a table
/// name that breaks its constraints (captured 12 and 13 November 2026): each
/// write request as it is written on the wire, with tables and every map's
/// members in order of name, as BatchGetItem's echo is. Bounded by
/// [`ECHO_LIMIT`].
fn request_items_json_echo(request_items: &HashMap<String, Vec<WriteRequest>>) -> String {
    let echo: serde_json::Map<String, serde_json::Value> = request_items
        .iter()
        .map(|(table_name, requests)| {
            let requests = requests
                .iter()
                .map(|request| {
                    let mut members = serde_json::Map::new();
                    if let Some(ref put) = request.put_request {
                        members.insert(
                            "PutRequest".to_string(),
                            serde_json::json!({ "Item": put.item }),
                        );
                    }
                    if let Some(ref delete) = request.delete_request {
                        members.insert(
                            "DeleteRequest".to_string(),
                            serde_json::json!({ "Key": delete.key }),
                        );
                    }
                    serde_json::Value::Object(members)
                })
                .collect();
            (table_name.clone(), serde_json::Value::Array(requests))
        })
        .collect();
    bound_echo(crate::sorted_json(serde_json::Value::Object(echo)).to_string())
}

pub async fn execute<S: StorageBackend>(
    storage: &S,
    mut request: BatchWriteItemRequest,
) -> Result<BatchWriteItemResponse> {
    const MAX_REQUEST_SIZE: usize = 16 * 1024 * 1024; // 16MB

    check_request(&request)?;

    // --- Pre-table validations ---
    // DynamoDB validates attribute values, item size, and empty write requests
    // BEFORE checking table existence.

    // An empty attribute name in an item or a key is refused bare, before
    // anything else here, a missing table included (captured in ten regions).
    let empty_name = request.request_items.values().flatten().any(|wr| {
        wr.put_request
            .as_ref()
            .is_some_and(|put| crate::empty_names::map_has_empty_name(&put.item))
            || wr
                .delete_request
                .as_ref()
                .is_some_and(|delete| crate::empty_names::map_has_empty_name(&delete.key))
    });
    if empty_name {
        return Err(crate::empty_names::bare());
    }
    for write_requests in request.request_items.values() {
        for wr in write_requests {
            // A write request holds exactly one of a put and a delete; one
            // holding both is refused in the same words as one holding
            // neither (captured eu-west-2 and us-east-1, 11 November 2026).
            if wr.put_request.is_some() == wr.delete_request.is_some() {
                return Err(DynoxideError::ValidationException(
                    "Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes".to_string(),
                ));
            }
            if let Some(ref put_req) = wr.put_request {
                // Validate attribute values (empty strings, empty sets, invalid numbers)
                crate::validation::validate_item_attribute_values(&put_req.item)?;

                // Validate item size before table lookup
                let size = types::item_size(&put_req.item);
                if size > types::MAX_ITEM_SIZE {
                    return Err(DynoxideError::ValidationException(
                        "Item size has exceeded the maximum allowed size".to_string(),
                    ));
                }
            }
            if let Some(ref del_req) = wr.delete_request {
                crate::validation::validate_item_attribute_values(&del_req.key)?;
            }
        }
    }

    // Validate aggregate request size
    let total_size: usize = request
        .request_items
        .values()
        .flat_map(|wrs| wrs.iter())
        .map(|wr| {
            if let Some(ref put_req) = wr.put_request {
                types::item_size(&put_req.item)
            } else if let Some(ref del_req) = wr.delete_request {
                types::item_size(&del_req.key)
            } else {
                0
            }
        })
        .sum();
    if total_size > MAX_REQUEST_SIZE {
        return Err(DynoxideError::ValidationException(
            "Item collection too large: aggregate size of items in BatchWriteItem exceeds 16MB limit".to_string(),
        ));
    }

    // Validate: no duplicate keys across all operations
    {
        let mut seen_keys: std::collections::HashSet<(String, String, String)> =
            std::collections::HashSet::new();
        for (table_name, write_requests) in &request.request_items {
            let meta = helpers::require_table_for_item_op(storage, table_name).await?;
            let key_schema = helpers::parse_key_schema(&meta)?;
            for wr in write_requests {
                // Validate keys BEFORE extract_key_strings: that helper returns
                // InternalServerError (HTTP 500) for a missing partition or sort
                // key, but a key-less request is client input and must surface as
                // a 400 ValidationException. Mirrors put_item.rs, which validates
                // before extracting.
                let key_item = if let Some(ref put) = wr.put_request {
                    require_key_attributes(&put.item, &key_schema)?;
                    helpers::validate_item_keys_for_batch(&put.item, &key_schema, &meta)?;
                    &put.item
                } else if let Some(ref del) = wr.delete_request {
                    helpers::validate_key_only(&del.key, &key_schema)?;
                    &del.key
                } else {
                    continue;
                };
                let (pk, sk) = helpers::extract_key_strings(key_item, &key_schema)?;
                let key = (table_name.clone(), pk, sk);
                if !seen_keys.insert(key) {
                    return Err(DynoxideError::ValidationException(
                        "Provided list of item keys contains duplicates".to_string(),
                    ));
                }
            }
        }
    }

    // Track per-table secondary index capacity and affected partition keys for
    // deferred metrics
    let mut table_gsi_units: HashMap<String, HashMap<String, f64>> = HashMap::new();
    let mut table_lsi_units: HashMap<String, HashMap<String, f64>> = HashMap::new();
    // Vector replication is billed in bytes, so it accumulates on its own map
    // and never joins the unit totals.
    let mut table_vector_bytes: HashMap<String, HashMap<String, f64>> = HashMap::new();
    // Track per-table WCU (table-level, excludes GSI)
    let mut table_wcu: HashMap<String, f64> = HashMap::new();
    // Collect unique (table, pk_str, pk_attr, pk_value) for deferred metrics computation
    let mut affected_partitions: Vec<(String, String, String, AttributeValue)> = Vec::new();

    for (table_name, write_requests) in &mut request.request_items {
        let meta = helpers::require_table_for_item_op(storage, table_name).await?;
        let key_schema = helpers::parse_key_schema(&meta)?;
        // Parse the index definitions once per table: the maintain helpers'
        // meta-accepting forms deserialise the JSON on every call, which for
        // a 25-item batch against one table would repeat the work per item.
        let gsi_defs = super::gsi::parse_gsi_defs(&meta)?;
        let lsi_defs = super::lsi::parse_lsi_defs(&meta)?;
        let vector_defs = super::vector_index::parse_vector_defs(&meta)?;
        let attr_defs = super::vector_index::parse_attr_defs(&meta)?;

        for wr in write_requests {
            if let Some(ref mut put_req) = wr.put_request {
                // Validate keys
                helpers::validate_item_keys_for_batch(&put_req.item, &key_schema, &meta)?;

                // Validate attribute values (empty strings, empty sets)
                crate::validation::validate_item_attribute_values(&put_req.item)?;

                // Normalize sets (deduplication)
                crate::validation::normalize_item_sets(&mut put_req.item);

                // Validate item size, by the number rule, as DynamoDB does
                if types::item_size(&put_req.item) > types::MAX_ITEM_SIZE {
                    return Err(DynoxideError::ValidationException(
                        "Item size has exceeded the maximum allowed size".to_string(),
                    ));
                }
                // What the row records and the write is charged: the billed
                // size, with a vector attribute at 4 bytes per dimension.
                let size = types::billed_item_size(&put_req.item, &vector_defs);

                // TODO: validation must precede this call -- if reaching this line, caller has already validated keys.
                let (pk, sk) = helpers::extract_key_strings(&put_req.item, &key_schema)?;
                let item_json = serde_json::to_string(&put_req.item)
                    .map_err(|e| DynoxideError::InternalServerError(e.to_string()))?;
                let hash_prefix = put_req
                    .item
                    .get(&key_schema.partition_key)
                    .map(crate::storage::compute_hash_prefix)
                    .unwrap_or_default();
                // Base write + index fan-out + stream are one atomic unit per
                // item: a mid-fan-out failure rolls this item's write back.
                // BatchWriteItem items are independent, so this is one
                // transaction per write request.
                let (old_size, gsi_units, lsi_units, vector_bytes) =
                    helpers::with_write_transaction(storage, async {
                        let old_json = storage
                            .put_item_with_hash(
                                table_name,
                                &pk,
                                &sk,
                                &item_json,
                                size,
                                &hash_prefix,
                            )
                            .await?;
                        let old_item: Option<Item> =
                            old_json.and_then(|j| serde_json::from_str(&j).ok());
                        let target = super::gsi::IndexWrite {
                            table_name,
                            pk: &pk,
                            sk: &sk,
                            pk_attr: &key_schema.partition_key,
                            sk_attr: key_schema.sort_key.as_deref(),
                            old_item: old_item.as_ref(),
                            capacity_mode: request.return_consumed_capacity.as_deref(),
                        };
                        let gsi_units = super::gsi::maintain_gsis_after_write_with_defs(
                            storage,
                            &gsi_defs,
                            &target,
                            &put_req.item,
                        )
                        .await?;
                        let lsi_units = super::lsi::maintain_lsis_after_write_with_defs(
                            storage,
                            &lsi_defs,
                            &target,
                            &put_req.item,
                        )
                        .await?;
                        let vector_bytes =
                            super::vector_index::maintain_vector_indexes_after_write_with_defs(
                                storage,
                                &vector_defs,
                                &attr_defs,
                                &target,
                                &put_req.item,
                                false,
                            )
                            .await?;
                        crate::streams::record_stream_event(
                            storage,
                            &meta,
                            old_item.as_ref(),
                            Some(&put_req.item),
                        )
                        .await?;
                        Ok((
                            old_item
                                .as_ref()
                                .map(|old| types::billed_item_size(old, &vector_defs)),
                            gsi_units,
                            lsi_units,
                            vector_bytes,
                        ))
                    })
                    .await?;

                // Accumulate WCU on the larger of the displaced and new images
                *table_wcu.entry(table_name.clone()).or_insert(0.0) +=
                    types::table_write_capacity_units(old_size, Some(size));

                // Accumulate secondary index units per table
                accumulate_index_units(&mut table_gsi_units, table_name, &gsi_units);
                accumulate_index_units(&mut table_lsi_units, table_name, &lsi_units);
                accumulate_index_units(&mut table_vector_bytes, table_name, &vector_bytes);

                // Track affected partition for deferred metrics
                if let Some(pk_val) = put_req.item.get(&key_schema.partition_key) {
                    affected_partitions.push((
                        table_name.clone(),
                        pk.clone(),
                        key_schema.partition_key.clone(),
                        pk_val.clone(),
                    ));
                }
            } else if let Some(ref del_req) = wr.delete_request {
                helpers::validate_key_only(&del_req.key, &key_schema)?;
                // TODO: validation must precede this call -- if reaching this line, caller has already validated keys.
                let (pk, sk) = helpers::extract_key_strings(&del_req.key, &key_schema)?;

                // Base delete + index fan-out + stream are one atomic unit per item.
                let (old_item, gsi_units, lsi_units, vector_bytes) =
                    helpers::with_write_transaction(storage, async {
                        let old_json = storage.delete_item(table_name, &pk, &sk).await?;
                        let old_item: Option<Item> =
                            old_json.as_ref().and_then(|j| serde_json::from_str(j).ok());
                        let target = super::gsi::IndexWrite {
                            table_name,
                            pk: &pk,
                            sk: &sk,
                            pk_attr: &key_schema.partition_key,
                            sk_attr: key_schema.sort_key.as_deref(),
                            old_item: old_item.as_ref(),
                            capacity_mode: request.return_consumed_capacity.as_deref(),
                        };
                        let gsi_units = super::gsi::maintain_gsis_after_delete_with_defs(
                            storage, &gsi_defs, &target,
                        )
                        .await?;
                        let lsi_units = super::lsi::maintain_lsis_after_delete_with_defs(
                            storage, &lsi_defs, &target,
                        )
                        .await?;
                        let vector_bytes =
                            super::vector_index::maintain_vector_indexes_after_delete_with_defs(
                                storage,
                                &vector_defs,
                                &attr_defs,
                                &target,
                            )
                            .await?;
                        if old_item.is_some() {
                            crate::streams::record_stream_event(
                                storage,
                                &meta,
                                old_item.as_ref(),
                                None,
                            )
                            .await?;
                        }
                        Ok((old_item, gsi_units, lsi_units, vector_bytes))
                    })
                    .await?;

                // Accumulate WCU: based on old item size if it existed, else 1 WCU
                *table_wcu.entry(table_name.clone()).or_insert(0.0) +=
                    types::table_write_capacity_units(
                        old_item
                            .as_ref()
                            .map(|old| types::billed_item_size(old, &vector_defs)),
                        None,
                    );

                // Accumulate secondary index units per table
                accumulate_index_units(&mut table_gsi_units, table_name, &gsi_units);
                accumulate_index_units(&mut table_lsi_units, table_name, &lsi_units);
                accumulate_index_units(&mut table_vector_bytes, table_name, &vector_bytes);

                // Track affected partition for deferred metrics
                if let Some(pk_val) = del_req.key.get(&key_schema.partition_key) {
                    affected_partitions.push((
                        table_name.clone(),
                        pk.clone(),
                        key_schema.partition_key.clone(),
                        pk_val.clone(),
                    ));
                }
            } else {
                return Err(DynoxideError::ValidationException(
                    "WriteRequest must contain either PutRequest or DeleteRequest".to_string(),
                ));
            }
        }
    }

    // Build consumed capacity per table using pre-tracked WCU
    let consumed_capacity = if matches!(
        request.return_consumed_capacity.as_deref(),
        Some("TOTAL") | Some("INDEXES")
    ) {
        let mut caps = Vec::new();
        for table_name in request.request_items.keys() {
            let total_wcu = table_wcu.get(table_name).copied().unwrap_or(0.0);
            let gsi_units = table_gsi_units.get(table_name).cloned().unwrap_or_default();
            let lsi_units = table_lsi_units.get(table_name).cloned().unwrap_or_default();
            let vector_bytes = table_vector_bytes
                .get(table_name)
                .cloned()
                .unwrap_or_default();
            if let Some(cc) = crate::types::consumed_capacity_with_vector_indexes(
                table_name,
                total_wcu,
                &gsi_units,
                &lsi_units,
                &vector_bytes,
                &request.return_consumed_capacity,
            ) {
                caps.push(cc);
            }
        }
        Some(caps)
    } else {
        None
    };

    // Item collection metrics are computed once per unique (table, pk), deferred from the write loop
    let mut all_item_collection_metrics: HashMap<String, Vec<crate::types::ItemCollectionMetrics>> =
        HashMap::new();
    if matches!(
        request.return_item_collection_metrics.as_deref(),
        Some("SIZE")
    ) {
        // Deduplicate by (table, pk) to avoid redundant queries
        let mut seen = std::collections::HashSet::new();
        for (tbl, pk_str, pk_attr, pk_val) in &affected_partitions {
            let key = (tbl.as_str(), pk_str.as_str());
            if !seen.insert(key) {
                continue;
            }
            let meta = helpers::require_table(storage, tbl).await?;
            if let Some(icm) = helpers::build_item_collection_metrics(
                storage,
                &meta,
                tbl,
                pk_str,
                pk_attr,
                pk_val,
                &request.return_item_collection_metrics,
            )
            .await?
            {
                all_item_collection_metrics
                    .entry(tbl.clone())
                    .or_default()
                    .push(icm);
            }
        }
    }
    let item_collection_metrics = if all_item_collection_metrics.is_empty() {
        None
    } else {
        Some(all_item_collection_metrics)
    };

    Ok(BatchWriteItemResponse {
        unprocessed_items: HashMap::new(),
        consumed_capacity,
        item_collection_metrics,
    })
}

/// A put whose item lacks a key attribute is refused with the schema message,
/// where PutItem names the missing key (captured us-east-1 and eu-west-2,
/// 2026-10-09).
fn require_key_attributes(item: &Item, schema: &helpers::KeySchema) -> Result<()> {
    let missing = !item.contains_key(&schema.partition_key)
        || schema
            .sort_key
            .as_ref()
            .is_some_and(|sk| !item.contains_key(sk));
    if missing {
        return Err(DynoxideError::ValidationException(
            "The provided key element does not match the schema".to_string(),
        ));
    }
    Ok(())
}

/// The most of a member a constraint message echoes, in bytes, before the echo
/// is cut. A cut echo is this many bytes (less any partial character at the
/// cut) followed by the three-character `...`.
///
/// DynamoDB echoes the refused member in its Java model's `toString`, which
/// names all sixteen of a value's fields and so runs to about twenty times the
/// request's own size: a rejected request near the 16 MiB body limit would
/// build an echo hundreds of megabytes long. Dynoxide stops the echo here and
/// ends it with `...`. The limit is Dynoxide's own, well above the echo of any
/// ordinary request (26 statements of 8,192 characters echo to about 220 KB);
/// what DynamoDB does with an echo this size has not been captured.
pub(crate) const ECHO_LIMIT: usize = 1024 * 1024;

/// A string that takes writes up to [`ECHO_LIMIT`] bytes and then refuses
/// them, so a formatter writing into it stops as soon as it is full rather than
/// building the whole echo first.
pub(crate) struct BoundedEcho {
    text: String,
    full: bool,
}

impl BoundedEcho {
    pub(crate) fn new() -> Self {
        BoundedEcho {
            text: String::new(),
            full: false,
        }
    }

    /// The echo, ending `...` if it reached the limit.
    pub(crate) fn finish(mut self) -> String {
        if self.full {
            self.text.push_str("...");
        }
        self.text
    }
}

impl std::fmt::Write for BoundedEcho {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        if self.full {
            return Err(std::fmt::Error);
        }
        let room = ECHO_LIMIT - self.text.len();
        if s.len() <= room {
            self.text.push_str(s);
            return Ok(());
        }
        let mut cut = room;
        while !s.is_char_boundary(cut) {
            cut -= 1;
        }
        self.text.push_str(&s[..cut]);
        self.full = true;
        Err(std::fmt::Error)
    }
}

/// Cut an echo already built to [`ECHO_LIMIT`], for an echo that is no larger
/// than the request it comes from.
pub(crate) fn bound_echo(echo: String) -> String {
    let mut out = BoundedEcho::new();
    let _ = std::fmt::Write::write_str(&mut out, &echo);
    out.finish()
}

/// The `RequestItems` map in the older echo, the request object's Java
/// `toString`, which names every field of every value. Eleven of twelve regions
/// printed it this way on 2026-10-09 for a request list that was empty or too
/// long and for a table name that broke its constraints. On 13 November 2026
/// eu-west-2 no longer echoed the map for a list's length and echoed it as JSON
/// for a table name (see [`check_request`]), and us-east-1 still gave this
/// echo for both. Both regions still give it for a list too long beside a put
/// with no `Item`, sometimes with each attribute value as an object hash
/// instead, so no echo matches every answer byte for byte. Tables are in order
/// of name, where DynamoDB's own order is its map's; attributes follow the Java
/// map's order. Bounded by [`ECHO_LIMIT`].
fn request_items_echo(request_items: &HashMap<String, Vec<WriteRequest>>) -> String {
    let mut out = BoundedEcho::new();
    let _ = write_request_items(&mut out, request_items);
    out.finish()
}

fn write_request_items(
    out: &mut dyn std::fmt::Write,
    request_items: &HashMap<String, Vec<WriteRequest>>,
) -> std::fmt::Result {
    let mut tables: Vec<(&String, &Vec<WriteRequest>)> = request_items.iter().collect();
    tables.sort_by(|a, b| a.0.cmp(b.0));
    out.write_str("{")?;
    for (i, (table_name, requests)) in tables.into_iter().enumerate() {
        if i > 0 {
            out.write_str(", ")?;
        }
        write!(out, "{table_name}=[")?;
        for (j, request) in requests.iter().enumerate() {
            if j > 0 {
                out.write_str(", ")?;
            }
            write_write_request(
                out,
                request.put_request.as_ref().map(|put| Some(&put.item)),
                request
                    .delete_request
                    .as_ref()
                    .map(|delete| Some(&delete.key)),
            )?;
        }
        out.write_str("]")?;
    }
    out.write_str("}")
}

/// The `RequestItems` map as read, before its missing and null members are
/// refused, echoed as [`request_items_echo`] echoes a request that passed
/// them: a null list, write request, item or key printed `null`, as the Java
/// model prints it (captured us-east-1, 13 November 2026).
fn wire_request_items_echo(
    request_items: &helpers::WrittenTables<Option<Vec<Option<WriteRequestWire>>>>,
) -> String {
    let mut out = BoundedEcho::new();
    let _ = write_wire_request_items(&mut out, request_items);
    out.finish()
}

fn write_wire_request_items(
    out: &mut dyn std::fmt::Write,
    request_items: &helpers::WrittenTables<Option<Vec<Option<WriteRequestWire>>>>,
) -> std::fmt::Result {
    let mut tables: Vec<&(String, Option<Vec<Option<WriteRequestWire>>>)> =
        request_items.0.iter().collect();
    tables.sort_by(|a, b| a.0.cmp(&b.0));
    out.write_str("{")?;
    for (i, (table_name, requests)) in tables.into_iter().enumerate() {
        if i > 0 {
            out.write_str(", ")?;
        }
        write!(out, "{table_name}=")?;
        let Some(requests) = requests else {
            out.write_str("null")?;
            continue;
        };
        out.write_str("[")?;
        for (j, request) in requests.iter().enumerate() {
            if j > 0 {
                out.write_str(", ")?;
            }
            match request {
                Some(request) => write_write_request(
                    out,
                    request
                        .put_request
                        .as_ref()
                        .map(|put| put.item.as_ref().map(|item| &item.0)),
                    request
                        .delete_request
                        .as_ref()
                        .map(|delete| delete.key.as_ref().map(|key| &key.0)),
                )?,
                None => out.write_str("null")?,
            }
        }
        out.write_str("]")?;
    }
    out.write_str("}")
}

/// One write request as the Java model prints it: each of its put and delete
/// absent (`None`), present with no item or key (`Some(None)`), or present
/// with one.
fn write_write_request(
    out: &mut dyn std::fmt::Write,
    put: Option<Option<&HashMap<String, AttributeValue>>>,
    delete: Option<Option<&HashMap<String, AttributeValue>>>,
) -> std::fmt::Result {
    let held = |out: &mut dyn std::fmt::Write,
                kind: &str,
                member: &str,
                map: Option<Option<&HashMap<String, AttributeValue>>>|
     -> std::fmt::Result {
        match map {
            Some(map) => {
                write!(out, "{kind}({member}=")?;
                match map {
                    Some(map) => write_java_item(out, map)?,
                    None => out.write_str("null")?,
                }
                out.write_str(", workloadProfileName=null)")
            }
            None => out.write_str("null"),
        }
    };
    out.write_str("WriteRequest(putRequest=")?;
    held(out, "PutRequest", "item", put)?;
    out.write_str(", deleteRequest=")?;
    held(out, "DeleteRequest", "key", delete)?;
    out.write_str(")")
}

/// A map's names in the order the Java model prints them: the iteration order
/// of a hash map sized to its entries (`new HashMap<>(n)`), so 1 name gives
/// two buckets, 2 or 3 give four, 4 to 6 give eight, and so on up, doubling
/// whenever more than three quarters full (captured us-east-1 and eu-west-2,
/// 2026-10-10, for an item's attributes, a nested map and
/// ExpressionAttributeNames, of 1 to 49 names). Names sharing a bucket keep the
/// order they were sent in on DynamoDB, which a decoded map no longer holds;
/// they are taken in order of name.
fn java_map_order<'a, I: Iterator<Item = &'a String>>(names: I) -> Vec<&'a str> {
    let mut names: Vec<&str> = names.map(String::as_str).collect();
    names.sort_unstable();
    crate::validation::java_hash_order(&names, true)
}

/// An attribute map, `{name=AttributeValue(...), ...}`, in the Java model's
/// order.
pub(crate) fn write_java_item(
    out: &mut dyn std::fmt::Write,
    item: &HashMap<String, AttributeValue>,
) -> std::fmt::Result {
    out.write_str("{")?;
    for (i, name) in java_map_order(item.keys()).into_iter().enumerate() {
        if i > 0 {
            out.write_str(", ")?;
        }
        write!(out, "{name}=")?;
        write_java_attribute_value(out, &item[name])?;
    }
    out.write_str("}")
}

/// Expression attribute names as written, a null name printed `null` as the
/// Java model prints it (captured eu-west-2 and us-east-1, 2026-11-12).
pub(crate) fn write_java_written_names(
    out: &mut dyn std::fmt::Write,
    names: Option<&crate::empty_names::WrittenNames>,
) -> std::fmt::Result {
    let Some(names) = names else {
        return out.write_str("null");
    };
    out.write_str("{")?;
    for (i, name) in java_map_order(names.keys()).into_iter().enumerate() {
        if i > 0 {
            out.write_str(", ")?;
        }
        write!(out, "{name}={}", names[name].as_deref().unwrap_or("null"))?;
    }
    out.write_str("}")
}

/// An optional string member as the Java model prints it: the text, or `null`.
pub(crate) fn write_java_text(
    out: &mut dyn std::fmt::Write,
    text: Option<&str>,
) -> std::fmt::Result {
    out.write_str(text.unwrap_or("null"))
}

/// One attribute value with all sixteen of the Java model's fields, the one in
/// use filled in, binary shown as Java prints a `ByteBuffer` (every type
/// captured us-east-1, eu-west-1 and us-west-2, 2026-10-10).
pub(crate) fn write_java_attribute_value(
    out: &mut dyn std::fmt::Write,
    value: &AttributeValue,
) -> std::fmt::Result {
    const FIELDS: [&str; 16] = [
        "s",
        "n",
        "b",
        "iNT",
        "dECIMAL",
        "dOUBLE",
        "sS",
        "nS",
        "bS",
        "iNTSET",
        "dECIMALSET",
        "dOUBLESET",
        "m",
        "l",
        "nULL",
        "bOOL",
    ];
    let slot = match value {
        AttributeValue::S(_) => 0,
        AttributeValue::N(_) => 1,
        AttributeValue::B(_) => 2,
        AttributeValue::SS(_) => 6,
        AttributeValue::NS(_) => 7,
        AttributeValue::BS(_) => 8,
        AttributeValue::M(_) => 12,
        AttributeValue::L(_) => 13,
        AttributeValue::NULL(_) => 14,
        AttributeValue::BOOL(_) => 15,
    };
    out.write_str("AttributeValue(")?;
    for (i, name) in FIELDS.iter().enumerate() {
        if i > 0 {
            out.write_str(", ")?;
        }
        write!(out, "{name}=")?;
        if i == slot {
            write_java_shown(out, value)?;
        } else {
            out.write_str("null")?;
        }
    }
    out.write_str(")")
}

fn write_java_shown(out: &mut dyn std::fmt::Write, value: &AttributeValue) -> std::fmt::Result {
    let byte_buffer = |out: &mut dyn std::fmt::Write, bytes: &[u8]| {
        write!(
            out,
            "java.nio.HeapByteBuffer[pos=0 lim={len} cap={len}]",
            len = bytes.len()
        )
    };
    match value {
        AttributeValue::S(s) => out.write_str(s),
        AttributeValue::N(n) => out.write_str(n),
        AttributeValue::B(b) => byte_buffer(out, b),
        AttributeValue::SS(set) | AttributeValue::NS(set) => {
            out.write_str("[")?;
            for (i, member) in set.iter().enumerate() {
                if i > 0 {
                    out.write_str(", ")?;
                }
                out.write_str(member)?;
            }
            out.write_str("]")
        }
        AttributeValue::BS(set) => {
            out.write_str("[")?;
            for (i, member) in set.iter().enumerate() {
                if i > 0 {
                    out.write_str(", ")?;
                }
                byte_buffer(out, member)?;
            }
            out.write_str("]")
        }
        AttributeValue::M(map) => write_java_item(out, map),
        AttributeValue::L(list) => {
            out.write_str("[")?;
            for (i, member) in list.iter().enumerate() {
                if i > 0 {
                    out.write_str(", ")?;
                }
                write_java_attribute_value(out, member)?;
            }
            out.write_str("]")
        }
        AttributeValue::NULL(null) => write!(out, "{null}"),
        AttributeValue::BOOL(b) => write!(out, "{b}"),
    }
}

#[cfg(test)]
mod tests {
    use super::{BoundedEcho, ECHO_LIMIT, bound_echo};
    use crate::actions::{batch_write_item, create_table};
    use crate::storage::Storage;
    use crate::storage_backend::StorageBackend;
    use std::fmt::Write;

    /// An echo that fits is returned untouched, with no `...`, right up to the
    /// limit itself.
    #[test]
    fn an_echo_of_exactly_the_limit_is_untouched() {
        let echo = "a".repeat(ECHO_LIMIT);
        assert_eq!(bound_echo(echo.clone()), echo);
        assert_eq!(bound_echo("short".to_string()), "short");
    }

    /// One byte over the limit is cut at the limit and ends `...`.
    #[test]
    fn an_echo_one_byte_over_the_limit_is_cut_and_marked() {
        let cut = bound_echo("a".repeat(ECHO_LIMIT + 1));
        assert_eq!(cut.len(), ECHO_LIMIT + 3);
        assert!(cut.ends_with("a..."));
    }

    /// A character that straddles the limit is left out whole rather than
    /// split, for two- and four-byte characters and for an echo written in
    /// many small pieces as well as one large one.
    #[test]
    fn an_echo_is_never_cut_inside_a_character() {
        for (filler, ch) in [(ECHO_LIMIT - 1, "é"), (ECHO_LIMIT - 2, "😀")] {
            let echo = format!("{}{ch}tail", "a".repeat(filler));
            let cut = bound_echo(echo);
            assert_eq!(cut, format!("{}...", "a".repeat(filler)), "cut at {ch}");

            let mut pieces = BoundedEcho::new();
            for _ in 0..filler {
                let _ = pieces.write_str("a");
            }
            assert!(pieces.write_str(ch).is_err(), "{ch} did not fill the echo");
            assert!(pieces.write_str("more").is_err(), "a full echo took more");
            assert_eq!(pieces.finish(), format!("{}...", "a".repeat(filler)));
        }
    }

    /// Each batch put is atomic with its own GSI fan-out: a mid-fan-out failure
    /// rolls that item's base write back rather than leaving a torn index.
    #[test]
    fn batch_put_rolls_back_base_write_when_gsi_fan_out_fails() {
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

        // Break the second GSI's fan-out by dropping its physical table.
        storage.drop_gsi_table("Orders", "PriorityIndex").unwrap();

        let batch = serde_json::from_value(serde_json::json!({
            "RequestItems": {
                "Orders": [
                    {"PutRequest": {"Item": {"UserId": {"S": "u1"}, "Status": {"S": "SHIPPED"}, "Priority": {"S": "HIGH"}}}}
                ]
            }
        }))
        .unwrap();
        let res = pollster::block_on(batch_write_item::execute(&storage, batch));
        assert!(
            res.is_err(),
            "a mid-fan-out failure must surface as an error"
        );

        // The base write must roll back: no item landed.
        let count =
            pollster::block_on(<Storage as StorageBackend>::count_items(&storage, "Orders"))
                .unwrap();
        assert_eq!(
            count, 0,
            "batch put base write must roll back when fan-out fails"
        );
    }
}
