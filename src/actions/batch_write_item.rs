use crate::actions::helpers;
use crate::errors::{DynoxideError, Result};
use crate::storage_backend::StorageBackend;
use crate::types::{self, AttributeValue, Item};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Default, Deserialize)]
pub struct BatchWriteItemRequest {
    #[serde(rename = "RequestItems")]
    pub request_items: HashMap<String, Vec<WriteRequest>>,
    #[serde(rename = "ReturnConsumedCapacity", default)]
    pub return_consumed_capacity: Option<String>,
    #[serde(rename = "ReturnItemCollectionMetrics", default)]
    pub return_item_collection_metrics: Option<String>,
}

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

/// The request-level checks, in the order DynamoDB reports them: an empty
/// `RequestItems`; then the member constraints together (a table's request list
/// that is empty or pushes the request past 25, ReturnConsumedCapacity and
/// ReturnItemCollectionMetrics); then a table name that breaks its constraints,
/// alone.
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

    // The request's member constraints, reported together before anything is
    // read or written: a table's request list that is empty or pushes the
    // request past 25, then ReturnConsumedCapacity and
    // ReturnItemCollectionMetrics outside their enums (captured us-east-1,
    // eu-west-2 and ten more regions, 2026-10-08 and 2026-10-09).
    let mut member_errors = Vec::new();
    let total_requests: usize = request.request_items.values().map(|v| v.len()).sum();
    if total_requests > 25 || request.request_items.values().any(|wrs| wrs.is_empty()) {
        member_errors.push(format!(
            "Value '{}' at 'requestItems' failed to satisfy constraint: Map value must satisfy constraint: [Member must have length less than or equal to 25, Member must have length greater than or equal to 1]",
            request_items_echo(&request.request_items)
        ));
    }
    if let Some(msg) = crate::validation::return_consumed_capacity_rejection(
        request.return_consumed_capacity.as_deref(),
    ) {
        member_errors.push(msg);
    }
    if let Some(msg) = crate::validation::return_item_collection_metrics_rejection(
        request.return_item_collection_metrics.as_deref(),
    ) {
        member_errors.push(msg);
    }
    if let Some(msg) = crate::validation::format_validation_errors(&member_errors) {
        return Err(DynoxideError::ValidationException(msg));
    }

    // A table name that breaks its constraints comes after those, and alone:
    // it is a constraint on the RequestItems map's keys, reported once for the
    // map with the map echoed.
    if !request
        .request_items
        .keys()
        .all(|name| crate::validation::member_table_name_is_valid(name))
    {
        return Err(DynoxideError::ValidationException(
            crate::validation::envelope_message(&format!(
                "Value '{}' at 'requestItems' failed to satisfy constraint: {}",
                request_items_echo(&request.request_items),
                crate::validation::REQUEST_ITEMS_KEY_CONSTRAINT
            )),
        ));
    }

    Ok(())
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
            if wr.put_request.is_none() && wr.delete_request.is_none() {
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

/// The `RequestItems` map as DynamoDB echoes it in a constraint message: the
/// request object's Java `toString`, which names every field of every value.
/// Eleven of twelve regions printed it this way on 2026-10-09; the other, and
/// eu-west-2 on other days, print each attribute value as an object hash, so no
/// echo matches every region byte for byte. Tables are in order of name, where
/// DynamoDB's own order is its map's; attributes follow the Java map's order.
/// Bounded by [`ECHO_LIMIT`].
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
            write_write_request(out, request)?;
        }
        out.write_str("]")?;
    }
    out.write_str("}")
}

fn write_write_request(out: &mut dyn std::fmt::Write, request: &WriteRequest) -> std::fmt::Result {
    out.write_str("WriteRequest(putRequest=")?;
    match request.put_request {
        Some(ref put) => {
            out.write_str("PutRequest(item=")?;
            write_java_item(out, &put.item)?;
            out.write_str(", workloadProfileName=null)")?;
        }
        None => out.write_str("null")?,
    }
    out.write_str(", deleteRequest=")?;
    match request.delete_request {
        Some(ref delete) => {
            out.write_str("DeleteRequest(key=")?;
            write_java_item(out, &delete.key)?;
            out.write_str(", workloadProfileName=null)")?;
        }
        None => out.write_str("null")?,
    }
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
