use crate::actions::helpers;
use crate::errors::{DynoxideError, Result};
use crate::storage::Storage;
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
    #[serde(rename = "Item")]
    pub item: Item,
}

#[derive(Debug, Default, Deserialize)]
pub struct DeleteRequest {
    #[serde(rename = "Key")]
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

pub fn execute(
    storage: &Storage,
    mut request: BatchWriteItemRequest,
) -> Result<BatchWriteItemResponse> {
    const MAX_REQUEST_SIZE: usize = 16 * 1024 * 1024; // 16MB

    // Validate RequestItems is not empty.
    // AWS routes the empty-map case through a separate parameter-required path
    // rather than the standard "N validation errors detected" envelope.
    if request.request_items.is_empty() {
        return Err(DynoxideError::ValidationException(
            "The requestItems parameter is required for BatchWriteItem".to_string(),
        ));
    }

    // Validate each table entry has at least one write request
    for (table_name, wrs) in &request.request_items {
        if wrs.is_empty() {
            return Err(DynoxideError::ValidationException(format!(
                "1 validation error detected: Value at 'requestItems.{table_name}.member' failed to satisfy constraint: Member must have length greater than or equal to 1"
            )));
        }
    }

    // Validate table name format for all tables before checking existence
    for table_name in request.request_items.keys() {
        crate::validation::validate_table_name(table_name)?;
    }

    // Validate total request count.
    // AWS surfaces this as the standard "1 validation error detected" envelope
    // and echoes the WriteRequest list inside `Value '{<table>=[<dump>]}'`. The
    // conformance suite anchors a regex around the envelope and the constraint
    // phrase but leaves the dump body unconstrained (because the AWS SDK's
    // Java-toString shape adds new AttributeValue fields over time). We emit
    // the table name verbatim and a Rust Debug dump of the WriteRequests so
    // the envelope matches without coupling to a specific SDK version. If a
    // future suite tightens the regex to pin the dump exactly, this site
    // will need a follow-up change.
    let total_requests: usize = request.request_items.values().map(|v| v.len()).sum();
    if total_requests > 25 {
        let empty: Vec<WriteRequest> = Vec::new();
        let (table_name, requests) = request
            .request_items
            .iter()
            .max_by_key(|(_, v)| v.len())
            .map(|(name, v)| (name.as_str(), v))
            .unwrap_or(("", &empty));
        let dump = format!("{requests:?}");
        return Err(DynoxideError::ValidationException(format!(
            "1 validation error detected: Value '{{{table_name}=[{dump}]}}' at 'requestItems' failed to satisfy constraint: Map value must satisfy constraint: [Member must have length less than or equal to 25, Member must have length greater than or equal to 1]"
        )));
    }

    // --- Pre-table validations ---
    // DynamoDB validates attribute values, item size, and empty write requests
    // BEFORE checking table existence.
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
            let meta = helpers::require_table_for_item_op(storage, table_name)?;
            let key_schema = helpers::parse_key_schema(&meta)?;
            for wr in write_requests {
                let key_item = if let Some(ref put) = wr.put_request {
                    &put.item
                } else if let Some(ref del) = wr.delete_request {
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

    // Track per-table GSI capacity and affected partition keys for deferred metrics
    let mut table_gsi_units: HashMap<String, HashMap<String, f64>> = HashMap::new();
    // Track per-table WCU (table-level, excludes GSI)
    let mut table_wcu: HashMap<String, f64> = HashMap::new();
    // Collect unique (table, pk_str, pk_attr, pk_value) for deferred metrics computation
    let mut affected_partitions: Vec<(String, String, String, AttributeValue)> = Vec::new();

    // OPTIMISATION: maintain_gsis_after_write/maintain_lsis_after_write each
    // deserialise GSI/LSI definitions from JSON on every call. For batch writes
    // of 25 items against one table, that's 50 redundant deserialise calls.
    // A future improvement would hoist parse_gsi_defs/parse_lsi_defs to this
    // level and pass pre-parsed defs into the maintenance functions.

    for (table_name, write_requests) in &mut request.request_items {
        let meta = helpers::require_table_for_item_op(storage, table_name)?;
        let key_schema = helpers::parse_key_schema(&meta)?;

        for wr in write_requests {
            if let Some(ref mut put_req) = wr.put_request {
                // Validate keys
                helpers::validate_item_keys(&put_req.item, &key_schema, &meta)?;

                // Validate attribute values (empty strings, empty sets)
                crate::validation::validate_item_attribute_values(&put_req.item)?;

                // Normalize sets (deduplication)
                crate::validation::normalize_item_sets(&mut put_req.item);

                // Validate item size
                let size = types::item_size(&put_req.item);
                if size > types::MAX_ITEM_SIZE {
                    return Err(DynoxideError::ValidationException(
                        "Item size has exceeded the maximum allowed size".to_string(),
                    ));
                }

                let (pk, sk) = helpers::extract_key_strings(&put_req.item, &key_schema)?;
                let item_json = serde_json::to_string(&put_req.item)
                    .map_err(|e| DynoxideError::InternalServerError(e.to_string()))?;
                let hash_prefix = put_req
                    .item
                    .get(&key_schema.partition_key)
                    .map(crate::storage::compute_hash_prefix)
                    .unwrap_or_default();
                let old_json = storage.put_item_with_hash(
                    table_name,
                    &pk,
                    &sk,
                    &item_json,
                    size,
                    &hash_prefix,
                )?;

                // Accumulate WCU based on item size
                *table_wcu.entry(table_name.clone()).or_insert(0.0) +=
                    types::write_capacity_units(size);

                // Maintain GSI tables
                let gsi_units = super::gsi::maintain_gsis_after_write(
                    storage,
                    table_name,
                    &meta,
                    &pk,
                    &sk,
                    &put_req.item,
                    &key_schema.partition_key,
                    key_schema.sort_key.as_deref(),
                )?;

                // Accumulate GSI units per table
                let table_entry = table_gsi_units.entry(table_name.clone()).or_default();
                for (gsi_name, units) in &gsi_units {
                    *table_entry.entry(gsi_name.clone()).or_insert(0.0) += units;
                }

                // Maintain LSI tables
                super::lsi::maintain_lsis_after_write(
                    storage,
                    table_name,
                    &meta,
                    &pk,
                    &sk,
                    &put_req.item,
                    &key_schema.partition_key,
                    key_schema.sort_key.as_deref(),
                )?;

                // Track affected partition for deferred metrics
                if let Some(pk_val) = put_req.item.get(&key_schema.partition_key) {
                    affected_partitions.push((
                        table_name.clone(),
                        pk.clone(),
                        key_schema.partition_key.clone(),
                        pk_val.clone(),
                    ));
                }

                // Record stream event
                let old_item: Option<Item> = old_json.and_then(|j| serde_json::from_str(&j).ok());
                crate::streams::record_stream_event(
                    storage,
                    &meta,
                    old_item.as_ref(),
                    Some(&put_req.item),
                )?;
            } else if let Some(ref del_req) = wr.delete_request {
                helpers::validate_key_only(&del_req.key, &key_schema)?;
                let (pk, sk) = helpers::extract_key_strings(&del_req.key, &key_schema)?;
                let old_json = storage.delete_item(table_name, &pk, &sk)?;

                // Accumulate WCU: based on old item size if it existed, else 1 WCU
                let old_item: Option<Item> =
                    old_json.as_ref().and_then(|j| serde_json::from_str(j).ok());
                let delete_wcu = if let Some(ref old) = old_item {
                    types::write_capacity_units(types::item_size(old))
                } else {
                    1.0
                };
                *table_wcu.entry(table_name.clone()).or_insert(0.0) += delete_wcu;

                // Maintain GSI tables
                let gsi_units =
                    super::gsi::maintain_gsis_after_delete(storage, table_name, &meta, &pk, &sk)?;

                // Accumulate GSI units per table
                let table_entry = table_gsi_units.entry(table_name.clone()).or_default();
                for (gsi_name, units) in &gsi_units {
                    *table_entry.entry(gsi_name.clone()).or_insert(0.0) += units;
                }

                // Maintain LSI tables
                super::lsi::maintain_lsis_after_delete(storage, table_name, &meta, &pk, &sk)?;

                // Track affected partition for deferred metrics
                if let Some(pk_val) = del_req.key.get(&key_schema.partition_key) {
                    affected_partitions.push((
                        table_name.clone(),
                        pk.clone(),
                        key_schema.partition_key.clone(),
                        pk_val.clone(),
                    ));
                }

                // Record stream event (old_item already parsed above)
                if old_item.is_some() {
                    crate::streams::record_stream_event(storage, &meta, old_item.as_ref(), None)?;
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
            if let Some(cc) = crate::types::consumed_capacity_with_indexes(
                table_name,
                total_wcu,
                &gsi_units,
                &request.return_consumed_capacity,
            ) {
                caps.push(cc);
            }
        }
        Some(caps)
    } else {
        None
    };

    // Compute item collection metrics once per unique (table, pk) — deferred from the write loop
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
            let meta = helpers::require_table(storage, tbl)?;
            if let Some(icm) = helpers::build_item_collection_metrics(
                storage,
                &meta,
                tbl,
                pk_str,
                pk_attr,
                pk_val,
                &request.return_item_collection_metrics,
            )? {
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
