//! TTL (Time to Live) support.
//!
//! Provides background expiry of items with expired TTL attributes.

use crate::actions::{gsi, lsi, vector_index};
use crate::errors::Result;
use crate::storage_backend::StorageBackend;
use crate::streams;
use crate::types::{AttributeValue, Item};

/// JSON representation of the TTL service identity for stream records.
const TTL_USER_IDENTITY: &str = r#"{"type":"Service","principalId":"dynamodb.amazonaws.com"}"#;

/// Sweep all TTL-enabled tables and delete expired items.
///
/// Returns the total number of items deleted across all tables.
pub async fn sweep_expired_items<S: StorageBackend>(storage: &S) -> Result<usize> {
    let now = storage.clock().now_unix_secs();

    let tables = storage.list_ttl_enabled_tables().await?;
    let mut total_deleted = 0;

    for meta in &tables {
        let ttl_attr = match meta.ttl_attribute.as_ref() {
            Some(attr) => attr.clone(),
            None => continue,
        };

        // Parsed once per table: the index fan-out needs the table key attribute
        // names to rebuild each expired item's projected index entries.
        let key_schema = crate::actions::helpers::parse_key_schema(meta)?;
        // The index definitions likewise, for the reason BatchWriteItem hoists
        // the same four: the meta-accepting forms deserialise the JSON on every
        // call, and a sweep is the case where many items expire at once,
        // because a shared TTL is how they got there.
        let gsi_defs = gsi::parse_gsi_defs(meta)?;
        let lsi_defs = lsi::parse_lsi_defs(meta)?;
        let vector_defs = vector_index::parse_vector_defs(meta)?;
        let attr_defs = vector_index::parse_attr_defs(meta)?;

        // Scan all items in the table
        let mut exclusive_start_pk: Option<String> = None;
        let mut exclusive_start_sk: Option<String> = None;

        loop {
            let rows = storage
                .scan_items(
                    &meta.table_name,
                    &crate::storage::ScanParams {
                        limit: Some(100),
                        exclusive_start_pk: exclusive_start_pk.as_deref(),
                        exclusive_start_sk: exclusive_start_sk.as_deref(),
                        ..Default::default()
                    },
                )
                .await?;

            if rows.is_empty() {
                break;
            }

            for (pk, sk, item_json) in &rows {
                let item: Item = match serde_json::from_str(item_json) {
                    Ok(i) => i,
                    Err(_) => continue,
                };

                if is_expired(&item, &ttl_attr, now) {
                    // Each TTL deletion is atomic with its own index fan-out: a
                    // mid-fan-out failure rolls that item's delete back rather
                    // than leaving a torn index. Items are independent, so this
                    // is one transaction per deleted item.
                    crate::actions::helpers::with_write_transaction(storage, async {
                        storage.delete_item(&meta.table_name, pk, sk).await?;
                        let target = gsi::IndexWrite {
                            table_name: &meta.table_name,
                            pk,
                            sk,
                            pk_attr: &key_schema.partition_key,
                            sk_attr: key_schema.sort_key.as_deref(),
                            old_item: Some(&item),
                            capacity_mode: None,
                        };
                        // A TTL deletion has no caller to report capacity to, so
                        // it asks for none and the fan-out skips sizing the
                        // indexes rather than sizing them for a discarded map.
                        let _ =
                            gsi::maintain_gsis_after_delete_with_defs(storage, &gsi_defs, &target)
                                .await?;
                        let _ =
                            lsi::maintain_lsis_after_delete_with_defs(storage, &lsi_defs, &target)
                                .await?;
                        let _ = vector_index::maintain_vector_indexes_after_delete_with_defs(
                            storage,
                            &vector_defs,
                            &attr_defs,
                            &target,
                        )
                        .await?;
                        // Generate stream REMOVE record with TTL service identity
                        if meta.stream_enabled {
                            record_ttl_stream_event(storage, meta, &key_schema, &item).await?;
                        }
                        Ok(())
                    })
                    .await?;

                    total_deleted += 1;
                }
            }

            // Set up pagination for next batch
            let last = rows.last().unwrap();
            exclusive_start_pk = Some(last.0.clone());
            exclusive_start_sk = Some(last.1.clone());
        }
    }

    Ok(total_deleted)
}

/// Check if an item's TTL attribute indicates it has expired.
///
/// Returns false if:
/// - The TTL attribute doesn't exist on the item
/// - The TTL attribute is not a Number type
/// - The TTL value is >= current epoch seconds (not yet expired)
fn is_expired(item: &Item, ttl_attr: &str, now_epoch_secs: u64) -> bool {
    match item.get(ttl_attr) {
        Some(AttributeValue::N(n)) => {
            // Parse as i64 first to handle potential negative values, then compare
            match n.parse::<i64>() {
                Ok(ttl_val) if ttl_val >= 0 => (ttl_val as u64) < now_epoch_secs,
                _ => false,
            }
        }
        _ => false,
    }
}

/// Record a stream REMOVE event for a TTL deletion, with the DynamoDB service
/// user identity to distinguish from manual deletes.
async fn record_ttl_stream_event<S: StorageBackend>(
    storage: &S,
    meta: &crate::storage::TableMetadata,
    key_schema: &crate::actions::helpers::KeySchema,
    old_item: &Item,
) -> Result<()> {
    let view_type = meta
        .stream_view_type
        .as_deref()
        .unwrap_or("NEW_AND_OLD_IMAGES");

    let keys = streams::extract_keys_with_schema(old_item, key_schema);
    let keys_json = serde_json::to_string(&keys).unwrap_or_default();

    let old_image_json = match view_type {
        "OLD_IMAGE" | "NEW_AND_OLD_IMAGES" => {
            Some(serde_json::to_string(old_item).unwrap_or_default())
        }
        _ => None,
    };

    let seq_num = storage
        .next_stream_sequence_number(&meta.table_name)
        .await?;
    let sid = streams::shard_id(&meta.table_name);
    let now = storage.clock().now_unix_secs() as i64;

    storage
        .insert_stream_record_with_identity(
            &meta.table_name,
            "REMOVE",
            &keys_json,
            None,
            old_image_json.as_deref(),
            &seq_num.to_string(),
            &sid,
            now,
            Some(TTL_USER_IDENTITY),
        )
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_expired_with_past_timestamp() {
        let mut item = Item::new();
        item.insert("ttl".to_string(), AttributeValue::N("1000".to_string()));
        assert!(is_expired(&item, "ttl", 2000));
    }

    #[test]
    fn test_is_expired_with_future_timestamp() {
        let mut item = Item::new();
        item.insert("ttl".to_string(), AttributeValue::N("3000".to_string()));
        assert!(!is_expired(&item, "ttl", 2000));
    }

    #[test]
    fn test_is_expired_with_equal_timestamp() {
        let mut item = Item::new();
        item.insert("ttl".to_string(), AttributeValue::N("2000".to_string()));
        // Equal means NOT expired (must be strictly less than)
        assert!(!is_expired(&item, "ttl", 2000));
    }

    #[test]
    fn test_is_expired_missing_attribute() {
        let item = Item::new();
        assert!(!is_expired(&item, "ttl", 2000));
    }

    #[test]
    fn test_is_expired_non_numeric_attribute() {
        let mut item = Item::new();
        item.insert(
            "ttl".to_string(),
            AttributeValue::S("not-a-number".to_string()),
        );
        assert!(!is_expired(&item, "ttl", 2000));
    }

    #[test]
    fn test_is_expired_negative_value() {
        let mut item = Item::new();
        item.insert("ttl".to_string(), AttributeValue::N("-100".to_string()));
        assert!(!is_expired(&item, "ttl", 2000));
    }
}
