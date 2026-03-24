//! TTL (Time to Live) support.
//!
//! Provides background expiry of items with expired TTL attributes.

use crate::actions::{gsi, lsi};
use crate::errors::Result;
use crate::storage::Storage;
use crate::streams;
use crate::types::{AttributeValue, Item};
use std::time::{SystemTime, UNIX_EPOCH};

/// JSON representation of the TTL service identity for stream records.
const TTL_USER_IDENTITY: &str = r#"{"type":"Service","principalId":"dynamodb.amazonaws.com"}"#;

/// Sweep all TTL-enabled tables and delete expired items.
///
/// Returns the total number of items deleted across all tables.
pub fn sweep_expired_items(storage: &Storage) -> Result<usize> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let tables = storage.list_ttl_enabled_tables()?;
    let mut total_deleted = 0;

    for meta in &tables {
        let ttl_attr = match meta.ttl_attribute.as_ref() {
            Some(attr) => attr.clone(),
            None => continue,
        };

        // Scan all items in the table
        let mut exclusive_start_pk: Option<String> = None;
        let mut exclusive_start_sk: Option<String> = None;

        loop {
            let rows = storage.scan_items(
                &meta.table_name,
                &crate::storage::ScanParams {
                    limit: Some(100),
                    exclusive_start_pk: exclusive_start_pk.as_deref(),
                    exclusive_start_sk: exclusive_start_sk.as_deref(),
                    ..Default::default()
                },
            )?;

            if rows.is_empty() {
                break;
            }

            for (pk, sk, item_json) in &rows {
                let item: Item = match serde_json::from_str(item_json) {
                    Ok(i) => i,
                    Err(_) => continue,
                };

                if is_expired(&item, &ttl_attr, now) {
                    // Delete the item
                    let old_json = storage.delete_item(&meta.table_name, pk, sk)?;

                    // Maintain GSI tables
                    let _ =
                        gsi::maintain_gsis_after_delete(storage, &meta.table_name, meta, pk, sk)?;

                    // Maintain LSI tables
                    lsi::maintain_lsis_after_delete(storage, &meta.table_name, meta, pk, sk)?;

                    // Generate stream REMOVE record with TTL service identity
                    if meta.stream_enabled {
                        record_ttl_stream_event(storage, meta, &item)?;
                    }

                    let _ = old_json; // consumed by delete_item
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
fn record_ttl_stream_event(
    storage: &Storage,
    meta: &crate::storage::TableMetadata,
    old_item: &Item,
) -> Result<()> {
    let view_type = meta
        .stream_view_type
        .as_deref()
        .unwrap_or("NEW_AND_OLD_IMAGES");

    let keys = streams::extract_keys(old_item, &meta.key_schema);
    let keys_json = serde_json::to_string(&keys).unwrap_or_default();

    let old_image_json = match view_type {
        "OLD_IMAGE" | "NEW_AND_OLD_IMAGES" => {
            Some(serde_json::to_string(old_item).unwrap_or_default())
        }
        _ => None,
    };

    let seq_num = storage.next_stream_sequence_number(&meta.table_name)?;
    let sid = streams::shard_id(&meta.table_name);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    storage.insert_stream_record_with_identity(
        &meta.table_name,
        "REMOVE",
        &keys_json,
        None,
        old_image_json.as_deref(),
        &seq_num.to_string(),
        &sid,
        now,
        Some(TTL_USER_IDENTITY),
    )?;

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
