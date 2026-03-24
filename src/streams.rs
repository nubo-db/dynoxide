//! DynamoDB Streams support.
//!
//! Generates stream records on write operations when streams are enabled on a table.

use crate::errors::Result;
use crate::storage::{Storage, TableMetadata};
use crate::types::{AttributeValue, Item};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// Region used in ARNs for the local emulator.
pub const LOCAL_REGION: &str = "dynoxide";
/// Account ID used in ARNs for the local emulator.
pub const LOCAL_ACCOUNT: &str = "000000000000";

/// Table ARN format for local emulator.
pub fn table_arn(table_name: &str) -> String {
    format!("arn:aws:dynamodb:{LOCAL_REGION}:{LOCAL_ACCOUNT}:table/{table_name}")
}

/// Index ARN format for local emulator (GSI or LSI).
pub fn index_arn(table_name: &str, index_name: &str) -> String {
    format!("arn:aws:dynamodb:{LOCAL_REGION}:{LOCAL_ACCOUNT}:table/{table_name}/index/{index_name}")
}

/// Stream ARN format for local emulator.
pub fn stream_arn(table_name: &str, label: &str) -> String {
    format!("arn:aws:dynamodb:{LOCAL_REGION}:{LOCAL_ACCOUNT}:table/{table_name}/stream/{label}")
}

/// Shard ID for a table (one shard per table in simplified model).
pub fn shard_id(table_name: &str) -> String {
    format!("shardId-00000001-{table_name}")
}

/// Generate a stream label from the current timestamp.
pub fn generate_stream_label() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    format!("{}.{:09}", now.as_secs(), now.subsec_nanos())
}

/// Extract only key attributes from an item given the key schema JSON.
pub fn extract_keys(item: &Item, key_schema_json: &str) -> HashMap<String, AttributeValue> {
    let key_schema: Vec<crate::types::KeySchemaElement> =
        serde_json::from_str(key_schema_json).unwrap_or_default();
    let mut keys = HashMap::new();
    for ks in &key_schema {
        if let Some(val) = item.get(&ks.attribute_name) {
            keys.insert(ks.attribute_name.clone(), val.clone());
        }
    }
    keys
}

/// Record a stream event if streams are enabled on the table.
///
/// `old_item` and `new_item` are the item before and after the operation.
/// For INSERT: old_item is None, new_item is Some.
/// For MODIFY: both are Some.
/// For REMOVE: old_item is Some, new_item is None.
#[allow(clippy::too_many_arguments)]
pub fn record_stream_event(
    storage: &Storage,
    meta: &TableMetadata,
    old_item: Option<&Item>,
    new_item: Option<&Item>,
) -> Result<()> {
    if !meta.stream_enabled {
        return Ok(());
    }

    let view_type = meta
        .stream_view_type
        .as_deref()
        .unwrap_or("NEW_AND_OLD_IMAGES");

    let event_name = match (old_item, new_item) {
        (None, Some(_)) => "INSERT",
        (Some(_), Some(_)) => "MODIFY",
        (Some(_), None) => "REMOVE",
        (None, None) => return Ok(()),
    };

    // Get key attributes from whichever image is available
    let ref_item = new_item.or(old_item).unwrap();
    let keys = extract_keys(ref_item, &meta.key_schema);
    let keys_json = serde_json::to_string(&keys).unwrap_or_default();

    let new_image_json = match view_type {
        "NEW_IMAGE" | "NEW_AND_OLD_IMAGES" => {
            new_item.map(|i| serde_json::to_string(i).unwrap_or_default())
        }
        _ => None,
    };

    let old_image_json = match view_type {
        "OLD_IMAGE" | "NEW_AND_OLD_IMAGES" => {
            old_item.map(|i| serde_json::to_string(i).unwrap_or_default())
        }
        _ => None,
    };

    let seq_num = storage.next_stream_sequence_number(&meta.table_name)?;
    let sid = shard_id(&meta.table_name);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    storage.insert_stream_record(
        &meta.table_name,
        event_name,
        &keys_json,
        new_image_json.as_deref(),
        old_image_json.as_deref(),
        &seq_num.to_string(),
        &sid,
        now,
    )?;

    Ok(())
}
