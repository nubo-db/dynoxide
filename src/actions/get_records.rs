use crate::errors::{DynoxideError, Result};
use crate::storage::Storage;
use crate::types::Item;
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Deserialize)]
pub struct GetRecordsRequest {
    #[serde(rename = "ShardIterator")]
    pub shard_iterator: String,
    #[serde(rename = "Limit", default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Default, Serialize)]
pub struct GetRecordsResponse {
    #[serde(rename = "Records")]
    pub records: Vec<Record>,
    #[serde(rename = "NextShardIterator", skip_serializing_if = "Option::is_none")]
    pub next_shard_iterator: Option<String>,
}

#[derive(Debug, Default, Serialize)]
pub struct Record {
    #[serde(rename = "eventID")]
    pub event_id: String,
    #[serde(rename = "eventName")]
    pub event_name: String,
    #[serde(rename = "eventVersion")]
    pub event_version: String,
    #[serde(rename = "eventSource")]
    pub event_source: String,
    #[serde(rename = "awsRegion")]
    pub aws_region: String,
    #[serde(rename = "dynamodb")]
    pub dynamodb: StreamRecord,
    #[serde(rename = "userIdentity", skip_serializing_if = "Option::is_none")]
    pub user_identity: Option<UserIdentity>,
}

#[derive(Debug, Default, Serialize)]
pub struct UserIdentity {
    #[serde(rename = "type")]
    pub identity_type: String,
    #[serde(rename = "principalId")]
    pub principal_id: String,
}

#[derive(Debug, Default, Serialize)]
pub struct StreamRecord {
    #[serde(rename = "Keys")]
    pub keys: Item,
    #[serde(rename = "NewImage", skip_serializing_if = "Option::is_none")]
    pub new_image: Option<Item>,
    #[serde(rename = "OldImage", skip_serializing_if = "Option::is_none")]
    pub old_image: Option<Item>,
    #[serde(rename = "SequenceNumber")]
    pub sequence_number: String,
    #[serde(rename = "SizeBytes")]
    pub size_bytes: i64,
    #[serde(rename = "StreamViewType")]
    pub stream_view_type: String,
    #[serde(rename = "ApproximateCreationDateTime")]
    pub approximate_creation_date_time: f64,
}

pub fn execute(storage: &Storage, request: GetRecordsRequest) -> Result<GetRecordsResponse> {
    let (table_name, shard_id, position) = super::get_shard_iterator::decode_shard_iterator(
        &request.shard_iterator,
    )
    .ok_or_else(|| DynoxideError::ValidationException("Invalid shard iterator".to_string()))?;

    let meta = storage.get_table_metadata(&table_name)?.ok_or_else(|| {
        DynoxideError::ResourceNotFoundException(format!(
            "Requested resource not found: Table: {table_name}"
        ))
    })?;

    let view_type = meta
        .stream_view_type
        .unwrap_or_else(|| "NEW_AND_OLD_IMAGES".to_string());

    let limit = request.limit.unwrap_or(1000).min(1000);
    let raw_records = storage.get_stream_records(&table_name, &shard_id, position, limit)?;

    let mut records = Vec::with_capacity(raw_records.len());
    let mut last_seq: i64 = position;

    for raw in &raw_records {
        let keys: Item = serde_json::from_str(&raw.keys_json).unwrap_or_default();
        let new_image: Option<Item> = raw
            .new_image
            .as_ref()
            .and_then(|j| serde_json::from_str(j).ok());
        let old_image: Option<Item> = raw
            .old_image
            .as_ref()
            .and_then(|j| serde_json::from_str(j).ok());

        let seq_num: i64 = raw.sequence_number.parse().unwrap_or(0);
        if seq_num > last_seq {
            last_seq = seq_num;
        }

        // Approximate size
        let size = raw.keys_json.len()
            + raw.new_image.as_ref().map_or(0, |s| s.len())
            + raw.old_image.as_ref().map_or(0, |s| s.len());

        let user_identity = raw.user_identity.as_ref().and_then(|ui| {
            serde_json::from_str::<serde_json::Value>(ui)
                .ok()
                .map(|v| UserIdentity {
                    identity_type: v["type"].as_str().unwrap_or("Service").to_string(),
                    principal_id: v["principalId"]
                        .as_str()
                        .unwrap_or("dynamodb.amazonaws.com")
                        .to_string(),
                })
        });

        records.push(Record {
            event_id: raw.sequence_number.clone(),
            event_name: raw.event_name.clone(),
            event_version: "1.1".to_string(),
            event_source: "aws:dynamodb".to_string(),
            aws_region: crate::streams::LOCAL_REGION.to_string(),
            dynamodb: StreamRecord {
                keys,
                new_image,
                old_image,
                sequence_number: raw.sequence_number.clone(),
                size_bytes: size as i64,
                stream_view_type: view_type.clone(),
                approximate_creation_date_time: raw.created_at as f64,
            },
            user_identity,
        });
    }

    // Build next shard iterator
    let next_iterator =
        super::get_shard_iterator::encode_shard_iterator(&table_name, &shard_id, last_seq);

    Ok(GetRecordsResponse {
        records,
        next_shard_iterator: Some(next_iterator),
    })
}
