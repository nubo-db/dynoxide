use crate::errors::{DynoxideError, Result};
use crate::storage::Storage;
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Deserialize)]
pub struct GetShardIteratorRequest {
    #[serde(rename = "StreamArn")]
    pub stream_arn: String,
    #[serde(rename = "ShardId")]
    pub shard_id: String,
    #[serde(rename = "ShardIteratorType")]
    pub shard_iterator_type: String,
    #[serde(rename = "SequenceNumber", default)]
    pub sequence_number: Option<String>,
}

#[derive(Debug, Default, Serialize)]
pub struct GetShardIteratorResponse {
    #[serde(rename = "ShardIterator", skip_serializing_if = "Option::is_none")]
    pub shard_iterator: Option<String>,
}

/// Shard iterator is a base64-encoded JSON of `{table_name, shard_id, position}`.
#[derive(Debug, Default, Serialize, Deserialize)]
struct ShardIteratorData {
    table_name: String,
    shard_id: String,
    position: i64,
}

pub fn encode_shard_iterator(table_name: &str, shard_id: &str, position: i64) -> String {
    let data = ShardIteratorData {
        table_name: table_name.to_string(),
        shard_id: shard_id.to_string(),
        position,
    };
    let json = serde_json::to_string(&data).unwrap_or_default();
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    STANDARD.encode(json.as_bytes())
}

pub fn decode_shard_iterator(iterator: &str) -> Option<(String, String, i64)> {
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    let bytes = STANDARD.decode(iterator).ok()?;
    let data: ShardIteratorData = serde_json::from_slice(&bytes).ok()?;
    Some((data.table_name, data.shard_id, data.position))
}

pub fn execute(
    storage: &Storage,
    request: GetShardIteratorRequest,
) -> Result<GetShardIteratorResponse> {
    // Parse table name from ARN
    let table_name = parse_table_name_from_arn(&request.stream_arn).ok_or_else(|| {
        DynoxideError::ResourceNotFoundException(format!(
            "Requested resource not found: Stream: {}",
            request.stream_arn
        ))
    })?;

    let meta = storage.get_table_metadata(&table_name)?.ok_or_else(|| {
        DynoxideError::ResourceNotFoundException(format!(
            "Requested resource not found: Stream: {}",
            request.stream_arn
        ))
    })?;

    if !meta.stream_enabled {
        return Err(DynoxideError::ResourceNotFoundException(format!(
            "Requested resource not found: Stream: {}",
            request.stream_arn
        )));
    }

    let position = match request.shard_iterator_type.as_str() {
        "TRIM_HORIZON" => 0,
        "LATEST" => {
            // Position at the latest record
            let seq = storage.next_stream_sequence_number(&table_name)?;
            seq - 1 // Will return records after this position
        }
        "AT_SEQUENCE_NUMBER" => {
            let seq: i64 = request
                .sequence_number
                .as_deref()
                .unwrap_or("0")
                .parse()
                .unwrap_or(0);
            seq - 1 // AT means include this record, so position before it
        }
        "AFTER_SEQUENCE_NUMBER" => {
            let seq: i64 = request
                .sequence_number
                .as_deref()
                .unwrap_or("0")
                .parse()
                .unwrap_or(0);
            seq
        }
        other => {
            return Err(DynoxideError::ValidationException(format!(
                "Invalid ShardIteratorType: {other}"
            )));
        }
    };

    let iterator = encode_shard_iterator(&table_name, &request.shard_id, position);

    Ok(GetShardIteratorResponse {
        shard_iterator: Some(iterator),
    })
}

fn parse_table_name_from_arn(arn: &str) -> Option<String> {
    let parts: Vec<&str> = arn.split('/').collect();
    if parts.len() >= 2 {
        Some(parts[1].to_string())
    } else {
        None
    }
}
