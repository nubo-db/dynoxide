use crate::errors::{DynoxideError, Result};
use crate::storage_backend::StorageBackend;
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

pub async fn execute<S: StorageBackend>(
    storage: &S,
    request: GetShardIteratorRequest,
) -> Result<GetShardIteratorResponse> {
    if let Some(value) = &request.sequence_number {
        check_sequence_number_length(value)?;
    }

    // Parse table name from ARN
    let table_name = parse_table_name_from_arn(&request.stream_arn).ok_or_else(|| {
        DynoxideError::ResourceNotFoundException(format!(
            "Requested resource not found: Stream: {}",
            request.stream_arn
        ))
    })?;

    let meta = storage
        .get_table_metadata(&table_name)
        .await?
        .ok_or_else(|| {
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

    let kind = request.shard_iterator_type.as_str();
    if matches!(kind, "TRIM_HORIZON" | "LATEST") && request.sequence_number.is_some() {
        return Err(DynoxideError::ValidationException(
            "One or more parameter values were invalid: SequenceNumber should not be passed for this iterator type"
                .to_string(),
        ));
    }

    let position = match kind {
        "TRIM_HORIZON" => 0,
        "LATEST" => {
            // Position at the shard's last record, so only later ones return.
            let (_, last) = storage
                .get_shard_sequence_range(&table_name, &request.shard_id)
                .await?;
            last.and_then(|seq| seq.parse().ok()).unwrap_or(0)
        }
        // AT includes the named record, so position just before it.
        "AT_SEQUENCE_NUMBER" => requested_sequence_number(&request)? - 1,
        "AFTER_SEQUENCE_NUMBER" => requested_sequence_number(&request)?,
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

/// DynamoDB's 21 to 40 character constraint on `SequenceNumber`.
fn check_sequence_number_length(value: &str) -> Result<()> {
    let len = value.chars().count();
    let bound = if len < 21 {
        "greater than or equal to 21"
    } else if len > 40 {
        "less than or equal to 40"
    } else {
        return Ok(());
    };
    Err(DynoxideError::ValidationException(format!(
        "1 validation error detected: Value at 'sequenceNumber' failed to satisfy constraint: Member must have length {bound}"
    )))
}

/// The stored sequence number an AT_ or AFTER_SEQUENCE_NUMBER request names.
fn requested_sequence_number(request: &GetShardIteratorRequest) -> Result<i64> {
    let value = request.sequence_number.as_deref().ok_or_else(|| {
        DynoxideError::ValidationException(
            "One or more parameter values were invalid: SequenceNumber cannot be null".to_string(),
        )
    })?;
    crate::streams::parse_sequence_number(value).ok_or_else(|| {
        DynoxideError::ValidationException("Invalid SequenceNumber for the shard".to_string())
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
