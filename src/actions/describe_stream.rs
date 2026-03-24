use crate::errors::{DynoxideError, Result};
use crate::storage::Storage;
use crate::streams;
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Deserialize)]
pub struct DescribeStreamRequest {
    #[serde(rename = "StreamArn")]
    pub stream_arn: String,
    #[serde(rename = "ExclusiveStartShardId", default)]
    pub exclusive_start_shard_id: Option<String>,
    #[serde(rename = "Limit", default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Default, Serialize)]
pub struct DescribeStreamResponse {
    #[serde(rename = "StreamDescription")]
    pub stream_description: StreamDescription,
}

#[derive(Debug, Default, Serialize)]
pub struct StreamDescription {
    #[serde(rename = "StreamArn")]
    pub stream_arn: String,
    #[serde(rename = "StreamLabel")]
    pub stream_label: String,
    #[serde(rename = "StreamStatus")]
    pub stream_status: String,
    #[serde(rename = "StreamViewType")]
    pub stream_view_type: String,
    #[serde(rename = "TableName")]
    pub table_name: String,
    #[serde(rename = "Shards")]
    pub shards: Vec<Shard>,
    #[serde(rename = "CreationRequestDateTime")]
    pub creation_request_date_time: f64,
}

#[derive(Debug, Default, Serialize)]
pub struct Shard {
    #[serde(rename = "ShardId")]
    pub shard_id: String,
    #[serde(rename = "SequenceNumberRange")]
    pub sequence_number_range: SequenceNumberRange,
}

#[derive(Debug, Default, Serialize)]
pub struct SequenceNumberRange {
    #[serde(
        rename = "StartingSequenceNumber",
        skip_serializing_if = "Option::is_none"
    )]
    pub starting_sequence_number: Option<String>,
    #[serde(
        rename = "EndingSequenceNumber",
        skip_serializing_if = "Option::is_none"
    )]
    pub ending_sequence_number: Option<String>,
}

pub fn execute(
    storage: &Storage,
    request: DescribeStreamRequest,
) -> Result<DescribeStreamResponse> {
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

    let label = meta.stream_label.clone().unwrap_or_default();
    let sid = streams::shard_id(&table_name);
    let (start_seq, end_seq) = storage.get_shard_sequence_range(&table_name, &sid)?;

    Ok(DescribeStreamResponse {
        stream_description: StreamDescription {
            stream_arn: request.stream_arn,
            stream_label: label,
            stream_status: "ENABLED".to_string(),
            stream_view_type: meta
                .stream_view_type
                .unwrap_or_else(|| "NEW_AND_OLD_IMAGES".to_string()),
            table_name,
            shards: vec![Shard {
                shard_id: sid,
                sequence_number_range: SequenceNumberRange {
                    starting_sequence_number: start_seq,
                    ending_sequence_number: end_seq,
                },
            }],
            creation_request_date_time: meta.created_at as f64,
        },
    })
}

fn parse_table_name_from_arn(arn: &str) -> Option<String> {
    // Format: arn:aws:dynamodb:dynoxide:000000000000:table/{table_name}/stream/{label}
    let parts: Vec<&str> = arn.split('/').collect();
    if parts.len() >= 2 {
        Some(parts[1].to_string())
    } else {
        None
    }
}
