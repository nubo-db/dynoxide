use crate::errors::Result;
use crate::storage::Storage;
use crate::streams;
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Deserialize)]
pub struct ListStreamsRequest {
    #[serde(rename = "TableName", default)]
    pub table_name: Option<String>,
    #[serde(rename = "ExclusiveStartStreamArn", default)]
    pub exclusive_start_stream_arn: Option<String>,
    #[serde(rename = "Limit", default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Default, Serialize)]
pub struct ListStreamsResponse {
    #[serde(rename = "Streams")]
    pub streams: Vec<StreamSummary>,
    #[serde(
        rename = "LastEvaluatedStreamArn",
        skip_serializing_if = "Option::is_none"
    )]
    pub last_evaluated_stream_arn: Option<String>,
}

#[derive(Debug, Default, Serialize)]
pub struct StreamSummary {
    #[serde(rename = "StreamArn")]
    pub stream_arn: String,
    #[serde(rename = "TableName")]
    pub table_name: String,
    #[serde(rename = "StreamLabel")]
    pub stream_label: String,
}

pub fn execute(storage: &Storage, request: ListStreamsRequest) -> Result<ListStreamsResponse> {
    let tables = storage.list_stream_enabled_tables()?;

    let mut summaries: Vec<StreamSummary> = tables
        .into_iter()
        .filter(|meta| {
            if let Some(ref filter_table) = request.table_name {
                &meta.table_name == filter_table
            } else {
                true
            }
        })
        .map(|meta| {
            let label = meta.stream_label.unwrap_or_default();
            StreamSummary {
                stream_arn: streams::stream_arn(&meta.table_name, &label),
                table_name: meta.table_name,
                stream_label: label,
            }
        })
        .collect();

    // Apply ExclusiveStartStreamArn pagination
    if let Some(ref start_arn) = request.exclusive_start_stream_arn {
        if let Some(pos) = summaries.iter().position(|s| &s.stream_arn == start_arn) {
            summaries = summaries.split_off(pos + 1);
        }
    }

    // Apply limit
    let last_arn = if let Some(limit) = request.limit {
        if summaries.len() > limit {
            summaries.truncate(limit);
            summaries.last().map(|s| s.stream_arn.clone())
        } else {
            None
        }
    } else {
        None
    };

    Ok(ListStreamsResponse {
        streams: summaries,
        last_evaluated_stream_arn: last_arn,
    })
}
