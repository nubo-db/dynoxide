pub mod batch_execute_statement;
pub mod batch_get_item;
pub mod batch_write_item;
pub mod create_table;
pub mod delete_item;
pub mod delete_table;
pub mod describe_stream;
pub mod describe_table;
pub mod describe_time_to_live;
pub mod execute_statement;
pub mod execute_transaction;
pub mod get_item;
pub mod get_records;
pub mod get_shard_iterator;
pub(crate) mod gsi;
pub(crate) mod helpers;
pub mod import_items;
pub mod list_streams;
pub mod list_tables;
pub mod list_tags_of_resource;
pub(crate) mod lsi;
pub mod put_item;
pub mod query;
pub mod scan;
pub mod tag_resource;
pub mod transact_get_items;
pub mod transact_write_items;
pub mod untag_resource;
pub mod update_item;
pub mod update_table;
pub mod update_time_to_live;

use crate::types::{
    AttributeDefinition, GlobalSecondaryIndex, KeySchemaElement, LocalSecondaryIndex, Projection,
};
use serde::{Deserialize, Serialize};

/// Full table description returned by DescribeTable and CreateTable.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TableDescription {
    #[serde(rename = "TableName")]
    pub table_name: String,

    #[serde(rename = "TableId", skip_serializing_if = "Option::is_none")]
    pub table_id: Option<String>,

    #[serde(rename = "TableArn")]
    pub table_arn: String,

    #[serde(rename = "TableStatus")]
    pub table_status: String,

    #[serde(rename = "KeySchema")]
    pub key_schema: Vec<KeySchemaElement>,

    #[serde(rename = "AttributeDefinitions")]
    pub attribute_definitions: Vec<AttributeDefinition>,

    #[serde(rename = "CreationDateTime", skip_serializing_if = "Option::is_none")]
    pub creation_date_time: Option<f64>,

    #[serde(rename = "ItemCount", skip_serializing_if = "Option::is_none")]
    pub item_count: Option<i64>,

    #[serde(rename = "TableSizeBytes", skip_serializing_if = "Option::is_none")]
    pub table_size_bytes: Option<i64>,

    #[serde(
        rename = "ProvisionedThroughput",
        skip_serializing_if = "Option::is_none"
    )]
    pub provisioned_throughput: Option<TableProvisionedThroughputDescription>,

    #[serde(rename = "BillingModeSummary", skip_serializing_if = "Option::is_none")]
    pub billing_mode_summary: Option<BillingModeSummary>,

    #[serde(
        rename = "TableThroughputModeSummary",
        skip_serializing_if = "Option::is_none"
    )]
    pub table_throughput_mode_summary: Option<TableThroughputModeSummary>,

    #[serde(
        rename = "GlobalSecondaryIndexes",
        skip_serializing_if = "Option::is_none"
    )]
    pub global_secondary_indexes: Option<Vec<GlobalSecondaryIndexDescription>>,

    #[serde(
        rename = "LocalSecondaryIndexes",
        skip_serializing_if = "Option::is_none"
    )]
    pub local_secondary_indexes: Option<Vec<LocalSecondaryIndexDescription>>,

    #[serde(
        rename = "StreamSpecification",
        skip_serializing_if = "Option::is_none"
    )]
    pub stream_specification: Option<StreamSpecificationDescription>,

    #[serde(rename = "LatestStreamArn", skip_serializing_if = "Option::is_none")]
    pub latest_stream_arn: Option<String>,

    #[serde(rename = "LatestStreamLabel", skip_serializing_if = "Option::is_none")]
    pub latest_stream_label: Option<String>,

    #[serde(rename = "SSEDescription", skip_serializing_if = "Option::is_none")]
    pub sse_description: Option<SseDescription>,

    #[serde(rename = "TableClassSummary", skip_serializing_if = "Option::is_none")]
    pub table_class_summary: Option<TableClassSummary>,

    #[serde(
        rename = "DeletionProtectionEnabled",
        skip_serializing_if = "Option::is_none"
    )]
    pub deletion_protection_enabled: Option<bool>,
}

/// SSE description returned in TableDescription.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SseDescription {
    #[serde(rename = "Status")]
    pub status: String,
    #[serde(rename = "SSEType", skip_serializing_if = "Option::is_none")]
    pub sse_type: Option<String>,
    #[serde(rename = "KMSMasterKeyArn", skip_serializing_if = "Option::is_none")]
    pub kms_master_key_arn: Option<String>,
}

/// Stream specification description returned in TableDescription.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StreamSpecificationDescription {
    #[serde(rename = "StreamEnabled")]
    pub stream_enabled: bool,
    #[serde(rename = "StreamViewType", skip_serializing_if = "Option::is_none")]
    pub stream_view_type: Option<String>,
}

/// Table class summary returned in TableDescription.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TableClassSummary {
    #[serde(rename = "TableClass")]
    pub table_class: String,
}

/// Provisioned throughput description (with additional metadata fields).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TableProvisionedThroughputDescription {
    #[serde(rename = "ReadCapacityUnits")]
    pub read_capacity_units: u64,
    #[serde(rename = "WriteCapacityUnits")]
    pub write_capacity_units: u64,
    #[serde(rename = "NumberOfDecreasesToday")]
    pub number_of_decreases_today: u64,
    #[serde(
        rename = "LastIncreaseDateTime",
        skip_serializing_if = "Option::is_none"
    )]
    pub last_increase_date_time: Option<f64>,
    #[serde(
        rename = "LastDecreaseDateTime",
        skip_serializing_if = "Option::is_none"
    )]
    pub last_decrease_date_time: Option<f64>,
}

/// Billing mode summary.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BillingModeSummary {
    #[serde(rename = "BillingMode")]
    pub billing_mode: String,
    #[serde(
        rename = "LastUpdateToPayPerRequestDateTime",
        skip_serializing_if = "Option::is_none"
    )]
    pub last_update_to_pay_per_request_date_time: Option<f64>,
}

/// Table throughput mode summary.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TableThroughputModeSummary {
    #[serde(rename = "TableThroughputMode")]
    pub table_throughput_mode: String,
    #[serde(
        rename = "LastUpdateToPayPerRequestDateTime",
        skip_serializing_if = "Option::is_none"
    )]
    pub last_update_to_pay_per_request_date_time: Option<f64>,
}

/// GSI description (returned in TableDescription).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GlobalSecondaryIndexDescription {
    #[serde(rename = "IndexName")]
    pub index_name: String,
    #[serde(rename = "IndexArn")]
    pub index_arn: String,
    #[serde(rename = "KeySchema")]
    pub key_schema: Vec<KeySchemaElement>,
    #[serde(rename = "Projection")]
    pub projection: Projection,
    #[serde(rename = "IndexStatus")]
    pub index_status: String,
    #[serde(
        rename = "ProvisionedThroughput",
        skip_serializing_if = "Option::is_none"
    )]
    pub provisioned_throughput: Option<TableProvisionedThroughputDescription>,
    #[serde(rename = "ItemCount", skip_serializing_if = "Option::is_none")]
    pub item_count: Option<i64>,
    #[serde(rename = "IndexSizeBytes", skip_serializing_if = "Option::is_none")]
    pub index_size_bytes: Option<i64>,
}

/// LSI description (returned in TableDescription).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LocalSecondaryIndexDescription {
    #[serde(rename = "IndexName")]
    pub index_name: String,
    #[serde(rename = "IndexArn")]
    pub index_arn: String,
    #[serde(rename = "KeySchema")]
    pub key_schema: Vec<KeySchemaElement>,
    #[serde(rename = "Projection")]
    pub projection: Projection,
    #[serde(rename = "ItemCount", skip_serializing_if = "Option::is_none")]
    pub item_count: Option<i64>,
    #[serde(rename = "IndexSizeBytes", skip_serializing_if = "Option::is_none")]
    pub index_size_bytes: Option<i64>,
}

/// Generate a UUID v4 for TableId.
fn generate_table_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// Helper: Build a TableDescription from stored metadata.
pub(crate) fn build_table_description(
    meta: &crate::storage::TableMetadata,
    item_count: Option<i64>,
    table_size_bytes: Option<i64>,
) -> TableDescription {
    use crate::streams;

    let key_schema: Vec<KeySchemaElement> =
        serde_json::from_str(&meta.key_schema).unwrap_or_default();
    let attribute_definitions: Vec<AttributeDefinition> =
        serde_json::from_str(&meta.attribute_definitions).unwrap_or_default();

    let gsi_definitions: Option<Vec<GlobalSecondaryIndex>> = meta
        .gsi_definitions
        .as_ref()
        .and_then(|s| serde_json::from_str(s).ok());

    let table_name = &meta.table_name;

    let global_secondary_indexes = gsi_definitions.map(|gsis| {
        gsis.into_iter()
            .map(|gsi| {
                let idx_arn = streams::index_arn(table_name, &gsi.index_name);
                GlobalSecondaryIndexDescription {
                    index_name: gsi.index_name,
                    index_arn: idx_arn,
                    key_schema: gsi.key_schema,
                    projection: gsi.projection,
                    index_status: "ACTIVE".to_string(),
                    provisioned_throughput: Some(if let Some(pt) = gsi.provisioned_throughput {
                        TableProvisionedThroughputDescription {
                            read_capacity_units: pt.read_capacity_units.unwrap_or(0) as u64,
                            write_capacity_units: pt.write_capacity_units.unwrap_or(0) as u64,
                            number_of_decreases_today: 0,
                            last_increase_date_time: None,
                            last_decrease_date_time: None,
                        }
                    } else {
                        // PAY_PER_REQUEST or no PT specified
                        TableProvisionedThroughputDescription {
                            read_capacity_units: 0,
                            write_capacity_units: 0,
                            number_of_decreases_today: 0,
                            last_increase_date_time: None,
                            last_decrease_date_time: None,
                        }
                    }),
                    item_count: Some(0),
                    index_size_bytes: Some(0),
                }
            })
            .collect()
    });

    let lsi_definitions: Option<Vec<LocalSecondaryIndex>> = meta
        .lsi_definitions
        .as_ref()
        .and_then(|s| serde_json::from_str(s).ok());

    let local_secondary_indexes = lsi_definitions.map(|lsis| {
        lsis.into_iter()
            .map(|lsi| {
                let idx_arn = streams::index_arn(table_name, &lsi.index_name);
                LocalSecondaryIndexDescription {
                    index_name: lsi.index_name,
                    index_arn: idx_arn,
                    key_schema: lsi.key_schema,
                    projection: lsi.projection,
                    item_count: Some(0),
                    index_size_bytes: Some(0),
                }
            })
            .collect()
    });

    let billing_mode = meta.billing_mode.clone();

    let provisioned_throughput = if let Some(pt_json) = &meta.provisioned_throughput {
        // Try parsing extended format (with timestamps) first, fall back to basic
        serde_json::from_str::<serde_json::Value>(pt_json)
            .ok()
            .map(|v| {
                let rcu = v
                    .get("ReadCapacityUnits")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0) as u64;
                let wcu = v
                    .get("WriteCapacityUnits")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0) as u64;
                let last_inc = v.get("LastIncreaseDateTime").and_then(|v| v.as_f64());
                let last_dec = v.get("LastDecreaseDateTime").and_then(|v| v.as_f64());
                let num_dec = v
                    .get("NumberOfDecreasesToday")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);
                TableProvisionedThroughputDescription {
                    read_capacity_units: rcu,
                    write_capacity_units: wcu,
                    number_of_decreases_today: num_dec,
                    last_increase_date_time: last_inc,
                    last_decrease_date_time: last_dec,
                }
            })
    } else if billing_mode.as_deref() == Some("PAY_PER_REQUEST") {
        // PAY_PER_REQUEST tables have zero provisioned throughput
        Some(TableProvisionedThroughputDescription {
            read_capacity_units: 0,
            write_capacity_units: 0,
            number_of_decreases_today: 0,
            last_increase_date_time: None,
            last_decrease_date_time: None,
        })
    } else {
        None
    };

    let stream_specification = if meta.stream_enabled {
        Some(StreamSpecificationDescription {
            stream_enabled: true,
            stream_view_type: meta.stream_view_type.clone(),
        })
    } else {
        None
    };

    let latest_stream_arn = if meta.stream_enabled {
        meta.stream_label
            .as_ref()
            .map(|label| streams::stream_arn(table_name, label))
    } else {
        None
    };

    let latest_stream_label = if meta.stream_enabled {
        meta.stream_label.clone()
    } else {
        None
    };

    // Build SSE description from stored specification
    let sse_description = meta.sse_specification.as_ref().and_then(|json| {
        serde_json::from_str::<crate::types::SseSpecification>(json)
            .ok()
            .map(|spec| SseDescription {
                status: if spec.enabled.unwrap_or(false) {
                    "ENABLED".to_string()
                } else {
                    "DISABLED".to_string()
                },
                sse_type: spec.sse_type,
                kms_master_key_arn: spec.kms_master_key_id,
            })
    });

    let table_class_summary = meta.table_class.as_ref().map(|tc| TableClassSummary {
        table_class: tc.clone(),
    });

    let deletion_protection_enabled = Some(meta.deletion_protection_enabled);

    TableDescription {
        table_name: meta.table_name.clone(),
        table_id: Some(generate_table_id()),
        table_arn: streams::table_arn(table_name),
        table_status: meta.table_status.clone(),
        key_schema,
        attribute_definitions,
        creation_date_time: Some(meta.created_at as f64),
        item_count,
        table_size_bytes,
        provisioned_throughput,
        billing_mode_summary: match billing_mode.as_deref() {
            Some("PAY_PER_REQUEST") => Some(BillingModeSummary {
                billing_mode: "PAY_PER_REQUEST".to_string(),
                last_update_to_pay_per_request_date_time: None,
            }),
            _ => None,
        },
        table_throughput_mode_summary: match billing_mode.as_deref() {
            Some("PAY_PER_REQUEST") => Some(TableThroughputModeSummary {
                table_throughput_mode: "PAY_PER_REQUEST".to_string(),
                last_update_to_pay_per_request_date_time: None,
            }),
            _ => None,
        },
        global_secondary_indexes,
        local_secondary_indexes,
        stream_specification,
        latest_stream_arn,
        latest_stream_label,
        sse_description,
        table_class_summary,
        deletion_protection_enabled,
    }
}
