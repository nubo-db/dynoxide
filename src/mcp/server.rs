//! MCP server implementation with tool definitions.

use rmcp::ErrorData as McpError;
use rmcp::handler::server::{router::tool::ToolRouter, wrapper::Parameters};
use rmcp::model::*;
use rmcp::schemars;
use rmcp::{ServerHandler, tool, tool_handler, tool_router};

use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;

use crate::Database;
use crate::types::AttributeValue;

use super::errors::to_tool_error;
use crate::snapshots;

// ---------------------------------------------------------------------------
// Server configuration
// ---------------------------------------------------------------------------

/// Configuration for the MCP server.
#[derive(Clone, Debug)]
pub struct McpConfig {
    /// When true, reject all write operations with a clear error.
    pub read_only: bool,
    /// Maximum number of items returned by query/scan. Caps the `limit` parameter.
    pub max_items: Option<usize>,
    /// Maximum response size in bytes for query/scan results.
    pub max_size_bytes: Option<usize>,
    /// Application-level data model parsed from a schema file (e.g., OneTable).
    pub data_model: Option<crate::schema::DataModel>,
    /// Maximum number of entities shown in the MCP instructions summary. 0 = suppress.
    pub data_model_summary_limit: usize,
}

impl Default for McpConfig {
    fn default() -> Self {
        Self {
            read_only: false,
            max_items: None,
            max_size_bytes: None,
            data_model: None,
            data_model_summary_limit: 20,
        }
    }
}

// ---------------------------------------------------------------------------
// Server struct
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct McpServer {
    tool_router: ToolRouter<Self>,
    db: Arc<Database>,
    config: McpConfig,
}

// ---------------------------------------------------------------------------
// Tool parameter types
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct TableNameParam {
    #[schemars(description = "Name of the table")]
    pub table_name: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct DescribeTableParams {
    #[schemars(description = "Name of the table to describe")]
    pub table_name: String,

    #[schemars(
        description = "If true, return the full DynamoDB-format response instead of the agent-friendly format"
    )]
    #[serde(default)]
    pub raw: bool,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct CreateTableParams {
    #[schemars(description = "Name of the table to create (3-255 characters, [a-zA-Z0-9._-])")]
    pub table_name: String,

    #[schemars(
        description = "Key schema as array of {attribute_name, key_type} objects. key_type is HASH or RANGE"
    )]
    pub key_schema: serde_json::Value,

    #[schemars(
        description = "Attribute definitions as array of {attribute_name, attribute_type} objects. attribute_type is S, N, or B"
    )]
    pub attribute_definitions: serde_json::Value,

    #[schemars(
        description = "Optional GSI definitions as array of {index_name, key_schema, projection} objects"
    )]
    pub global_secondary_indexes: Option<serde_json::Value>,

    #[schemars(
        description = "Optional LSI definitions as array of {index_name, key_schema, projection} objects. LSIs share the table's partition key."
    )]
    pub local_secondary_indexes: Option<serde_json::Value>,

    #[schemars(description = "Optional stream specification {stream_enabled, stream_view_type}")]
    pub stream_specification: Option<serde_json::Value>,

    #[schemars(
        description = "Optional SSE specification {Enabled: bool}. Accepted for compatibility; has no effect in dynoxide."
    )]
    pub sse_specification: Option<serde_json::Value>,

    #[schemars(
        description = "Optional table class: STANDARD or STANDARD_INFREQUENT_ACCESS. Accepted for compatibility."
    )]
    pub table_class: Option<String>,

    #[schemars(description = "Optional tags as array of {Key, Value} objects")]
    pub tags: Option<serde_json::Value>,

    #[schemars(description = "If true, prevents the table from being deleted. Default: false.")]
    pub deletion_protection_enabled: Option<bool>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct PutItemParams {
    #[schemars(description = "Name of the table")]
    pub table_name: String,

    #[schemars(
        description = "Item to put, in DynamoDB JSON format. Example: {\"pk\": {\"S\": \"user#1\"}, \"name\": {\"S\": \"Alice\"}}"
    )]
    pub item: serde_json::Value,

    #[schemars(description = "Condition expression that must be satisfied for the put to succeed")]
    pub condition_expression: Option<String>,

    #[schemars(
        description = "Substitution tokens for expression attribute names, e.g. {\"#n\": \"name\"}"
    )]
    pub expression_attribute_names: Option<HashMap<String, String>>,

    #[schemars(
        description = "Substitution tokens for expression attribute values, in DynamoDB JSON format"
    )]
    pub expression_attribute_values: Option<serde_json::Value>,

    #[schemars(description = "What to return: NONE or ALL_OLD")]
    pub return_values: Option<String>,

    #[schemars(description = "What to return on ConditionalCheckFailedException: ALL_OLD or NONE")]
    pub return_values_on_condition_check_failure: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct GetItemParams {
    #[schemars(description = "Name of the table")]
    pub table_name: String,

    #[schemars(
        description = "Primary key in DynamoDB JSON format. Example: {\"pk\": {\"S\": \"user#1\"}}"
    )]
    pub key: serde_json::Value,

    #[schemars(description = "Projection expression to limit returned attributes")]
    pub projection_expression: Option<String>,

    #[schemars(description = "Substitution tokens for expression attribute names")]
    pub expression_attribute_names: Option<HashMap<String, String>>,

    #[schemars(description = "Whether to use strongly consistent reads")]
    #[serde(default)]
    pub consistent_read: Option<bool>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct UpdateItemParams {
    #[schemars(description = "Name of the table")]
    pub table_name: String,

    #[schemars(
        description = "Primary key of the item to update, in DynamoDB JSON format. Example: {\"pk\": {\"S\": \"user#1\"}, \"sk\": {\"S\": \"profile\"}}"
    )]
    pub key: serde_json::Value,

    #[schemars(
        description = "Update expression, e.g. \"SET #n = :val, age = age + :inc REMOVE obsolete\""
    )]
    pub update_expression: Option<String>,

    #[schemars(
        description = "Condition expression that must be satisfied for the update to succeed"
    )]
    pub condition_expression: Option<String>,

    #[schemars(description = "Substitution tokens for expression attribute names")]
    pub expression_attribute_names: Option<HashMap<String, String>>,

    #[schemars(
        description = "Substitution tokens for expression attribute values, in DynamoDB JSON format"
    )]
    pub expression_attribute_values: Option<serde_json::Value>,

    #[schemars(
        description = "What to return: NONE, ALL_OLD, UPDATED_OLD, ALL_NEW, or UPDATED_NEW"
    )]
    pub return_values: Option<String>,

    #[schemars(description = "What to return on ConditionalCheckFailedException: ALL_OLD or NONE")]
    pub return_values_on_condition_check_failure: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct QueryParams {
    #[schemars(description = "Name of the table to query")]
    pub table_name: String,

    #[schemars(
        description = "Key condition expression, e.g. \"pk = :pk AND begins_with(sk, :prefix)\""
    )]
    pub key_condition_expression: String,

    #[schemars(description = "Filter expression applied after the query")]
    pub filter_expression: Option<String>,

    #[schemars(description = "Projection expression to limit returned attributes")]
    pub projection_expression: Option<String>,

    #[schemars(description = "Substitution tokens for expression attribute names")]
    pub expression_attribute_names: Option<HashMap<String, String>>,

    #[schemars(
        description = "Substitution tokens for expression attribute values, in DynamoDB JSON format"
    )]
    pub expression_attribute_values: Option<serde_json::Value>,

    #[schemars(
        description = "Whether to scan forward (true, default) or backward (false) by sort key"
    )]
    pub scan_index_forward: Option<bool>,

    #[schemars(description = "Maximum number of items to evaluate")]
    pub limit: Option<usize>,

    #[schemars(description = "Exclusive start key for pagination, in DynamoDB JSON format")]
    pub exclusive_start_key: Option<serde_json::Value>,

    #[schemars(description = "Name of a GSI or LSI to query")]
    pub index_name: Option<String>,

    #[schemars(
        description = "What to return: ALL_ATTRIBUTES (default), ALL_PROJECTED_ATTRIBUTES, COUNT, or SPECIFIC_ATTRIBUTES. When COUNT, the response contains only Count and ScannedCount (no items)."
    )]
    pub select: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ScanParams {
    #[schemars(description = "Name of the table to scan")]
    pub table_name: String,

    #[schemars(description = "Filter expression to apply")]
    pub filter_expression: Option<String>,

    #[schemars(description = "Projection expression to limit returned attributes")]
    pub projection_expression: Option<String>,

    #[schemars(description = "Substitution tokens for expression attribute names")]
    pub expression_attribute_names: Option<HashMap<String, String>>,

    #[schemars(
        description = "Substitution tokens for expression attribute values, in DynamoDB JSON format"
    )]
    pub expression_attribute_values: Option<serde_json::Value>,

    #[schemars(description = "Maximum number of items to evaluate")]
    pub limit: Option<usize>,

    #[schemars(description = "Exclusive start key for pagination, in DynamoDB JSON format")]
    pub exclusive_start_key: Option<serde_json::Value>,

    #[schemars(description = "Name of a GSI or LSI to scan")]
    pub index_name: Option<String>,

    #[schemars(
        description = "What to return: ALL_ATTRIBUTES (default), ALL_PROJECTED_ATTRIBUTES, COUNT, or SPECIFIC_ATTRIBUTES. When COUNT, the response contains only Count and ScannedCount (no items)."
    )]
    pub select: Option<String>,

    #[schemars(
        description = "Segment number for parallel scan (0-based). Must be used with total_segments."
    )]
    pub segment: Option<u32>,

    #[schemars(
        description = "Total number of parallel scan segments (1 to 1000000). Must be used with segment."
    )]
    pub total_segments: Option<u32>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct DeleteItemParams {
    #[schemars(description = "Name of the table")]
    pub table_name: String,

    #[schemars(
        description = "Primary key of the item to delete, in DynamoDB JSON format. Example: {\"pk\": {\"S\": \"user#1\"}, \"sk\": {\"S\": \"profile\"}}"
    )]
    pub key: serde_json::Value,

    #[schemars(
        description = "Condition expression that must be satisfied for the delete to succeed"
    )]
    pub condition_expression: Option<String>,

    #[schemars(description = "Substitution tokens for expression attribute names")]
    pub expression_attribute_names: Option<HashMap<String, String>>,

    #[schemars(
        description = "Substitution tokens for expression attribute values, in DynamoDB JSON format"
    )]
    pub expression_attribute_values: Option<serde_json::Value>,

    #[schemars(description = "What to return: NONE or ALL_OLD")]
    pub return_values: Option<String>,

    #[schemars(description = "What to return on ConditionalCheckFailedException: ALL_OLD or NONE")]
    pub return_values_on_condition_check_failure: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct BatchWriteItemParams {
    #[schemars(
        description = "Map of table name to array of write requests. Each request is either {put_request: {item: ...}} or {delete_request: {key: ...}}. Max 25 operations total."
    )]
    pub request_items: serde_json::Value,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct BatchGetItemParams {
    #[schemars(
        description = "Map of table name to {keys: [...], projection_expression?, expression_attribute_names?}. Max 100 keys total."
    )]
    pub request_items: serde_json::Value,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ExecutePartiqlParams {
    #[schemars(
        description = "PartiQL statement to execute. Supports SELECT (with COUNT(*), LIMIT, nested path projections), INSERT (with IF NOT EXISTS, parameter placeholders), UPDATE (with SET arithmetic, REMOVE, nested paths), DELETE. WHERE supports BETWEEN, IN, CONTAINS, IS MISSING, IS NOT MISSING, OR, nested paths."
    )]
    pub statement: String,

    #[schemars(description = "Positional parameters for the statement, in DynamoDB JSON format")]
    pub parameters: Option<serde_json::Value>,

    #[schemars(description = "Maximum number of items to evaluate for SELECT statements")]
    pub limit: Option<usize>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct CreateSnapshotParams {
    #[schemars(
        description = "Optional name for the snapshot. If omitted, a timestamped name is generated."
    )]
    pub name: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct RestoreSnapshotParams {
    #[schemars(
        description = "Snapshot name to restore (e.g., 'my-snapshot' or 'pre-delete-Users-20260308T143022Z'). Use list_snapshots to see available names."
    )]
    pub name: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct DeleteSnapshotParams {
    #[schemars(
        description = "Snapshot name to delete (e.g., 'my-snapshot'). Use list_snapshots to see available names."
    )]
    pub name: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct UpdateTableParams {
    #[schemars(description = "Name of the table to update")]
    pub table_name: String,

    #[schemars(
        description = "Attribute definitions for new GSI key attributes, as array of {attribute_name, attribute_type} objects"
    )]
    pub attribute_definitions: Option<serde_json::Value>,

    #[schemars(
        description = "GSI updates as array of {Create?: {index_name, key_schema, projection}, Delete?: {index_name}} objects"
    )]
    pub global_secondary_index_updates: Option<serde_json::Value>,

    #[schemars(description = "Stream specification {stream_enabled, stream_view_type}")]
    pub stream_specification: Option<serde_json::Value>,

    #[schemars(description = "Enable or disable deletion protection on the table")]
    pub deletion_protection_enabled: Option<bool>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct TransactWriteItemsParams {
    #[schemars(
        description = "Array of transact items (max 100). Each item is one of: {Put: {TableName, Item, ...}}, {Update: {TableName, Key, UpdateExpression, ...}}, {Delete: {TableName, Key, ...}}, {ConditionCheck: {TableName, Key, ConditionExpression, ...}}"
    )]
    pub transact_items: serde_json::Value,

    #[schemars(
        description = "Idempotency token (max 36 chars). Same token returns cached result."
    )]
    pub client_request_token: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct TransactGetItemsParams {
    #[schemars(
        description = "Array of get items (max 100). Each item is: {Get: {TableName, Key, ProjectionExpression?, ExpressionAttributeNames?}}"
    )]
    pub transact_items: serde_json::Value,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct BatchExecutePartiqlParams {
    #[schemars(
        description = "Array of PartiQL statements. Each is {Statement: \"...\", Parameters?: [...]}."
    )]
    pub statements: serde_json::Value,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ExecuteTransactionPartiqlParams {
    #[schemars(
        description = "Array of PartiQL statements (max 100). Each is {Statement: \"...\", Parameters?: [...]}. All succeed or all fail atomically."
    )]
    pub transact_statements: serde_json::Value,
    #[schemars(description = "Optional idempotency token for the transaction")]
    pub client_request_token: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct UpdateTimeToLiveParams {
    #[schemars(description = "Name of the table")]
    pub table_name: String,

    #[schemars(
        description = "TTL specification: {Enabled: true/false, AttributeName: \"ttl_attr\"}"
    )]
    pub time_to_live_specification: serde_json::Value,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct DescribeTimeToLiveParams {
    #[schemars(description = "Name of the table")]
    pub table_name: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ListStreamsParams {
    #[schemars(description = "Filter streams by table name")]
    pub table_name: Option<String>,

    #[schemars(description = "Maximum number of streams to return")]
    pub limit: Option<usize>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct DescribeStreamParams {
    #[schemars(description = "ARN of the stream to describe")]
    pub stream_arn: String,

    #[schemars(description = "Maximum number of shards to return")]
    pub limit: Option<usize>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct GetShardIteratorParams {
    #[schemars(description = "ARN of the stream")]
    pub stream_arn: String,

    #[schemars(description = "ID of the shard")]
    pub shard_id: String,

    #[schemars(
        description = "Iterator type: TRIM_HORIZON, LATEST, AT_SEQUENCE_NUMBER, or AFTER_SEQUENCE_NUMBER"
    )]
    pub shard_iterator_type: String,

    #[schemars(description = "Sequence number (required for AT/AFTER_SEQUENCE_NUMBER)")]
    pub sequence_number: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct GetRecordsParams {
    #[schemars(description = "Shard iterator from get_shard_iterator")]
    pub shard_iterator: String,

    #[schemars(description = "Maximum number of records to return")]
    pub limit: Option<usize>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct TagResourceParams {
    #[schemars(description = "ARN of the resource (table ARN)")]
    pub resource_arn: String,

    #[schemars(description = "Array of tags: [{Key: \"env\", Value: \"prod\"}, ...]")]
    pub tags: serde_json::Value,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct UntagResourceParams {
    #[schemars(description = "ARN of the resource (table ARN)")]
    pub resource_arn: String,

    #[schemars(description = "Array of tag keys to remove: [\"env\", \"team\"]")]
    pub tag_keys: Vec<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ListTagsOfResourceParams {
    #[schemars(description = "ARN of the resource (table ARN)")]
    pub resource_arn: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct BulkPutItemsParams {
    #[schemars(description = "Name of the table")]
    pub table_name: String,

    #[schemars(
        description = "Array of items in DynamoDB JSON format. Example: [{\"pk\": {\"S\": \"user#1\"}, \"name\": {\"S\": \"Alice\"}}, ...]. Maximum 10,000 items per call."
    )]
    pub items: Vec<serde_json::Value>,

    #[schemars(
        description = "Whether to record DynamoDB Stream events for the imported items. Default: false."
    )]
    #[serde(default)]
    pub record_streams: bool,
}

// ---------------------------------------------------------------------------
// Helper: convert serde_json::Value → HashMap<String, AttributeValue>
// ---------------------------------------------------------------------------

fn parse_dynamo_map(
    val: serde_json::Value,
    field_name: &str,
) -> Result<HashMap<String, AttributeValue>, CallToolResult> {
    serde_json::from_value(val).map_err(|e| {
        CallToolResult::error(vec![Content::text(
            serde_json::json!({
                "error_type": "ValidationException",
                "message": format!("Invalid DynamoDB JSON in '{field_name}': {e}"),
                "retryable": false,
            })
            .to_string(),
        )])
    })
}

fn parse_optional_dynamo_map(
    val: Option<serde_json::Value>,
    field_name: &str,
) -> Result<Option<HashMap<String, AttributeValue>>, CallToolResult> {
    match val {
        Some(v) => parse_dynamo_map(v, field_name).map(Some),
        None => Ok(None),
    }
}

// ---------------------------------------------------------------------------
// Helper: serialize response to tool result
// ---------------------------------------------------------------------------

/// Return a tool-level validation error (keeps the agent conversation flowing,
/// unlike McpError which may abort it).
fn tool_validation_error(error_type: &str, message: &str) -> CallToolResult {
    CallToolResult::error(vec![Content::text(
        serde_json::json!({
            "error_type": error_type,
            "message": message,
            "retryable": false,
        })
        .to_string(),
    )])
}

/// Parse a required JSON value into a typed result, returning a tool validation
/// error (not an McpError) on failure so the agent conversation continues.
fn parse_json_param<T: serde::de::DeserializeOwned>(
    val: serde_json::Value,
    param_name: &str,
) -> Result<T, CallToolResult> {
    serde_json::from_value(val).map_err(|e| {
        tool_validation_error(
            &format!("Invalid{}", capitalize_first(param_name)),
            &format!("Invalid {param_name}: {e}"),
        )
    })
}

fn capitalize_first(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        None => String::new(),
        Some(c) => c.to_uppercase().collect::<String>() + chars.as_str(),
    }
}

fn json_result(value: impl serde::Serialize) -> Result<CallToolResult, McpError> {
    let json =
        serde_json::to_value(value).map_err(|e| McpError::internal_error(e.to_string(), None))?;
    Ok(CallToolResult::success(vec![Content::text(
        json.to_string(),
    )]))
}

// ---------------------------------------------------------------------------
// Tool implementations
// ---------------------------------------------------------------------------

#[tool_router]
impl McpServer {
    pub fn new(db: Arc<Database>) -> Self {
        Self::with_config(db, McpConfig::default())
    }

    pub fn with_config(db: Arc<Database>, config: McpConfig) -> Self {
        Self {
            tool_router: Self::tool_router(),
            db,
            config,
        }
    }

    /// Reject the call if the server is in read-only mode.
    fn reject_if_read_only(&self, tool_name: &str) -> Option<CallToolResult> {
        if self.config.read_only {
            Some(CallToolResult::error(vec![Content::text(
                serde_json::json!({
                    "error_type": "AccessDeniedException",
                    "message": format!("Tool '{tool_name}' is disabled: server is in read-only mode (--read-only)"),
                    "retryable": false,
                })
                .to_string(),
            )]))
        } else {
            None
        }
    }

    /// Apply the max_items cap to the user-provided limit.
    fn apply_item_limit(&self, user_limit: Option<usize>) -> Option<usize> {
        match (user_limit, self.config.max_items) {
            (Some(u), Some(m)) => Some(u.min(m)),
            (None, Some(m)) => Some(m),
            (Some(u), None) => Some(u),
            (None, None) => None,
        }
    }

    /// Serialize a response to JSON and check if it exceeds max_size_bytes.
    /// Returns Ok with the CallToolResult, or the size-exceeded error.
    ///
    /// Uses `serde_json::to_vec` to avoid an intermediate String allocation,
    /// and checks the byte length directly.
    fn json_result_checked(&self, value: &serde_json::Value) -> Result<CallToolResult, McpError> {
        let serialized =
            serde_json::to_vec(value).map_err(|e| McpError::internal_error(e.to_string(), None))?;
        if let Some(max_bytes) = self.config.max_size_bytes {
            if serialized.len() > max_bytes {
                let error_json = serde_json::json!({
                    "error_type": "ResponseSizeLimitExceeded",
                    "message": format!(
                        "Response size ({} bytes) exceeds --max-size-bytes limit ({} bytes). \
                         Use a smaller `limit` parameter or add a projection_expression to reduce response size.",
                        serialized.len(),
                        max_bytes
                    ),
                    "retryable": false,
                });
                return Ok(CallToolResult::error(vec![Content::text(
                    error_json.to_string(),
                )]));
            }
        }
        // SAFETY: serde_json::to_vec always produces valid UTF-8
        let text = unsafe { String::from_utf8_unchecked(serialized) };
        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    #[tool(
        description = "List all DynamoDB tables in the database. Returns table names as an array."
    )]
    fn list_tables(&self) -> Result<CallToolResult, McpError> {
        let request = crate::actions::list_tables::ListTablesRequest::default();
        match self.db.list_tables(request) {
            Ok(resp) => json_result(&resp),
            Err(err) => Ok(to_tool_error(err)),
        }
    }

    #[tool(
        description = "Get detailed schema information for a table including key schema, GSIs, LSIs, item count, size, stream config, and TTL settings. Returns an agent-friendly flattened format by default; set raw=true for the full DynamoDB-format response."
    )]
    fn describe_table(
        &self,
        Parameters(params): Parameters<DescribeTableParams>,
    ) -> Result<CallToolResult, McpError> {
        let request = crate::actions::describe_table::DescribeTableRequest {
            table_name: params.table_name,
        };
        match self.db.describe_table(request) {
            Ok(resp) => {
                if params.raw {
                    json_result(&resp)
                } else {
                    let mut flattened = flatten_table_description(&resp.table);
                    enrich_with_ttl(&mut flattened, &self.db, &resp.table.table_name);
                    json_result(flattened)
                }
            }
            Err(err) => Ok(to_tool_error(err)),
        }
    }

    #[tool(
        description = "[WRITE] Create a new DynamoDB table with specified key schema and optional GSIs and LSIs. Supports SSESpecification, TableClass, Tags, and DeletionProtectionEnabled. The table is available immediately after creation."
    )]
    fn create_table(
        &self,
        Parameters(params): Parameters<CreateTableParams>,
    ) -> Result<CallToolResult, McpError> {
        if let Some(err) = self.reject_if_read_only("create_table") {
            return Ok(err);
        }
        let key_schema = match parse_json_param(params.key_schema, "key_schema") {
            Ok(v) => v,
            Err(err) => return Ok(err),
        };
        let attribute_definitions =
            match parse_json_param(params.attribute_definitions, "attribute_definitions") {
                Ok(v) => v,
                Err(err) => return Ok(err),
            };
        let global_secondary_indexes = match params
            .global_secondary_indexes
            .map(serde_json::from_value)
            .transpose()
        {
            Ok(v) => v,
            Err(e) => {
                return Ok(tool_validation_error(
                    "InvalidGSI",
                    &format!("Invalid global_secondary_indexes: {e}"),
                ));
            }
        };
        let local_secondary_indexes = match params
            .local_secondary_indexes
            .map(serde_json::from_value)
            .transpose()
        {
            Ok(v) => v,
            Err(e) => {
                return Ok(tool_validation_error(
                    "InvalidLSI",
                    &format!("Invalid local_secondary_indexes: {e}"),
                ));
            }
        };
        let stream_specification = match params
            .stream_specification
            .map(serde_json::from_value)
            .transpose()
        {
            Ok(v) => v,
            Err(e) => {
                return Ok(tool_validation_error(
                    "InvalidStreamSpec",
                    &format!("Invalid stream_specification: {e}"),
                ));
            }
        };
        let sse_specification = match params
            .sse_specification
            .map(serde_json::from_value)
            .transpose()
        {
            Ok(v) => v,
            Err(e) => {
                return Ok(tool_validation_error(
                    "InvalidSSESpec",
                    &format!("Invalid sse_specification: {e}"),
                ));
            }
        };
        let tags = match params.tags.map(serde_json::from_value).transpose() {
            Ok(v) => v,
            Err(e) => {
                return Ok(tool_validation_error(
                    "InvalidTags",
                    &format!("Invalid tags: {e}"),
                ));
            }
        };

        let request = crate::actions::create_table::CreateTableRequest {
            table_name: params.table_name,
            key_schema,
            attribute_definitions,
            global_secondary_indexes,
            local_secondary_indexes,
            stream_specification,
            sse_specification,
            table_class: params.table_class,
            tags,
            deletion_protection_enabled: params.deletion_protection_enabled,
            ..Default::default()
        };
        match self.db.create_table(request) {
            Ok(resp) => json_result(&resp),
            Err(err) => Ok(to_tool_error(err)),
        }
    }

    #[tool(
        description = "[DESTRUCTIVE] Delete a table and all its data. An auto-snapshot is created before deletion for recovery via restore_snapshot."
    )]
    fn delete_table(
        &self,
        Parameters(params): Parameters<TableNameParam>,
    ) -> Result<CallToolResult, McpError> {
        if let Some(err) = self.reject_if_read_only("delete_table") {
            return Ok(err);
        }
        // Auto-snapshot before destructive operation — failure blocks the delete
        let snap_info = match snapshots::auto_snapshot(&self.db, &params.table_name) {
            Ok(info) => info,
            Err(e) => {
                return Ok(CallToolResult::error(vec![Content::text(
                    serde_json::json!({
                        "error_type": "SnapshotFailed",
                        "message": format!(
                            "Auto-snapshot failed, delete_table aborted to prevent data loss: {e}"
                        ),
                    })
                    .to_string(),
                )]));
            }
        };

        let request = crate::actions::delete_table::DeleteTableRequest {
            table_name: params.table_name,
        };
        match self.db.delete_table(request) {
            Ok(resp) => {
                let mut result = serde_json::to_value(&resp)
                    .map_err(|e| McpError::internal_error(e.to_string(), None))?;
                result["_auto_snapshot"] = serde_json::json!({
                    "name": snap_info.name,
                    "message": "Auto-snapshot created before deletion. Use restore_snapshot to recover.",
                });
                json_result(&result)
            }
            Err(err) => Ok(to_tool_error(err)),
        }
    }

    #[tool(
        description = "[WRITE] Insert or replace an item in a table. Supports conditional writes via condition_expression. Empty strings, empty sets, and invalid numbers are rejected. Sets are deduplicated. Unused ExpressionAttributeNames/Values are rejected."
    )]
    fn put_item(
        &self,
        Parameters(params): Parameters<PutItemParams>,
    ) -> Result<CallToolResult, McpError> {
        if let Some(err) = self.reject_if_read_only("put_item") {
            return Ok(err);
        }
        let item = match parse_dynamo_map(params.item, "item") {
            Ok(v) => v,
            Err(err) => return Ok(err),
        };
        let expression_attribute_values = match parse_optional_dynamo_map(
            params.expression_attribute_values,
            "expression_attribute_values",
        ) {
            Ok(v) => v,
            Err(err) => return Ok(err),
        };

        let request = crate::actions::put_item::PutItemRequest {
            table_name: params.table_name,
            item,
            condition_expression: params.condition_expression,
            expression_attribute_names: params.expression_attribute_names,
            expression_attribute_values,
            return_values: params.return_values,
            return_values_on_condition_check_failure: params
                .return_values_on_condition_check_failure,
            ..Default::default()
        };
        match self.db.put_item(request) {
            Ok(resp) => json_result(&resp),
            Err(err) => Ok(to_tool_error(err)),
        }
    }

    #[tool(description = "Retrieve a single item by its primary key.")]
    fn get_item(
        &self,
        Parameters(params): Parameters<GetItemParams>,
    ) -> Result<CallToolResult, McpError> {
        let key = match parse_dynamo_map(params.key, "key") {
            Ok(v) => v,
            Err(err) => return Ok(err),
        };

        let request = crate::actions::get_item::GetItemRequest {
            table_name: params.table_name,
            key,
            projection_expression: params.projection_expression,
            expression_attribute_names: params.expression_attribute_names,
            consistent_read: params.consistent_read,
            ..Default::default()
        };
        match self.db.get_item(request) {
            Ok(resp) => json_result(&resp),
            Err(err) => Ok(to_tool_error(err)),
        }
    }

    #[tool(
        description = "[WRITE] Update an item's attributes using an update expression. Supports SET, REMOVE, ADD, DELETE actions and conditional updates. REMOVE/ADD/DELETE on key attributes are rejected. Arithmetic uses arbitrary-precision decimal."
    )]
    fn update_item(
        &self,
        Parameters(params): Parameters<UpdateItemParams>,
    ) -> Result<CallToolResult, McpError> {
        if let Some(err) = self.reject_if_read_only("update_item") {
            return Ok(err);
        }
        let key = match parse_dynamo_map(params.key, "key") {
            Ok(v) => v,
            Err(err) => return Ok(err),
        };
        let expression_attribute_values = match parse_optional_dynamo_map(
            params.expression_attribute_values,
            "expression_attribute_values",
        ) {
            Ok(v) => v,
            Err(err) => return Ok(err),
        };

        let request = crate::actions::update_item::UpdateItemRequest {
            table_name: params.table_name,
            key,
            update_expression: params.update_expression,
            condition_expression: params.condition_expression,
            expression_attribute_names: params.expression_attribute_names,
            expression_attribute_values,
            return_values: params.return_values,
            return_values_on_condition_check_failure: params
                .return_values_on_condition_check_failure,
            ..Default::default()
        };
        match self.db.update_item(request) {
            Ok(resp) => json_result(&resp),
            Err(err) => Ok(to_tool_error(err)),
        }
    }

    #[tool(
        description = "Query items using a key condition expression. Supports filtering, projection, pagination, and GSI/LSI queries via IndexName. Use select=COUNT to get only Count and ScannedCount without items."
    )]
    fn query(
        &self,
        Parameters(params): Parameters<QueryParams>,
    ) -> Result<CallToolResult, McpError> {
        let expression_attribute_values = match parse_optional_dynamo_map(
            params.expression_attribute_values,
            "expression_attribute_values",
        ) {
            Ok(v) => v,
            Err(err) => return Ok(err),
        };
        let exclusive_start_key =
            match parse_optional_dynamo_map(params.exclusive_start_key, "exclusive_start_key") {
                Ok(v) => v,
                Err(err) => return Ok(err),
            };

        let request = crate::actions::query::QueryRequest {
            table_name: params.table_name,
            key_condition_expression: Some(params.key_condition_expression),
            filter_expression: params.filter_expression,
            projection_expression: params.projection_expression,
            expression_attribute_names: params.expression_attribute_names,
            expression_attribute_values,
            scan_index_forward: params.scan_index_forward.unwrap_or(true),
            limit: self.apply_item_limit(params.limit),
            exclusive_start_key,
            index_name: params.index_name,
            select: params.select,
            ..Default::default()
        };
        match self.db.query(request) {
            Ok(resp) => {
                let json = serde_json::to_value(&resp)
                    .map_err(|e| McpError::internal_error(e.to_string(), None))?;
                self.json_result_checked(&json)
            }
            Err(err) => Ok(to_tool_error(err)),
        }
    }

    #[tool(
        description = "Scan an entire table or GSI/LSI, optionally applying a filter expression. Use query instead when you know the partition key. Use select=COUNT to get only Count and ScannedCount without items. Use Segment and TotalSegments for parallel scan; segment filtering uses FNV-1a hash at the SQLite level."
    )]
    fn scan(&self, Parameters(params): Parameters<ScanParams>) -> Result<CallToolResult, McpError> {
        let expression_attribute_values = match parse_optional_dynamo_map(
            params.expression_attribute_values,
            "expression_attribute_values",
        ) {
            Ok(v) => v,
            Err(err) => return Ok(err),
        };
        let exclusive_start_key =
            match parse_optional_dynamo_map(params.exclusive_start_key, "exclusive_start_key") {
                Ok(v) => v,
                Err(err) => return Ok(err),
            };

        let request = crate::actions::scan::ScanRequest {
            table_name: params.table_name,
            filter_expression: params.filter_expression,
            projection_expression: params.projection_expression,
            expression_attribute_names: params.expression_attribute_names,
            expression_attribute_values,
            limit: self.apply_item_limit(params.limit),
            exclusive_start_key,
            index_name: params.index_name,
            select: params.select,
            segment: params.segment,
            total_segments: params.total_segments,
            ..Default::default()
        };
        match self.db.scan(request) {
            Ok(resp) => {
                let json = serde_json::to_value(&resp)
                    .map_err(|e| McpError::internal_error(e.to_string(), None))?;
                self.json_result_checked(&json)
            }
            Err(err) => Ok(to_tool_error(err)),
        }
    }

    #[tool(
        description = "[WRITE] Delete a single item by its primary key. Returns the deleted item if return_values is set to ALL_OLD. Blocked if DeletionProtectionEnabled is set on the table. Condition expression values are validated for empty strings/sets/numbers."
    )]
    fn delete_item(
        &self,
        Parameters(params): Parameters<DeleteItemParams>,
    ) -> Result<CallToolResult, McpError> {
        if let Some(err) = self.reject_if_read_only("delete_item") {
            return Ok(err);
        }
        let key = match parse_dynamo_map(params.key, "key") {
            Ok(v) => v,
            Err(err) => return Ok(err),
        };
        let expression_attribute_values = match parse_optional_dynamo_map(
            params.expression_attribute_values,
            "expression_attribute_values",
        ) {
            Ok(v) => v,
            Err(err) => return Ok(err),
        };

        let request = crate::actions::delete_item::DeleteItemRequest {
            table_name: params.table_name,
            key,
            condition_expression: params.condition_expression,
            expression_attribute_names: params.expression_attribute_names,
            expression_attribute_values,
            return_values: params.return_values,
            return_values_on_condition_check_failure: params
                .return_values_on_condition_check_failure,
            ..Default::default()
        };
        match self.db.delete_item(request) {
            Ok(resp) => json_result(&resp),
            Err(err) => Ok(to_tool_error(err)),
        }
    }

    #[tool(
        description = "[WRITE] Write up to 25 put or delete operations across one or more tables in a single call. Accepts both PascalCase (PutRequest/DeleteRequest) and snake_case (put_request/delete_request) keys. Duplicate keys across operations are rejected. ReturnItemCollectionMetrics: SIZE returns partition collection size for tables with LSIs."
    )]
    fn batch_write_item(
        &self,
        Parameters(params): Parameters<BatchWriteItemParams>,
    ) -> Result<CallToolResult, McpError> {
        if let Some(err) = self.reject_if_read_only("batch_write_item") {
            return Ok(err);
        }
        // Parse the request_items Value into the typed struct.
        // We accept both PascalCase and snake_case keys for agent ergonomics.
        let request_items = match parse_batch_write_request_items(params.request_items) {
            Ok(v) => v,
            Err(e) => return Ok(tool_validation_error("InvalidRequestItems", &e)),
        };

        let request = crate::actions::batch_write_item::BatchWriteItemRequest {
            request_items,
            ..Default::default()
        };
        match self.db.batch_write_item(request) {
            Ok(resp) => json_result(&resp),
            Err(err) => Ok(to_tool_error(err)),
        }
    }

    #[tool(
        description = "Retrieve up to 100 items from one or more tables in a single call. Accepts both PascalCase (Keys) and snake_case (keys) in request_items."
    )]
    fn batch_get_item(
        &self,
        Parameters(params): Parameters<BatchGetItemParams>,
    ) -> Result<CallToolResult, McpError> {
        let request_items = match parse_batch_get_request_items(params.request_items) {
            Ok(v) => v,
            Err(e) => return Ok(tool_validation_error("InvalidRequestItems", &e)),
        };

        let request = crate::actions::batch_get_item::BatchGetItemRequest {
            request_items,
            ..Default::default()
        };
        match self.db.batch_get_item(request) {
            Ok(resp) => json_result(&resp),
            Err(err) => Ok(to_tool_error(err)),
        }
    }

    #[tool(
        description = "Execute a PartiQL statement. Supports SELECT (with COUNT(*), LIMIT, nested paths), INSERT (with IF NOT EXISTS, parameter placeholders), UPDATE (with SET arithmetic, REMOVE, nested paths), DELETE. WHERE supports BETWEEN, IN, CONTAINS, IS MISSING, IS NOT MISSING, OR, nested paths. Write statements are blocked in read-only mode."
    )]
    fn execute_partiql(
        &self,
        Parameters(params): Parameters<ExecutePartiqlParams>,
    ) -> Result<CallToolResult, McpError> {
        // In read-only mode, only allow single SELECT statements
        if self.config.read_only {
            let trimmed = params.statement.trim_start().to_uppercase();
            if !trimmed.starts_with("SELECT") {
                return Ok(self
                    .reject_if_read_only("execute_partiql")
                    .expect("read_only is true"));
            }
            // Reject statements with semicolons (potential multi-statement injection)
            if params.statement.contains(';') {
                return Ok(tool_validation_error(
                    "AccessDeniedException",
                    "Multi-statement PartiQL is not allowed in read-only mode",
                ));
            }
        }
        let parameters: Option<Vec<AttributeValue>> =
            match params.parameters {
                Some(val) => Some(serde_json::from_value(val).map_err(|e| {
                    McpError::invalid_params(format!("Invalid parameters: {e}"), None)
                })?),
                None => None,
            };

        let request = crate::actions::execute_statement::ExecuteStatementRequest {
            statement: params.statement,
            parameters,
            limit: params.limit,
            next_token: None,
            ..Default::default()
        };
        match self.db.execute_statement(request) {
            Ok(resp) => json_result(&resp),
            Err(err) => Ok(to_tool_error(err)),
        }
    }

    #[tool(
        description = "Get database-level information: storage mode, path, size, table count, and per-table summaries. Use this as your first call to orient yourself."
    )]
    fn get_database_info(&self) -> Result<CallToolResult, McpError> {
        let db_info = self
            .db
            .database_info()
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;

        let storage_mode = if db_info.path.is_some() {
            "persistent"
        } else {
            "in-memory"
        };

        let tables: Vec<serde_json::Value> = db_info
            .tables
            .into_iter()
            .map(|t| {
                let mut entry = serde_json::json!({
                    "table_name": t.stats.table_name,
                    "item_count": t.stats.item_count,
                    "size_bytes": t.stats.size_bytes,
                });

                // Add partition_key and sort_key from key schema
                if let Some(meta) = t.metadata {
                    if let Ok(serde_json::Value::Array(ks)) =
                        serde_json::from_str::<serde_json::Value>(&meta.key_schema)
                    {
                        for elem in &ks {
                            let name = elem.get("AttributeName").and_then(|v| v.as_str());
                            let key_type = elem.get("KeyType").and_then(|v| v.as_str());
                            match (name, key_type) {
                                (Some(n), Some("HASH")) => {
                                    entry["partition_key"] =
                                        serde_json::Value::String(n.to_string());
                                }
                                (Some(n), Some("RANGE")) => {
                                    entry["sort_key"] = serde_json::Value::String(n.to_string());
                                }
                                _ => {}
                            }
                        }
                    }
                }

                entry
            })
            .collect();

        let mut info = serde_json::json!({
            "storage_mode": storage_mode,
            "path": db_info.path,
            "size_bytes": db_info.size_bytes,
            "table_count": db_info.table_count,
            "tables": tables,
            "read_only": self.config.read_only,
            "max_items": self.config.max_items,
            "max_size_bytes": self.config.max_size_bytes,
        });

        if let Some(ref data_model) = self.config.data_model {
            info["data_model"] = serde_json::to_value(data_model)
                .map_err(|e| McpError::internal_error(e.to_string(), None))?;
        }

        json_result(&info)
    }

    #[tool(
        description = "[WRITE] Create a snapshot of the current database state. Returns the snapshot name for later restoration via restore_snapshot. A global limit of 20 snapshots is enforced — when full, auto-snapshots are evicted first, then the oldest manual snapshots. Note: holds the database lock for the duration of the copy — large databases may briefly block other operations."
    )]
    fn create_snapshot(
        &self,
        Parameters(params): Parameters<CreateSnapshotParams>,
    ) -> Result<CallToolResult, McpError> {
        if let Some(err) = self.reject_if_read_only("create_snapshot") {
            return Ok(err);
        }
        match snapshots::create_snapshot(&self.db, params.name.as_deref()) {
            Ok(info) => json_result(serde_json::json!({
                "name": info.name,
                "size_bytes": info.size_bytes,
                "message": "Snapshot created successfully",
            })),
            Err(err) => Ok(to_tool_error(err)),
        }
    }

    #[tool(
        description = "[DESTRUCTIVE] Restore the database from a snapshot. This replaces all current data with the snapshot contents. Use list_snapshots to see available snapshot names."
    )]
    fn restore_snapshot(
        &self,
        Parameters(params): Parameters<RestoreSnapshotParams>,
    ) -> Result<CallToolResult, McpError> {
        if let Some(err) = self.reject_if_read_only("restore_snapshot") {
            return Ok(err);
        }
        match snapshots::restore_snapshot(&self.db, &params.name) {
            Ok(()) => json_result(serde_json::json!({
                "message": "Snapshot restored successfully",
                "restored_from": params.name,
            })),
            Err(err) => Ok(to_tool_error(err)),
        }
    }

    #[tool(
        description = "List available database snapshots with name, size, and creation time. Returns newest first, up to 20 entries."
    )]
    fn list_snapshots(&self) -> Result<CallToolResult, McpError> {
        match snapshots::list_snapshots(&self.db, None) {
            Ok(snaps) => {
                let count = snaps.len();
                json_result(serde_json::json!({
                    "snapshots": snaps,
                    "count": count,
                }))
            }
            Err(err) => Ok(to_tool_error(err)),
        }
    }

    #[tool(
        description = "[DESTRUCTIVE] Delete a snapshot by name. Use list_snapshots to see available snapshots."
    )]
    fn delete_snapshot(
        &self,
        Parameters(params): Parameters<DeleteSnapshotParams>,
    ) -> Result<CallToolResult, McpError> {
        if let Some(err) = self.reject_if_read_only("delete_snapshot") {
            return Ok(err);
        }
        match snapshots::delete_snapshot(&self.db, &params.name) {
            Ok(()) => json_result(serde_json::json!({
                "message": "Snapshot deleted successfully",
                "deleted": params.name,
            })),
            Err(err) => Ok(to_tool_error(err)),
        }
    }

    // -----------------------------------------------------------------------
    // Table management
    // -----------------------------------------------------------------------

    #[tool(
        description = "[WRITE] Update a table: add/remove GSIs, change stream settings, or toggle DeletionProtectionEnabled."
    )]
    fn update_table(
        &self,
        Parameters(params): Parameters<UpdateTableParams>,
    ) -> Result<CallToolResult, McpError> {
        if let Some(err) = self.reject_if_read_only("update_table") {
            return Ok(err);
        }
        let attribute_definitions = match params
            .attribute_definitions
            .map(serde_json::from_value)
            .transpose()
        {
            Ok(v) => v,
            Err(e) => {
                return Ok(tool_validation_error(
                    "InvalidAttributeDefinitions",
                    &format!("Invalid attribute_definitions: {e}"),
                ));
            }
        };
        let global_secondary_index_updates = match params
            .global_secondary_index_updates
            .map(serde_json::from_value)
            .transpose()
        {
            Ok(v) => v,
            Err(e) => {
                return Ok(tool_validation_error(
                    "InvalidGSIUpdates",
                    &format!("Invalid global_secondary_index_updates: {e}"),
                ));
            }
        };
        let stream_specification = match params
            .stream_specification
            .map(serde_json::from_value)
            .transpose()
        {
            Ok(v) => v,
            Err(e) => {
                return Ok(tool_validation_error(
                    "InvalidStreamSpec",
                    &format!("Invalid stream_specification: {e}"),
                ));
            }
        };

        let request = crate::actions::update_table::UpdateTableRequest {
            table_name: params.table_name,
            attribute_definitions,
            global_secondary_index_updates,
            stream_specification,
            deletion_protection_enabled: params.deletion_protection_enabled,
            billing_mode: None,
            provisioned_throughput: None,
        };
        match self.db.update_table(request) {
            Ok(resp) => json_result(&resp),
            Err(err) => Ok(to_tool_error(err)),
        }
    }

    // -----------------------------------------------------------------------
    // Transactions
    // -----------------------------------------------------------------------

    #[tool(
        description = "[WRITE] Execute up to 100 write actions (Put, Update, Delete, ConditionCheck) atomically. All succeed or all fail. Response includes ItemCollectionMetrics for tables with LSIs."
    )]
    fn transact_write_items(
        &self,
        Parameters(params): Parameters<TransactWriteItemsParams>,
    ) -> Result<CallToolResult, McpError> {
        if let Some(err) = self.reject_if_read_only("transact_write_items") {
            return Ok(err);
        }
        let transact_items = match parse_json_param(params.transact_items, "transact_items") {
            Ok(v) => v,
            Err(err) => return Ok(err),
        };
        let request = crate::actions::transact_write_items::TransactWriteItemsRequest {
            transact_items,
            client_request_token: params.client_request_token,
            ..Default::default()
        };
        match self.db.transact_write_items(request) {
            Ok(resp) => json_result(&resp),
            Err(err) => Ok(to_tool_error(err)),
        }
    }

    #[tool(
        description = "Execute up to 100 get operations atomically. Returns a consistent snapshot of all requested items."
    )]
    fn transact_get_items(
        &self,
        Parameters(params): Parameters<TransactGetItemsParams>,
    ) -> Result<CallToolResult, McpError> {
        let transact_items = match parse_json_param(params.transact_items, "transact_items") {
            Ok(v) => v,
            Err(err) => return Ok(err),
        };
        let request = crate::actions::transact_get_items::TransactGetItemsRequest {
            transact_items,
            ..Default::default()
        };
        match self.db.transact_get_items(request) {
            Ok(resp) => json_result(&resp),
            Err(err) => Ok(to_tool_error(err)),
        }
    }

    // -----------------------------------------------------------------------
    // PartiQL batch
    // -----------------------------------------------------------------------

    #[tool(
        description = "[WRITE] Execute a batch of PartiQL statements. Each statement can be SELECT, INSERT, UPDATE, or DELETE. Write statements are blocked in read-only mode."
    )]
    fn batch_execute_partiql(
        &self,
        Parameters(params): Parameters<BatchExecutePartiqlParams>,
    ) -> Result<CallToolResult, McpError> {
        let statements: Vec<crate::actions::batch_execute_statement::BatchStatementRequest> =
            match parse_json_param(params.statements, "statements") {
                Ok(v) => v,
                Err(err) => return Ok(err),
            };

        // In read-only mode, check all statements are SELECT and reject semicolons
        if self.config.read_only {
            for stmt in &statements {
                let trimmed = stmt.statement.trim_start().to_uppercase();
                if !trimmed.starts_with("SELECT") {
                    return Ok(self
                        .reject_if_read_only("batch_execute_partiql")
                        .expect("read_only is true"));
                }
                if stmt.statement.contains(';') {
                    return Ok(tool_validation_error(
                        "AccessDeniedException",
                        "Multi-statement PartiQL is not allowed in read-only mode",
                    ));
                }
            }
        }

        let request =
            crate::actions::batch_execute_statement::BatchExecuteStatementRequest { statements };
        match self.db.batch_execute_statement(request) {
            Ok(resp) => json_result(&resp),
            Err(err) => Ok(to_tool_error(err)),
        }
    }

    // -----------------------------------------------------------------------
    // PartiQL transaction
    // -----------------------------------------------------------------------

    #[tool(
        description = "[WRITE] [DESTRUCTIVE] Execute PartiQL statements transactionally (all-or-nothing). Max 100 statements. If any fails, the entire transaction is rolled back. Example transact_statements: [{\"Statement\": \"INSERT INTO Users VALUE {'pk': 'u1', 'name': 'Alice'}\"}]"
    )]
    fn execute_transaction_partiql(
        &self,
        Parameters(params): Parameters<ExecuteTransactionPartiqlParams>,
    ) -> Result<CallToolResult, McpError> {
        if let Some(err) = self.reject_if_read_only("execute_transaction_partiql") {
            return Ok(err);
        }

        let transact_statements: Vec<crate::actions::execute_transaction::ParameterizedStatement> =
            match parse_json_param(params.transact_statements, "transact_statements") {
                Ok(v) => v,
                Err(err) => return Ok(err),
            };

        let request = crate::actions::execute_transaction::ExecuteTransactionRequest {
            transact_statements,
            client_request_token: params.client_request_token,
            ..Default::default()
        };
        match self.db.execute_transaction(request) {
            Ok(resp) => json_result(&resp),
            Err(err) => Ok(to_tool_error(err)),
        }
    }

    // -----------------------------------------------------------------------
    // TTL
    // -----------------------------------------------------------------------

    #[tool(
        description = "[WRITE] Enable or disable Time to Live (TTL) on a table. Specify the attribute that holds the expiration timestamp."
    )]
    fn update_time_to_live(
        &self,
        Parameters(params): Parameters<UpdateTimeToLiveParams>,
    ) -> Result<CallToolResult, McpError> {
        if let Some(err) = self.reject_if_read_only("update_time_to_live") {
            return Ok(err);
        }
        let time_to_live_specification = match parse_json_param(
            params.time_to_live_specification,
            "time_to_live_specification",
        ) {
            Ok(v) => v,
            Err(err) => return Ok(err),
        };
        let request = crate::actions::update_time_to_live::UpdateTimeToLiveRequest {
            table_name: params.table_name,
            time_to_live_specification,
        };
        match self.db.update_time_to_live(request) {
            Ok(resp) => json_result(&resp),
            Err(err) => Ok(to_tool_error(err)),
        }
    }

    #[tool(description = "Get the TTL configuration for a table.")]
    fn describe_time_to_live(
        &self,
        Parameters(params): Parameters<DescribeTimeToLiveParams>,
    ) -> Result<CallToolResult, McpError> {
        let request = crate::actions::describe_time_to_live::DescribeTimeToLiveRequest {
            table_name: params.table_name,
        };
        match self.db.describe_time_to_live(request) {
            Ok(resp) => json_result(&resp),
            Err(err) => Ok(to_tool_error(err)),
        }
    }

    #[tool(
        description = "[WRITE] Run a TTL sweep, deleting expired items from all TTL-enabled tables. Returns the number of items deleted."
    )]
    fn sweep_ttl(&self) -> Result<CallToolResult, McpError> {
        if let Some(err) = self.reject_if_read_only("sweep_ttl") {
            return Ok(err);
        }
        match self.db.sweep_ttl() {
            Ok(count) => json_result(serde_json::json!({
                "items_deleted": count,
            })),
            Err(err) => Ok(to_tool_error(err)),
        }
    }

    // -----------------------------------------------------------------------
    // Tags
    // -----------------------------------------------------------------------

    #[tool(description = "[WRITE] Add tags to a DynamoDB table.")]
    fn tag_resource(
        &self,
        Parameters(params): Parameters<TagResourceParams>,
    ) -> Result<CallToolResult, McpError> {
        if let Some(err) = self.reject_if_read_only("tag_resource") {
            return Ok(err);
        }
        let tags = match parse_json_param(params.tags, "tags") {
            Ok(v) => v,
            Err(err) => return Ok(err),
        };
        let request = crate::actions::tag_resource::TagResourceRequest {
            resource_arn: Some(params.resource_arn),
            tags,
        };
        match self.db.tag_resource(request) {
            Ok(resp) => json_result(&resp),
            Err(err) => Ok(to_tool_error(err)),
        }
    }

    #[tool(description = "[WRITE] Remove tags from a DynamoDB table.")]
    fn untag_resource(
        &self,
        Parameters(params): Parameters<UntagResourceParams>,
    ) -> Result<CallToolResult, McpError> {
        if let Some(err) = self.reject_if_read_only("untag_resource") {
            return Ok(err);
        }
        let request = crate::actions::untag_resource::UntagResourceRequest {
            resource_arn: Some(params.resource_arn),
            tag_keys: params.tag_keys,
        };
        match self.db.untag_resource(request) {
            Ok(resp) => json_result(&resp),
            Err(err) => Ok(to_tool_error(err)),
        }
    }

    #[tool(description = "List tags for a DynamoDB table.")]
    fn list_tags_of_resource(
        &self,
        Parameters(params): Parameters<ListTagsOfResourceParams>,
    ) -> Result<CallToolResult, McpError> {
        let request = crate::actions::list_tags_of_resource::ListTagsOfResourceRequest {
            resource_arn: Some(params.resource_arn),
        };
        match self.db.list_tags_of_resource(request) {
            Ok(resp) => json_result(&resp),
            Err(err) => Ok(to_tool_error(err)),
        }
    }

    // -----------------------------------------------------------------------
    // Streams
    // -----------------------------------------------------------------------

    #[tool(description = "List DynamoDB Streams, optionally filtered by table name.")]
    fn list_streams(
        &self,
        Parameters(params): Parameters<ListStreamsParams>,
    ) -> Result<CallToolResult, McpError> {
        let request = crate::actions::list_streams::ListStreamsRequest {
            table_name: params.table_name,
            limit: params.limit,
            ..Default::default()
        };
        match self.db.list_streams(request) {
            Ok(resp) => json_result(&resp),
            Err(err) => Ok(to_tool_error(err)),
        }
    }

    #[tool(description = "Get details about a DynamoDB Stream including its shards.")]
    fn describe_stream(
        &self,
        Parameters(params): Parameters<DescribeStreamParams>,
    ) -> Result<CallToolResult, McpError> {
        let request = crate::actions::describe_stream::DescribeStreamRequest {
            stream_arn: params.stream_arn,
            limit: params.limit,
            ..Default::default()
        };
        match self.db.describe_stream(request) {
            Ok(resp) => json_result(&resp),
            Err(err) => Ok(to_tool_error(err)),
        }
    }

    #[tool(
        description = "Get a shard iterator for reading stream records. Use TRIM_HORIZON for all records or LATEST for new records only."
    )]
    fn get_shard_iterator(
        &self,
        Parameters(params): Parameters<GetShardIteratorParams>,
    ) -> Result<CallToolResult, McpError> {
        let request = crate::actions::get_shard_iterator::GetShardIteratorRequest {
            stream_arn: params.stream_arn,
            shard_id: params.shard_id,
            shard_iterator_type: params.shard_iterator_type,
            sequence_number: params.sequence_number,
        };
        match self.db.get_shard_iterator(request) {
            Ok(resp) => json_result(&resp),
            Err(err) => Ok(to_tool_error(err)),
        }
    }

    #[tool(description = "Read stream records using a shard iterator.")]
    fn get_records(
        &self,
        Parameters(params): Parameters<GetRecordsParams>,
    ) -> Result<CallToolResult, McpError> {
        let request = crate::actions::get_records::GetRecordsRequest {
            shard_iterator: params.shard_iterator,
            limit: params.limit,
        };
        match self.db.get_records(request) {
            Ok(resp) => json_result(&resp),
            Err(err) => Ok(to_tool_error(err)),
        }
    }

    #[tool(
        description = "[WRITE] Bulk-insert items into a table. Faster than repeated put_item calls. Maximum 10,000 items per call. Existing items with the same key are overwritten."
    )]
    fn bulk_put_items(
        &self,
        Parameters(params): Parameters<BulkPutItemsParams>,
    ) -> Result<CallToolResult, McpError> {
        if let Some(err) = self.reject_if_read_only("bulk_put_items") {
            return Ok(err);
        }

        const MAX_ITEMS: usize = 10_000;
        if params.items.len() > MAX_ITEMS {
            return Ok(tool_validation_error(
                "ValidationException",
                &format!(
                    "Too many items: {} exceeds maximum of {MAX_ITEMS} per call",
                    params.items.len()
                ),
            ));
        }

        let mut parsed_items = Vec::with_capacity(params.items.len());
        for (i, item_val) in params.items.into_iter().enumerate() {
            match parse_dynamo_map(item_val, &format!("items[{i}]")) {
                Ok(item) => parsed_items.push(item),
                Err(err) => return Ok(err),
            }
        }

        let options = crate::ImportOptions {
            record_streams: params.record_streams,
            ..Default::default()
        };

        match self
            .db
            .import_items(&params.table_name, parsed_items, options)
        {
            Ok(result) => json_result(serde_json::json!({
                "items_imported": result.items_imported,
                "bytes_imported": result.bytes_imported,
            })),
            Err(err) => Ok(to_tool_error(err)),
        }
    }
}

// ---------------------------------------------------------------------------
// ServerHandler implementation
// ---------------------------------------------------------------------------

#[tool_handler]
impl ServerHandler for McpServer {
    fn get_info(&self) -> ServerInfo {
        let base_instructions = "\
            Dynoxide is a lightweight, embeddable DynamoDB emulator backed by SQLite. \
            All data is local — no AWS credentials required.\n\n\
            ## Getting started\n\
            1. Call `get_database_info` first to see tables, key schemas, and server config.\n\
            2. Use `describe_table` for detailed schema of a specific table.\n\n\
            ## Key concepts\n\
            - Items use DynamoDB JSON format: `{\"pk\": {\"S\": \"value\"}, \"age\": {\"N\": \"30\"}}`\n\
            - Tool names prefixed with `[WRITE]` modify data; `[DESTRUCTIVE]` may cause data loss.\n\
            - In read-only mode (`--read-only`), all write tools return an error.\n\
            - Batch tools accept both PascalCase and snake_case keys (e.g. `PutRequest` or `put_request`).\n\n\
            ## Available tools\n\
            Tables: list_tables, describe_table, create_table, delete_table, update_table\n\
            Items: put_item, get_item, update_item, delete_item\n\
            Batch: batch_write_item, batch_get_item, bulk_put_items\n\
            Query: query, scan\n\
            Transactions: transact_write_items, transact_get_items\n\
            PartiQL: execute_partiql, batch_execute_partiql, execute_transaction_partiql\n\
            TTL: update_time_to_live, describe_time_to_live, sweep_ttl\n\
            Tags: tag_resource, untag_resource, list_tags_of_resource\n\
            Streams: list_streams, describe_stream, get_shard_iterator, get_records\n\
            Snapshots: create_snapshot, restore_snapshot, list_snapshots, delete_snapshot\n\
            Info: get_database_info";

        let instructions = match self
            .config
            .data_model
            .as_ref()
            .and_then(|dm| dm.instructions_summary(self.config.data_model_summary_limit))
        {
            Some(summary) => format!("{base_instructions}\n\n{summary}"),
            None => base_instructions.to_string(),
        };

        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("dynoxide", env!("CARGO_PKG_VERSION")))
            .with_instructions(instructions)
    }
}

// ---------------------------------------------------------------------------
// Batch operation parsing helpers
// ---------------------------------------------------------------------------

/// Parse batch_write_item request_items from JSON Value.
/// Accepts both PascalCase and snake_case keys.
fn parse_batch_write_request_items(
    val: serde_json::Value,
) -> Result<HashMap<String, Vec<crate::actions::batch_write_item::WriteRequest>>, String> {
    let map = val
        .as_object()
        .ok_or_else(|| "request_items must be an object".to_string())?;

    let mut result = HashMap::new();
    for (table_name, requests) in map {
        let arr = requests
            .as_array()
            .ok_or_else(|| format!("request_items['{table_name}'] must be an array"))?;

        let mut write_requests = Vec::new();
        for req in arr {
            let put = req.get("PutRequest").or_else(|| req.get("put_request"));
            let del = req
                .get("DeleteRequest")
                .or_else(|| req.get("delete_request"));

            let put_request = match put {
                Some(p) => {
                    let item_val = p
                        .get("Item")
                        .or_else(|| p.get("item"))
                        .ok_or_else(|| "PutRequest missing Item".to_string())?;
                    let item: HashMap<String, AttributeValue> =
                        serde_json::from_value(item_val.clone())
                            .map_err(|e| format!("Invalid item in PutRequest: {e}"))?;
                    Some(crate::actions::batch_write_item::PutRequest { item })
                }
                None => None,
            };

            let delete_request = match del {
                Some(d) => {
                    let key_val = d
                        .get("Key")
                        .or_else(|| d.get("key"))
                        .ok_or_else(|| "DeleteRequest missing Key".to_string())?;
                    let key: HashMap<String, AttributeValue> =
                        serde_json::from_value(key_val.clone())
                            .map_err(|e| format!("Invalid key in DeleteRequest: {e}"))?;
                    Some(crate::actions::batch_write_item::DeleteRequest { key })
                }
                None => None,
            };

            write_requests.push(crate::actions::batch_write_item::WriteRequest {
                put_request,
                delete_request,
            });
        }
        result.insert(table_name.clone(), write_requests);
    }
    Ok(result)
}

/// Parse batch_get_item request_items from JSON Value.
/// Accepts both PascalCase and snake_case keys.
fn parse_batch_get_request_items(
    val: serde_json::Value,
) -> Result<HashMap<String, crate::actions::batch_get_item::KeysAndAttributes>, String> {
    let map = val
        .as_object()
        .ok_or_else(|| "request_items must be an object".to_string())?;

    let mut result = HashMap::new();
    for (table_name, attrs) in map {
        let keys_val = attrs
            .get("Keys")
            .or_else(|| attrs.get("keys"))
            .ok_or_else(|| format!("request_items['{table_name}'] missing Keys"))?;

        let keys: Vec<HashMap<String, AttributeValue>> =
            serde_json::from_value(keys_val.clone())
                .map_err(|e| format!("Invalid keys for '{table_name}': {e}"))?;

        let projection_expression = attrs
            .get("ProjectionExpression")
            .or_else(|| attrs.get("projection_expression"))
            .and_then(|v| v.as_str())
            .map(String::from);

        let expression_attribute_names = attrs
            .get("ExpressionAttributeNames")
            .or_else(|| attrs.get("expression_attribute_names"))
            .and_then(|v| serde_json::from_value(v.clone()).ok());

        let consistent_read = attrs
            .get("ConsistentRead")
            .or_else(|| attrs.get("consistent_read"))
            .and_then(|v| v.as_bool());

        result.insert(
            table_name.clone(),
            crate::actions::batch_get_item::KeysAndAttributes {
                keys,
                projection_expression,
                expression_attribute_names,
                consistent_read,
                attributes_to_get: None,
            },
        );
    }
    Ok(result)
}

// ---------------------------------------------------------------------------
// Agent-friendly describe_table response
// ---------------------------------------------------------------------------

fn flatten_table_description(desc: &crate::actions::TableDescription) -> serde_json::Value {
    let mut pk = serde_json::Value::Null;
    let mut sk = serde_json::Value::Null;

    for key in &desc.key_schema {
        let key_info = serde_json::json!({
            "name": key.attribute_name,
            "type": find_attribute_type(&key.attribute_name, &desc.attribute_definitions),
        });
        if key.key_type == crate::types::KeyType::HASH {
            pk = key_info;
        } else {
            sk = key_info;
        }
    }

    let gsis: Vec<serde_json::Value> = desc
        .global_secondary_indexes
        .as_ref()
        .map(|gsis| {
            gsis.iter()
                .map(|gsi| {
                    let mut gsi_pk = serde_json::Value::Null;
                    let mut gsi_sk = serde_json::Value::Null;
                    for key in &gsi.key_schema {
                        let key_info = serde_json::json!({
                            "name": key.attribute_name,
                            "type": find_attribute_type(&key.attribute_name, &desc.attribute_definitions),
                        });
                        if key.key_type == crate::types::KeyType::HASH {
                            gsi_pk = key_info;
                        } else {
                            gsi_sk = key_info;
                        }
                    }
                    let projection_type = &gsi.projection.projection_type;
                    serde_json::json!({
                        "index_name": gsi.index_name,
                        "partition_key": gsi_pk,
                        "sort_key": gsi_sk,
                        "projection_type": projection_type,
                        "item_count": gsi.item_count.unwrap_or(0),
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    let stream_enabled = desc.latest_stream_arn.is_some();

    serde_json::json!({
        "table_name": desc.table_name,
        "status": desc.table_status,
        "partition_key": pk,
        "sort_key": sk,
        "item_count": desc.item_count.unwrap_or(0),
        "size_bytes": desc.table_size_bytes.unwrap_or(0),
        "gsis": gsis,
        "stream_enabled": stream_enabled,
    })
}

/// Enrich a flattened table description with TTL info from storage metadata.
fn enrich_with_ttl(flattened: &mut serde_json::Value, db: &Database, table_name: &str) {
    if let Ok(Some(meta)) = db.get_table_metadata(table_name) {
        if meta.ttl_enabled {
            flattened["ttl"] = serde_json::json!({
                "enabled": true,
                "attribute": meta.ttl_attribute,
            });
        } else {
            flattened["ttl"] = serde_json::json!({ "enabled": false });
        }
    }
}

fn find_attribute_type(
    name: &str,
    attr_defs: &[crate::types::AttributeDefinition],
) -> &'static str {
    attr_defs
        .iter()
        .find(|d| d.attribute_name == name)
        .map(|d| match d.attribute_type {
            crate::types::ScalarAttributeType::S => "S",
            crate::types::ScalarAttributeType::N => "N",
            crate::types::ScalarAttributeType::B => "B",
        })
        .unwrap_or("S")
}
