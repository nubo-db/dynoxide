//! MCP server implementation with tool definitions.

use rmcp::ErrorData as McpError;
use rmcp::handler::server::wrapper::Parameters;
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
    /// What `dynoxide import` did to fill this database, when it did. None
    /// when the data came from anywhere else.
    pub import: Option<ImportReport>,
}

impl Default for McpConfig {
    fn default() -> Self {
        Self {
            read_only: false,
            max_items: None,
            max_size_bytes: None,
            data_model: None,
            data_model_summary_limit: 20,
            import: None,
        }
    }
}

/// How much an import notice matters. Mirrors the import pipeline's own
/// grading without depending on it, so the server builds without `import`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ImportConcern {
    /// An original value was seen reaching the output.
    Exposure,
    /// Something the import could not do or could not check.
    Caution,
}

impl ImportConcern {
    fn as_str(self) -> &'static str {
        match self {
            Self::Exposure => "exposure",
            Self::Caution => "caution",
        }
    }
}

/// One message an import raised, with its concern.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImportNotice {
    pub concern: ImportConcern,
    pub message: String,
}

/// What `dynoxide import` did before this server started. The CLI prints
/// the same facts to stderr, which an MCP client does not show to the agent,
/// so this is the agent's only way to learn whether the data was anonymised
/// cleanly and what the run warned about.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ImportReport {
    /// Tables imported.
    pub tables: usize,
    /// Items imported across all tables.
    pub items: usize,
    /// Lines skipped because they did not parse.
    pub skipped: usize,
    /// Every notice the import raised, in the order it raised them.
    pub notices: Vec<ImportNotice>,
    /// Whether `--accept-exposure` let the run serve despite its exposures.
    pub exposures_accepted: bool,
}

impl ImportReport {
    /// How many notices say an original value reached the output.
    pub fn exposure_count(&self) -> usize {
        self.notices
            .iter()
            .filter(|n| n.concern == ImportConcern::Exposure)
            .count()
    }

    /// The one sentence the server instructions carry about the import.
    pub fn instructions_sentence(&self) -> String {
        let notices = self.notices.len();
        let exposures = self.exposure_count();
        if notices == 0 {
            return "This database was populated by dynoxide import, which raised no notices."
                .to_string();
        }
        let counted = format!(
            "This database was populated by dynoxide import; get_database_info reports {} {}",
            notices,
            if notices == 1 { "notice" } else { "notices" }
        );
        if exposures == 0 {
            return format!("{counted}, none of them exposures.");
        }
        let which = if exposures == 1 {
            format!("{exposures} is an exposure")
        } else {
            format!("{exposures} are exposures")
        };
        if self.exposures_accepted {
            let was = if exposures == 1 { "was" } else { "were" };
            format!("{counted}, of which {which} that {was} accepted with --accept-exposure.")
        } else {
            format!("{counted}, of which {which}.")
        }
    }

    /// The `import` object `get_database_info` returns.
    fn to_json(&self) -> serde_json::Value {
        let notices: Vec<serde_json::Value> = self
            .notices
            .iter()
            .map(|n| {
                serde_json::json!({
                    "concern": n.concern.as_str(),
                    "message": n.message,
                })
            })
            .collect();
        serde_json::json!({
            "tables": self.tables,
            "items": self.items,
            "skipped": self.skipped,
            "notice_count": self.notices.len(),
            "exposure_count": self.exposure_count(),
            "exposures_accepted": self.exposures_accepted,
            "notices": notices,
        })
    }
}

// ---------------------------------------------------------------------------
// Server struct
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct McpServer {
    db: Arc<Database>,
    config: McpConfig,
}

// ---------------------------------------------------------------------------
// Tool parameter types
// ---------------------------------------------------------------------------

/// The description every capacity-bearing tool gives `return_consumed_capacity`.
const RETURN_CONSUMED_CAPACITY: &str = "What consumed capacity to return: TOTAL for the \
    operation's total, INDEXES for the total broken down by table and index, or NONE (the \
    default) for none. Figures follow DynamoDB's charging rules and match what the DynamoDB \
    API returns for the same request.";

/// The schema's `enum` for `return_consumed_capacity`, so a client can see the
/// values without parsing the description. Null is listed because the field is
/// optional and a null means the same as leaving it out. The field itself stays
/// a string, so an unknown value still reaches the handler and gets DynamoDB's
/// own error rather than a deserialisation failure.
const RETURN_CONSUMED_CAPACITY_VALUES: [Option<&str>; 4] =
    [Some("INDEXES"), Some("TOTAL"), Some("NONE"), None];

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

    #[schemars(
        description = "Optional vector index definitions as array of {index_name, vector_attribute: {attribute_name}, dimensions, distance_function, projection} objects. distance_function is COSINE, EUCLIDEAN or DOT_PRODUCT. Optional search_schema is an array of {attribute_name, search_schema_element_type} with element type HASH or INLINE_FILTER. A table carrying one must use PAY_PER_REQUEST. Search it with search_vectors."
    )]
    pub vector_indexes: Option<serde_json::Value>,

    #[schemars(description = "Optional stream specification {stream_enabled, stream_view_type}")]
    pub stream_specification: Option<serde_json::Value>,

    #[schemars(
        description = "Optional SSE specification {Enabled: bool}. Accepted for compatibility; has no effect in dynoxide."
    )]
    pub sse_specification: Option<serde_json::Value>,

    #[schemars(
        description = "Optional billing mode: PROVISIONED or PAY_PER_REQUEST. Defaults to PROVISIONED."
    )]
    pub billing_mode: Option<String>,

    #[schemars(
        description = "Optional provisioned throughput {read_capacity_units, write_capacity_units}. Required when billing_mode is explicitly PROVISIONED; must be omitted for PAY_PER_REQUEST."
    )]
    pub provisioned_throughput: Option<serde_json::Value>,

    #[schemars(
        description = "Optional on-demand throughput ceilings {max_read_request_units, max_write_request_units}. Stored and reported back by describe_table; not enforced."
    )]
    pub on_demand_throughput: Option<serde_json::Value>,

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

    #[schemars(
        description = RETURN_CONSUMED_CAPACITY,
        extend("enum" = RETURN_CONSUMED_CAPACITY_VALUES)
    )]
    #[serde(default)]
    pub return_consumed_capacity: Option<String>,
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

    #[schemars(
        description = RETURN_CONSUMED_CAPACITY,
        extend("enum" = RETURN_CONSUMED_CAPACITY_VALUES)
    )]
    #[serde(default)]
    pub return_consumed_capacity: Option<String>,
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

    #[schemars(
        description = RETURN_CONSUMED_CAPACITY,
        extend("enum" = RETURN_CONSUMED_CAPACITY_VALUES)
    )]
    #[serde(default)]
    pub return_consumed_capacity: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SearchVectorsParams {
    #[schemars(description = "Name of the table holding the vector index")]
    pub table_name: String,

    #[schemars(description = "Name of the vector index to search")]
    pub index_name: String,

    #[schemars(
        description = "Query vector as an array of DynamoDB numbers, e.g. [{\"N\": \"0.1\"}, {\"N\": \"0.2\"}]. Its length must equal the index's dimensions."
    )]
    pub search_vector: serde_json::Value,

    #[schemars(description = "How many nearest entries to return.")]
    pub top_k: i64,

    #[schemars(
        description = "Optional equality conditions over the index's SearchSchema attributes, e.g. \"tenant = :t\". Equality only, one condition per attribute, and mandatory when the schema declares a HASH element."
    )]
    pub search_condition_expression: Option<String>,

    #[schemars(
        description = "Optional projection expression. The vector attribute is excluded unless named here and projected by the index."
    )]
    pub projection_expression: Option<String>,

    #[schemars(description = "Optional expression attribute name substitutions")]
    pub expression_attribute_names: Option<std::collections::HashMap<String, String>>,

    #[schemars(description = "Optional expression attribute values as DynamoDB-typed JSON")]
    pub expression_attribute_values: Option<serde_json::Value>,

    #[schemars(
        description = RETURN_CONSUMED_CAPACITY,
        extend("enum" = RETURN_CONSUMED_CAPACITY_VALUES)
    )]
    #[serde(default)]
    pub return_consumed_capacity: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
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

    #[schemars(
        description = RETURN_CONSUMED_CAPACITY,
        extend("enum" = RETURN_CONSUMED_CAPACITY_VALUES)
    )]
    #[serde(default)]
    pub return_consumed_capacity: Option<String>,
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

    #[schemars(
        description = RETURN_CONSUMED_CAPACITY,
        extend("enum" = RETURN_CONSUMED_CAPACITY_VALUES)
    )]
    #[serde(default)]
    pub return_consumed_capacity: Option<String>,
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

    #[schemars(
        description = RETURN_CONSUMED_CAPACITY,
        extend("enum" = RETURN_CONSUMED_CAPACITY_VALUES)
    )]
    #[serde(default)]
    pub return_consumed_capacity: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct BatchWriteItemParams {
    #[schemars(
        description = "Map of table name to array of write requests. Each request is either {put_request: {item: ...}} or {delete_request: {key: ...}}. Max 25 operations total."
    )]
    pub request_items: serde_json::Value,

    #[schemars(
        description = RETURN_CONSUMED_CAPACITY,
        extend("enum" = RETURN_CONSUMED_CAPACITY_VALUES)
    )]
    #[serde(default)]
    pub return_consumed_capacity: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct BatchGetItemParams {
    #[schemars(
        description = "Map of table name to {keys: [...], projection_expression?, expression_attribute_names?}. Max 100 keys total."
    )]
    pub request_items: serde_json::Value,

    #[schemars(
        description = RETURN_CONSUMED_CAPACITY,
        extend("enum" = RETURN_CONSUMED_CAPACITY_VALUES)
    )]
    #[serde(default)]
    pub return_consumed_capacity: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ExecutePartiqlParams {
    #[schemars(
        description = "PartiQL statement to execute. Supports SELECT (with LIMIT, nested path projections, and NextToken paging), INSERT (with IF NOT EXISTS, parameter placeholders), UPDATE (with SET arithmetic, REMOVE, nested paths), DELETE. WHERE supports BETWEEN, IN, CONTAINS, IS MISSING, IS NOT MISSING, AND, OR, NOT, parenthesised grouping up to 64 levels deep, and nested paths; AND binds tighter than OR, and a NOT over a group is applied by De Morgan. An index is named in the FROM clause as \"table\".\"index\", which serves the read from that GSI or LSI on SELECT only; a write statement rejects the qualifier."
    )]
    pub statement: String,

    #[schemars(description = "Positional parameters for the statement, in DynamoDB JSON format")]
    pub parameters: Option<serde_json::Value>,

    #[schemars(
        description = "Whether to use strongly consistent reads. Does not change which rows come back, but it doubles the rate a SELECT is charged at and it makes a SELECT qualified by a GSI a rejection, because a GSI cannot serve one."
    )]
    #[serde(default)]
    pub consistent_read: Option<bool>,

    #[schemars(
        description = "Maximum number of items to evaluate for SELECT statements (not necessarily the number returned); a filtered page can come back short or empty and still carry a NextToken"
    )]
    pub limit: Option<usize>,

    #[schemars(
        description = "Continuation token from a previous SELECT response, to fetch the next page. Pass back the NextToken the last call returned; its absence means there are no more pages. The statement must stay identical character for character across pages, and so must the parameters; a token replayed with any difference is rejected with a ValidationException. A statement with a list or map parameter cannot be paged past its first page, as on DynamoDB."
    )]
    pub next_token: Option<String>,

    #[schemars(
        description = RETURN_CONSUMED_CAPACITY,
        extend("enum" = RETURN_CONSUMED_CAPACITY_VALUES)
    )]
    #[serde(default)]
    pub return_consumed_capacity: Option<String>,

    #[schemars(
        description = "What to return when an UPDATE or DELETE fails its WHERE condition: ALL_OLD hands back the item the condition was evaluated against as the error's item; NONE, the default, does not. Any other value is rejected with a ValidationException.",
        extend("enum" = ["ALL_OLD", "NONE"])
    )]
    #[serde(default)]
    pub return_values_on_condition_check_failure: Option<String>,
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
        description = "Attribute definitions for new GSI key attributes, as array of {attribute_name, attribute_type} objects. Only attributes used by a key schema are kept; any others are dropped, and a redeclared attribute keeps its existing type, matching DynamoDB"
    )]
    pub attribute_definitions: Option<serde_json::Value>,

    #[schemars(
        description = "GSI updates as array of {Create?: {index_name, key_schema, projection}, Delete?: {index_name}} objects"
    )]
    pub global_secondary_index_updates: Option<serde_json::Value>,

    #[schemars(
        description = "Vector index updates as array of {create} or {delete} objects. A create carries the same shape as a create_table vector index; a delete carries {index_name}. One index per call. A created index is not searchable straight away: it reports index_status CREATING, then ACTIVE while still refusing searches. The table cannot be deleted while the index reports CREATING, which is the earlier of those two and lifts before searches start working. A delete of the index is refused for a short allocation window after the create, with an answer naming the phase to retry in, and accepted from then on."
    )]
    pub vector_index_updates: Option<serde_json::Value>,

    #[schemars(description = "Stream specification {stream_enabled, stream_view_type}")]
    pub stream_specification: Option<serde_json::Value>,

    #[schemars(description = "Enable or disable deletion protection on the table")]
    pub deletion_protection_enabled: Option<bool>,

    #[schemars(
        description = "Switch billing mode: PROVISIONED or PAY_PER_REQUEST. Switching to PROVISIONED requires provisioned_throughput."
    )]
    pub billing_mode: Option<String>,

    #[schemars(
        description = "New provisioned throughput {read_capacity_units, write_capacity_units}. Not allowed while the table is PAY_PER_REQUEST unless billing_mode switches to PROVISIONED."
    )]
    pub provisioned_throughput: Option<serde_json::Value>,

    #[schemars(description = "Change table class: STANDARD or STANDARD_INFREQUENT_ACCESS")]
    pub table_class: Option<String>,

    #[schemars(
        description = "New on-demand throughput ceilings {max_read_request_units, max_write_request_units}. Stored and reported back by describe_table; not enforced."
    )]
    pub on_demand_throughput: Option<serde_json::Value>,
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

    #[schemars(
        description = RETURN_CONSUMED_CAPACITY,
        extend("enum" = RETURN_CONSUMED_CAPACITY_VALUES)
    )]
    #[serde(default)]
    pub return_consumed_capacity: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct TransactGetItemsParams {
    #[schemars(
        description = "Array of get items (max 100). Each item is: {Get: {TableName, Key, ProjectionExpression?, ExpressionAttributeNames?}}"
    )]
    pub transact_items: serde_json::Value,

    #[schemars(
        description = RETURN_CONSUMED_CAPACITY,
        extend("enum" = RETURN_CONSUMED_CAPACITY_VALUES)
    )]
    #[serde(default)]
    pub return_consumed_capacity: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct BatchExecutePartiqlParams {
    #[schemars(
        description = "Array of PartiQL statements. Each is {Statement: \"...\", Parameters?: [...], ConsistentRead?: true|false, ReturnValuesOnConditionCheckFailure?: \"ALL_OLD\"|\"NONE\"}. ConsistentRead is per member and sets the rate that member's read is charged at rather than which rows come back. ReturnValuesOnConditionCheckFailure ALL_OLD returns the item a failed UPDATE or DELETE condition was evaluated against on that member's Error. A batch is all-read or all-write, and a SELECT member must name the table's primary key and may not name an index."
    )]
    pub statements: serde_json::Value,

    #[schemars(
        description = RETURN_CONSUMED_CAPACITY,
        extend("enum" = RETURN_CONSUMED_CAPACITY_VALUES)
    )]
    #[serde(default)]
    pub return_consumed_capacity: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ExecuteTransactionPartiqlParams {
    #[schemars(
        description = "Array of PartiQL statements (max 100). Each is {Statement: \"...\", Parameters?: [...], ReturnValuesOnConditionCheckFailure?: \"ALL_OLD\"|\"NONE\"}. All succeed or all fail atomically. ALL_OLD returns the item a failed UPDATE or DELETE condition was evaluated against on that statement's cancellation reason."
    )]
    pub transact_statements: serde_json::Value,
    #[schemars(description = "Optional idempotency token for the transaction")]
    pub client_request_token: Option<String>,
    #[schemars(
        description = RETURN_CONSUMED_CAPACITY,
        extend("enum" = RETURN_CONSUMED_CAPACITY_VALUES)
    )]
    #[serde(default)]
    pub return_consumed_capacity: Option<String>,
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

    #[schemars(
        description = "Sequence number, 21 to 40 digits (required for AT/AFTER_SEQUENCE_NUMBER, refused for TRIM_HORIZON and LATEST)"
    )]
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

/// Decode a DynamoDB JSON map, classifying failures as `DynoxideError`s.
///
/// Marker-tagged serde failures classify through the shared decoder in
/// `serde_errors`, so the internal markers never reach a tool error; any
/// other failure keeps the field-specific message.
fn decode_dynamo_map(
    val: serde_json::Value,
    field_name: &str,
) -> Result<HashMap<String, AttributeValue>, crate::errors::DynoxideError> {
    serde_json::from_value(val).map_err(|e| {
        let msg = e.to_string();
        crate::serde_errors::classify_marked_serde_error(&msg).unwrap_or_else(|| {
            crate::errors::DynoxideError::ValidationException(format!(
                "Invalid DynamoDB JSON in '{field_name}': {msg}"
            ))
        })
    })
}

/// Parse a DynamoDB JSON map, resolving the wire-invisible request-validation
/// tag with the given function: the put_item and update_item tools pass
/// `envelope_request_validation`, matching their envelope on the HTTP
/// surface; every other tool passes `strip_request_validation_tag`, reporting
/// the family bare.
fn parse_dynamo_map(
    val: serde_json::Value,
    field_name: &str,
    resolve: fn(crate::DynoxideError) -> crate::DynoxideError,
) -> Result<HashMap<String, AttributeValue>, CallToolResult> {
    decode_dynamo_map(val, field_name).map_err(|e| to_tool_error(resolve(e)))
}

/// Optional-map variant of [`parse_dynamo_map`].
fn parse_optional_dynamo_map(
    val: Option<serde_json::Value>,
    field_name: &str,
    resolve: fn(crate::DynoxideError) -> crate::DynoxideError,
) -> Result<Option<HashMap<String, AttributeValue>>, CallToolResult> {
    match val {
        Some(v) => parse_dynamo_map(v, field_name, resolve).map(Some),
        None => Ok(None),
    }
}

// ---------------------------------------------------------------------------
// Helper: serialize response to tool result
// ---------------------------------------------------------------------------

/// Rewrite a `vector_index_updates` value into the wire spelling.
///
/// This surface documents snake_case, as the rest of its tools do, while the
/// shared parser and its guards are case sensitive like DynamoDB. Translating
/// here rather than loosening the parser keeps a wire client held to the wire
/// names and still lets an agent write the shape the tool describes. An entry
/// naming neither action is passed through untouched, so the parser reports it
/// rather than this dropping it quietly.
fn canonicalise_vector_index_updates(val: &serde_json::Value) -> serde_json::Value {
    let Some(arr) = val.as_array() else {
        return val.clone();
    };
    serde_json::Value::Array(
        arr.iter()
            .map(|entry| {
                let Some(obj) = entry.as_object() else {
                    return entry.clone();
                };
                let mut out = serde_json::Map::new();
                // Whatever the action holds is renamed and handed on, null
                // included: the parser and the collector each have their own
                // rule for a null or ill-typed action, and both of those match
                // the wire already. Deciding here instead would answer a type
                // error where the wire answers that neither action was given.
                if let Some(c) = obj.get("Create").or_else(|| obj.get("create")) {
                    out.insert("Create".to_string(), canonicalise_member_names(c));
                }
                if let Some(d) = obj.get("Delete").or_else(|| obj.get("delete")) {
                    out.insert("Delete".to_string(), canonicalise_member_names(d));
                }
                if out.is_empty() {
                    return entry.clone();
                }
                serde_json::Value::Object(out)
            })
            .collect(),
    )
}

/// The wire spelling of a vector index member name, for the members a create
/// or delete action carries. Anything else is passed through as it arrived.
///
/// The names are unique across the shapes a definition nests, so one flat map
/// applied at every depth cannot collide: `attribute_name` means the same
/// thing under `VectorAttribute` as under a `SearchSchema` element.
fn wire_member_name(key: &str) -> &str {
    match key {
        "index_name" => "IndexName",
        "vector_attribute" => "VectorAttribute",
        "search_schema" => "SearchSchema",
        "projection" => "Projection",
        "dimensions" => "Dimensions",
        "distance_function" => "DistanceFunction",
        "attribute_name" => "AttributeName",
        "search_schema_element_type" => "SearchSchemaElementType",
        "projection_type" => "ProjectionType",
        "non_key_attributes" => "NonKeyAttributes",
        other => other,
    }
}

/// Rewrite every member name in an action into its wire spelling, at any depth.
///
/// A rename and nothing else, so it cannot fail. That matters more than it
/// looks: the request-model collector reads wire spellings alone, so anything
/// reaching it in another casing is reported as a null member the caller did in
/// fact supply. Translating through the typed definition instead left exactly
/// the values that fail a `u32` parse, a fractional or over-range `dimensions`
/// among them, to fall through untranslated and be described that way.
fn canonicalise_member_names(val: &serde_json::Value) -> serde_json::Value {
    match val {
        serde_json::Value::Object(obj) => serde_json::Value::Object(
            obj.iter()
                .map(|(k, v)| {
                    (
                        wire_member_name(k).to_string(),
                        canonicalise_member_names(v),
                    )
                })
                .collect(),
        ),
        serde_json::Value::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(canonicalise_member_names).collect())
        }
        other => other.clone(),
    }
}

/// Return a tool-level validation error (keeps the agent conversation flowing,
/// unlike McpError which may abort it).
fn tool_validation_error(error_type: &str, message: &str) -> CallToolResult {
    CallToolResult::error(vec![ContentBlock::text(
        serde_json::json!({
            "error_type": error_type,
            "message": message,
            "retryable": false,
        })
        .to_string(),
    )])
}

/// Refuse a `return_consumed_capacity` outside DynamoDB's enum with the error a
/// wire client gets, before the tool reads or writes anything. Most tools build
/// their action request directly, which skips the check the wire path makes
/// while deserialising, so every capacity-bearing tool asks here instead.
fn refuse_bad_capacity_mode(mode: Option<&str>) -> Option<CallToolResult> {
    crate::validation::reject_bad_capacity_mode(mode)
        .err()
        .map(to_tool_error)
}

/// Parse a required JSON value into a typed result, returning a tool validation
/// error (not an McpError) on failure so the agent conversation continues.
///
/// Marker-tagged serde failures classify through the shared decoder in
/// `serde_errors` and report the request-validation family bare, exactly as
/// [`decode_dynamo_map`] does; genuinely malformed input keeps the
/// field-specific message, with the echoed serde text cleaned so neither
/// marker nor position suffix can leak.
fn parse_json_param<T: serde::de::DeserializeOwned>(
    val: serde_json::Value,
    param_name: &str,
) -> Result<T, CallToolResult> {
    serde_json::from_value(val).map_err(|e| {
        let msg = e.to_string();
        match crate::serde_errors::classify_marked_serde_error(&msg) {
            Some(err) => to_tool_error(crate::validation::strip_request_validation_tag(err)),
            None => tool_validation_error(
                &format!("Invalid{}", capitalize_first(param_name)),
                &format!(
                    "Invalid {param_name}: {}",
                    crate::serde_errors::clean_serde_message(&msg)
                ),
            ),
        }
    })
}

/// Parse an optional JSON tool parameter into a typed value, mapping a
/// deserialisation failure to a tool validation error with the caller's
/// error code. The optional counterpart to `parse_json_param`, which owns
/// the required-parameter path and derives its error code mechanically.
fn parse_optional_json_param<T: serde::de::DeserializeOwned>(
    value: Option<serde_json::Value>,
    error_type: &str,
    param_name: &str,
) -> Result<Option<T>, CallToolResult> {
    value
        .map(|v| {
            serde_json::from_value(v).map_err(|e| {
                tool_validation_error(error_type, &format!("Invalid {param_name}: {e}"))
            })
        })
        .transpose()
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
    Ok(CallToolResult::success(vec![ContentBlock::text(
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
        Self { db, config }
    }

    /// Reject the call if the server is in read-only mode.
    fn reject_if_read_only(&self, tool_name: &str) -> Option<CallToolResult> {
        if self.config.read_only {
            Some(CallToolResult::error(vec![ContentBlock::text(
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
        if let Some(max_bytes) = self.config.max_size_bytes
            && serialized.len() > max_bytes
        {
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
            return Ok(CallToolResult::error(vec![ContentBlock::text(
                error_json.to_string(),
            )]));
        }
        // SAFETY: serde_json::to_vec always produces valid UTF-8
        let text = unsafe { String::from_utf8_unchecked(serialized) };
        Ok(CallToolResult::success(vec![ContentBlock::text(text)]))
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
        description = "[WRITE] Create a new DynamoDB table with specified key schema and optional GSIs and LSIs. Supports BillingMode, ProvisionedThroughput, OnDemandThroughput, SSESpecification, TableClass, Tags, and DeletionProtectionEnabled. The table is available immediately after creation."
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
        // Request-model constraints run against the raw values, before any
        // normalisation, because that is the order the wire path uses and the
        // collector echoes back what it was given. Normalising first told an
        // agent its `-1` was a `0`, which is not the string a wire client gets
        // for the same mistake. Only the member names are rewritten, since the
        // collector reads the wire spelling and these params are snake_case.
        let raw_wire_vixs = params
            .vector_indexes
            .as_ref()
            .map(canonicalise_member_names);
        if let Some(msg) =
            crate::actions::create_table::vector_indexes_request_model_error(&raw_wire_vixs)
        {
            return Ok(tool_validation_error("ValidationException", &msg));
        }
        // Then normalised, exactly as the wire path does, so a fractional
        // dimension count truncates here rather than being refused.
        let vector_indexes: Option<Vec<crate::types::VectorIndex>> = match params
            .vector_indexes
            .as_ref()
            .map(crate::actions::create_table::normalise_vix_dimensions)
            .map(serde_json::from_value)
            .transpose()
        {
            Ok(v) => v,
            Err(e) => {
                return Ok(tool_validation_error(
                    "InvalidVectorIndexes",
                    &format!("Invalid vector_indexes: {e}"),
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
        let provisioned_throughput = match params
            .provisioned_throughput
            .map(serde_json::from_value)
            .transpose()
        {
            Ok(v) => v,
            Err(e) => {
                return Ok(tool_validation_error(
                    "InvalidProvisionedThroughput",
                    &format!("Invalid provisioned_throughput: {e}"),
                ));
            }
        };
        let on_demand_throughput = match parse_optional_json_param(
            params.on_demand_throughput,
            "InvalidOnDemandThroughput",
            "on_demand_throughput",
        ) {
            Ok(v) => v,
            Err(e) => return Ok(e),
        };

        let request = crate::actions::create_table::CreateTableRequest {
            table_name: params.table_name,
            key_schema,
            attribute_definitions,
            global_secondary_indexes,
            local_secondary_indexes,
            vector_indexes,
            stream_specification,
            sse_specification,
            billing_mode: params.billing_mode,
            provisioned_throughput,
            on_demand_throughput,
            table_class: params.table_class,
            tags,
            deletion_protection_enabled: params.deletion_protection_enabled,
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
        // Auto-snapshot before destructive operation: failure blocks the delete
        let snap_info = match snapshots::auto_snapshot(&self.db, &params.table_name) {
            Ok(info) => info,
            Err(e) => {
                return Ok(CallToolResult::error(vec![ContentBlock::text(
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
        if let Some(err) = refuse_bad_capacity_mode(params.return_consumed_capacity.as_deref()) {
            return Ok(err);
        }
        let item = match parse_dynamo_map(
            params.item,
            "item",
            crate::validation::envelope_request_validation,
        ) {
            Ok(v) => v,
            Err(err) => return Ok(err),
        };
        let expression_attribute_values = match parse_optional_dynamo_map(
            params.expression_attribute_values,
            "expression_attribute_values",
            crate::validation::envelope_request_validation,
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
            return_consumed_capacity: params.return_consumed_capacity,
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
        if let Some(err) = refuse_bad_capacity_mode(params.return_consumed_capacity.as_deref()) {
            return Ok(err);
        }
        let key = match parse_dynamo_map(
            params.key,
            "key",
            crate::validation::strip_request_validation_tag,
        ) {
            Ok(v) => v,
            Err(err) => return Ok(err),
        };

        let request = crate::actions::get_item::GetItemRequest {
            table_name: params.table_name,
            key,
            projection_expression: params.projection_expression,
            expression_attribute_names: params.expression_attribute_names,
            consistent_read: params.consistent_read,
            return_consumed_capacity: params.return_consumed_capacity,
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
        if let Some(err) = refuse_bad_capacity_mode(params.return_consumed_capacity.as_deref()) {
            return Ok(err);
        }
        let key = match parse_dynamo_map(
            params.key,
            "key",
            crate::validation::envelope_request_validation,
        ) {
            Ok(v) => v,
            Err(err) => return Ok(err),
        };
        let expression_attribute_values = match parse_optional_dynamo_map(
            params.expression_attribute_values,
            "expression_attribute_values",
            crate::validation::envelope_request_validation,
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
            return_consumed_capacity: params.return_consumed_capacity,
            ..Default::default()
        };
        match self.db.update_item(request) {
            Ok(resp) => json_result(&resp),
            Err(err) => Ok(to_tool_error(err)),
        }
    }

    #[tool(
        description = "[READ] Search a vector index for the nearest entries to a query vector. Exact brute-force KNN over the whole index, so the top-k is the true top-k. COSINE and EUCLIDEAN score as distances, where a self match is 0; DOT_PRODUCT scores as a similarity and can be negative. The vector attribute is excluded from results unless a projection_expression names it and the index projects it. Create the index with create_table's vector_indexes, which is searchable at once. An index added later through update_table refuses with 'Cannot search backfilling vector index' until it settles, for longer than describe_table reports it CREATING, so retry the search rather than waiting for the status."
    )]
    fn search_vectors(
        &self,
        Parameters(params): Parameters<SearchVectorsParams>,
    ) -> Result<CallToolResult, McpError> {
        if let Some(err) = refuse_bad_capacity_mode(params.return_consumed_capacity.as_deref()) {
            return Ok(err);
        }
        let expression_attribute_values = match parse_optional_dynamo_map(
            params.expression_attribute_values,
            "expression_attribute_values",
            crate::validation::strip_request_validation_tag,
        ) {
            Ok(v) => v,
            Err(err) => return Ok(err),
        };

        // Built as wire JSON and deserialised through the request's own
        // `Deserialize`, rather than assembled field by field. That is where
        // the `topK`, `SearchVector` and `IndexName` bounds live, so building
        // the struct directly skipped every one of them and answered an agent
        // with the operation-layer message where a wire client gets the
        // enveloped member-path constraint. The values go across as given; only
        // the member names change, because these params are snake_case.
        let mut wire = serde_json::json!({
            "TableName": params.table_name,
            "IndexName": params.index_name,
            "SearchVector": params.search_vector,
            "TopK": params.top_k,
        });
        let optional: [(&str, Option<serde_json::Value>); 5] = [
            (
                "SearchConditionExpression",
                params
                    .search_condition_expression
                    .map(serde_json::Value::from),
            ),
            (
                "ProjectionExpression",
                params.projection_expression.map(serde_json::Value::from),
            ),
            (
                "ExpressionAttributeNames",
                params
                    .expression_attribute_names
                    .and_then(|v| serde_json::to_value(v).ok()),
            ),
            (
                "ExpressionAttributeValues",
                expression_attribute_values.and_then(|v| serde_json::to_value(v).ok()),
            ),
            (
                "ReturnConsumedCapacity",
                params.return_consumed_capacity.map(serde_json::Value::from),
            ),
        ];
        for (name, value) in optional {
            if let Some(value) = value {
                wire[name] = value;
            }
        }
        let request: crate::actions::search_vectors::SearchVectorsRequest =
            match parse_json_param(wire, "search_request") {
                Ok(r) => r,
                Err(e) => return Ok(e),
            };
        match self.db.search_vectors(request) {
            Ok(resp) => {
                let json = serde_json::to_value(&resp)
                    .map_err(|e| McpError::internal_error(e.to_string(), None))?;
                self.json_result_checked(&json)
            }
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
        if let Some(err) = refuse_bad_capacity_mode(params.return_consumed_capacity.as_deref()) {
            return Ok(err);
        }
        let expression_attribute_values = match parse_optional_dynamo_map(
            params.expression_attribute_values,
            "expression_attribute_values",
            crate::validation::strip_request_validation_tag,
        ) {
            Ok(v) => v,
            Err(err) => return Ok(err),
        };
        let exclusive_start_key = match parse_optional_dynamo_map(
            params.exclusive_start_key,
            "exclusive_start_key",
            crate::validation::strip_request_validation_tag,
        ) {
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
            return_consumed_capacity: params.return_consumed_capacity,
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
        if let Some(err) = refuse_bad_capacity_mode(params.return_consumed_capacity.as_deref()) {
            return Ok(err);
        }
        let expression_attribute_values = match parse_optional_dynamo_map(
            params.expression_attribute_values,
            "expression_attribute_values",
            crate::validation::strip_request_validation_tag,
        ) {
            Ok(v) => v,
            Err(err) => return Ok(err),
        };
        let exclusive_start_key = match parse_optional_dynamo_map(
            params.exclusive_start_key,
            "exclusive_start_key",
            crate::validation::strip_request_validation_tag,
        ) {
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
            return_consumed_capacity: params.return_consumed_capacity,
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
        if let Some(err) = refuse_bad_capacity_mode(params.return_consumed_capacity.as_deref()) {
            return Ok(err);
        }
        let key = match parse_dynamo_map(
            params.key,
            "key",
            crate::validation::envelope_request_validation,
        ) {
            Ok(v) => v,
            Err(err) => return Ok(err),
        };
        let expression_attribute_values = match parse_optional_dynamo_map(
            params.expression_attribute_values,
            "expression_attribute_values",
            crate::validation::envelope_request_validation,
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
            return_consumed_capacity: params.return_consumed_capacity,
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
        if let Some(err) = refuse_bad_capacity_mode(params.return_consumed_capacity.as_deref()) {
            return Ok(err);
        }
        // Parse the request_items Value into the typed struct.
        // We accept both PascalCase and snake_case keys for agent ergonomics.
        let request_items = match parse_batch_write_request_items(params.request_items) {
            Ok(v) => v,
            Err(e) => return Ok(e),
        };

        let request = crate::actions::batch_write_item::BatchWriteItemRequest {
            request_items,
            return_consumed_capacity: params.return_consumed_capacity,
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
        if let Some(err) = refuse_bad_capacity_mode(params.return_consumed_capacity.as_deref()) {
            return Ok(err);
        }
        let request_items = match parse_batch_get_request_items(params.request_items) {
            Ok(v) => v,
            Err(e) => return Ok(e),
        };

        let request = crate::actions::batch_get_item::BatchGetItemRequest {
            request_items,
            return_consumed_capacity: params.return_consumed_capacity,
        };
        match self.db.batch_get_item(request) {
            Ok(resp) => json_result(&resp),
            Err(err) => Ok(to_tool_error(err)),
        }
    }

    #[tool(
        description = "Execute a PartiQL statement. Supports SELECT (with LIMIT, nested paths), INSERT (with IF NOT EXISTS, parameter placeholders), UPDATE (with SET arithmetic, REMOVE, nested paths), DELETE. UPDATE and DELETE accept a RETURNING clause, surfaced in the response Items: DELETE allows only RETURNING ALL OLD * (returning the deleted item; other variants are rejected), UPDATE allows all four RETURNING <ALL|MODIFIED> <OLD|NEW> * variants. WHERE supports BETWEEN, IN, CONTAINS, IS MISSING, IS NOT MISSING, AND, OR, NOT, parenthesised grouping up to 64 levels deep, and nested paths; AND binds tighter than OR, and a NOT over a group is applied by De Morgan. An index is named in the FROM clause as \"table\".\"index\", which serves the read from that GSI or LSI on SELECT only; a write statement rejects the qualifier. An UPDATE or DELETE whose WHERE condition fails is a ConditionalCheckFailedException; with return_values_on_condition_check_failure set to ALL_OLD the error carries the item the condition was evaluated against. Write statements are blocked in read-only mode."
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
        // The action lists a bad ReturnConsumedCapacity with the statement's
        // other member errors, so it isn't refused on its own here.
        let parameters: Option<Vec<AttributeValue>> = match params.parameters {
            Some(val) => match serde_json::from_value(val) {
                Ok(v) => Some(v),
                Err(e) => {
                    // Marker-tagged serde failures classify through the shared
                    // decoder, reporting the request-validation family bare;
                    // malformed input keeps the field-specific message, with
                    // the echoed serde text cleaned so neither marker nor
                    // position suffix can leak.
                    let msg = e.to_string();
                    if let Some(err) = crate::serde_errors::classify_marked_serde_error(&msg) {
                        return Ok(to_tool_error(
                            crate::validation::strip_request_validation_tag(err),
                        ));
                    }
                    return Err(McpError::invalid_params(
                        format!(
                            "Invalid parameters: {}",
                            crate::serde_errors::clean_serde_message(&msg)
                        ),
                        None,
                    ));
                }
            },
            None => None,
        };

        let request = crate::actions::execute_statement::ExecuteStatementRequest {
            statement: params.statement,
            parameters,
            limit: params.limit,
            next_token: params.next_token,
            consistent_read: params.consistent_read,
            return_consumed_capacity: params.return_consumed_capacity,
            return_values_on_condition_check_failure: params
                .return_values_on_condition_check_failure,
        };
        match self.db.execute_statement(request) {
            Ok(resp) => json_result(&resp),
            Err(err) => Ok(to_tool_error(err)),
        }
    }

    #[tool(
        description = "Get database-level information: storage mode, path, size, table count, and per-table summaries. When the data was loaded by dynoxide import, also returns the import's notices and whether any exposures were accepted. Use this as your first call to orient yourself."
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
                if let Some(meta) = t.metadata
                    && let Ok(serde_json::Value::Array(ks)) =
                        serde_json::from_str::<serde_json::Value>(&meta.key_schema)
                {
                    for elem in &ks {
                        let name = elem.get("AttributeName").and_then(|v| v.as_str());
                        let key_type = elem.get("KeyType").and_then(|v| v.as_str());
                        match (name, key_type) {
                            (Some(n), Some("HASH")) => {
                                entry["partition_key"] = serde_json::Value::String(n.to_string());
                            }
                            (Some(n), Some("RANGE")) => {
                                entry["sort_key"] = serde_json::Value::String(n.to_string());
                            }
                            _ => {}
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

        if let Some(ref report) = self.config.import {
            info["import"] = report.to_json();
        }

        json_result(&info)
    }

    #[tool(
        description = "[WRITE] Create a snapshot of the current database state. Returns the snapshot name for later restoration via restore_snapshot. A global limit of 20 snapshots is enforced, and when it is reached auto-snapshots are evicted first, then the oldest manual ones. This holds the database lock for the duration of the copy, so a large database may briefly block other operations."
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
        description = "[WRITE] Update a table: add/remove GSIs, change stream settings, switch BillingMode, ProvisionedThroughput or OnDemandThroughput, change TableClass, or toggle DeletionProtectionEnabled."
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
        // Rewritten into the wire spelling before anything reads it. The shared
        // parser and every guard around it are case sensitive, as DynamoDB is,
        // so a second spelling accepted down there would reach the parser and
        // miss the validation. Canonicalising here keeps the wire strict and
        // still lets an agent write the snake_case this surface documents.
        let canonical_updates = params
            .vector_index_updates
            .as_ref()
            .map(canonicalise_vector_index_updates);
        // Request-model constraints first, then the typed parse, which is the
        // order the raw request's Deserialize runs them in. Reversed, a create
        // missing a member answers a serde error where the wire names the
        // member and its path.
        if let Some(ref canonical) = canonical_updates
            && let Some(msg) =
                crate::actions::update_table::vector_index_updates_request_model_error(canonical)
        {
            return Ok(tool_validation_error("ValidationException", &msg));
        }
        let vector_index_updates = match canonical_updates
            .as_ref()
            .map(crate::actions::update_table::parse_vector_index_updates)
            .transpose()
        {
            Ok(v) => v,
            Err(e) => {
                return Ok(tool_validation_error(
                    "InvalidVectorIndexUpdates",
                    &format!("Invalid vector_index_updates: {e}"),
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

        // The update action reads ProvisionedThroughput from raw JSON with
        // PascalCase keys, so normalise through the typed struct, which
        // accepts either case. Passing snake_case straight through would
        // silently read the capacity units as zero.
        let provisioned_throughput = match params
            .provisioned_throughput
            .map(|v| {
                serde_json::from_value::<crate::types::ProvisionedThroughput>(v)
                    .and_then(|pt| serde_json::to_value(&pt))
            })
            .transpose()
        {
            Ok(v) => v,
            Err(e) => {
                return Ok(tool_validation_error(
                    "InvalidProvisionedThroughput",
                    &format!("Invalid provisioned_throughput: {e}"),
                ));
            }
        };

        let on_demand_throughput = match parse_optional_json_param(
            params.on_demand_throughput,
            "InvalidOnDemandThroughput",
            "on_demand_throughput",
        ) {
            Ok(v) => v,
            Err(e) => return Ok(e),
        };

        let request = crate::actions::update_table::UpdateTableRequest {
            table_name: params.table_name,
            attribute_definitions,
            global_secondary_index_updates,
            vector_index_updates,
            stream_specification,
            deletion_protection_enabled: params.deletion_protection_enabled,
            billing_mode: params.billing_mode,
            provisioned_throughput,
            table_class: params.table_class,
            on_demand_throughput,
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
        if let Some(err) = refuse_bad_capacity_mode(params.return_consumed_capacity.as_deref()) {
            return Ok(err);
        }
        let transact_items = match parse_json_param(params.transact_items, "transact_items") {
            Ok(v) => v,
            Err(err) => return Ok(err),
        };
        let request = crate::actions::transact_write_items::TransactWriteItemsRequest {
            transact_items,
            client_request_token: params.client_request_token,
            return_consumed_capacity: params.return_consumed_capacity,
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
        if let Some(err) = refuse_bad_capacity_mode(params.return_consumed_capacity.as_deref()) {
            return Ok(err);
        }
        let transact_items = match parse_json_param(params.transact_items, "transact_items") {
            Ok(v) => v,
            Err(err) => return Ok(err),
        };
        let request = crate::actions::transact_get_items::TransactGetItemsRequest {
            transact_items,
            return_consumed_capacity: params.return_consumed_capacity,
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
        description = "[WRITE] Execute a batch of PartiQL statements. Each statement can be SELECT, INSERT, UPDATE, or DELETE. A member's RETURNING clause is honoured, with the returned item surfaced on that response's Item field. Write statements are blocked in read-only mode."
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

        // The mode is checked by the action with the other members, so a bad
        // mode is listed with them rather than refused on its own first.
        let request = crate::actions::batch_execute_statement::BatchExecuteStatementRequest {
            statements,
            return_consumed_capacity: params.return_consumed_capacity,
        };
        match self.db.batch_execute_statement(request) {
            Ok(resp) => json_result(&resp),
            Err(err) => Ok(to_tool_error(err)),
        }
    }

    // -----------------------------------------------------------------------
    // PartiQL transaction
    // -----------------------------------------------------------------------

    #[tool(
        description = "[WRITE] [DESTRUCTIVE] Execute PartiQL statements transactionally (all-or-nothing). Max 100 statements. If any fails, the entire transaction is rolled back. A RETURNING clause is not supported inside a transaction and is rejected with a ValidationException. Example transact_statements: [{\"Statement\": \"INSERT INTO Users VALUE {'pk': 'u1', 'name': 'Alice'}\"}]"
    )]
    fn execute_transaction_partiql(
        &self,
        Parameters(params): Parameters<ExecuteTransactionPartiqlParams>,
    ) -> Result<CallToolResult, McpError> {
        if let Some(err) = self.reject_if_read_only("execute_transaction_partiql") {
            return Ok(err);
        }
        // The mode is checked by the action with the token and the
        // statements, so a bad mode is listed with them.

        let transact_statements: Vec<crate::actions::execute_transaction::ParameterizedStatement> =
            match parse_json_param(params.transact_statements, "transact_statements") {
                Ok(v) => v,
                Err(err) => return Ok(err),
            };

        let request = crate::actions::execute_transaction::ExecuteTransactionRequest {
            transact_statements,
            client_request_token: params.client_request_token,
            return_consumed_capacity: params.return_consumed_capacity,
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
            match parse_dynamo_map(
                item_val,
                &format!("items[{i}]"),
                crate::validation::strip_request_validation_tag,
            ) {
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
            All data is local: no AWS credentials required.\n\n\
            ## Getting started\n\
            1. Call `get_database_info` first to see tables, key schemas, and server config.\n\
            2. Use `describe_table` for detailed schema of a specific table.\n\n\
            ## Key concepts\n\
            - Items use DynamoDB JSON format: `{\"pk\": {\"S\": \"value\"}, \"age\": {\"N\": \"30\"}}`\n\
            - Tool names prefixed with `[WRITE]` modify data; `[DESTRUCTIVE]` may cause data loss.\n\
            - In read-only mode (`--read-only`), all write tools return an error.\n\
            - Batch tools accept both PascalCase and snake_case keys (e.g. `PutRequest` or `put_request`).\n\
            - The item, query, scan, search_vectors, batch_get_item, batch_write_item, \
            transaction and PartiQL tools take `return_consumed_capacity`: TOTAL or INDEXES adds \
            the block DynamoDB would return under a `ConsumedCapacity` key, and NONE, the default, \
            adds nothing.\n\n\
            ## Available tools\n\
            Tables: list_tables, describe_table, create_table, delete_table, update_table\n\
            Items: put_item, get_item, update_item, delete_item\n\
            Batch: batch_write_item, batch_get_item, bulk_put_items\n\
            Query: query, scan\n\
            Vector search: search_vectors\n\
            Transactions: transact_write_items, transact_get_items\n\
            PartiQL: execute_partiql, batch_execute_partiql, execute_transaction_partiql\n\
            TTL: update_time_to_live, describe_time_to_live, sweep_ttl\n\
            Tags: tag_resource, untag_resource, list_tags_of_resource\n\
            Streams: list_streams, describe_stream, get_shard_iterator, get_records\n\
            Snapshots: create_snapshot, restore_snapshot, list_snapshots, delete_snapshot\n\
            Info: get_database_info";

        let mut instructions = base_instructions.to_string();
        if let Some(report) = &self.config.import {
            instructions.push_str("\n\n");
            instructions.push_str(&report.instructions_sentence());
        }
        if let Some(summary) = self
            .config
            .data_model
            .as_ref()
            .and_then(|dm| dm.instructions_summary(self.config.data_model_summary_limit))
        {
            instructions.push_str("\n\n");
            instructions.push_str(&summary);
        }

        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("dynoxide", crate::PRODUCT_VERSION))
            .with_instructions(instructions)
    }
}

// ---------------------------------------------------------------------------
// Batch operation parsing helpers
// ---------------------------------------------------------------------------

/// Tool error for malformed batch request_items, mirroring the wording the
/// batch decoders below have always produced.
fn invalid_request_items_error(message: &str) -> CallToolResult {
    tool_validation_error("InvalidRequestItems", message)
}

/// Classify a serde failure inside the hand-rolled batch decoders: a
/// marker-tagged failure classifies through the shared decoder in
/// `serde_errors`, reporting the request-validation family bare; anything else
/// keeps the site's message, with the echoed serde text cleaned so neither
/// marker nor position suffix can leak.
fn batch_decode_error(context: &str, msg: &str) -> CallToolResult {
    match crate::serde_errors::classify_marked_serde_error(msg) {
        Some(err) => to_tool_error(crate::validation::strip_request_validation_tag(err)),
        None => invalid_request_items_error(&format!(
            "{context}: {}",
            crate::serde_errors::clean_serde_message(msg)
        )),
    }
}

/// Parse batch_write_item request_items from JSON Value.
/// Accepts both PascalCase and snake_case keys.
fn parse_batch_write_request_items(
    val: serde_json::Value,
) -> Result<HashMap<String, Vec<crate::actions::batch_write_item::WriteRequest>>, CallToolResult> {
    let map = val
        .as_object()
        .ok_or_else(|| invalid_request_items_error("request_items must be an object"))?;

    let mut result = HashMap::new();
    for (table_name, requests) in map {
        let arr = requests.as_array().ok_or_else(|| {
            invalid_request_items_error(&format!("request_items['{table_name}'] must be an array"))
        })?;

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
                        .ok_or_else(|| invalid_request_items_error("PutRequest missing Item"))?;
                    let item: HashMap<String, AttributeValue> =
                        serde_json::from_value(item_val.clone()).map_err(|e| {
                            batch_decode_error("Invalid item in PutRequest", &e.to_string())
                        })?;
                    Some(crate::actions::batch_write_item::PutRequest { item })
                }
                None => None,
            };

            let delete_request = match del {
                Some(d) => {
                    let key_val = d
                        .get("Key")
                        .or_else(|| d.get("key"))
                        .ok_or_else(|| invalid_request_items_error("DeleteRequest missing Key"))?;
                    let key: HashMap<String, AttributeValue> =
                        serde_json::from_value(key_val.clone()).map_err(|e| {
                            batch_decode_error("Invalid key in DeleteRequest", &e.to_string())
                        })?;
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
) -> Result<HashMap<String, crate::actions::batch_get_item::KeysAndAttributes>, CallToolResult> {
    let map = val
        .as_object()
        .ok_or_else(|| invalid_request_items_error("request_items must be an object"))?;

    let mut result = HashMap::new();
    for (table_name, attrs) in map {
        let keys_val = attrs
            .get("Keys")
            .or_else(|| attrs.get("keys"))
            .ok_or_else(|| {
                invalid_request_items_error(&format!("request_items['{table_name}'] missing Keys"))
            })?;

        let keys: Vec<HashMap<String, AttributeValue>> = serde_json::from_value(keys_val.clone())
            .map_err(|e| {
            batch_decode_error(&format!("Invalid keys for '{table_name}'"), &e.to_string())
        })?;

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

    // Vector indexes are summarised on the same terms as the GSIs. Without
    // this an agent could create one and never see it again, since this view
    // is the only shape describe_table returns through MCP.
    let vector_indexes: Vec<serde_json::Value> = desc
        .vector_indexes
        .as_ref()
        .map(|vixs| {
            vixs.iter()
                .map(|vix| {
                    // Absent stays absent. Defaulting to `[]` would be read
                    // back and fed to create_table as an empty search schema
                    // the index never carried.
                    let schema: Option<Vec<serde_json::Value>> =
                        vix.search_schema.as_ref().map(|elems| {
                            elems
                                .iter()
                                .map(|e| {
                                    serde_json::json!({
                                        "attribute_name": e.attribute_name,
                                        "search_schema_element_type":
                                            e.search_schema_element_type,
                                    })
                                })
                                .collect()
                        });
                    let mut out = serde_json::json!({
                        "index_name": vix.index_name,
                        // Shaped as create_table takes it, not flattened to a
                        // bare name: reading an index back and handing it to
                        // the create tool is the obvious way to clone a table,
                        // and a bare string fails to deserialise there.
                        "vector_attribute": {
                            "attribute_name": vix.vector_attribute.attribute_name
                        },
                        "dimensions": vix.dimensions,
                        "distance_function": vix.distance_function,
                        "projection": {"projection_type": vix.projection.projection_type},
                        "item_count": vix.item_count.unwrap_or(0),
                        // An index added to a live table is not searchable at
                        // once, so an agent needs the status to know when to
                        // retry. Without it this view reports every index the
                        // same and the wait is unwritable.
                        "index_status": vix.index_status,
                    });
                    // Absent stays absent here too: the wire drops the field
                    // once the index is ACTIVE rather than reporting false, and
                    // a readiness check written against `== false` must fail
                    // the same way on both surfaces.
                    if let Some(backfilling) = vix.backfilling {
                        out["backfilling"] = serde_json::Value::Bool(backfilling);
                    }
                    if let Some(schema) = schema {
                        out["search_schema"] = serde_json::Value::Array(schema);
                    }
                    out
                })
                .collect()
        })
        .unwrap_or_default();

    let stream_enabled = desc.latest_stream_arn.is_some();

    let billing_mode = desc
        .billing_mode_summary
        .as_ref()
        .map(|s| s.billing_mode.as_str())
        .unwrap_or("PROVISIONED");
    let table_class = desc
        .table_class_summary
        .as_ref()
        .map(|s| s.table_class.as_str())
        .unwrap_or("STANDARD");

    let mut flat = serde_json::json!({
        "table_name": desc.table_name,
        "status": desc.table_status,
        "partition_key": pk,
        "sort_key": sk,
        "item_count": desc.item_count.unwrap_or(0),
        "size_bytes": desc.table_size_bytes.unwrap_or(0),
        "gsis": gsis,
        "vector_indexes": vector_indexes,
        "stream_enabled": stream_enabled,
        "billing_mode": billing_mode,
        "table_class": table_class,
    });

    // Capacity settings only appear in the mode they belong to: zeroed
    // provisioned throughput on an on-demand table is noise, not signal.
    if billing_mode == "PROVISIONED"
        && let Some(ref pt) = desc.provisioned_throughput
    {
        flat["provisioned_throughput"] = serde_json::json!({
            "read_capacity_units": pt.read_capacity_units,
            "write_capacity_units": pt.write_capacity_units,
        });
    }
    if let Some(ref odt) = desc.on_demand_throughput {
        flat["on_demand_throughput"] = serde_json::json!({
            "max_read_request_units": odt.max_read_request_units,
            "max_write_request_units": odt.max_write_request_units,
        });
    }

    flat
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

#[cfg(test)]
mod tests {
    use super::*;

    fn report(exposures: usize, cautions: usize, accepted: bool) -> ImportReport {
        let mut notices = Vec::new();
        for i in 0..exposures {
            notices.push(ImportNotice {
                concern: ImportConcern::Exposure,
                message: format!("exposure {i}"),
            });
        }
        for i in 0..cautions {
            notices.push(ImportNotice {
                concern: ImportConcern::Caution,
                message: format!("caution {i}"),
            });
        }
        ImportReport {
            tables: 1,
            items: 1,
            skipped: 0,
            notices,
            exposures_accepted: accepted,
        }
    }

    #[test]
    fn several_accepted_exposures_read_as_a_plural() {
        assert_eq!(
            report(2, 1, true).instructions_sentence(),
            "This database was populated by dynoxide import; get_database_info reports 3 notices, \
             of which 2 are exposures that were accepted with --accept-exposure."
        );
    }

    #[test]
    fn an_exposure_nobody_accepted_is_still_named() {
        // Only reachable for a crate user building the config by hand, since
        // the CLI exits before serving. The sentence must not claim acceptance.
        assert_eq!(
            report(1, 0, false).instructions_sentence(),
            "This database was populated by dynoxide import; get_database_info reports 1 notice, \
             of which 1 is an exposure."
        );
    }

    #[test]
    fn the_json_counts_agree_with_the_notices() {
        let json = report(1, 2, true).to_json();
        assert_eq!(json["notice_count"], 3);
        assert_eq!(json["exposure_count"], 1);
        assert_eq!(json["exposures_accepted"], true);
        assert_eq!(json["notices"][0]["concern"], "exposure");
        assert_eq!(json["notices"][1]["concern"], "caution");
        assert_eq!(json["notices"].as_array().unwrap().len(), 3);
    }
}

/// `return_consumed_capacity` on the tools. Each tool has to hand back the
/// block its action returns for the same request against the same data,
/// refuse a value outside DynamoDB's enum with the error a wire client gets,
/// and add nothing when the caller does not ask.
#[cfg(test)]
mod capacity_tests {
    use super::*;
    use serde_json::{Value, json};

    /// What DynamoDB returns for a mode outside its enum, on every operation
    /// that takes one (captured eu-west-2, 2026-10-08).
    const BOGUS_MODE: &str = "1 validation error detected: Value 'BOGUS' at \
        'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy \
        enum value set: [INDEXES, TOTAL, NONE]";

    fn wire<T: serde::de::DeserializeOwned>(value: Value) -> T {
        serde_json::from_value(value).expect("fixture request deserialises")
    }

    /// Two tables, built the same way every time so two copies answer any
    /// request with the same figures. `Cap` has a GSI and an LSI, so a write's
    /// INDEXES block carries both arms; `Vecs` has a vector index for search.
    fn seeded() -> Database {
        let db = Database::memory().unwrap();
        db.create_table(wire(json!({
            "TableName": "Cap",
            "BillingMode": "PAY_PER_REQUEST",
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"}
            ],
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
                {"AttributeName": "g", "AttributeType": "S"},
                {"AttributeName": "l", "AttributeType": "S"}
            ],
            "GlobalSecondaryIndexes": [{
                "IndexName": "byG",
                "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"}
            }],
            "LocalSecondaryIndexes": [{
                "IndexName": "byL",
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "l", "KeyType": "RANGE"}
                ],
                "Projection": {"ProjectionType": "ALL"}
            }]
        })))
        .unwrap();
        for (sk, g, l, size) in [
            ("s1", Some("g1"), "l1", 600),
            ("s2", Some("g1"), "l2", 5000),
            ("s3", None, "l3", 9000),
        ] {
            let mut item = json!({
                "pk": {"S": "p"}, "sk": {"S": sk}, "l": {"S": l},
                "data": {"S": "x".repeat(size)}
            });
            if let Some(g) = g {
                item["g"] = json!({"S": g});
            }
            db.put_item(wire(json!({"TableName": "Cap", "Item": item})))
                .unwrap();
        }
        db.create_table(wire(json!({
            "TableName": "Vecs",
            "BillingMode": "PAY_PER_REQUEST",
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "VectorIndexes": [{
                "IndexName": "cosine",
                "VectorAttribute": {"AttributeName": "embedding"},
                "Dimensions": 3,
                "DistanceFunction": "COSINE",
                "Projection": {"ProjectionType": "ALL"}
            }]
        })))
        .unwrap();
        for (pk, v) in [("a", ["1", "0", "0"]), ("b", ["0", "1", "0"])] {
            let embedding: Vec<Value> = v.iter().map(|n| json!({"N": n})).collect();
            db.put_item(wire(json!({
                "TableName": "Vecs",
                "Item": {"pk": {"S": pk}, "embedding": {"L": embedding}}
            })))
            .unwrap();
        }
        db
    }

    fn server() -> McpServer {
        McpServer::new(Arc::new(seeded()))
    }

    /// A tool's reply parsed as JSON, and whether it is an error.
    fn reply(result: Result<CallToolResult, McpError>) -> (bool, Value) {
        let result = result.expect("the tool answers rather than failing the protocol");
        let text = &result.content[0]
            .as_text()
            .expect("tools reply in text")
            .text;
        (
            result.is_error == Some(true),
            serde_json::from_str(text).expect("tools reply in JSON"),
        )
    }

    /// A capacity block with any per-table list sorted by table name. The
    /// batch and transaction operations build that list from a map, so its
    /// order varies between two runs of the same request.
    fn by_table(block: Value) -> Value {
        match block {
            Value::Array(mut entries) => {
                entries.sort_by(|a, b| {
                    a["TableName"]
                        .as_str()
                        .unwrap_or_default()
                        .cmp(b["TableName"].as_str().unwrap_or_default())
                });
                Value::Array(entries)
            }
            other => other,
        }
    }

    type ToolCall = fn(&McpServer, Value) -> Result<CallToolResult, McpError>;
    type ActionCall = fn(&Database, Value) -> Value;

    /// One tool and the action behind it, given the same request in each
    /// shape: `args` as the tool takes it, `request` as the wire takes it.
    struct Case {
        tool: ToolCall,
        args: Value,
        action: ActionCall,
        request: Value,
    }

    impl Case {
        fn call(&self, server: &McpServer, mode: Option<&str>) -> (bool, Value) {
            match mode {
                Some(mode) => self.call_with(server, json!(mode)),
                None => reply((self.tool)(server, self.args.clone())),
            }
        }

        /// Call with `return_consumed_capacity` set to any JSON value.
        fn call_with(&self, server: &McpServer, mode: Value) -> (bool, Value) {
            let mut args = self.args.clone();
            args["return_consumed_capacity"] = mode;
            reply((self.tool)(server, args))
        }

        /// The action's own block for `mode`, from a fresh copy of the data.
        fn expected(&self, mode: &str) -> Value {
            let mut request = self.request.clone();
            request["ReturnConsumedCapacity"] = json!(mode);
            let body = (self.action)(&seeded(), request);
            assert!(
                !body["ConsumedCapacity"].is_null(),
                "{mode}: the action reported no capacity, so a match would prove nothing"
            );
            by_table(body["ConsumedCapacity"].clone())
        }

        /// The tool's block for `mode`, from a fresh copy of the data.
        fn reported(&self, mode: &str) -> Value {
            let (is_error, body) = self.call(&server(), Some(mode));
            assert!(!is_error, "{mode}: the tool failed: {body}");
            by_table(body["ConsumedCapacity"].clone())
        }

        /// Every check the fourteen tools share.
        fn check(&self) {
            for mode in ["TOTAL", "INDEXES"] {
                assert_eq!(self.reported(mode), self.expected(mode), "{mode}");
            }
            for mode in [Some("NONE"), None] {
                let (is_error, body) = self.call(&server(), mode);
                assert!(!is_error, "{mode:?}: the tool failed: {body}");
                assert!(
                    body.get("ConsumedCapacity").is_none(),
                    "{mode:?}: unasked capacity in {body}"
                );
            }
            // An explicit null is how some clients spell "not set".
            let (is_error, body) = self.call_with(&server(), Value::Null);
            assert!(!is_error, "null: the tool failed: {body}");
            assert!(
                body.get("ConsumedCapacity").is_none(),
                "null: unasked capacity in {body}"
            );
            let (is_error, body) = self.call(&server(), Some("BOGUS"));
            assert!(is_error, "BOGUS was accepted: {body}");
            assert_eq!(body["error_type"], "ValidationException");
            assert_eq!(body["message"], BOGUS_MODE);
        }
    }

    fn get_item_case(consistent: bool) -> Case {
        let key = json!({"pk": {"S": "p"}, "sk": {"S": "s2"}});
        Case {
            tool: |s, v| s.get_item(Parameters(wire(v))),
            args: json!({"table_name": "Cap", "key": key, "consistent_read": consistent}),
            action: |db, v| serde_json::to_value(db.get_item(wire(v)).unwrap()).unwrap(),
            request: json!({"TableName": "Cap", "Key": key, "ConsistentRead": consistent}),
        }
    }

    fn put_item_case() -> Case {
        let item = json!({
            "pk": {"S": "p"}, "sk": {"S": "s9"}, "g": {"S": "g9"}, "l": {"S": "l9"},
            "data": {"S": "y".repeat(1500)}
        });
        Case {
            tool: |s, v| s.put_item(Parameters(wire(v))),
            args: json!({"table_name": "Cap", "item": item}),
            action: |db, v| serde_json::to_value(db.put_item(wire(v)).unwrap()).unwrap(),
            request: json!({"TableName": "Cap", "Item": item}),
        }
    }

    fn query_case(index: Option<&str>) -> Case {
        let mut args = json!({
            "table_name": "Cap",
            "key_condition_expression": "pk = :p",
            "expression_attribute_values": {":p": {"S": "p"}}
        });
        let mut request = json!({
            "TableName": "Cap",
            "KeyConditionExpression": "pk = :p",
            "ExpressionAttributeValues": {":p": {"S": "p"}}
        });
        if let Some(index) = index {
            args["index_name"] = json!(index);
            request["IndexName"] = json!(index);
        }
        Case {
            tool: |s, v| s.query(Parameters(wire(v))),
            args,
            action: |db, v| serde_json::to_value(db.query(wire(v)).unwrap()).unwrap(),
            request,
        }
    }

    fn transact_write_case() -> Case {
        let items = json!([
            {"Put": {"TableName": "Cap", "Item": {
                "pk": {"S": "p"}, "sk": {"S": "s8"}, "g": {"S": "g8"}, "l": {"S": "l8"}
            }}},
            {"ConditionCheck": {
                "TableName": "Cap",
                "Key": {"pk": {"S": "p"}, "sk": {"S": "s1"}},
                "ConditionExpression": "attribute_exists(pk)"
            }}
        ]);
        Case {
            tool: |s, v| s.transact_write_items(Parameters(wire(v))),
            args: json!({"transact_items": items}),
            action: |db, v| {
                serde_json::to_value(db.transact_write_items(wire(v)).unwrap()).unwrap()
            },
            request: json!({"TransactItems": items}),
        }
    }

    #[test]
    fn get_item_reports_the_actions_capacity() {
        get_item_case(false).check();
    }

    #[test]
    fn a_consistent_get_item_reports_the_doubled_rate() {
        let eventual = get_item_case(false).reported("TOTAL");
        let consistent = get_item_case(true);
        consistent.check();
        assert_eq!(
            consistent.reported("TOTAL")["CapacityUnits"]
                .as_f64()
                .unwrap(),
            eventual["CapacityUnits"].as_f64().unwrap() * 2.0
        );
    }

    #[test]
    fn put_item_reports_the_actions_capacity() {
        let case = put_item_case();
        case.check();
        let indexes = case.reported("INDEXES");
        assert!(
            indexes["GlobalSecondaryIndexes"]["byG"].is_object(),
            "no GSI arm in {indexes}"
        );
        assert!(
            indexes["LocalSecondaryIndexes"]["byL"].is_object(),
            "no LSI arm in {indexes}"
        );
    }

    #[test]
    fn update_item_reports_the_actions_capacity() {
        let key = json!({"pk": {"S": "p"}, "sk": {"S": "s1"}});
        Case {
            tool: |s, v| s.update_item(Parameters(wire(v))),
            args: json!({
                "table_name": "Cap", "key": key,
                "update_expression": "SET g = :g",
                "expression_attribute_values": {":g": {"S": "g3"}}
            }),
            action: |db, v| serde_json::to_value(db.update_item(wire(v)).unwrap()).unwrap(),
            request: json!({
                "TableName": "Cap", "Key": key,
                "UpdateExpression": "SET g = :g",
                "ExpressionAttributeValues": {":g": {"S": "g3"}}
            }),
        }
        .check();
    }

    #[test]
    fn delete_item_reports_the_actions_capacity() {
        let key = json!({"pk": {"S": "p"}, "sk": {"S": "s2"}});
        Case {
            tool: |s, v| s.delete_item(Parameters(wire(v))),
            args: json!({"table_name": "Cap", "key": key}),
            action: |db, v| serde_json::to_value(db.delete_item(wire(v)).unwrap()).unwrap(),
            request: json!({"TableName": "Cap", "Key": key}),
        }
        .check();
    }

    #[test]
    fn query_reports_the_actions_capacity() {
        query_case(None).check();
    }

    #[test]
    fn an_lsi_query_reports_the_actions_capacity() {
        let case = query_case(Some("byL"));
        case.check();
        let indexes = case.reported("INDEXES");
        assert!(
            indexes["LocalSecondaryIndexes"]["byL"].is_object(),
            "no LSI arm in {indexes}"
        );
    }

    #[test]
    fn a_count_query_reports_the_actions_capacity() {
        let mut case = query_case(None);
        case.args["select"] = json!("COUNT");
        case.request["Select"] = json!("COUNT");
        case.check();
    }

    #[test]
    fn a_capped_query_reports_the_capped_read() {
        let config = McpConfig {
            max_items: Some(1),
            ..McpConfig::default()
        };
        let capped = McpServer::with_config(Arc::new(seeded()), config);
        let case = query_case(None);
        let (is_error, body) = case.call(&capped, Some("TOTAL"));
        assert!(!is_error, "{body}");
        let mut request = case.request.clone();
        request["Limit"] = json!(1);
        let expected = Case { request, ..case }.expected("TOTAL");
        assert_eq!(body["ConsumedCapacity"], expected);
        assert_ne!(
            body["ConsumedCapacity"],
            query_case(None).expected("TOTAL"),
            "the cap made no difference to the figure, so this proves nothing"
        );
    }

    #[test]
    fn scan_reports_the_actions_capacity() {
        Case {
            tool: |s, v| s.scan(Parameters(wire(v))),
            args: json!({"table_name": "Cap", "filter_expression": "attribute_exists(g)"}),
            action: |db, v| serde_json::to_value(db.scan(wire(v)).unwrap()).unwrap(),
            request: json!({"TableName": "Cap", "FilterExpression": "attribute_exists(g)"}),
        }
        .check();
    }

    #[test]
    fn search_vectors_reports_the_actions_capacity() {
        let vector = json!([{"N": "1"}, {"N": "0"}, {"N": "0"}]);
        Case {
            tool: |s, v| s.search_vectors(Parameters(wire(v))),
            args: json!({
                "table_name": "Vecs", "index_name": "cosine",
                "search_vector": vector, "top_k": 1
            }),
            action: |db, v| serde_json::to_value(db.search_vectors(wire(v)).unwrap()).unwrap(),
            request: json!({
                "TableName": "Vecs", "IndexName": "cosine",
                "SearchVector": vector, "TopK": 1
            }),
        }
        .check();
    }

    #[test]
    fn batch_get_item_reports_the_actions_capacity() {
        let items = json!({
            "Cap": {"Keys": [
                {"pk": {"S": "p"}, "sk": {"S": "s1"}},
                {"pk": {"S": "p"}, "sk": {"S": "s3"}}
            ]},
            "Vecs": {"Keys": [{"pk": {"S": "a"}}]}
        });
        Case {
            tool: |s, v| s.batch_get_item(Parameters(wire(v))),
            args: json!({"request_items": items}),
            action: |db, v| serde_json::to_value(db.batch_get_item(wire(v)).unwrap()).unwrap(),
            request: json!({"RequestItems": items}),
        }
        .check();
    }

    #[test]
    fn batch_write_item_reports_the_actions_capacity() {
        let items = json!({
            "Cap": [
                {"PutRequest": {"Item": {
                    "pk": {"S": "p"}, "sk": {"S": "s7"}, "g": {"S": "g7"}, "l": {"S": "l7"}
                }}},
                {"DeleteRequest": {"Key": {"pk": {"S": "p"}, "sk": {"S": "s1"}}}}
            ]
        });
        Case {
            tool: |s, v| s.batch_write_item(Parameters(wire(v))),
            args: json!({"request_items": items}),
            action: |db, v| serde_json::to_value(db.batch_write_item(wire(v)).unwrap()).unwrap(),
            request: json!({"RequestItems": items}),
        }
        .check();
    }

    #[test]
    fn transact_get_items_reports_the_actions_capacity() {
        let items = json!([
            {"Get": {"TableName": "Cap", "Key": {"pk": {"S": "p"}, "sk": {"S": "s2"}}}},
            {"Get": {"TableName": "Vecs", "Key": {"pk": {"S": "a"}}}}
        ]);
        Case {
            tool: |s, v| s.transact_get_items(Parameters(wire(v))),
            args: json!({"transact_items": items}),
            action: |db, v| serde_json::to_value(db.transact_get_items(wire(v)).unwrap()).unwrap(),
            request: json!({"TransactItems": items}),
        }
        .check();
    }

    #[test]
    fn transact_write_items_reports_the_actions_capacity() {
        transact_write_case().check();
    }

    #[test]
    fn a_replayed_transaction_reports_what_the_action_reports() {
        let mut case = transact_write_case();
        case.args["client_request_token"] = json!("replay-token");
        case.request["ClientRequestToken"] = json!("replay-token");

        let server = server();
        case.call(&server, Some("TOTAL"));
        let (is_error, replayed) = case.call(&server, Some("TOTAL"));
        assert!(!is_error, "{replayed}");

        let db = seeded();
        let mut request = case.request.clone();
        request["ReturnConsumedCapacity"] = json!("TOTAL");
        (case.action)(&db, request.clone());
        let expected = (case.action)(&db, request);
        assert!(!expected["ConsumedCapacity"].is_null());
        assert_eq!(
            by_table(replayed["ConsumedCapacity"].clone()),
            by_table(expected["ConsumedCapacity"].clone())
        );
    }

    #[test]
    fn execute_partiql_select_reports_the_actions_capacity() {
        let statement = "SELECT * FROM \"Cap\" WHERE pk = 'p'";
        Case {
            tool: |s, v| s.execute_partiql(Parameters(wire(v))),
            args: json!({"statement": statement}),
            action: |db, v| serde_json::to_value(db.execute_statement(wire(v)).unwrap()).unwrap(),
            request: json!({"Statement": statement}),
        }
        .check();
    }

    #[test]
    fn an_index_qualified_select_reports_the_actions_capacity() {
        let statement = "SELECT * FROM \"Cap\".\"byG\" WHERE g = 'g1'";
        let case = Case {
            tool: |s, v| s.execute_partiql(Parameters(wire(v))),
            args: json!({"statement": statement}),
            action: |db, v| serde_json::to_value(db.execute_statement(wire(v)).unwrap()).unwrap(),
            request: json!({"Statement": statement}),
        };
        case.check();
        let indexes = case.reported("INDEXES");
        assert!(
            indexes["GlobalSecondaryIndexes"]["byG"].is_object(),
            "no GSI arm in {indexes}"
        );
    }

    #[test]
    fn execute_partiql_update_reports_the_actions_capacity() {
        let statement = "UPDATE \"Cap\" SET g = 'g5' WHERE pk = 'p' AND sk = 's1'";
        Case {
            tool: |s, v| s.execute_partiql(Parameters(wire(v))),
            args: json!({"statement": statement}),
            action: |db, v| serde_json::to_value(db.execute_statement(wire(v)).unwrap()).unwrap(),
            request: json!({"Statement": statement}),
        }
        .check();
    }

    #[test]
    fn batch_execute_partiql_reports_the_actions_capacity() {
        // The first member fails on a key that exists; the batch still
        // charges it the write it attempted.
        let statements = json!([
            {"Statement": "INSERT INTO \"Cap\" VALUE {'pk': 'p', 'sk': 's1'}"},
            {"Statement": "INSERT INTO \"Cap\" VALUE {'pk': 'n', 'sk': 'n1', 'g': 'g6'}"}
        ]);
        Case {
            tool: |s, v| s.batch_execute_partiql(Parameters(wire(v))),
            args: json!({"statements": statements}),
            action: |db, v| {
                serde_json::to_value(db.batch_execute_statement(wire(v)).unwrap()).unwrap()
            },
            request: json!({"Statements": statements}),
        }
        .check();
    }

    #[test]
    fn execute_transaction_partiql_reports_the_actions_capacity() {
        let statements = json!([
            {"Statement": "UPDATE \"Cap\" SET g = 'g4' WHERE pk = 'p' AND sk = 's1'"},
            {"Statement": "INSERT INTO \"Cap\" VALUE {'pk': 'n', 'sk': 'n2'}"}
        ]);
        Case {
            tool: |s, v| s.execute_transaction_partiql(Parameters(wire(v))),
            args: json!({"transact_statements": statements}),
            action: |db, v| serde_json::to_value(db.execute_transaction(wire(v)).unwrap()).unwrap(),
            request: json!({"TransactStatements": statements}),
        }
        .check();
    }

    /// The PartiQL tools list a bad mode with the other member errors, as the
    /// wire and library do, rather than refusing it on its own first (the
    /// orders captured eu-west-2 and us-east-1, 2026-10-10: on ExecuteStatement
    /// the statement before the mode).
    #[test]
    fn partiql_tools_list_the_mode_with_the_other_member_errors() {
        let server = server();
        let long = format!("SELECT * FROM \"Cap\" WHERE pk = '{}'", "x".repeat(9_000));
        let (is_error, body) = reply(server.execute_partiql(Parameters(wire(json!({
            "statement": long,
            "return_consumed_capacity": "BOGUS"
        })))));
        assert!(is_error, "the statement was accepted: {body}");
        let message = body["message"].as_str().unwrap_or_default();
        assert!(
            message.starts_with("2 validation errors detected: Value '"),
            "the statement listed {message}"
        );
        assert!(message.contains("' at 'statement' failed to satisfy constraint"));
        assert!(message.ends_with(
            "Value 'BOGUS' at 'returnConsumedCapacity' failed to satisfy constraint: \
             Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"
        ));

        let statements: Vec<Value> = (0..26)
            .map(|_| json!({"Statement": "SELECT * FROM \"Cap\" WHERE pk = 'p'"}))
            .collect();
        let (is_error, body) = reply(server.batch_execute_partiql(Parameters(wire(json!({
            "statements": statements,
            "return_consumed_capacity": "BOGUS"
        })))));
        assert!(is_error, "the batch was accepted: {body}");
        let message = body["message"].as_str().unwrap_or_default();
        assert!(
            message.starts_with(
                "2 validation errors detected: Value 'BOGUS' at 'returnConsumedCapacity'"
            ),
            "the batch listed {message}"
        );
        assert!(message.contains("' at 'statements' failed to satisfy constraint"));

        let long = format!("SELECT * FROM \"Cap\" WHERE pk = '{}'", "x".repeat(9_000));
        let (is_error, body) = reply(server.execute_transaction_partiql(Parameters(wire(json!({
            "transact_statements": [{"Statement": long}],
            "client_request_token": "t".repeat(40),
            "return_consumed_capacity": "BOGUS"
        })))));
        assert!(is_error, "the transaction was accepted: {body}");
        let message = body["message"].as_str().unwrap_or_default();
        assert!(
            message.starts_with("3 validation errors detected: Value '"),
            "the transaction listed {message}"
        );
        assert!(message.contains("' at 'clientRequestToken'"));
        assert!(message.contains("' at 'transactStatements.1.member.statement'"));
        assert!(message.ends_with(
            "Value 'BOGUS' at 'returnConsumedCapacity' failed to satisfy constraint: \
             Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"
        ));
    }

    #[test]
    fn a_refused_mode_writes_nothing() {
        let server = server();
        let key = json!({"pk": {"S": "p"}, "sk": {"S": "w1"}});
        let mut item = key.clone();
        item["g"] = json!({"S": "gw"});
        let attempts: [(ToolCall, Value); 3] = [
            (
                |s, v| s.put_item(Parameters(wire(v))),
                json!({"table_name": "Cap", "item": item}),
            ),
            (
                |s, v| s.batch_write_item(Parameters(wire(v))),
                json!({"request_items": {"Cap": [{"PutRequest": {"Item": item}}]}}),
            ),
            (
                |s, v| s.transact_write_items(Parameters(wire(v))),
                json!({"transact_items": [{"Put": {"TableName": "Cap", "Item": item}}]}),
            ),
        ];
        for (tool, mut args) in attempts {
            args["return_consumed_capacity"] = json!("BOGUS");
            let (is_error, body) = reply(tool(&server, args));
            assert!(is_error, "BOGUS was accepted: {body}");
            assert_eq!(body["message"], BOGUS_MODE);
            let (_, got) = reply(server.get_item(Parameters(wire(json!({
                "table_name": "Cap", "key": key
            })))));
            assert!(got.get("Item").is_none(), "a refused request wrote {got}");
        }
    }

    #[test]
    fn the_mode_is_case_sensitive() {
        let (is_error, body) = get_item_case(false).call(&server(), Some("total"));
        assert!(is_error, "lower-case total was accepted: {body}");
        assert_eq!(body["message"], BOGUS_MODE.replace("BOGUS", "total"));
    }

    #[test]
    fn a_read_only_server_refuses_a_write_before_checking_the_mode() {
        let config = McpConfig {
            read_only: true,
            ..McpConfig::default()
        };
        let read_only = McpServer::with_config(Arc::new(seeded()), config);
        let (is_error, body) = put_item_case().call(&read_only, Some("BOGUS"));
        assert!(is_error);
        assert_eq!(body["error_type"], "AccessDeniedException", "{body}");
    }

    #[test]
    fn a_read_only_server_still_reports_select_capacity() {
        let config = McpConfig {
            read_only: true,
            ..McpConfig::default()
        };
        let read_only = McpServer::with_config(Arc::new(seeded()), config);
        let (is_error, body) = reply(read_only.execute_partiql(Parameters(wire(json!({
            "statement": "SELECT * FROM \"Cap\" WHERE pk = 'p'",
            "return_consumed_capacity": "TOTAL"
        })))));
        assert!(!is_error, "{body}");
        assert!(
            body["ConsumedCapacity"].is_object(),
            "no capacity in {body}"
        );
    }

    #[test]
    fn a_failed_condition_carries_no_capacity() {
        let (is_error, body) = reply(server().update_item(Parameters(wire(json!({
            "table_name": "Cap",
            "key": {"pk": {"S": "p"}, "sk": {"S": "s1"}},
            "update_expression": "SET g = :g",
            "condition_expression": "attribute_not_exists(pk)",
            "expression_attribute_values": {":g": {"S": "g3"}},
            "return_consumed_capacity": "TOTAL"
        })))));
        assert!(is_error, "{body}");
        assert_eq!(body["error_type"], "ConditionalCheckFailedException");
        assert!(body.get("ConsumedCapacity").is_none(), "{body}");
    }
}
