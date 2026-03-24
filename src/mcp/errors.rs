//! DynoxideError → MCP tool error mapping.
//!
//! Domain errors are returned as tool results with `isError: true` so the
//! agent conversation continues flowing. Protocol-level errors (McpError)
//! are reserved for infrastructure failures.

use crate::errors::DynoxideError;
use rmcp::model::{CallToolResult, Content};

/// Short error type name without the DynamoDB namespace prefix.
fn short_error_type(err: &DynoxideError) -> &'static str {
    match err {
        DynoxideError::ResourceNotFoundException(_) => "ResourceNotFoundException",
        DynoxideError::ResourceInUseException(_) => "ResourceInUseException",
        DynoxideError::ValidationException(_) => "ValidationException",
        DynoxideError::ConditionalCheckFailedException(..) => "ConditionalCheckFailedException",
        DynoxideError::TransactionCanceledException(..) => "TransactionCanceledException",
        DynoxideError::ItemCollectionSizeLimitExceededException(_) => {
            "ItemCollectionSizeLimitExceededException"
        }
        DynoxideError::ProvisionedThroughputExceededException(_) => {
            "ProvisionedThroughputExceededException"
        }
        DynoxideError::InternalServerError(_) | DynoxideError::SqliteError(_) => {
            "InternalServerError"
        }
        DynoxideError::ConversionError(_) => "ValidationException",
        DynoxideError::DuplicateItemException(_) => "DuplicateItemException",
        DynoxideError::AccessDeniedException(_) => "AccessDeniedException",
        DynoxideError::SerializationException(_) => "SerializationException",
        DynoxideError::LimitExceededException(_) => "LimitExceededException",
        DynoxideError::IdempotentParameterMismatchException(_) => {
            "IdempotentParameterMismatchException"
        }
    }
}

/// Whether this error type would be retryable against real DynamoDB.
///
/// This is informational — for a local emulator, the same request will produce
/// the same error. But agents building muscle memory for production should learn
/// which errors are transient vs permanent.
fn is_retryable(err: &DynoxideError) -> bool {
    matches!(
        err,
        DynoxideError::ProvisionedThroughputExceededException(_)
            | DynoxideError::InternalServerError(_)
            | DynoxideError::SqliteError(_)
    )
}

/// Convert a DynoxideError into an MCP tool result with `isError: true`.
pub fn to_tool_error(err: DynoxideError) -> CallToolResult {
    let error_json = serde_json::json!({
        "error_type": short_error_type(&err),
        "message": err.to_string(),
        "retryable": is_retryable(&err),
    });

    CallToolResult::error(vec![Content::text(error_json.to_string())])
}
