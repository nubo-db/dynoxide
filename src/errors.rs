use serde::Serialize;
use std::collections::HashMap;
use std::fmt;

/// Per-item cancellation reason in a `TransactionCanceledException` response.
///
/// Real DynamoDB returns one reason per `TransactItem`, with `Code: "None"` for
/// items that would have succeeded.
#[derive(Debug, Clone, Default, Serialize)]
pub struct CancellationReason {
    #[serde(rename = "Code")]
    pub code: String,
    #[serde(rename = "Message", skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(rename = "Item", skip_serializing_if = "Option::is_none")]
    pub item: Option<HashMap<String, crate::types::AttributeValue>>,
}

/// DynamoDB error types.
///
/// Each variant corresponds to a DynamoDB API error, carrying a human-readable
/// message that matches DynamoDB's actual error messages.
#[derive(Debug, thiserror::Error)]
pub enum DynoxideError {
    /// Table or resource not found.
    #[error("{0}")]
    ResourceNotFoundException(String),

    /// Table or resource already exists / is in use.
    #[error("{0}")]
    ResourceInUseException(String),

    /// Input validation failed.
    #[error("{0}")]
    ValidationException(String),

    /// Conditional check (ConditionExpression) failed on write.
    /// Optionally carries the existing item when `ReturnValuesOnConditionCheckFailure` is `ALL_OLD`.
    #[error("{0}")]
    ConditionalCheckFailedException(
        String,
        Option<HashMap<String, crate::types::AttributeValue>>,
    ),

    /// One or more transaction conditions failed.
    /// Carries the message and per-item cancellation reasons.
    #[error("{0}")]
    TransactionCanceledException(String, Vec<CancellationReason>),

    /// Item collection exceeded size limit (10 GB per partition key value).
    #[error("{0}")]
    ItemCollectionSizeLimitExceededException(String),

    /// Duplicate primary key on PartiQL INSERT (distinct from ConditionalCheckFailedException).
    #[error("{0}")]
    DuplicateItemException(String),

    /// Throughput exceeded (stored but not enforced — included for API fidelity).
    #[error("{0}")]
    ProvisionedThroughputExceededException(String),

    /// Request body deserialisation failed (malformed JSON, wrong types).
    #[error("{0}")]
    SerializationException(String),

    /// Too many concurrent operations or index updates.
    #[error("{0}")]
    LimitExceededException(String),

    /// Access denied (e.g. non-existent resource ARN in tag operations).
    #[error("{0}")]
    AccessDeniedException(String),

    /// Idempotent request token reused with different request content.
    #[error("{0}")]
    IdempotentParameterMismatchException(String),

    /// Catch-all for internal / unexpected errors (SQLite failures, etc.).
    #[error("{0}")]
    InternalServerError(String),

    /// Type conversion error (e.g. wrong AttributeValue variant).
    #[error("Conversion error: {0}")]
    ConversionError(#[from] crate::types::ConversionError),

    /// SQLite error (converted from rusqlite).
    #[error("Internal error: {0}")]
    SqliteError(#[from] rusqlite::Error),
}

impl DynoxideError {
    /// Returns the DynamoDB `__type` string for this error.
    pub fn error_type(&self) -> &'static str {
        match self {
            DynoxideError::ResourceNotFoundException(_) => {
                "com.amazonaws.dynamodb.v20120810#ResourceNotFoundException"
            }
            DynoxideError::ResourceInUseException(_) => {
                "com.amazonaws.dynamodb.v20120810#ResourceInUseException"
            }
            DynoxideError::ValidationException(_) => {
                "com.amazon.coral.validate#ValidationException"
            }
            DynoxideError::ConditionalCheckFailedException(..) => {
                "com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException"
            }
            DynoxideError::TransactionCanceledException(..) => {
                "com.amazonaws.dynamodb.v20120810#TransactionCanceledException"
            }
            DynoxideError::DuplicateItemException(_) => {
                "com.amazonaws.dynamodb.v20120810#DuplicateItemException"
            }
            DynoxideError::ItemCollectionSizeLimitExceededException(_) => {
                "com.amazonaws.dynamodb.v20120810#ItemCollectionSizeLimitExceededException"
            }
            DynoxideError::ProvisionedThroughputExceededException(_) => {
                "com.amazonaws.dynamodb.v20120810#ProvisionedThroughputExceededException"
            }
            DynoxideError::SerializationException(_) => {
                "com.amazon.coral.service#SerializationException"
            }
            DynoxideError::LimitExceededException(_) => {
                "com.amazonaws.dynamodb.v20120810#LimitExceededException"
            }
            DynoxideError::AccessDeniedException(_) => {
                "com.amazonaws.dynamodb.v20120810#AccessDeniedException"
            }
            DynoxideError::IdempotentParameterMismatchException(_) => {
                "com.amazonaws.dynamodb.v20120810#IdempotentParameterMismatchException"
            }
            DynoxideError::ConversionError(_) => "com.amazon.coral.validate#ValidationException",
            DynoxideError::InternalServerError(_) | DynoxideError::SqliteError(_) => {
                "com.amazonaws.dynamodb.v20120810#InternalServerError"
            }
        }
    }

    /// Returns the short error code used in `BatchExecuteStatement` per-statement errors.
    ///
    /// These are the short-form codes that DynamoDB uses in `BatchStatementError.Code`,
    /// as opposed to the fully qualified `__type` strings from `error_type()`.
    pub fn short_error_code(&self) -> &'static str {
        match self {
            DynoxideError::ResourceNotFoundException(_) => "ResourceNotFound",
            DynoxideError::ResourceInUseException(_) => "ResourceInUse",
            DynoxideError::ValidationException(_) | DynoxideError::ConversionError(_) => {
                "ValidationError"
            }
            DynoxideError::ConditionalCheckFailedException(..) => "ConditionalCheckFailed",
            DynoxideError::TransactionCanceledException(..) => "TransactionConflict",
            DynoxideError::DuplicateItemException(_) => "DuplicateItem",
            DynoxideError::ItemCollectionSizeLimitExceededException(_) => {
                "ItemCollectionSizeLimitExceeded"
            }
            DynoxideError::ProvisionedThroughputExceededException(_) => {
                "ProvisionedThroughputExceeded"
            }
            DynoxideError::AccessDeniedException(_) => "AccessDenied",
            DynoxideError::IdempotentParameterMismatchException(_) => "IdempotentParameterMismatch",
            DynoxideError::SerializationException(_) => "SerializationError",
            DynoxideError::LimitExceededException(_) => "RequestLimitExceeded",
            DynoxideError::InternalServerError(_) | DynoxideError::SqliteError(_) => {
                "InternalServerError"
            }
        }
    }

    /// Returns the HTTP status code for this error.
    pub fn status_code(&self) -> u16 {
        match self {
            DynoxideError::InternalServerError(_) | DynoxideError::SqliteError(_) => 500,
            _ => 400,
        }
    }

    /// Convert to a DynamoDB-compatible JSON error response body.
    pub fn to_response(&self) -> ErrorResponse {
        let item = if let DynoxideError::ConditionalCheckFailedException(_, item) = self {
            item.clone()
        } else {
            None
        };
        ErrorResponse {
            error_type: self.error_type().to_string(),
            message: self.to_string(),
            item,
        }
    }

    /// Serialise to DynamoDB-compatible JSON string.
    ///
    /// `SerializationException` and `TransactionCanceledException` use
    /// `Message` (capital M) while all other errors use `message` (lowercase),
    /// matching real DynamoDB behaviour.
    pub fn to_json(&self) -> String {
        let error_type = self.error_type();
        let message = self.to_string();

        match self {
            DynoxideError::TransactionCanceledException(_, reasons) => {
                let mut m = serde_json::Map::new();
                m.insert(
                    "__type".to_string(),
                    serde_json::Value::String(error_type.to_string()),
                );
                m.insert("Message".to_string(), serde_json::Value::String(message));
                if let Ok(reasons_val) = serde_json::to_value(reasons) {
                    m.insert("CancellationReasons".to_string(), reasons_val);
                }
                serde_json::to_string(&m).unwrap_or_default()
            }
            DynoxideError::SerializationException(_) => {
                let mut m = serde_json::Map::new();
                m.insert(
                    "__type".to_string(),
                    serde_json::Value::String(error_type.to_string()),
                );
                m.insert("Message".to_string(), serde_json::Value::String(message));
                serde_json::to_string(&m).unwrap_or_default()
            }
            _ => {
                let resp = self.to_response();
                serde_json::to_string(&resp).unwrap_or_default()
            }
        }
    }
}

/// DynamoDB JSON error response body.
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    #[serde(rename = "__type")]
    pub error_type: String,
    #[serde(rename = "message")]
    pub message: String,
    #[serde(rename = "Item", skip_serializing_if = "Option::is_none")]
    pub item: Option<HashMap<String, crate::types::AttributeValue>>,
}

impl fmt::Display for ErrorResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", serde_json::to_string(self).unwrap_or_default())
    }
}

/// Convenience alias.
pub type Result<T> = std::result::Result<T, DynoxideError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_response_format() {
        let err = DynoxideError::ResourceNotFoundException(
            "Requested resource not found: Table: NonExistent not found".to_string(),
        );
        let resp = err.to_response();
        let json = serde_json::to_string(&resp).unwrap();

        assert!(json.contains("\"__type\""));
        assert!(json.contains("ResourceNotFoundException"));
        assert!(json.contains("NonExistent not found"));
    }

    #[test]
    fn test_status_codes() {
        assert_eq!(
            DynoxideError::ResourceNotFoundException("".into()).status_code(),
            400
        );
        assert_eq!(
            DynoxideError::ResourceInUseException("".into()).status_code(),
            400
        );
        assert_eq!(
            DynoxideError::ValidationException("".into()).status_code(),
            400
        );
        assert_eq!(
            DynoxideError::ConditionalCheckFailedException("".into(), None).status_code(),
            400
        );
        assert_eq!(
            DynoxideError::TransactionCanceledException("".into(), vec![]).status_code(),
            400
        );
        assert_eq!(
            DynoxideError::InternalServerError("".into()).status_code(),
            500
        );
    }

    #[test]
    fn test_error_type_strings() {
        let err = DynoxideError::ValidationException("bad input".into());
        assert_eq!(
            err.error_type(),
            "com.amazon.coral.validate#ValidationException"
        );
    }

    #[test]
    fn test_sqlite_error_maps_to_internal() {
        let sqlite_err = rusqlite::Error::QueryReturnedNoRows;
        let err = DynoxideError::from(sqlite_err);
        assert_eq!(err.status_code(), 500);
        assert!(err.error_type().contains("InternalServerError"));
    }

    #[test]
    fn test_error_response_json_structure() {
        let err = DynoxideError::ValidationException("1 validation error detected".to_string());
        let resp = err.to_response();
        let json: serde_json::Value = serde_json::to_value(&resp).unwrap();

        assert!(json.get("__type").is_some());
        assert!(json.get("message").is_some());
        assert_eq!(
            json["__type"],
            "com.amazon.coral.validate#ValidationException"
        );
        assert_eq!(json["message"], "1 validation error detected");
    }

    #[test]
    fn test_short_error_codes() {
        assert_eq!(
            DynoxideError::ResourceNotFoundException("".into()).short_error_code(),
            "ResourceNotFound"
        );
        assert_eq!(
            DynoxideError::ValidationException("".into()).short_error_code(),
            "ValidationError"
        );
        assert_eq!(
            DynoxideError::ConditionalCheckFailedException("".into(), None).short_error_code(),
            "ConditionalCheckFailed"
        );
        assert_eq!(
            DynoxideError::DuplicateItemException("".into()).short_error_code(),
            "DuplicateItem"
        );
        assert_eq!(
            DynoxideError::InternalServerError("".into()).short_error_code(),
            "InternalServerError"
        );
    }

    #[test]
    fn test_transaction_cancelled_json_has_cancellation_reasons() {
        let reasons = vec![
            CancellationReason {
                code: "ConditionalCheckFailed".to_string(),
                message: Some("The conditional request failed".to_string()),
                item: None,
            },
            CancellationReason {
                code: "None".to_string(),
                message: None,
                item: None,
            },
        ];
        let err = DynoxideError::TransactionCanceledException(
            "Transaction cancelled, please refer cancellation reasons for specific reasons [ConditionalCheckFailed, None]".to_string(),
            reasons,
        );
        let json_str = err.to_json();
        let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        // CancellationReasons must be a top-level field
        assert!(json.get("CancellationReasons").is_some());
        let reasons = json["CancellationReasons"].as_array().unwrap();
        assert_eq!(reasons.len(), 2);
        assert_eq!(reasons[0]["Code"], "ConditionalCheckFailed");
        assert_eq!(reasons[1]["Code"], "None");

        // Uses capital Message (not lowercase)
        assert!(json.get("Message").is_some());
        assert!(json.get("message").is_none());
    }
}
