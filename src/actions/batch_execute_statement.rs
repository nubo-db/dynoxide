use crate::errors::{DynoxideError, Result};
use crate::partiql;
use crate::storage::Storage;
use crate::types::{AttributeValue, Item};
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Deserialize)]
pub struct BatchExecuteStatementRequest {
    #[serde(rename = "Statements")]
    pub statements: Vec<BatchStatementRequest>,
}

#[derive(Debug, Default, Deserialize)]
pub struct BatchStatementRequest {
    #[serde(rename = "Statement")]
    pub statement: String,
    #[serde(rename = "Parameters", default)]
    pub parameters: Option<Vec<AttributeValue>>,
}

#[derive(Debug, Default, Serialize)]
pub struct BatchExecuteStatementResponse {
    #[serde(rename = "Responses")]
    pub responses: Vec<BatchStatementResponse>,
}

#[derive(Debug, Default, Serialize)]
pub struct BatchStatementResponse {
    #[serde(rename = "Error", skip_serializing_if = "Option::is_none")]
    pub error: Option<BatchStatementError>,
    #[serde(rename = "Item", skip_serializing_if = "Option::is_none")]
    pub item: Option<Item>,
}

#[derive(Debug, Default, Serialize)]
pub struct BatchStatementError {
    #[serde(rename = "Code")]
    pub code: String,
    #[serde(rename = "Message")]
    pub message: String,
}

pub fn execute(
    storage: &Storage,
    request: BatchExecuteStatementRequest,
) -> Result<BatchExecuteStatementResponse> {
    if request.statements.is_empty() {
        return Err(DynoxideError::ValidationException(
            "1 validation error detected: Value '[]' at 'statements' failed to satisfy constraint: Member must have length greater than or equal to 1".to_string(),
        ));
    }

    if request.statements.len() > 25 {
        return Err(DynoxideError::ValidationException(
            "Too many statements in BatchExecuteStatement; limit is 25".to_string(),
        ));
    }

    let mut responses = Vec::with_capacity(request.statements.len());

    for stmt_req in &request.statements {
        let parsed = partiql::parser::parse(&stmt_req.statement);

        let response = match parsed {
            Err(e) => BatchStatementResponse {
                error: Some(BatchStatementError {
                    code: "ValidationException".to_string(),
                    message: format!("Statement wasn't well formed, got error: {e}"),
                }),
                item: None,
            },
            Ok(stmt) => {
                let params = stmt_req.parameters.as_deref().unwrap_or_default();
                match partiql::executor::execute(storage, &stmt, params, None) {
                    Ok(Some(items)) => {
                        // For SELECT, return first item (batch returns single item per statement)
                        BatchStatementResponse {
                            error: None,
                            item: items.into_iter().next(),
                        }
                    }
                    Ok(None) => BatchStatementResponse {
                        error: None,
                        item: None,
                    },
                    Err(e) => BatchStatementResponse {
                        error: Some(BatchStatementError {
                            code: e.short_error_code().to_string(),
                            message: e.to_string(),
                        }),
                        item: None,
                    },
                }
            }
        };

        responses.push(response);
    }

    Ok(BatchExecuteStatementResponse { responses })
}
