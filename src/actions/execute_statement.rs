use crate::errors::{DynoxideError, Result};
use crate::partiql;
use crate::storage::Storage;
use crate::types::{AttributeValue, Item};
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Deserialize)]
pub struct ExecuteStatementRequest {
    #[serde(rename = "Statement")]
    pub statement: String,
    #[serde(rename = "Parameters", default)]
    pub parameters: Option<Vec<AttributeValue>>,
    #[serde(rename = "Limit", default)]
    pub limit: Option<usize>,
    #[serde(rename = "NextToken", default)]
    pub next_token: Option<String>,
    /// Accepted for API compatibility. Has no behavioural effect — SQLite
    /// reads are always consistent.
    #[serde(rename = "ConsistentRead", default)]
    pub consistent_read: Option<bool>,
}

#[derive(Debug, Default, Serialize)]
pub struct ExecuteStatementResponse {
    #[serde(rename = "Items", skip_serializing_if = "Option::is_none")]
    pub items: Option<Vec<Item>>,
    #[serde(rename = "NextToken", skip_serializing_if = "Option::is_none")]
    pub next_token: Option<String>,
}

pub fn execute(
    storage: &Storage,
    request: ExecuteStatementRequest,
) -> Result<ExecuteStatementResponse> {
    let stmt = partiql::parser::parse(&request.statement).map_err(|e| {
        DynoxideError::ValidationException(format!("Statement wasn't well formed, got error: {e}"))
    })?;

    let params = request.parameters.unwrap_or_default();
    let result = partiql::executor::execute(storage, &stmt, &params, request.limit)?;

    Ok(ExecuteStatementResponse {
        items: result,
        next_token: None,
    })
}
