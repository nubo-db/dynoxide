use crate::errors::{DynoxideError, Result};
use crate::partiql;
use crate::storage_backend::StorageBackend;
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
    #[serde(rename = "ReturnConsumedCapacity", default)]
    pub return_consumed_capacity: Option<String>,
}

#[derive(Debug, Default, Serialize)]
pub struct ExecuteStatementResponse {
    #[serde(rename = "Items", skip_serializing_if = "Option::is_none")]
    pub items: Option<Vec<Item>>,
    #[serde(rename = "NextToken", skip_serializing_if = "Option::is_none")]
    pub next_token: Option<String>,
    #[serde(rename = "ConsumedCapacity", skip_serializing_if = "Option::is_none")]
    pub consumed_capacity: Option<crate::types::ConsumedCapacity>,
}

pub async fn execute<S: StorageBackend>(
    storage: &S,
    request: ExecuteStatementRequest,
) -> Result<ExecuteStatementResponse> {
    // Limit is checked before the statement is parsed. A zero limit would
    // otherwise read nothing and mint no token, silently ending a paginated
    // walk. The wording follows Scan's shape (value kept, lowercase 'limit'),
    // not Query's, matching what ExecuteStatement itself returns (captured
    // eu-west-2, 2026-07-29).
    if request.limit == Some(0) {
        return Err(DynoxideError::ValidationException(
            crate::validation::envelope_message(
                "Value '0' at 'limit' failed to satisfy constraint: \
                 Member must have value greater than or equal to 1",
            ),
        ));
    }

    let stmt = partiql::parser::parse(&request.statement).map_err(|e| {
        DynoxideError::ValidationException(format!(
            "Statement wasn't well formed, can't be processed: {e}"
        ))
    })?;

    let params = request.parameters.unwrap_or_default();
    let page = partiql::executor::execute_page(
        storage,
        &stmt,
        &params,
        request.limit,
        request.next_token.as_deref(),
    )
    .await?;
    let partiql::executor::StatementPage {
        items,
        size,
        next_token,
        ..
    } = page;

    // ConsumedCapacity is returned whenever ReturnConsumedCapacity is requested,
    // unlike some emulators that omit it. A SELECT is charged read units (an
    // eventually consistent read unless ConsistentRead is set); INSERT, UPDATE
    // and DELETE are charged write units. The unit (single object, not an array)
    // comes from the shared `types.rs` helpers.
    let consumed_capacity = partiql::parser::table_name(&stmt).and_then(|table| {
        let units = if matches!(stmt, partiql::parser::Statement::Select { .. }) {
            crate::types::read_capacity_units_with_consistency(
                size,
                request.consistent_read.unwrap_or(false),
            )
        } else {
            crate::types::write_capacity_units(size)
        };
        crate::types::consumed_capacity(table, units, &request.return_consumed_capacity)
    });

    Ok(ExecuteStatementResponse {
        items,
        next_token,
        consumed_capacity,
    })
}
