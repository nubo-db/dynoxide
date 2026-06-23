use crate::actions::helpers;
use crate::errors::{CancellationReason, DynoxideError, Result};
use crate::partiql;
use crate::storage_backend::StorageBackend;
use crate::types::{AttributeValue, Item};
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Deserialize)]
pub struct ExecuteTransactionRequest {
    #[serde(rename = "TransactStatements")]
    pub transact_statements: Vec<ParameterizedStatement>,
    #[serde(rename = "ClientRequestToken", default)]
    pub client_request_token: Option<String>,
    #[serde(rename = "ReturnConsumedCapacity", default)]
    pub return_consumed_capacity: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
pub struct ParameterizedStatement {
    #[serde(rename = "Statement")]
    pub statement: String,
    #[serde(rename = "Parameters", default)]
    pub parameters: Option<Vec<AttributeValue>>,
}

#[derive(Debug, Default, Serialize)]
pub struct ExecuteTransactionResponse {
    #[serde(rename = "Responses", skip_serializing_if = "Option::is_none")]
    pub responses: Option<Vec<ItemResponse>>,
    #[serde(rename = "ConsumedCapacity", skip_serializing_if = "Option::is_none")]
    pub consumed_capacity: Option<Vec<crate::types::ConsumedCapacity>>,
}

#[derive(Debug, Default, Serialize)]
pub struct ItemResponse {
    #[serde(rename = "Item", skip_serializing_if = "Option::is_none")]
    pub item: Option<Item>,
}

pub async fn execute<S: StorageBackend>(
    storage: &S,
    request: ExecuteTransactionRequest,
) -> Result<ExecuteTransactionResponse> {
    let statements = &request.transact_statements;

    // Validate: must have between 1 and 100 statements
    if statements.is_empty() {
        return Err(DynoxideError::ValidationException(
            "1 validation error detected: Value at 'transactStatements' failed to satisfy constraint: Member must have length greater than or equal to 1".to_string(),
        ));
    }
    if statements.len() > 100 {
        return Err(DynoxideError::ValidationException(
            "Member must have length less than or equal to 100".to_string(),
        ));
    }

    // Parse all statements before executing any, to fail fast on syntax errors
    let mut parsed = Vec::with_capacity(statements.len());
    for stmt in statements {
        let ast = partiql::parser::parse(&stmt.statement).map_err(|e| {
            DynoxideError::ValidationException(format!(
                "Statement wasn't well formed, got error: {e}"
            ))
        })?;
        let params = stmt.parameters.clone().unwrap_or_default();
        parsed.push((ast, params));
    }

    // All statements run inside one SQLite transaction (all-or-nothing).
    let responses =
        helpers::with_write_transaction(storage, execute_within_transaction(storage, &parsed))
            .await?;

    // Build ConsumedCapacity if requested (simple estimate: 1 WCU per statement)
    let consumed_capacity = if matches!(
        request.return_consumed_capacity.as_deref(),
        Some("TOTAL") | Some("INDEXES")
    ) {
        // Aggregate capacity by table name from parsed statements
        let mut table_units: std::collections::HashMap<String, f64> =
            std::collections::HashMap::new();
        for (stmt, _) in &parsed {
            if let Some(tbl) = partiql::parser::table_name(stmt) {
                *table_units.entry(tbl.to_string()).or_default() += 1.0;
            }
        }
        let caps: Vec<_> = table_units
            .iter()
            .filter_map(|(table, &units)| {
                crate::types::consumed_capacity(table, units, &request.return_consumed_capacity)
            })
            .collect();
        Some(caps)
    } else {
        None
    };

    Ok(ExecuteTransactionResponse {
        responses: Some(responses),
        consumed_capacity,
    })
}

async fn execute_within_transaction<S: StorageBackend>(
    storage: &S,
    parsed: &[(partiql::parser::Statement, Vec<AttributeValue>)],
) -> Result<Vec<ItemResponse>> {
    let mut responses = Vec::with_capacity(parsed.len());
    let mut cancellation_reasons: Vec<CancellationReason> = Vec::with_capacity(parsed.len());

    for (stmt, params) in parsed {
        match partiql::executor::execute(storage, stmt, params, None).await {
            Ok(result) => {
                let item = result.and_then(|items| items.into_iter().next());
                responses.push(ItemResponse { item });
                cancellation_reasons.push(CancellationReason {
                    code: "None".to_string(),
                    message: None,
                    item: None,
                });
            }
            Err(e) => {
                // Record the failure reason
                let message = Some(e.to_string());
                let (code, item) = match e {
                    DynoxideError::ConditionalCheckFailedException(_, item) => {
                        ("ConditionalCheckFailed".to_string(), item)
                    }
                    DynoxideError::DuplicateItemException(_) => ("DuplicateItem".to_string(), None),
                    // Group KeyEmptyStringValidation with ValidationException so an empty-string
                    // key keeps the "ValidationError" reason instead of falling through to
                    // InternalError (#95).
                    DynoxideError::ValidationException(_)
                    | DynoxideError::KeyEmptyStringValidation(_) => {
                        ("ValidationError".to_string(), None)
                    }
                    _ => ("InternalError".to_string(), None),
                };
                responses.push(ItemResponse { item: None });
                cancellation_reasons.push(CancellationReason {
                    code,
                    message,
                    item,
                });

                // Fill remaining slots with None and stop — don't execute
                // statements that will be rolled back.
                for _ in responses.len()..parsed.len() {
                    responses.push(ItemResponse { item: None });
                    cancellation_reasons.push(CancellationReason {
                        code: "None".to_string(),
                        message: None,
                        item: None,
                    });
                }

                let codes: Vec<&str> = cancellation_reasons
                    .iter()
                    .map(|r| r.code.as_str())
                    .collect();
                let message = format!(
                    "Transaction cancelled, please refer cancellation reasons for specific reasons [{}]",
                    codes.join(", ")
                );
                return Err(DynoxideError::TransactionCanceledException(
                    message,
                    cancellation_reasons,
                ));
            }
        }
    }

    Ok(responses)
}
