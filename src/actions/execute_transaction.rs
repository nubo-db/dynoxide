use crate::actions::helpers;
use crate::errors::{CancellationReason, DynoxideError, Result};
use crate::partiql;
use crate::storage_backend::StorageBackend;
use crate::types::{AttributeValue, Item};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Deserialize)]
pub struct ExecuteTransactionRequest {
    #[serde(rename = "TransactStatements")]
    pub transact_statements: Vec<ParameterizedStatement>,
    #[serde(rename = "ClientRequestToken", default)]
    pub client_request_token: Option<String>,
    #[serde(rename = "ReturnConsumedCapacity", default)]
    pub return_consumed_capacity: Option<String>,
}

// `Serialize` backs the idempotency request hash (the statements and their
// parameters are serialised via `serde_json`), so a same-token call differing
// only in `ReturnConsumedCapacity` replays rather than mismatches.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct ParameterizedStatement {
    #[serde(rename = "Statement")]
    pub statement: String,
    #[serde(rename = "Parameters", default)]
    pub parameters: Option<Vec<AttributeValue>>,
}

// `Clone` so the idempotency cache can store the first-call response and clone
// its `Responses` for the replay.
#[derive(Debug, Clone, Default, Serialize)]
pub struct ExecuteTransactionResponse {
    #[serde(rename = "Responses", skip_serializing_if = "Option::is_none")]
    pub responses: Option<Vec<ItemResponse>>,
    #[serde(rename = "ConsumedCapacity", skip_serializing_if = "Option::is_none")]
    pub consumed_capacity: Option<Vec<crate::types::ConsumedCapacity>>,
}

#[derive(Debug, Clone, Default, Serialize)]
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

    // Transactional capacity, split by statement kind: an all-SELECT read set
    // reports read capacity, any INSERT/UPDATE/DELETE makes it a write set.
    //
    // TODO: capacity is charged a flat per-statement transactional unit, not an
    // item-size computation, so it under-counts PartiQL items above 1KB (writes
    // round at 1KB, reads at 4KB). Correct for the small items conformance pins;
    // size-accurate rounding for large PartiQL statements is a tracked follow-up.
    let builder = if is_read_set(&parsed) {
        crate::types::transactional_read_capacity
    } else {
        crate::types::transactional_write_capacity
    };
    let consumed_capacity = build_transaction_capacity(
        &statement_table_units(parsed.iter().map(|(stmt, _)| stmt)),
        &request.return_consumed_capacity,
        builder,
    );

    Ok(ExecuteTransactionResponse {
        responses: Some(responses),
        consumed_capacity,
    })
}

/// A transaction is a read set only when every statement is a `SELECT`; any
/// `INSERT`/`UPDATE`/`DELETE` makes it a write set. AWS requires a transaction
/// to be all-read or all-write and rejects a mixed set before capacity is
/// computed, but dynoxide does not enforce that, so a mixed set is classified
/// here as a write set. Revisit the predicate if condition-only checks (which
/// AWS counts in the write set) are ever parsed.
fn is_read_set(parsed: &[(partiql::parser::Statement, Vec<AttributeValue>)]) -> bool {
    parsed
        .iter()
        .all(|(stmt, _)| matches!(stmt, partiql::parser::Statement::Select { .. }))
}

/// Per-table transactional units for a set of parsed statements. Each statement
/// costs the per-statement base (1 unit) doubled by the transactional factor,
/// summed by target table (matching `TransactWriteItems`, which doubles per
/// item). Item-size rounding is not applied here (see the TODO in `execute`).
fn statement_table_units<'a>(
    statements: impl Iterator<Item = &'a partiql::parser::Statement>,
) -> std::collections::HashMap<String, f64> {
    let mut table_units: std::collections::HashMap<String, f64> = std::collections::HashMap::new();
    for stmt in statements {
        if let Some(tbl) = partiql::parser::table_name(stmt) {
            *table_units.entry(tbl.to_string()).or_default() +=
                crate::types::TRANSACTIONAL_CAPACITY_FACTOR;
        }
    }
    table_units
}

/// Build the per-table `ConsumedCapacity` vec from the per-table units using
/// `builder` (write for a first-call write set, read for a read set or a
/// replay). Returns `None` unless `ReturnConsumedCapacity` is `TOTAL` or
/// `INDEXES`, so the mode guard lives in one place.
fn build_transaction_capacity(
    table_units: &std::collections::HashMap<String, f64>,
    mode: &Option<String>,
    builder: fn(&str, f64, &Option<String>) -> Option<crate::types::ConsumedCapacity>,
) -> Option<Vec<crate::types::ConsumedCapacity>> {
    if matches!(mode.as_deref(), Some("TOTAL") | Some("INDEXES")) {
        Some(
            table_units
                .iter()
                .filter_map(|(table, &units)| builder(table, units, mode))
                .collect(),
        )
    } else {
        None
    }
}

/// Build the response for a same-token idempotent replay. The statements are
/// identical to the first call (the idempotency hash matched), so `Responses`
/// carry over from the cached first call and capacity is reported as a
/// transactional READ, honouring the replay request's own
/// `ReturnConsumedCapacity` mode (the original call's mode does not carry over).
/// The statements are re-parsed to recover per-table units; they parsed
/// successfully on the first call, so an unexpected parse error just drops that
/// statement from the estimate rather than failing the replay.
pub(crate) fn replay_response(
    statements: &[ParameterizedStatement],
    mode: &Option<String>,
    cached_responses: Option<Vec<ItemResponse>>,
) -> ExecuteTransactionResponse {
    let parsed: Vec<partiql::parser::Statement> = statements
        .iter()
        .filter_map(|s| partiql::parser::parse(&s.statement).ok())
        .collect();
    ExecuteTransactionResponse {
        responses: cached_responses,
        consumed_capacity: build_transaction_capacity(
            &statement_table_units(parsed.iter()),
            mode,
            crate::types::transactional_read_capacity,
        ),
    }
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
                    // Group KeyEmptyValueValidation with ValidationException so an empty-value
                    // key keeps the "ValidationError" reason instead of falling through to
                    // InternalError (#95).
                    DynoxideError::ValidationException(_)
                    | DynoxideError::KeyEmptyValueValidation(_) => {
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
