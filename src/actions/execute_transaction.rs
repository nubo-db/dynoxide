use crate::actions::helpers;
use crate::actions::index_capacity::{
    WriteCapacity, aggregate_by_table, per_table_capacity, transactional_read_units,
};
use crate::errors::{CancellationReason, DynoxideError, Result};
use crate::partiql;
use crate::partiql::executor::ResolvedTable;
use crate::storage_backend::StorageBackend;
use crate::types::{AttributeValue, Item};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

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

/// A first-call result together with what a same-token replay needs to bill it.
///
/// The replay is charged against the image each statement touched, which the
/// statement text cannot supply: a `DELETE` names a key, not the row it
/// removed. Those sizes are internal bookkeeping, so they live here, on the type
/// the idempotency cache holds, rather than on the response type callers see.
#[derive(Debug, Clone, Default)]
pub(crate) struct CachedTransaction {
    pub(crate) response: ExecuteTransactionResponse,
    /// Per-statement `(table, image size)`.
    pub(crate) replay_sizes: Vec<(String, usize)>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ItemResponse {
    #[serde(rename = "Item", skip_serializing_if = "Option::is_none")]
    pub item: Option<Item>,
}

/// Run a PartiQL transaction.
///
/// Callers driving idempotency want [`execute_cached`], which also hands back
/// the sizes a replay is billed against.
pub async fn execute<S: StorageBackend>(
    storage: &S,
    request: ExecuteTransactionRequest,
) -> Result<ExecuteTransactionResponse> {
    Ok(execute_cached(storage, request).await?.response)
}

pub(crate) async fn execute_cached<S: StorageBackend>(
    storage: &S,
    request: ExecuteTransactionRequest,
) -> Result<CachedTransaction> {
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
    for (index, stmt) in statements.iter().enumerate() {
        // A COUNT projection is rejected before parsing, at the same top level
        // as a parse error but with the bare message captured on
        // ExecuteStatement, not the wasn't-well-formed wrapper.
        if let Some(msg) = partiql::parser::count_projection_rejection(&stmt.statement) {
            return Err(DynoxideError::ValidationException(msg));
        }
        let ast = partiql::parser::parse(&stmt.statement)
            .map_err(|e| DynoxideError::ValidationException(e.into_message()))?;
        // DynamoDB rejects a RETURNING clause on any member of a transaction with
        // a top-level ValidationException, before applying any write. This is a
        // plain validation failure, not a TransactionCanceledException.
        if partiql::parser::returning_variant(&ast).is_some() {
            return Err(DynoxideError::ValidationException(format!(
                "Validation failed in TransactStatements[{index}]: RETURNING clause is not supported in ExecuteTransaction."
            )));
        }
        // An index-qualified read is rejected outright inside a transaction, so
        // there is no arm for it to be charged to. Rejected up front like the
        // RETURNING case, not as a cancellation. Captured eu-west-2 2026-08-15.
        if matches!(ast, partiql::parser::Statement::Select { .. })
            && partiql::parser::index_name(&ast).is_some()
        {
            return Err(DynoxideError::ValidationException(format!(
                "Validation failed in TransactStatements[{index}]: Reads on indices are not supported within transactions."
            )));
        }
        let params = stmt.parameters.clone().unwrap_or_default();
        parsed.push((ast, params));
    }

    // A transaction is all-read or all-write, and may not touch one item twice.
    // AWS rejects both up front, before any statement runs.
    let reads = parsed
        .iter()
        .filter(|(stmt, _)| matches!(stmt, partiql::parser::Statement::Select { .. }))
        .count();
    if reads > 0 && reads < parsed.len() {
        return Err(DynoxideError::ValidationException(
            "ExecuteTransaction API does not support both read and write operations in the same request."
                .to_string(),
        ));
    }

    // Duplicate detection resolves each statement's table, and the executor
    // needs the same metadata and key schema a moment later, so they are kept
    // here rather than resolved twice per statement.
    let mut seen_targets = HashSet::new();
    // Keyed by table rather than by statement: the metadata and key schema are
    // per table, and only the key is per statement.
    let mut tables: HashMap<String, ResolvedTable> = HashMap::new();
    for (stmt, params) in &parsed {
        let Some(name) = partiql::parser::table_name(stmt) else {
            continue;
        };
        if !tables.contains_key(name)
            && let Ok(resolved) = ResolvedTable::load(storage, name).await
        {
            tables.insert(name.to_string(), resolved);
        }
        // Absent when the table could not be read, which leaves the statement to
        // fail on its own terms during execution rather than as a duplicate.
        let Some(resolved) = tables.get(name) else {
            continue;
        };
        if let Some(target) = partiql::executor::statement_target_in(stmt, params, name, resolved)
            && !seen_targets.insert(target)
        {
            return Err(DynoxideError::ValidationException(
                "Transaction request cannot include multiple operations on one item".to_string(),
            ));
        }
    }

    // All statements run inside one SQLite transaction (all-or-nothing).
    let (responses, charges) = helpers::with_write_transaction(
        storage,
        execute_within_transaction(
            storage,
            &parsed,
            &tables,
            request.return_consumed_capacity.as_deref(),
        ),
    )
    .await?;

    // Transactional capacity, split by statement kind: an all-SELECT read set
    // reports read capacity, any INSERT/UPDATE/DELETE makes it a write set. Both
    // are sized on the items each statement touched, and the transactional
    // factor reaches the base table arm only.
    let mode = &request.return_consumed_capacity;
    let consumed_capacity = if is_read_set(&parsed) {
        crate::types::build_transactional_capacity(
            &transactional_read_units(&replay_sizes(&charges)),
            mode,
            crate::types::transactional_read_capacity,
        )
    } else {
        build_write_capacity(&charges, mode)
    };

    Ok(CachedTransaction {
        response: ExecuteTransactionResponse {
            responses: Some(responses),
            consumed_capacity,
        },
        replay_sizes: replay_sizes(&charges),
    })
}

/// What one statement contributed to the transaction's capacity.
enum StatementCharge {
    /// A `SELECT`, charged on the rows it walked at read granularity.
    Read { table_name: String, size: usize },
    /// A write, charged on its images and its per-index units.
    Write(WriteCapacity),
}

impl StatementCharge {
    fn table_name(&self) -> &str {
        match self {
            Self::Read { table_name, .. } => table_name,
            Self::Write(capacity) => &capacity.table_name,
        }
    }

    /// The image size this statement is charged on, for a replay.
    fn size(&self) -> usize {
        match self {
            Self::Read { size, .. } => *size,
            Self::Write(capacity) => capacity
                .old_size
                .unwrap_or(0)
                .max(capacity.new_size.unwrap_or(0)),
        }
    }

    /// The write record for this statement.
    ///
    /// A mixed set is rejected before execution, so a `Read` charge never
    /// reaches the write path in practice. It maps to a table-only record
    /// rather than panicking, so a future relaxation of that check degrades to
    /// an over-estimate instead of a wrong shape.
    fn as_write(&self) -> WriteCapacity {
        match self {
            Self::Write(capacity) => capacity.clone(),
            Self::Read { table_name, size } => WriteCapacity::new(
                table_name,
                None,
                Some(*size),
                HashMap::new(),
                HashMap::new(),
            ),
        }
    }
}

/// Fold the per-statement records into one `ConsumedCapacity` per table.
fn build_write_capacity(
    charges: &[StatementCharge],
    mode: &Option<String>,
) -> Option<Vec<crate::types::ConsumedCapacity>> {
    // Checked before the clone, not just inside the shared builder. Most calls
    // ask for no capacity, and `as_write` clones every record and its index
    // maps before the builder could discard the lot.
    if !matches!(mode.as_deref(), Some("TOTAL") | Some("INDEXES")) {
        return None;
    }

    let records: Vec<WriteCapacity> = charges.iter().map(StatementCharge::as_write).collect();
    let by_table = aggregate_by_table(&records, crate::types::TRANSACTIONAL_CAPACITY_FACTOR);
    per_table_capacity(
        &by_table,
        mode,
        crate::types::transactional_write_capacity_with_indexes,
    )
}

/// The image size each statement was charged on, kept for a same-token replay.
fn replay_sizes(charges: &[StatementCharge]) -> Vec<(String, usize)> {
    charges
        .iter()
        .map(|charge| (charge.table_name().to_string(), charge.size()))
        .collect()
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

/// Build the response for a same-token idempotent replay. The statements are
/// identical to the first call (the idempotency hash matched), so `Responses`
/// carry over from the cached first call and capacity is reported as a
/// transactional READ, honouring the replay request's own
/// `ReturnConsumedCapacity` mode (the original call's mode does not carry over).
/// The statements are re-parsed to recover per-table units; they parsed
/// successfully on the first call, so an unexpected parse error just drops that
/// statement from the estimate rather than failing the replay.
pub(crate) fn replay_response(
    cached: &CachedTransaction,
    mode: &Option<String>,
) -> CachedTransaction {
    CachedTransaction {
        response: ExecuteTransactionResponse {
            responses: cached.response.responses.clone(),
            consumed_capacity: crate::types::build_transactional_capacity(
                &transactional_read_units(&cached.replay_sizes),
                mode,
                crate::types::transactional_read_capacity,
            ),
        },
        replay_sizes: cached.replay_sizes.clone(),
    }
}

/// Run every statement, returning the responses and what each contributed to
/// `ConsumedCapacity`. The charges are only meaningful when the whole
/// transaction commits; a cancellation returns an error and reports nothing.
async fn execute_within_transaction<S: StorageBackend>(
    storage: &S,
    parsed: &[(partiql::parser::Statement, Vec<AttributeValue>)],
    tables: &HashMap<String, ResolvedTable>,
    capacity_mode: Option<&str>,
) -> Result<(Vec<ItemResponse>, Vec<StatementCharge>)> {
    let mut responses = Vec::with_capacity(parsed.len());
    let mut charges: Vec<StatementCharge> = Vec::with_capacity(parsed.len());
    let mut cancellation_reasons: Vec<CancellationReason> = Vec::with_capacity(parsed.len());

    for (stmt, params) in parsed {
        match partiql::executor::execute_page(
            storage,
            stmt,
            params,
            None,
            None,
            false,
            capacity_mode,
            partiql::parser::table_name(stmt).and_then(|t| tables.get(t)),
        )
        .await
        {
            Ok(page) => {
                let table_name = partiql::parser::table_name(stmt).unwrap_or_default();
                charges.push(match page.capacity {
                    Some(capacity) => StatementCharge::Write(capacity),
                    None => StatementCharge::Read {
                        table_name: table_name.to_string(),
                        size: page.size,
                    },
                });
                let item = page.items.and_then(|items| items.into_iter().next());
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

    Ok((responses, charges))
}
