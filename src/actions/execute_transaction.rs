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

#[derive(Debug, Clone, Default)]
pub struct ExecuteTransactionRequest {
    pub transact_statements: Vec<ParameterizedStatement>,
    pub client_request_token: Option<String>,
    pub return_consumed_capacity: Option<String>,
}

impl<'de> Deserialize<'de> for ExecuteTransactionRequest {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        /// The request as it arrives on the wire.
        #[derive(Deserialize)]
        struct Wire {
            #[serde(rename = "TransactStatements", default)]
            transact_statements: Option<Vec<WireStatement>>,
            #[serde(rename = "ClientRequestToken", default)]
            client_request_token: Option<String>,
            #[serde(rename = "ReturnConsumedCapacity", default)]
            return_consumed_capacity: Option<String>,
        }
        /// A [`ParameterizedStatement`] as it arrives on the wire, with a
        /// refused parameter held back.
        #[derive(Deserialize)]
        struct WireStatement {
            #[serde(rename = "Statement")]
            statement: String,
            #[serde(
                rename = "Parameters",
                default,
                deserialize_with = "crate::types::deserialize_held_parameters"
            )]
            parameters: crate::types::HeldParameters,
            #[serde(rename = "ReturnValuesOnConditionCheckFailure", default)]
            return_values_on_condition_check_failure: Option<String>,
        }

        let wire = Wire::deserialize(deserializer)?;
        // A statement list left out is a member error of its own, listed
        // after the token and ahead of ReturnConsumedCapacity (captured
        // eu-west-2 and us-east-1, 2026-11-11).
        let Some(wire_statements) = wire.transact_statements else {
            let violations = violations_of(
                wire.client_request_token.as_deref(),
                None,
                wire.return_consumed_capacity.as_deref(),
            );
            let msg = crate::validation::format_validation_errors(&violations)
                .expect("a statement list left out is always a violation");
            return Err(serde::de::Error::custom(format!(
                "{}{msg}",
                crate::serde_errors::VALIDATION_MARKER
            )));
        };
        // DynamoDB walks every statement's parameters as one list, after the
        // request's member errors, so the first statement's refusal is the
        // one answered, and only when no member error is (captured
        // eu-west-2, 10 November 2026).
        let mut refusal = None;
        let mut transact_statements = Vec::with_capacity(wire_statements.len());
        for s in wire_statements {
            refusal = refusal.or(s.parameters.refusal);
            transact_statements.push(ParameterizedStatement {
                statement: s.statement,
                parameters: s.parameters.values,
                return_values_on_condition_check_failure: s
                    .return_values_on_condition_check_failure,
            });
        }
        let request = ExecuteTransactionRequest {
            transact_statements,
            client_request_token: wire.client_request_token,
            return_consumed_capacity: wire.return_consumed_capacity,
        };
        if let Some(refusal) = refusal {
            let violations = member_violations(&request);
            if let Some(msg) = crate::validation::format_validation_errors(&violations) {
                return Err(serde::de::Error::custom(format!(
                    "{}{msg}",
                    crate::serde_errors::VALIDATION_MARKER
                )));
            }
            return Err(serde::de::Error::custom(refusal));
        }
        Ok(request)
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct ParameterizedStatement {
    #[serde(rename = "Statement")]
    pub statement: String,
    #[serde(
        rename = "Parameters",
        default,
        deserialize_with = "crate::types::deserialize_parameters"
    )]
    pub parameters: Option<Vec<AttributeValue>>,
    /// `ALL_OLD` hands back the item this statement's UPDATE or DELETE failed
    /// its condition on, on the statement's cancellation reason; `NONE`, or
    /// leaving it out, does not.
    #[serde(rename = "ReturnValuesOnConditionCheckFailure", default)]
    pub return_values_on_condition_check_failure: Option<String>,
}

/// One statement as a `ClientRequestToken` match compares it.
#[derive(Serialize)]
pub(crate) struct TokenStatement<'a> {
    #[serde(rename = "Statement")]
    statement: &'a str,
    #[serde(rename = "Parameters")]
    parameters: Option<&'a [AttributeValue]>,
    #[serde(rename = "ReturnValuesOnConditionCheckFailure")]
    mode: &'a str,
}

/// The statements as the idempotency hash sees them: each one's text,
/// parameters and mode, and not `ReturnConsumedCapacity`, so a same-token call
/// differing only in that replays rather than mismatches. A mode left out is
/// `NONE`, so a same-token retry that leaves it out replays a call that sent
/// `NONE`, and one sending `ALL_OLD` mismatches (captured eu-west-2 and
/// us-east-1, 2026-10-10). The statements' own `Serialize` writes a left-out
/// mode as left out.
pub(crate) fn token_statements(statements: &[ParameterizedStatement]) -> Vec<TokenStatement<'_>> {
    statements
        .iter()
        .map(|s| TokenStatement {
            statement: &s.statement,
            parameters: s.parameters.as_deref(),
            mode: s
                .return_values_on_condition_check_failure
                .as_deref()
                .unwrap_or("NONE"),
        })
        .collect()
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
    #[serde(
        rename = "Item",
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::set_order::item_in_hash_order"
    )]
    // Number sets in Java hash-set order here, as DynamoDB returns them;
    // pinned by set_order's every_response_item_field test.
    pub item: Option<Item>,
}

/// Run a PartiQL transaction.
///
/// Callers driving idempotency want `execute_cached`, which also hands back
/// the sizes a replay is billed against.
pub async fn execute<S: StorageBackend>(
    storage: &S,
    request: ExecuteTransactionRequest,
) -> Result<ExecuteTransactionResponse> {
    Ok(execute_cached(storage, request).await?.response)
}

/// The statement list as DynamoDB echoes it in a constraint message: each
/// statement's Java `toString` (captured eu-west-2 and us-east-1, 2026-10-10;
/// some hosts print an object hash per statement instead, and the form most
/// give is the one followed). A statement's
/// `ReturnValuesOnConditionCheckFailure` is echoed as sent, `null` when left
/// out. Bounded by [`crate::actions::batch_write_item::ECHO_LIMIT`].
fn statements_echo(statements: &[ParameterizedStatement]) -> String {
    let mut out = crate::actions::batch_write_item::BoundedEcho::new();
    let _ = write_statements(&mut out, statements);
    out.finish()
}

fn write_statements(
    out: &mut dyn std::fmt::Write,
    statements: &[ParameterizedStatement],
) -> std::fmt::Result {
    out.write_str("[")?;
    for (i, statement) in statements.iter().enumerate() {
        if i > 0 {
            out.write_str(", ")?;
        }
        write!(
            out,
            "ParameterizedStatement(statement={}, parameters=",
            statement.statement
        )?;
        match statement.parameters {
            Some(ref parameters) => {
                out.write_str("[")?;
                for (j, parameter) in parameters.iter().enumerate() {
                    if j > 0 {
                        out.write_str(", ")?;
                    }
                    crate::actions::batch_write_item::write_java_attribute_value(out, parameter)?;
                }
                out.write_str("]")?;
            }
            None => out.write_str("null")?,
        }
        out.write_str(", returnValuesOnConditionCheckFailure=")?;
        out.write_str(
            statement
                .return_values_on_condition_check_failure
                .as_deref()
                .unwrap_or("null"),
        )?;
        out.write_str(")")?;
    }
    out.write_str("]")
}

/// The request's member violations, listed as DynamoDB lists them: the
/// token, then each statement's ReturnValuesOnConditionCheckFailure, text and
/// parameters by index, then ReturnConsumedCapacity (captured eu-west-2 and
/// us-east-1, 2026-10-10, the regions identical). Any of them refuses the
/// whole request before a statement is parsed or the idempotency cache is
/// consulted.
///
/// The statement list's own length (empty, or more than 100) is listed with
/// them, after the token and ahead of the statements, echoing the list: `[]`
/// when empty, each statement's Java `toString` when too long (captured
/// eu-west-2 and us-east-1, 2026-10-10; the empty list in six regions).
///
/// The token is refused empty as well as too long, first in the list either
/// way (captured eu-west-2 and us-east-1, 2026-11-11).
fn member_violations(request: &ExecuteTransactionRequest) -> Vec<String> {
    violations_of(
        request.client_request_token.as_deref(),
        Some(&request.transact_statements),
        request.return_consumed_capacity.as_deref(),
    )
}

/// [`member_violations`] for a request read off the wire, whose statement list
/// may have been left out: `None` lists it as null in the list's place.
fn violations_of(
    token: Option<&str>,
    statements: Option<&[ParameterizedStatement]>,
    return_consumed_capacity: Option<&str>,
) -> Vec<String> {
    let mut violations = Vec::new();
    violations.extend(crate::token_constraint_error(token));
    let Some(statements) = statements else {
        violations.push(
            "Value null at 'transactStatements' failed to satisfy constraint: Member must not \
             be null"
                .to_string(),
        );
        violations.extend(crate::validation::return_consumed_capacity_rejection(
            return_consumed_capacity,
        ));
        return violations;
    };
    if statements.is_empty() {
        violations.push(
            "Value '[]' at 'transactStatements' failed to satisfy constraint: Member must \
             have length greater than or equal to 1"
                .to_string(),
        );
    }
    if statements.len() > 100 {
        violations.push(format!(
            "Value '{}' at 'transactStatements' failed to satisfy constraint: Member must \
             have length less than or equal to 100",
            statements_echo(statements)
        ));
    }
    for (i, statement) in statements.iter().enumerate() {
        let member = format!("transactStatements.{}.member", i + 1);
        violations.extend(
            crate::validation::return_values_on_condition_check_failure_rejection(
                statement
                    .return_values_on_condition_check_failure
                    .as_deref(),
                &format!("{member}.returnValuesOnConditionCheckFailure"),
            ),
        );
        violations.extend(crate::partiql::statement_length_violation(
            &statement.statement,
            &format!("{member}.statement"),
        ));
        violations.extend(crate::partiql::parameters_violation(
            statement.parameters.as_deref(),
            &format!("{member}.parameters"),
        ));
    }
    violations.extend(crate::validation::return_consumed_capacity_rejection(
        return_consumed_capacity,
    ));
    violations
}

/// Refuse a request whose members DynamoDB refuses, every violation listed in
/// one error. The entry points call this ahead of the idempotency cache,
/// because a same-token replay never reaches [`execute_cached`] and the token
/// is keyed on the statements alone, so a replay may carry a mode or token
/// the first call never did.
pub(crate) fn check_members(request: &ExecuteTransactionRequest) -> Result<()> {
    match crate::partiql::member_rejection(&member_violations(request)) {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

pub(crate) async fn execute_cached<S: StorageBackend>(
    storage: &S,
    request: ExecuteTransactionRequest,
) -> Result<CachedTransaction> {
    // The entry points check the members ahead of the idempotency cache too;
    // this stays for callers that come here directly.
    check_members(&request)?;

    // The statement list's length is checked with the other members, so a
    // list that reaches here holds 1 to 100 statements.
    let statements = &request.transact_statements;

    // A parameter holding an empty attribute name is a top-level rejection,
    // before any statement is parsed (captured in ten regions).
    if statements
        .iter()
        .any(|s| crate::empty_names::parameters_have_empty_name(s.parameters.as_deref()))
    {
        return Err(crate::empty_names::bare());
    }

    // Every statement's parameters are checked before any statement is read,
    // used or not and ahead of a missing table, and a failure refuses the
    // whole request with the bare message (captured eu-west-2 and us-east-1,
    // 2026-10-09).
    for stmt in statements {
        for parameter in stmt.parameters.iter().flatten() {
            partiql::executor::check_parameter_value(parameter)?;
        }
    }

    // Parse all statements before executing any, to fail fast on syntax errors
    let mut parsed = Vec::with_capacity(statements.len());
    for (index, stmt) in statements.iter().enumerate() {
        // A parameter nested too deep fails the whole transaction at the top
        // level, before this statement is parsed, but after the statements
        // ahead of it have been checked.
        if crate::value_nesting::parameters_nest_too_deep(
            stmt.parameters.as_deref().unwrap_or_default(),
        ) {
            return Err(crate::value_nesting::deep_parameter_error());
        }
        // A call in a SELECT list is rejected before parsing, with the
        // statement's index in front of the bare message, not the
        // wasn't-well-formed wrapper (captured eu-west-2 and us-east-1,
        // 2026-10-09).
        if let Some(msg) = partiql::parser::count_projection_rejection(&stmt.statement) {
            return Err(DynoxideError::ValidationException(format!(
                "Validation failed in TransactStatements[{index}]: {msg}"
            )));
        }
        // A statement that does not parse, or carries a literal DynamoDB
        // refuses, is reported with its index in front, as the RETURNING and
        // index-read refusals below are (captured eu-west-2 and us-east-1,
        // 2026-10-09).
        let supplied = stmt.parameters.as_ref().map_or(0, Vec::len);
        let ast = partiql::parser::parse_with_parameters(&stmt.statement, supplied).map_err(
            |failure| {
                let message = match failure {
                    partiql::parser::ParseFailure::Statement(e) => e.into_message(),
                    partiql::parser::ParseFailure::Literal(message) => message,
                };
                DynoxideError::ValidationException(format!(
                    "Validation failed in TransactStatements[{index}]: {message}"
                ))
            },
        )?;
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
    for (index, (stmt, params)) in parsed.iter().enumerate() {
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
        // A key value no key can have in an UPDATE or DELETE refuses the
        // request; one of the wrong key type cancels it when it runs
        // (captured eu-west-2 and us-east-1, 2026-10-09).
        if let Some(partiql::executor::KeyFault::NotAKeyType(message)) =
            partiql::executor::write_key_fault(stmt, params, resolved)
        {
            return Err(DynoxideError::ValidationException(format!(
                "Validation failed in TransactStatements[{index}]: {message}"
            )));
        }
        // A SELECT is served as a GetItem, so it may name the primary key and
        // nothing else, once overlapping key conditions have been ruled out
        // (captured eu-west-2 and us-east-1, 2026-10-09).
        if matches!(stmt, partiql::parser::Statement::Select { .. })
            && let Err(e) = partiql::executor::reject_overlapping_select(stmt, params, resolved)
        {
            return Err(DynoxideError::ValidationException(format!(
                "Validation failed in TransactStatements[{index}]: {e}"
            )));
        }
        if matches!(stmt, partiql::parser::Statement::Select { .. })
            && matches!(
                partiql::executor::keyed_select(stmt, params, resolved),
                partiql::executor::KeyedSelect::NotByPrimaryKey
            )
        {
            return Err(DynoxideError::ValidationException(format!(
                "Validation failed in TransactStatements[{index}]: Select statements within \
                 ExecuteTransaction must specify the primary key in the where clause."
            )));
        }
        if let Some(target) = partiql::executor::statement_target_in(stmt, params, resolved)
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
            statements,
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

    // A transaction of writes answers with an empty `Responses` list, not one
    // empty entry per statement; a transaction of reads has one entry per
    // statement (captured eu-west-2, us-east-1, eu-west-1 and us-west-2,
    // 2026-10-09).
    let responses = if is_read_set(&parsed) {
        responses
    } else {
        Vec::new()
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
///
/// A cancellation lists every statement that failed, as DynamoDB's does
/// (captured eu-west-2 and us-east-1, 2026-10-10: `[ConditionalCheckFailed,
/// None, ConditionalCheckFailed]`, `[ConditionalCheckFailed, DuplicateItem]`),
/// so every statement runs even after one has failed. The SQLite transaction
/// is rolled back on the error, so nothing any of them wrote is kept.
///
/// A failure of the storage itself (a full disk, an I/O error) is the
/// exception: it stops the transaction there and is returned as it is. SQLite
/// may already have rolled the whole transaction back for it, and a statement
/// run after that would commit on its own.
async fn execute_within_transaction<S: StorageBackend>(
    storage: &S,
    statements: &[ParameterizedStatement],
    parsed: &[(partiql::parser::Statement, Vec<AttributeValue>)],
    tables: &HashMap<String, ResolvedTable>,
    capacity_mode: Option<&str>,
) -> Result<(Vec<ItemResponse>, Vec<StatementCharge>)> {
    let resolved_for = |stmt: &partiql::parser::Statement| {
        partiql::parser::table_name(stmt).and_then(|t| tables.get(t))
    };

    // A SELECT key of the wrong type, or of a type no key can have, and an
    // UPDATE or DELETE key of the wrong type, cancel the transaction with
    // GetItem's reason (captured eu-west-2 and us-east-1, 2026-10-09). They
    // are checked of every statement before any runs: when one has such a
    // key, every other statement is reported `None`, a statement whose
    // condition is false included (captured eu-west-2 and us-east-1,
    // 2026-10-10; inferred for the other failure kinds).
    let wrong_key_type: Vec<bool> = parsed
        .iter()
        .map(|(stmt, params)| {
            resolved_for(stmt).is_some_and(|resolved| match stmt {
                partiql::parser::Statement::Select { .. } => matches!(
                    partiql::executor::keyed_select(stmt, params, resolved),
                    partiql::executor::KeyedSelect::KeyType(_)
                ),
                _ => partiql::executor::write_key_fault(stmt, params, resolved).is_some(),
            })
        })
        .collect();
    if wrong_key_type.contains(&true) {
        return Err(cancelled(
            wrong_key_type
                .iter()
                .map(|&wrong| {
                    if wrong {
                        CancellationReason {
                            code: "ValidationError".to_string(),
                            message: Some(
                                "The provided key element does not match the schema".to_string(),
                            ),
                            item: None,
                        }
                    } else {
                        no_reason()
                    }
                })
                .collect(),
        ));
    }

    let mut responses = Vec::with_capacity(parsed.len());
    let mut charges: Vec<StatementCharge> = Vec::with_capacity(parsed.len());
    let mut cancellation_reasons: Vec<CancellationReason> = Vec::with_capacity(parsed.len());
    let mut failed = false;

    // `parsed` holds each statement of `statements`, in order.
    for ((stmt, params), written) in parsed.iter().zip(statements) {
        let resolved = resolved_for(stmt);
        // Whether the statement asked for the item a failed condition was
        // evaluated against.
        let old_on_failure =
            written.return_values_on_condition_check_failure.as_deref() == Some("ALL_OLD");
        // An INSERT key of the wrong type cancels the transaction with
        // PutItem's reason (captured eu-west-2 and us-east-1, 2026-10-09).
        let insert_keys = match resolved {
            Some(resolved) => {
                partiql::executor::check_transaction_insert_keys(stmt, params, resolved)
            }
            None => Ok(()),
        };
        let outcome = match insert_keys {
            Err(e) => Err(e),
            Ok(()) => {
                partiql::executor::execute_written_page(
                    storage,
                    &written.statement,
                    stmt,
                    params,
                    None,
                    None,
                    false,
                    capacity_mode,
                    resolved,
                    old_on_failure,
                )
                .await
            }
        };
        match outcome {
            Ok(page) => {
                // A DELETE with a condition, of an item that isn't there, does
                // nothing when the transaction commits, and is reported as a
                // failed condition, with no item, when it is cancelled
                // (captured eu-west-2 and us-east-1, 2026-10-10).
                let missed_condition = matches!(stmt, partiql::parser::Statement::Delete { .. })
                    && page
                        .capacity
                        .as_ref()
                        .is_some_and(|capacity| capacity.old_size.is_none())
                    && resolved.is_some_and(|resolved| {
                        partiql::executor::delete_has_condition(stmt, resolved)
                    });
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
                cancellation_reasons.push(if missed_condition {
                    CancellationReason {
                        code: "ConditionalCheckFailed".to_string(),
                        message: Some("The conditional request failed".to_string()),
                        item: None,
                    }
                } else {
                    no_reason()
                });
            }
            Err(e) => {
                let Some(code) = outcome_code(&e) else {
                    return Err(e);
                };
                failed = true;
                responses.push(ItemResponse { item: None });
                cancellation_reasons.push(failure_reason(code, e));
            }
        }
    }

    if failed {
        return Err(cancelled(cancellation_reasons));
    }
    Ok((responses, charges))
}

/// The reason a statement that ran without failing is given.
fn no_reason() -> CancellationReason {
    CancellationReason {
        code: "None".to_string(),
        message: None,
        item: None,
    }
}

/// The cancellation code for a statement that failed, or `None` when the
/// error is not the statement's outcome but a failure of the storage under it,
/// which stops the transaction instead.
fn outcome_code(e: &DynoxideError) -> Option<&'static str> {
    match e {
        DynoxideError::ConditionalCheckFailedException(..) => Some("ConditionalCheckFailed"),
        DynoxideError::DuplicateItemException(_) => Some("DuplicateItem"),
        // A statement against a table that does not exist cancels the
        // transaction with this reason (captured eu-west-2 and us-east-1,
        // 2026-10-09).
        DynoxideError::ResourceNotFoundException(_) => Some("ResourceNotFound"),
        // Group KeyEmptyValueValidation with ValidationException so an
        // empty-value key keeps the "ValidationError" reason (#95).
        DynoxideError::ValidationException(_) | DynoxideError::KeyEmptyValueValidation(_) => {
            Some("ValidationError")
        }
        _ => None,
    }
}

/// The reason a failed statement is given, under the code
/// [`outcome_code`] found for it, with the item its condition was evaluated
/// against when it asked for it.
fn failure_reason(code: &str, e: DynoxideError) -> CancellationReason {
    let message = Some(e.to_string());
    let item = match e {
        DynoxideError::ConditionalCheckFailedException(_, item) => item,
        _ => None,
    };
    CancellationReason {
        code: code.to_string(),
        message,
        item,
    }
}

/// The cancellation for these reasons, its message listing their codes.
fn cancelled(reasons: Vec<CancellationReason>) -> DynoxideError {
    let codes: Vec<&str> = reasons.iter().map(|r| r.code.as_str()).collect();
    let message = format!(
        "Transaction cancelled, please refer cancellation reasons for specific reasons [{}]",
        codes.join(", ")
    );
    DynoxideError::TransactionCanceledException(message, reasons)
}
