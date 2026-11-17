use crate::actions::index_capacity::{WriteCapacity, aggregate_by_table, per_table_capacity};
use crate::errors::{DynoxideError, Result};
use crate::partiql;
use crate::partiql::executor::ResolvedTable;
use crate::storage_backend::StorageBackend;
use crate::types::{AttributeValue, Item};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Default, Deserialize)]
pub struct BatchExecuteStatementRequest {
    #[serde(rename = "Statements")]
    pub statements: Vec<BatchStatementRequest>,
    #[serde(rename = "ReturnConsumedCapacity", default)]
    pub return_consumed_capacity: Option<String>,
}

/// One member of a batch.
///
/// `#[non_exhaustive]` because DynamoDB grows this shape and has already grown
/// it twice here: `ConsistentRead` and `ReturnValuesOnConditionCheckFailure`
/// both arrived after the type was first written. The attribute means the next
/// one costs consumers nothing. Construct one from `Default` and assign, or
/// deserialise it. The enclosing `BatchExecuteStatementRequest` is deliberately
/// not marked: it carries `Statements` and `ReturnConsumedCapacity`, which is
/// the whole of DynamoDB's shape.
#[derive(Debug, Default)]
#[non_exhaustive]
pub struct BatchStatementRequest {
    pub statement: String,
    pub parameters: Option<Vec<AttributeValue>>,
    /// Per member, not per batch. Does not change which rows come back, because
    /// every read against SQLite is already strongly consistent, but it does
    /// change the rate the read is charged at: a keyed batch `SELECT` costs 0.5
    /// without it and 1 with it, and a batch mixing the two sums both rates.
    /// Captured eu-west-2 2026-08-15.
    pub consistent_read: Option<bool>,
    /// `ALL_OLD` hands back the item this member's UPDATE or DELETE failed its
    /// condition on, on the member's `Error`; `NONE`, or leaving it out, does
    /// not (captured eu-west-2 and us-east-1, 2026-10-10, which corrects a
    /// 2026-08-15 measurement that found no item either way).
    pub return_values_on_condition_check_failure: Option<String>,
    /// A parameter DynamoDB refused as the request was decoded, such as a
    /// number it can't store. DynamoDB reports it against this statement and
    /// runs the rest (captured eu-west-2 and us-east-1, 2026-10-09), so the
    /// refusal is carried here rather than failing the whole request.
    pub(crate) parameter_rejection: Option<String>,
}

impl<'de> Deserialize<'de> for BatchStatementRequest {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Wire {
            #[serde(rename = "Statement")]
            statement: String,
            #[serde(rename = "Parameters", default)]
            parameters: Option<Vec<serde_json::Value>>,
            #[serde(
                rename = "ConsistentRead",
                default,
                deserialize_with = "crate::types::deserialize_lenient_flag"
            )]
            consistent_read: Option<bool>,
            #[serde(rename = "ReturnValuesOnConditionCheckFailure", default)]
            return_values_on_condition_check_failure: Option<String>,
        }
        let wire = Wire::deserialize(deserializer)?;
        let mut parameter_rejection = None;
        let parameters = match wire.parameters {
            None => None,
            Some(raw) => {
                let mut parameters = Vec::with_capacity(raw.len());
                const EMPTY: &str = "Supplied AttributeValue is empty, must contain exactly \
                                     one of the supported datatypes";
                for value in raw {
                    if value.as_object().is_some_and(|fields| fields.is_empty()) {
                        parameter_rejection.get_or_insert_with(|| EMPTY.to_string());
                        continue;
                    }
                    match crate::types::reading_parameters(|| {
                        serde_json::from_value::<AttributeValue>(value)
                    }) {
                        Ok(parameter) => parameters.push(parameter),
                        // A value refusal belongs to this statement, an empty
                        // value among them, and `{"NULL": false}`, which is
                        // raised in the request-validation class (captured
                        // eu-west-2 and us-east-1, 2026-10-10: only its
                        // statement fails); anything else, a malformed value,
                        // still fails the request.
                        Err(e) => {
                            let message = e.to_string();
                            // `VALIDATION_REQUEST:` extends `VALIDATION`, but
                            // not `VALIDATION:`, so both are checked.
                            if !message.starts_with(crate::serde_errors::VALIDATION_MARKER)
                                && !message
                                    .starts_with(crate::serde_errors::REQUEST_VALIDATION_MARKER)
                                && !message.starts_with(EMPTY)
                            {
                                return Err(serde::de::Error::custom(message));
                            }
                            parameter_rejection.get_or_insert_with(|| {
                                crate::serde_errors::clean_serde_message(&message).to_string()
                            });
                        }
                    }
                }
                Some(parameters)
            }
        };
        Ok(Self {
            statement: wire.statement,
            parameters,
            consistent_read: wire.consistent_read,
            return_values_on_condition_check_failure: wire.return_values_on_condition_check_failure,
            parameter_rejection,
        })
    }
}

#[derive(Debug, Default, Serialize)]
pub struct BatchExecuteStatementResponse {
    #[serde(rename = "Responses")]
    pub responses: Vec<BatchStatementResponse>,
    /// Per-table capacity, aggregated across the statements in the batch. No
    /// transactional factor applies. Absent when no mode was asked for, and also
    /// when every statement failed, which is what AWS does.
    #[serde(rename = "ConsumedCapacity", skip_serializing_if = "Option::is_none")]
    pub consumed_capacity: Option<Vec<crate::types::ConsumedCapacity>>,
}

#[derive(Debug, Default, Serialize)]
#[non_exhaustive]
pub struct BatchStatementResponse {
    #[serde(rename = "Error", skip_serializing_if = "Option::is_none")]
    pub error: Option<BatchStatementError>,
    #[serde(
        rename = "Item",
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::set_order::item_in_hash_order"
    )]
    // Number sets in Java hash-set order here, as DynamoDB returns them;
    // pinned by set_order's every_response_item_field test.
    pub item: Option<Item>,
    /// The member statement's target table, echoed on a successful response
    /// (with or without an `Item`), matching DynamoDB. Omitted on a
    /// per-statement error and when the statement fails to parse.
    #[serde(rename = "TableName", skip_serializing_if = "Option::is_none")]
    pub table_name: Option<String>,
}

#[derive(Debug, Default, Serialize)]
pub struct BatchStatementError {
    #[serde(rename = "Code")]
    pub code: String,
    #[serde(rename = "Message")]
    pub message: String,
    /// The item a failed condition was evaluated against, when the member set
    /// `ReturnValuesOnConditionCheckFailure` to `ALL_OLD`.
    #[serde(
        rename = "Item",
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::set_order::item_in_hash_order"
    )]
    // Number sets in Java hash-set order here, as DynamoDB returns them;
    // pinned by set_order's every_response_item_field test.
    pub item: Option<Item>,
}

/// The statement list as DynamoDB echoes it in a constraint message: each
/// request's Java `toString`, in us-east-1's form, captured 2026-10-10.
/// eu-west-2 printed `BatchStatementRequest@` and a hash in its place, the same
/// hash for each of the 26 identical statements captured, so it is derived from
/// the value; no obvious Java formula reproduces it, so us-east-1's form is the
/// one followed. A member's parameters follow the same Java model, uncaptured
/// (the capture's statements had none). Bounded by
/// [`crate::actions::batch_write_item::ECHO_LIMIT`].
fn statements_echo(statements: &[BatchStatementRequest]) -> String {
    let mut out = crate::actions::batch_write_item::BoundedEcho::new();
    let _ = write_statements(&mut out, statements);
    out.finish()
}

fn write_statements(
    out: &mut dyn std::fmt::Write,
    statements: &[BatchStatementRequest],
) -> std::fmt::Result {
    out.write_str("[")?;
    for (i, s) in statements.iter().enumerate() {
        if i > 0 {
            out.write_str(", ")?;
        }
        write!(
            out,
            "BatchStatementRequest(statement={}, parameters=",
            s.statement
        )?;
        match s.parameters {
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
        out.write_str(", consistentRead=")?;
        match s.consistent_read {
            Some(c) => write!(out, "{c}")?,
            None => out.write_str("null")?,
        }
        out.write_str(", returnValuesOnConditionCheckFailure=")?;
        out.write_str(
            s.return_values_on_condition_check_failure
                .as_deref()
                .unwrap_or("null"),
        )?;
        out.write_str(")")?;
    }
    out.write_str("]")
}

pub async fn execute<S: StorageBackend>(
    storage: &S,
    request: BatchExecuteStatementRequest,
) -> Result<BatchExecuteStatementResponse> {
    // Every member DynamoDB refuses is listed in one error: ReturnConsumedCapacity,
    // then the statements (the list's length, then each statement's
    // ReturnValuesOnConditionCheckFailure, text and parameters by index). Any
    // of them refuses the whole request, before a statement is parsed or
    // runs. Captured eu-west-2 and us-east-1, 2026-10-10: statement then
    // parameters across members, every over-long statement listed,
    // ReturnConsumedCapacity ahead of an over-long list and of a member's own
    // violations, and a member's mode ahead of its statement.
    let mut violations = Vec::new();
    violations.extend(crate::validation::return_consumed_capacity_rejection(
        request.return_consumed_capacity.as_deref(),
    ));
    if request.statements.is_empty() {
        violations.push(
            "Value '[]' at 'statements' failed to satisfy constraint: Member must have length \
             greater than or equal to 1"
                .to_string(),
        );
    }
    if request.statements.len() > 25 {
        violations.push(format!(
            "Value '{}' at 'statements' failed to satisfy constraint: Member must have length \
             less than or equal to 25",
            statements_echo(&request.statements)
        ));
    }
    for (i, statement) in request.statements.iter().enumerate() {
        let member = format!("statements.{}.member", i + 1);
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
        // A member whose every parameter was refused as it was decoded holds
        // an empty list too, but that is its own statement's refusal, not an
        // empty list sent.
        let sent = statement
            .parameters
            .as_deref()
            .filter(|_| statement.parameter_rejection.is_none());
        violations.extend(crate::partiql::parameters_violation(
            sent,
            &format!("{member}.parameters"),
        ));
    }
    if let Some(e) = crate::partiql::member_rejection(&violations) {
        return Err(e);
    }

    // Parse once, and carry the resolved target alongside. Statements that fail
    // to parse keep their error for the per-statement path below: a batch
    // holding one is not rejected outright, it reports `ValidationError` against
    // that member and runs the rest. Parse failure and an unresolvable target
    // are different things, which is why they are a `Result` and an `Option`
    // rather than one fallible value.
    let (prepared, tables) = prepare(storage, &request.statements).await?;

    // A statement refused for its own parameters, or for anything else about
    // it alone, fails on its own and takes no part in the batch-wide checks
    // below, as one that does not parse doesn't (captured eu-west-2 and
    // us-east-1, 2026-10-09, for a parameter nested 63 levels and for an empty
    // attribute name in a parameter; inferred for the other refusals here).
    let mut own_refusals: Vec<Option<String>> =
        request.statements.iter().map(own_refusal).collect();
    // A COUNT projection reading a multi-attribute index is refused for the
    // index, as on ExecuteStatement (captured eu-west-2, 2026-11-11).
    for (stmt_req, refusal) in request.statements.iter().zip(&mut own_refusals) {
        if refusal.is_some()
            && *refusal == partiql::parser::count_projection_rejection(&stmt_req.statement)
            && let Some((table, Some(index))) = partiql::parser::select_target(&stmt_req.statement)
        {
            match partiql::executor::reject_multi_attribute_index_by_name(storage, &table, &index)
                .await
            {
                Ok(()) => {}
                Err(DynoxideError::ValidationException(message)) => *refusal = Some(message),
                Err(other) => return Err(other),
            }
        }
    }
    let counted = || {
        prepared
            .iter()
            .zip(&own_refusals)
            .filter(|(_, refusal)| refusal.is_none())
            .map(|(p, _)| p)
    };

    // A batch is all-read or all-write. AWS rejects a mixed one up front,
    // before any statement runs, rather than per statement. A statement that
    // did not parse takes no part in the classification, and its presence does
    // not stop the check firing on the ones that did.
    let kinds = counted().filter_map(|p| p.stmt.as_ref().ok());
    let (reads, writes): (Vec<_>, Vec<_>) =
        kinds.partition(|stmt| matches!(stmt, partiql::parser::Statement::Select { .. }));
    if !reads.is_empty() && !writes.is_empty() {
        return Err(DynoxideError::ValidationException(
            "Read and write requests together in the same batch is not supported.".to_string(),
        ));
    }

    // Two statements against one item are rejected up front, reads included. A
    // statement whose key cannot be read off the request has no target and is
    // left to fail on its own terms below.
    let mut seen_targets = HashSet::new();
    for target in counted().filter_map(|p| p.target.as_ref()) {
        if !seen_targets.insert(target) {
            return Err(DynoxideError::ValidationException(
                "Provided list of item keys contains duplicates".to_string(),
            ));
        }
    }

    let mut responses = Vec::with_capacity(request.statements.len());
    let mut records: Vec<WriteCapacity> = Vec::new();
    // Table, bytes read, and whether that member asked for a consistent read.
    let mut read_units: Vec<(String, usize, bool)> = Vec::new();
    let mut failures: Vec<(String, f64)> = Vec::new();

    for ((stmt_req, prepared), own_refusal) in
        request.statements.iter().zip(prepared).zip(own_refusals)
    {
        if let Some(message) = own_refusal {
            responses.push(BatchStatementResponse {
                error: Some(BatchStatementError {
                    code: "ValidationError".to_string(),
                    message,
                    item: None,
                }),
                item: None,
                table_name: None,
            });
            continue;
        }

        let response = match prepared.stmt {
            Err(e) => BatchStatementResponse {
                error: Some(BatchStatementError {
                    // A per-statement parse failure carries the short-form
                    // `ValidationError` code, the same as an execution error,
                    // matching DynamoDB.
                    code: "ValidationError".to_string(),
                    message: e.into_message(),
                    item: None,
                }),
                item: None,
                table_name: None,
            },
            Ok(stmt) => {
                // DynamoDB echoes the target table on a successful response, but
                // not on a per-statement error.
                let table = partiql::parser::table_name(&stmt).map(str::to_string);
                let params = stmt_req.parameters.as_deref().unwrap_or_default();
                // A batch read must name a single item. A SELECT that does not
                // resolve to one, or that names an index, is rejected against
                // itself while the rest of the batch runs. Both shapes carry the
                // same message, so an index-qualified read is unreachable here
                // even when it does name the primary key. Captured eu-west-2
                // 2026-08-15. A read of a multi-attribute index never gets here:
                // the statement check refused it in its own words.
                //
                // Only for a table that resolved. A statement has no target
                // either when its WHERE names no key or when its table could not
                // be read, and the second is not this rejection: an INSERT or a
                // DELETE against a table that does not exist reports
                // ResourceNotFound with the table echoed, and a SELECT must say
                // the same rather than claim the key is missing when it is
                // there. Letting it through to `execute_written_page` is what
                // produces that, because table resolution is the first thing a
                // SELECT does.
                //
                // Overlapping key conditions are refused first, and a key of the
                // wrong type is reported in ExecuteStatement's words (captured
                // eu-west-2 and us-east-1, 2026-10-09).
                let resolved_table = table.as_deref().and_then(|t| tables.get(t));
                let unkeyed = || {
                    "Select statements within BatchExecuteStatement must specify the \
                     primary key in the where clause."
                        .to_string()
                };
                let refusal = match resolved_table {
                    Some(resolved) if matches!(stmt, partiql::parser::Statement::Select { .. }) => {
                        if partiql::parser::index_name(&stmt).is_some() {
                            Some(unkeyed())
                        } else if let Err(e) =
                            partiql::executor::reject_overlapping_select(&stmt, params, resolved)
                        {
                            Some(e.to_string())
                        } else {
                            match partiql::executor::keyed_select(&stmt, params, resolved) {
                                partiql::executor::KeyedSelect::ByPrimaryKey => None,
                                partiql::executor::KeyedSelect::NotByPrimaryKey => Some(unkeyed()),
                                partiql::executor::KeyedSelect::KeyType(message) => Some(message),
                            }
                        }
                    }
                    _ => None,
                };
                if let Some(message) = refusal {
                    responses.push(BatchStatementResponse {
                        error: Some(BatchStatementError {
                            code: "ValidationError".to_string(),
                            message,
                            item: None,
                        }),
                        item: None,
                        table_name: None,
                    });
                    continue;
                }
                let run = partiql::executor::execute_written_page(
                    storage,
                    &stmt_req.statement,
                    &stmt,
                    params,
                    None,
                    None,
                    stmt_req.consistent_read.unwrap_or(false),
                    request.return_consumed_capacity.as_deref(),
                    table.as_deref().and_then(|t| tables.get(t)),
                    stmt_req.return_values_on_condition_check_failure.as_deref() == Some("ALL_OLD"),
                );
                // One transaction per statement, not per batch: a batch is not
                // atomic across its statements, so each one commits or rolls
                // back on its own, base write and index fan-out together.
                let outcome = if matches!(stmt, partiql::parser::Statement::Select { .. }) {
                    run.await
                } else {
                    super::helpers::with_write_transaction(storage, run).await
                };
                match outcome {
                    Ok(page) => {
                        match page.capacity {
                            Some(capacity) => records.push(capacity),
                            // A SELECT is charged read units against the rows it
                            // walked, with no index arm on a base table read.
                            None => {
                                if let Some(ref name) = table {
                                    read_units.push((
                                        name.clone(),
                                        page.size,
                                        stmt_req.consistent_read.unwrap_or(false),
                                    ));
                                }
                            }
                        }
                        // A SELECT yields its row here; a DELETE/UPDATE carrying a
                        // RETURNING clause yields the returned item. Batch surfaces
                        // a single item per statement, so take the first.
                        BatchStatementResponse {
                            error: None,
                            item: page.items.and_then(|items| items.into_iter().next()),
                            table_name: table,
                        }
                    }
                    Err(e) => {
                        let code = e.short_error_code().to_string();
                        let message = e.to_string();
                        // Present only when the member asked for it.
                        let item = match e {
                            DynoxideError::ConditionalCheckFailedException(_, item) => item,
                            _ => None,
                        };
                        // A key too large to be one is refused before anything
                        // is read or written, and is charged nothing (captured
                        // eu-west-2 and us-east-1, 2026-10-10). So is an UPDATE
                        // of a key attribute (refused before the item is read,
                        // captured eu-west-2 and us-east-1, 10 October 2026;
                        // that it is charged nothing is inferred).
                        let refused_before_reading = code == "ValidationError"
                            && (message == crate::actions::helpers::HASH_KEY_TOO_LARGE
                                || message == crate::actions::helpers::RANGE_KEY_TOO_LARGE
                                || message
                                    .starts_with(crate::actions::helpers::KEY_ATTRIBUTE_UPDATE));
                        // A failed statement is charged the write it attempted,
                        // added to its table's total while appearing in no arm.
                        // Captured: a failing 3KB insert beside a small success
                        // reports total 5 against arms summing to 2, and three
                        // failures of 1, 1 and 3 units report total 7.
                        //
                        // Only an INSERT carries its item in the statement, so
                        // that is the one kind whose attempt can be sized
                        // without reading anything. Everything else falls back
                        // to the one-unit minimum.
                        if let Some(ref name) = table
                            && !refused_before_reading
                        {
                            let units =
                                attempted_units(storage, &stmt, params, prepared.target.as_ref())
                                    .await;
                            failures.push((name.clone(), units));
                        }
                        // DynamoDB echoes the table on a member whose statement
                        // ran and failed, and omits it on one rejected before it
                        // ran. `ConditionalCheckFailed` and `DuplicateItem` both
                        // carry it; a `ValidationError` does not, which is what
                        // an invalid RETURNING variant or a bad expression is.
                        // Captured eu-west-2 2026-08-15 for the first pair and
                        // 2026-07 for the second. A key too large to be one is
                        // the exception: DynamoDB echoes the table with that
                        // ValidationError, for a partition key and a sort key
                        // alike (captured eu-west-2 and us-east-1, 2026-10-10),
                        // and with the refusal of an UPDATE of a key attribute
                        // (captured eu-west-2 and us-east-1, 10 October 2026).
                        let echoes_table = code != "ValidationError" || refused_before_reading;
                        BatchStatementResponse {
                            error: Some(BatchStatementError {
                                code,
                                message,
                                item,
                            }),
                            item: None,
                            table_name: if echoes_table { table } else { None },
                        }
                    }
                }
            }
        };

        responses.push(response);
    }

    Ok(BatchExecuteStatementResponse {
        consumed_capacity: build_capacity(
            &records,
            &read_units,
            &failures,
            &request.return_consumed_capacity,
        ),
        responses,
    })
}

/// One statement, parsed once and resolved once.
struct Prepared {
    /// The parse result. An `Err` is a per-statement error, not a request-level
    /// one: AWS reports `ValidationError` against that member and runs the rest.
    stmt: std::result::Result<partiql::parser::Statement, partiql::parser::ParseError>,
    /// The item this statement targets, for duplicate detection. `None` when it
    /// does not resolve to one, which covers a partition-spanning `SELECT` and
    /// anything whose key cannot be read off the request.
    target: Option<(String, String, String)>,
}

/// What fails a batch member on its own, before its statement runs: in this
/// order, a parameter refused as it was decoded, one nested too deep (the
/// nesting message ahead of the value checks), one DynamoDB can't store, a
/// COUNT projection (refused before parsing with the bare message captured on
/// ExecuteStatement) and an empty attribute name in a parameter. Each is
/// reported against that member while the rest run (captured eu-west-2 and
/// us-east-1, 2026-10-09, and the empty name in ten regions). That an
/// unstorable value drops the member out of the read/write and duplicate
/// checks is inferred, not captured. For a COUNT projection the exclusion
/// changes nothing: COUNT doesn't parse, so the member was already outside both
/// checks.
fn own_refusal(stmt_req: &BatchStatementRequest) -> Option<String> {
    if let Some(rejection) = &stmt_req.parameter_rejection {
        return Some(rejection.clone());
    }
    let parameters = stmt_req.parameters.as_deref().unwrap_or_default();
    if crate::value_nesting::parameters_nest_too_deep(parameters) {
        return Some(crate::value_nesting::deep_parameter_error().to_string());
    }
    if let Err(e) = parameters
        .iter()
        .try_for_each(partiql::executor::check_parameter_value)
    {
        return Some(e.to_string());
    }
    if let Some(message) = partiql::parser::count_projection_rejection(&stmt_req.statement) {
        return Some(message);
    }
    if crate::empty_names::parameters_have_empty_name(stmt_req.parameters.as_deref()) {
        return Some(crate::empty_names::MESSAGE.to_string());
    }
    None
}

/// Parse every statement and resolve its target, once each.
///
/// The statement text was previously parsed three times per call: to classify
/// reads against writes, to find duplicate targets, and to execute. Measured on
/// a 25-statement batch, each pass costs about as much as everything the target
/// resolution does, so the parsing was the larger half of the overhead by more
/// than two to one.
///
/// Target resolution loads the table's metadata and parses its key schema, and
/// both are now kept rather than thrown away once the target is read off them.
/// The executor takes them as they are instead of resolving the same table a
/// second time, which halves the metadata loads a batch performs. That is worth
/// most on the wasm backend, where every load crosses the bridge to a JS worker
/// and nothing caches the result, but the key schema half is a JSON parse and is
/// paid on both backends.
async fn prepare<S: StorageBackend>(
    storage: &S,
    statements: &[BatchStatementRequest],
) -> Result<(Vec<Prepared>, HashMap<String, ResolvedTable>)> {
    let mut prepared = Vec::with_capacity(statements.len());
    // Keyed by table, not by statement: a batch is usually 25 statements
    // against one table, and that is one resolution rather than 25.
    let mut tables: HashMap<String, ResolvedTable> = HashMap::new();

    for stmt_req in statements {
        // A statement check's refusal is the member's own error, so the member
        // takes no part in the batch's checks (captured eu-west-2,
        // 2026-11-11).
        let checked = partiql::executor::checked_statement(
            storage,
            &stmt_req.statement,
            stmt_req.parameters.as_deref().unwrap_or_default(),
            partiql::executor::StatementSource::BatchMember,
        )
        .await;
        // A failure that isn't a refusal of the statement fails the request.
        let parsed = match checked {
            Ok(checked) => {
                if let Some(table) = checked.table {
                    tables.entry(table.table_name.clone()).or_insert(table);
                }
                Ok(checked.statement)
            }
            Err(partiql::executor::CheckFailure::Refused(
                partiql::parser::ParseFailure::Statement(e),
            )) => Err(e),
            Err(partiql::executor::CheckFailure::Refused(
                partiql::parser::ParseFailure::Literal(message),
            )) => Err(partiql::parser::ParseError::Validation(message)),
            Err(partiql::executor::CheckFailure::Failed(error)) => return Err(error),
        };
        let mut target = None;
        if let Ok(ref stmt) = parsed
            && let Some(name) = partiql::parser::table_name(stmt)
        {
            if !tables.contains_key(name)
                && let Ok(resolved) = ResolvedTable::load(storage, name).await
            {
                tables.insert(name.to_string(), resolved);
            }
            // Absent when the table could not be read, which leaves the
            // statement to fail on its own terms during execution.
            if let Some(resolved) = tables.get(name) {
                let params = stmt_req.parameters.as_deref().unwrap_or_default();
                target = partiql::executor::statement_target_in(stmt, params, resolved);
            }
        }
        prepared.push(Prepared {
            stmt: parsed,
            target,
        });
    }
    Ok((prepared, tables))
}

/// The write units a failed statement is charged.
///
/// Sized on the larger of the item already stored at the target and the item
/// the statement carried, which is the same rule a successful write follows.
///
/// The stored side is load-bearing rather than incidental. Captured: a batch of
/// three failing inserts alongside one success reports 7 against arms summing
/// to 2. Two of the failures name tiny items and cost 1 each; the third names a
/// tiny item too, but the row already at that key is 3KB, and it costs 3. Sizing
/// on the statement alone reports 5.
/// `target` is the one the preparation pass already resolved. Sizing used to
/// resolve it again here, which made a batch of failures cost three metadata
/// loads per statement rather than two.
async fn attempted_units<S: StorageBackend>(
    storage: &S,
    stmt: &partiql::parser::Statement,
    parameters: &[AttributeValue],
    target: Option<&(String, String, String)>,
) -> f64 {
    let stored_size = match target {
        Some((table, pk, sk)) => storage
            .get_item(table, pk, sk)
            .await
            .ok()
            .flatten()
            .and_then(|json| serde_json::from_str::<Item>(&json).ok())
            .map(|item| crate::types::item_size(&item)),
        None => None,
    };

    // Only an INSERT carries its item in the statement text.
    let attempted_size = match stmt {
        partiql::parser::Statement::Insert { item, .. } => {
            let mut resolved = Item::new();
            for (name, value) in item {
                let av = match value {
                    partiql::parser::PartiqlValue::Literal(av) => av.clone(),
                    // An unresolvable parameter is why the statement failed, so
                    // size on what did resolve.
                    partiql::parser::PartiqlValue::Parameter(idx) => match parameters.get(*idx) {
                        Some(av) => av.clone(),
                        None => continue,
                    },
                };
                resolved.insert(name.clone(), av);
            }
            Some(crate::types::item_size(&resolved))
        }
        _ => None,
    };

    crate::types::table_write_capacity_units(stored_size, attempted_size)
}

/// Fold the batch into one `ConsumedCapacity` per table.
///
/// No transactional factor applies. A batch in which nothing succeeded reports
/// no capacity at all, which is what AWS does even though the same failure
/// counts for a unit when another statement in the batch succeeds.
fn build_capacity(
    records: &[WriteCapacity],
    read_units: &[(String, usize, bool)],
    failures: &[(String, f64)],
    mode: &Option<String>,
) -> Option<Vec<crate::types::ConsumedCapacity>> {
    if !matches!(mode.as_deref(), Some("TOTAL") | Some("INDEXES")) {
        return None;
    }
    if records.is_empty() && read_units.is_empty() {
        return None;
    }

    let mut by_table = aggregate_by_table(records, 1.0);
    for (table, size, consistent) in read_units {
        by_table.entry(table.clone()).or_default().table_units +=
            crate::types::read_capacity_units_with_consistency(*size, *consistent);
    }

    let mut entries = per_table_capacity(
        &by_table,
        mode,
        crate::types::consumed_capacity_with_secondary_indexes,
    )?;

    // The surcharge is applied here rather than inside the shared fold, because
    // it is the one rule of the three that differs and it has already been got
    // wrong once. A failed statement lands on the total without reaching the
    // Table arm or any index arm, so it cannot be folded into the units the
    // builder sees. A table whose statements all failed has no entry to attach
    // it to and gets none: captured against a two-table batch where the failing
    // table was omitted entirely and only the succeeding one was reported.
    let mut surcharge: HashMap<&str, f64> = HashMap::new();
    for (table, units) in failures {
        *surcharge.entry(table.as_str()).or_default() += units;
    }
    for entry in &mut entries {
        entry.capacity_units += surcharge
            .get(entry.table_name.as_str())
            .copied()
            .unwrap_or(0.0);
    }

    Some(entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Storage;
    use crate::types::AttributeValue;

    fn s(v: &str) -> AttributeValue {
        AttributeValue::S(v.to_string())
    }

    async fn seed(storage: &Storage) {
        let req = serde_json::from_value(serde_json::json!({
            "TableName": "pq_btx",
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "g1", "AttributeType": "S"},
                {"AttributeName": "g2", "AttributeType": "S"}
            ],
            "BillingMode": "PAY_PER_REQUEST",
            "GlobalSecondaryIndexes": [
                {
                    "IndexName": "gsi-one",
                    "KeySchema": [{"AttributeName": "g1", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "ALL"}
                },
                {
                    "IndexName": "gsi-two",
                    "KeySchema": [{"AttributeName": "g2", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "ALL"}
                }
            ]
        }))
        .unwrap();
        crate::actions::create_table::execute(storage, req)
            .await
            .unwrap();
        for pk in ["a", "b"] {
            let put = crate::actions::put_item::PutItemRequest {
                table_name: "pq_btx".to_string(),
                item: [
                    ("pk".to_string(), s(pk)),
                    ("g1".to_string(), s("x")),
                    ("g2".to_string(), s("y")),
                    ("val".to_string(), s("before")),
                ]
                .into_iter()
                .collect(),
                ..Default::default()
            };
            crate::actions::put_item::execute(storage, put)
                .await
                .unwrap();
        }
    }

    async fn val_of(storage: &Storage, pk: &str) -> String {
        let req = crate::actions::get_item::GetItemRequest {
            table_name: "pq_btx".to_string(),
            key: [("pk".to_string(), s(pk))].into_iter().collect(),
            ..Default::default()
        };
        match crate::actions::get_item::execute(storage, req)
            .await
            .unwrap()
            .item
            .unwrap()
            .get("val")
            .unwrap()
        {
            AttributeValue::S(v) => v.clone(),
            other => panic!("unexpected val: {other:?}"),
        }
    }

    fn stmt(statement: &str) -> BatchStatementRequest {
        BatchStatementRequest {
            statement: statement.to_string(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn a_failed_statement_rolls_back_without_taking_its_neighbour_with_it() {
        // The transaction is per statement, not per batch. A batch is not
        // atomic across its statements on DynamoDB, so the one that fails must
        // roll its own fan-out back and leave the one that succeeded standing.
        let storage = Storage::memory().unwrap();
        seed(&storage).await;

        // The first statement's fan-out runs clean (two inserts), the second
        // gets one insert in and fails on the other.
        storage.fail_gsi_insert_after(3);
        let out = execute(
            &storage,
            BatchExecuteStatementRequest {
                statements: vec![
                    stmt("UPDATE \"pq_btx\" SET val='after' WHERE pk='a'"),
                    stmt("UPDATE \"pq_btx\" SET val='after' WHERE pk='b'"),
                ],
                ..Default::default()
            },
        )
        .await
        .unwrap();

        assert!(
            out.responses[0].error.is_none(),
            "the first statement should stand: {:?}",
            out.responses[0].error
        );
        assert_eq!(
            val_of(&storage, "a").await,
            "after",
            "a committed statement must survive its neighbour failing"
        );
        assert_eq!(
            val_of(&storage, "b").await,
            "before",
            "the failed statement must roll its own base row back"
        );
    }
}
