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
/// `#[non_exhaustive]` because this type is short of DynamoDB's: it still lacks
/// `ConsistentRead` and `ReturnValuesOnConditionCheckFailure`, so it will gain
/// fields again. Construct one from `Default` and assign, or deserialise it.
/// The enclosing `BatchExecuteStatementRequest` is deliberately not marked: it
/// carries `Statements` and `ReturnConsumedCapacity`, which is the whole of
/// DynamoDB's shape, so there is nothing left to add to it.
#[derive(Debug, Default, Deserialize)]
#[non_exhaustive]
pub struct BatchStatementRequest {
    #[serde(rename = "Statement")]
    pub statement: String,
    #[serde(rename = "Parameters", default)]
    pub parameters: Option<Vec<AttributeValue>>,
    /// Per member, not per batch. Does not change which rows come back, because
    /// every read against SQLite is already strongly consistent, but it does
    /// change the rate the read is charged at: a keyed batch `SELECT` costs 0.5
    /// without it and 1 with it, and a batch mixing the two sums both rates.
    /// Captured eu-west-2 2026-08-15.
    #[serde(rename = "ConsistentRead", default)]
    pub consistent_read: Option<bool>,
    /// Accepted and inert, which is what DynamoDB does with it. A batch member
    /// whose condition fails returns the same response whether this is
    /// `ALL_OLD`, `NONE`, or absent, and never carries the item: measured
    /// against a `TransactWriteItems` `ConditionCheck` in the same round, which
    /// does return it. The field is deserialised so a client setting it meets a
    /// field dynoxide knows rather than one it drops.
    #[serde(rename = "ReturnValuesOnConditionCheckFailure", default)]
    pub return_values_on_condition_check_failure: Option<String>,
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
    #[serde(rename = "Item", skip_serializing_if = "Option::is_none")]
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
}

pub async fn execute<S: StorageBackend>(
    storage: &S,
    request: BatchExecuteStatementRequest,
) -> Result<BatchExecuteStatementResponse> {
    if let Some(msg) = crate::validation::return_consumed_capacity_rejection(
        request.return_consumed_capacity.as_deref(),
    ) {
        return Err(DynoxideError::ValidationException(
            crate::validation::envelope_message(&msg),
        ));
    }

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

    // Parse once, and carry the resolved target alongside. Statements that fail
    // to parse keep their error for the per-statement path below: a batch
    // holding one is not rejected outright, it reports `ValidationError` against
    // that member and runs the rest. Parse failure and an unresolvable target
    // are different things, which is why they are a `Result` and an `Option`
    // rather than one fallible value.
    let (prepared, tables) = prepare(storage, &request.statements).await;

    // A batch is all-read or all-write. AWS rejects a mixed one up front,
    // before any statement runs, rather than per statement. A statement that
    // did not parse takes no part in the classification, and its presence does
    // not stop the check firing on the ones that did.
    let kinds = prepared.iter().filter_map(|p| p.stmt.as_ref().ok());
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
    for target in prepared.iter().filter_map(|p| p.target.as_ref()) {
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

    for (stmt_req, prepared) in request.statements.iter().zip(prepared) {
        // A COUNT projection is rejected before parsing with the bare message
        // captured on ExecuteStatement, carried here under the same
        // per-statement ValidationError code a parse failure uses.
        if let Some(msg) = partiql::parser::count_projection_rejection(&stmt_req.statement) {
            responses.push(BatchStatementResponse {
                error: Some(BatchStatementError {
                    code: "ValidationError".to_string(),
                    message: msg,
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
                // 2026-08-15.
                //
                // Only for a table that resolved. A statement has no target
                // either when its WHERE names no key or when its table could not
                // be read, and the second is not this rejection: an INSERT or a
                // DELETE against a table that does not exist reports
                // ResourceNotFound with the table echoed, and a SELECT must say
                // the same rather than claim the key is missing when it is
                // there. Letting it through to `execute_page` is what produces
                // that, because table resolution is the first thing a SELECT
                // does.
                let table_resolved = table.as_deref().is_some_and(|t| tables.contains_key(t));
                let unkeyed_read = matches!(stmt, partiql::parser::Statement::Select { .. })
                    && table_resolved
                    && (prepared.target.is_none() || partiql::parser::index_name(&stmt).is_some());
                if unkeyed_read {
                    responses.push(BatchStatementResponse {
                        error: Some(BatchStatementError {
                            code: "ValidationError".to_string(),
                            message: "Select statements within BatchExecuteStatement must \
                                      specify the primary key in the where clause."
                                .to_string(),
                        }),
                        item: None,
                        table_name: None,
                    });
                    continue;
                }
                match partiql::executor::execute_page(
                    storage,
                    &stmt,
                    params,
                    None,
                    None,
                    stmt_req.consistent_read.unwrap_or(false),
                    request.return_consumed_capacity.as_deref(),
                    table.as_deref().and_then(|t| tables.get(t)),
                )
                .await
                {
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
                        if let Some(ref name) = table {
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
                        // 2026-07 for the second.
                        let code = e.short_error_code().to_string();
                        let echoes_table = code != "ValidationError";
                        BatchStatementResponse {
                            error: Some(BatchStatementError {
                                code,
                                message: e.to_string(),
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
) -> (Vec<Prepared>, HashMap<String, ResolvedTable>) {
    let mut prepared = Vec::with_capacity(statements.len());
    // Keyed by table, not by statement: a batch is usually 25 statements
    // against one table, and that is one resolution rather than 25.
    let mut tables: HashMap<String, ResolvedTable> = HashMap::new();

    for stmt_req in statements {
        let parsed = partiql::parser::parse(&stmt_req.statement);
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
                target = partiql::executor::statement_target_in(stmt, params, name, resolved);
            }
        }
        prepared.push(Prepared {
            stmt: parsed,
            target,
        });
    }
    (prepared, tables)
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
