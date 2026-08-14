use crate::actions::index_capacity::{WriteCapacity, aggregate_by_table};
use crate::errors::{DynoxideError, Result};
use crate::partiql;
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

    // A batch is all-read or all-write. AWS rejects a mixed one up front,
    // before any statement runs, rather than per statement. Statements that do
    // not parse are left to the per-statement error path below and take no part
    // in the classification.
    let kinds: Vec<bool> = request
        .statements
        .iter()
        .filter_map(|s| partiql::parser::parse(&s.statement).ok())
        .map(|stmt| matches!(stmt, partiql::parser::Statement::Select { .. }))
        .collect();
    if kinds.iter().any(|is_read| *is_read) && kinds.iter().any(|is_read| !is_read) {
        return Err(DynoxideError::ValidationException(
            "Read and write requests together in the same batch is not supported.".to_string(),
        ));
    }

    // Two statements against one item are rejected up front. A statement whose
    // key cannot be read off the request is skipped here and left to fail on
    // its own terms below.
    let mut seen_targets = HashSet::new();
    for stmt_req in &request.statements {
        let Ok(stmt) = partiql::parser::parse(&stmt_req.statement) else {
            continue;
        };
        let params = stmt_req.parameters.as_deref().unwrap_or_default();
        // Written as nested ifs rather than a let-chain, which would raise the
        // crate's declared MSRV.
        if let Some(target) = partiql::executor::statement_target(storage, &stmt, params).await {
            if !seen_targets.insert(target) {
                return Err(DynoxideError::ValidationException(
                    "Provided list of item keys contains duplicates".to_string(),
                ));
            }
        }
    }

    let mut responses = Vec::with_capacity(request.statements.len());
    let mut records: Vec<WriteCapacity> = Vec::new();
    let mut read_units: Vec<(String, usize)> = Vec::new();
    let mut failures: Vec<(String, f64)> = Vec::new();

    for stmt_req in &request.statements {
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

        let parsed = partiql::parser::parse(&stmt_req.statement);

        let response = match parsed {
            Err(e) => BatchStatementResponse {
                error: Some(BatchStatementError {
                    // A per-statement parse failure carries the short-form
                    // `ValidationError` code, the same as an execution error,
                    // matching DynamoDB.
                    code: "ValidationError".to_string(),
                    message: format!("Statement wasn't well formed, can't be processed: {e}"),
                }),
                item: None,
                table_name: None,
            },
            Ok(stmt) => {
                // DynamoDB echoes the target table on a successful response, but
                // not on a per-statement error.
                let table = partiql::parser::table_name(&stmt).map(str::to_string);
                let params = stmt_req.parameters.as_deref().unwrap_or_default();
                match partiql::executor::execute_page(storage, &stmt, params, None, None).await {
                    Ok(page) => {
                        match page.capacity {
                            Some(capacity) => records.push(capacity),
                            // A SELECT is charged read units against the rows it
                            // returned, with no index arm on a base table read.
                            None => {
                                if let Some(ref name) = table {
                                    read_units.push((name.clone(), page.size));
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
                        if let Some(name) = table {
                            let units = attempted_units(storage, &stmt, params).await;
                            failures.push((name, units));
                        }
                        BatchStatementResponse {
                            error: Some(BatchStatementError {
                                code: e.short_error_code().to_string(),
                                message: e.to_string(),
                            }),
                            item: None,
                            table_name: None,
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
async fn attempted_units<S: StorageBackend>(
    storage: &S,
    stmt: &partiql::parser::Statement,
    parameters: &[AttributeValue],
) -> f64 {
    let stored_size = match partiql::executor::statement_target(storage, stmt, parameters).await {
        Some((table, pk, sk)) => storage
            .get_item(&table, &pk, &sk)
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
    read_units: &[(String, usize)],
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
    for (table, size) in read_units {
        by_table.entry(table.clone()).or_default().table_units +=
            crate::types::read_capacity_units_with_consistency(*size, false);
    }

    // The surcharge lands on the total without reaching the Table arm, so it is
    // added after the arms are built rather than folded into them. A table whose
    // statements all failed gets no entry at all: captured against a two-table
    // batch where the failing table was omitted entirely and only the
    // succeeding one was reported.
    let mut surcharge: HashMap<&str, f64> = HashMap::new();
    for (table, units) in failures {
        *surcharge.entry(table.as_str()).or_default() += units;
    }

    let mut tables: Vec<&String> = by_table.keys().collect();
    tables.sort();

    Some(
        tables
            .into_iter()
            .filter_map(|table| {
                let units = by_table.get(table)?;
                let mut capacity = crate::types::consumed_capacity_with_secondary_indexes(
                    table,
                    units.table_units,
                    &units.gsi_units,
                    &units.lsi_units,
                    mode,
                )?;
                capacity.capacity_units += surcharge.get(table.as_str()).copied().unwrap_or(0.0);
                Some(capacity)
            })
            .collect(),
    )
}
