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
    /// Does not change which rows come back, because every read against SQLite
    /// is already strongly consistent. It does change two things: the rate the
    /// read is charged at, and whether a select qualified by a GSI is rejected
    /// at all.
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

    // A COUNT projection is rejected before parsing, with DynamoDB's bare
    // message rather than the wasn't-well-formed wrapper the parse errors
    // below carry. ExecuteStatement is the captured surface for this shape.
    if let Some(msg) = partiql::parser::count_projection_rejection(&request.statement) {
        return Err(DynoxideError::ValidationException(msg));
    }

    // The parser says which envelope its rejection takes: a malformed statement
    // gets DynamoDB's "wasn't well formed" wrapper, while a rejection DynamoDB
    // reports on its own terms is passed through bare.
    let stmt = partiql::parser::parse(&request.statement)
        .map_err(|e| DynoxideError::ValidationException(e.into_message()))?;

    let params = request.parameters.unwrap_or_default();
    let page = partiql::executor::execute_page(
        storage,
        &stmt,
        &params,
        request.limit,
        request.next_token.as_deref(),
        request.consistent_read.unwrap_or(false),
    )
    .await?;
    let partiql::executor::StatementPage {
        items,
        size,
        capacity,
        next_token,
        read_index,
        base_reads,
    } = page;

    // ConsumedCapacity is returned whenever ReturnConsumedCapacity is requested,
    // unlike some emulators that omit it. A SELECT is charged read units (an
    // eventually consistent read unless ConsistentRead is set) against the rows
    // it walked, which is before its WHERE clause and its projection narrow
    // them. A write is charged the base table arm plus a per-index arm,
    // with no transactional factor: a capture against real DynamoDB reports a
    // PartiQL INSERT of an item in two indexes as total 3, table 1, one unit per
    // index, exactly as the equivalent `PutItem`.
    let consumed_capacity = partiql::parser::table_name(&stmt).and_then(|table| {
        let Some(capacity) = capacity else {
            let units = crate::types::read_capacity_units_with_consistency(
                size,
                request.consistent_read.unwrap_or(false),
            );
            // A read served from an index is charged against that index's arm
            // with the table arm at zero, the same shape Query and Scan already
            // report. Captured eu-west-2 2026-08-15: a keyed GSI select is
            // total 0.5, table 0, gsi 0.5, where dynoxide charged the table.
            return match read_index {
                Some(index) if index.is_lsi => {
                    // A reach-back read the base item for each row, and those
                    // land on the table arm at read granularity apiece, leaving
                    // the index arm to cover the index read. Captured
                    // eu-west-2 2026-08-15: three rows served this way reported
                    // total 2, table 1.5, lsi 0.5.
                    let table_units = crate::types::read_capacity_units_with_consistency(0, false)
                        * base_reads as f64;
                    let lsi_units = std::collections::HashMap::from([(index.name, units)]);
                    crate::types::consumed_capacity_with_secondary_indexes(
                        table,
                        table_units,
                        &std::collections::HashMap::new(),
                        &lsi_units,
                        &request.return_consumed_capacity,
                    )
                }
                Some(index) => {
                    let gsi_units = std::collections::HashMap::from([(index.name, units)]);
                    crate::types::consumed_capacity_with_indexes(
                        table,
                        0.0,
                        &gsi_units,
                        &request.return_consumed_capacity,
                    )
                }
                None => {
                    crate::types::consumed_capacity(table, units, &request.return_consumed_capacity)
                }
            };
        };
        crate::types::consumed_capacity_with_secondary_indexes(
            table,
            crate::types::table_write_capacity_units(capacity.old_size, capacity.new_size),
            &capacity.gsi_units,
            &capacity.lsi_units,
            &request.return_consumed_capacity,
        )
    });

    Ok(ExecuteStatementResponse {
        items,
        next_token,
        consumed_capacity,
    })
}
