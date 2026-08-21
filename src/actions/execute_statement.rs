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

    if let Some(msg) = crate::validation::return_consumed_capacity_rejection(
        request.return_consumed_capacity.as_deref(),
    ) {
        return Err(DynoxideError::ValidationException(
            crate::validation::envelope_message(&msg),
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
    let run = partiql::executor::execute_page(
        storage,
        &stmt,
        &params,
        request.limit,
        request.next_token.as_deref(),
        request.consistent_read.unwrap_or(false),
        request.return_consumed_capacity.as_deref(),
        // A single statement has no preparation pass to share a resolution
        // with, so the executor resolves the table itself, once.
        None,
    );
    // Base write, index fan-out and stream record are one atomic unit, as they
    // are on every other write path. Without it a fan-out that failed part way
    // left the base row committed and the indexes disagreeing with it. A read
    // opens no transaction: it has nothing to roll back.
    let page = if matches!(stmt, partiql::parser::Statement::Select { .. }) {
        run.await?
    } else {
        super::helpers::with_write_transaction(storage, run).await?
    };
    let partiql::executor::StatementPage {
        items,
        size,
        capacity,
        next_token,
        read_index,
        base_read_units,
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
                    // land on the table arm, leaving the index arm to cover the
                    // index read. Each one is charged on its own bytes at the
                    // request's consistency and the charges are summed, which
                    // the executor works out because only it sees the sizes.
                    // Captured eu-west-2 2026-08-15: three small rows served
                    // this way reported total 2, table 1.5, lsi 0.5. Captured
                    // again 2026-08-17 across item sizes: the same three rows at
                    // ~9KB apiece report table 4.5, and 9 under ConsistentRead.
                    let lsi_units = std::collections::HashMap::from([(index.name, units)]);
                    crate::types::consumed_capacity_with_secondary_indexes(
                        table,
                        base_read_units,
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
        crate::types::consumed_capacity_with_vector_indexes(
            table,
            crate::types::table_write_capacity_units(capacity.old_size, capacity.new_size),
            &capacity.gsi_units,
            &capacity.lsi_units,
            &capacity.vector_bytes,
            &request.return_consumed_capacity,
        )
    });

    Ok(ExecuteStatementResponse {
        items,
        next_token,
        consumed_capacity,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Storage;

    /// Two GSIs, so the fan-out has a second insert to fail on.
    async fn table_with_two_gsis(storage: &Storage) {
        let req = serde_json::from_value(serde_json::json!({
            "TableName": "pq_tx",
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
    }

    fn s(v: &str) -> AttributeValue {
        AttributeValue::S(v.to_string())
    }

    async fn val_of(storage: &Storage, pk: &str) -> String {
        let req = crate::actions::get_item::GetItemRequest {
            table_name: "pq_tx".to_string(),
            key: [("pk".to_string(), s(pk))].into_iter().collect(),
            ..Default::default()
        };
        let got = crate::actions::get_item::execute(storage, req)
            .await
            .unwrap();
        match got.item.unwrap().get("val").unwrap() {
            AttributeValue::S(v) => v.clone(),
            other => panic!("unexpected val: {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_failed_fan_out_rolls_the_base_row_back() {
        // Every other write path wraps its base write and index fan-out in one
        // transaction. These two did not, so a fan-out that failed part way
        // left the base row committed and the indexes disagreeing with it, with
        // nothing to signal it.
        let storage = Storage::memory().unwrap();
        table_with_two_gsis(&storage).await;

        let put = crate::actions::put_item::PutItemRequest {
            table_name: "pq_tx".to_string(),
            item: [
                ("pk".to_string(), s("a")),
                ("g1".to_string(), s("x")),
                ("g2".to_string(), s("y")),
                ("val".to_string(), s("before")),
            ]
            .into_iter()
            .collect(),
            ..Default::default()
        };
        crate::actions::put_item::execute(&storage, put)
            .await
            .unwrap();
        assert_eq!(val_of(&storage, "a").await, "before");

        // Let the first index insert land, fail the second.
        storage.fail_gsi_insert_after(1);
        let err = execute(
            &storage,
            ExecuteStatementRequest {
                statement: "UPDATE \"pq_tx\" SET val='after' WHERE pk='a'".to_string(),
                ..Default::default()
            },
        )
        .await
        .expect_err("the injected failure must surface");
        assert!(
            format!("{err:?}").contains("injected GSI insert failure"),
            "unexpected error: {err:?}"
        );

        assert_eq!(
            val_of(&storage, "a").await,
            "before",
            "the base row must roll back with the fan-out that failed"
        );
    }
}
