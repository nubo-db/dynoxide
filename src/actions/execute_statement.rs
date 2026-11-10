use crate::errors::{DynoxideError, Result};
use crate::partiql;
use crate::storage_backend::StorageBackend;
use crate::types::{AttributeValue, Item};
use serde::{Deserialize, Serialize};

#[derive(Debug, Default)]
pub struct ExecuteStatementRequest {
    pub statement: String,
    pub parameters: Option<Vec<AttributeValue>>,
    pub limit: Option<usize>,
    pub next_token: Option<String>,
    /// Does not change which rows come back, because every read against SQLite
    /// is already strongly consistent. It does change two things: the rate the
    /// read is charged at, and whether a select qualified by a GSI is rejected
    /// at all.
    pub consistent_read: Option<bool>,
    pub return_consumed_capacity: Option<String>,
    /// `ALL_OLD` hands back the item an UPDATE or DELETE failed its condition
    /// on, on the `ConditionalCheckFailedException`; `NONE`, or leaving it out,
    /// does not.
    pub return_values_on_condition_check_failure: Option<String>,
}

/// The request as it arrives on the wire, with `Limit` read as DynamoDB reads
/// it: any JSON number, as a 32-bit integer.
#[derive(Deserialize)]
struct ExecuteStatementRequestRaw {
    #[serde(rename = "Statement")]
    statement: String,
    #[serde(
        rename = "Parameters",
        default,
        deserialize_with = "crate::types::deserialize_held_parameters"
    )]
    parameters: crate::types::HeldParameters,
    #[serde(
        rename = "Limit",
        default,
        deserialize_with = "super::helpers::deserialize_int32"
    )]
    limit: Option<super::helpers::Int32>,
    #[serde(rename = "NextToken", default)]
    next_token: Option<String>,
    #[serde(rename = "ConsistentRead", default)]
    consistent_read: Option<bool>,
    #[serde(rename = "ReturnConsumedCapacity", default)]
    return_consumed_capacity: Option<String>,
    #[serde(rename = "ReturnValuesOnConditionCheckFailure", default)]
    return_values_on_condition_check_failure: Option<String>,
}

impl<'de> Deserialize<'de> for ExecuteStatementRequest {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let raw = ExecuteStatementRequestRaw::deserialize(deserializer)?;
        // DynamoDB reads `Limit` as a 32-bit integer: a fraction truncates
        // toward zero and a value past either end saturates there, so `1.5`
        // reads as 1 and `1e+20` as every row (captured eu-west-2 and
        // us-east-1, 5 November 2026). Unlike Query and Scan, a value that lost
        // a fraction or saturated is checked like any other.
        let limit = raw.limit.map(|n| n.value);
        // A Limit below 1 can't be carried on to `execute`, whose field holds
        // no negative, and a refused parameter is answered only after the
        // member errors, so in either case they are listed here, with the
        // value echoed as read, in the order `execute` lists them.
        let refusal = raw.parameters.refusal;
        if refusal.is_some() || limit.is_some_and(|l| l < 1) {
            let violations = member_violations(
                raw.return_values_on_condition_check_failure.as_deref(),
                limit,
                &raw.statement,
                raw.return_consumed_capacity.as_deref(),
                raw.parameters.values.as_deref(),
            );
            if let Some(msg) = crate::validation::format_validation_errors(&violations) {
                return Err(serde::de::Error::custom(format!(
                    "{}{msg}",
                    crate::serde_errors::VALIDATION_MARKER
                )));
            }
        }
        if let Some(refusal) = refusal {
            return Err(serde::de::Error::custom(refusal));
        }
        Ok(ExecuteStatementRequest {
            statement: raw.statement,
            parameters: raw.parameters.values,
            // At least 1 here, and at most i32::MAX.
            limit: limit.map(|l| l as usize),
            next_token: raw.next_token,
            consistent_read: raw.consistent_read,
            return_consumed_capacity: raw.return_consumed_capacity,
            return_values_on_condition_check_failure: raw.return_values_on_condition_check_failure,
        })
    }
}

/// Every member DynamoDB refuses, in DynamoDB's order:
/// ReturnValuesOnConditionCheckFailure, Limit, the statement,
/// ReturnConsumedCapacity, the parameters (captured eu-west-2 and us-east-1,
/// 2026-10-10). A Limit below 1 is worded as Scan words it (value kept,
/// lowercase 'limit'), not as Query does, matching what ExecuteStatement
/// itself returns (captured eu-west-2, 2026-07-29).
fn member_violations(
    return_values_on_condition_check_failure: Option<&str>,
    limit: Option<i64>,
    statement: &str,
    return_consumed_capacity: Option<&str>,
    parameters: Option<&[AttributeValue]>,
) -> Vec<String> {
    [
        crate::validation::return_values_on_condition_check_failure_rejection(
            return_values_on_condition_check_failure,
            "returnValuesOnConditionCheckFailure",
        ),
        limit.filter(|l| *l < 1).map(|l| {
            format!(
                "Value '{l}' at 'limit' failed to satisfy constraint: Member must have value \
                 greater than or equal to 1"
            )
        }),
        crate::partiql::statement_length_violation(statement, "statement"),
        crate::validation::return_consumed_capacity_rejection(return_consumed_capacity),
        crate::partiql::parameters_violation(parameters, "parameters"),
    ]
    .into_iter()
    .flatten()
    .collect()
}

#[derive(Debug, Default, Serialize)]
pub struct ExecuteStatementResponse {
    #[serde(
        rename = "Items",
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::set_order::items_in_hash_order"
    )]
    // Number sets in Java hash-set order here, as DynamoDB returns them;
    // pinned by set_order's every_response_item_field test.
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
    // Every member DynamoDB refuses is listed in one error, checked before the
    // statement is parsed. A request read off the wire with a Limit below 1
    // was refused as it was read; this covers one built in code, where a zero
    // limit would otherwise read nothing and mint no token, silently ending a
    // paginated walk.
    let violations = member_violations(
        request.return_values_on_condition_check_failure.as_deref(),
        (request.limit == Some(0)).then_some(0),
        &request.statement,
        request.return_consumed_capacity.as_deref(),
        request.parameters.as_deref(),
    );
    if let Some(e) = crate::partiql::member_rejection(&violations) {
        return Err(e);
    }

    // A parameter holding an empty attribute name is refused bare, before the
    // statement is parsed or its table looked up (captured in ten regions).
    if crate::empty_names::parameters_have_empty_name(request.parameters.as_deref()) {
        return Err(crate::empty_names::bare());
    }

    // A parameter nested too deep is refused before the statement is parsed or
    // its table looked up.
    if crate::value_nesting::parameters_nest_too_deep(
        request.parameters.as_deref().unwrap_or_default(),
    ) {
        return Err(crate::value_nesting::deep_parameter_error());
    }

    // Every parameter's numbers are checked before the statement is read,
    // whether the statement uses the parameter or not, with the message any
    // attribute value gets and no path after it (captured eu-west-2 and
    // us-east-1, 2026-10-09).
    for parameter in request.parameters.iter().flatten() {
        partiql::executor::check_parameter_value(parameter)?;
    }

    // A COUNT projection is rejected before parsing, with DynamoDB's bare
    // message rather than the wasn't-well-formed wrapper the parse errors
    // below carry. ExecuteStatement is the captured surface for this shape.
    // A read of a multi-attribute index is refused ahead of it (captured
    // eu-west-2 and us-east-1, 2026-10-09).
    if let Some(msg) = partiql::parser::count_projection_rejection(&request.statement) {
        if let Some((table, Some(index))) = partiql::parser::select_target(&request.statement) {
            partiql::executor::reject_multi_attribute_index_by_name(storage, &table, &index)
                .await?;
        }
        return Err(DynoxideError::ValidationException(msg));
    }

    // The parser says which envelope its rejection takes: a malformed statement
    // gets DynamoDB's "wasn't well formed" wrapper, while a rejection DynamoDB
    // reports on its own terms is passed through bare.
    let supplied = request.parameters.as_ref().map_or(0, Vec::len);
    let stmt = partiql::parser::parse_with_parameters(&request.statement, supplied).map_err(
        |failure| {
            DynoxideError::ValidationException(match failure {
                partiql::parser::ParseFailure::Statement(e) => e.into_message(),
                partiql::parser::ParseFailure::Literal(message) => message,
            })
        },
    )?;

    let params = request.parameters.unwrap_or_default();
    // The statement's text goes with it, so a continuation token is bound to
    // the statement as written, as DynamoDB binds it.
    let run = partiql::executor::execute_written_page(
        storage,
        &request.statement,
        &stmt,
        &params,
        // A Limit past what a 32-bit integer holds is read as the largest one,
        // as Query and Scan read it. DynamoDB accepts 2147483648 and reads as
        // usual (captured eu-west-2 and us-east-1, 2026-11-04), where passing
        // the value on overflowed SQLite's integer for a read past 2^63 - 1.
        request.limit.map(|l| l.min(i32::MAX as usize)),
        request.next_token.as_deref(),
        request.consistent_read.unwrap_or(false),
        request.return_consumed_capacity.as_deref(),
        // A single statement has no preparation pass to share a resolution
        // with, so the executor resolves the table itself, once.
        None,
        request.return_values_on_condition_check_failure.as_deref() == Some("ALL_OLD"),
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
        size: _,
        read_units,
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
            let consistent = request.consistent_read.unwrap_or(false);
            let units = if consistent {
                read_units
            } else {
                read_units / 2.0
            };
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

    // A write without a RETURNING clause answers with an empty `Items` list,
    // not without one (captured eu-west-2, us-east-1, eu-west-1 and
    // us-west-2, 2026-10-09).
    Ok(ExecuteStatementResponse {
        items: Some(items.unwrap_or_default()),
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
