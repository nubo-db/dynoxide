use crate::actions::helpers;
use crate::errors::{DynoxideError, Result};
use crate::expressions;
use crate::storage::Storage;
use crate::types::{AttributeValue, Item};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Default, Deserialize)]
pub struct TransactGetItemsRequest {
    #[serde(rename = "TransactItems")]
    pub transact_items: Vec<TransactGetItem>,
    #[serde(rename = "ReturnConsumedCapacity", default)]
    pub return_consumed_capacity: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
pub struct TransactGetItem {
    #[serde(rename = "Get")]
    pub get: TransactGet,
}

#[derive(Debug, Default, Deserialize)]
pub struct TransactGet {
    #[serde(rename = "TableName")]
    pub table_name: String,
    #[serde(rename = "Key")]
    pub key: HashMap<String, AttributeValue>,
    #[serde(rename = "ProjectionExpression", default)]
    pub projection_expression: Option<String>,
    #[serde(rename = "ExpressionAttributeNames", default)]
    pub expression_attribute_names: Option<HashMap<String, String>>,
}

#[derive(Debug, Default, Serialize)]
pub struct TransactGetItemsResponse {
    #[serde(rename = "Responses")]
    pub responses: Vec<TransactGetResponse>,
    #[serde(rename = "ConsumedCapacity", skip_serializing_if = "Option::is_none")]
    pub consumed_capacity: Option<Vec<crate::types::ConsumedCapacity>>,
}

#[derive(Debug, Default, Serialize)]
pub struct TransactGetResponse {
    #[serde(rename = "Item", skip_serializing_if = "Option::is_none")]
    pub item: Option<Item>,
}

pub fn execute(
    storage: &Storage,
    request: TransactGetItemsRequest,
) -> Result<TransactGetItemsResponse> {
    // Validate: at least 1 action
    if request.transact_items.is_empty() {
        return Err(DynoxideError::ValidationException(
            "1 validation error detected: Value '[]' at 'transactItems' failed to satisfy constraint: Member must have length greater than or equal to 1".to_string(),
        ));
    }

    // Validate: up to 100 actions
    if request.transact_items.len() > 100 {
        return Err(DynoxideError::ValidationException(
            "Member must have length less than or equal to 100".to_string(),
        ));
    }

    // Validate: no duplicate item targets
    let mut seen_targets = HashSet::new();
    for transact_item in &request.transact_items {
        let get = &transact_item.get;
        crate::validation::validate_table_name(&get.table_name)?;
        let meta = helpers::require_table_for_item_op(storage, &get.table_name)?;
        let key_schema = helpers::parse_key_schema(&meta)?;
        let (pk, sk) = helpers::extract_key_strings(&get.key, &key_schema)?;
        let target = format!("{}#{}#{}", get.table_name, pk, sk);
        if !seen_targets.insert(target) {
            return Err(DynoxideError::ValidationException(
                "Transaction request cannot include multiple operations on one item".to_string(),
            ));
        }
    }

    let mut responses = Vec::with_capacity(request.transact_items.len());

    for transact_item in &request.transact_items {
        let get = &transact_item.get;
        crate::validation::validate_table_name(&get.table_name)?;
        let meta = helpers::require_table_for_item_op(storage, &get.table_name)?;
        let key_schema = helpers::parse_key_schema(&meta)?;

        helpers::validate_key_only(&get.key, &key_schema)?;
        let (pk, sk) = helpers::extract_key_strings(&get.key, &key_schema)?;

        let item_json = storage.get_item(&get.table_name, &pk, &sk)?;

        let item: Option<Item> = item_json.and_then(|j| serde_json::from_str(&j).ok());

        // Apply projection if present
        let tracker = crate::expressions::TrackedExpressionAttributes::new(
            &get.expression_attribute_names,
            &None, // TransactGet has no ExpressionAttributeValues
        );

        let item = if let Some(proj_expr) = &get.projection_expression {
            let projection = expressions::projection::parse(proj_expr)
                .map_err(DynoxideError::ValidationException)?;
            tracker.track_projection_expr(&projection);

            if let Some(item) = item {
                let mut key_attrs = vec![key_schema.partition_key.clone()];
                if let Some(ref sk) = key_schema.sort_key {
                    key_attrs.push(sk.clone());
                }

                let projected =
                    expressions::projection::apply(&item, &projection, &tracker, &key_attrs)
                        .map_err(DynoxideError::ValidationException)?;
                Some(projected)
            } else {
                None
            }
        } else {
            item
        };

        tracker.check_unused()?;

        responses.push(TransactGetResponse { item });
    }

    // Build consumed capacity per table
    let consumed_capacity = if matches!(
        request.return_consumed_capacity.as_deref(),
        Some("TOTAL") | Some("INDEXES")
    ) {
        let mut table_sizes: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();
        for (resp, req_item) in responses.iter().zip(request.transact_items.iter()) {
            let size = resp.item.as_ref().map(crate::types::item_size).unwrap_or(0);
            *table_sizes
                .entry(req_item.get.table_name.clone())
                .or_default() += size;
        }
        let caps: Vec<_> = table_sizes
            .iter()
            .filter_map(|(table, &size)| {
                crate::types::consumed_capacity(
                    table,
                    crate::types::read_capacity_units_with_consistency(size, true),
                    &request.return_consumed_capacity,
                )
            })
            .collect();
        Some(caps)
    } else {
        None
    };

    Ok(TransactGetItemsResponse {
        responses,
        consumed_capacity,
    })
}
