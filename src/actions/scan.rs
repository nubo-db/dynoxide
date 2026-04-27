use crate::actions::helpers;
use crate::errors::{DynoxideError, Result};
use crate::expressions;
use crate::storage::Storage;
use crate::types::{AttributeValue, Item};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// 1MB response size limit for Query/Scan.
const MAX_RESPONSE_SIZE: usize = 1_048_576;

/// Internal deserialization struct for detecting missing fields.
#[derive(Debug, Default, Deserialize)]
struct ScanRequestRaw {
    #[serde(rename = "TableName", default)]
    table_name: Option<String>,
    #[serde(rename = "FilterExpression", default)]
    filter_expression: Option<String>,
    #[serde(rename = "ProjectionExpression", default)]
    projection_expression: Option<String>,
    #[serde(rename = "ExpressionAttributeNames", default)]
    expression_attribute_names: Option<HashMap<String, String>>,
    #[serde(rename = "ExpressionAttributeValues", default)]
    expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    #[serde(rename = "Limit", default)]
    limit: Option<usize>,
    #[serde(rename = "ExclusiveStartKey", default)]
    exclusive_start_key: Option<serde_json::Value>,
    #[serde(rename = "Select", default)]
    select: Option<String>,
    #[serde(rename = "ConsistentRead", default)]
    consistent_read: Option<bool>,
    #[serde(rename = "IndexName", default)]
    index_name: Option<String>,
    #[serde(rename = "Segment", default)]
    segment: Option<u32>,
    #[serde(rename = "TotalSegments", default)]
    total_segments: Option<u32>,
    #[serde(rename = "ReturnConsumedCapacity", default)]
    return_consumed_capacity: Option<String>,
    #[serde(rename = "AttributesToGet", default)]
    attributes_to_get: Option<Vec<String>>,
    #[serde(rename = "ScanFilter", default)]
    scan_filter: Option<serde_json::Value>,
    #[serde(rename = "ConditionalOperator", default)]
    conditional_operator: Option<String>,
}

#[derive(Debug, Default)]
pub struct ScanRequest {
    pub table_name: String,
    pub filter_expression: Option<String>,
    pub projection_expression: Option<String>,
    pub expression_attribute_names: Option<HashMap<String, String>>,
    pub expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    pub limit: Option<usize>,
    pub exclusive_start_key: Option<HashMap<String, AttributeValue>>,
    pub select: Option<String>,
    pub consistent_read: Option<bool>,
    pub index_name: Option<String>,
    pub segment: Option<u32>,
    pub total_segments: Option<u32>,
    pub return_consumed_capacity: Option<String>,
    pub attributes_to_get: Option<Vec<String>>,
    pub scan_filter: Option<serde_json::Value>,
    pub conditional_operator: Option<String>,
    /// Raw JSON for ExclusiveStartKey when deserialized from HTTP request.
    /// Parsed lazily in `execute()` after other validations run.
    pub exclusive_start_key_raw: Option<serde_json::Value>,
}

impl<'de> serde::Deserialize<'de> for ScanRequest {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let raw = ScanRequestRaw::deserialize(deserializer)?;
        use crate::validation::{
            format_validation_errors, table_name_constraint_errors, TableNameContext,
        };

        let mut errors = Vec::new();
        errors.extend(table_name_constraint_errors(
            raw.table_name.as_deref(),
            TableNameContext::ReadWrite,
        ));
        let table_name = raw.table_name.unwrap_or_default();

        // ReturnConsumedCapacity enum
        if let Some(ref rcc) = raw.return_consumed_capacity {
            if !["INDEXES", "TOTAL", "NONE"].contains(&rcc.as_str()) {
                errors.push(format!(
                    "Value '{}' at 'returnConsumedCapacity' failed to satisfy constraint: \
                     Member must satisfy enum value set: [INDEXES, TOTAL, NONE]",
                    rcc
                ));
            }
        }

        // Select enum
        if let Some(ref sel) = raw.select {
            if ![
                "ALL_ATTRIBUTES",
                "ALL_PROJECTED_ATTRIBUTES",
                "COUNT",
                "SPECIFIC_ATTRIBUTES",
            ]
            .contains(&sel.as_str())
            {
                errors.push(format!(
                    "Value '{}' at 'select' failed to satisfy constraint: \
                     Member must satisfy enum value set: [SPECIFIC_ATTRIBUTES, COUNT, ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES]",
                    sel
                ));
            }
        }

        // Limit must be >= 1
        if let Some(limit) = raw.limit {
            if limit == 0 {
                errors.push(
                    "Value '0' at 'Limit' failed to satisfy constraint: \
                     Member must have value greater than or equal to 1"
                        .to_string(),
                );
            }
        }

        if let Some(msg) = format_validation_errors(&errors) {
            return Err(serde::de::Error::custom(format!("VALIDATION:{}", msg)));
        }

        Ok(ScanRequest {
            table_name,
            filter_expression: raw.filter_expression,
            projection_expression: raw.projection_expression,
            expression_attribute_names: raw.expression_attribute_names,
            expression_attribute_values: raw.expression_attribute_values,
            limit: raw.limit,
            exclusive_start_key: None,
            select: raw.select,
            consistent_read: raw.consistent_read,
            index_name: raw.index_name,
            segment: raw.segment,
            total_segments: raw.total_segments,
            return_consumed_capacity: raw.return_consumed_capacity,
            attributes_to_get: raw.attributes_to_get,
            scan_filter: raw.scan_filter,
            conditional_operator: raw.conditional_operator,
            exclusive_start_key_raw: raw.exclusive_start_key,
        })
    }
}

#[derive(Debug, Default, Serialize)]
pub struct ScanResponse {
    #[serde(rename = "Items", skip_serializing_if = "Option::is_none")]
    pub items: Option<Vec<Item>>,
    #[serde(rename = "Count")]
    pub count: usize,
    #[serde(rename = "ScannedCount")]
    pub scanned_count: usize,
    #[serde(rename = "LastEvaluatedKey", skip_serializing_if = "Option::is_none")]
    pub last_evaluated_key: Option<HashMap<String, AttributeValue>>,
    #[serde(rename = "ConsumedCapacity", skip_serializing_if = "Option::is_none")]
    pub consumed_capacity: Option<crate::types::ConsumedCapacity>,
}

pub fn execute(storage: &Storage, mut request: ScanRequest) -> Result<ScanResponse> {
    // Validate table name format before checking existence (DynamoDB validates input first)
    crate::validation::validate_table_name(&request.table_name)?;

    // ---- Expression vs non-expression mixing validation ----
    {
        let mut non_expr = Vec::new();
        let mut expr = Vec::new();
        if request.attributes_to_get.is_some() {
            non_expr.push("AttributesToGet");
        }
        if request.scan_filter.is_some()
            && request.scan_filter.as_ref().is_some_and(|v| !v.is_null())
        {
            non_expr.push("ScanFilter");
        }
        if request.conditional_operator.is_some() {
            non_expr.push("ConditionalOperator");
        }
        if request.projection_expression.is_some() {
            expr.push("ProjectionExpression");
        }
        if request.filter_expression.is_some() {
            expr.push("FilterExpression");
        }
        let no_raw_eav: Option<serde_json::Value> = None;
        let ctx = helpers::ExpressionParamContext {
            non_expression_params: non_expr,
            expression_params: expr,
            all_expression_param_names: vec!["FilterExpression"],
            expression_attribute_names: &request.expression_attribute_names,
            expression_attribute_values: &request.expression_attribute_values,
            expression_attribute_values_raw: &no_raw_eav,
        };
        helpers::validate_expression_params(&ctx)?;
    }

    // ---- Validate ScanFilter attribute values (before argument counts, matching DynamoDB) ----
    helpers::validate_filter_conditions_raw(request.scan_filter.as_ref(), "ScanFilter")?;

    // ---- Validate filter argument counts and type compatibility ----
    helpers::validate_filter_condition_args(request.scan_filter.as_ref())?;

    // ---- Validate duplicate AttributesToGet ----
    if let Some(ref attrs) = request.attributes_to_get {
        helpers::validate_attributes_to_get_no_duplicates(attrs)?;
    }

    // ---- Parse ExclusiveStartKey from JSON value ----
    let exclusive_start_key = if let Some(ref esk_val) = request.exclusive_start_key_raw {
        Some(helpers::parse_exclusive_start_key(esk_val)?)
    } else {
        request.exclusive_start_key.clone()
    };

    // Convert legacy ScanFilter to FilterExpression if no expression is set
    if request.filter_expression.is_none() {
        if let Some(ref sf_val) = request.scan_filter {
            if let Ok(sf) =
                serde_json::from_value::<HashMap<String, helpers::FilterCondition>>(sf_val.clone())
            {
                if !sf.is_empty() {
                    let converted = helpers::convert_filter_conditions(
                        &sf,
                        request.conditional_operator.as_deref(),
                    )?;
                    if !converted.expression.is_empty() {
                        request.filter_expression = Some(converted.expression);
                        let expr_values = request
                            .expression_attribute_values
                            .get_or_insert_with(HashMap::new);
                        expr_values.extend(converted.attribute_values);
                        let expr_names = request
                            .expression_attribute_names
                            .get_or_insert_with(HashMap::new);
                        expr_names.extend(converted.attribute_names);
                    }
                }
            }
        }
    }

    // Validate parallel scan parameters (before table existence check)
    match (request.segment, request.total_segments) {
        (Some(segment), Some(total)) => {
            if !(1..=1_000_000).contains(&total) {
                return Err(DynoxideError::ValidationException(
                    "1 validation error detected: Value at 'totalSegments' failed to satisfy constraint: \
                     Member must have value between 1 and 1000000".to_string(),
                ));
            }
            if segment >= total {
                return Err(DynoxideError::ValidationException(format!(
                    "The Segment parameter is zero-based and must be less than parameter TotalSegments: Segment: {} is not less than TotalSegments: {}",
                    segment, total
                )));
            }
        }
        (Some(_), None) => {
            return Err(DynoxideError::ValidationException(
                "The TotalSegments parameter is required but was not present in the request when Segment parameter is present".to_string(),
            ));
        }
        (None, Some(_)) => {
            return Err(DynoxideError::ValidationException(
                "The Segment parameter is required but was not present in the request when parameter TotalSegments is present".to_string(),
            ));
        }
        (None, None) => {}
    }

    // ---- Validate FilterExpression and ProjectionExpression BEFORE table existence ----
    // DynamoDB validates expression syntax before checking if the table exists.
    if let Some(ref filter_expr_str) = request.filter_expression {
        if filter_expr_str.is_empty() {
            // Only report empty if the user explicitly set FilterExpression
            // (not if it was converted from ScanFilter, which never produces empty)
            if request.scan_filter.is_none() || request.filter_expression.as_deref() == Some("") {
                return Err(DynoxideError::ValidationException(
                    "Invalid FilterExpression: The expression can not be empty;".to_string(),
                ));
            }
        } else {
            // Try parsing the expression to catch syntax errors early
            let parsed_fe = expressions::condition::parse(filter_expr_str).map_err(|e| {
                DynoxideError::ValidationException(format!("Invalid FilterExpression: {e}"))
            })?;
            // Validate that all #name references are defined in ExpressionAttributeNames
            if let Err(e) = expressions::condition::validate_name_refs(
                &parsed_fe,
                &request.expression_attribute_names,
            ) {
                return Err(DynoxideError::ValidationException(format!(
                    "Invalid FilterExpression: {e}"
                )));
            }
        }
    }
    if let Some(ref proj_expr_str) = request.projection_expression {
        if proj_expr_str.is_empty() {
            return Err(DynoxideError::ValidationException(
                "Invalid ProjectionExpression: The expression can not be empty;".to_string(),
            ));
        }
    }

    // SPECIFIC_ATTRIBUTES requires ProjectionExpression or AttributesToGet
    if request.select.as_deref() == Some("SPECIFIC_ATTRIBUTES")
        && request.projection_expression.is_none()
        && request.attributes_to_get.is_none()
    {
        return Err(DynoxideError::ValidationException(
            "SPECIFIC_ATTRIBUTES requires either ProjectionExpression or AttributesToGet"
                .to_string(),
        ));
    }

    let meta = helpers::require_table_for_item_op(storage, &request.table_name)?;
    let table_key_schema = helpers::parse_key_schema(&meta)?;

    // Convert legacy AttributesToGet to ProjectionExpression if no expression-based
    // projection is provided.
    let legacy_projection = if request.projection_expression.is_none() {
        request
            .attributes_to_get
            .as_ref()
            .map(|attrs| helpers::attributes_to_get_to_projection(attrs))
    } else {
        None
    };

    // Determine effective key schema (GSI, LSI, or base table)
    let lsi_keys = request
        .index_name
        .as_ref()
        .and_then(|idx| super::lsi::parse_lsi_key_schema(&meta, idx).ok());
    let is_lsi = lsi_keys.is_some();

    // ConsistentRead is not supported on GSIs (LSIs are fine)
    if request.consistent_read.unwrap_or(false) && request.index_name.is_some() && !is_lsi {
        return Err(DynoxideError::ValidationException(
            "Consistent reads are not supported on global secondary indexes".to_string(),
        ));
    }

    let (effective_pk, effective_sk) = if let Some(ref index_name) = request.index_name {
        if let Some(keys) = lsi_keys {
            keys
        } else {
            super::gsi::parse_gsi_key_schema(&meta, index_name)?
        }
    } else {
        (
            table_key_schema.partition_key.clone(),
            table_key_schema.sort_key.clone(),
        )
    };

    // ---- Validate ExclusiveStartKey structure against key schema ----
    // Stage 1+2: count check + index key type check
    if let Some(ref esk) = exclusive_start_key {
        let count_msg = if request.index_name.is_some() {
            "The provided starting key is invalid"
        } else {
            "The provided starting key is invalid: The provided key element does not match the schema"
        };
        helpers::validate_esk_count_and_index_keys(
            esk,
            &meta,
            request.index_name.as_deref(),
            count_msg,
        )?;
    }

    // Check ALL_ATTRIBUTES on global index (between index key check and table key check)
    if let Some(ref index_name) = request.index_name {
        if !is_lsi {
            if let Some(ref select) = request.select {
                if select == "ALL_ATTRIBUTES" {
                    // Check if index projection is ALL
                    let gsi_defs = super::gsi::parse_gsi_defs(&meta)?;
                    if let Some(gsi) = gsi_defs.iter().find(|g| g.index_name == *index_name) {
                        if gsi.projection_type != crate::types::ProjectionType::ALL {
                            return Err(DynoxideError::ValidationException(format!(
                                "One or more parameter values were invalid: \
                                 Select type ALL_ATTRIBUTES is not supported for global secondary index {} \
                                 because its projection type is not ALL",
                                index_name
                            )));
                        }
                    }
                }
            }
        }
    }

    // Stage 3: table key type check
    if let Some(ref esk) = exclusive_start_key {
        helpers::validate_esk_table_keys(esk, &meta)?;
    }

    // Extract ExclusiveStartKey pk/sk using effective key names
    let (start_pk, start_sk) = if let Some(ref esk) = exclusive_start_key {
        let pk = esk.get(&effective_pk).and_then(|v| v.to_key_string());
        let sk = if let Some(ref sk_name) = effective_sk {
            esk.get(sk_name).and_then(|v| v.to_key_string())
        } else {
            Some(String::new())
        };
        (pk, sk)
    } else {
        (None, None)
    };

    // For index scans (LSI and GSI), extract the base table key from
    // ExclusiveStartKey for composite cursor pagination. The GSI/LSI
    // tables have a composite primary key that includes the base table
    // keys, so the cursor must include them to avoid skipping rows that
    // share the same index key.
    let (start_base_pk, start_base_sk) = if is_lsi || request.index_name.is_some() {
        if let Some(ref esk) = exclusive_start_key {
            let base_pk = esk
                .get(&table_key_schema.partition_key)
                .and_then(|v| v.to_key_string());
            let base_sk = table_key_schema
                .sort_key
                .as_ref()
                .and_then(|sk_name| esk.get(sk_name))
                .and_then(|v| v.to_key_string());
            (base_pk, base_sk)
        } else {
            (None, None)
        }
    } else {
        (None, None)
    };

    // Scan either GSI table or base table
    let scan_params = crate::storage::ScanParams {
        limit: request.limit,
        exclusive_start_pk: start_pk.as_deref(),
        exclusive_start_sk: start_sk.as_deref(),
        segment: request.segment,
        total_segments: request.total_segments,
        exclusive_start_base_pk: start_base_pk.as_deref(),
        exclusive_start_base_sk: start_base_sk.as_deref(),
    };
    let rows = if let Some(ref index_name) = request.index_name {
        if is_lsi {
            storage.scan_lsi_items(&request.table_name, index_name, &scan_params)?
        } else {
            storage.scan_gsi_items(&request.table_name, index_name, &scan_params)?
        }
    } else {
        storage.scan_items(&request.table_name, &scan_params)?
    };

    // Create tracker for unused expression attribute names/values
    let tracker = crate::expressions::TrackedExpressionAttributes::new(
        &request.expression_attribute_names,
        &request.expression_attribute_values,
    );

    // Parse filter expression if present
    let filter_expr = request
        .filter_expression
        .as_ref()
        .map(|expr| expressions::condition::parse(expr))
        .transpose()
        .map_err(DynoxideError::ValidationException)?;

    // Check for non-scalar key access in FilterExpression
    if let Some(ref filter) = filter_expr {
        // Build key attribute lists for non-scalar check
        let mut base_key_attrs = vec![table_key_schema.partition_key.clone()];
        if let Some(ref sk) = table_key_schema.sort_key {
            base_key_attrs.push(sk.clone());
        }
        let mut index_key_attrs = Vec::new();
        if request.index_name.is_some() {
            if !base_key_attrs.contains(&effective_pk) {
                index_key_attrs.push(effective_pk.clone());
            }
            if let Some(ref sk) = effective_sk {
                if !base_key_attrs.contains(sk) {
                    index_key_attrs.push(sk.clone());
                }
            }
        }
        if let Some((attr, is_index)) = expressions::condition::check_non_scalar_key_access(
            filter,
            &request.expression_attribute_names,
            &base_key_attrs,
            &index_key_attrs,
        ) {
            let prefix = if is_index { "IndexKey" } else { "Key" };
            return Err(DynoxideError::ValidationException(format!(
                "Key attributes must be scalars; \
                 list random access '[]' and map lookup '.' are not allowed: {prefix}: {attr}"
            )));
        }
    }

    // Parse projection expression if present; fall back to legacy AttributesToGet
    let projection = if let Some(ref proj_expr) = request.projection_expression {
        Some(
            expressions::projection::parse(proj_expr)
                .map_err(DynoxideError::ValidationException)?,
        )
    } else {
        legacy_projection.clone()
    };

    // Pre-register expression references so unused check works even with zero items
    if let Some(ref filter) = filter_expr {
        tracker.track_condition_expr(filter);
    }
    if let Some(ref proj) = projection {
        tracker.track_projection_expr(proj);
    }

    // Untracked variant for the per-item hot loop — tracking already done above
    let loop_tracker = crate::expressions::TrackedExpressionAttributes::without_tracking(
        &request.expression_attribute_names,
        &request.expression_attribute_values,
    );

    // Determine if SELECT COUNT
    let is_count = request
        .select
        .as_deref()
        .map(|s| s.eq_ignore_ascii_case("COUNT"))
        .unwrap_or(false);

    // Key attribute names for projection (use effective keys for GSI)
    let mut key_attrs = vec![effective_pk.clone()];
    if let Some(ref sk) = effective_sk {
        key_attrs.push(sk.clone());
    }
    if request.index_name.is_some() {
        if !key_attrs.contains(&table_key_schema.partition_key) {
            key_attrs.push(table_key_schema.partition_key.clone());
        }
        if let Some(ref sk) = table_key_schema.sort_key {
            if !key_attrs.contains(sk) {
                key_attrs.push(sk.clone());
            }
        }
    }

    let mut items = Vec::new();
    let mut scanned_count = 0;
    let mut filtered_count = 0;
    let mut cumulative_size = 0;
    let mut last_evaluated_item: Option<Item> = None;
    let mut truncated_by_size = false;

    for (_pk, _sk, item_json) in &rows {
        let item: Item = serde_json::from_str(item_json).map_err(|e| {
            DynoxideError::InternalServerError(format!("Bad item JSON in storage: {e}"))
        })?;

        scanned_count += 1;

        // Check 1MB limit BEFORE filtering — DynamoDB counts all evaluated data
        // towards the 1MB response size limit, not just items that pass the filter.
        let item_size = crate::types::item_size(&item);
        if cumulative_size + item_size > MAX_RESPONSE_SIZE && scanned_count > 1 {
            truncated_by_size = true;
            break;
        }
        cumulative_size += item_size;

        // Apply filter
        if let Some(ref filter) = filter_expr {
            let passes = expressions::condition::evaluate(filter, &item, &loop_tracker)
                .map_err(DynoxideError::ValidationException)?;
            if !passes {
                last_evaluated_item = Some(item);
                continue;
            }
        }

        filtered_count += 1;

        // Apply projection -- do NOT auto-include key attributes when the
        // user explicitly specified ProjectionExpression or AttributesToGet.
        let result_item = if let Some(ref proj) = projection {
            let no_keys: &[String] = &[];
            expressions::projection::apply(&item, proj, &loop_tracker, no_keys)
                .map_err(DynoxideError::ValidationException)?
        } else {
            item.clone()
        };

        last_evaluated_item = Some(item);
        if !is_count {
            items.push(result_item);
        }
    }

    // Check for unused expression attribute names/values
    tracker.check_unused()?;

    let count = if is_count {
        filtered_count
    } else {
        items.len()
    };

    // Determine LastEvaluatedKey
    let has_more = truncated_by_size
        || (request.limit.is_some() && scanned_count >= request.limit.unwrap_or(usize::MAX));

    // For index scans, include the base table primary key in LastEvaluatedKey
    // alongside the effective (index) keys so the cursor can uniquely identify
    // the position. For LSIs, include the table sort key. For GSIs, include
    // both the table partition key and sort key.
    let is_gsi_scan = request.index_name.is_some() && !is_lsi;
    let last_evaluated_key = if has_more {
        last_evaluated_item.map(|item| {
            let mut key = HashMap::new();
            if let Some(pk_val) = item.get(&effective_pk) {
                key.insert(effective_pk.clone(), pk_val.clone());
            }
            if let Some(ref sk_name) = effective_sk {
                if let Some(sk_val) = item.get(sk_name) {
                    key.insert(sk_name.clone(), sk_val.clone());
                }
            }
            // For LSI scans, add the table sort key if different from the index sort key
            if is_lsi {
                if let Some(tsk) = table_key_schema.sort_key.as_deref() {
                    if !key.contains_key(tsk) {
                        if let Some(v) = item.get(tsk) {
                            key.insert(tsk.to_string(), v.clone());
                        }
                    }
                }
            }
            // For GSI scans, add the base table primary key (pk and sk)
            if is_gsi_scan {
                if !key.contains_key(&table_key_schema.partition_key) {
                    if let Some(v) = item.get(&table_key_schema.partition_key) {
                        key.insert(table_key_schema.partition_key.clone(), v.clone());
                    }
                }
                if let Some(ref tsk) = table_key_schema.sort_key {
                    if !key.contains_key(tsk) {
                        if let Some(v) = item.get(tsk) {
                            key.insert(tsk.clone(), v.clone());
                        }
                    }
                }
            }
            key
        })
    } else {
        None
    };

    // Attribute read capacity to the GSI if scanning one
    let is_gsi = is_gsi_scan;
    let consistent = request.consistent_read.unwrap_or(false);
    let consumed_capacity = if is_gsi {
        let mut gsi_units = std::collections::HashMap::new();
        gsi_units.insert(
            request.index_name.as_ref().unwrap().clone(),
            crate::types::read_capacity_units_with_consistency(cumulative_size, consistent),
        );
        crate::types::consumed_capacity_with_indexes(
            &request.table_name,
            0.0,
            &gsi_units,
            &request.return_consumed_capacity,
        )
    } else {
        crate::types::consumed_capacity(
            &request.table_name,
            crate::types::read_capacity_units_with_consistency(cumulative_size, consistent),
            &request.return_consumed_capacity,
        )
    };

    Ok(ScanResponse {
        items: if is_count { None } else { Some(items) },
        count,
        scanned_count,
        last_evaluated_key,
        consumed_capacity,
    })
}
