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
struct QueryRequestRaw {
    #[serde(rename = "TableName", default)]
    table_name: Option<String>,
    #[serde(rename = "KeyConditionExpression", default)]
    key_condition_expression: Option<String>,
    #[serde(rename = "FilterExpression", default)]
    filter_expression: Option<String>,
    #[serde(rename = "ProjectionExpression", default)]
    projection_expression: Option<String>,
    #[serde(rename = "ExpressionAttributeNames", default)]
    expression_attribute_names: Option<HashMap<String, String>>,
    #[serde(rename = "ExpressionAttributeValues", default)]
    expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    #[serde(rename = "ScanIndexForward", default = "default_true")]
    scan_index_forward: bool,
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
    #[serde(rename = "ReturnConsumedCapacity", default)]
    return_consumed_capacity: Option<String>,
    #[serde(rename = "KeyConditions", default)]
    key_conditions: Option<serde_json::Value>,
    #[serde(rename = "AttributesToGet", default)]
    attributes_to_get: Option<Vec<String>>,
    #[serde(rename = "QueryFilter", default)]
    query_filter: Option<serde_json::Value>,
    #[serde(rename = "ConditionalOperator", default)]
    conditional_operator: Option<String>,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Default)]
pub struct QueryRequest {
    pub table_name: String,
    pub key_condition_expression: Option<String>,
    pub filter_expression: Option<String>,
    pub projection_expression: Option<String>,
    pub expression_attribute_names: Option<HashMap<String, String>>,
    pub expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    pub scan_index_forward: bool,
    pub limit: Option<usize>,
    pub exclusive_start_key: Option<HashMap<String, AttributeValue>>,
    pub select: Option<String>,
    pub consistent_read: Option<bool>,
    pub index_name: Option<String>,
    pub return_consumed_capacity: Option<String>,
    pub key_conditions: Option<serde_json::Value>,
    pub attributes_to_get: Option<Vec<String>>,
    pub query_filter: Option<serde_json::Value>,
    pub conditional_operator: Option<String>,
    /// Raw JSON for ExclusiveStartKey when deserialized from HTTP request.
    /// Parsed lazily in `execute()` after other validations run.
    /// When constructed directly (e.g. from MCP), this is `None` and
    /// `exclusive_start_key` is used instead.
    pub exclusive_start_key_raw: Option<serde_json::Value>,
}

impl<'de> serde::Deserialize<'de> for QueryRequest {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let raw = QueryRequestRaw::deserialize(deserializer)?;
        use crate::validation::{format_validation_errors, table_name_constraint_errors};

        let mut errors = Vec::new();
        errors.extend(table_name_constraint_errors(raw.table_name.as_deref()));
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
                     Member must satisfy enum value set: [ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES, COUNT, SPECIFIC_ATTRIBUTES]",
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

        Ok(QueryRequest {
            table_name,
            key_condition_expression: raw.key_condition_expression,
            filter_expression: raw.filter_expression,
            projection_expression: raw.projection_expression,
            expression_attribute_names: raw.expression_attribute_names,
            expression_attribute_values: raw.expression_attribute_values,
            scan_index_forward: raw.scan_index_forward,
            limit: raw.limit,
            exclusive_start_key: None,
            select: raw.select,
            consistent_read: raw.consistent_read,
            index_name: raw.index_name,
            return_consumed_capacity: raw.return_consumed_capacity,
            key_conditions: raw.key_conditions,
            attributes_to_get: raw.attributes_to_get,
            query_filter: raw.query_filter,
            conditional_operator: raw.conditional_operator,
            exclusive_start_key_raw: raw.exclusive_start_key,
        })
    }
}

#[derive(Debug, Default, Serialize)]
pub struct QueryResponse {
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

pub fn execute(storage: &Storage, mut request: QueryRequest) -> Result<QueryResponse> {
    // Validate table name format before checking existence (DynamoDB validates input first)
    crate::validation::validate_table_name(&request.table_name)?;

    // ---- Expression vs non-expression mixing validation ----
    // DynamoDB checks this before anything else (except table name format and ESK values).
    {
        let mut non_expr = Vec::new();
        let mut expr = Vec::new();
        if request.attributes_to_get.is_some() {
            non_expr.push("AttributesToGet");
        }
        if request.query_filter.is_some()
            && request.query_filter.as_ref().is_some_and(|v| !v.is_null())
        {
            non_expr.push("QueryFilter");
        }
        if request.conditional_operator.is_some() {
            non_expr.push("ConditionalOperator");
        }
        if request.key_conditions.is_some()
            && request
                .key_conditions
                .as_ref()
                .is_some_and(|v| !v.is_null())
        {
            non_expr.push("KeyConditions");
        }
        if request.projection_expression.is_some() {
            expr.push("ProjectionExpression");
        }
        if request.filter_expression.is_some() {
            expr.push("FilterExpression");
        }
        if request.key_condition_expression.is_some() {
            expr.push("KeyConditionExpression");
        }
        let no_raw_eav: Option<serde_json::Value> = None;
        let ctx = helpers::ExpressionParamContext {
            non_expression_params: non_expr,
            expression_params: expr,
            all_expression_param_names: vec!["FilterExpression", "KeyConditionExpression"],
            expression_attribute_names: &request.expression_attribute_names,
            expression_attribute_values: &request.expression_attribute_values,
            expression_attribute_values_raw: &no_raw_eav,
        };
        helpers::validate_expression_params(&ctx)?;
    }

    // ---- Validate filter attribute values (before argument counts, matching DynamoDB) ----
    helpers::validate_filter_conditions_raw(request.query_filter.as_ref(), "QueryFilter")?;
    helpers::validate_filter_conditions_raw(request.key_conditions.as_ref(), "KeyConditions")?;

    // ---- Validate filter argument counts and type compatibility ----
    helpers::validate_filter_condition_args(request.query_filter.as_ref())?;
    helpers::validate_filter_condition_args(request.key_conditions.as_ref())?;

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

    // ---- Validate expression syntax BEFORE table existence ----
    // DynamoDB validates KeyConditionExpression, FilterExpression, and
    // ProjectionExpression syntax before checking if the table exists.
    if let Some(ref kce) = request.key_condition_expression {
        if kce.is_empty() {
            return Err(DynoxideError::ValidationException(
                "Invalid KeyConditionExpression: The expression can not be empty;".to_string(),
            ));
        }
    }
    if let Some(ref fe) = request.filter_expression {
        if fe.is_empty() {
            if request.query_filter.is_none() || request.filter_expression.as_deref() == Some("") {
                return Err(DynoxideError::ValidationException(
                    "Invalid FilterExpression: The expression can not be empty;".to_string(),
                ));
            }
        } else {
            let parsed_fe = expressions::condition::parse(fe).map_err(|e| {
                DynoxideError::ValidationException(format!("Invalid FilterExpression: {e}"))
            })?;
            // Validate that all #name references are defined in ExpressionAttributeNames
            // (before table existence check, matching DynamoDB's validation ordering)
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
    if let Some(ref pe) = request.projection_expression {
        if pe.is_empty() {
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
    // For KeyConditionExpression, validate syntax early too
    if let Some(ref kce) = request.key_condition_expression {
        if !kce.is_empty() {
            // Create a temporary tracker for early syntax validation
            let temp_tracker = crate::expressions::TrackedExpressionAttributes::new(
                &request.expression_attribute_names,
                &request.expression_attribute_values,
            );
            if let Err(e) = expressions::key_condition::parse(kce, &temp_tracker) {
                return Err(DynoxideError::ValidationException(e));
            }
        }
    }

    let meta = helpers::require_table_for_item_op(storage, &request.table_name)?;
    let table_key_schema = helpers::parse_key_schema(&meta)?;

    // Determine effective partition key name early so we can pass it to
    // the legacy KeyConditions converter (ensures correct ordering when
    // both hash and range keys use EQ).
    let effective_pk_for_kc = if let Some(ref index_name) = request.index_name {
        if let Some((pk, _)) = request
            .index_name
            .as_ref()
            .and_then(|idx| super::lsi::parse_lsi_key_schema(&meta, idx).ok())
        {
            pk
        } else if let Ok((pk, _)) = super::gsi::parse_gsi_key_schema(&meta, index_name) {
            pk
        } else {
            table_key_schema.partition_key.clone()
        }
    } else {
        table_key_schema.partition_key.clone()
    };

    // Convert legacy KeyConditions to KeyConditionExpression if no expression is set
    if request.key_condition_expression.is_none() {
        if let Some(ref kc_val) = request.key_conditions {
            if let Ok(kc) =
                serde_json::from_value::<HashMap<String, helpers::KeyCondition>>(kc_val.clone())
            {
                if !kc.is_empty() {
                    let converted =
                        helpers::convert_key_conditions(&kc, Some(&effective_pk_for_kc))?;
                    request.key_condition_expression = Some(converted.expression);
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

    // Convert legacy QueryFilter to FilterExpression if no expression is set
    if request.filter_expression.is_none() {
        if let Some(ref qf_val) = request.query_filter {
            if let Ok(qf) =
                serde_json::from_value::<HashMap<String, helpers::FilterCondition>>(qf_val.clone())
            {
                if !qf.is_empty() {
                    let converted = helpers::convert_filter_conditions(
                        &qf,
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

    // Convert legacy AttributesToGet to projection
    let legacy_projection = if request.projection_expression.is_none() {
        request
            .attributes_to_get
            .as_ref()
            .map(|attrs| helpers::attributes_to_get_to_projection(attrs))
    } else {
        None
    };

    // Ensure KeyConditionExpression is present (required)
    let key_condition_expression = request.key_condition_expression.as_deref().ok_or_else(|| {
        DynoxideError::ValidationException(
            "Either the KeyConditions or KeyConditionExpression parameter must be specified in the request."
                .to_string(),
        )
    })?;
    let key_condition_expression = key_condition_expression.to_string();

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

    // Parse full index definition to get projection type
    let index_projection_type = if let Some(ref index_name) = request.index_name {
        if is_lsi {
            super::lsi::parse_lsi_defs(&meta)?
                .into_iter()
                .find(|l| l.index_name == *index_name)
                .map(|l| l.projection_type)
        } else {
            super::gsi::parse_gsi_defs(&meta)?
                .into_iter()
                .find(|g| g.index_name == *index_name)
                .map(|g| g.projection_type)
        }
    } else {
        None
    };

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
    if let Some(ref esk) = exclusive_start_key {
        // Stage 1+2: count check + index key type check
        helpers::validate_esk_count_and_index_keys(
            esk,
            &meta,
            request.index_name.as_deref(),
            "The provided starting key is invalid",
        )?;
        // Stage 3: table key type check
        helpers::validate_esk_table_keys(esk, &meta)?;
    }

    // Create tracker for unused expression attribute names/values
    let tracker = crate::expressions::TrackedExpressionAttributes::new(
        &request.expression_attribute_names,
        &request.expression_attribute_values,
    );

    // Parse KeyConditionExpression
    let key_cond = expressions::key_condition::parse(&key_condition_expression, &tracker)
        .map_err(DynoxideError::ValidationException)?;

    // Validate pk_name matches the effective partition key
    if key_cond.pk_name != effective_pk {
        return Err(DynoxideError::ValidationException(format!(
            "Query condition missed key schema element: {}",
            effective_pk
        )));
    }

    // Resolve values
    let resolved = expressions::key_condition::resolve_values(&key_cond, &tracker)
        .map_err(DynoxideError::ValidationException)?;

    // Get pk string
    let pk_str = resolved.pk_value.to_key_string().ok_or_else(|| {
        DynoxideError::ValidationException(
            "Cannot convert partition key value to string".to_string(),
        )
    })?;

    // Build sk SQL conditions
    let mut sk_sql_parts = Vec::new();
    let mut sk_param_values = Vec::new();

    if let Some(ref sk_cond) = resolved.sk_condition {
        // Validate sk name matches effective sort key
        if let Some(ref eff_sk) = effective_sk {
            if sk_cond.sk_name() != eff_sk {
                return Err(DynoxideError::ValidationException(format!(
                    "Query condition missed key schema element: {eff_sk}"
                )));
            }
        } else {
            return Err(DynoxideError::ValidationException(
                "Query filter contains a sort key condition but the table has no sort key"
                    .to_string(),
            ));
        }

        let conditions = sk_cond.to_sql_conditions();
        for (i, (op, val)) in conditions.iter().enumerate() {
            let param_idx = i + 2; // pk is ?1, sk params start at ?2
            if op == "LIKE" {
                sk_sql_parts.push(format!("AND sk LIKE ?{param_idx} ESCAPE '\\'"));
            } else {
                sk_sql_parts.push(format!("AND sk {op} ?{param_idx}"));
            }
            sk_param_values.push(val.clone());
        }
    }

    // ---- Validate QueryFilter/FilterExpression don't reference primary key attrs ----
    // Collect effective key attribute names
    let mut effective_key_attrs = vec![effective_pk.clone()];
    if let Some(ref sk) = effective_sk {
        effective_key_attrs.push(sk.clone());
    }

    // Check legacy QueryFilter
    if let Some(ref qf_val) = request.query_filter {
        if let Some(obj) = qf_val.as_object() {
            for attr_name in obj.keys() {
                if effective_key_attrs.contains(attr_name) {
                    return Err(DynoxideError::ValidationException(format!(
                        "QueryFilter can only contain non-primary key attributes: \
                         Primary key attribute: {attr_name}"
                    )));
                }
            }
        }
    }

    // Check FilterExpression for key attribute references (only for user-supplied expressions,
    // not those converted from QueryFilter - QueryFilter is checked separately above)
    if request.query_filter.is_none() {
        if let Some(ref fe) = request.filter_expression {
            if let Ok(parsed_fe) = expressions::condition::parse(fe) {
                let top_attrs = expressions::condition::extract_top_level_attributes(
                    &parsed_fe,
                    &request.expression_attribute_names,
                );
                for attr in &top_attrs {
                    if effective_key_attrs.contains(attr) {
                        return Err(DynoxideError::ValidationException(format!(
                            "Filter Expression can only contain non-primary key attributes: \
                             Primary key attribute: {attr}"
                        )));
                    }
                }
                // Check for non-scalar key access in FilterExpression
                // Build index key attribute lists
                let mut index_key_attrs = Vec::new();
                if request.index_name.is_some() {
                    // Index keys that are not also table keys
                    if !effective_key_attrs
                        .iter()
                        .any(|k| k == &table_key_schema.partition_key)
                    {
                        // This shouldn't normally happen for query, but just in case
                    }
                    // Check all effective key attrs for non-scalar access
                    for k in &effective_key_attrs {
                        if ![table_key_schema.partition_key.clone()]
                            .iter()
                            .chain(table_key_schema.sort_key.iter())
                            .any(|tk| tk == k)
                        {
                            index_key_attrs.push(k.clone());
                        }
                    }
                }
                let base_key_attrs: Vec<String> = {
                    let mut v = vec![table_key_schema.partition_key.clone()];
                    if let Some(ref sk) = table_key_schema.sort_key {
                        v.push(sk.clone());
                    }
                    v
                };
                if let Some((attr, is_index)) = expressions::condition::check_non_scalar_key_access(
                    &parsed_fe,
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
        }
    }

    let is_index_query = request.index_name.is_some();

    // Build ExclusiveStartKey sk value.
    // For hash-only GSIs (no sort key), use empty string so the composite
    // cursor (gsi_sk, table_pk, table_sk) can drive pagination.
    let start_sk = if let Some(ref esk) = exclusive_start_key {
        if let Some(ref sk_name) = effective_sk {
            esk.get(sk_name).and_then(|v| v.to_key_string())
        } else if is_index_query {
            // Hash-only index: gsi_sk / lsi_sk is always ''
            Some(String::new())
        } else {
            None
        }
    } else {
        None
    };

    // For LSI and GSI queries, extract the base table keys from ExclusiveStartKey
    // to enable composite cursor pagination.
    let (start_base_pk, start_base_sk) = if is_index_query {
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

    // Validate Select=ALL_ATTRIBUTES against index projection type.
    // For GSI with non-ALL projection, DynamoDB rejects ALL_ATTRIBUTES.
    let is_select_all_attributes = request
        .select
        .as_deref()
        .map(|s| s.eq_ignore_ascii_case("ALL_ATTRIBUTES"))
        .unwrap_or(false);
    let fetch_from_base_table = if is_select_all_attributes {
        if let Some(ref proj_type) = index_projection_type {
            if *proj_type != crate::types::ProjectionType::ALL {
                if !is_lsi {
                    return Err(DynoxideError::ValidationException(format!(
                        "One or more parameter values were invalid: \
                         Select type ALL_ATTRIBUTES is not supported for global secondary index {} \
                         because its projection type is not ALL",
                        request.index_name.as_deref().unwrap_or("")
                    )));
                }
                // LSI with non-ALL projection: fetch full items from base table
                true
            } else {
                false
            }
        } else {
            false
        }
    } else {
        false
    };

    // Combine sk conditions into a single SQL fragment
    let sk_condition_sql = if sk_sql_parts.is_empty() {
        None
    } else {
        Some(sk_sql_parts.join(" "))
    };

    let fetch_limit = request.limit;
    let sk_params_refs: Vec<&str> = sk_param_values.iter().map(|s| s.as_str()).collect();

    // Query either GSI table or base table
    let query_params = crate::storage::QueryParams {
        sk_condition: sk_condition_sql.as_deref(),
        sk_params: &sk_params_refs,
        forward: request.scan_index_forward,
        limit: fetch_limit,
        exclusive_start_sk: start_sk.as_deref(),
        exclusive_start_base_pk: start_base_pk.as_deref(),
        exclusive_start_base_sk: start_base_sk.as_deref(),
    };
    let rows = if let Some(ref index_name) = request.index_name {
        if is_lsi {
            storage.query_lsi_items(&request.table_name, index_name, &pk_str, &query_params)?
        } else {
            storage.query_gsi_items(&request.table_name, index_name, &pk_str, &query_params)?
        }
    } else {
        storage.query_items(&request.table_name, &pk_str, &query_params)?
    };

    // Parse filter expression if present
    let filter_expr = request
        .filter_expression
        .as_ref()
        .map(|expr| expressions::condition::parse(expr))
        .transpose()
        .map_err(DynoxideError::ValidationException)?;

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
    // Also include base table keys when querying a GSI
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

    // Track sizes separately for ALL_ATTRIBUTES LSI queries where both
    // index reads and base table reads contribute to ConsumedCapacity.
    let mut base_table_cumulative_size = 0usize;
    let mut index_cumulative_size = 0usize;

    for (_pk, _sk, item_json) in &rows {
        let index_item: Item = serde_json::from_str(item_json).map_err(|e| {
            DynoxideError::InternalServerError(format!("Bad item JSON in storage: {e}"))
        })?;

        // If Select=ALL_ATTRIBUTES on LSI with non-ALL projection, fetch full
        // item from the base table for the response while using the index item
        // for cursor tracking.
        index_cumulative_size += crate::types::item_size(&index_item);
        let item = if fetch_from_base_table {
            let base_pk = index_item
                .get(&table_key_schema.partition_key)
                .and_then(|v| v.to_key_string())
                .unwrap_or_default();
            let base_sk = table_key_schema
                .sort_key
                .as_ref()
                .and_then(|sk_name| index_item.get(sk_name))
                .and_then(|v| v.to_key_string())
                .unwrap_or_default();
            if let Some(full_json) = storage.get_item(&request.table_name, &base_pk, &base_sk)? {
                let full_item: Item = serde_json::from_str(&full_json).map_err(|e| {
                    DynoxideError::InternalServerError(format!("Bad item JSON: {e}"))
                })?;
                base_table_cumulative_size += crate::types::item_size(&full_item);
                full_item
            } else {
                index_item.clone()
            }
        } else {
            index_item.clone()
        };

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
                last_evaluated_item = Some(index_item);
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
            item
        };

        last_evaluated_item = Some(index_item);
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
    // We return LEK if: we hit the Limit, or we hit the 1MB limit
    let has_more = truncated_by_size
        || (fetch_limit.is_some() && scanned_count >= fetch_limit.unwrap_or(usize::MAX));

    // For index queries, include the base table primary key in LastEvaluatedKey
    // alongside the effective (index) keys so the cursor can uniquely identify
    // the position. For LSIs, include the table sort key. For GSIs, include
    // both the table partition key and sort key.
    let is_gsi_query = request.index_name.is_some() && !is_lsi;
    let last_evaluated_key = if has_more {
        last_evaluated_item.map(|item| {
            let mut key = build_last_evaluated_key(&item, &effective_pk, effective_sk.as_deref());
            // For LSI queries, add the table sort key if different from the index sort key
            if is_lsi {
                if let Some(tsk) = table_key_schema.sort_key.as_deref() {
                    if !key.contains_key(tsk) {
                        if let Some(v) = item.get(tsk) {
                            key.insert(tsk.to_string(), v.clone());
                        }
                    }
                }
            }
            // For GSI queries, add the base table primary key (pk and sk)
            if is_gsi_query {
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

    // Attribute read capacity to the index if querying one
    let is_gsi = is_gsi_query;
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
    } else if is_lsi {
        // When fetching from the base table (ALL_ATTRIBUTES on non-ALL LSI),
        // split capacity between the index read and the table read.
        let (table_cap, lsi_cap) = if fetch_from_base_table {
            let table_rcu = crate::types::read_capacity_units_with_consistency(
                base_table_cumulative_size,
                consistent,
            );
            let lsi_rcu = crate::types::read_capacity_units_with_consistency(
                index_cumulative_size,
                consistent,
            );
            (table_rcu, lsi_rcu)
        } else {
            (
                0.0,
                crate::types::read_capacity_units_with_consistency(cumulative_size, consistent),
            )
        };
        let mut lsi_units = std::collections::HashMap::new();
        lsi_units.insert(request.index_name.as_ref().unwrap().clone(), lsi_cap);
        crate::types::consumed_capacity_with_secondary_indexes(
            &request.table_name,
            table_cap,
            &std::collections::HashMap::new(),
            &lsi_units,
            &request.return_consumed_capacity,
        )
    } else {
        crate::types::consumed_capacity(
            &request.table_name,
            crate::types::read_capacity_units_with_consistency(cumulative_size, consistent),
            &request.return_consumed_capacity,
        )
    };

    Ok(QueryResponse {
        items: if is_count { None } else { Some(items) },
        count,
        scanned_count,
        last_evaluated_key,
        consumed_capacity,
    })
}

fn build_last_evaluated_key(
    item: &Item,
    pk_name: &str,
    sk_name: Option<&str>,
) -> HashMap<String, AttributeValue> {
    let mut key = HashMap::new();
    if let Some(pk_val) = item.get(pk_name) {
        key.insert(pk_name.to_string(), pk_val.clone());
    }
    if let Some(sk) = sk_name {
        if let Some(sk_val) = item.get(sk) {
            key.insert(sk.to_string(), sk_val.clone());
        }
    }
    key
}
