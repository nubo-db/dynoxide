use crate::actions::helpers;
use crate::errors::{DynoxideError, Result};
use crate::expressions;
use crate::storage_backend::StorageBackend;
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
    #[serde(
        rename = "ExpressionAttributeValues",
        default,
        deserialize_with = "crate::types::deserialize_named_empty_and_null_refusals"
    )]
    expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    #[serde(
        rename = "ScanIndexForward",
        default,
        deserialize_with = "crate::types::deserialize_lenient_flag"
    )]
    scan_index_forward: Option<bool>,
    #[serde(
        rename = "Limit",
        default,
        deserialize_with = "helpers::deserialize_int32"
    )]
    limit: Option<helpers::Int32>,
    #[serde(rename = "ExclusiveStartKey", default)]
    exclusive_start_key: Option<serde_json::Value>,
    #[serde(rename = "Select", default)]
    select: Option<String>,
    #[serde(
        rename = "ConsistentRead",
        default,
        deserialize_with = "crate::types::deserialize_lenient_flag"
    )]
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

/// The rejection for a `Select` outside the enum, or `None` when it is one
/// DynamoDB accepts. The one copy of the set and the wording.
pub(crate) fn select_rejection(select: &str) -> Option<String> {
    if [
        "ALL_ATTRIBUTES",
        "ALL_PROJECTED_ATTRIBUTES",
        "COUNT",
        "SPECIFIC_ATTRIBUTES",
    ]
    .contains(&select)
    {
        return None;
    }
    Some(format!(
        "Value '{select}' at 'select' failed to satisfy constraint: Member must satisfy \
         enum value set: [SPECIFIC_ATTRIBUTES, COUNT, ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES]"
    ))
}

impl<'de> serde::Deserialize<'de> for QueryRequest {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let raw = QueryRequestRaw::deserialize(deserializer)?;
        use crate::validation::format_validation_errors;

        // A BOOL or NULL in a legacy condition that doesn't read as a boolean
        // makes the whole request malformed, ahead of everything else.
        if let Some(message) =
            crate::types::malformed_condition_boolean(raw.key_conditions.as_ref())
                .or_else(|| crate::types::malformed_condition_boolean(raw.query_filter.as_ref()))
        {
            return Err(serde::de::Error::custom(message));
        }

        // A Limit below 1 and an empty name are the newer validation layer's
        // own checks, reported together, Limit first, ahead of everything
        // else. A name error and ReturnConsumedCapacity follow, together;
        // Select waits until after the expression-mixing check in `execute`
        // (captured 2026-10-09, six of seven regions). Scan words them the same
        // way where the newer layer runs it (see `ScanMembers`).
        //
        // With no TableName at all, the request goes to the older layer, which
        // reports every member error together: Select, ReturnConsumedCapacity,
        // the missing name, then a Limit below 1 with its value echoed and the
        // member spelled 'limit', as Scan does (captured eu-west-2 and
        // us-east-1, 2026-10-09 and 2026-10-20).
        //
        // A Limit that saturated or lost a fraction as it was read (see
        // `helpers::Int32`) is checked by the older layer the same way: the
        // value as read, and an empty name in the older wording (captured
        // eu-west-2, eu-central-1 and us-east-1, 2026-10-20).
        let limit = raw.limit.map(|n| n.value);
        let coerced = raw.limit.is_some_and(|n| n.coerced);
        let limit_too_small = limit.filter(|l| *l < 1);
        if raw.table_name.is_none() || coerced {
            let mut errors = Vec::new();
            errors.extend(raw.select.as_deref().and_then(select_rejection));
            errors.extend(crate::validation::return_consumed_capacity_rejection(
                raw.return_consumed_capacity.as_deref(),
            ));
            if matches!(raw.table_name.as_deref(), None | Some("")) {
                errors.extend(crate::validation::table_name_constraint_errors(
                    raw.table_name.as_deref(),
                    crate::validation::TableNameContext::ReadWrite,
                ));
            }
            // The missing operators' place is inferred from Scan's.
            errors.extend(helpers::missing_comparison_operators(
                raw.key_conditions.as_ref(),
                "keyConditions",
            ));
            errors.extend(helpers::missing_comparison_operators(
                raw.query_filter.as_ref(),
                "queryFilter",
            ));
            if let Some(limit) = limit_too_small {
                errors.push(format!(
                    "Value '{limit}' at 'limit' failed to satisfy constraint: \
                     Member must have value greater than or equal to 1"
                ));
            }
            if let Some(msg) = format_validation_errors(&errors) {
                return Err(serde::de::Error::custom(format!("VALIDATION:{}", msg)));
            }
        }
        let mut first_pass = Vec::new();
        if limit_too_small.is_some() {
            first_pass.push(
                "Value at 'Limit' failed to satisfy constraint: \
                 Member must have value greater than or equal to 1"
                    .to_string(),
            );
        }
        if raw.table_name.as_deref() == Some("") {
            first_pass.push(crate::validation::EMPTY_TABLE_NAME_REJECTION.to_string());
        }
        if let Some(msg) = format_validation_errors(&first_pass) {
            return Err(serde::de::Error::custom(format!("VALIDATION:{}", msg)));
        }

        let mut errors = crate::validation::item_table_name_errors(raw.table_name.as_deref());
        let table_name = raw.table_name.unwrap_or_default();

        // ReturnConsumedCapacity enum
        if let Some(msg) = crate::validation::return_consumed_capacity_rejection(
            raw.return_consumed_capacity.as_deref(),
        ) {
            errors.push(msg);
        }

        // A KeyConditions or QueryFilter condition with no ComparisonOperator,
        // as a ScanFilter's is refused (inferred, as is the order).
        errors.extend(helpers::missing_comparison_operators(
            raw.key_conditions.as_ref(),
            "keyConditions",
        ));
        errors.extend(helpers::missing_comparison_operators(
            raw.query_filter.as_ref(),
            "queryFilter",
        ));

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
            // Left out or null, a Query reads forward (captured eu-west-2 and
            // us-east-1, 2026-10-10).
            scan_index_forward: raw.scan_index_forward.unwrap_or(true),
            limit: limit.map(|l| l as usize),
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
    // Number sets in numeric order here, as DynamoDB returns them;
    // pinned by set_order's every_response_item_field test.
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

pub async fn execute<S: StorageBackend>(
    storage: &S,
    mut request: QueryRequest,
) -> Result<QueryResponse> {
    // A Limit of 0 and an empty table name come first and together, Limit
    // first, then any other table-name error, as the wire path reports them
    // when it reads the body; a request built in code gets the same answer.
    let mut first_pass = Vec::new();
    if request.limit == Some(0) {
        first_pass.push(
            "Value at 'Limit' failed to satisfy constraint: \
             Member must have value greater than or equal to 1"
                .to_string(),
        );
    }
    if request.table_name.is_empty() {
        first_pass.push(crate::validation::EMPTY_TABLE_NAME_REJECTION.to_string());
    }
    if let Some(msg) = crate::validation::format_validation_errors(&first_pass) {
        return Err(DynoxideError::ValidationException(msg));
    }
    crate::validation::validate_item_table_name(&request.table_name)?;

    // A Limit past what a 32-bit integer holds is read as the largest one.
    request.limit = request.limit.map(|l| l.min(i32::MAX as usize));

    // An empty attribute name is refused bare, ahead of the table lookup and the
    // expressions, wherever the request carries one: AttributesToGet, a
    // QueryFilter value, the ExclusiveStartKey, the expression names and values
    // (captured in ten regions). An empty name in QueryFilter itself is accepted.
    if crate::empty_names::names_include_empty(request.attributes_to_get.as_ref())
        || crate::empty_names::filter_values_have_empty_name(request.query_filter.as_ref())
        || request
            .exclusive_start_key
            .as_ref()
            .is_some_and(crate::empty_names::map_has_empty_name)
        || request
            .exclusive_start_key_raw
            .as_ref()
            .is_some_and(crate::empty_names::json_map_has_empty_name)
    {
        return Err(crate::empty_names::bare());
    }
    crate::empty_names::check_names(request.expression_attribute_names.as_ref())?;
    if crate::empty_names::value_placeholder(request.expression_attribute_values.as_ref(), None)
        .is_some()
    {
        return Err(crate::empty_names::bare());
    }

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
            layer: crate::validation::ValueLayer::Current,
        };
        helpers::validate_expression_params(&ctx)?;
    }

    // ---- Select, after the mixing check (captured 2026-10-09) ----
    if let Some(msg) = request.select.as_deref().and_then(select_rejection) {
        return Err(DynoxideError::ValidationException(
            crate::validation::envelope_message(&msg),
        ));
    }

    // ---- ConditionalOperator only joins legacy QueryFilter conditions, so it
    // is refused without two of them, whatever its value. Query reports it
    // without the envelope (captured 2026-10-09, all seven regions). ----
    if let Some(msg) = crate::validation::conditional_operator_misuse(
        request.conditional_operator.as_deref(),
        crate::validation::legacy_condition_count(request.query_filter.as_ref()),
    ) {
        return Err(DynoxideError::ValidationException(msg.to_string()));
    }

    // ---- Validate filter attribute values (before argument counts, matching DynamoDB) ----
    // A QueryFilter or KeyConditions value nested too deep is refused first.
    crate::value_nesting::check_query_conditions(request.query_filter.as_ref())?;
    crate::value_nesting::check_query_conditions(request.key_conditions.as_ref())?;
    helpers::validate_filter_conditions_raw(
        request.query_filter.as_ref(),
        "QueryFilter",
        crate::validation::ValueLayer::Current,
    )?;
    helpers::validate_filter_conditions_raw(
        request.key_conditions.as_ref(),
        "KeyConditions",
        crate::validation::ValueLayer::Current,
    )?;

    // ---- Validate filter argument counts and type compatibility ----
    helpers::validate_filter_condition_args(request.query_filter.as_ref(), "queryFilter")?;
    helpers::validate_filter_condition_args(request.key_conditions.as_ref(), "keyConditions")?;

    // ---- Validate duplicate AttributesToGet ----
    if let Some(ref attrs) = request.attributes_to_get {
        helpers::validate_attributes_to_get_no_duplicates(attrs)?;
    }

    // ---- Parse ExclusiveStartKey from JSON value ----
    let exclusive_start_key = if let Some(ref esk_val) = request.exclusive_start_key_raw {
        Some(
            helpers::parse_exclusive_start_key(esk_val, crate::validation::ValueLayer::Current)
                .map_err(start_key_value_error)?,
        )
    } else {
        request.exclusive_start_key.clone()
    };
    crate::value_nesting::check_query_start_key(exclusive_start_key.as_ref())?;

    // ---- Validate expression syntax BEFORE table existence ----
    // DynamoDB validates KeyConditionExpression, FilterExpression, and
    // ProjectionExpression syntax before checking if the table exists.
    if let Some(ref kce) = request.key_condition_expression
        && kce.is_empty()
    {
        return Err(DynoxideError::ValidationException(
            "Invalid KeyConditionExpression: The expression can not be empty;".to_string(),
        ));
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
            if let Err(e) = expressions::condition::validate_operand_semantics(
                &parsed_fe,
                &request.expression_attribute_names,
                &request.expression_attribute_values,
            ) {
                return Err(DynoxideError::ValidationException(format!(
                    "Invalid FilterExpression: {e}"
                )));
            }
        }
    }
    if let Some(ref pe) = request.projection_expression
        && pe.is_empty()
    {
        return Err(DynoxideError::ValidationException(
            "Invalid ProjectionExpression: The expression can not be empty;".to_string(),
        ));
    }

    // Select against the projection members and the index, in the envelope.
    if let Some(msg) = helpers::select_projection_rejection(
        request.select.as_deref(),
        request.attributes_to_get.is_some(),
        request.projection_expression.is_some(),
        request.index_name.is_some(),
    ) {
        return Err(DynoxideError::ValidationException(msg));
    }

    // For KeyConditionExpression, validate syntax early too
    if let Some(ref kce) = request.key_condition_expression
        && !kce.is_empty()
    {
        // Create a temporary tracker for early syntax validation
        let temp_tracker = crate::expressions::TrackedExpressionAttributes::new(
            &request.expression_attribute_names,
            &request.expression_attribute_values,
        );
        if let Err(e) = expressions::key_condition::parse(kce, &temp_tracker) {
            return Err(DynoxideError::ValidationException(key_condition_error(
                kce, e,
            )));
        }
    }

    let meta = helpers::require_table_for_item_op(storage, &request.table_name).await?;
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
    let key_conditions: Option<HashMap<String, helpers::KeyCondition>> = match request
        .key_conditions
        .as_ref()
    {
        Some(kc_val) if request.key_condition_expression.is_none() && !kc_val.is_null() => Some(
            helpers::read_legacy_conditions(kc_val, crate::validation::ValueLayer::Current)?,
        ),
        _ => None,
    };
    if let Some(kc) = key_conditions
        && !kc.is_empty()
    {
        check_legacy_key_condition_types(&kc, &meta)?;
        let converted = helpers::convert_key_conditions(&kc, Some(&effective_pk_for_kc))?;
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

    // Convert legacy QueryFilter to FilterExpression if no expression is set
    let query_filter: Option<HashMap<String, helpers::FilterCondition>> =
        match request.query_filter.as_ref() {
            Some(qf_val) if request.filter_expression.is_none() && !qf_val.is_null() => Some(
                helpers::read_legacy_conditions(qf_val, crate::validation::ValueLayer::Current)?,
            ),
            _ => None,
        };
    if let Some(qf) = query_filter
        && !qf.is_empty()
    {
        let converted =
            helpers::convert_filter_conditions(&qf, request.conditional_operator.as_deref())?;
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
        if let Some(keys) = lsi_keys.clone() {
            keys
        } else {
            // A vector index is listed by DescribeTable but no Query can read
            // it, and DynamoDB refuses it by type rather than reporting the
            // index missing (captured eu-west-2, 2026-08-19). Checked before
            // the GSI lookup, which would otherwise answer "does not have the
            // specified index" for an index the table does have.
            reject_vector_index(&meta, index_name)?;
            super::gsi::parse_gsi_key_schema(&meta, index_name)?
        }
    } else {
        (
            table_key_schema.partition_key.clone(),
            table_key_schema.sort_key.clone(),
        )
    };

    // Every partition key and sort key attribute, in key-schema order. Only a
    // GSI can have more than one of each.
    let (key_hash, key_range): (Vec<String>, Vec<String>) = match request.index_name {
        Some(ref index_name) if !is_lsi => super::gsi::parse_gsi_key_attrs(&meta, index_name)?,
        _ => (
            vec![effective_pk.clone()],
            effective_sk.iter().cloned().collect(),
        ),
    };

    // ---- Validate ExclusiveStartKey structure against key schema ----
    if let Some(ref esk) = exclusive_start_key {
        // Stage 1+2: count check + index key type check
        helpers::validate_esk_count_and_index_keys(
            esk,
            &meta,
            request.index_name.as_deref(),
            "The provided starting key is invalid",
            false,
        )
        .map_err(bare_start_key_mismatch)?;
        // Stage 3: table key type check
        helpers::validate_esk_table_keys(esk, &meta).map_err(bare_start_key_mismatch)?;
        // Stage 4: an empty string or binary key value has its own message.
        check_start_key_not_empty(
            esk,
            &table_key_schema.partition_key,
            table_key_schema.sort_key.as_deref(),
        )?;
    }

    // Create tracker for unused expression attribute names/values
    let tracker = crate::expressions::TrackedExpressionAttributes::new(
        &request.expression_attribute_names,
        &request.expression_attribute_values,
    );

    // Parse KeyConditionExpression and bind it to the key being read
    let key_cond =
        expressions::key_condition::parse(&key_condition_expression, &tracker).map_err(|e| {
            DynoxideError::ValidationException(key_condition_error(&key_condition_expression, e))
        })?;
    let bound = expressions::key_condition::bind(
        &key_cond,
        expressions::key_condition::KeyAttributes {
            hash: &key_hash,
            range: &key_range,
        },
        &tracker,
    )
    .map_err(DynoxideError::ValidationException)?;
    check_key_condition_types(&bound, &meta)?;

    // Get pk string
    let pk_str = bound.partition_key_string().ok_or_else(|| {
        DynoxideError::ValidationException(
            "Cannot convert partition key value to string".to_string(),
        )
    })?;

    // Build sk SQL conditions
    let (sk_condition_sql, sk_param_values) = bound.sort_key_sql(key_range.len());

    // A starting key from another partition, or outside the sort key condition,
    // is refused, on a table and on an index.
    if let Some(ref esk) = exclusive_start_key {
        let table_without_sort_key = request.index_name.is_none() && effective_sk.is_none();
        check_start_key_within_condition(esk, &bound, table_without_sort_key)?;
    }

    // ---- Validate QueryFilter/FilterExpression don't reference primary key attrs ----
    // Collect effective key attribute names
    let effective_key_attrs: Vec<String> = key_hash.iter().chain(&key_range).cloned().collect();

    // Check legacy QueryFilter
    if let Some(ref qf_val) = request.query_filter
        && let Some(obj) = qf_val.as_object()
    {
        for attr_name in obj.keys() {
            if effective_key_attrs.contains(attr_name) {
                return Err(DynoxideError::ValidationException(format!(
                    "QueryFilter can only contain non-primary key attributes: \
                         Primary key attribute: {attr_name}"
                )));
            }
        }
    }

    // Check FilterExpression for key attribute references (only for user-supplied expressions,
    // not those converted from QueryFilter - QueryFilter is checked separately above)
    if request.query_filter.is_none()
        && let Some(ref fe) = request.filter_expression
        && let Ok(parsed_fe) = expressions::condition::parse(fe)
    {
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

    let is_index_query = request.index_name.is_some();

    // Build ExclusiveStartKey sk value.
    // For hash-only GSIs (no sort key), use empty string so the composite
    // cursor (gsi_sk, table_pk, table_sk) can drive pagination.
    let start_sk = if let Some(ref esk) = exclusive_start_key {
        if !key_range.is_empty() {
            let values: Option<Vec<&AttributeValue>> =
                key_range.iter().map(|a| esk.get(a)).collect();
            values.and_then(|v| super::gsi::composite_key_string(&v))
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
            // When the base table is hash-only there is no base sort key, but the
            // GSI/LSI row stores the empty-string default in its table_sk column.
            // Default base_sk to "" (mirroring start_sk) so the composite cursor
            // keeps its full width and disambiguates tied index keys by table_pk.
            // Leaving it None collapses query_gsi_items to the 1-column gsi_sk
            // cursor, which cannot advance past rows sharing the same index key
            // and silently drops them. See scan.rs for the Scan-path counterpart.
            let base_sk = if let Some(sk_name) = table_key_schema.sort_key.as_ref() {
                esk.get(sk_name).and_then(|v| v.to_key_string())
            } else {
                Some(String::new())
            };
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
            storage
                .query_lsi_items(&request.table_name, index_name, &pk_str, &query_params)
                .await?
        } else {
            storage
                .query_gsi_items(&request.table_name, index_name, &pk_str, &query_params)
                .await?
        }
    } else {
        storage
            .query_items(&request.table_name, &pk_str, &query_params)
            .await?
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
        // Reject undefined names and overlapping paths before the row loop, so a
        // zero-match query still rejects.
        expressions::projection::validate(proj, &tracker)
            .map_err(DynoxideError::ValidationException)?;
    }

    // Untracked variant for the per-item hot loop: tracking already done above
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
        if let Some(ref sk) = table_key_schema.sort_key
            && !key_attrs.contains(sk)
        {
            key_attrs.push(sk.clone());
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

    // A read of the table itself is charged on its items' billed size, a vector
    // at four bytes per dimension (captured eu-west-2 and us-east-1,
    // 2026-10-23). Index entries stay sized by their numbers, as their writes
    // are, and the 1MB page limit stays on the stored size.
    let vector_defs = super::vector_index::parse_vector_defs(&meta)?;
    let mut billed_cumulative_size = 0usize;

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
            if let Some(full_json) = storage
                .get_item(&request.table_name, &base_pk, &base_sk)
                .await?
            {
                let full_item: Item = serde_json::from_str(&full_json).map_err(|e| {
                    DynoxideError::InternalServerError(format!("Bad item JSON: {e}"))
                })?;
                base_table_cumulative_size +=
                    crate::types::billed_item_size(&full_item, &vector_defs);
                full_item
            } else {
                index_item.clone()
            }
        } else {
            index_item.clone()
        };

        scanned_count += 1;

        // Check 1MB limit BEFORE filtering - DynamoDB counts all evaluated data
        // towards the 1MB response size limit, not just items that pass the filter.
        let item_size = crate::types::item_size(&item);
        if cumulative_size + item_size > MAX_RESPONSE_SIZE && scanned_count > 1 {
            truncated_by_size = true;
            break;
        }
        cumulative_size += item_size;
        if request.index_name.is_none() {
            billed_cumulative_size += crate::types::billed_item_size(&item, &vector_defs);
        }

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
            let mut key = build_last_evaluated_key(&item, &key_hash, &key_range);
            // For LSI queries, add the table sort key if different from the index sort key
            if is_lsi
                && let Some(tsk) = table_key_schema.sort_key.as_deref()
                && !key.contains_key(tsk)
                && let Some(v) = item.get(tsk)
            {
                key.insert(tsk.to_string(), v.clone());
            }
            // For GSI queries, add the base table primary key (pk and sk)
            if is_gsi_query {
                if !key.contains_key(&table_key_schema.partition_key)
                    && let Some(v) = item.get(&table_key_schema.partition_key)
                {
                    key.insert(table_key_schema.partition_key.clone(), v.clone());
                }
                if let Some(ref tsk) = table_key_schema.sort_key
                    && !key.contains_key(tsk)
                    && let Some(v) = item.get(tsk)
                {
                    key.insert(tsk.clone(), v.clone());
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
            crate::types::read_capacity_units_with_consistency(billed_cumulative_size, consistent),
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

/// The read key's attributes as the item holds them: every partition key and
/// sort key attribute of the table or index being read.
fn build_last_evaluated_key(
    item: &Item,
    hash: &[String],
    range: &[String],
) -> HashMap<String, AttributeValue> {
    hash.iter()
        .chain(range)
        .filter_map(|name| item.get(name).map(|v| (name.clone(), v.clone())))
        .collect()
}

/// Refuse a key condition value whose type is not its key attribute's declared
/// type. DynamoDB checks this once the condition has bound to the key: a string
/// compared with a number sort key, or a boolean in an equality, fails here,
/// while an operand no key type could take (a number in begins_with) fails
/// earlier as an operand error. Captured eu-west-2, 2026-10-08 and October 2026.
fn check_key_condition_types(
    bound: &expressions::key_condition::BoundKeyCondition,
    meta: &crate::storage::TableMetadata,
) -> Result<()> {
    let defs: Vec<crate::types::AttributeDefinition> =
        serde_json::from_str(&meta.attribute_definitions).map_err(|e| {
            DynoxideError::InternalServerError(format!("Bad attribute definitions JSON: {e}"))
        })?;
    for (attr, value) in bound.compared_values() {
        if let Some(def) = defs.iter().find(|d| d.attribute_name == attr)
            && !helpers::attr_value_matches_scalar_type(value, &def.attribute_type)
        {
            return Err(DynoxideError::ValidationException(
                "One or more parameter values were invalid: Condition parameter type does not \
                 match schema type"
                    .to_string(),
            ));
        }
    }
    Ok(())
}

/// Refuse a read against a vector index by type.
///
/// The wording differs between the two operations, full stop included: AWS
/// ends Query's with one where the other has none. Captured eu-west-2,
/// 2026-08-19.
fn reject_vector_index(
    meta: &crate::storage::TableMetadata,
    index_name: &str,
) -> crate::errors::Result<()> {
    if super::vector_index::parse_vector_defs(meta)?
        .iter()
        .any(|v| v.index_name == index_name)
    {
        return Err(DynoxideError::ValidationException(
            "Query operation not supported on this index type.".to_string(),
        ));
    }
    Ok(())
}

/// A KeyConditionExpression parse failure as DynamoDB reports it. DynamoDB
/// reads the expression with the general expression grammar before applying
/// the key condition's own rules, so when the text breaks that grammar the
/// error is its `Syntax error; token: ..., near: ...` (or an unknown function
/// name). Size and redundant-parenthesis errors come first and keep their
/// wording, as does every failure of the key condition's own rules.
fn key_condition_error(expr: &str, err: String) -> String {
    if err.contains("Expression size has exceeded") || err.contains("redundant parentheses") {
        return err;
    }
    match expressions::condition::syntax_error(expr) {
        Some(syntax) => format!("Invalid KeyConditionExpression: {syntax}"),
        None => err,
    }
}

/// A Query answers a starting key that doesn't fit the key schema with this
/// message alone, whatever the shape: a missing or extra attribute, a wrong
/// type, a map or set, NULL, or an index query missing the table's or the
/// index's keys. Captured in eu-west-2, us-east-1, us-west-2 and eu-central-1,
/// 2026-10-09. A Scan answers the same way where the newer validation layer
/// runs it, which is most regions since October 2026.
const START_KEY_INVALID: &str = "The provided starting key is invalid";

/// The schema mismatch the shared starting-key checks report, with or without
/// the starting-key prefix, answered as [`START_KEY_INVALID`] alone. An index
/// key of the wrong type answers so too (captured eu-west-2, eu-central-1 and
/// us-east-1, 2026-10-20).
pub(crate) fn bare_start_key_mismatch(err: DynoxideError) -> DynoxideError {
    match err {
        DynoxideError::ValidationException(msg)
            if msg
                == "The provided starting key is invalid: \
                    The provided key element does not match the schema"
                || msg == "The provided key element does not match the schema" =>
        {
            DynoxideError::ValidationException(START_KEY_INVALID.to_string())
        }
        other => other,
    }
}

/// A Query reports an invalid attribute value in its starting key (`NULL:
/// false`, an empty set) without the starting-key prefix a Scan can add. An
/// empty or multi-typed value keeps the prefix on both. Captured in eu-west-2
/// and us-east-1, 2026-10-09.
fn start_key_value_error(err: DynoxideError) -> DynoxideError {
    match err {
        DynoxideError::ValidationException(msg) => {
            match msg.strip_prefix("The provided starting key is invalid: ") {
                Some(rest) if rest.starts_with("One or more parameter values were invalid:") => {
                    DynoxideError::ValidationException(rest.to_string())
                }
                _ => DynoxideError::ValidationException(msg),
            }
        }
        other => other,
    }
}

/// Refuse a starting key that the key condition could never have produced.
///
/// DynamoDB walks the key condition in order. A partition key attribute other
/// than the last that differs from the start key ends the check: "The query can
/// return at most one row and cannot be restarted". Then a sort key condition
/// the start key breaks: "does not match the range key predicate". Then a
/// different last partition key attribute: "at most one row" again when a sort
/// key condition follows it, "The provided exclusive start key is invalid" on a
/// table with no sort key, and "outside query boundaries based on provided
/// conditions" otherwise. The same on a table, an LSI, a GSI and a GSI with a
/// multi-attribute key. Captured in eu-west-2 and us-east-1, 2026-10-09, and
/// on tables in us-west-2 and eu-central-1 too.
fn check_start_key_within_condition(
    esk: &Item,
    bound: &expressions::key_condition::BoundKeyCondition,
    table_without_sort_key: bool,
) -> Result<()> {
    use crate::expressions::condition::{CompOp, compare_values};

    const AT_MOST_ONE_ROW: &str = "The query can return at most one row and cannot be restarted";
    let refuse = |message: &str| Err(DynoxideError::ValidationException(message.to_string()));
    let holds = |name: &str, value: &AttributeValue| {
        esk.get(name)
            .is_some_and(|v| compare_values(v, &CompOp::Eq, value))
    };

    let Some(((last_name, last_value), leading)) = bound.hash.split_last() else {
        return Ok(());
    };
    if leading.iter().any(|(name, value)| !holds(name, value)) {
        return refuse(AT_MOST_ONE_ROW);
    }
    let outside_equalities = bound
        .range_eq
        .iter()
        .any(|(name, value)| esk.contains_key(name) && !holds(name, value));
    let outside_last = bound.range_last.as_ref().is_some_and(|condition| {
        esk.get(condition.sk_name())
            .is_some_and(|value| !sort_key_satisfies(value, condition))
    });
    if outside_equalities || outside_last {
        return refuse("The provided starting key does not match the range key predicate");
    }
    if !holds(last_name, last_value) {
        if !bound.range_eq.is_empty() || bound.range_last.is_some() {
            return refuse(AT_MOST_ONE_ROW);
        }
        if table_without_sort_key {
            return refuse("The provided exclusive start key is invalid");
        }
        return refuse(
            "The provided starting key is outside query boundaries based on provided conditions",
        );
    }
    Ok(())
}

fn sort_key_satisfies(
    value: &AttributeValue,
    condition: &expressions::key_condition::ResolvedSortKeyCondition,
) -> bool {
    use crate::expressions::condition::{CompOp, compare_values};
    use expressions::key_condition::ResolvedSortKeyCondition as Sk;

    match condition {
        Sk::Eq(_, v) => compare_values(value, &CompOp::Eq, v),
        Sk::Lt(_, v) => compare_values(value, &CompOp::Lt, v),
        Sk::Le(_, v) => compare_values(value, &CompOp::Le, v),
        Sk::Gt(_, v) => compare_values(value, &CompOp::Gt, v),
        Sk::Ge(_, v) => compare_values(value, &CompOp::Ge, v),
        Sk::Between(_, low, high) => {
            compare_values(value, &CompOp::Ge, low) && compare_values(value, &CompOp::Le, high)
        }
        Sk::BeginsWith(_, prefix) => match (value, prefix) {
            (AttributeValue::S(s), AttributeValue::S(p)) => s.starts_with(p.as_str()),
            (AttributeValue::B(b), AttributeValue::B(p)) => b.starts_with(p),
            _ => false,
        },
    }
}

/// A legacy KeyConditions value must be the key's schema type. Any other type
/// (a map, a list, a set, BOOL, NULL, or the wrong scalar) is refused with one
/// message, after the check that a BETWEEN's two values share a type. Captured
/// in eu-west-2, us-east-1, us-west-2 and eu-central-1, 2026-10-09.
///
/// Each check runs over every condition before the next starts, so the answer
/// doesn't depend on the order the conditions arrive in.
fn check_legacy_key_condition_types(
    conditions: &HashMap<String, helpers::KeyCondition>,
    meta: &crate::storage::TableMetadata,
) -> Result<()> {
    use crate::types::{AttributeDefinition, ScalarAttributeType};

    let definitions: Vec<AttributeDefinition> = serde_json::from_str(&meta.attribute_definitions)
        .map_err(|e| {
        DynoxideError::InternalServerError(format!("Bad attribute definitions JSON: {e}"))
    })?;
    let lists = || {
        conditions
            .iter()
            .filter_map(|(name, condition)| Some((name, condition.attribute_value_list.as_ref()?)))
    };
    if lists().any(|(_, values)| {
        values
            .windows(2)
            .any(|pair| std::mem::discriminant(&pair[0]) != std::mem::discriminant(&pair[1]))
    }) {
        return Err(DynoxideError::ValidationException(
            "One or more parameter values were invalid: \
             AttributeValues inside AttributeValueList must be of same type"
                .to_string(),
        ));
    }
    for (name, values) in lists() {
        let Some(definition) = definitions.iter().find(|d| &d.attribute_name == name) else {
            continue;
        };
        let matches = |value: &AttributeValue| {
            matches!(
                (value, &definition.attribute_type),
                (AttributeValue::S(_), ScalarAttributeType::S)
                    | (AttributeValue::N(_), ScalarAttributeType::N)
                    | (AttributeValue::B(_), ScalarAttributeType::B)
            )
        };
        if !values.iter().all(matches) {
            return Err(DynoxideError::ValidationException(
                "One or more parameter values were invalid: \
                 Condition parameter type does not match schema type"
                    .to_string(),
            ));
        }
    }
    Ok(())
}

/// A starting key whose table key value is an empty string or empty binary is
/// refused with the empty key-value message, ahead of the check against the key
/// condition. Captured for an empty partition key in eu-west-2, us-east-1,
/// us-west-2 and eu-central-1, 2026-10-09; the binary wording is the one item
/// keys already use.
fn check_start_key_not_empty(
    esk: &Item,
    partition_key: &str,
    sort_key: Option<&str>,
) -> Result<()> {
    for name in std::iter::once(partition_key).chain(sort_key) {
        let kind = match esk.get(name) {
            Some(AttributeValue::S(s)) if s.is_empty() => "string",
            Some(AttributeValue::B(b)) if b.is_empty() => "binary",
            _ => continue,
        };
        return Err(DynoxideError::KeyEmptyValueValidation(format!(
            "One or more parameter values are not valid. The AttributeValue for a key \
             attribute cannot contain an empty {kind} value. Key: {name}"
        )));
    }
    Ok(())
}
