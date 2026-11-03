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
    #[serde(
        rename = "Segment",
        default,
        deserialize_with = "helpers::deserialize_int32"
    )]
    segment: Option<helpers::Int32>,
    #[serde(
        rename = "TotalSegments",
        default,
        deserialize_with = "helpers::deserialize_int32"
    )]
    total_segments: Option<helpers::Int32>,
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

/// The members of a Scan that DynamoDB checks before it reads anything else
/// about the request, and what it says when one fails. Captured in eu-west-2
/// and eu-central-1 (20 October 2026), where the newer validation layer runs
/// Scan, as it does in 20 of the 33 observed regions.
pub(crate) struct ScanMembers<'a> {
    pub table_name: Option<&'a str>,
    pub select: Option<&'a str>,
    pub return_consumed_capacity: Option<&'a str>,
    pub conditional_operator: Option<&'a str>,
    pub scan_filter: Option<&'a serde_json::Value>,
    pub limit: Option<i64>,
    pub segment: Option<i64>,
    pub total_segments: Option<i64>,
    /// Whether `Limit`, `Segment` or `TotalSegments` saturated or lost a
    /// fraction as it was read.
    pub coerced: bool,
}

const TOTAL_SEGMENTS_MAX: i64 = 1_000_000;
const SEGMENT_MAX: i64 = 999_999;

impl ScanMembers<'_> {
    /// The message for the first stage with an error, or `None`. A stage's
    /// errors come back together, and a later stage is not reached while an
    /// earlier one has any:
    ///
    /// 1. A missing table name, a legacy condition with no
    ///    `ComparisonOperator`, or a `Limit`, `Segment` or `TotalSegments` that
    ///    saturated or lost a fraction as it was read hands the request to the
    ///    older layer, which reports every member error it finds, in its
    ///    wording and in this order: `Select`, `TotalSegments` (outside 1 to
    ///    1000000), `ReturnConsumedCapacity`, the table name (missing or
    ///    empty), `ConditionalOperator`, the missing operators, `Segment`
    ///    (outside 0 to 999999), `Limit` below 1.
    /// 2. `Limit` below 1, `Segment` outside 0 to 999999, `TotalSegments`
    ///    outside 1 to 1000000 and an empty table name, in the newer wording:
    ///    the member's name capitalised and the value not echoed.
    /// 3. The first table-name error beside a `ReturnConsumedCapacity` outside
    ///    its enum.
    ///
    /// Everything else about the request waits for `execute`. The wire path
    /// asks here as the body is read and `execute` asks again, so a request
    /// built in code gets the same answer; the one difference is that code
    /// cannot leave the table name out.
    pub(crate) fn rejection(&self) -> Option<String> {
        use crate::validation::{
            EMPTY_TABLE_NAME_REJECTION, TableNameContext, conditional_operator_rejection,
            first_table_name_error, format_validation_errors, return_consumed_capacity_rejection,
            table_name_constraint_errors,
        };

        let missing_operators =
            helpers::missing_comparison_operators(self.scan_filter, "scanFilter");
        if self.table_name.is_none() || !missing_operators.is_empty() || self.coerced {
            let mut errors = Vec::new();
            errors.extend(self.select.and_then(super::query::select_rejection));
            if let Some(total) = self.total_segments {
                if total < 1 {
                    errors.push(format!(
                        "Value '{total}' at 'totalSegments' failed to satisfy constraint: \
                         Member must have value greater than or equal to 1"
                    ));
                } else if total > TOTAL_SEGMENTS_MAX {
                    errors.push(format!(
                        "Value '{total}' at 'totalSegments' failed to satisfy constraint: \
                         Member must have value less than or equal to {TOTAL_SEGMENTS_MAX}"
                    ));
                }
            }
            errors.extend(return_consumed_capacity_rejection(
                self.return_consumed_capacity,
            ));
            if matches!(self.table_name, None | Some("")) {
                errors.extend(table_name_constraint_errors(
                    self.table_name,
                    TableNameContext::ReadWrite,
                ));
            }
            errors.extend(conditional_operator_rejection(self.conditional_operator));
            errors.extend(missing_operators);
            if let Some(segment) = self.segment {
                if segment < 0 {
                    errors.push(format!(
                        "Value '{segment}' at 'segment' failed to satisfy constraint: \
                         Member must have value greater than or equal to 0"
                    ));
                } else if segment > SEGMENT_MAX {
                    errors.push(format!(
                        "Value '{segment}' at 'segment' failed to satisfy constraint: \
                         Member must have value less than or equal to {SEGMENT_MAX}"
                    ));
                }
            }
            if let Some(limit) = self.limit.filter(|l| *l < 1) {
                errors.push(format!(
                    "Value '{limit}' at 'limit' failed to satisfy constraint: \
                     Member must have value greater than or equal to 1"
                ));
            }
            return format_validation_errors(&errors);
        }

        let mut errors = Vec::new();
        if self.limit.is_some_and(|l| l < 1) {
            errors.push(
                "Value at 'Limit' failed to satisfy constraint: \
                 Member must have value greater than or equal to 1"
                    .to_string(),
            );
        }
        if let Some(segment) = self.segment {
            if segment < 0 {
                errors.push(
                    "Value at 'Segment' failed to satisfy constraint: \
                     Member must have value greater than or equal to 0"
                        .to_string(),
                );
            } else if segment > SEGMENT_MAX {
                errors.push(format!(
                    "Value at 'Segment' failed to satisfy constraint: \
                     Member must have value less than or equal to {SEGMENT_MAX}"
                ));
            }
        }
        if let Some(total) = self.total_segments {
            if total < 1 {
                errors.push(
                    "Value at 'TotalSegments' failed to satisfy constraint: \
                     Member must have value greater than or equal to 1"
                        .to_string(),
                );
            } else if total > TOTAL_SEGMENTS_MAX {
                errors.push(format!(
                    "Value at 'TotalSegments' failed to satisfy constraint: \
                     Member must have value less than or equal to {TOTAL_SEGMENTS_MAX}"
                ));
            }
        }
        if self.table_name == Some("") {
            errors.push(EMPTY_TABLE_NAME_REJECTION.to_string());
        }
        if let Some(msg) = format_validation_errors(&errors) {
            return Some(msg);
        }

        let mut errors = Vec::new();
        errors.extend(self.table_name.and_then(first_table_name_error));
        errors.extend(return_consumed_capacity_rejection(
            self.return_consumed_capacity,
        ));
        format_validation_errors(&errors)
    }
}

impl<'de> serde::Deserialize<'de> for ScanRequest {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let raw = ScanRequestRaw::deserialize(deserializer)?;
        // A BOOL or NULL in a legacy condition, or its `Exists`, that doesn't
        // read as a boolean makes the whole request malformed, ahead of
        // everything else.
        if let Some(message) = crate::types::malformed_condition_boolean(raw.scan_filter.as_ref()) {
            return Err(serde::de::Error::custom(message));
        }

        // The members DynamoDB checks before it reads anything else, in
        // the stages `ScanMembers::rejection` describes.
        if let Some(msg) = (ScanMembers {
            table_name: raw.table_name.as_deref(),
            select: raw.select.as_deref(),
            return_consumed_capacity: raw.return_consumed_capacity.as_deref(),
            conditional_operator: raw.conditional_operator.as_deref(),
            scan_filter: raw.scan_filter.as_ref(),
            limit: raw.limit.map(|n| n.value),
            segment: raw.segment.map(|n| n.value),
            total_segments: raw.total_segments.map(|n| n.value),
            coerced: [raw.limit, raw.segment, raw.total_segments]
                .iter()
                .flatten()
                .any(|n| n.coerced),
        })
        .rejection()
        {
            return Err(serde::de::Error::custom(format!("VALIDATION:{}", msg)));
        }
        let table_name = raw.table_name.unwrap_or_default();

        Ok(ScanRequest {
            table_name,
            filter_expression: raw.filter_expression,
            projection_expression: raw.projection_expression,
            expression_attribute_names: raw.expression_attribute_names,
            expression_attribute_values: raw.expression_attribute_values,
            limit: raw.limit.map(|n| n.value as usize),
            exclusive_start_key: None,
            select: raw.select,
            consistent_read: raw.consistent_read,
            index_name: raw.index_name,
            segment: raw.segment.map(|n| n.value as u32),
            total_segments: raw.total_segments.map(|n| n.value as u32),
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
    #[serde(
        rename = "Items",
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::set_order::items_in_hash_order"
    )]
    // Number sets in Java hash-set order here, as DynamoDB returns them;
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
    mut request: ScanRequest,
) -> Result<ScanResponse> {
    // The request's own members, as the wire path checks them when it reads the
    // body: a request built in code gets the same answer.
    if let Some(msg) = (ScanMembers {
        table_name: Some(&request.table_name),
        select: request.select.as_deref(),
        return_consumed_capacity: request.return_consumed_capacity.as_deref(),
        conditional_operator: request.conditional_operator.as_deref(),
        scan_filter: request.scan_filter.as_ref(),
        limit: request.limit.map(|l| i64::try_from(l).unwrap_or(i64::MAX)),
        segment: request.segment.map(i64::from),
        total_segments: request.total_segments.map(i64::from),
        coerced: false,
    })
    .rejection()
    {
        return Err(DynoxideError::ValidationException(msg));
    }

    // A Limit past what a 32-bit integer holds is read as the largest one.
    request.limit = request.limit.map(|l| l.min(i32::MAX as usize));

    // An empty attribute name is refused ahead of the table lookup and the
    // expressions, bare whichever member holds it: 20 of 33 regions answer so
    // for the ExclusiveStartKey and the expression values (2026-10-20); the
    // other 13 name what they are. An empty name in ScanFilter itself is
    // accepted.
    if crate::empty_names::names_include_empty(request.attributes_to_get.as_ref())
        || crate::empty_names::filter_values_have_empty_name(request.scan_filter.as_ref())
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
            layer: crate::validation::ValueLayer::Current,
        };
        helpers::validate_expression_params(&ctx)?;
    }

    // ---- ConditionalOperator only joins legacy ScanFilter conditions, so a
    // valid one is refused without two of them; Scan reports it without the
    // envelope (captured 2026-10-09, all seven regions). ----
    if let Some(msg) = crate::validation::conditional_operator_misuse(
        request.conditional_operator.as_deref(),
        crate::validation::legacy_condition_count(request.scan_filter.as_ref()),
    ) {
        return Err(DynoxideError::ValidationException(msg.to_string()));
    }
    // ---- A ScanFilter value nested too deep is refused first ----
    crate::value_nesting::check_scan_filter(request.scan_filter.as_ref())?;

    // ---- Validate ScanFilter attribute values (before argument counts, matching DynamoDB) ----
    helpers::validate_filter_conditions_raw(
        request.scan_filter.as_ref(),
        "ScanFilter",
        crate::validation::ValueLayer::Current,
    )?;

    // ---- Validate filter argument counts and type compatibility ----
    helpers::validate_filter_condition_args(request.scan_filter.as_ref(), "scanFilter")?;

    // ---- Validate duplicate AttributesToGet ----
    if let Some(ref attrs) = request.attributes_to_get {
        helpers::validate_attributes_to_get_no_duplicates(attrs)?;
    }

    // ---- Parse ExclusiveStartKey from JSON value ----
    let exclusive_start_key = if let Some(ref esk_val) = request.exclusive_start_key_raw {
        Some(helpers::parse_exclusive_start_key(
            esk_val,
            crate::validation::ValueLayer::Current,
        )?)
    } else {
        request.exclusive_start_key.clone()
    };
    crate::value_nesting::check_scan_start_key(exclusive_start_key.as_ref())?;

    // Convert legacy ScanFilter to FilterExpression if no expression is set
    let scan_filter: Option<HashMap<String, helpers::FilterCondition>> =
        match request.scan_filter.as_ref() {
            Some(sf_val) if request.filter_expression.is_none() && !sf_val.is_null() => Some(
                helpers::read_legacy_conditions(sf_val, crate::validation::ValueLayer::Current)?,
            ),
            _ => None,
        };
    if let Some(sf) = scan_filter
        && !sf.is_empty()
    {
        let converted =
            helpers::convert_filter_conditions(&sf, request.conditional_operator.as_deref())?;
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
            // A Scan is the one FilterExpression whose size message still
            // carries the figure in us-east-1, so it is checked here first.
            expressions::check_expression_size_with_figure(filter_expr_str).map_err(|e| {
                DynoxideError::ValidationException(format!("Invalid FilterExpression: {e}"))
            })?;
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
    if let Some(ref proj_expr_str) = request.projection_expression
        && proj_expr_str.is_empty()
    {
        return Err(DynoxideError::ValidationException(
            "Invalid ProjectionExpression: The expression can not be empty;".to_string(),
        ));
    }

    // Select outside its enum, then Select against the projection members and
    // the index, each in the envelope, then the parallel-scan members. The range of
    // TotalSegments was settled with the other members above.
    if let Some(msg) = request
        .select
        .as_deref()
        .and_then(super::query::select_rejection)
    {
        return Err(DynoxideError::ValidationException(
            crate::validation::envelope_message(&msg),
        ));
    }
    if let Some(msg) = helpers::select_projection_rejection(
        request.select.as_deref(),
        request.attributes_to_get.is_some(),
        request.projection_expression.is_some(),
        request.index_name.is_some(),
    ) {
        return Err(DynoxideError::ValidationException(msg));
    }

    match (request.segment, request.total_segments) {
        (Some(segment), Some(total)) if segment >= total => {
            return Err(DynoxideError::ValidationException(
                "The Segment parameter is zero-based and must be less than parameter TotalSegments"
                    .to_string(),
            ));
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
        _ => {}
    }

    let meta = helpers::require_table_for_item_op(storage, &request.table_name).await?;
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
            // A vector index is listed by DescribeTable but no Scan can read
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

    // Select ALL_ATTRIBUTES on a global index that doesn't project everything is
    // refused ahead of the starting key's checks (captured eu-west-2 and
    // eu-central-1, 20 October 2026).
    if let Some(ref index_name) = request.index_name
        && !is_lsi
        && let Some(ref select) = request.select
        && select == "ALL_ATTRIBUTES"
    {
        let gsi_defs = super::gsi::parse_gsi_defs(&meta)?;
        if let Some(gsi) = gsi_defs.iter().find(|g| g.index_name == *index_name)
            && gsi.projection_type != crate::types::ProjectionType::ALL
        {
            return Err(DynoxideError::ValidationException(format!(
                "One or more parameter values were invalid: \
                                 Select type ALL_ATTRIBUTES is not supported for global secondary index {} \
                                 because its projection type is not ALL",
                index_name
            )));
        }
    }

    // ---- Validate ExclusiveStartKey structure against key schema ----
    // Stage 1+2: count check + index key type check
    if let Some(ref esk) = exclusive_start_key {
        helpers::validate_esk_count_and_index_keys(
            esk,
            &meta,
            request.index_name.as_deref(),
            "The provided starting key is invalid",
            true,
        )?;
    }

    // Stage 3: table key type check
    if let Some(ref esk) = exclusive_start_key {
        helpers::validate_esk_table_keys(esk, &meta)
            .map_err(super::query::bare_start_key_mismatch)?;
    }

    // Extract ExclusiveStartKey pk/sk using effective key names
    let (start_pk, start_sk) = if let Some(ref esk) = exclusive_start_key {
        let key_string = |attrs: &[String]| {
            let values: Option<Vec<&AttributeValue>> = attrs.iter().map(|a| esk.get(a)).collect();
            values.and_then(|v| super::gsi::composite_key_string(&v))
        };
        let pk = key_string(&key_hash);
        let sk = if key_range.is_empty() {
            Some(String::new())
        } else {
            key_string(&key_range)
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
            // When the base table is hash-only there is no base sort key, but
            // the GSI/LSI row stores the empty-string default in its table_sk
            // column. Default base_sk to "" (mirroring start_sk above) so the
            // composite cursor keeps its full width and disambiguates tied
            // index keys by table_pk. Leaving it None collapses scan_gsi_items
            // to the 2-column (gsi_pk, gsi_sk) cursor, which cannot advance past
            // rows that share the same index key and silently drops them.
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
            storage
                .scan_lsi_items(&request.table_name, index_name, &scan_params)
                .await?
        } else {
            storage
                .scan_gsi_items(&request.table_name, index_name, &scan_params)
                .await?
        }
    } else {
        storage
            .scan_items(&request.table_name, &scan_params)
            .await?
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
            for attr in key_hash.iter().chain(&key_range) {
                if !base_key_attrs.contains(attr) {
                    index_key_attrs.push(attr.clone());
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
        // Reject undefined names and overlapping paths before the row loop, so a
        // zero-match scan still rejects.
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

    // A scan of the table itself is charged on its items' billed size, a
    // vector at four bytes per dimension (captured eu-west-2 and us-east-1,
    // 2026-10-23). Index entries stay sized by their numbers, and the 1MB page
    // limit stays on the stored size.
    let vector_defs = super::vector_index::parse_vector_defs(&meta)?;
    let mut billed_cumulative_size = 0usize;

    for (_pk, _sk, item_json) in &rows {
        let item: Item = serde_json::from_str(item_json).map_err(|e| {
            DynoxideError::InternalServerError(format!("Bad item JSON in storage: {e}"))
        })?;

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
            let mut key: HashMap<String, AttributeValue> = key_hash
                .iter()
                .chain(&key_range)
                .filter_map(|name| item.get(name).map(|v| (name.clone(), v.clone())))
                .collect();
            // For LSI scans, add the table sort key if different from the index sort key
            if is_lsi
                && let Some(tsk) = table_key_schema.sort_key.as_deref()
                && !key.contains_key(tsk)
                && let Some(v) = item.get(tsk)
            {
                key.insert(tsk.to_string(), v.clone());
            }
            // For GSI scans, add the base table primary key (pk and sk)
            if is_gsi_scan {
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
            crate::types::read_capacity_units_with_consistency(
                if request.index_name.is_none() {
                    billed_cumulative_size
                } else {
                    cumulative_size
                },
                consistent,
            ),
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

/// Refuse a read against a vector index by type.
///
/// The wording differs between the two operations, full stop included: AWS
/// ends Scan's with one where the other has none. Captured eu-west-2,
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
            "Scan operation not supported on this index type".to_string(),
        ));
    }
    Ok(())
}
