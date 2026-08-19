//! The SearchVectors operation: exact brute-force KNN over a vector index's
//! shadow table.
//!
//! Validation ordering and every pinned error string come from real DynamoDB
//! behaviour captured in eu-west-2 on 2026-08-11, with follow-up captures on
//! 2026-08-12 and 2026-08-13 that were byte-identical in eu-west-2 and
//! us-east-1. Where the captures are silent (noted inline) the behaviour
//! follows the Query family's conventions.
//!
//! Scoring follows the captured precision rule: elements are f32 (the
//! index's storage precision), but all accumulation and the full score
//! computation run in f64, and the final score saturates to the f32 range,
//! so a score that would overflow f32 reports exactly f32::MAX or -f32::MAX,
//! never infinity or NaN (captured from real DynamoDB (eu-west-2 and
//! us-east-1, 2026-08-13)). The zero-magnitude COSINE operand keeps its
//! defined answer (exactly 1.0, captured) for true zero vectors only; an
//! operand whose f32 squares would underflow scores normally under the f64
//! accumulation. Equal scores order by score then
//! base-table primary key; real DynamoDB's tie order is non-deterministic
//! (three identical calls returned three orderings, captured 2026-08-12), so
//! a deterministic local order is a benign documented divergence.

use crate::actions::helpers;
use crate::actions::vector_index::{
    hash_attr, parse_attr_defs, parse_vector_defs, scalar_type_str,
};
use crate::errors::{DynoxideError, Result};
use crate::expressions;
use crate::expressions::PathElement;
use crate::expressions::condition::{CompOp, ConditionExpr, Operand};
use crate::storage_backend::{StorageBackend, VectorCandidateRow};
use crate::types::{AttributeValue, Item, VectorIndex};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Internal deserialisation struct for detecting missing fields.
#[derive(Debug, Default, Deserialize)]
struct SearchVectorsRequestRaw {
    #[serde(rename = "TableName", default)]
    table_name: Option<String>,
    #[serde(rename = "IndexName", default)]
    index_name: Option<String>,
    /// A bare JSON array of number attribute values, not wrapped in `L`
    /// (confirmed on the wire against the AWS SDK's serialisation).
    #[serde(rename = "SearchVector", default)]
    search_vector: Option<Vec<AttributeValue>>,
    /// Deserialised as i32 so a fractional or out-of-i32-range value rejects
    /// inside serde with the raw serde-style SerializationException message
    /// real DynamoDB returns for a fractional TopK: "invalid type: floating
    /// point `3.5`, expected i32 at line 1 column N", captured from real
    /// DynamoDB (eu-west-2 and us-east-1, 2026-08-13). See
    /// `serde_errors::map_serde_to_dynamodb_message` for the pass-through.
    #[serde(rename = "TopK", default)]
    top_k: Option<i32>,
    #[serde(rename = "SearchConditionExpression", default)]
    search_condition_expression: Option<String>,
    #[serde(rename = "ProjectionExpression", default)]
    projection_expression: Option<String>,
    #[serde(rename = "ExpressionAttributeNames", default)]
    expression_attribute_names: Option<HashMap<String, String>>,
    #[serde(rename = "ExpressionAttributeValues", default)]
    expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    #[serde(rename = "ReturnConsumedCapacity", default)]
    return_consumed_capacity: Option<String>,
}

/// A SearchVectors request.
#[derive(Debug, Default)]
pub struct SearchVectorsRequest {
    pub table_name: String,
    pub index_name: String,
    /// The query vector as wire-shaped attribute values; validated against
    /// the index in `execute`.
    pub search_vector: Vec<AttributeValue>,
    pub top_k: i64,
    pub search_condition_expression: Option<String>,
    pub projection_expression: Option<String>,
    pub expression_attribute_names: Option<HashMap<String, String>>,
    pub expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    pub return_consumed_capacity: Option<String>,
}

impl<'de> serde::Deserialize<'de> for SearchVectorsRequest {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let raw = SearchVectorsRequestRaw::deserialize(deserializer)?;
        use crate::validation::{
            TableNameContext, format_validation_errors, table_name_constraint_errors,
        };

        let mut errors = Vec::new();
        errors.extend(table_name_constraint_errors(
            raw.table_name.as_deref(),
            TableNameContext::ReadWrite,
        ));

        // Required members reject at the request-model layer. The null-member
        // wording follows the captured request-model convention (`Value null
        // at '<path>' failed to satisfy constraint: Member must not be null`,
        // captured on the UpdateTable path, 2026-08-12); this operation's
        // member paths and aggregation order are uncaptured.
        match raw.index_name.as_deref() {
            None => errors.push(
                "Value null at 'indexName' failed to satisfy constraint: \
                 Member must not be null"
                    .to_string(),
            ),
            // The short-name rejection carries no value echo and spells the
            // member path 'IndexName', unlike the camelCase null convention
            // above (captured from real DynamoDB (eu-west-2 and us-east-1,
            // 2026-08-13)). The upper bound mirrors the 255 the CreateTable
            // family enforces; its wording for this operation is uncaptured
            // beyond the no-echo form the captured lower bound establishes.
            Some(name) if name.len() < 3 => errors.push(
                "Value at 'IndexName' failed to satisfy constraint: \
                 Member must have length greater than or equal to 3"
                    .to_string(),
            ),
            Some(name) if name.len() > 255 => errors.push(
                "Value at 'IndexName' failed to satisfy constraint: \
                 Member must have length less than or equal to 255"
                    .to_string(),
            ),
            Some(_) => {}
        }
        match raw.search_vector.as_deref() {
            None => errors.push(
                "Value null at 'searchVector' failed to satisfy constraint: \
                 Member must not be null"
                    .to_string(),
            ),
            // An empty vector rejects in the same no-echo, 'SearchVector'
            // member-path form as the IndexName constraint (captured from
            // real DynamoDB (eu-west-2 and us-east-1, 2026-08-13)).
            Some([]) => errors.push(
                "Value at 'SearchVector' failed to satisfy constraint: \
                 Member must have length greater than or equal to 1"
                    .to_string(),
            ),
            Some(_) => {}
        }
        // TopK 0 rejects at the request-model layer (captured 2026-08-11);
        // the exact wording is uncaptured and follows the
        // greater-than-or-equal convention the Dimensions member uses. Only
        // the lower bound lives here: 101 rejects bare at the operation
        // layer with its own captured message, and non-integer values never
        // reach this point (the i32 field rejects them inside serde).
        match raw.top_k {
            None => errors.push(
                "Value null at 'topK' failed to satisfy constraint: \
                 Member must not be null"
                    .to_string(),
            ),
            Some(v) if v < 1 => errors.push(format!(
                "Value '{v}' at 'topK' failed to satisfy constraint: \
                 Member must have value greater than or equal to 1"
            )),
            Some(_) => {}
        }

        // ReturnConsumedCapacity enum validation
        if let Some(msg) = crate::validation::return_consumed_capacity_rejection(
            raw.return_consumed_capacity.as_deref(),
        ) {
            errors.push(msg);
        }

        if let Some(msg) = format_validation_errors(&errors) {
            return Err(serde::de::Error::custom(format!("VALIDATION:{msg}")));
        }

        Ok(SearchVectorsRequest {
            table_name: raw.table_name.unwrap_or_default(),
            index_name: raw.index_name.unwrap_or_default(),
            search_vector: raw.search_vector.unwrap_or_default(),
            top_k: raw.top_k.map(i64::from).unwrap_or_default(),
            search_condition_expression: raw.search_condition_expression,
            projection_expression: raw.projection_expression,
            expression_attribute_names: raw.expression_attribute_names,
            expression_attribute_values: raw.expression_attribute_values,
            return_consumed_capacity: raw.return_consumed_capacity,
        })
    }
}

/// A SearchVectors response: ranked results, best first. `SearchResults` is
/// the response's only data key; there is no pagination surface (captured).
///
/// `ConsumedCapacity` carries request bytes alone, with no `CapacityUnits` and
/// no `TableName`, and reads the same under `TOTAL` and `INDEXES`.
#[derive(Debug, Default, Serialize)]
pub struct SearchVectorsResponse {
    #[serde(rename = "SearchResults")]
    pub search_results: Vec<SearchResult>,
    #[serde(rename = "ConsumedCapacity", skip_serializing_if = "Option::is_none")]
    pub consumed_capacity: Option<crate::types::VectorSearchCapacity>,
}

/// The billable size of a search: the index entries it scanned, held to the
/// 1KB floor.
///
/// Captured against eu-west-2 on 2026-08-18, real DynamoDB bills this on the
/// data the search read rather than on the request. The figure moved with the
/// table's contents and not with the query, and it does not repeat: five
/// identical searches over one unchanged 512-dimension index reported 14214,
/// 13903, 14214, 14214 and 14518. There is no figure to match, so this reports
/// the same quantity deterministically instead.
///
/// The measure is DynamoDB's own item measure over each entry, the same one the
/// write axis is captured against, summed over the entries the search scanned.
/// Each entry's size is computed once at write and stored with the row, so this
/// reads a column rather than rebuilding what it scanned; an earlier cut sized
/// on the serialised length instead, to avoid parsing every entry, and this
/// needs neither compromise.
///
/// The quantity still diverges from AWS by construction, because AWS does not
/// reproduce its own, so it is not evidence in a conformance claim. What has
/// changed is that the unit is now the captured one rather than a stand-in.
///
/// A documented divergence in the same class as the equal-score tie break:
/// deterministic where AWS is not.
fn search_scanned_bytes(candidates: &[VectorCandidateRow]) -> f64 {
    let scanned: i64 = candidates.iter().map(|row| row.entry_bytes).sum();
    crate::types::vector_request_bytes(scanned.max(0) as usize)
}

/// One search hit: the projected item and its score as a JSON double.
#[derive(Debug, Serialize)]
pub struct SearchResult {
    #[serde(rename = "Item")]
    pub item: Item,
    #[serde(rename = "Score")]
    pub score: f64,
}

/// The captured rejection for a query vector whose elements are not all
/// finite 32-bit floats (an L-wrapped element, a non-number, or an
/// out-of-f32-range value all report the same string; captured 2026-08-11
/// and 2026-08-12).
fn invalid_search_vector() -> DynoxideError {
    DynoxideError::ValidationException(
        "Search vector contains invalid values. All values in the search vector must be a \
         32-bit floating-point number attribute"
            .to_string(),
    )
}

/// The captured rejection for a SearchConditionExpression that is not a pure
/// AND of `=` comparisons. Captured for non-equality comparators on both HASH
/// and INLINE_FILTER elements; other disallowed shapes (OR, NOT, BETWEEN, IN,
/// functions, operand pairings without a path and a value) are uncaptured and
/// adopt the same rejection.
fn invalid_comparator() -> DynoxideError {
    DynoxideError::ValidationException(
        "Invalid SearchConditionExpression: Invalid comparator used in SearchConditionExpression"
            .to_string(),
    )
}

/// One `attribute = :value` conjunct of a SearchConditionExpression.
struct EqualityPair {
    /// Resolved top-level attribute name.
    attr: String,
    /// Whether the path descends below the top level. A nested path can never
    /// equal a stored scalar schema value, so such a pair matches nothing
    /// (uncaptured; the top-level name still counts for schema membership).
    nested: bool,
    /// The resolved comparison operand.
    value: AttributeValue,
}

/// Restriction pass over the parsed condition AST: only `=` comparisons
/// joined by AND are legal, each pairing a document path with a value
/// reference. Collects the equality pairs and every referenced top-level
/// attribute name.
fn collect_equalities(
    expr: &ConditionExpr,
    tracker: &expressions::TrackedExpressionAttributes,
    pairs: &mut Vec<EqualityPair>,
    attrs: &mut Vec<String>,
) -> Result<()> {
    match expr {
        ConditionExpr::And(left, right) => {
            collect_equalities(left, tracker, pairs, attrs)?;
            collect_equalities(right, tracker, pairs, attrs)?;
            Ok(())
        }
        ConditionExpr::Comparison { left, op, right } => {
            if *op != CompOp::Eq {
                return Err(invalid_comparator());
            }
            let (path, value_ref) = match (left, right) {
                (Operand::Path(p), Operand::ValueRef(v))
                | (Operand::ValueRef(v), Operand::Path(p)) => (p, v),
                _ => return Err(invalid_comparator()),
            };
            let top = match path.first() {
                Some(PathElement::Attribute(name)) => tracker.resolve_name(name).map_err(|e| {
                    DynoxideError::ValidationException(format!(
                        "Invalid SearchConditionExpression: {e}"
                    ))
                })?,
                // A path starting with an index access cannot parse, so this
                // arm is defensive.
                _ => return Err(invalid_comparator()),
            };
            // An undefined `:value` carries the operation's prefix, matching
            // the `resolve_name` arm above and the captured prefix
            // convention (the prefixed form itself is uncaptured for value
            // references).
            let value = tracker
                .resolve_value(value_ref)
                .map_err(|e| {
                    DynoxideError::ValidationException(format!(
                        "Invalid SearchConditionExpression: {e}"
                    ))
                })?
                .clone();
            attrs.push(top.clone());
            pairs.push(EqualityPair {
                attr: top,
                nested: path.len() > 1,
                value,
            });
            Ok(())
        }
        _ => Err(invalid_comparator()),
    }
}

/// Compute one candidate's score against the query vector. Elements are f32
/// (the index's storage precision), but all accumulation and the full score
/// computation run in f64, and the final score saturates to the f32 range:
/// AWS accumulates in wider-than-f32 precision and clamps the result, so
/// cancelling DOT_PRODUCT terms whose products overflow f32 report exactly 0
/// and an overflowing EUCLIDEAN distance reports exactly f32::MAX rather
/// than infinity (captured from real DynamoDB (eu-west-2 and us-east-1,
/// 2026-08-13)).
fn score_candidate(distance_function: &str, query: &[f32], stored: &[f32]) -> f64 {
    let score = match distance_function {
        "EUCLIDEAN" => {
            let mut sum = 0.0f64;
            for (q, s) in query.iter().zip(stored) {
                let d = f64::from(*q) - f64::from(*s);
                sum += d * d;
            }
            sum.sqrt()
        }
        "DOT_PRODUCT" => {
            // Higher is better; negative values are legal (captured).
            let mut dot = 0.0f64;
            for (q, s) in query.iter().zip(stored) {
                dot += f64::from(*q) * f64::from(*s);
            }
            dot
        }
        // COSINE
        _ => {
            let mut dot = 0.0f64;
            let mut qq = 0.0f64;
            let mut ss = 0.0f64;
            for (q, s) in query.iter().zip(stored) {
                dot += f64::from(*q) * f64::from(*s);
                qq += f64::from(*q) * f64::from(*q);
                ss += f64::from(*s) * f64::from(*s);
            }
            // A zero-magnitude operand on either side scores exactly 1.0
            // (captured 2026-08-12): an explicit branch, never a division.
            // Only a true zero vector lands here: the f64 squares of finite
            // f32 elements cannot underflow to zero, so an operand like
            // [1e-23, 0] scores normally (captured from real DynamoDB
            // (eu-west-2 and us-east-1, 2026-08-13)).
            if qq == 0.0 || ss == 0.0 {
                return 1.0;
            }
            // A self-match must land exactly on the extremal score; rounding
            // through the norms could otherwise leave sign noise near zero.
            if query == stored {
                return 0.0;
            }
            let c = 1.0 - dot / (qq.sqrt() * ss.sqrt());
            // Clamp at the metric boundaries so floating-point noise can
            // never report a negative distance or one beyond opposite.
            c.clamp(0.0, 2.0)
        }
    };
    // Saturate the final score to the f32 range, preserving sign: a value
    // that would overflow becomes exactly f32::MAX or -f32::MAX (captured
    // from real DynamoDB (eu-west-2 and us-east-1, 2026-08-13)).
    score.clamp(-f64::from(f32::MAX), f64::from(f32::MAX))
}

/// Whether this distance function ranks higher scores as better.
fn higher_is_better(distance_function: &str) -> bool {
    distance_function == "DOT_PRODUCT"
}

/// Validate the query vector: element validity first, then the dimension
/// count. The order is captured: an L-wrapped single element reports the
/// invalid-values message even though its element count also mismatches.
fn parse_query_vector(search_vector: &[AttributeValue], vix: &VectorIndex) -> Result<Vec<f32>> {
    let mut out = Vec::with_capacity(search_vector.len());
    for elem in search_vector {
        let AttributeValue::N(n) = elem else {
            return Err(invalid_search_vector());
        };
        // Out-of-f32-range values overflow to infinity on conversion; real
        // DynamoDB rejects them with the same invalid-values string
        // (captured 2026-08-12).
        match n.parse::<f32>() {
            Ok(v) if v.is_finite() => out.push(v),
            _ => return Err(invalid_search_vector()),
        }
    }
    if out.len() != vix.dimensions as usize {
        return Err(DynoxideError::ValidationException(format!(
            "Input search vector dimension {} does not match vector index dimension {}",
            out.len(),
            vix.dimensions
        )));
    }
    Ok(out)
}

pub async fn execute<S: StorageBackend>(
    storage: &S,
    request: SearchVectorsRequest,
) -> Result<SearchVectorsResponse> {
    // Validate table name format before checking existence, mirroring the
    // Query family.
    crate::validation::validate_table_name(&request.table_name)?;

    // TopK beyond the request-model lower bound rejects bare with the value
    // interpolated (captured 2026-08-11: 101). Checked before the table
    // lookup, mirroring the Query family's input-first posture (the relative
    // order against table existence is uncaptured). The lower arm is
    // unreachable through the wire (the request model rejects it first,
    // enveloped) and guards direct in-process construction.
    if !(1..=100).contains(&request.top_k) {
        return Err(DynoxideError::ValidationException(format!(
            "Provided TopK value '{}' is out of valid range. The value must be between 1 and \
             100 inclusive",
            request.top_k
        )));
    }

    // Names/values without any expression reject the way the Query family
    // rejects them (uncaptured for this operation).
    {
        let mut expr = Vec::new();
        if request.search_condition_expression.is_some() {
            expr.push("SearchConditionExpression");
        }
        if request.projection_expression.is_some() {
            expr.push("ProjectionExpression");
        }
        let no_raw_eav: Option<serde_json::Value> = None;
        let ctx = helpers::ExpressionParamContext {
            non_expression_params: Vec::new(),
            expression_params: expr,
            all_expression_param_names: vec!["SearchConditionExpression"],
            expression_attribute_names: &request.expression_attribute_names,
            expression_attribute_values: &request.expression_attribute_values,
            expression_attribute_values_raw: &no_raw_eav,
        };
        helpers::validate_expression_params(&ctx)?;
    }

    // Expression syntax and the equality-only restriction pass run before the
    // table existence check, mirroring the Query family's captured
    // validate-input-first ordering (uncaptured for this operation). Schema
    // membership needs the index definition and runs after resolution below.
    let tracker = expressions::TrackedExpressionAttributes::new(
        &request.expression_attribute_names,
        &request.expression_attribute_values,
    );

    let mut pairs: Vec<EqualityPair> = Vec::new();
    let mut referenced_attrs: Vec<String> = Vec::new();
    let condition = match request.search_condition_expression.as_deref() {
        Some("") => {
            return Err(DynoxideError::ValidationException(
                "Invalid SearchConditionExpression: The expression can not be empty;".to_string(),
            ));
        }
        Some(expr_str) => {
            // The parser applies the shared expression size limit; its errors
            // (and undefined `#name` references) carry the operation's prefix
            // the way FilterExpression errors do on Query. Only the
            // non-equality comparator string is captured with the prefix; the
            // rest of the family adopts it.
            let parsed = expressions::condition::parse(expr_str).map_err(|e| {
                DynoxideError::ValidationException(format!(
                    "Invalid SearchConditionExpression: {e}"
                ))
            })?;
            expressions::condition::validate_name_refs(
                &parsed,
                &request.expression_attribute_names,
            )
            .map_err(|e| {
                DynoxideError::ValidationException(format!(
                    "Invalid SearchConditionExpression: {e}"
                ))
            })?;
            collect_equalities(&parsed, &tracker, &mut pairs, &mut referenced_attrs)?;
            Some(parsed)
        }
        None => None,
    };

    let projection = match request.projection_expression.as_deref() {
        Some("") => {
            return Err(DynoxideError::ValidationException(
                "Invalid ProjectionExpression: The expression can not be empty;".to_string(),
            ));
        }
        Some(proj_expr) => Some(
            expressions::projection::parse(proj_expr)
                .map_err(DynoxideError::ValidationException)?,
        ),
        None => None,
    };

    let meta = helpers::require_table_for_item_op(storage, &request.table_name).await?;
    let vixs = parse_vector_defs(&meta)?;
    let Some(vix) = vixs.iter().find(|v| v.index_name == request.index_name) else {
        return Err(DynoxideError::ValidationException(format!(
            "The table does not have the specified index: {}",
            request.index_name
        )));
    };

    let query_vec = parse_query_vector(&request.search_vector, vix)?;

    // Every referenced attribute must belong to the SearchSchema. The message
    // preserves AWS's own grammar exactly as captured.
    for attr in &referenced_attrs {
        let in_schema = vix
            .search_schema
            .as_ref()
            .is_some_and(|schema| schema.iter().any(|e| e.attribute_name == *attr));
        if !in_schema {
            return Err(DynoxideError::ValidationException(format!(
                "SearchConditionExpression must not contain any attributes that is not in \
                 SearchSchema. Invalid attribute: {attr}"
            )));
        }
    }

    // A condition on a nested attribute is refused outright, with one message
    // covering both element types (captured eu-west-2, 2026-08-19). Without
    // this a nested path silently matched nothing, so a malformed search was
    // indistinguishable from one that found nothing.
    if pairs.iter().any(|p| p.nested) {
        return Err(DynoxideError::ValidationException(
            "Invalid SearchConditionExpression: SearchConditionExpression cannot have \
             conditions on nested attributes"
                .to_string(),
        ));
    }

    // Exactly one condition per attribute. Two that contradict, two that agree,
    // and two carrying a type error between them all give this same message,
    // and the order they appear in does not change it (captured eu-west-2,
    // 2026-08-19). Reconciling two scope values would be modelling something
    // the wire never accepts, so the checks below can assume one pair each.
    {
        let mut seen: Vec<&str> = Vec::with_capacity(pairs.len());
        for pair in &pairs {
            if seen.contains(&pair.attr.as_str()) {
                return Err(DynoxideError::ValidationException(
                    "Invalid SearchConditionExpression: SearchConditionExpression must only \
                     contain one condition per attribute"
                        .to_string(),
                ));
            }
            seen.push(&pair.attr);
        }
    }

    // A HASH-schema index scopes every search to one partition value, so an
    // equality on the HASH attribute is mandatory. Captured for the
    // absent-expression case; an expression that omits the HASH attribute
    // adopts the same string (uncaptured).
    let index_hash_attr = hash_attr(vix);
    if let Some(hash) = index_hash_attr {
        let provided = pairs.iter().any(|p| p.attr == hash && !p.nested);
        if !provided {
            return Err(DynoxideError::ValidationException(
                "SearchConditionExpression must be provided when SearchSchema has a HASH key"
                    .to_string(),
            ));
        }
    }

    // Pre-register expression references so the unused check works even with
    // zero candidates, mirroring Query.
    if let Some(ref cond) = condition {
        tracker.track_condition_expr(cond);
    }
    if let Some(ref proj) = projection {
        tracker.track_projection_expr(proj);
        expressions::projection::validate(proj, &tracker)
            .map_err(DynoxideError::ValidationException)?;
    }

    // Candidate load, scoped through the indexed hash_value column when the
    // schema declares a HASH element. The operand is encoded with the same
    // key-string encoding the write path stores, so the two sides agree by
    // construction.
    let mut hash_scope: Option<String> = None;
    let mut unscopable_hash = false;
    if let Some(hash) = index_hash_attr {
        let attr_defs = parse_attr_defs(&meta)?;
        // At most one pair survives the duplicate check above, so this settles
        // a single scope value rather than reconciling several.
        for pair in pairs.iter().filter(|p| p.attr == hash) {
            // An operand whose type differs from the attribute's declared
            // AttributeDefinitions type rejects. Captured for an N operand
            // against a declared S (eu-west-2 and us-east-1, 2026-08-13);
            // the format generalises to every other operand type, so the
            // BOOL, L, and M letters are uncaptured within the captured
            // format. CreateTable guarantees a declaration exists for every
            // SearchSchema attribute, so the lookup is defensive only.
            if let Some(def) = attr_defs.iter().find(|d| d.attribute_name == hash) {
                let declared = scalar_type_str(&def.attribute_type);
                let actual = pair.value.type_name();
                if actual != declared {
                    return Err(DynoxideError::ValidationException(format!(
                        "Type of '{hash}' attribute in SearchConditionExpression ({actual}) \
                         does not match type in search schema ({declared})"
                    )));
                }
            }
            // An empty-string operand rejects with a "Key:" suffix, distinct
            // from the write path's IndexName/IndexKey form (captured from
            // real DynamoDB (eu-west-2 and us-east-1, 2026-08-13)). An empty
            // binary operand is uncaptured and falls through: the write path
            // never stores an empty binary hash value, so it matches nothing.
            if matches!(&pair.value, AttributeValue::S(s) if s.is_empty()) {
                return Err(DynoxideError::ValidationException(format!(
                    "One or more parameter values are not valid. The AttributeValue for a key \
                     attribute cannot contain an empty string value. Key: {hash}"
                )));
            }
            // The non-key-able arm of `to_key_string` is unreachable after the
            // type check above, which itself only runs when the attribute has a
            // declaration to check against. It stays as defence in depth, and
            // it fails closed: leaving the scope unset would load every
            // partition rather than none, so a value that cannot be a key
            // matches nothing instead of matching everything.
            match pair.value.to_key_string() {
                Some(key) => hash_scope = Some(key),
                None => unscopable_hash = true,
            }
        }
    }

    let candidates: Vec<VectorCandidateRow> = if unscopable_hash {
        Vec::new()
    } else {
        storage
            .query_vector_candidates(
                &request.table_name,
                &request.index_name,
                hash_scope.as_deref(),
            )
            .await?
    };

    // Score the candidates that pass every INLINE_FILTER equality: the pairs
    // compare wire-shaped values against the stored filter_json entries with
    // the condition machinery's equality (numeric for N). An item without
    // the filter attribute is excluded. HASH pairs were fully settled by the
    // scoped load above.
    let filter_pairs: Vec<&EqualityPair> = pairs
        .iter()
        .filter(|p| Some(p.attr.as_str()) != index_hash_attr)
        .collect();
    let mut scored: Vec<(f64, &VectorCandidateRow)> = Vec::with_capacity(candidates.len());
    for row in &candidates {
        let filter_map: HashMap<String, AttributeValue> = if filter_pairs.is_empty() {
            HashMap::new()
        } else {
            serde_json::from_str(&row.filter_json).map_err(|e| {
                DynoxideError::InternalServerError(format!("Bad filter JSON in storage: {e}"))
            })?
        };
        let passes = filter_pairs.iter().all(|pair| {
            filter_map
                .get(&pair.attr)
                .is_some_and(|stored| expressions::condition::values_equal(stored, &pair.value))
        });
        if !passes {
            continue;
        }

        let stored_vec: Vec<f32> = serde_json::from_str(&row.vector_json).map_err(|e| {
            DynoxideError::InternalServerError(format!("Bad vector JSON in storage: {e}"))
        })?;
        scored.push((
            score_candidate(&vix.distance_function, &query_vec, &stored_vec),
            row,
        ));
    }

    // Rank in the metric's direction with a total-order comparison. The sort
    // is stable and the candidate load is ordered by base-table primary key,
    // so equal scores keep that order: the deterministic tie-break.
    if higher_is_better(&vix.distance_function) {
        scored.sort_by(|a, b| b.0.total_cmp(&a.0));
    } else {
        scored.sort_by(|a, b| a.0.total_cmp(&b.0));
    }
    scored.truncate(request.top_k as usize);

    // Build results from the projected item copies. The vector attribute is
    // excluded by default and returned only when a ProjectionExpression
    // requests it AND the index projects it (the copy is then the index's
    // f32 truncation, captured).
    let loop_tracker = expressions::TrackedExpressionAttributes::without_tracking(
        &request.expression_attribute_names,
        &request.expression_attribute_values,
    );
    let no_keys: &[String] = &[];

    // The projected entries are loaded here, for the survivors alone. The
    // candidate scan leaves them behind, so a search over a large index no
    // longer materialises every entry to score it and drop it.
    let survivor_keys: Vec<(String, String)> = scored
        .iter()
        .map(|(_, row)| (row.table_pk.clone(), row.table_sk.clone()))
        .collect();
    let items: HashMap<(String, String), String> = storage
        .vector_items_for_keys(&request.table_name, &request.index_name, &survivor_keys)
        .await?
        .into_iter()
        .map(|(pk, sk, item_json)| ((pk, sk), item_json))
        .collect();

    let mut search_results = Vec::with_capacity(scored.len());
    for (score, row) in scored {
        let key = (row.table_pk.clone(), row.table_sk.clone());
        // A row that went between the scoring pass and this read is dropped
        // rather than raised. The facade holds the backend for the whole call
        // so it cannot happen there, but a caller driving the storage trait
        // directly can delete underneath a search, and answering a concurrent
        // delete with a 500 would be worse than answering with one result
        // fewer.
        let Some(item_json) = items.get(&key) else {
            continue;
        };
        let item: Item = serde_json::from_str(item_json).map_err(|e| {
            DynoxideError::InternalServerError(format!("Bad item JSON in storage: {e}"))
        })?;
        let result_item = if let Some(ref proj) = projection {
            expressions::projection::apply(&item, proj, &loop_tracker, no_keys)
                .map_err(DynoxideError::ValidationException)?
        } else {
            let mut item = item;
            item.remove(&vix.vector_attribute.attribute_name);
            item
        };
        search_results.push(SearchResult {
            item: result_item,
            score,
        });
    }

    // Unused expression attribute names/values reject the way the Query
    // family rejects them (uncaptured for this operation).
    tracker.check_unused()?;

    // Identical under TOTAL and INDEXES, absent under NONE (captured).
    let consumed_capacity =
        crate::types::capacity_wanted(request.return_consumed_capacity.as_deref()).then(|| {
            crate::types::VectorSearchCapacity {
                vector_search_request_bytes: search_scanned_bytes(&candidates),
            }
        });

    Ok(SearchVectorsResponse {
        search_results,
        consumed_capacity,
    })
}
