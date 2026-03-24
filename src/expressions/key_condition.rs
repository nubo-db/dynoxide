//! KeyConditionExpression parsing.
//!
//! KeyConditionExpression supports: `pk = :val [AND sk_condition]`
//! Sort key conditions: `=`, `<`, `<=`, `>`, `>=`, `BETWEEN ... AND ...`, `begins_with(sk, :prefix)`

use crate::expressions::condition::parse_raw_path;
use crate::expressions::tokenizer::{Token, TokenStream, tokenize};
use crate::expressions::{PathElement, TrackedExpressionAttributes};
use crate::types::AttributeValue;

/// Parsed key condition.
#[derive(Debug)]
pub struct KeyCondition {
    /// Partition key attribute name (resolved).
    pub pk_name: String,
    /// Partition key value reference (e.g. `:pk`).
    pub pk_value_ref: String,
    /// Optional sort key condition.
    pub sk_condition: Option<SortKeyCondition>,
}

/// Sort key condition variants.
#[derive(Debug)]
pub enum SortKeyCondition {
    Eq(String, String), // (sk_name, value_ref)
    Lt(String, String),
    Le(String, String),
    Gt(String, String),
    Ge(String, String),
    Between(String, String, String), // (sk_name, lo_ref, hi_ref)
    BeginsWith(String, String),      // (sk_name, prefix_ref)
}

/// Parse a KeyConditionExpression string, tracking attribute name usage.
pub fn parse(expr: &str, tracker: &TrackedExpressionAttributes) -> Result<KeyCondition, String> {
    let tokens = tokenize(expr).map_err(|e| format!("Invalid KeyConditionExpression: {e}"))?;
    let mut stream = TokenStream::new(tokens);

    let cond1 = parse_single_condition(&mut stream, tracker)?;

    let (pk_cond, sk_cond) = if matches!(stream.peek(), Some(Token::And)) {
        stream.next();
        let cond2 = parse_single_condition(&mut stream, tracker)?;
        match (cond1, cond2) {
            (ParsedCond::Eq(n1, v1), c2) => ((n1, v1), Some(c2)),
            (c1, ParsedCond::Eq(n2, v2)) => ((n2, v2), Some(c1)),
            _ => {
                return Err(
                    "Invalid KeyConditionExpression: partition key must use equality".to_string(),
                );
            }
        }
    } else {
        match cond1 {
            ParsedCond::Eq(name, val_ref) => ((name, val_ref), None),
            _ => {
                return Err(
                    "Invalid KeyConditionExpression: partition key must use equality".to_string(),
                );
            }
        }
    };

    if !stream.at_end() {
        return Err(format!(
            "Unexpected token in KeyConditionExpression: {}",
            stream.peek().unwrap()
        ));
    }

    let (pk_name, pk_value_ref) = pk_cond;
    let sk_condition = sk_cond.map(|c| c.into_sk_condition()).transpose()?;

    Ok(KeyCondition {
        pk_name,
        pk_value_ref,
        sk_condition,
    })
}

/// Resolve the actual attribute values from the parsed key condition, tracking usage.
pub fn resolve_values(
    condition: &KeyCondition,
    tracker: &TrackedExpressionAttributes,
) -> Result<ResolvedKeyCondition, String> {
    let pk_val = tracker.resolve_value(&condition.pk_value_ref)?.clone();

    let sk = if let Some(ref sk_cond) = condition.sk_condition {
        Some(resolve_sk_condition(sk_cond, tracker)?)
    } else {
        None
    };

    Ok(ResolvedKeyCondition {
        pk_name: condition.pk_name.clone(),
        pk_value: pk_val,
        sk_condition: sk,
    })
}

/// Resolved key condition with actual values.
#[derive(Debug)]
pub struct ResolvedKeyCondition {
    pub pk_name: String,
    pub pk_value: AttributeValue,
    pub sk_condition: Option<ResolvedSortKeyCondition>,
}

#[derive(Debug)]
pub enum ResolvedSortKeyCondition {
    Eq(String, AttributeValue),
    Lt(String, AttributeValue),
    Le(String, AttributeValue),
    Gt(String, AttributeValue),
    Ge(String, AttributeValue),
    Between(String, AttributeValue, AttributeValue),
    BeginsWith(String, AttributeValue),
}

impl ResolvedSortKeyCondition {
    pub fn sk_name(&self) -> &str {
        match self {
            Self::Eq(n, _)
            | Self::Lt(n, _)
            | Self::Le(n, _)
            | Self::Gt(n, _)
            | Self::Ge(n, _)
            | Self::Between(n, _, _)
            | Self::BeginsWith(n, _) => n,
        }
    }

    /// Convert to SQL WHERE clause components for sk column.
    /// Returns (operator, value_string) pairs.
    /// For BETWEEN, returns two conditions.
    pub fn to_sql_conditions(&self) -> Vec<(String, String)> {
        match self {
            Self::Eq(_, v) => vec![("=".into(), val_to_key_string(v))],
            Self::Lt(_, v) => vec![("<".into(), val_to_key_string(v))],
            Self::Le(_, v) => vec![("<=".into(), val_to_key_string(v))],
            Self::Gt(_, v) => vec![(">".into(), val_to_key_string(v))],
            Self::Ge(_, v) => vec![(">=".into(), val_to_key_string(v))],
            Self::Between(_, lo, hi) => vec![
                (">=".into(), val_to_key_string(lo)),
                ("<=".into(), val_to_key_string(hi)),
            ],
            Self::BeginsWith(_, prefix) => {
                let prefix_str = val_to_key_string(prefix);
                // Escape LIKE wildcards in the prefix value before appending %
                let escaped = prefix_str
                    .replace('\\', "\\\\")
                    .replace('%', "\\%")
                    .replace('_', "\\_");
                vec![("LIKE".into(), format!("{escaped}%"))]
            }
        }
    }
}

fn val_to_key_string(val: &AttributeValue) -> String {
    val.to_key_string().unwrap_or_default()
}

fn resolve_sk_condition(
    cond: &SortKeyCondition,
    tracker: &TrackedExpressionAttributes,
) -> Result<ResolvedSortKeyCondition, String> {
    match cond {
        SortKeyCondition::Eq(sk, vr) => {
            let v = tracker.resolve_value(vr)?.clone();
            Ok(ResolvedSortKeyCondition::Eq(sk.clone(), v))
        }
        SortKeyCondition::Lt(sk, vr) => {
            let v = tracker.resolve_value(vr)?.clone();
            Ok(ResolvedSortKeyCondition::Lt(sk.clone(), v))
        }
        SortKeyCondition::Le(sk, vr) => {
            let v = tracker.resolve_value(vr)?.clone();
            Ok(ResolvedSortKeyCondition::Le(sk.clone(), v))
        }
        SortKeyCondition::Gt(sk, vr) => {
            let v = tracker.resolve_value(vr)?.clone();
            Ok(ResolvedSortKeyCondition::Gt(sk.clone(), v))
        }
        SortKeyCondition::Ge(sk, vr) => {
            let v = tracker.resolve_value(vr)?.clone();
            Ok(ResolvedSortKeyCondition::Ge(sk.clone(), v))
        }
        SortKeyCondition::Between(sk, lo_ref, hi_ref) => {
            let lo = tracker.resolve_value(lo_ref)?.clone();
            let hi = tracker.resolve_value(hi_ref)?.clone();
            // Validate same type
            if std::mem::discriminant(&lo) != std::mem::discriminant(&hi) {
                return Err(format!(
                    "Invalid KeyConditionExpression: The BETWEEN operator requires same data type \
                     for lower and upper bounds; lower bound operand: AttributeValue: {{{}}}, \
                     upper bound operand: AttributeValue: {{{}}}",
                    format_attr_value_short(&lo),
                    format_attr_value_short(&hi)
                ));
            }
            // Validate ordering (upper >= lower)
            if !between_order_valid(&lo, &hi) {
                return Err(format!(
                    "Invalid KeyConditionExpression: The BETWEEN operator requires upper bound \
                     to be greater than or equal to lower bound; lower bound operand: \
                     AttributeValue: {{{}}}, upper bound operand: AttributeValue: {{{}}}",
                    format_attr_value_short(&lo),
                    format_attr_value_short(&hi)
                ));
            }
            Ok(ResolvedSortKeyCondition::Between(sk.clone(), lo, hi))
        }
        SortKeyCondition::BeginsWith(sk, vr) => {
            let v = tracker.resolve_value(vr)?.clone();
            Ok(ResolvedSortKeyCondition::BeginsWith(sk.clone(), v))
        }
    }
}

// ---------------------------------------------------------------------------
// Internal parsing helpers
// ---------------------------------------------------------------------------

#[derive(Debug)]
enum ParsedCond {
    Eq(String, String), // (attr_name, value_ref)
    Lt(String, String),
    Le(String, String),
    Gt(String, String),
    Ge(String, String),
    Between(String, String, String), // (attr_name, lo_ref, hi_ref)
    BeginsWith(String, String),      // (attr_name, prefix_ref)
}

impl ParsedCond {
    fn into_sk_condition(self) -> Result<SortKeyCondition, String> {
        match self {
            ParsedCond::Eq(n, v) => Ok(SortKeyCondition::Eq(n, v)),
            ParsedCond::Lt(n, v) => Ok(SortKeyCondition::Lt(n, v)),
            ParsedCond::Le(n, v) => Ok(SortKeyCondition::Le(n, v)),
            ParsedCond::Gt(n, v) => Ok(SortKeyCondition::Gt(n, v)),
            ParsedCond::Ge(n, v) => Ok(SortKeyCondition::Ge(n, v)),
            ParsedCond::Between(n, lo, hi) => Ok(SortKeyCondition::Between(n, lo, hi)),
            ParsedCond::BeginsWith(n, v) => Ok(SortKeyCondition::BeginsWith(n, v)),
        }
    }
}

fn parse_single_condition(
    stream: &mut TokenStream,
    tracker: &TrackedExpressionAttributes,
) -> Result<ParsedCond, String> {
    // Check for begins_with function
    if let Some(Token::Identifier(name)) = stream.peek() {
        if name.to_lowercase() == "begins_with" {
            stream.next();
            stream.expect(&Token::LParen)?;
            let path = parse_raw_path(stream)?;
            let attr_name = resolve_path_to_name(&path, tracker)?;
            stream.expect(&Token::Comma)?;
            let val_ref = expect_value_ref(stream)?;
            stream.expect(&Token::RParen)?;
            return Ok(ParsedCond::BeginsWith(attr_name, val_ref));
        }
    }

    // attr op :val
    let path = parse_raw_path(stream)?;
    let attr_name = resolve_path_to_name(&path, tracker)?;

    match stream.next() {
        Some(Token::Eq) => {
            let val_ref = expect_value_ref(stream)?;
            Ok(ParsedCond::Eq(attr_name, val_ref))
        }
        Some(Token::Lt) => {
            let val_ref = expect_value_ref(stream)?;
            Ok(ParsedCond::Lt(attr_name, val_ref))
        }
        Some(Token::Le) => {
            let val_ref = expect_value_ref(stream)?;
            Ok(ParsedCond::Le(attr_name, val_ref))
        }
        Some(Token::Gt) => {
            let val_ref = expect_value_ref(stream)?;
            Ok(ParsedCond::Gt(attr_name, val_ref))
        }
        Some(Token::Ge) => {
            let val_ref = expect_value_ref(stream)?;
            Ok(ParsedCond::Ge(attr_name, val_ref))
        }
        Some(Token::Between) => {
            let lo_ref = expect_value_ref(stream)?;
            stream.expect(&Token::And)?;
            let hi_ref = expect_value_ref(stream)?;
            Ok(ParsedCond::Between(attr_name, lo_ref, hi_ref))
        }
        Some(t) => Err(format!(
            "Unexpected operator in KeyConditionExpression: {t}"
        )),
        None => Err("Unexpected end of KeyConditionExpression".to_string()),
    }
}

fn resolve_path_to_name(
    path: &[PathElement],
    tracker: &TrackedExpressionAttributes,
) -> Result<String, String> {
    if path.len() != 1 {
        return Err("KeyConditionExpression only supports top-level attributes".to_string());
    }
    match &path[0] {
        PathElement::Attribute(name) => {
            if name.starts_with('#') {
                tracker.resolve_name(name)
            } else {
                Ok(name.clone())
            }
        }
        PathElement::Index(_) => Err("KeyConditionExpression cannot use index paths".to_string()),
    }
}

/// Format an attribute value for error messages (DynamoDB short format).
fn format_attr_value_short(val: &AttributeValue) -> String {
    match val {
        AttributeValue::S(s) => format!("S:{s}"),
        AttributeValue::N(n) => format!("N:{n}"),
        AttributeValue::B(b) => {
            use base64::Engine;
            let encoded = base64::engine::general_purpose::STANDARD.encode(b);
            format!("B:{encoded}")
        }
        AttributeValue::BOOL(b) => format!("BOOL:{b}"),
        AttributeValue::NULL(_) => "NULL:true".to_string(),
        AttributeValue::SS(set) => format!("SS:{:?}", set),
        AttributeValue::NS(set) => format!("NS:{:?}", set),
        AttributeValue::BS(_) => "BS:[...]".to_string(),
        AttributeValue::L(_) => "L:[...]".to_string(),
        AttributeValue::M(_) => "M:{...}".to_string(),
    }
}

/// Check if BETWEEN bounds are in valid order (lo <= hi).
fn between_order_valid(lo: &AttributeValue, hi: &AttributeValue) -> bool {
    match (lo, hi) {
        (AttributeValue::S(a), AttributeValue::S(b)) => a <= b,
        (AttributeValue::N(a), AttributeValue::N(b)) => {
            let a_f = a.parse::<f64>().unwrap_or(0.0);
            let b_f = b.parse::<f64>().unwrap_or(0.0);
            a_f <= b_f
        }
        (AttributeValue::B(a), AttributeValue::B(b)) => a <= b,
        _ => true,
    }
}

fn expect_value_ref(stream: &mut TokenStream) -> Result<String, String> {
    match stream.next() {
        Some(Token::ValueRef(name)) => Ok(name.clone()),
        Some(t) => Err(format!("Expected value reference (:name), got {t}")),
        None => Err("Expected value reference, got end of expression".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_tracker<'a>(
        names: &'a Option<HashMap<String, String>>,
        values: &'a Option<HashMap<String, AttributeValue>>,
    ) -> TrackedExpressionAttributes<'a> {
        TrackedExpressionAttributes::new(names, values)
    }

    #[test]
    fn test_pk_only() {
        let no_names = None;
        let no_values = None;
        let tracker = make_tracker(&no_names, &no_values);
        let kc = parse("pk = :pk", &tracker).unwrap();
        assert_eq!(kc.pk_name, "pk");
        assert_eq!(kc.pk_value_ref, ":pk");
        assert!(kc.sk_condition.is_none());
    }

    #[test]
    fn test_pk_and_sk_eq() {
        let no_names = None;
        let no_values = None;
        let tracker = make_tracker(&no_names, &no_values);
        let kc = parse("pk = :pk AND sk = :sk", &tracker).unwrap();
        assert_eq!(kc.pk_name, "pk");
        assert!(matches!(kc.sk_condition, Some(SortKeyCondition::Eq(_, _))));
    }

    #[test]
    fn test_pk_and_sk_between() {
        let no_names = None;
        let no_values = None;
        let tracker = make_tracker(&no_names, &no_values);
        let kc = parse("pk = :pk AND sk BETWEEN :lo AND :hi", &tracker).unwrap();
        assert!(matches!(
            kc.sk_condition,
            Some(SortKeyCondition::Between(_, _, _))
        ));
    }

    #[test]
    fn test_pk_and_begins_with() {
        let no_names = None;
        let no_values = None;
        let tracker = make_tracker(&no_names, &no_values);
        let kc = parse("pk = :pk AND begins_with(sk, :prefix)", &tracker).unwrap();
        assert!(matches!(
            kc.sk_condition,
            Some(SortKeyCondition::BeginsWith(_, _))
        ));
    }

    #[test]
    fn test_with_attribute_names() {
        let an = Some(HashMap::from([
            ("#pk".to_string(), "partitionKey".to_string()),
            ("#sk".to_string(), "sortKey".to_string()),
        ]));
        let no_values = None;
        let tracker = make_tracker(&an, &no_values);
        let kc = parse("#pk = :pk AND #sk > :sk", &tracker).unwrap();
        assert_eq!(kc.pk_name, "partitionKey");
        assert!(matches!(kc.sk_condition, Some(SortKeyCondition::Gt(ref n, _)) if n == "sortKey"));
    }

    #[test]
    fn test_resolve_values() {
        let no_names = None;
        let no_values = None;
        let parse_tracker = make_tracker(&no_names, &no_values);
        let kc = parse("pk = :pk AND sk >= :sk", &parse_tracker).unwrap();
        let av = Some(HashMap::from([
            (":pk".to_string(), AttributeValue::S("user#1".into())),
            (":sk".to_string(), AttributeValue::S("2024-01-01".into())),
        ]));
        let resolve_tracker = make_tracker(&no_names, &av);
        let resolved = resolve_values(&kc, &resolve_tracker).unwrap();
        assert_eq!(resolved.pk_value, AttributeValue::S("user#1".into()));
        assert!(matches!(
            resolved.sk_condition,
            Some(ResolvedSortKeyCondition::Ge(_, _))
        ));
    }

    #[test]
    fn test_sk_comparisons() {
        let no_names = None;
        let no_values = None;
        for (op, variant) in [("<", "Lt"), ("<=", "Le"), (">", "Gt"), (">=", "Ge")] {
            let tracker = make_tracker(&no_names, &no_values);
            let kc = parse(&format!("pk = :pk AND sk {op} :sk"), &tracker).unwrap();
            let sk = kc.sk_condition.unwrap();
            let name = match &sk {
                SortKeyCondition::Lt(n, _) => format!("Lt:{n}"),
                SortKeyCondition::Le(n, _) => format!("Le:{n}"),
                SortKeyCondition::Gt(n, _) => format!("Gt:{n}"),
                SortKeyCondition::Ge(n, _) => format!("Ge:{n}"),
                _ => "other".to_string(),
            };
            assert!(name.starts_with(variant), "Expected {variant}, got {name}");
        }
    }
}
