//! ConditionExpression and FilterExpression parsing and evaluation.
//!
//! Both use identical syntax: comparisons, functions, BETWEEN, IN, AND/OR/NOT.

use crate::expressions::tokenizer::{Token, TokenStream, tokenize};
use crate::expressions::{
    PathElement, TrackedExpressionAttributes, resolve_path, resolve_path_elements,
};
use crate::types::AttributeValue;
use std::collections::HashMap;

/// Parsed condition expression AST.
#[derive(Debug, Clone)]
pub enum ConditionExpr {
    /// `path comparator operand`
    Comparison {
        left: Operand,
        op: CompOp,
        right: Operand,
    },
    /// `operand BETWEEN lo AND hi`
    Between {
        operand: Operand,
        lo: Operand,
        hi: Operand,
    },
    /// `operand IN (val1, val2, ...)`
    In {
        operand: Operand,
        values: Vec<Operand>,
    },
    /// `attribute_exists(path)`
    AttributeExists(Vec<PathElement>),
    /// `attribute_not_exists(path)`
    AttributeNotExists(Vec<PathElement>),
    /// `attribute_type(path, :type_val)`
    AttributeType(Vec<PathElement>, Operand),
    /// `begins_with(path, operand)`
    BeginsWith(Operand, Operand),
    /// `contains(path, operand)`
    Contains(Operand, Operand),
    /// `size(path)` — used as operand in comparisons, handled specially
    /// This is actually an operand, not standalone. We handle `size()` as an Operand variant.

    /// `expr AND expr`
    And(Box<ConditionExpr>, Box<ConditionExpr>),
    /// `expr OR expr`
    Or(Box<ConditionExpr>, Box<ConditionExpr>),
    /// `NOT expr`
    Not(Box<ConditionExpr>),
}

/// Comparison operators.
#[derive(Debug, Clone, PartialEq)]
pub enum CompOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

/// An operand in an expression.
#[derive(Debug, Clone)]
pub enum Operand {
    /// A document path (e.g., `attr`, `a.b[0].c`, `#name.sub`)
    Path(Vec<PathElement>),
    /// A value reference (`:val`)
    ValueRef(String),
    /// `size(path)` function used as an operand
    Size(Vec<PathElement>),
}

/// Parse a condition/filter expression string.
///
/// Errors returned are the raw error text without the "Invalid FilterExpression: " etc.
/// prefix — callers must add the appropriate prefix for their expression type.
pub fn parse(expr: &str) -> Result<ConditionExpr, String> {
    let tokens = tokenize(expr).map_err(|e| e.to_string())?;
    let mut stream = TokenStream::new(tokens);
    let result = parse_or(&mut stream)?;
    if !stream.at_end() {
        return Err(format!(
            "Syntax error; token: \"{}\"",
            stream.peek().unwrap()
        ));
    }
    Ok(result)
}

/// Evaluate a condition expression against an item, using a `TrackedExpressionAttributes`
/// to resolve and track which names/values are referenced.
pub fn evaluate(
    expr: &ConditionExpr,
    item: &HashMap<String, AttributeValue>,
    tracker: &TrackedExpressionAttributes,
) -> Result<bool, String> {
    match expr {
        ConditionExpr::Comparison { left, op, right } => {
            let lv = resolve_operand(left, item, tracker)?;
            let rv = resolve_operand(right, item, tracker)?;
            match (lv, rv) {
                (Some(l), Some(r)) => Ok(compare_values(&l, op, &r)),
                _ => Ok(false),
            }
        }

        ConditionExpr::Between { operand, lo, hi } => {
            let val = resolve_operand(operand, item, tracker)?;
            let lo_val = resolve_operand(lo, item, tracker)?;
            let hi_val = resolve_operand(hi, item, tracker)?;
            match (val, lo_val, hi_val) {
                (Some(v), Some(l), Some(h)) => {
                    Ok(compare_values(&v, &CompOp::Ge, &l) && compare_values(&v, &CompOp::Le, &h))
                }
                _ => Ok(false),
            }
        }

        ConditionExpr::In { operand, values } => {
            let val = resolve_operand(operand, item, tracker)?;
            match val {
                Some(v) => {
                    for candidate in values {
                        let cv = resolve_operand(candidate, item, tracker)?;
                        if let Some(c) = cv {
                            if compare_values(&v, &CompOp::Eq, &c) {
                                return Ok(true);
                            }
                        }
                    }
                    Ok(false)
                }
                None => Ok(false),
            }
        }

        ConditionExpr::AttributeExists(path) => {
            let resolved = resolve_path_elements(path, tracker)?;
            Ok(resolve_path(item, &resolved).is_some())
        }

        ConditionExpr::AttributeNotExists(path) => {
            let resolved = resolve_path_elements(path, tracker)?;
            Ok(resolve_path(item, &resolved).is_none())
        }

        ConditionExpr::AttributeType(path, type_operand) => {
            let resolved = resolve_path_elements(path, tracker)?;
            let val = resolve_path(item, &resolved);
            let type_val = resolve_operand(type_operand, item, tracker)?;
            match (val, type_val) {
                (Some(v), Some(AttributeValue::S(type_name))) => Ok(v.type_name() == type_name),
                _ => Ok(false),
            }
        }

        ConditionExpr::BeginsWith(path_op, prefix_op) => {
            let val = resolve_operand(path_op, item, tracker)?;
            let prefix = resolve_operand(prefix_op, item, tracker)?;
            match (val, prefix) {
                (Some(AttributeValue::S(s)), Some(AttributeValue::S(p))) => Ok(s.starts_with(&p)),
                (Some(AttributeValue::B(b)), Some(AttributeValue::B(p))) => Ok(b.starts_with(&p)),
                _ => Ok(false),
            }
        }

        ConditionExpr::Contains(path_op, search_op) => {
            let val = resolve_operand(path_op, item, tracker)?;
            let search = resolve_operand(search_op, item, tracker)?;
            match (val, search) {
                (Some(AttributeValue::S(s)), Some(AttributeValue::S(sub))) => Ok(s.contains(&sub)),
                (Some(AttributeValue::B(b)), Some(AttributeValue::B(sub))) => {
                    Ok(sub.is_empty() || b.windows(sub.len()).any(|w| w == sub.as_slice()))
                }
                (Some(AttributeValue::SS(set)), Some(AttributeValue::S(elem))) => {
                    Ok(set.contains(&elem))
                }
                (Some(AttributeValue::NS(set)), Some(AttributeValue::N(elem))) => {
                    Ok(set.contains(&elem))
                }
                (Some(AttributeValue::BS(set)), Some(AttributeValue::B(elem))) => {
                    Ok(set.contains(&elem))
                }
                (Some(AttributeValue::L(list)), Some(search_val)) => Ok(list
                    .iter()
                    .any(|v| compare_values(v, &CompOp::Eq, &search_val))),
                _ => Ok(false),
            }
        }

        ConditionExpr::And(left, right) => {
            if !evaluate(left, item, tracker)? {
                return Ok(false); // short-circuit
            }
            evaluate(right, item, tracker)
        }

        ConditionExpr::Or(left, right) => {
            if evaluate(left, item, tracker)? {
                return Ok(true); // short-circuit
            }
            evaluate(right, item, tracker)
        }

        ConditionExpr::Not(inner) => {
            let v = evaluate(inner, item, tracker)?;
            Ok(!v)
        }
    }
}

// ---------------------------------------------------------------------------
// Parser (recursive descent with precedence: OR < AND < NOT < comparison)
// ---------------------------------------------------------------------------

fn parse_or(stream: &mut TokenStream) -> Result<ConditionExpr, String> {
    let mut left = parse_and(stream)?;
    while matches!(stream.peek(), Some(Token::Or)) {
        stream.next();
        let right = parse_and(stream)?;
        left = ConditionExpr::Or(Box::new(left), Box::new(right));
    }
    Ok(left)
}

fn parse_and(stream: &mut TokenStream) -> Result<ConditionExpr, String> {
    let mut left = parse_not(stream)?;
    while matches!(stream.peek(), Some(Token::And)) {
        stream.next();
        let right = parse_not(stream)?;
        left = ConditionExpr::And(Box::new(left), Box::new(right));
    }
    Ok(left)
}

fn parse_not(stream: &mut TokenStream) -> Result<ConditionExpr, String> {
    if matches!(stream.peek(), Some(Token::Not)) {
        stream.next();
        let inner = parse_not(stream)?;
        return Ok(ConditionExpr::Not(Box::new(inner)));
    }
    parse_primary(stream)
}

fn parse_primary(stream: &mut TokenStream) -> Result<ConditionExpr, String> {
    // Parenthesized expression
    if matches!(stream.peek(), Some(Token::LParen)) {
        stream.next();
        let expr = parse_or(stream)?;
        stream.expect(&Token::RParen)?;
        return Ok(expr);
    }

    // Check for function calls
    if let Some(Token::Identifier(name)) = stream.peek() {
        let name_owned = name.clone();
        let func_name = name_owned.to_lowercase();

        // Check if next token after identifier is '(' — if so, it's a function call
        let is_function_call = {
            let saved = stream.pos();
            stream.next(); // consume identifier
            let is_lparen = matches!(stream.peek(), Some(Token::LParen));
            stream.set_pos(saved); // restore position
            is_lparen
        };

        if is_function_call {
            // Known condition-level functions (return bool, valid as primary expression)
            match func_name.as_str() {
                "attribute_exists" => {
                    stream.next();
                    stream.expect(&Token::LParen)?;
                    let path = parse_raw_path(stream)?;
                    stream.expect(&Token::RParen)?;
                    return Ok(ConditionExpr::AttributeExists(path));
                }
                "attribute_not_exists" => {
                    stream.next();
                    stream.expect(&Token::LParen)?;
                    let path = parse_raw_path(stream)?;
                    stream.expect(&Token::RParen)?;
                    return Ok(ConditionExpr::AttributeNotExists(path));
                }
                "attribute_type" => {
                    stream.next();
                    stream.expect(&Token::LParen)?;
                    let path = parse_raw_path(stream)?;
                    stream.expect(&Token::Comma)?;
                    let type_val = parse_operand(stream)?;
                    stream.expect(&Token::RParen)?;
                    return Ok(ConditionExpr::AttributeType(path, type_val));
                }
                "begins_with" => {
                    stream.next();
                    stream.expect(&Token::LParen)?;
                    let path_op = parse_operand(stream)?;
                    stream.expect(&Token::Comma)?;
                    let prefix_op = parse_operand(stream)?;
                    stream.expect(&Token::RParen)?;
                    return Ok(ConditionExpr::BeginsWith(path_op, prefix_op));
                }
                "contains" => {
                    stream.next();
                    stream.expect(&Token::LParen)?;
                    let path_op = parse_operand(stream)?;
                    stream.expect(&Token::Comma)?;
                    let search_op = parse_operand(stream)?;
                    stream.expect(&Token::RParen)?;
                    return Ok(ConditionExpr::Contains(path_op, search_op));
                }
                "size" => {
                    // size() is an operand-level function, not a condition-level function.
                    // Fall through to comparison parsing where parse_operand handles it.
                }
                _ => {
                    // Unknown function name
                    return Err(format!("Invalid function name; function: {}", name_owned));
                }
            }
        } else {
            // Not a function call — fall through to comparison parsing.
            // Reserved keyword check happens in parse_raw_path.
        }
    }

    // Comparison: operand op operand, or operand BETWEEN, or operand IN
    let left = parse_operand(stream)?;

    match stream.peek() {
        Some(Token::Eq) => {
            stream.next();
            let right = parse_operand(stream)?;
            Ok(ConditionExpr::Comparison {
                left,
                op: CompOp::Eq,
                right,
            })
        }
        Some(Token::Ne) => {
            stream.next();
            let right = parse_operand(stream)?;
            Ok(ConditionExpr::Comparison {
                left,
                op: CompOp::Ne,
                right,
            })
        }
        Some(Token::Lt) => {
            stream.next();
            let right = parse_operand(stream)?;
            Ok(ConditionExpr::Comparison {
                left,
                op: CompOp::Lt,
                right,
            })
        }
        Some(Token::Le) => {
            stream.next();
            let right = parse_operand(stream)?;
            Ok(ConditionExpr::Comparison {
                left,
                op: CompOp::Le,
                right,
            })
        }
        Some(Token::Gt) => {
            stream.next();
            let right = parse_operand(stream)?;
            Ok(ConditionExpr::Comparison {
                left,
                op: CompOp::Gt,
                right,
            })
        }
        Some(Token::Ge) => {
            stream.next();
            let right = parse_operand(stream)?;
            Ok(ConditionExpr::Comparison {
                left,
                op: CompOp::Ge,
                right,
            })
        }
        Some(Token::Between) => {
            stream.next();
            let lo = parse_operand(stream)?;
            stream.expect(&Token::And)?;
            let hi = parse_operand(stream)?;
            Ok(ConditionExpr::Between {
                operand: left,
                lo,
                hi,
            })
        }
        Some(Token::In) => {
            stream.next();
            stream.expect(&Token::LParen)?;
            let mut values = vec![parse_operand(stream)?];
            while matches!(stream.peek(), Some(Token::Comma)) {
                stream.next();
                values.push(parse_operand(stream)?);
            }
            stream.expect(&Token::RParen)?;
            Ok(ConditionExpr::In {
                operand: left,
                values,
            })
        }
        _ => Err("Expected comparison operator, BETWEEN, or IN".to_string()),
    }
}

/// Parse an operand (path, value ref, or size function).
fn parse_operand(stream: &mut TokenStream) -> Result<Operand, String> {
    // Check for size() function
    if let Some(Token::Identifier(name)) = stream.peek() {
        if name.to_lowercase() == "size" {
            stream.next();
            stream.expect(&Token::LParen)?;
            let path = parse_raw_path(stream)?;
            stream.expect(&Token::RParen)?;
            return Ok(Operand::Size(path));
        }
    }

    match stream.peek() {
        Some(Token::ValueRef(_)) => {
            if let Some(Token::ValueRef(name)) = stream.next().cloned() {
                Ok(Operand::ValueRef(name))
            } else {
                unreachable!()
            }
        }
        Some(Token::Identifier(_)) | Some(Token::NameRef(_)) => {
            let path = parse_raw_path(stream)?;
            Ok(Operand::Path(path))
        }
        Some(t) => Err(format!("Expected operand, got {t}")),
        None => Err("Expected operand, got end of expression".to_string()),
    }
}

/// Parse a raw document path (not resolving #names yet).
/// Path format: `ident(.ident | [n])*`
pub fn parse_raw_path(stream: &mut TokenStream) -> Result<Vec<PathElement>, String> {
    let first = match stream.next() {
        Some(Token::Identifier(name)) => {
            if super::reserved::is_reserved_keyword(name) {
                return Err(format!(
                    "Attribute name is a reserved keyword; reserved keyword: {name}"
                ));
            }
            PathElement::Attribute(name.clone())
        }
        Some(Token::NameRef(name)) => PathElement::Attribute(name.clone()),
        Some(t) => return Err(format!("Expected attribute name, got {t}")),
        None => return Err("Expected attribute name, got end of expression".to_string()),
    };

    let mut path = vec![first];

    loop {
        match stream.peek() {
            Some(Token::Dot) => {
                stream.next();
                match stream.next() {
                    Some(Token::Identifier(name)) => {
                        if super::reserved::is_reserved_keyword(name) {
                            return Err(format!(
                                "Attribute name is a reserved keyword; reserved keyword: {name}"
                            ));
                        }
                        path.push(PathElement::Attribute(name.clone()));
                    }
                    Some(Token::NameRef(name)) => {
                        path.push(PathElement::Attribute(name.clone()));
                    }
                    Some(t) => return Err(format!("Expected attribute name after '.', got {t}")),
                    None => return Err("Expected attribute name after '.'".to_string()),
                }
            }
            Some(Token::LBracket) => {
                stream.next();
                match stream.next() {
                    Some(Token::Number(n)) => {
                        let idx: usize = n.parse().map_err(|_| format!("Invalid index: {n}"))?;
                        path.push(PathElement::Index(idx));
                    }
                    Some(t) => return Err(format!("Expected number in brackets, got {t}")),
                    None => return Err("Expected number in brackets".to_string()),
                }
                stream.expect(&Token::RBracket)?;
            }
            _ => break,
        }
    }

    Ok(path)
}

// ---------------------------------------------------------------------------
// Value resolution
// ---------------------------------------------------------------------------

/// Resolve an operand to an AttributeValue, tracking usage.
fn resolve_operand(
    operand: &Operand,
    item: &HashMap<String, AttributeValue>,
    tracker: &TrackedExpressionAttributes,
) -> Result<Option<AttributeValue>, String> {
    match operand {
        Operand::Path(path) => {
            let resolved = resolve_path_elements(path, tracker)?;
            Ok(resolve_path(item, &resolved))
        }
        Operand::ValueRef(name) => {
            let val = tracker.resolve_value(name)?;
            Ok(Some(val.clone()))
        }
        Operand::Size(path) => {
            let resolved = resolve_path_elements(path, tracker)?;
            match resolve_path(item, &resolved) {
                Some(val) => {
                    let size = match &val {
                        AttributeValue::S(s) => s.len(),
                        AttributeValue::B(b) => b.len(),
                        AttributeValue::SS(set) => set.len(),
                        AttributeValue::NS(set) => set.len(),
                        AttributeValue::BS(set) => set.len(),
                        AttributeValue::L(list) => list.len(),
                        AttributeValue::M(map) => map.len(),
                        // N, BOOL, NULL do not support size() — return None
                        // so the comparison evaluates to false (no match).
                        _ => return Ok(None),
                    };
                    Ok(Some(AttributeValue::N(size.to_string())))
                }
                None => Ok(None),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Comparison logic
// ---------------------------------------------------------------------------

/// Compare two AttributeValues using a comparison operator.
fn compare_values(left: &AttributeValue, op: &CompOp, right: &AttributeValue) -> bool {
    match (left, right) {
        // String comparisons
        (AttributeValue::S(a), AttributeValue::S(b)) => compare_ord(a, b, op),

        // Number comparisons — f64 fast-path for common cases, BigDecimal for edge cases
        (AttributeValue::N(a), AttributeValue::N(b)) => {
            // Fast path: f64 is exact for ≤15 significant digits with no scientific notation
            if can_use_f64(a) && can_use_f64(b) {
                if let (Ok(fa), Ok(fb)) = (a.parse::<f64>(), b.parse::<f64>()) {
                    if fa.is_finite() && fb.is_finite() {
                        return compare_ord(&fa, &fb, op);
                    }
                }
            }
            // Slow path: BigDecimal for 38-digit precision edge cases
            use bigdecimal::BigDecimal;
            use std::str::FromStr;
            match (BigDecimal::from_str(a), BigDecimal::from_str(b)) {
                (Ok(da), Ok(db)) => compare_ord(&da, &db, op),
                _ => false,
            }
        }

        // Binary comparisons
        (AttributeValue::B(a), AttributeValue::B(b)) => compare_ord(a, b, op),

        // Bool — only equality
        (AttributeValue::BOOL(a), AttributeValue::BOOL(b)) => match op {
            CompOp::Eq => a == b,
            CompOp::Ne => a != b,
            _ => false,
        },

        // Null — only equality
        (AttributeValue::NULL(a), AttributeValue::NULL(b)) => match op {
            CompOp::Eq => a == b,
            CompOp::Ne => a != b,
            _ => false,
        },

        // String Set — set equality (order-independent)
        (AttributeValue::SS(a), AttributeValue::SS(b)) => {
            let mut sa = a.clone();
            let mut sb = b.clone();
            sa.sort();
            sb.sort();
            match op {
                CompOp::Eq => sa == sb,
                CompOp::Ne => sa != sb,
                _ => false,
            }
        }

        // Number Set — set equality (order-independent)
        (AttributeValue::NS(a), AttributeValue::NS(b)) => {
            if a.len() != b.len() {
                return matches!(op, CompOp::Ne);
            }
            let mut fa: Vec<f64> = match a.iter().map(|n| n.parse::<f64>()).collect() {
                Ok(v) => v,
                Err(_) => return false,
            };
            let mut fb: Vec<f64> = match b.iter().map(|n| n.parse::<f64>()).collect() {
                Ok(v) => v,
                Err(_) => return false,
            };
            fa.sort_by(|x, y| x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal));
            fb.sort_by(|x, y| x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal));
            match op {
                CompOp::Eq => fa == fb,
                CompOp::Ne => fa != fb,
                _ => false,
            }
        }

        // Binary Set — set equality (order-independent)
        (AttributeValue::BS(a), AttributeValue::BS(b)) => {
            let mut sa = a.clone();
            let mut sb = b.clone();
            sa.sort();
            sb.sort();
            match op {
                CompOp::Eq => sa == sb,
                CompOp::Ne => sa != sb,
                _ => false,
            }
        }

        // Different types — only <> is true
        _ => matches!(op, CompOp::Ne),
    }
}

fn compare_ord<T: PartialOrd>(a: &T, b: &T, op: &CompOp) -> bool {
    match op {
        CompOp::Eq => a == b,
        CompOp::Ne => a != b,
        CompOp::Lt => a < b,
        CompOp::Le => a <= b,
        CompOp::Gt => a > b,
        CompOp::Ge => a >= b,
    }
}

/// Walk a condition expression and track all attribute name and value references
/// without evaluating. Used for pre-validation to detect unused names/values.
pub fn track_references(
    expr: &ConditionExpr,
    tracker: &TrackedExpressionAttributes,
) -> Result<(), String> {
    match expr {
        ConditionExpr::Comparison { left, op: _, right } => {
            track_operand_refs(left, tracker)?;
            track_operand_refs(right, tracker)
        }
        ConditionExpr::Between { operand, lo, hi } => {
            track_operand_refs(operand, tracker)?;
            track_operand_refs(lo, tracker)?;
            track_operand_refs(hi, tracker)
        }
        ConditionExpr::In { operand, values } => {
            track_operand_refs(operand, tracker)?;
            for v in values {
                track_operand_refs(v, tracker)?;
            }
            Ok(())
        }
        ConditionExpr::AttributeExists(path) | ConditionExpr::AttributeNotExists(path) => {
            track_cond_path_refs(path, tracker)
        }
        ConditionExpr::AttributeType(path, type_op) => {
            track_cond_path_refs(path, tracker)?;
            track_operand_refs(type_op, tracker)
        }
        ConditionExpr::BeginsWith(a, b) | ConditionExpr::Contains(a, b) => {
            track_operand_refs(a, tracker)?;
            track_operand_refs(b, tracker)
        }
        ConditionExpr::And(left, right) | ConditionExpr::Or(left, right) => {
            track_references(left, tracker)?;
            track_references(right, tracker)
        }
        ConditionExpr::Not(inner) => track_references(inner, tracker),
    }
}

fn track_operand_refs(
    operand: &Operand,
    tracker: &TrackedExpressionAttributes,
) -> Result<(), String> {
    match operand {
        Operand::Path(path) => track_cond_path_refs(path, tracker),
        Operand::ValueRef(name) => {
            tracker.resolve_value(name)?;
            Ok(())
        }
        Operand::Size(path) => track_cond_path_refs(path, tracker),
    }
}

fn track_cond_path_refs(
    path: &[PathElement],
    tracker: &TrackedExpressionAttributes,
) -> Result<(), String> {
    for elem in path {
        if let PathElement::Attribute(name) = elem {
            if name.starts_with('#') {
                tracker.resolve_name(name)?;
            }
        }
    }
    Ok(())
}

/// Statically validate a condition expression against ExpressionAttributeValues.
///
/// Checks BETWEEN operands for:
/// - Same data type (lower and upper bound must have the same type)
/// - Correct ordering (lower bound must not be greater than upper bound)
///
/// This validation happens before table lookup, matching DynamoDB behaviour.
pub fn validate_static(
    expr: &ConditionExpr,
    values: &Option<HashMap<String, AttributeValue>>,
) -> Result<(), String> {
    match expr {
        ConditionExpr::Between { operand: _, lo, hi } => {
            // Only validate when both bounds are value refs
            if let (Operand::ValueRef(lo_name), Operand::ValueRef(hi_name)) = (lo, hi) {
                if let Some(vals) = values {
                    let lo_val = vals.get(lo_name.as_str());
                    let hi_val = vals.get(hi_name.as_str());
                    if let (Some(lo_v), Some(hi_v)) = (lo_val, hi_val) {
                        // Check same data type
                        if std::mem::discriminant(lo_v) != std::mem::discriminant(hi_v) {
                            return Err(format!(
                                "Invalid ConditionExpression: The BETWEEN operator requires same data type for lower and upper bounds; \
                                 lower bound operand: AttributeValue: {{{}}}, upper bound operand: AttributeValue: {{{}}}",
                                format_av_for_error(lo_v),
                                format_av_for_error(hi_v),
                            ));
                        }
                        // Check ordering
                        if compare_values(lo_v, &CompOp::Gt, hi_v) {
                            return Err(format!(
                                "Invalid ConditionExpression: The BETWEEN operator requires upper bound to be greater than or equal to lower bound; \
                                 lower bound operand: AttributeValue: {{{}}}, upper bound operand: AttributeValue: {{{}}}",
                                format_av_for_error(lo_v),
                                format_av_for_error(hi_v),
                            ));
                        }
                    }
                }
            }
            Ok(())
        }
        ConditionExpr::And(left, right) | ConditionExpr::Or(left, right) => {
            validate_static(left, values)?;
            validate_static(right, values)
        }
        ConditionExpr::Not(inner) => validate_static(inner, values),
        _ => Ok(()),
    }
}

/// Format an AttributeValue for error messages (e.g., "S:hello", "N:42").
fn format_av_for_error(av: &AttributeValue) -> String {
    match av {
        AttributeValue::S(s) => format!("S:{s}"),
        AttributeValue::N(n) => format!("N:{n}"),
        AttributeValue::B(b) => {
            use base64::Engine;
            format!("B:{}", base64::engine::general_purpose::STANDARD.encode(b))
        }
        AttributeValue::BOOL(b) => format!("BOOL:{b}"),
        AttributeValue::NULL(_) => "NULL:true".to_string(),
        AttributeValue::SS(set) => format!("SS:{set:?}"),
        AttributeValue::NS(set) => format!("NS:{set:?}"),
        AttributeValue::BS(_) => "BS:[...]".to_string(),
        AttributeValue::L(_) => "L:[...]".to_string(),
        AttributeValue::M(_) => "M:{...}".to_string(),
    }
}

/// Check for non-scalar key access in an expression.
///
/// DynamoDB rejects expressions that use `.` (map lookup) or `[]` (list index) on
/// key attributes. Returns the offending key attribute name if found.
///
/// `key_attrs` contains the effective key attribute names.
/// `index_key_attrs` contains secondary index key attribute names (for "IndexKey:" prefix).
pub fn check_non_scalar_key_access(
    expr: &ConditionExpr,
    attr_names: &Option<HashMap<String, String>>,
    key_attrs: &[String],
    index_key_attrs: &[String],
) -> Option<(String, bool)> {
    // Returns (attr_name, is_index_key)
    let mut result = None;
    check_non_scalar_key_access_inner(expr, attr_names, key_attrs, index_key_attrs, &mut result);
    result
}

fn check_non_scalar_key_access_inner(
    expr: &ConditionExpr,
    attr_names: &Option<HashMap<String, String>>,
    key_attrs: &[String],
    index_key_attrs: &[String],
    result: &mut Option<(String, bool)>,
) {
    if result.is_some() {
        return;
    }
    match expr {
        ConditionExpr::Comparison { left, right, .. } => {
            check_operand_non_scalar(left, attr_names, key_attrs, index_key_attrs, result);
            check_operand_non_scalar(right, attr_names, key_attrs, index_key_attrs, result);
        }
        ConditionExpr::Between { operand, lo, hi } => {
            check_operand_non_scalar(operand, attr_names, key_attrs, index_key_attrs, result);
            check_operand_non_scalar(lo, attr_names, key_attrs, index_key_attrs, result);
            check_operand_non_scalar(hi, attr_names, key_attrs, index_key_attrs, result);
        }
        ConditionExpr::In { operand, values } => {
            check_operand_non_scalar(operand, attr_names, key_attrs, index_key_attrs, result);
            for v in values {
                check_operand_non_scalar(v, attr_names, key_attrs, index_key_attrs, result);
            }
        }
        ConditionExpr::AttributeExists(path) | ConditionExpr::AttributeNotExists(path) => {
            check_path_non_scalar(path, attr_names, key_attrs, index_key_attrs, result);
        }
        ConditionExpr::AttributeType(path, _) => {
            check_path_non_scalar(path, attr_names, key_attrs, index_key_attrs, result);
        }
        ConditionExpr::BeginsWith(a, b) | ConditionExpr::Contains(a, b) => {
            check_operand_non_scalar(a, attr_names, key_attrs, index_key_attrs, result);
            check_operand_non_scalar(b, attr_names, key_attrs, index_key_attrs, result);
        }
        ConditionExpr::And(a, b) | ConditionExpr::Or(a, b) => {
            check_non_scalar_key_access_inner(a, attr_names, key_attrs, index_key_attrs, result);
            check_non_scalar_key_access_inner(b, attr_names, key_attrs, index_key_attrs, result);
        }
        ConditionExpr::Not(inner) => {
            check_non_scalar_key_access_inner(
                inner,
                attr_names,
                key_attrs,
                index_key_attrs,
                result,
            );
        }
    }
}

fn check_operand_non_scalar(
    operand: &Operand,
    attr_names: &Option<HashMap<String, String>>,
    key_attrs: &[String],
    index_key_attrs: &[String],
    result: &mut Option<(String, bool)>,
) {
    if result.is_some() {
        return;
    }
    match operand {
        Operand::Path(path) | Operand::Size(path) => {
            check_path_non_scalar(path, attr_names, key_attrs, index_key_attrs, result);
        }
        Operand::ValueRef(_) => {}
    }
}

fn check_path_non_scalar(
    path: &[PathElement],
    attr_names: &Option<HashMap<String, String>>,
    key_attrs: &[String],
    index_key_attrs: &[String],
    result: &mut Option<(String, bool)>,
) {
    if result.is_some() || path.len() <= 1 {
        return; // single-element paths are fine (scalar access)
    }
    if let Some(name) = resolve_top_level_path(path, attr_names) {
        if key_attrs.contains(&name) {
            *result = Some((name, false));
        } else if index_key_attrs.contains(&name) {
            *result = Some((name, true));
        }
    }
}

/// Extract the top-level attribute names referenced in a condition expression.
///
/// Resolves `#name` references using `expression_attribute_names`.
/// For paths like `a.b.c` or `a[1]`, only the root attribute `a` is returned.
/// This is used for checking that FilterExpression doesn't reference key attributes.
pub fn extract_top_level_attributes(
    expr: &ConditionExpr,
    attr_names: &Option<HashMap<String, String>>,
) -> Vec<String> {
    let mut attrs = Vec::new();
    collect_top_level_attrs(expr, attr_names, &mut attrs);
    attrs.sort();
    attrs.dedup();
    attrs
}

fn collect_top_level_attrs(
    expr: &ConditionExpr,
    attr_names: &Option<HashMap<String, String>>,
    out: &mut Vec<String>,
) {
    match expr {
        ConditionExpr::Comparison { left, right, .. } => {
            collect_operand_top_attr(left, attr_names, out);
            collect_operand_top_attr(right, attr_names, out);
        }
        ConditionExpr::Between { operand, lo, hi } => {
            collect_operand_top_attr(operand, attr_names, out);
            collect_operand_top_attr(lo, attr_names, out);
            collect_operand_top_attr(hi, attr_names, out);
        }
        ConditionExpr::In { operand, values } => {
            collect_operand_top_attr(operand, attr_names, out);
            for v in values {
                collect_operand_top_attr(v, attr_names, out);
            }
        }
        ConditionExpr::AttributeExists(path) | ConditionExpr::AttributeNotExists(path) => {
            if let Some(name) = resolve_top_level_path(path, attr_names) {
                out.push(name);
            }
        }
        ConditionExpr::AttributeType(path, _) => {
            if let Some(name) = resolve_top_level_path(path, attr_names) {
                out.push(name);
            }
        }
        ConditionExpr::BeginsWith(a, b) | ConditionExpr::Contains(a, b) => {
            collect_operand_top_attr(a, attr_names, out);
            collect_operand_top_attr(b, attr_names, out);
        }
        ConditionExpr::And(a, b) | ConditionExpr::Or(a, b) => {
            collect_top_level_attrs(a, attr_names, out);
            collect_top_level_attrs(b, attr_names, out);
        }
        ConditionExpr::Not(inner) => {
            collect_top_level_attrs(inner, attr_names, out);
        }
    }
}

fn collect_operand_top_attr(
    operand: &Operand,
    attr_names: &Option<HashMap<String, String>>,
    out: &mut Vec<String>,
) {
    match operand {
        Operand::Path(path) => {
            if let Some(name) = resolve_top_level_path(path, attr_names) {
                out.push(name);
            }
        }
        Operand::Size(path) => {
            if let Some(name) = resolve_top_level_path(path, attr_names) {
                out.push(name);
            }
        }
        Operand::ValueRef(_) => {}
    }
}

fn resolve_top_level_path(
    path: &[PathElement],
    attr_names: &Option<HashMap<String, String>>,
) -> Option<String> {
    match path.first() {
        Some(PathElement::Attribute(name)) => {
            if name.starts_with('#') {
                attr_names
                    .as_ref()
                    .and_then(|m| m.get(name.as_str()))
                    .cloned()
            } else {
                Some(name.clone())
            }
        }
        _ => None,
    }
}

/// Validate that all `#name` references in a condition expression are defined
/// in the provided `ExpressionAttributeNames` map. Returns `Err` with the
/// DynamoDB-style error message for the first undefined reference found.
pub fn validate_name_refs(
    expr: &ConditionExpr,
    attr_names: &Option<HashMap<String, String>>,
) -> Result<(), String> {
    let mut undefined = Vec::new();
    collect_undefined_name_refs(expr, attr_names, &mut undefined);
    if let Some(name) = undefined.first() {
        Err(format!(
            "An expression attribute name used in the document path is not defined; attribute name: {}",
            name
        ))
    } else {
        Ok(())
    }
}

fn collect_undefined_name_refs(
    expr: &ConditionExpr,
    attr_names: &Option<HashMap<String, String>>,
    out: &mut Vec<String>,
) {
    match expr {
        ConditionExpr::Comparison { left, right, .. } => {
            collect_operand_undefined_refs(left, attr_names, out);
            collect_operand_undefined_refs(right, attr_names, out);
        }
        ConditionExpr::Between { operand, lo, hi } => {
            collect_operand_undefined_refs(operand, attr_names, out);
            collect_operand_undefined_refs(lo, attr_names, out);
            collect_operand_undefined_refs(hi, attr_names, out);
        }
        ConditionExpr::In { operand, values } => {
            collect_operand_undefined_refs(operand, attr_names, out);
            for v in values {
                collect_operand_undefined_refs(v, attr_names, out);
            }
        }
        ConditionExpr::AttributeExists(path) | ConditionExpr::AttributeNotExists(path) => {
            collect_path_undefined_refs(path, attr_names, out);
        }
        ConditionExpr::AttributeType(path, operand) => {
            collect_path_undefined_refs(path, attr_names, out);
            collect_operand_undefined_refs(operand, attr_names, out);
        }
        ConditionExpr::BeginsWith(a, b) | ConditionExpr::Contains(a, b) => {
            collect_operand_undefined_refs(a, attr_names, out);
            collect_operand_undefined_refs(b, attr_names, out);
        }
        ConditionExpr::And(a, b) | ConditionExpr::Or(a, b) => {
            collect_undefined_name_refs(a, attr_names, out);
            collect_undefined_name_refs(b, attr_names, out);
        }
        ConditionExpr::Not(inner) => {
            collect_undefined_name_refs(inner, attr_names, out);
        }
    }
}

fn collect_operand_undefined_refs(
    operand: &Operand,
    attr_names: &Option<HashMap<String, String>>,
    out: &mut Vec<String>,
) {
    match operand {
        Operand::Path(path) | Operand::Size(path) => {
            collect_path_undefined_refs(path, attr_names, out);
        }
        Operand::ValueRef(_) => {}
    }
}

fn collect_path_undefined_refs(
    path: &[PathElement],
    attr_names: &Option<HashMap<String, String>>,
    out: &mut Vec<String>,
) {
    for elem in path {
        if let PathElement::Attribute(name) = elem {
            if name.starts_with('#') {
                let defined = attr_names
                    .as_ref()
                    .is_some_and(|m| m.contains_key(name.as_str()));
                if !defined && !out.contains(name) {
                    out.push(name.clone());
                }
            }
        }
    }
}

/// Returns true if a DynamoDB number string can be safely compared using f64.
/// f64 has 15-17 significant decimal digits of precision; ≤15 digit strings
/// are always exactly representable so no precision is lost.
fn can_use_f64(s: &str) -> bool {
    // Reject scientific notation — uncommon and complicates digit counting
    if s.contains('E') || s.contains('e') {
        return false;
    }
    // Count digit characters (skip sign and decimal point).
    // If total digits ≤ 15, the number fits exactly in f64.
    let digit_count = s.bytes().filter(|b| b.is_ascii_digit()).count();
    digit_count <= 15
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::evaluate_without_tracking;

    fn make_item(pairs: &[(&str, AttributeValue)]) -> HashMap<String, AttributeValue> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    fn vals(pairs: &[(&str, AttributeValue)]) -> Option<HashMap<String, AttributeValue>> {
        Some(make_item(pairs))
    }

    fn names(pairs: &[(&str, &str)]) -> Option<HashMap<String, String>> {
        Some(
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        )
    }

    #[test]
    fn test_simple_equality() {
        let expr = parse("pk = :val").unwrap();
        let item = make_item(&[("pk", AttributeValue::S("hello".into()))]);
        let av = vals(&[(":val", AttributeValue::S("hello".into()))]);
        assert!(evaluate_without_tracking(&expr, &item, &None, &av).unwrap());
    }

    #[test]
    fn test_inequality() {
        let expr = parse("pk <> :val").unwrap();
        let item = make_item(&[("pk", AttributeValue::S("hello".into()))]);
        let av = vals(&[(":val", AttributeValue::S("world".into()))]);
        assert!(evaluate_without_tracking(&expr, &item, &None, &av).unwrap());
    }

    #[test]
    fn test_numeric_comparison() {
        let expr = parse("price > :min").unwrap();
        let item = make_item(&[("price", AttributeValue::N("42".into()))]);
        let av = vals(&[(":min", AttributeValue::N("10".into()))]);
        assert!(evaluate_without_tracking(&expr, &item, &None, &av).unwrap());
    }

    #[test]
    fn test_between() {
        let expr = parse("age BETWEEN :lo AND :hi").unwrap();
        let item = make_item(&[("age", AttributeValue::N("25".into()))]);
        let av = vals(&[
            (":lo", AttributeValue::N("18".into())),
            (":hi", AttributeValue::N("65".into())),
        ]);
        assert!(evaluate_without_tracking(&expr, &item, &None, &av).unwrap());
    }

    #[test]
    fn test_in_operator() {
        let expr = parse("state_val IN (:s1, :s2, :s3)").unwrap();
        let item = make_item(&[("state_val", AttributeValue::S("active".into()))]);
        let av = vals(&[
            (":s1", AttributeValue::S("active".into())),
            (":s2", AttributeValue::S("pending".into())),
            (":s3", AttributeValue::S("closed".into())),
        ]);
        assert!(evaluate_without_tracking(&expr, &item, &None, &av).unwrap());
    }

    #[test]
    fn test_attribute_exists() {
        let expr = parse("attribute_exists(email)").unwrap();
        let item = make_item(&[("email", AttributeValue::S("a@b.com".into()))]);
        assert!(evaluate_without_tracking(&expr, &item, &None, &None).unwrap());

        let empty_item: HashMap<String, AttributeValue> = HashMap::new();
        assert!(!evaluate_without_tracking(&expr, &empty_item, &None, &None).unwrap());
    }

    #[test]
    fn test_attribute_not_exists() {
        let expr = parse("attribute_not_exists(email)").unwrap();
        let item: HashMap<String, AttributeValue> = HashMap::new();
        assert!(evaluate_without_tracking(&expr, &item, &None, &None).unwrap());
    }

    #[test]
    fn test_begins_with() {
        let expr = parse("begins_with(sk, :prefix)").unwrap();
        let item = make_item(&[("sk", AttributeValue::S("user#123".into()))]);
        let av = vals(&[(":prefix", AttributeValue::S("user#".into()))]);
        assert!(evaluate_without_tracking(&expr, &item, &None, &av).unwrap());
    }

    #[test]
    fn test_contains_string() {
        let expr = parse("contains(description, :sub)").unwrap();
        let item = make_item(&[("description", AttributeValue::S("hello world".into()))]);
        let av = vals(&[(":sub", AttributeValue::S("world".into()))]);
        assert!(evaluate_without_tracking(&expr, &item, &None, &av).unwrap());
    }

    #[test]
    fn test_contains_string_set() {
        let expr = parse("contains(tags, :tag)").unwrap();
        let item = make_item(&[(
            "tags",
            AttributeValue::SS(vec!["rust".into(), "dynamo".into()]),
        )]);
        let av = vals(&[(":tag", AttributeValue::S("rust".into()))]);
        assert!(evaluate_without_tracking(&expr, &item, &None, &av).unwrap());
    }

    #[test]
    fn test_size_function() {
        let expr = parse("size(label) > :len").unwrap();
        let item = make_item(&[("label", AttributeValue::S("Alice".into()))]);
        let av = vals(&[(":len", AttributeValue::N("3".into()))]);
        assert!(evaluate_without_tracking(&expr, &item, &None, &av).unwrap());
    }

    #[test]
    fn test_and_operator() {
        let expr = parse("price > :min AND price < :max").unwrap();
        let item = make_item(&[("price", AttributeValue::N("50".into()))]);
        let av = vals(&[
            (":min", AttributeValue::N("10".into())),
            (":max", AttributeValue::N("100".into())),
        ]);
        assert!(evaluate_without_tracking(&expr, &item, &None, &av).unwrap());
    }

    #[test]
    fn test_or_operator() {
        let expr = parse("state_val = :s1 OR state_val = :s2").unwrap();
        let item = make_item(&[("state_val", AttributeValue::S("pending".into()))]);
        let av = vals(&[
            (":s1", AttributeValue::S("active".into())),
            (":s2", AttributeValue::S("pending".into())),
        ]);
        assert!(evaluate_without_tracking(&expr, &item, &None, &av).unwrap());
    }

    #[test]
    fn test_not_operator() {
        let expr = parse("NOT state_val = :val").unwrap();
        let item = make_item(&[("state_val", AttributeValue::S("active".into()))]);
        let av = vals(&[(":val", AttributeValue::S("closed".into()))]);
        assert!(evaluate_without_tracking(&expr, &item, &None, &av).unwrap());
    }

    #[test]
    fn test_expression_attribute_names() {
        let expr = parse("#s = :val").unwrap();
        let item = make_item(&[("status", AttributeValue::S("active".into()))]);
        let an = names(&[("#s", "status")]);
        let av = vals(&[(":val", AttributeValue::S("active".into()))]);
        assert!(evaluate_without_tracking(&expr, &item, &an, &av).unwrap());
    }

    #[test]
    fn test_nested_path() {
        let expr = parse("profile.label = :val").unwrap();
        let mut nested = HashMap::new();
        nested.insert("label".to_string(), AttributeValue::S("Alice".into()));
        let item = make_item(&[("profile", AttributeValue::M(nested))]);
        let av = vals(&[(":val", AttributeValue::S("Alice".into()))]);
        assert!(evaluate_without_tracking(&expr, &item, &None, &av).unwrap());
    }

    #[test]
    fn test_parenthesized() {
        let expr = parse("(a = :x OR b = :y) AND c = :z").unwrap();
        let item = make_item(&[
            ("a", AttributeValue::S("1".into())),
            ("b", AttributeValue::S("2".into())),
            ("c", AttributeValue::S("3".into())),
        ]);
        let av = vals(&[
            (":x", AttributeValue::S("wrong".into())),
            (":y", AttributeValue::S("2".into())),
            (":z", AttributeValue::S("3".into())),
        ]);
        assert!(evaluate_without_tracking(&expr, &item, &None, &av).unwrap());
    }

    #[test]
    fn test_missing_attribute_is_false() {
        let expr = parse("nonexistent = :val").unwrap();
        let item: HashMap<String, AttributeValue> = HashMap::new();
        let av = vals(&[(":val", AttributeValue::S("x".into()))]);
        assert!(!evaluate_without_tracking(&expr, &item, &None, &av).unwrap());
    }
}
