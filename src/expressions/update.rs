//! UpdateExpression parsing and evaluation.
//!
//! Supports SET, REMOVE, ADD, DELETE clauses.

use crate::expressions::condition::parse_raw_path;
use crate::expressions::tokenizer::{
    Token, TokenStream, near_window_parser, near_window_tokenizer, tokenize,
};
use crate::expressions::{
    PathElement, TrackedExpressionAttributes, remove_path, resolve_path, resolve_path_elements,
    set_path,
};
use crate::types::AttributeValue;
use std::collections::HashMap;

/// Parsed update expression with all clause actions.
#[derive(Debug)]
pub struct UpdateExpr {
    pub set_actions: Vec<SetAction>,
    pub remove_actions: Vec<Vec<PathElement>>,
    pub add_actions: Vec<AddAction>,
    pub delete_actions: Vec<DeleteAction>,
}

/// A SET action: `path = value_expr`
#[derive(Debug)]
pub struct SetAction {
    pub path: Vec<PathElement>,
    pub value: SetValue,
}

/// Value expression for SET.
#[derive(Debug)]
pub enum SetValue {
    /// Direct value or path reference
    Operand(SetOperand),
    /// `operand + operand`
    Plus(SetOperand, SetOperand),
    /// `operand - operand`
    Minus(SetOperand, SetOperand),
}

/// An operand in a SET expression.
#[derive(Debug)]
pub enum SetOperand {
    Path(Vec<PathElement>),
    ValueRef(String),
    IfNotExists(Vec<PathElement>, Box<SetOperand>),
    ListAppend(Box<SetOperand>, Box<SetOperand>),
}

/// An ADD action: `path :value`
#[derive(Debug)]
pub struct AddAction {
    pub path: Vec<PathElement>,
    pub value_ref: String,
}

/// A DELETE action: `path :value`
#[derive(Debug)]
pub struct DeleteAction {
    pub path: Vec<PathElement>,
    pub value_ref: String,
}

/// Parse an UpdateExpression string.
pub fn parse(expr: &str) -> Result<UpdateExpr, String> {
    let tokens = match tokenize(expr) {
        Ok(t) => t,
        Err(err) => {
            // Tokenizer-level syntax error (e.g. stray `!` mid-expression):
            // emit the same shape as parser-level errors, with a tokenizer-style
            // near: window (offending byte plus at most one more non-whitespace byte).
            let bad = &expr[err.position..err.position + err.bad_len];
            let near = near_window_tokenizer(expr, err.position);
            return Err(format!(
                r#"Invalid UpdateExpression: Syntax error; token: "{bad}", near: "{near}""#
            ));
        }
    };
    let mut stream = TokenStream::new(tokens);

    let mut set_actions = Vec::new();
    let mut remove_actions = Vec::new();
    let mut add_actions = Vec::new();
    let mut delete_actions = Vec::new();

    let mut seen_set = false;
    let mut seen_remove = false;
    let mut seen_add = false;
    let mut seen_delete = false;

    while !stream.at_end() {
        match stream.peek() {
            Some(Token::Set) => {
                if seen_set {
                    return Err("Invalid UpdateExpression: The \"SET\" section can only be used once in an update expression;".to_string());
                }
                seen_set = true;
                stream.next();
                parse_set_clause(&mut stream, &mut set_actions).map_err(wrap_syntax_error)?;
            }
            Some(Token::Remove) => {
                if seen_remove {
                    return Err("Invalid UpdateExpression: The \"REMOVE\" section can only be used once in an update expression;".to_string());
                }
                seen_remove = true;
                stream.next();
                parse_remove_clause(&mut stream, &mut remove_actions).map_err(wrap_syntax_error)?;
            }
            Some(Token::Add) => {
                if seen_add {
                    return Err("Invalid UpdateExpression: The \"ADD\" section can only be used once in an update expression;".to_string());
                }
                seen_add = true;
                stream.next();
                parse_add_clause(&mut stream, &mut add_actions).map_err(wrap_syntax_error)?;
            }
            Some(Token::Delete) => {
                if seen_delete {
                    return Err("Invalid UpdateExpression: The \"DELETE\" section can only be used once in an update expression;".to_string());
                }
                seen_delete = true;
                stream.next();
                parse_delete_clause(&mut stream, &mut delete_actions).map_err(wrap_syntax_error)?;
            }
            Some(_) => {
                // Unexpected leading token where SET/REMOVE/ADD/DELETE was required.
                // Build the AWS-style "token: \"X\", near: \"X Y\"" window from the
                // offending token's span and the next token's span (if any).
                let offending_span = stream
                    .peek_span()
                    .expect("peek_span must yield when peek did");
                let bad = &expr[offending_span.start..offending_span.end()];
                stream.next();
                let next_span = stream.peek_span();
                let near = near_window_parser(expr, offending_span, next_span);
                return Err(format!(
                    r#"Invalid UpdateExpression: Syntax error; token: "{bad}", near: "{near}""#
                ));
            }
            None => break,
        }
    }

    Ok(UpdateExpr {
        set_actions,
        remove_actions,
        add_actions,
        delete_actions,
    })
}

/// Wrap a sub-parser error with the standard syntax error prefix,
/// unless it already has a recognised higher-level prefix.
fn wrap_syntax_error(err: String) -> String {
    if err.starts_with("Invalid UpdateExpression:") {
        err
    } else if err.starts_with("Attribute name is a reserved keyword") {
        format!("Invalid UpdateExpression: {err}")
    } else {
        format!("Invalid UpdateExpression: Syntax error; {err}")
    }
}

/// Walk an UpdateExpr and track all attribute name and value references
/// without actually evaluating or modifying any item. This is used for
/// pre-validation: checking that all referenced names/values are defined,
/// and detecting unused names/values.
pub fn track_references(
    expr: &UpdateExpr,
    tracker: &TrackedExpressionAttributes,
) -> Result<(), String> {
    // Collect all target paths for overlap/conflict detection
    let mut all_target_paths: Vec<Vec<PathElement>> = Vec::new();

    for action in &expr.set_actions {
        track_path_refs(&action.path, tracker)?;
        track_set_value_refs(&action.value, tracker)?;
        all_target_paths.push(resolve_tracked_path(&action.path, tracker));
    }
    for path in &expr.remove_actions {
        track_path_refs(path, tracker)?;
        all_target_paths.push(resolve_tracked_path(path, tracker));
    }
    for action in &expr.add_actions {
        track_path_refs(&action.path, tracker)?;
        let val = tracker.resolve_value(&action.value_ref)?;
        // Validate ADD operand type statically
        validate_add_type(val)?;
        all_target_paths.push(resolve_tracked_path(&action.path, tracker));
    }
    for action in &expr.delete_actions {
        track_path_refs(&action.path, tracker)?;
        let val = tracker.resolve_value(&action.value_ref)?;
        // Validate DELETE operand type statically
        validate_delete_type(val)?;
        all_target_paths.push(resolve_tracked_path(&action.path, tracker));
    }

    // Static type validation for SET value expressions
    for action in &expr.set_actions {
        validate_set_value_types(&action.value, tracker)?;
    }

    // Check for overlapping/conflicting paths
    check_path_overlaps(&all_target_paths)?;

    Ok(())
}

/// Validate that an ADD operand has a compatible type.
fn validate_add_type(val: &crate::types::AttributeValue) -> Result<(), String> {
    use crate::types::AttributeValue;
    match val {
        AttributeValue::N(_)
        | AttributeValue::SS(_)
        | AttributeValue::NS(_)
        | AttributeValue::BS(_) => Ok(()),
        _ => Err(format!(
            "Invalid UpdateExpression: Incorrect operand type for operator or function; \
             operator: ADD, operand type: {}",
            dynamo_type_name(val)
        )),
    }
}

/// Validate that a DELETE operand has a compatible type.
fn validate_delete_type(val: &crate::types::AttributeValue) -> Result<(), String> {
    use crate::types::AttributeValue;
    match val {
        AttributeValue::SS(_) | AttributeValue::NS(_) | AttributeValue::BS(_) => Ok(()),
        _ => Err(format!(
            "Invalid UpdateExpression: Incorrect operand type for operator or function; \
             operator: DELETE, operand type: {}",
            dynamo_type_name(val)
        )),
    }
}

/// Map an AttributeValue to its DynamoDB type name for error messages.
fn dynamo_type_name(val: &crate::types::AttributeValue) -> &'static str {
    use crate::types::AttributeValue;
    match val {
        AttributeValue::S(_) => "STRING",
        AttributeValue::N(_) => "NUMBER",
        AttributeValue::B(_) => "BINARY",
        AttributeValue::BOOL(_) => "BOOLEAN",
        AttributeValue::NULL(_) => "NULL",
        AttributeValue::SS(_) => "SS",
        AttributeValue::NS(_) => "NS",
        AttributeValue::BS(_) => "BS",
        AttributeValue::L(_) => "LIST",
        AttributeValue::M(_) => "MAP",
    }
}

/// Validate types for SET value expressions (arithmetic, list_append).
fn validate_set_value_types(
    value: &SetValue,
    tracker: &TrackedExpressionAttributes,
) -> Result<(), String> {
    match value {
        SetValue::Operand(op) => validate_set_operand_types(op, tracker),
        SetValue::Plus(left, right) => {
            validate_arithmetic_operand(left, "+", tracker)?;
            validate_arithmetic_operand(right, "+", tracker)
        }
        SetValue::Minus(left, right) => {
            validate_arithmetic_operand(left, "-", tracker)?;
            validate_arithmetic_operand(right, "-", tracker)
        }
    }
}

/// Validate that an operand used in + or - is a number (if it's a value ref).
fn validate_arithmetic_operand(
    operand: &SetOperand,
    op: &str,
    tracker: &TrackedExpressionAttributes,
) -> Result<(), String> {
    use crate::types::AttributeValue;
    match operand {
        SetOperand::ValueRef(name) => {
            let val = tracker.resolve_value(name)?;
            if !matches!(val, AttributeValue::N(_)) {
                return Err(format!(
                    "Invalid UpdateExpression: Incorrect operand type for operator or function; \
                     operator or function: {op}, operand type: {}",
                    dynamo_type_name(val)
                ));
            }
            Ok(())
        }
        SetOperand::IfNotExists(_, default) => validate_set_operand_types(default, tracker),
        SetOperand::ListAppend(a, b) => {
            validate_list_append_operand(a, tracker)?;
            validate_list_append_operand(b, tracker)
        }
        SetOperand::Path(_) => Ok(()), // Path types checked at runtime
    }
}

/// Validate types for a set operand (recursively).
fn validate_set_operand_types(
    operand: &SetOperand,
    tracker: &TrackedExpressionAttributes,
) -> Result<(), String> {
    match operand {
        SetOperand::ListAppend(a, b) => {
            validate_list_append_operand(a, tracker)?;
            validate_list_append_operand(b, tracker)
        }
        SetOperand::IfNotExists(_, default) => validate_set_operand_types(default, tracker),
        _ => Ok(()),
    }
}

/// Validate a list_append operand is a list if it's a value ref.
fn validate_list_append_operand(
    operand: &SetOperand,
    tracker: &TrackedExpressionAttributes,
) -> Result<(), String> {
    use crate::types::AttributeValue;
    if let SetOperand::ValueRef(name) = operand {
        let val = tracker.resolve_value(name)?;
        if !matches!(val, AttributeValue::L(_)) {
            return Err(format!(
                "Invalid UpdateExpression: Incorrect operand type for operator or function; \
                 operator or function: list_append, operand type: {}",
                dynamo_type_name(val)
            ));
        }
    }
    Ok(())
}

/// Resolve path elements to their final names (expanding #name refs).
fn resolve_tracked_path(
    path: &[PathElement],
    tracker: &TrackedExpressionAttributes,
) -> Vec<PathElement> {
    path.iter()
        .map(|elem| {
            if let PathElement::Attribute(name) = elem {
                if name.starts_with('#') {
                    if let Ok(resolved) = tracker.resolve_name(name) {
                        return PathElement::Attribute(resolved);
                    }
                }
            }
            elem.clone()
        })
        .collect()
}

/// Format a path for error messages in dynalite format: [a, b, [1], c].
fn format_path_for_error(path: &[PathElement]) -> String {
    let parts: Vec<String> = path
        .iter()
        .map(|elem| match elem {
            PathElement::Attribute(name) => name.clone(),
            PathElement::Index(i) => format!("[{i}]"),
        })
        .collect();
    format!("[{}]", parts.join(", "))
}

/// Check for overlapping or conflicting document paths.
///
/// Two paths overlap if one is a prefix of the other (e.g., `a.b` and `a.b.c`).
/// Two paths conflict if they share elements but diverge in type at the same
/// position (e.g., `a[3].c` and `a.c[3]`).
fn check_path_overlaps(paths: &[Vec<PathElement>]) -> Result<(), String> {
    for i in 0..paths.len() {
        for j in (i + 1)..paths.len() {
            let a = &paths[i];
            let b = &paths[j];
            let min_len = a.len().min(b.len());

            // Check common prefix length
            let mut common = 0;
            for k in 0..min_len {
                if a[k] == b[k] {
                    common += 1;
                } else {
                    break;
                }
            }

            if common == 0 {
                continue;
            }

            // If one path is a prefix of the other, they overlap
            if common == a.len() || common == b.len() {
                let (shorter, longer) = if a.len() <= b.len() { (a, b) } else { (b, a) };
                return Err(format!(
                    "Invalid UpdateExpression: Two document paths overlap with each other; \
                     must remove or rewrite one of these paths; \
                     path one: {}, path two: {}",
                    format_path_for_error(longer),
                    format_path_for_error(shorter)
                ));
            }

            // If paths share a prefix but diverge, they conflict
            if common > 0 && common < min_len && a == b {
                return Err(format!(
                    "Invalid UpdateExpression: Two document paths conflict with each other; \
                     must remove or rewrite one of these paths; \
                     path one: {}, path two: {}",
                    format_path_for_error(a),
                    format_path_for_error(b)
                ));
            }
        }
    }
    Ok(())
}

fn track_path_refs(
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

fn track_set_value_refs(
    value: &SetValue,
    tracker: &TrackedExpressionAttributes,
) -> Result<(), String> {
    match value {
        SetValue::Operand(op) => track_set_operand_refs(op, tracker),
        SetValue::Plus(left, right) | SetValue::Minus(left, right) => {
            track_set_operand_refs(left, tracker)?;
            track_set_operand_refs(right, tracker)
        }
    }
}

fn track_set_operand_refs(
    operand: &SetOperand,
    tracker: &TrackedExpressionAttributes,
) -> Result<(), String> {
    match operand {
        SetOperand::Path(path) => track_path_refs(path, tracker),
        SetOperand::ValueRef(name) => {
            tracker.resolve_value(name)?;
            Ok(())
        }
        SetOperand::IfNotExists(path, default) => {
            track_path_refs(path, tracker)?;
            track_set_operand_refs(default, tracker)
        }
        SetOperand::ListAppend(a, b) => {
            track_set_operand_refs(a, tracker)?;
            track_set_operand_refs(b, tracker)
        }
    }
}

/// Apply an update expression to an item (mutating it in place), tracking attribute usage.
pub fn apply(
    item: &mut HashMap<String, AttributeValue>,
    expr: &UpdateExpr,
    tracker: &TrackedExpressionAttributes,
) -> Result<(), String> {
    // Process SET actions
    for action in &expr.set_actions {
        let resolved_path = resolve_path_elements(&action.path, tracker)?;
        let value = evaluate_set_value(&action.value, item, tracker)?;
        set_path(item, &resolved_path, value)?;
    }

    // Process REMOVE actions
    for path in &expr.remove_actions {
        let resolved_path = resolve_path_elements(path, tracker)?;
        remove_path(item, &resolved_path)?;
    }

    // Process ADD actions
    for action in &expr.add_actions {
        let resolved_path = resolve_path_elements(&action.path, tracker)?;
        let add_val = tracker.resolve_value(&action.value_ref)?.clone();
        apply_add(item, &resolved_path, &add_val).map_err(|_| {
            "An operand in the update expression has an incorrect data type".to_string()
        })?;
    }

    // Process DELETE actions
    for action in &expr.delete_actions {
        let resolved_path = resolve_path_elements(&action.path, tracker)?;
        let del_val = tracker.resolve_value(&action.value_ref)?.clone();
        apply_delete(item, &resolved_path, &del_val).map_err(|_| {
            "An operand in the update expression has an incorrect data type".to_string()
        })?;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// SET value evaluation
// ---------------------------------------------------------------------------

fn evaluate_set_value(
    value: &SetValue,
    item: &HashMap<String, AttributeValue>,
    tracker: &TrackedExpressionAttributes,
) -> Result<AttributeValue, String> {
    match value {
        SetValue::Operand(op) => evaluate_set_operand(op, item, tracker),
        SetValue::Plus(left, right) => {
            let lv = evaluate_set_operand(left, item, tracker)?;
            let rv = evaluate_set_operand(right, item, tracker)?;
            match (&lv, &rv) {
                (AttributeValue::N(a), AttributeValue::N(b)) => {
                    use bigdecimal::BigDecimal;
                    use std::str::FromStr;
                    let da = BigDecimal::from_str(a).map_err(|_| format!("Invalid number: {a}"))?;
                    let db = BigDecimal::from_str(b).map_err(|_| format!("Invalid number: {b}"))?;
                    let result = &da + &db;
                    Ok(AttributeValue::N(format_number(&result)))
                }
                _ => Err("Operands for + must be numbers".to_string()),
            }
        }
        SetValue::Minus(left, right) => {
            let lv = evaluate_set_operand(left, item, tracker)?;
            let rv = evaluate_set_operand(right, item, tracker)?;
            match (&lv, &rv) {
                (AttributeValue::N(a), AttributeValue::N(b)) => {
                    use bigdecimal::BigDecimal;
                    use std::str::FromStr;
                    let da = BigDecimal::from_str(a).map_err(|_| format!("Invalid number: {a}"))?;
                    let db = BigDecimal::from_str(b).map_err(|_| format!("Invalid number: {b}"))?;
                    let result = &da - &db;
                    Ok(AttributeValue::N(format_number(&result)))
                }
                _ => Err("Operands for - must be numbers".to_string()),
            }
        }
    }
}

fn evaluate_set_operand(
    operand: &SetOperand,
    item: &HashMap<String, AttributeValue>,
    tracker: &TrackedExpressionAttributes,
) -> Result<AttributeValue, String> {
    match operand {
        SetOperand::Path(path) => {
            let resolved = resolve_path_elements(path, tracker)?;
            resolve_path(item, &resolved).ok_or_else(|| {
                "The provided expression refers to an attribute that does not exist in the item"
                    .to_string()
            })
        }
        SetOperand::ValueRef(name) => Ok(tracker.resolve_value(name)?.clone()),
        SetOperand::IfNotExists(path, default) => {
            let resolved = resolve_path_elements(path, tracker)?;
            match resolve_path(item, &resolved) {
                Some(existing) => Ok(existing),
                None => evaluate_set_operand(default, item, tracker),
            }
        }
        SetOperand::ListAppend(list1, list2) => {
            let v1 = evaluate_set_operand(list1, item, tracker)?;
            let v2 = evaluate_set_operand(list2, item, tracker)?;
            match (v1, v2) {
                (AttributeValue::L(mut a), AttributeValue::L(b)) => {
                    a.extend(b);
                    Ok(AttributeValue::L(a))
                }
                _ => Err("list_append requires two list operands".to_string()),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// ADD action
// ---------------------------------------------------------------------------

/// Public wrapper for use by legacy `AttributeUpdates` support.
pub fn apply_add_public(
    item: &mut HashMap<String, AttributeValue>,
    path: &[PathElement],
    add_val: &AttributeValue,
) -> Result<(), String> {
    apply_add(item, path, add_val)
}

fn apply_add(
    item: &mut HashMap<String, AttributeValue>,
    path: &[PathElement],
    add_val: &AttributeValue,
) -> Result<(), String> {
    let existing = resolve_path(item, path);

    match (existing, add_val) {
        // Number: add to existing number or create
        (Some(AttributeValue::N(existing_n)), AttributeValue::N(add_n)) => {
            use bigdecimal::BigDecimal;
            use std::str::FromStr;
            let de = BigDecimal::from_str(&existing_n)
                .map_err(|_| format!("Invalid number: {existing_n}"))?;
            let da = BigDecimal::from_str(add_n).map_err(|_| format!("Invalid number: {add_n}"))?;
            let result = &de + &da;
            set_path(item, path, AttributeValue::N(format_number(&result)))
        }
        (None, AttributeValue::N(_)) => {
            // Create with the provided value
            set_path(item, path, add_val.clone())
        }

        // String set: union
        (Some(AttributeValue::SS(mut existing_set)), AttributeValue::SS(add_set)) => {
            for s in add_set {
                if !existing_set.contains(s) {
                    existing_set.push(s.clone());
                }
            }
            set_path(item, path, AttributeValue::SS(existing_set))
        }
        (None, AttributeValue::SS(_)) => set_path(item, path, add_val.clone()),

        // Number set: union
        (Some(AttributeValue::NS(mut existing_set)), AttributeValue::NS(add_set)) => {
            for n in add_set {
                if !existing_set.contains(n) {
                    existing_set.push(n.clone());
                }
            }
            set_path(item, path, AttributeValue::NS(existing_set))
        }
        (None, AttributeValue::NS(_)) => set_path(item, path, add_val.clone()),

        // Binary set: union
        (Some(AttributeValue::BS(mut existing_set)), AttributeValue::BS(add_set)) => {
            for b in add_set {
                if !existing_set.contains(b) {
                    existing_set.push(b.clone());
                }
            }
            set_path(item, path, AttributeValue::BS(existing_set))
        }
        (None, AttributeValue::BS(_)) => set_path(item, path, add_val.clone()),

        // List: append elements (legacy AttributeUpdates behaviour)
        (Some(AttributeValue::L(mut existing_list)), AttributeValue::L(add_list)) => {
            existing_list.extend(add_list.iter().cloned());
            set_path(item, path, AttributeValue::L(existing_list))
        }
        (None, AttributeValue::L(_)) => set_path(item, path, add_val.clone()),

        _ => Err("Type mismatch for attribute to update".to_string()),
    }
}

// ---------------------------------------------------------------------------
// DELETE action
// ---------------------------------------------------------------------------

/// Public wrapper for use by legacy `AttributeUpdates` support.
pub fn apply_delete_public(
    item: &mut HashMap<String, AttributeValue>,
    path: &[PathElement],
    del_val: &AttributeValue,
) -> Result<(), String> {
    apply_delete(item, path, del_val)
}

fn apply_delete(
    item: &mut HashMap<String, AttributeValue>,
    path: &[PathElement],
    del_val: &AttributeValue,
) -> Result<(), String> {
    let existing = resolve_path(item, path);

    match (existing, del_val) {
        (Some(AttributeValue::SS(existing_set)), AttributeValue::SS(del_set)) => {
            let new_set: Vec<String> = existing_set
                .into_iter()
                .filter(|s| !del_set.contains(s))
                .collect();
            if new_set.is_empty() {
                remove_path(item, path)
            } else {
                set_path(item, path, AttributeValue::SS(new_set))
            }
        }
        (Some(AttributeValue::NS(existing_set)), AttributeValue::NS(del_set)) => {
            let new_set: Vec<String> = existing_set
                .into_iter()
                .filter(|n| !del_set.contains(n))
                .collect();
            if new_set.is_empty() {
                remove_path(item, path)
            } else {
                set_path(item, path, AttributeValue::NS(new_set))
            }
        }
        (Some(AttributeValue::BS(existing_set)), AttributeValue::BS(del_set)) => {
            let new_set: Vec<Vec<u8>> = existing_set
                .into_iter()
                .filter(|b| !del_set.contains(b))
                .collect();
            if new_set.is_empty() {
                remove_path(item, path)
            } else {
                set_path(item, path, AttributeValue::BS(new_set))
            }
        }
        (None, _) => Ok(()), // Nothing to delete from
        _ => Err("Type mismatch for attribute to update".to_string()),
    }
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

fn parse_set_clause(stream: &mut TokenStream, actions: &mut Vec<SetAction>) -> Result<(), String> {
    actions.push(parse_set_action(stream)?);
    while matches!(stream.peek(), Some(Token::Comma)) {
        stream.next();
        actions.push(parse_set_action(stream)?);
    }
    Ok(())
}

fn parse_set_action(stream: &mut TokenStream) -> Result<SetAction, String> {
    let path = parse_raw_path(stream)?;
    stream.expect(&Token::Eq)?;
    let value = parse_set_value(stream)?;
    Ok(SetAction { path, value })
}

fn parse_set_value(stream: &mut TokenStream) -> Result<SetValue, String> {
    let left = parse_set_operand(stream)?;

    match stream.peek() {
        Some(Token::Plus) => {
            stream.next();
            let right = parse_set_operand(stream)?;
            Ok(SetValue::Plus(left, right))
        }
        Some(Token::Minus) => {
            stream.next();
            let right = parse_set_operand(stream)?;
            Ok(SetValue::Minus(left, right))
        }
        _ => Ok(SetValue::Operand(left)),
    }
}

fn parse_set_operand(stream: &mut TokenStream) -> Result<SetOperand, String> {
    // Check for functions: if_not_exists, list_append
    if let Some(Token::Identifier(name)) = stream.peek() {
        let func_name = name.to_lowercase();
        let orig_name = name.clone();
        match func_name.as_str() {
            "if_not_exists" => {
                stream.next();
                stream.expect(&Token::LParen)?;

                // First argument must be a document path (not a value ref or function)
                match stream.peek() {
                    Some(Token::ValueRef(_)) => {
                        return Err(
                            "Invalid UpdateExpression: Operator or function requires a document path; \
                             operator or function: if_not_exists".to_string()
                        );
                    }
                    Some(Token::Identifier(fname))
                        if fname.to_lowercase() == "if_not_exists"
                            || fname.to_lowercase() == "list_append" =>
                    {
                        return Err(
                            "Invalid UpdateExpression: Operator or function requires a document path; \
                             operator or function: if_not_exists".to_string()
                        );
                    }
                    _ => {}
                }

                let path = parse_raw_path(stream)?;

                // Check for correct number of operands
                if !matches!(stream.peek(), Some(Token::Comma)) {
                    return Err(
                        "Invalid UpdateExpression: Incorrect number of operands for operator or function; \
                         operator or function: if_not_exists, number of operands: 1".to_string()
                    );
                }
                stream.expect(&Token::Comma)?;
                let default = parse_set_operand(stream)?;
                stream.expect(&Token::RParen)?;
                return Ok(SetOperand::IfNotExists(path, Box::new(default)));
            }
            "list_append" => {
                stream.next();
                stream.expect(&Token::LParen)?;
                let list1 = parse_set_operand(stream)?;

                // Check for correct number of operands
                if !matches!(stream.peek(), Some(Token::Comma)) {
                    return Err(
                        "Invalid UpdateExpression: Incorrect number of operands for operator or function; \
                         operator or function: list_append, number of operands: 1".to_string()
                    );
                }
                stream.expect(&Token::Comma)?;
                let list2 = parse_set_operand(stream)?;
                stream.expect(&Token::RParen)?;
                return Ok(SetOperand::ListAppend(Box::new(list1), Box::new(list2)));
            }
            _ => {
                // Check if this looks like a function call (identifier followed by '(')
                // If so, report "Invalid function name" for unknown functions.
                let saved_pos = stream.pos();
                stream.next();
                if matches!(stream.peek(), Some(Token::LParen)) {
                    return Err(format!(
                        "Invalid UpdateExpression: Invalid function name; function: {}",
                        orig_name
                    ));
                }
                // Rewind — not a function call, treat as path
                stream.set_pos(saved_pos);
            }
        }
    }

    match stream.peek() {
        Some(Token::ValueRef(_)) => {
            if let Some(Token::ValueRef(name)) = stream.next().cloned() {
                Ok(SetOperand::ValueRef(name))
            } else {
                unreachable!()
            }
        }
        Some(Token::Identifier(_)) | Some(Token::NameRef(_)) => {
            let path = parse_raw_path(stream)?;
            Ok(SetOperand::Path(path))
        }
        Some(t) => Err(format!("Expected operand in SET, got {t}")),
        None => Err("Expected operand in SET, got end of expression".to_string()),
    }
}

fn parse_remove_clause(
    stream: &mut TokenStream,
    actions: &mut Vec<Vec<PathElement>>,
) -> Result<(), String> {
    actions.push(parse_raw_path(stream)?);
    while matches!(stream.peek(), Some(Token::Comma)) {
        stream.next();
        actions.push(parse_raw_path(stream)?);
    }
    Ok(())
}

fn parse_add_clause(stream: &mut TokenStream, actions: &mut Vec<AddAction>) -> Result<(), String> {
    actions.push(parse_add_action(stream)?);
    while matches!(stream.peek(), Some(Token::Comma)) {
        stream.next();
        actions.push(parse_add_action(stream)?);
    }
    Ok(())
}

fn parse_add_action(stream: &mut TokenStream) -> Result<AddAction, String> {
    let path = parse_raw_path(stream)?;
    match stream.next() {
        Some(Token::ValueRef(name)) => Ok(AddAction {
            path,
            value_ref: name.clone(),
        }),
        Some(t) => Err(format!("Expected value reference in ADD, got {t}")),
        None => Err("Expected value reference in ADD, got end of expression".to_string()),
    }
}

fn parse_delete_clause(
    stream: &mut TokenStream,
    actions: &mut Vec<DeleteAction>,
) -> Result<(), String> {
    actions.push(parse_delete_action(stream)?);
    while matches!(stream.peek(), Some(Token::Comma)) {
        stream.next();
        actions.push(parse_delete_action(stream)?);
    }
    Ok(())
}

fn parse_delete_action(stream: &mut TokenStream) -> Result<DeleteAction, String> {
    let path = parse_raw_path(stream)?;
    match stream.next() {
        Some(Token::ValueRef(name)) => Ok(DeleteAction {
            path,
            value_ref: name.clone(),
        }),
        Some(t) => Err(format!("Expected value reference in DELETE, got {t}")),
        None => Err("Expected value reference in DELETE, got end of expression".to_string()),
    }
}

/// Format a BigDecimal number, stripping unnecessary trailing zeros.
/// DynamoDB returns numbers without scientific notation.
fn format_number(n: &bigdecimal::BigDecimal) -> String {
    let normalized = n.normalized();
    // Force scale >= 0 so BigDecimal renders without scientific notation.
    // When the exponent is negative (large integer like 1e38), with_scale(0)
    // expands to full decimal digits.
    if normalized.as_bigint_and_exponent().1 < 0 {
        normalized.with_scale(0).to_string()
    } else {
        normalized.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_item(pairs: &[(&str, AttributeValue)]) -> HashMap<String, AttributeValue> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    fn vals(pairs: &[(&str, AttributeValue)]) -> Option<HashMap<String, AttributeValue>> {
        Some(make_item(pairs))
    }

    fn make_tracker<'a>(
        names: &'a Option<HashMap<String, String>>,
        values: &'a Option<HashMap<String, AttributeValue>>,
    ) -> TrackedExpressionAttributes<'a> {
        TrackedExpressionAttributes::new(names, values)
    }

    #[test]
    fn test_set_simple() {
        let expr = parse("SET label = :val").unwrap();
        assert_eq!(expr.set_actions.len(), 1);
        assert!(expr.remove_actions.is_empty());
    }

    #[test]
    fn test_set_multiple() {
        let expr = parse("SET a = :v1, b = :v2").unwrap();
        assert_eq!(expr.set_actions.len(), 2);
    }

    #[test]
    fn test_set_arithmetic_plus() {
        let expr = parse("SET tally = tally + :inc").unwrap();
        let mut item = make_item(&[
            ("pk", AttributeValue::S("k".into())),
            ("tally", AttributeValue::N("10".into())),
        ]);
        let av = vals(&[(":inc", AttributeValue::N("5".into()))]);
        let no_names = None;
        let tracker = make_tracker(&no_names, &av);
        apply(&mut item, &expr, &tracker).unwrap();
        assert_eq!(item["tally"], AttributeValue::N("15".into()));
    }

    #[test]
    fn test_set_arithmetic_minus() {
        let expr = parse("SET price = price - :discount").unwrap();
        let mut item = make_item(&[
            ("pk", AttributeValue::S("k".into())),
            ("price", AttributeValue::N("100".into())),
        ]);
        let av = vals(&[(":discount", AttributeValue::N("25".into()))]);
        let no_names = None;
        let tracker = make_tracker(&no_names, &av);
        apply(&mut item, &expr, &tracker).unwrap();
        assert_eq!(item["price"], AttributeValue::N("75".into()));
    }

    #[test]
    fn test_set_if_not_exists() {
        let expr = parse("SET hits = if_not_exists(hits, :zero)").unwrap();
        let mut item = make_item(&[("pk", AttributeValue::S("k".into()))]);
        let av = vals(&[(":zero", AttributeValue::N("0".into()))]);
        let no_names = None;
        let tracker = make_tracker(&no_names, &av);
        apply(&mut item, &expr, &tracker).unwrap();
        assert_eq!(item["hits"], AttributeValue::N("0".into()));

        // Apply again — existing value should be preserved
        let tracker2 = make_tracker(&no_names, &av);
        apply(&mut item, &expr, &tracker2).unwrap();
        assert_eq!(item["hits"], AttributeValue::N("0".into()));
    }

    #[test]
    fn test_set_list_append() {
        let expr = parse("SET entries = list_append(entries, :new)").unwrap();
        let mut item = make_item(&[
            ("pk", AttributeValue::S("k".into())),
            (
                "entries",
                AttributeValue::L(vec![AttributeValue::S("a".into())]),
            ),
        ]);
        let av = vals(&[(
            ":new",
            AttributeValue::L(vec![AttributeValue::S("b".into())]),
        )]);
        let no_names = None;
        let tracker = make_tracker(&no_names, &av);
        apply(&mut item, &expr, &tracker).unwrap();
        if let AttributeValue::L(list) = &item["entries"] {
            assert_eq!(list.len(), 2);
        } else {
            panic!("Expected list");
        }
    }

    #[test]
    fn test_remove() {
        let expr = parse("REMOVE attr1, attr2").unwrap();
        let mut item = make_item(&[
            ("pk", AttributeValue::S("k".into())),
            ("attr1", AttributeValue::S("a".into())),
            ("attr2", AttributeValue::S("b".into())),
            ("attr3", AttributeValue::S("c".into())),
        ]);
        let no_names = None;
        let no_values = None;
        let tracker = make_tracker(&no_names, &no_values);
        apply(&mut item, &expr, &tracker).unwrap();
        assert!(!item.contains_key("attr1"));
        assert!(!item.contains_key("attr2"));
        assert!(item.contains_key("attr3"));
    }

    #[test]
    fn test_add_number() {
        let expr = parse("ADD tally :inc").unwrap();
        let mut item = make_item(&[
            ("pk", AttributeValue::S("k".into())),
            ("tally", AttributeValue::N("10".into())),
        ]);
        let av = vals(&[(":inc", AttributeValue::N("5".into()))]);
        let no_names = None;
        let tracker = make_tracker(&no_names, &av);
        apply(&mut item, &expr, &tracker).unwrap();
        assert_eq!(item["tally"], AttributeValue::N("15".into()));
    }

    #[test]
    fn test_add_number_create() {
        let expr = parse("ADD tally :val").unwrap();
        let mut item = make_item(&[("pk", AttributeValue::S("k".into()))]);
        let av = vals(&[(":val", AttributeValue::N("1".into()))]);
        let no_names = None;
        let tracker = make_tracker(&no_names, &av);
        apply(&mut item, &expr, &tracker).unwrap();
        assert_eq!(item["tally"], AttributeValue::N("1".into()));
    }

    #[test]
    fn test_add_string_set() {
        let expr = parse("ADD colors :new_colors").unwrap();
        let mut item = make_item(&[
            ("pk", AttributeValue::S("k".into())),
            (
                "colors",
                AttributeValue::SS(vec!["red".into(), "blue".into()]),
            ),
        ]);
        let av = vals(&[(
            ":new_colors",
            AttributeValue::SS(vec!["blue".into(), "green".into()]),
        )]);
        let no_names = None;
        let tracker = make_tracker(&no_names, &av);
        apply(&mut item, &expr, &tracker).unwrap();
        if let AttributeValue::SS(set) = &item["colors"] {
            assert_eq!(set.len(), 3); // red, blue, green (blue deduplicated)
            assert!(set.contains(&"green".to_string()));
        } else {
            panic!("Expected SS");
        }
    }

    #[test]
    fn test_delete_string_set() {
        let expr = parse("DELETE colors :remove").unwrap();
        let mut item = make_item(&[
            ("pk", AttributeValue::S("k".into())),
            (
                "colors",
                AttributeValue::SS(vec!["red".into(), "blue".into(), "green".into()]),
            ),
        ]);
        let av = vals(&[(
            ":remove",
            AttributeValue::SS(vec!["blue".into(), "green".into()]),
        )]);
        let no_names = None;
        let tracker = make_tracker(&no_names, &av);
        apply(&mut item, &expr, &tracker).unwrap();
        if let AttributeValue::SS(set) = &item["colors"] {
            assert_eq!(set, &vec!["red".to_string()]);
        } else {
            panic!("Expected SS");
        }
    }

    #[test]
    fn test_combined_set_remove() {
        let expr = parse("SET label = :name REMOVE old_attr").unwrap();
        assert_eq!(expr.set_actions.len(), 1);
        assert_eq!(expr.remove_actions.len(), 1);
    }

    #[test]
    fn test_duplicate_clause_error() {
        let result = parse("SET a = :v SET b = :w");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("only be used once"));
    }
}
