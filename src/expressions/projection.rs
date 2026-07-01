//! ProjectionExpression parsing and application.
//!
//! Parses comma-separated attribute paths, supports dot notation and bracket indexing.
//! Always includes key attributes in the result.

use crate::expressions::tokenizer::{Token, TokenStream, near_window_tokenizer, tokenize};
use crate::expressions::{
    PathElement, TrackedExpressionAttributes, resolve_path, resolve_path_elements,
};
use crate::types::AttributeValue;
use std::collections::HashMap;

/// A parsed projection — list of attribute paths.
#[derive(Debug, Clone)]
pub struct ProjectionExpr {
    pub paths: Vec<Vec<PathElement>>,
}

/// Parse a ProjectionExpression string.
pub fn parse(expr: &str) -> Result<ProjectionExpr, String> {
    let tokens = match tokenize(expr) {
        Ok(t) => t,
        Err(err) => {
            // Tokenizer-level syntax error (e.g. stray `!`): build the
            // AWS-style `near: "..."` window from the offending byte position.
            let bad = &expr[err.position..err.position + err.bad_len];
            let near = near_window_tokenizer(expr, err.position);
            return Err(format!(
                r#"Invalid ProjectionExpression: Syntax error; token: "{bad}", near: "{near}""#
            ));
        }
    };
    let mut stream = TokenStream::new(tokens);

    let mut paths = Vec::new();

    if stream.at_end() {
        return Ok(ProjectionExpr { paths });
    }

    paths.push(parse_path(&mut stream).map_err(|e| projection_parser_error(expr, &mut stream, e))?);

    while matches!(stream.peek(), Some(Token::Comma)) {
        stream.next();
        paths.push(
            parse_path(&mut stream).map_err(|e| projection_parser_error(expr, &mut stream, e))?,
        );
    }

    if !stream.at_end() {
        return Err(format!(
            "Unexpected token in ProjectionExpression: {}",
            stream.peek().unwrap()
        ));
    }

    Ok(ProjectionExpr { paths })
}

/// Wrap a parser-level ProjectionExpression error in the standard envelope.
/// For now this is a passthrough through the existing message shape; if the
/// conformance suite later pins a `near:` window for parser-level projection
/// errors, the offending span is available via `stream.current_span()` and
/// the next span via `stream.peek_span()`.
fn projection_parser_error(_expr: &str, _stream: &mut TokenStream, msg: String) -> String {
    format!("Invalid ProjectionExpression: {msg}")
}

/// Apply a projection to an item, returning only the specified attributes.
/// Key attributes are always included.
pub fn apply(
    item: &HashMap<String, AttributeValue>,
    projection: &ProjectionExpr,
    tracker: &TrackedExpressionAttributes,
    key_attrs: &[String],
) -> Result<HashMap<String, AttributeValue>, String> {
    let mut result = HashMap::new();

    // Always include key attributes
    for key_attr in key_attrs {
        if let Some(val) = item.get(key_attr) {
            result.insert(key_attr.clone(), val.clone());
        }
    }

    // Resolve every path first so name refs are concrete before ordering.
    let mut resolved_paths: Vec<Vec<PathElement>> = Vec::with_capacity(projection.paths.len());
    for raw_path in &projection.paths {
        resolved_paths.push(resolve_path_elements(raw_path, tracker)?);
    }

    // DynamoDB returns projected list elements compacted and in ascending index
    // order, independent of the order the indices were requested (e.g. `l[2], l[0]`
    // yields `[l0, l2]`). Sorting the resolved paths index-aware before insertion
    // produces that ordering; dropping identical paths compacts repeated indices.
    // Top-level attributes and nested map keys land in unordered maps, so this
    // reordering is observable only in list element order, which is exactly where
    // AWS parity requires it.
    resolved_paths.sort_by(|a, b| compare_paths(a, b));
    resolved_paths.dedup();

    for resolved in &resolved_paths {
        if let Some(val) = resolve_path(item, resolved) {
            insert_at_path(&mut result, resolved, val);
        }
    }

    Ok(result)
}

/// Order two resolved projection paths so that, within one list, the requested
/// indices sort ascending. Attribute names sort lexicographically (their result
/// map is unordered, so this only groups siblings); index elements sort by value.
fn compare_paths(a: &[PathElement], b: &[PathElement]) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    for (ea, eb) in a.iter().zip(b.iter()) {
        let ord = match (ea, eb) {
            (PathElement::Attribute(x), PathElement::Attribute(y)) => x.cmp(y),
            (PathElement::Index(x), PathElement::Index(y)) => x.cmp(y),
            (PathElement::Attribute(_), PathElement::Index(_)) => Ordering::Less,
            (PathElement::Index(_), PathElement::Attribute(_)) => Ordering::Greater,
        };
        if ord != Ordering::Equal {
            return ord;
        }
    }
    a.len().cmp(&b.len())
}

fn parse_path(stream: &mut TokenStream) -> Result<Vec<PathElement>, String> {
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

/// Create the appropriate default structure for the next path element.
fn default_for_next(next: &PathElement) -> AttributeValue {
    match next {
        PathElement::Attribute(_) => AttributeValue::M(HashMap::new()),
        PathElement::Index(_) => AttributeValue::L(Vec::new()),
    }
}

/// Insert a value at the path location in the result map.
/// For simple top-level attributes, this is a direct insert.
/// For nested paths, we build the necessary intermediate structure.
pub(crate) fn insert_at_path(
    result: &mut HashMap<String, AttributeValue>,
    path: &[PathElement],
    value: AttributeValue,
) {
    if path.is_empty() {
        return;
    }

    if path.len() == 1 {
        if let PathElement::Attribute(name) = &path[0] {
            result.insert(name.clone(), value);
        }
        return;
    }

    // For nested paths, we need the top-level attribute name
    if let PathElement::Attribute(name) = &path[0] {
        let entry = result
            .entry(name.clone())
            .or_insert_with(|| default_for_next(&path[1]));
        insert_nested(entry, &path[1..], value);
    }
}

fn insert_nested(current: &mut AttributeValue, path: &[PathElement], value: AttributeValue) {
    if path.is_empty() {
        return;
    }

    if path.len() == 1 {
        match &path[0] {
            PathElement::Attribute(name) => {
                if let AttributeValue::M(map) = current {
                    map.insert(name.clone(), value);
                }
            }
            PathElement::Index(_) => {
                if let AttributeValue::L(list) = current {
                    list.push(value);
                }
            }
        }
        return;
    }

    match &path[0] {
        PathElement::Attribute(name) => {
            if let AttributeValue::M(map) = current {
                let entry = map
                    .entry(name.clone())
                    .or_insert_with(|| default_for_next(&path[1]));
                insert_nested(entry, &path[1..], value);
            }
        }
        PathElement::Index(_) => {
            if let AttributeValue::L(list) = current {
                // Push a new element for this projected index
                list.push(default_for_next(&path[1]));
                let last = list.last_mut().unwrap();
                insert_nested(last, &path[1..], value);
            }
        }
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

    #[test]
    fn test_parse_simple() {
        let proj = parse("Title, Price, Color").unwrap();
        assert_eq!(proj.paths.len(), 3);
    }

    #[test]
    fn test_parse_nested() {
        let proj = parse("ProductReviews.FiveStar").unwrap();
        assert_eq!(proj.paths[0].len(), 2);
        assert_eq!(
            proj.paths[0][0],
            PathElement::Attribute("ProductReviews".into())
        );
        assert_eq!(proj.paths[0][1], PathElement::Attribute("FiveStar".into()));
    }

    #[test]
    fn test_parse_with_index() {
        let proj = parse("RelatedItems[0]").unwrap();
        assert_eq!(proj.paths[0].len(), 2);
        assert_eq!(proj.paths[0][1], PathElement::Index(0));
    }

    #[test]
    fn test_apply_simple() {
        let proj = parse("label").unwrap();
        let item = make_item(&[
            ("pk", AttributeValue::S("key1".into())),
            ("label", AttributeValue::S("Alice".into())),
            ("age", AttributeValue::N("30".into())),
        ]);
        let no_names = None;
        let no_values = None;
        let tracker = TrackedExpressionAttributes::new(&no_names, &no_values);
        let result = apply(&item, &proj, &tracker, &["pk".to_string()]).unwrap();
        assert!(result.contains_key("pk")); // Always included
        assert!(result.contains_key("label")); // Projected
        assert!(!result.contains_key("age")); // Not projected
    }

    #[test]
    fn test_apply_nested() {
        let mut nested = HashMap::new();
        nested.insert("nested_val".to_string(), AttributeValue::S("value".into()));
        nested.insert("extra".to_string(), AttributeValue::S("skip".into()));

        let proj = parse("payload.nested_val").unwrap();
        let item = make_item(&[
            ("pk", AttributeValue::S("key1".into())),
            ("payload", AttributeValue::M(nested)),
        ]);
        let no_names = None;
        let no_values = None;
        let tracker = TrackedExpressionAttributes::new(&no_names, &no_values);
        let result = apply(&item, &proj, &tracker, &["pk".to_string()]).unwrap();
        assert!(result.contains_key("payload"));
        if let AttributeValue::M(map) = &result["payload"] {
            assert!(map.contains_key("nested_val"));
            assert!(!map.contains_key("extra"));
        } else {
            panic!("Expected map");
        }
    }

    #[test]
    fn test_apply_with_name_refs() {
        let proj = parse("#n").unwrap();
        let item = make_item(&[
            ("pk", AttributeValue::S("key1".into())),
            ("name", AttributeValue::S("Alice".into())),
        ]);
        let names = Some(HashMap::from([("#n".to_string(), "name".to_string())]));
        let no_values = None;
        let tracker = TrackedExpressionAttributes::new(&names, &no_values);
        let result = apply(&item, &proj, &tracker, &["pk".to_string()]).unwrap();
        assert!(result.contains_key("name"));
    }

    #[test]
    fn test_apply_list_indices_compacted_and_ordered() {
        // Real AWS returns projected list elements compacted and in ascending
        // index order regardless of request order: `#l[2], #l[0]` -> [l0, l2].
        let proj = parse("#l[2], #l[0]").unwrap();
        let item = make_item(&[
            ("pk", AttributeValue::S("key1".into())),
            (
                "l",
                AttributeValue::L(vec![
                    AttributeValue::S("l0".into()),
                    AttributeValue::S("l1".into()),
                    AttributeValue::S("l2".into()),
                ]),
            ),
        ]);
        let names = Some(HashMap::from([("#l".to_string(), "l".to_string())]));
        let no_values = None;
        let tracker = TrackedExpressionAttributes::new(&names, &no_values);
        let result = apply(&item, &proj, &tracker, &["pk".to_string()]).unwrap();
        match &result["l"] {
            AttributeValue::L(list) => assert_eq!(
                list,
                &vec![
                    AttributeValue::S("l0".into()),
                    AttributeValue::S("l2".into()),
                ]
            ),
            _ => panic!("expected list"),
        }
    }

    #[test]
    fn test_apply_single_list_index() {
        let proj = parse("#l[1]").unwrap();
        let item = make_item(&[
            ("pk", AttributeValue::S("key1".into())),
            (
                "l",
                AttributeValue::L(vec![
                    AttributeValue::S("l0".into()),
                    AttributeValue::S("l1".into()),
                ]),
            ),
        ]);
        let names = Some(HashMap::from([("#l".to_string(), "l".to_string())]));
        let no_values = None;
        let tracker = TrackedExpressionAttributes::new(&names, &no_values);
        let result = apply(&item, &proj, &tracker, &["pk".to_string()]).unwrap();
        match &result["l"] {
            AttributeValue::L(list) => assert_eq!(list, &vec![AttributeValue::S("l1".into())]),
            _ => panic!("expected list"),
        }
    }

    #[test]
    fn test_apply_missing_attribute() {
        let proj = parse("nonexistent").unwrap();
        let item = make_item(&[("pk", AttributeValue::S("key1".into()))]);
        let no_names = None;
        let no_values = None;
        let tracker = TrackedExpressionAttributes::new(&no_names, &no_values);
        let result = apply(&item, &proj, &tracker, &["pk".to_string()]).unwrap();
        assert!(!result.contains_key("nonexistent"));
        assert!(result.contains_key("pk")); // Key always present
    }
}
