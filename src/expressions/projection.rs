//! ProjectionExpression parsing and application.
//!
//! Parses comma-separated attribute paths, supports dot notation and bracket indexing.
//! Always includes key attributes in the result.

use crate::expressions::tokenizer::{Token, TokenStream, near_window_tokenizer, tokenize};
use crate::expressions::{
    PathElement, TrackedExpressionAttributes, format_path_for_error, resolve_path,
    resolve_path_elements,
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
    // Prefix the size error like every other ProjectionExpression error; real
    // DynamoDB returns "Invalid ProjectionExpression: Expression size has
    // exceeded the maximum allowed size". Confirmed against real DynamoDB
    // (eu-west-2).
    super::check_expression_size(expr).map_err(|e| format!("Invalid ProjectionExpression: {e}"))?;
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

/// Validate a ProjectionExpression before any item is read: reject undefined
/// expression-attribute names and overlapping document paths. Run eagerly, so a
/// Scan/Query/GetItem that matches nothing still rejects.
pub fn validate(
    projection: &ProjectionExpr,
    tracker: &TrackedExpressionAttributes,
) -> Result<(), String> {
    // Resolve first: surfaces undefined names, and lets the overlap check compare
    // resolved names as AWS does.
    let mut resolved: Vec<Vec<PathElement>> = Vec::with_capacity(projection.paths.len());
    for raw_path in &projection.paths {
        let r = resolve_path_elements(raw_path, tracker)
            .map_err(|e| format!("Invalid ProjectionExpression: {e}"))?;
        resolved.push(r);
    }
    check_path_overlaps(&resolved)
}

/// Reject two paths where one is a prefix of the other (a duplicate is the
/// self-prefix case). Reported in expression order, matching AWS.
fn check_path_overlaps(paths: &[Vec<PathElement>]) -> Result<(), String> {
    for i in 0..paths.len() {
        for j in (i + 1)..paths.len() {
            let (a, b) = (&paths[i], &paths[j]);
            let min_len = a.len().min(b.len());
            let common = (0..min_len).take_while(|&k| a[k] == b[k]).count();
            if common == a.len() || common == b.len() {
                return Err(format!(
                    "Invalid ProjectionExpression: Two document paths overlap with each other; \
                     must remove or rewrite one of these paths; path one: {}, path two: {}",
                    format_path_for_error(a),
                    format_path_for_error(b)
                ));
            }
        }
    }
    Ok(())
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
    // order regardless of request order (`l[2], l[0]` yields `[l0, l2]`). Sort and
    // dedup the resolved paths before insertion; only list order is affected,
    // since map keys are unordered.
    resolved_paths.sort_by(|a, b| compare_paths(a, b));
    resolved_paths.dedup();

    // Reconstruct each path in sorted order. Two paths sharing a list index
    // (`l[0].a`, `l[0].b`) must land in one output element, not split across two.
    // Because the paths are sorted, same-index paths are contiguous, so the
    // common-prefix length with the previously inserted path says how deep to
    // reuse existing list elements instead of pushing new ones.
    let mut prev: Option<&[PathElement]> = None;
    for resolved in &resolved_paths {
        if let Some(val) = resolve_path(item, resolved) {
            let common = prev.map_or(0, |p| common_prefix_len(p, resolved));
            insert_at_path_merging(&mut result, resolved, val, common);
            prev = Some(resolved);
        }
    }

    Ok(result)
}

/// Order resolved paths so a list's requested indices sort ascending. Attribute
/// names sort lexicographically, which only groups siblings since result maps
/// are unordered.
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

/// Number of leading path elements two resolved paths share.
fn common_prefix_len(a: &[PathElement], b: &[PathElement]) -> usize {
    a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
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
///
/// Each call is independent: no list element is reused (a common prefix of
/// zero). Projection reconstruction, which coalesces several paths that share a
/// list index, goes through `insert_at_path_merging` instead.
pub(crate) fn insert_at_path(
    result: &mut HashMap<String, AttributeValue>,
    path: &[PathElement],
    value: AttributeValue,
) {
    insert_at_path_merging(result, path, value, 0);
}

/// Insert a value at the path location, reusing already-built list elements
/// within the first `common` path elements. `common` is the length of the
/// prefix this path shares with the previously inserted path; because paths are
/// inserted in sorted order, a shared prefix means the previous path already
/// created the list element for each index inside it, so this path merges into
/// the same element rather than pushing a fresh one.
fn insert_at_path_merging(
    result: &mut HashMap<String, AttributeValue>,
    path: &[PathElement],
    value: AttributeValue,
    common: usize,
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
        insert_nested(entry, &path[1..], value, common, 1);
    }
}

/// Reconstruct `path` into `current`, merging into the last list element when
/// this position falls inside the shared prefix with the previously inserted
/// path (`depth < common`). `depth` is the index of `path[0]` in the full path.
fn insert_nested(
    current: &mut AttributeValue,
    path: &[PathElement],
    value: AttributeValue,
    common: usize,
    depth: usize,
) {
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
                insert_nested(entry, &path[1..], value, common, depth + 1);
            }
        }
        PathElement::Index(_) => {
            if let AttributeValue::L(list) = current {
                // Inside the shared prefix, the previously inserted path already
                // built the element for this source index (paths are sorted, so
                // same-index paths are adjacent and ascending), so merge into the
                // last element. Otherwise start a new one.
                let reuse_last = depth < common && !list.is_empty();
                if !reuse_last {
                    list.push(default_for_next(&path[1]));
                }
                let last = list.last_mut().unwrap();
                insert_nested(last, &path[1..], value, common, depth + 1);
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
    fn validate_rejects_overlapping_paths() {
        let proj = parse("#a, #a.#b").unwrap();
        let names = Some(HashMap::from([
            ("#a".to_string(), "a".to_string()),
            ("#b".to_string(), "b".to_string()),
        ]));
        let no_values = None;
        let tracker = TrackedExpressionAttributes::new(&names, &no_values);
        let err = validate(&proj, &tracker).unwrap_err();
        assert_eq!(
            err,
            "Invalid ProjectionExpression: Two document paths overlap with each other; must remove or rewrite one of these paths; path one: [a], path two: [a, b]"
        );
    }

    #[test]
    fn validate_rejects_duplicate_paths() {
        let proj = parse("#a, #a").unwrap();
        let names = Some(HashMap::from([("#a".to_string(), "a".to_string())]));
        let no_values = None;
        let tracker = TrackedExpressionAttributes::new(&names, &no_values);
        let err = validate(&proj, &tracker).unwrap_err();
        assert!(err.contains("Two document paths overlap"), "got: {err}");
        assert!(err.ends_with("path one: [a], path two: [a]"), "got: {err}");
    }

    #[test]
    fn validate_reports_overlap_in_expression_order() {
        // Overlapping pair is the 2nd and 3rd path; reported in expression order.
        let proj = parse("#x, #a, #a.#b").unwrap();
        let names = Some(HashMap::from([
            ("#x".to_string(), "x".to_string()),
            ("#a".to_string(), "a".to_string()),
            ("#b".to_string(), "b".to_string()),
        ]));
        let no_values = None;
        let tracker = TrackedExpressionAttributes::new(&names, &no_values);
        let err = validate(&proj, &tracker).unwrap_err();
        assert!(
            err.ends_with("path one: [a], path two: [a, b]"),
            "got: {err}"
        );
    }

    #[test]
    fn validate_rejects_list_attr_and_its_index() {
        let proj = parse("#l, #l[0]").unwrap();
        let names = Some(HashMap::from([("#l".to_string(), "l".to_string())]));
        let no_values = None;
        let tracker = TrackedExpressionAttributes::new(&names, &no_values);
        assert!(validate(&proj, &tracker).is_err());
    }

    #[test]
    fn validate_accepts_sibling_list_indices() {
        let proj = parse("#l[0], #l[1]").unwrap();
        let names = Some(HashMap::from([("#l".to_string(), "l".to_string())]));
        let no_values = None;
        let tracker = TrackedExpressionAttributes::new(&names, &no_values);
        assert!(validate(&proj, &tracker).is_ok());
    }

    #[test]
    fn validate_accepts_sibling_paths() {
        let proj = parse("#a.#b, #a.#c").unwrap();
        let names = Some(HashMap::from([
            ("#a".to_string(), "a".to_string()),
            ("#b".to_string(), "b".to_string()),
            ("#c".to_string(), "c".to_string()),
        ]));
        let no_values = None;
        let tracker = TrackedExpressionAttributes::new(&names, &no_values);
        assert!(validate(&proj, &tracker).is_ok());
    }

    #[test]
    fn validate_rejects_undefined_name() {
        let proj = parse("#undef").unwrap();
        let no_names = None;
        let no_values = None;
        let tracker = TrackedExpressionAttributes::new(&no_names, &no_values);
        let err = validate(&proj, &tracker).unwrap_err();
        assert_eq!(
            err,
            "Invalid ProjectionExpression: An expression attribute name used in the document path is not defined; attribute name: #undef"
        );
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

    // ---------------------------------------------------------------------------
    // #126: paths sharing a list index merge into one reconstructed element.
    // ---------------------------------------------------------------------------

    /// A tracker whose names cover every segment used by the merge fixtures.
    /// `apply` ignores names it does not resolve, so one shared map is fine.
    fn merge_names() -> Option<HashMap<String, String>> {
        Some(HashMap::from([
            ("#l".to_string(), "l".to_string()),
            ("#a".to_string(), "a".to_string()),
            ("#b".to_string(), "b".to_string()),
            ("#c".to_string(), "c".to_string()),
            ("#m".to_string(), "m".to_string()),
            ("#n".to_string(), "n".to_string()),
            ("#p".to_string(), "p".to_string()),
            ("#q".to_string(), "q".to_string()),
            ("#x".to_string(), "x".to_string()),
            ("#y".to_string(), "y".to_string()),
        ]))
    }

    /// Item whose list index 0 carries scalars, a nested map, and a nested list,
    /// so same-index merges can be probed at depth. Indices 1 and 2 give a
    /// distinct-index element to compact against.
    fn merge_fixture_item() -> HashMap<String, AttributeValue> {
        let map = |pairs: &[(&str, &str)]| {
            AttributeValue::M(
                pairs
                    .iter()
                    .map(|(k, v)| (k.to_string(), AttributeValue::S((*v).into())))
                    .collect(),
            )
        };
        let elem0 = AttributeValue::M(HashMap::from([
            ("a".to_string(), AttributeValue::S("a0".into())),
            ("b".to_string(), AttributeValue::S("b0".into())),
            ("m".to_string(), map(&[("x", "x0"), ("y", "y0")])),
            (
                "n".to_string(),
                AttributeValue::L(vec![
                    map(&[("p", "p00"), ("q", "q00")]),
                    map(&[("p", "p01"), ("q", "q01")]),
                ]),
            ),
        ]));
        make_item(&[
            ("pk", AttributeValue::S("k".into())),
            (
                "l",
                AttributeValue::L(vec![
                    elem0,
                    map(&[("a", "a1"), ("b", "b1")]),
                    map(&[("a", "a2"), ("b", "b2"), ("c", "c2")]),
                ]),
            ),
        ])
    }

    fn apply_merge(expr: &str) -> HashMap<String, AttributeValue> {
        let names = merge_names();
        let no_values = None;
        let tracker = TrackedExpressionAttributes::new(&names, &no_values);
        let proj = parse(expr).unwrap();
        apply(&merge_fixture_item(), &proj, &tracker, &["pk".to_string()]).unwrap()
    }

    fn expect_list(item: &HashMap<String, AttributeValue>, key: &str) -> Vec<AttributeValue> {
        as_list(&item[key])
    }

    fn as_list(value: &AttributeValue) -> Vec<AttributeValue> {
        match value {
            AttributeValue::L(l) => l.clone(),
            other => panic!("expected list, got {other:?}"),
        }
    }

    fn expect_map(value: &AttributeValue) -> HashMap<String, AttributeValue> {
        match value {
            AttributeValue::M(m) => m.clone(),
            other => panic!("expected map, got {other:?}"),
        }
    }

    #[test]
    fn test_apply_same_list_index_merges_scalar_siblings() {
        // #126 core case: `l[0].a, l[0].b` merges into one element, not two.
        let result = apply_merge("#l[0].#a, #l[0].#b");
        let list = expect_list(&result, "l");
        assert_eq!(list.len(), 1, "same index must merge into one element");
        let m = expect_map(&list[0]);
        assert_eq!(m.get("a"), Some(&AttributeValue::S("a0".into())));
        assert_eq!(m.get("b"), Some(&AttributeValue::S("b0".into())));
        assert!(!m.contains_key("c"), "unprojected sibling is dropped");
    }

    #[test]
    fn test_apply_same_list_index_merges_nested_map() {
        // Depth case: `l[0].m.x, l[0].m.y` shares the nested map under index 0.
        let result = apply_merge("#l[0].#m.#x, #l[0].#m.#y");
        let list = expect_list(&result, "l");
        assert_eq!(list.len(), 1);
        let inner = expect_map(&expect_map(&list[0])["m"]);
        assert_eq!(inner.get("x"), Some(&AttributeValue::S("x0".into())));
        assert_eq!(inner.get("y"), Some(&AttributeValue::S("y0".into())));
    }

    #[test]
    fn test_apply_same_list_index_merges_scalar_and_nested_siblings() {
        // Mixed shape: a scalar and a nested-map sibling under one index merge.
        let result = apply_merge("#l[0].#a, #l[0].#m.#x");
        let list = expect_list(&result, "l");
        assert_eq!(list.len(), 1);
        let m = expect_map(&list[0]);
        assert_eq!(m.get("a"), Some(&AttributeValue::S("a0".into())));
        let inner = expect_map(&m["m"]);
        assert_eq!(inner.get("x"), Some(&AttributeValue::S("x0".into())));
        assert!(!inner.contains_key("y"));
    }

    #[test]
    fn test_apply_same_and_distinct_list_index_compacts() {
        // `l[0].a, l[0].b, l[2].c`: index 0 merged, index 2 separate, compacted.
        let result = apply_merge("#l[0].#a, #l[0].#b, #l[2].#c");
        let list = expect_list(&result, "l");
        assert_eq!(list.len(), 2, "merge on index 0, keep index 2, compact");
        let e0 = expect_map(&list[0]);
        assert_eq!(e0.get("a"), Some(&AttributeValue::S("a0".into())));
        assert_eq!(e0.get("b"), Some(&AttributeValue::S("b0".into())));
        let e1 = expect_map(&list[1]);
        assert_eq!(e1.get("c"), Some(&AttributeValue::S("c2".into())));
        assert!(!e1.contains_key("a"));
    }

    #[test]
    fn test_apply_same_list_index_merges_nested_list_element() {
        // Nested list: `l[0].n[0].p, l[0].n[0].q` shares inner index 0 too.
        let result = apply_merge("#l[0].#n[0].#p, #l[0].#n[0].#q");
        let list = expect_list(&result, "l");
        assert_eq!(list.len(), 1);
        let n = as_list(&expect_map(&list[0])["n"]);
        assert_eq!(n.len(), 1, "nested list element merged");
        let n0 = expect_map(&n[0]);
        assert_eq!(n0.get("p"), Some(&AttributeValue::S("p00".into())));
        assert_eq!(n0.get("q"), Some(&AttributeValue::S("q00".into())));
    }

    #[test]
    fn test_apply_distinct_inner_list_indices_stay_separate() {
        // `l[0].n[0].p, l[0].n[1].q`: outer index shared, inner indices distinct.
        let result = apply_merge("#l[0].#n[0].#p, #l[0].#n[1].#q");
        let list = expect_list(&result, "l");
        assert_eq!(list.len(), 1);
        let n = as_list(&expect_map(&list[0])["n"]);
        assert_eq!(
            n.len(),
            2,
            "distinct inner indices stay separate, compacted"
        );
        let n0 = expect_map(&n[0]);
        assert_eq!(n0.get("p"), Some(&AttributeValue::S("p00".into())));
        assert!(!n0.contains_key("q"));
        let n1 = expect_map(&n[1]);
        assert_eq!(n1.get("q"), Some(&AttributeValue::S("q01".into())));
        assert!(!n1.contains_key("p"));
    }
}
