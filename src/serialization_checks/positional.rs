//! The forms DynamoDB reads by position, rewritten to the object form they
//! stand for before any other check reads the request.
//!
//! DynamoDB refuses a JSON array wherever it expects an object, with two
//! exceptions it reads by position (captured eu-west-2 and us-east-1, 11 and 12
//! November 2026):
//!
//! - a BatchGetItem or BatchWriteItem body of `[RequestItems]` or
//!   `[RequestItems, ReturnConsumedCapacity]`;
//! - in BatchWriteItem, a `PutRequest` given as `[Item]` and a
//!   `DeleteRequest` given as `[Key]`;
//! - on Query and Scan, a `Condition` in `KeyConditions`, `QueryFilter` or
//!   `ScanFilter` given as `[AttributeValueList, ComparisonOperator]`.
//!
//! BatchWriteItem's forms are read this way in eu-west-2 and refused in
//! us-east-1; Dynoxide follows eu-west-2. Each is rewritten here so the object form's checks and wording apply to it.
//! Any other array in those places, or an element of the wrong JSON type, is
//! left alone for the type checks to refuse with no message. The rewrite
//! splices the request's own text: a parsed value is never written back out,
//! since that would reorder members and change how numbers are written.

/// `body` with each positional form rewritten to its object form, or `None`
/// when it holds none and is read as it is. `body` is valid JSON.
pub(super) fn rewrite(operation: &str, body: &str) -> Option<String> {
    let bytes = body.as_bytes();
    let start = skip_ws(bytes, 0);
    match operation {
        "BatchGetItem" => batch_body(body, start),
        "BatchWriteItem" => match batch_body(body, start) {
            Some(rewritten) => {
                let start = skip_ws(rewritten.as_bytes(), 0);
                Some(write_requests(&rewritten, start).unwrap_or(rewritten))
            }
            None => write_requests(body, start),
        },
        "Query" => conditions(body, start, &["KeyConditions", "QueryFilter"]),
        "Scan" => conditions(body, start, &["ScanFilter"]),
        _ => None,
    }
}

/// A BatchGetItem or BatchWriteItem body of one or two elements, the first an
/// object and the second a string or `null` (which leaves
/// `ReturnConsumedCapacity` unset), as the object form it stands for. Only
/// those two positions were captured, so a third element leaves the body to be
/// refused as an array.
fn batch_body(body: &str, start: usize) -> Option<String> {
    let elements = elements(body, start)?;
    let (first, second) = match elements.as_slice() {
        [first] => (*first, None),
        [first, second] => (*first, Some(*second)),
        _ => return None,
    };
    if !starts_with(body, first, b'{') {
        return None;
    }
    let mut out = format!("{{\"RequestItems\":{}", &body[first.0..first.1]);
    match second {
        None => {}
        Some(second) if is_null(body, second) => {}
        Some(second) if starts_with(body, second, b'"') => {
            out.push_str(",\"ReturnConsumedCapacity\":");
            out.push_str(&body[second.0..second.1]);
        }
        Some(_) => return None,
    }
    out.push('}');
    Some(out)
}

/// `body` with each BatchWriteItem `PutRequest` given as `[Item]` and each
/// `DeleteRequest` given as `[Key]` rewritten to the object form. Only a
/// one-element array holding an object is read this way; any other array is
/// left to be refused, as is a write request itself given as an array.
fn write_requests(body: &str, start: usize) -> Option<String> {
    let mut replacements = Vec::new();
    for (key, tables) in object_members(body, start)? {
        if key != "RequestItems" || !starts_with(body, tables, b'{') {
            continue;
        }
        for (_, requests) in object_members(body, tables.0)? {
            if !starts_with(body, requests, b'[') {
                continue;
            }
            for request in elements(body, requests.0)? {
                if !starts_with(body, request, b'{') {
                    continue;
                }
                for (action, value) in object_members(body, request.0)? {
                    let member = match action.as_str() {
                        "PutRequest" => "Item",
                        "DeleteRequest" => "Key",
                        _ => continue,
                    };
                    if !starts_with(body, value, b'[') {
                        continue;
                    }
                    let [only] = elements(body, value.0)?[..] else {
                        continue;
                    };
                    if starts_with(body, only, b'{') {
                        replacements
                            .push((value, format!("{{\"{member}\":{}}}", &body[only.0..only.1])));
                    }
                }
            }
        }
    }
    splice(body, replacements)
}

/// `body` with each `Condition` written by position under one of `members`
/// rewritten to the object form. The first element must be a list or `null`
/// (no `AttributeValueList`) and the second a string.
fn conditions(body: &str, start: usize, members: &[&str]) -> Option<String> {
    let mut replacements = Vec::new();
    for (key, map) in object_members(body, start)? {
        if !members.contains(&key.as_str()) || !starts_with(body, map, b'{') {
            continue;
        }
        for (_, condition) in object_members(body, map.0)? {
            if !starts_with(body, condition, b'[') {
                continue;
            }
            let [list, operator] = elements(body, condition.0)?[..] else {
                continue;
            };
            let list_ok = starts_with(body, list, b'[') || is_null(body, list);
            if !list_ok || !starts_with(body, operator, b'"') {
                continue;
            }
            replacements.push((
                condition,
                format!(
                    "{{\"AttributeValueList\":{},\"ComparisonOperator\":{}}}",
                    &body[list.0..list.1],
                    &body[operator.0..operator.1]
                ),
            ));
        }
    }
    splice(body, replacements)
}

/// `body` with each span replaced by its text, or `None` when there are none.
/// The spans are in order and do not overlap.
fn splice(body: &str, replacements: Vec<(Span, String)>) -> Option<String> {
    if replacements.is_empty() {
        return None;
    }
    let mut out = String::with_capacity(body.len() + 64 * replacements.len());
    let mut copied = 0;
    for ((from, to), text) in replacements {
        out.push_str(&body[copied..from]);
        out.push_str(&text);
        copied = to;
    }
    out.push_str(&body[copied..]);
    Some(out)
}

/// A value's span in the body: its first byte and the byte after its last.
type Span = (usize, usize);

fn starts_with(body: &str, span: Span, byte: u8) -> bool {
    body.as_bytes().get(span.0) == Some(&byte)
}

fn is_null(body: &str, span: Span) -> bool {
    &body[span.0..span.1] == "null"
}

fn skip_ws(bytes: &[u8], mut i: usize) -> usize {
    while i < bytes.len() && matches!(bytes[i], b' ' | b'\t' | b'\n' | b'\r') {
        i += 1;
    }
    i
}

/// The end of the string whose opening quote is at `i`.
fn string_end(bytes: &[u8], mut i: usize) -> Option<usize> {
    i += 1;
    while i < bytes.len() {
        match bytes[i] {
            b'\\' => i += 2,
            b'"' => return Some(i + 1),
            _ => i += 1,
        }
    }
    None
}

/// The end of the value starting at `i`.
fn value_end(bytes: &[u8], i: usize) -> Option<usize> {
    match *bytes.get(i)? {
        b'"' => string_end(bytes, i),
        b'{' | b'[' => {
            let mut depth = 0usize;
            let mut j = i;
            while j < bytes.len() {
                match bytes[j] {
                    b'"' => {
                        j = string_end(bytes, j)?;
                        continue;
                    }
                    b'{' | b'[' => depth += 1,
                    b'}' | b']' => {
                        depth -= 1;
                        if depth == 0 {
                            return Some(j + 1);
                        }
                    }
                    _ => {}
                }
                j += 1;
            }
            None
        }
        _ => {
            let mut j = i;
            while j < bytes.len()
                && !matches!(bytes[j], b',' | b']' | b'}' | b' ' | b'\t' | b'\n' | b'\r')
            {
                j += 1;
            }
            Some(j)
        }
    }
}

/// The spans of the elements of the array whose `[` is at `start`, or `None`
/// when there is no array there.
fn elements(body: &str, start: usize) -> Option<Vec<Span>> {
    let bytes = body.as_bytes();
    if bytes.get(start) != Some(&b'[') {
        return None;
    }
    let mut spans = Vec::new();
    let mut i = skip_ws(bytes, start + 1);
    if bytes.get(i) == Some(&b']') {
        return Some(spans);
    }
    loop {
        let end = value_end(bytes, i)?;
        spans.push((i, end));
        i = skip_ws(bytes, end);
        match *bytes.get(i)? {
            b',' => i = skip_ws(bytes, i + 1),
            b']' => return Some(spans),
            _ => return None,
        }
    }
}

/// The members of the object whose `{` is at `start`, as each key and the span
/// of its value, or `None` when there is no object there.
fn object_members(body: &str, start: usize) -> Option<Vec<(String, Span)>> {
    let bytes = body.as_bytes();
    if bytes.get(start) != Some(&b'{') {
        return None;
    }
    let mut members = Vec::new();
    let mut i = skip_ws(bytes, start + 1);
    if bytes.get(i) == Some(&b'}') {
        return Some(members);
    }
    loop {
        let key_end = string_end(bytes, i)?;
        let key: String = serde_json::from_str(&body[i..key_end]).ok()?;
        i = skip_ws(bytes, key_end);
        if bytes.get(i) != Some(&b':') {
            return None;
        }
        i = skip_ws(bytes, i + 1);
        let end = value_end(bytes, i)?;
        members.push((key, (i, end)));
        i = skip_ws(bytes, end);
        match *bytes.get(i)? {
            b',' => i = skip_ws(bytes, i + 1),
            b'}' => return Some(members),
            _ => return None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::rewrite;

    #[test]
    fn a_body_with_no_positional_form_is_left_alone() {
        for (operation, body) in [
            ("BatchGetItem", r#"{"RequestItems":{"t":{"Keys":[]}}}"#),
            ("BatchGetItem", r#"[]"#),
            ("BatchGetItem", r#"[{}, "TOTAL", "x"]"#),
            ("BatchGetItem", r#"[{}, 5]"#),
            ("BatchGetItem", r#"[null]"#),
            (
                "Query",
                r#"{"KeyConditions":{"pk":{"ComparisonOperator":"EQ"}}}"#,
            ),
            ("Query", r#"{"KeyConditions":{"pk":["EQ",[]]}}"#),
            ("Query", r#"{"KeyConditions":{"pk":[[],"EQ","x"]}}"#),
            ("Query", r#"{"KeyConditions":{"pk":[[],null]}}"#),
            ("Query", r#"{"ScanFilter":{"pk":[[],"EQ"]}}"#),
            ("Scan", r#"{"QueryFilter":{"pk":[[],"EQ"]}}"#),
            ("PutItem", r#"["t", {"pk": {"S": "a"}}]"#),
            ("BatchWriteItem", r#"[]"#),
            ("BatchWriteItem", r#"[{}, "TOTAL", "x"]"#),
            (
                "BatchWriteItem",
                r#"{"RequestItems":{"t":[[{"Item":{}}]]}}"#,
            ),
            (
                "BatchWriteItem",
                r#"{"RequestItems":{"t":[{"PutRequest":[]}]}}"#,
            ),
            (
                "BatchWriteItem",
                r#"{"RequestItems":{"t":[{"PutRequest":[{}, {}]}]}}"#,
            ),
            (
                "BatchWriteItem",
                r#"{"RequestItems":{"t":[{"DeleteRequest":["x"]}]}}"#,
            ),
            (
                "BatchGetItem",
                r#"{"RequestItems":{"t":[{"PutRequest":[{}]}]}}"#,
            ),
        ] {
            assert_eq!(rewrite(operation, body), None, "{operation} {body}");
        }
    }

    #[test]
    fn a_batch_get_item_body_by_position_is_read_as_its_object_form() {
        assert_eq!(
            rewrite(
                "BatchGetItem",
                r#" [ {"t": {"Keys": [{"pk": {"N": "1.50"}}]}} , "TOTAL" ] "#
            )
            .as_deref(),
            Some(
                r#"{"RequestItems":{"t": {"Keys": [{"pk": {"N": "1.50"}}]}},"ReturnConsumedCapacity":"TOTAL"}"#
            )
        );
        assert_eq!(
            rewrite("BatchGetItem", r#"[{"t":{}}, null]"#).as_deref(),
            Some(r#"{"RequestItems":{"t":{}}}"#)
        );
    }

    #[test]
    fn batch_write_item_forms_by_position_are_read_as_their_object_forms() {
        assert_eq!(
            rewrite(
                "BatchWriteItem",
                r#"[{"t":[{"PutRequest":[{"pk":{"S":"b"}}]},{"DeleteRequest":[{"pk":{"S":"a"}}]}]},"TOTAL"]"#
            )
            .as_deref(),
            Some(
                r#"{"RequestItems":{"t":[{"PutRequest":{"Item":{"pk":{"S":"b"}}}},{"DeleteRequest":{"Key":{"pk":{"S":"a"}}}}]},"ReturnConsumedCapacity":"TOTAL"}"#
            )
        );
        assert_eq!(
            rewrite("BatchWriteItem", r#"[{}]"#).as_deref(),
            Some(r#"{"RequestItems":{}}"#)
        );
    }

    #[test]
    fn a_condition_by_position_is_spliced_into_the_request_as_written() {
        let body = r#"{"TableName":"t","KeyConditions":{"pk":{"AttributeValueList":[{"S":"a"}],"ComparisonOperator":"EQ"}},"QueryFilter":{"x":[[{"N":1e400}], "EQ"],"y\"]":[null,"NULL"]},"Limit":1}"#;
        assert_eq!(
            rewrite("Query", body).as_deref(),
            Some(
                r#"{"TableName":"t","KeyConditions":{"pk":{"AttributeValueList":[{"S":"a"}],"ComparisonOperator":"EQ"}},"QueryFilter":{"x":{"AttributeValueList":[{"N":1e400}],"ComparisonOperator":"EQ"},"y\"]":{"AttributeValueList":null,"ComparisonOperator":"NULL"}},"Limit":1}"#
            )
        );
    }
}
