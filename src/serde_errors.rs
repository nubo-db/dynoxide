//! Serde wrappers that map (de)serialisation failures to DynamoDB errors.
//!
//! serde_json reports failures like "invalid type: integer `23`, expected a
//! string at line 1 column 42"; DynamoDB reports "NUMBER_VALUE cannot be
//! converted to String". The helpers here translate the former into the
//! latter, choosing between ValidationException and SerializationException
//! the way DynamoDB does.
//!
//! Custom `Deserialize` impls signal the DynamoDB error class with a marker
//! prefix on the serde message:
//!
//! - `VALIDATION:` maps to a bare `ValidationException`.
//! - `VALIDATION_REQUEST:` maps to the request-validation class that PutItem
//!   and UpdateItem wrap in the `1 validation error detected: ` envelope; it
//!   becomes the wire-invisible `DynoxideError::EnvelopedValidation`, which
//!   the server dispatch envelopes on those two operations and untags on
//!   every other one.
//! - `SERIALIZATION:` maps to a `SerializationException` carrying the rest of
//!   the message as DynamoDB words it, for a value the decoder has already
//!   classified (an empty rest is a `SerializationException` with no message).
//!
//! No marker may ever reach the wire. Callers that decode raw serde
//! messages by hand go through [`clean_serde_message`], which strips both
//! markers along with serde_json's position suffix.

// The request-body decoding path serves the HTTP server, the wasm engine API
// and the MCP tools, and the legacy `Expected` reader in every build, so a
// library build refuses an unreadable condition in the same words.
pub(crate) fn deserialize<T: serde::de::DeserializeOwned>(body: &str) -> crate::Result<T> {
    serde_json::from_str(body).map_err(|e| {
        let msg = e.to_string();
        // Marker prefixes from our Deserialize impls select the DynamoDB
        // error class (see the module doc); serde_json appends
        // " at line N column N" to custom errors, so strip that too.
        if let Some(classified) = classify_marked_serde_error(&msg) {
            return classified;
        }
        // DynamoDB returns ValidationException for missing required fields,
        // null values, and unrecognised enum variants. Only true JSON type
        // mismatches (e.g. number where string is expected) produce a
        // SerializationException.
        if msg.contains("missing field")
            || msg.contains("unknown variant")
            || msg.contains("invalid type: null")
        {
            crate::DynoxideError::ValidationException(msg)
        } else if msg.contains("empty AttributeValue") {
            crate::DynoxideError::ValidationException(
                "Supplied AttributeValue is empty, must contain exactly one of the supported datatypes".to_string(),
            )
        } else if msg.contains("Supplied AttributeValue") {
            // Multi-datatype or empty AV error: strip position info and return as-is
            crate::DynoxideError::ValidationException(strip_position(&msg).to_string())
        } else {
            crate::DynoxideError::SerializationException(map_serde_to_dynamodb_message(&msg, body))
        }
    })
}

/// Classify a raw serde error message by its marker prefix, stripping the
/// marker and serde_json's position suffix.
///
/// `VALIDATION_REQUEST:` becomes the wire-invisible
/// [`EnvelopedValidation`](crate::DynoxideError::EnvelopedValidation) tag,
/// `VALIDATION:` a bare `ValidationException` and `SERIALIZATION:` a
/// `SerializationException`, as does `SERIALIZATION_AS_READ:` with the
/// position left on; an unmarked message returns
/// `None` for the caller to classify. This is the single owner of the
/// marker-to-variant mapping, shared by [`deserialize`] and callers that
/// decode via `serde_json::from_value` (the MCP surface).
pub(crate) fn classify_marked_serde_error(msg: &str) -> Option<crate::DynoxideError> {
    if let Some(as_read) = msg.strip_prefix(SERIALIZATION_AS_READ_MARKER) {
        return Some(crate::DynoxideError::SerializationException(
            as_read.to_string(),
        ));
    }
    if let Some(stripped) = msg.strip_prefix(SERIALIZATION_MARKER) {
        return Some(crate::DynoxideError::SerializationException(
            strip_position(stripped).to_string(),
        ));
    }
    if let Some(stripped) = msg.strip_prefix(REQUEST_VALIDATION_MARKER) {
        return Some(crate::DynoxideError::EnvelopedValidation(
            strip_position(stripped).to_string(),
        ));
    }
    if let Some(stripped) = msg.strip_prefix(VALIDATION_MARKER) {
        return Some(crate::DynoxideError::ValidationException(
            strip_position(stripped).to_string(),
        ));
    }
    None
}

/// The `SerializationException` a value the decoder has already classified as
/// malformed fails its request with, as it is read and before any key is
/// named; `None` for any other message.
pub(crate) fn marked_serialization_error(msg: &str) -> Option<crate::DynoxideError> {
    msg.strip_prefix(SERIALIZATION_MARKER).map(|stripped| {
        crate::DynoxideError::SerializationException(strip_position(stripped).to_string())
    })
}

/// DynamoDB's refusal of a NULL that isn't true.
pub(crate) const NULL_NOT_TRUE: &str = "One or more parameter values were invalid: Null attribute value types must have the value \
     of true";

/// DynamoDB's refusal of a value with no type set.
pub(crate) const EMPTY_VALUE: &str =
    "Supplied AttributeValue is empty, must contain exactly one of the supported datatypes";

/// DynamoDB's refusal of a value with more than one type set.
pub(crate) const MORE_THAN_ONE_TYPE: &str = "Supplied AttributeValue has more than one datatypes \
                                             set, must contain exactly one of the supported \
                                             datatypes";

/// What a value refused as it was read was refused for, as the readers of
/// `ExpressionAttributeValues` tell the cases apart to decide which name the
/// value's key.
pub(crate) enum ValueRefusal<'a> {
    /// No type set, or not an object.
    Empty,
    /// More than one type set.
    MoreThanOneType,
    /// An empty set beside another member, refused as the value is read.
    EmptySet(&'a str),
    /// A number the value layer can't store, in the layer's words.
    Number(&'a str),
    /// A NULL refused as it was parsed: a string that reads as false, or false
    /// beside another member.
    ParsedNull(&'a str),
    /// `{"NULL": false}` on its own, refused by the request's validation.
    RequestNull(&'a str),
    /// Anything else, passed through as it is.
    Other,
}

/// Which refusal a raw serde message from reading one value is.
pub(crate) fn value_refusal(msg: &str) -> ValueRefusal<'_> {
    let inner = clean_serde_message(msg);
    if inner.contains("empty AttributeValue") || inner.contains("Supplied AttributeValue is empty")
    {
        ValueRefusal::Empty
    } else if inner.contains("more than one datatypes") {
        ValueRefusal::MoreThanOneType
    } else if inner.contains("set  may not be empty") || inner.contains("sets should not be empty")
    {
        ValueRefusal::EmptySet(inner)
    } else if inner.contains("cannot be converted to a numeric value")
        || inner.contains("significant digits")
        || inner.contains("Number overflow")
        || inner.contains("Number underflow")
    {
        ValueRefusal::Number(inner)
    } else if inner.contains(NULL_NOT_TRUE) {
        if msg.starts_with(REQUEST_VALIDATION_MARKER) {
            ValueRefusal::RequestNull(inner)
        } else {
            ValueRefusal::ParsedNull(inner)
        }
    } else {
        ValueRefusal::Other
    }
}

/// A value's refusal naming its key, as DynamoDB words it.
pub(crate) fn named_value_refusal(refusal: &str, key: &str) -> String {
    format!("ExpressionAttributeValues contains invalid value: {refusal} for key {key}")
}

/// Marker prefix for validation rejections raised inside serde
/// deserialisation, mapped to a bare `ValidationException`.
pub(crate) const VALIDATION_MARKER: &str = "VALIDATION:";

/// Marker prefix for a malformed value classified inside serde
/// deserialisation, mapped to a `SerializationException` whose message is the
/// rest of the text.
pub(crate) const SERIALIZATION_MARKER: &str = "SERIALIZATION:";

/// Marker prefix for a deserialiser error passed to the wire as it was raised,
/// mapped to a `SerializationException` that keeps the message whole,
/// serde_json's position suffix included. The suffix belongs to the inner
/// error, since serde_json adds none to a custom error raised at the top level.
pub(crate) const SERIALIZATION_AS_READ_MARKER: &str = "SERIALIZATION_AS_READ:";

/// Marker prefix for the request-validation class raised inside serde
/// deserialisation, mapped to the wire-invisible `EnvelopedValidation` tag
/// that PutItem and UpdateItem envelope. Must be checked before
/// [`VALIDATION_MARKER`], whose spelling it extends.
pub(crate) const REQUEST_VALIDATION_MARKER: &str = "VALIDATION_REQUEST:";

/// The members a request reads as booleans, whose numbers
/// [`boolean_numbers_as_written`] looks at.
#[cfg(any(feature = "http-server", feature = "wasm-sqlite", test))]
const BOOLEAN_MEMBERS: [&str; 5] = [
    "BOOL",
    "NULL",
    "ConsistentRead",
    "ScanIndexForward",
    "Exists",
];

/// A request body with each whole number serde_json would read as a float,
/// `-0` and an integer past 64 bits, written as `0` where it is the value of a
/// member read as a boolean (`BOOL`, `NULL`, `ConsistentRead`,
/// `ScanIndexForward`, `Exists`).
///
/// DynamoDB names a number given for a boolean by how it is written: a whole
/// number of any size, `-0` included, is `NUMBER_VALUE`, and one with a point
/// or an exponent `DECIMAL_VALUE` (captured eu-west-2 and us-east-1,
/// 2026-10-10). serde_json keeps no text for a number and reads those two as
/// floats, which would name them `DECIMAL_VALUE`; as `0` they read as the
/// whole numbers they were written as. A number in one of these members fails
/// the request whatever its value, so nothing else reads it. The body comes
/// back as it is when there is nothing to rewrite.
#[cfg(any(feature = "http-server", feature = "wasm-sqlite", test))]
pub(crate) fn boolean_numbers_as_written(body: &str) -> std::borrow::Cow<'_, str> {
    let bytes = body.as_bytes();
    let mut rewrites: Vec<(usize, usize)> = Vec::new();
    // The key whose value comes next, once its colon has been read.
    let mut key: Option<&str> = None;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'"' => {
                let start = i + 1;
                i = start;
                while i < bytes.len() && bytes[i] != b'"' {
                    if bytes[i] == b'\\' {
                        i += 1;
                    }
                    i += 1;
                }
                let text = &body[start..i.min(bytes.len())];
                i += 1;
                // A string followed by a colon is a key.
                let mut next = i;
                while next < bytes.len() && bytes[next].is_ascii_whitespace() {
                    next += 1;
                }
                key = None;
                if next < bytes.len() && bytes[next] == b':' {
                    key = Some(text);
                    i = next + 1;
                }
                continue;
            }
            b'-' | b'0'..=b'9' => {
                let start = i;
                while i < bytes.len()
                    && matches!(bytes[i], b'-' | b'+' | b'.' | b'e' | b'E' | b'0'..=b'9')
                {
                    i += 1;
                }
                if key.is_some_and(|key| BOOLEAN_MEMBERS.contains(&key))
                    && whole_number_read_as_float(&body[start..i])
                {
                    rewrites.push((start, i));
                }
                key = None;
                continue;
            }
            byte if byte.is_ascii_whitespace() => {}
            _ => key = None,
        }
        i += 1;
    }
    if rewrites.is_empty() {
        return std::borrow::Cow::Borrowed(body);
    }
    let mut rewritten = String::with_capacity(body.len());
    let mut copied = 0;
    for (start, end) in rewrites {
        rewritten.push_str(&body[copied..start]);
        rewritten.push('0');
        copied = end;
    }
    rewritten.push_str(&body[copied..]);
    std::borrow::Cow::Owned(rewritten)
}

/// Whether a number token is a JSON integer that serde_json reads as a float:
/// `-0`, or one outside the 64-bit range. Anything that isn't a well-formed
/// integer is left for the parser to refuse.
#[cfg(any(feature = "http-server", feature = "wasm-sqlite", test))]
fn whole_number_read_as_float(token: &str) -> bool {
    let digits = token.strip_prefix('-').unwrap_or(token);
    let well_formed = !digits.is_empty()
        && digits.bytes().all(|b| b.is_ascii_digit())
        && (digits == "0" || !digits.starts_with('0'));
    if !well_formed {
        return false;
    }
    if token.starts_with('-') {
        digits == "0" || token.parse::<i64>().is_err()
    } else {
        token.parse::<u64>().is_err()
    }
}

/// Strip serde_json's " at line N column N" suffix from error messages.
fn strip_position(msg: &str) -> &str {
    if let Some(idx) = msg.rfind(" at line ") {
        // Verify the suffix looks like " at line N column N"
        if msg[idx..].contains("column") {
            return &msg[..idx];
        }
    }
    msg
}

/// Strip serde_json's position suffix and any marker prefix from a raw serde
/// error message.
///
/// Hand-rolled decoders that pattern-match serde messages route through this
/// so no marker class can ever leak into a wire message.
pub(crate) fn clean_serde_message(msg: &str) -> &str {
    let clean = strip_position(msg);
    clean
        .strip_prefix(SERIALIZATION_AS_READ_MARKER)
        .or_else(|| clean.strip_prefix(REQUEST_VALIDATION_MARKER))
        .or_else(|| clean.strip_prefix(VALIDATION_MARKER))
        .or_else(|| clean.strip_prefix(SERIALIZATION_MARKER))
        .unwrap_or(clean)
}

/// Map serde deserialisation error messages to DynamoDB-style SerializationException messages.
///
/// DynamoDB returns specific messages like "NUMBER_VALUE cannot be converted to String"
/// whereas serde returns "invalid type: integer `23`, expected a string at line 1 column 42".
fn map_serde_to_dynamodb_message(msg: &str, body: &str) -> String {
    // "invalid type: <type>, expected <target>"
    if let Some(rest) = msg.strip_prefix("invalid type: ") {
        // Extract the source type and target type
        let (source_part, target_part) = match rest.split_once(", expected ") {
            Some((s, t)) => (s, t),
            None => return msg.to_string(),
        };
        // Strip " at line N column N" from target
        let target = target_part
            .split(" at line ")
            .next()
            .unwrap_or(target_part)
            .trim();

        // The i32-modelled members leak the raw serde-style rejection,
        // position suffix included: "invalid type: floating point `3.5`,
        // expected i32 at line 1 column N", captured from real DynamoDB
        // (eu-west-2 and us-east-1, 2026-08-13).
        //
        // That capture is of `SearchVectors`' `TopK`, which is the only
        // i32-modelled request member in the crate, so the type is a safe
        // stand-in for the field today. Serde does not name the field in this
        // message, so there is nothing narrower to match on. Anyone modelling a
        // second member as i32 has to check that AWS leaks it the same way
        // rather than inheriting this by the type alone; every other member of
        // this shape is mapped, not passed through.
        if target == "i32" {
            return msg.to_string();
        }

        return map_type_mismatch(source_part.trim(), target);
    }

    // "invalid length N, expected struct X ..." → struct-level errors
    if msg.contains("expected struct") && msg.starts_with("invalid length ") {
        // Extract struct name from "invalid length N, expected struct X with M elements"
        if let Some(rest) = msg.split("expected struct ").nth(1) {
            let struct_name = rest.split(' ').next().unwrap_or("Unknown");
            if let Some(dynamo_class) = map_struct_to_dynamo_class(struct_name) {
                return format!("Unrecognized collection type class {dynamo_class}");
            }
        }
        return "Start of structure or map found where not expected".to_string();
    }

    // "expected string for X at line N column N" → wrong type inside AttributeValue
    if msg.starts_with("expected string for ") {
        return infer_type_conversion_error(msg, body, "String");
    }

    // "expected value at line N column N" → wrong value type at position
    if msg.starts_with("expected value at line ") {
        return infer_type_conversion_error(msg, body, "String");
    }

    msg.to_string()
}

/// Map a serde type mismatch to DynamoDB's SerializationException message.
fn map_type_mismatch(source: &str, target: &str) -> String {
    // Determine target type category
    let target_is_string = target == "a string";
    let target_is_bool = target == "a boolean";
    let target_is_sequence = target == "a sequence";
    let target_is_integer = target == "i64" || target == "u64";
    let target_is_struct = target.starts_with("struct ");
    let target_is_map = target.starts_with("a map") || target.starts_with("map");

    // Determine source type
    let is_integer = source.starts_with("integer ");
    let is_float = source.starts_with("floating point ");
    let is_bool_true = source == "boolean `true`";
    let is_bool_false = source == "boolean `false`";
    let _is_bool = is_bool_true || is_bool_false;
    let is_string = source.starts_with("string ");
    let is_sequence = source == "sequence";
    let is_map = source == "map";

    // Map to DynamoDB message based on (source_type, target_type) combination
    if target_is_sequence {
        // List/array fields
        if is_map {
            return "Start of structure or map found where not expected".to_string();
        }
        return "Unexpected field type".to_string();
    }

    if target_is_string {
        if is_bool_true {
            return "TRUE_VALUE cannot be converted to String".to_string();
        }
        if is_bool_false {
            return "FALSE_VALUE cannot be converted to String".to_string();
        }
        if is_float {
            return "DECIMAL_VALUE cannot be converted to String".to_string();
        }
        if is_integer {
            return "NUMBER_VALUE cannot be converted to String".to_string();
        }
        if is_sequence {
            return "Unrecognized collection type class java.lang.String".to_string();
        }
        if is_map {
            return "Start of structure or map found where not expected".to_string();
        }
    }

    if target_is_bool {
        if is_string {
            return "Unexpected token received from parser".to_string();
        }
        if is_float {
            return "DECIMAL_VALUE cannot be converted to Boolean".to_string();
        }
        if is_integer {
            return "NUMBER_VALUE cannot be converted to Boolean".to_string();
        }
        if is_sequence {
            return "Unrecognized collection type class java.lang.Boolean".to_string();
        }
        if is_map {
            return "Start of structure or map found where not expected".to_string();
        }
    }

    if target_is_integer {
        if is_string {
            return "STRING_VALUE cannot be converted to Long".to_string();
        }
        if is_bool_true {
            return "TRUE_VALUE cannot be converted to Long".to_string();
        }
        if is_bool_false {
            return "FALSE_VALUE cannot be converted to Long".to_string();
        }
        if is_sequence {
            return "Unrecognized collection type class java.lang.Long".to_string();
        }
        if is_map {
            return "Start of structure or map found where not expected".to_string();
        }
    }

    if target_is_struct || target_is_map {
        if is_sequence {
            // Need to figure out the class from target
            if let Some(struct_name) = target.strip_prefix("struct ") {
                let name = struct_name.split(' ').next().unwrap_or("Unknown");
                if let Some(dynamo_class) = map_struct_to_dynamo_class(name) {
                    return format!("Unrecognized collection type class {dynamo_class}");
                }
            }
        }
        if is_map && target_is_struct {
            return "Start of structure or map found where not expected".to_string();
        }
        if !is_map && !is_sequence {
            return "Unexpected field type".to_string();
        }
    }

    // Fallback: return the original message
    source
        .split(" at line ")
        .next()
        .unwrap_or(source)
        .to_string()
}

/// Infer the DynamoDB type conversion error from a serde error message.
/// Uses the column position to inspect the actual JSON value in the body.
fn infer_type_conversion_error(msg: &str, body: &str, target_type: &str) -> String {
    // Try to extract column number from "at line N column N"
    if let Some(col_str) = msg.rsplit("column ").next()
        && let Ok(col) = col_str.trim().parse::<usize>()
    {
        // Column is 1-based. Look at the character just before the column
        // to determine what type of value serde encountered.
        if col > 0 && col <= body.len() {
            let ch = body.as_bytes()[col - 1];
            return match ch {
                b't' => format!("TRUE_VALUE cannot be converted to {target_type}"),
                b'f' => format!("FALSE_VALUE cannot be converted to {target_type}"),
                b'0'..=b'9' | b'-' => {
                    format!("NUMBER_VALUE cannot be converted to {target_type}")
                }
                _ => format!("TRUE_VALUE cannot be converted to {target_type}"),
            };
        }
    }
    format!("TRUE_VALUE cannot be converted to {target_type}")
}

/// Map Rust struct names to DynamoDB Java class names for SerializationException messages.
fn map_struct_to_dynamo_class(struct_name: &str) -> Option<&'static str> {
    match struct_name {
        "ProvisionedThroughput" | "ProvisionedThroughputRaw" => {
            Some("com.amazonaws.dynamodb.v20120810.ProvisionedThroughput")
        }
        "Projection" | "ProjectionRaw" => Some("com.amazonaws.dynamodb.v20120810.Projection"),
        "KeySchemaElement" | "KeySchemaElementRaw" => {
            Some("com.amazonaws.dynamodb.v20120810.KeySchemaElement")
        }
        "AttributeDefinition" | "AttributeDefinitionRaw" => {
            Some("com.amazonaws.dynamodb.v20120810.AttributeDefinition")
        }
        "LocalSecondaryIndex" | "LocalSecondaryIndexRaw" => {
            Some("com.amazonaws.dynamodb.v20120810.LocalSecondaryIndex")
        }
        "GlobalSecondaryIndex" | "GlobalSecondaryIndexRaw" => {
            Some("com.amazonaws.dynamodb.v20120810.GlobalSecondaryIndex")
        }
        "DeleteGsiAction" | "DeleteGsiActionRaw" => {
            Some("com.amazonaws.dynamodb.v20120810.DeleteGlobalSecondaryIndexAction")
        }
        "CreateGsiAction" | "CreateGsiActionRaw" => {
            Some("com.amazonaws.dynamodb.v20120810.CreateGlobalSecondaryIndexAction")
        }
        "UpdateGsiAction" | "UpdateGsiActionRaw" => {
            Some("com.amazonaws.dynamodb.v20120810.UpdateGlobalSecondaryIndexAction")
        }
        "GlobalSecondaryIndexUpdate" | "GlobalSecondaryIndexUpdateRaw" => {
            Some("com.amazonaws.dynamodb.v20120810.GlobalSecondaryIndexUpdate")
        }
        "VectorIndex" => Some("com.amazonaws.dynamodb.v20120810.VectorIndex"),
        "VectorAttributeDefinition" => {
            Some("com.amazonaws.dynamodb.v20120810.VectorAttributeDefinition")
        }
        "SearchSchemaElement" => Some("com.amazonaws.dynamodb.v20120810.SearchSchemaElement"),
        "Tag" | "TagRaw" => Some("com.amazonaws.dynamodb.v20120810.Tag"),
        _ => None,
    }
}

#[cfg(feature = "http-server")]
pub(crate) fn serialize<T: serde::Serialize>(val: &T) -> crate::Result<String> {
    serde_json::to_string(val).map_err(|e| crate::DynoxideError::InternalServerError(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_marker_maps_to_enveloped_validation() {
        // A VALIDATION_REQUEST-marked serde error becomes the wire-invisible
        // tag, with the marker and position suffix both stripped.
        let err = serde_json::from_str::<crate::types::AttributeValue>(r#"{"NULL": false}"#)
            .map_err(|e| e.to_string())
            .unwrap_err();
        assert!(
            err.starts_with("VALIDATION_REQUEST:"),
            "marker missing: {err}"
        );

        let decoded: crate::Result<crate::types::AttributeValue> =
            deserialize(r#"{"NULL": false}"#);
        match decoded.unwrap_err() {
            crate::DynoxideError::EnvelopedValidation(msg) => {
                assert_eq!(
                    msg,
                    "One or more parameter values were invalid: \
                     Null attribute value types must have the value of true"
                );
            }
            other => panic!("expected EnvelopedValidation, got {other:?}"),
        }
    }

    #[test]
    fn test_bare_marker_maps_to_validation_exception() {
        // A VALIDATION-marked serde error stays a bare ValidationException.
        let decoded: crate::Result<crate::types::AttributeValue> =
            deserialize(r#"{"S": "a", "N": "1"}"#);
        match decoded.unwrap_err() {
            crate::DynoxideError::ValidationException(msg) => {
                assert_eq!(
                    msg,
                    "Supplied AttributeValue has more than one datatypes set, \
                     must contain exactly one of the supported datatypes"
                );
            }
            other => panic!("expected ValidationException, got {other:?}"),
        }
    }

    #[test]
    fn test_clean_serde_message_strips_both_markers_and_position() {
        assert_eq!(
            clean_serde_message("VALIDATION_REQUEST:msg at line 1 column 42"),
            "msg"
        );
        assert_eq!(
            clean_serde_message("VALIDATION:msg at line 1 column 42"),
            "msg"
        );
        assert_eq!(clean_serde_message("VALIDATION:msg"), "msg");
        assert_eq!(
            clean_serde_message("plain msg at line 3 column 7"),
            "plain msg"
        );
        // A trailing " at line " without "column" is not a serde position
        // suffix and stays untouched.
        assert_eq!(clean_serde_message("look at line 9"), "look at line 9");
        assert_eq!(clean_serde_message("plain msg"), "plain msg");
    }

    #[test]
    fn boolean_numbers_are_rewritten_only_where_serde_would_read_a_float() {
        // A whole number past 64 bits, or `-0`, in a boolean member reads as
        // the integer it was written as.
        assert_eq!(
            boolean_numbers_as_written(
                r#"{"v": {"BOOL": -0}, "w": {"NULL" : 18446744073709551616}}"#
            ),
            r#"{"v": {"BOOL": 0}, "w": {"NULL" : 0}}"#
        );
        assert_eq!(
            boolean_numbers_as_written(r#"{"ConsistentRead":-9223372036854775809}"#),
            r#"{"ConsistentRead":0}"#
        );
        // Integers serde reads as integers, decimals, other members and
        // strings that merely mention a boolean member are left alone.
        for body in [
            r#"{"v": {"BOOL": 9223372036854775807}}"#,
            r#"{"v": {"BOOL": 18446744073709551615}}"#,
            r#"{"v": {"BOOL": -0.0}, "w": {"NULL": 1e30}}"#,
            r#"{"Limit": -0, "v": {"N": "18446744073709551616"}}"#,
            r#"{"s": "\"BOOL\": -0", "t": ["BOOL", -0]}"#,
            r#"{"v": {"BOOL": 01}}"#,
        ] {
            assert!(
                matches!(
                    boolean_numbers_as_written(body),
                    std::borrow::Cow::Borrowed(_)
                ),
                "{body}"
            );
        }
    }

    #[test]
    fn test_strip_position_unchanged() {
        assert_eq!(strip_position("msg at line 1 column 2"), "msg");
        assert_eq!(strip_position("msg"), "msg");
        assert_eq!(strip_position("look at line 9"), "look at line 9");
    }
}
