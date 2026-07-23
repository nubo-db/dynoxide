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
//!
//! Neither marker may ever reach the wire. Callers that decode raw serde
//! messages by hand go through [`clean_serde_message`], which strips both
//! markers along with serde_json's position suffix.

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
            let clean = strip_serde_position(&msg);
            crate::DynoxideError::ValidationException(clean)
        } else {
            crate::DynoxideError::SerializationException(map_serde_to_dynamodb_message(&msg, body))
        }
    })
}

/// Classify a raw serde error message by its marker prefix, stripping the
/// marker and serde_json's position suffix.
///
/// `VALIDATION_REQUEST:` becomes the wire-invisible
/// [`EnvelopedValidation`](crate::DynoxideError::EnvelopedValidation) tag and
/// `VALIDATION:` a bare `ValidationException`; an unmarked message returns
/// `None` for the caller to classify. This is the single owner of the
/// marker-to-variant mapping, shared by [`deserialize`] and callers that
/// decode via `serde_json::from_value` (the MCP surface).
pub(crate) fn classify_marked_serde_error(msg: &str) -> Option<crate::DynoxideError> {
    if let Some(stripped) = msg.strip_prefix("VALIDATION_REQUEST:") {
        return Some(crate::DynoxideError::EnvelopedValidation(
            strip_serde_position(stripped),
        ));
    }
    if let Some(stripped) = msg.strip_prefix("VALIDATION:") {
        return Some(crate::DynoxideError::ValidationException(
            strip_serde_position(stripped),
        ));
    }
    None
}

/// Strip serde_json's " at line N column N" suffix from error messages.
fn strip_serde_position(msg: &str) -> String {
    strip_position(msg).to_string()
}

/// Slice-based position strip shared by [`strip_serde_position`] and
/// [`clean_serde_message`].
fn strip_position(msg: &str) -> &str {
    if let Some(idx) = msg.rfind(" at line ") {
        // Verify the suffix looks like " at line N column N"
        if msg[idx..].contains("column") {
            return &msg[..idx];
        }
    }
    msg
}

/// Strip serde_json's position suffix and either marker prefix from a raw
/// serde error message.
///
/// Hand-rolled decoders that pattern-match serde messages route through this
/// so no marker class can ever leak into a wire message.
pub(crate) fn clean_serde_message(msg: &str) -> &str {
    let clean = strip_position(msg);
    clean
        .strip_prefix("VALIDATION_REQUEST:")
        .or_else(|| clean.strip_prefix("VALIDATION:"))
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
    if let Some(col_str) = msg.rsplit("column ").next() {
        if let Ok(col) = col_str.trim().parse::<usize>() {
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
    fn test_strip_serde_position_unchanged() {
        assert_eq!(strip_serde_position("msg at line 1 column 2"), "msg");
        assert_eq!(strip_serde_position("msg"), "msg");
        assert_eq!(strip_serde_position("look at line 9"), "look at line 9");
    }
}
