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
        // A member read as a fixed-width number gets a bare
        // SerializationException for one past f64's range, as every member
        // did before `Limit`, `Segment` and `TotalSegments` read their text.
        if is_number_out_of_range(&e) {
            return crate::DynoxideError::SerializationException(String::new());
        }
        unmarked_validation(&msg).unwrap_or_else(|| {
            crate::DynoxideError::SerializationException(map_serde_to_dynamodb_message(&msg, body))
        })
    })
}

/// The `ValidationException` an unmarked serde message is answered with, or
/// `None` when it is a `SerializationException`.
///
/// DynamoDB returns ValidationException for missing required fields, null
/// values, and unrecognised enum variants. Only true JSON type mismatches
/// (e.g. number where string is expected) produce a SerializationException.
fn unmarked_validation(msg: &str) -> Option<crate::DynoxideError> {
    if msg.contains("missing field")
        || msg.contains("unknown variant")
        || msg.contains("invalid type: null")
    {
        Some(crate::DynoxideError::ValidationException(msg.to_string()))
    } else if msg.contains("empty AttributeValue") {
        Some(crate::DynoxideError::ValidationException(
            EMPTY_VALUE.to_string(),
        ))
    } else if msg.contains("Supplied AttributeValue") {
        // Multi-datatype or empty AV error: strip position info and return as-is
        Some(crate::DynoxideError::ValidationException(
            strip_position(msg).to_string(),
        ))
    } else {
        None
    }
}

/// Whether [`deserialize`] answers a raw serde message with a validation
/// error rather than a `SerializationException`. A PartiQL parameter refused
/// this way waits for the request's member checks; one refused as malformed
/// does not.
pub(crate) fn is_validation_refusal(msg: &str) -> bool {
    match classify_marked_serde_error(msg) {
        Some(crate::DynoxideError::SerializationException(_)) => false,
        Some(_) => true,
        None => unmarked_validation(msg).is_some(),
    }
}

/// Whether serde_json refused a number for being past f64's range. The JSON is
/// well formed, and DynamoDB reads such a number (captured eu-west-2 and
/// us-east-1, 6 November 2026).
pub(crate) fn is_number_out_of_range(err: &serde_json::Error) -> bool {
    err.to_string().starts_with("number out of range")
}

/// A request body as a JSON value, for the checks that read its shape before
/// the typed read. These checks look at what type a member is, never at a
/// number's value, and DynamoDB names a number by how it is written, a decimal
/// with a point or an exponent and a whole number without, whatever its size
/// (captured eu-west-2 and us-east-1, 6 November 2026). serde_json keeps no
/// text for a number: it refuses one past f64's range, though that is
/// well-formed JSON DynamoDB reads, and reads `-0` and a whole number past 64
/// bits as floats. So when the body holds either, it is read again with each
/// such number standing in as a zero written the same way. The member's own
/// reader takes the number from the body as written.
#[cfg(any(
    feature = "http-server",
    feature = "wasm-sqlite",
    feature = "mcp-server",
    test
))]
pub(crate) fn body_value(body: &str) -> Result<serde_json::Value, serde_json::Error> {
    match serde_json::from_str(body) {
        Err(e) if is_number_out_of_range(&e) => serde_json::from_str(&numbers_as_written(body)),
        Ok(value) if holds_float(&value) => serde_json::from_str(&numbers_as_written(body)),
        read => read,
    }
}

/// Whether a JSON value holds a number serde_json read as a float.
#[cfg(any(
    feature = "http-server",
    feature = "wasm-sqlite",
    feature = "mcp-server",
    test
))]
fn holds_float(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Number(n) => n.is_f64(),
        serde_json::Value::Array(items) => items.iter().any(holds_float),
        serde_json::Value::Object(map) => map.values().any(holds_float),
        _ => false,
    }
}

/// `body` with every number serde_json can't hold, or would read as a float
/// though it was written whole, standing in as `0.0`, or as `0` when it was
/// written with no point or exponent. Strings are skipped whole, escapes
/// included, so text that looks like a number inside one is left alone.
#[cfg(any(
    feature = "http-server",
    feature = "wasm-sqlite",
    feature = "mcp-server",
    test
))]
fn numbers_as_written(body: &str) -> String {
    let bytes = body.as_bytes();
    let mut out = String::with_capacity(body.len());
    let mut i = 0;
    while i < bytes.len() {
        let start = i;
        match bytes[i] {
            b'"' => {
                i += 1;
                while i < bytes.len() && bytes[i] != b'"' {
                    i += if bytes[i] == b'\\' { 2 } else { 1 };
                }
                i = (i + 1).min(bytes.len());
                out.push_str(&body[start..i]);
            }
            b'-' | b'0'..=b'9' => {
                while i < bytes.len()
                    && matches!(bytes[i], b'-' | b'+' | b'.' | b'e' | b'E' | b'0'..=b'9')
                {
                    i += 1;
                }
                let token = &body[start..i];
                let whole = !token.contains(['.', 'e', 'E']);
                match serde_json::from_str::<serde_json::Number>(token) {
                    Err(e) if is_number_out_of_range(&e) => {
                        out.push_str(if whole { "0" } else { "0.0" })
                    }
                    Ok(n) if whole && n.is_f64() => out.push('0'),
                    _ => out.push_str(token),
                }
            }
            _ => {
                // Copy up to the next string or number, keeping UTF-8 whole.
                i += 1;
                while i < bytes.len() && !matches!(bytes[i], b'"' | b'-' | b'0'..=b'9') {
                    i += 1;
                }
                out.push_str(&body[start..i]);
            }
        }
    }
    out
}

/// Classify a raw serde error message by its marker prefix, stripping the
/// marker and serde_json's position suffix.
///
/// `VALIDATION_REQUEST:` becomes the wire-invisible
/// [`EnvelopedValidation`](crate::DynoxideError::EnvelopedValidation) tag,
/// `VALIDATION:` a bare `ValidationException` and `SERIALIZATION:` a
/// `SerializationException`, as does `SERIALIZATION_AS_READ:` with the
/// position left on, and `VALIDATION_AS_READ:` a `ValidationException` the
/// same way; an unmarked message returns
/// `None` for the caller to classify. This is the single owner of the
/// marker-to-variant mapping, shared by [`deserialize`] and callers that
/// decode via `serde_json::from_value` (the MCP surface).
pub(crate) fn classify_marked_serde_error(msg: &str) -> Option<crate::DynoxideError> {
    if let Some(as_read) = msg.strip_prefix(SERIALIZATION_AS_READ_MARKER) {
        return Some(crate::DynoxideError::SerializationException(
            as_read.to_string(),
        ));
    }
    if let Some(as_read) = msg.strip_prefix(VALIDATION_AS_READ_MARKER) {
        return Some(crate::DynoxideError::ValidationException(
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

/// The raw serde message, marker and all, that [`deserialize`] answers with
/// `error`, for a request decoder that raises an error found by a check made
/// outside serde; `None` for an error no marker carries.
pub(crate) fn marked_message(error: &crate::DynoxideError) -> Option<String> {
    match error {
        crate::DynoxideError::ValidationException(message) => {
            Some(format!("{VALIDATION_MARKER}{message}"))
        }
        crate::DynoxideError::EnvelopedValidation(message) => {
            Some(format!("{REQUEST_VALIDATION_MARKER}{message}"))
        }
        crate::DynoxideError::SerializationException(message) => {
            Some(format!("{SERIALIZATION_MARKER}{message}"))
        }
        _ => None,
    }
}

/// DynamoDB's refusal of a NULL that isn't true.
pub(crate) const NULL_NOT_TRUE: &str = "One or more parameter values were invalid: Null attribute value types must have the value \
     of true";

/// DynamoDB's refusal of a JSON null where an attribute value belongs. A
/// member of a map of attributes is named after it by
/// [`null_member_refusal`]; a list member, a PartiQL parameter and a legacy
/// `AttributeValueList` member are not (captured eu-west-2 and us-east-1,
/// 2026-11-11).
pub(crate) const NULL_VALUE: &str = "The supplied Item contains a null AttributeValue";

/// DynamoDB's refusal of a JSON null given for the attribute `name` of an
/// item, a key or a nested `M`.
pub(crate) fn null_member_refusal(name: &str) -> String {
    format!("{NULL_VALUE}: {name} is null")
}

/// DynamoDB's refusal of a JSON null among the members of a set of
/// `type_key`'s type: a value fault, refused bare, and named by key in the
/// expression values as a null value is (captured eu-west-2 and us-east-1,
/// 2026-11-12).
pub(crate) fn null_set_member_refusal(type_key: &str) -> Option<&'static str> {
    NULL_SET_MEMBER
        .iter()
        .find(|(set, _)| *set == type_key)
        .map(|(_, refusal)| *refusal)
}

/// The refusals [`null_set_member_refusal`] gives, by set type.
const NULL_SET_MEMBER: [(&str, &str); 3] = [
    (
        "SS",
        "One or more parameter values were invalid: An string set may not have a null string as a member",
    ),
    (
        "NS",
        "One or more parameter values were invalid: Numeric sets may not contain null",
    ),
    (
        "BS",
        "One or more parameter values were invalid: Binary sets may not contain null or empty values",
    ),
];

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
    /// A JSON null, named after its attribute when it was a map's member, or
    /// a member of a set.
    Null(&'a str),
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
    if inner.starts_with(NULL_VALUE) || NULL_SET_MEMBER.iter().any(|(_, refusal)| inner == *refusal)
    {
        ValueRefusal::Null(inner)
    } else if inner.contains("empty AttributeValue")
        || inner.contains("Supplied AttributeValue is empty")
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

/// As [`SERIALIZATION_AS_READ_MARKER`], mapped to a bare `ValidationException`
/// that keeps the deserialiser's message whole.
pub(crate) const VALIDATION_AS_READ_MARKER: &str = "VALIDATION_AS_READ:";

/// Marker prefix for the request-validation class raised inside serde
/// deserialisation, mapped to the wire-invisible `EnvelopedValidation` tag
/// that PutItem and UpdateItem envelope. Must be checked before
/// [`VALIDATION_MARKER`], whose spelling it extends.
pub(crate) const REQUEST_VALIDATION_MARKER: &str = "VALIDATION_REQUEST:";

/// The members a request reads as booleans, whose numbers
/// [`misread_numbers_as_written`] looks at.
#[cfg(any(feature = "http-server", feature = "wasm-sqlite", test))]
const BOOLEAN_MEMBERS: [&str; 5] = [
    "BOOL",
    "NULL",
    "ConsistentRead",
    "ScanIndexForward",
    "Exists",
];

/// The attribute value members that hold something other than a boolean, whose
/// numbers [`misread_numbers_as_written`] looks at.
#[cfg(any(feature = "http-server", feature = "wasm-sqlite", test))]
const VALUE_MEMBERS: [&str; 8] = ["S", "N", "B", "SS", "NS", "BS", "L", "M"];

/// The attribute value members holding an array, whose own members' numbers
/// [`misread_numbers_as_written`] looks at too.
#[cfg(any(feature = "http-server", feature = "wasm-sqlite", test))]
const LIST_MEMBERS: [&str; 4] = ["SS", "NS", "BS", "L"];

/// A request body with each number serde_json would misread written as one it
/// reads the same way, where a number fails the request whatever its value:
///
/// - the value of a member read as a boolean (`BOOL`, `NULL`,
///   `ConsistentRead`, `ScanIndexForward`, `Exists`);
/// - the value of an attribute value's other members (`S`, `N`, `B`, `SS`,
///   `NS`, `BS`, `L`, `M`), a member of its `SS`, `NS`, `BS` or `L` array, or
///   a value in its `M` object.
///
/// DynamoDB names a number given in these places by how it is written: a
/// whole number of any size, `-0` included, is `NUMBER_VALUE`, and one with a
/// point or an exponent `DECIMAL_VALUE` (captured eu-west-2 and us-east-1,
/// 2026-10-10 for booleans and 2026-11-08 for attribute values). serde_json
/// keeps no text for a number: it reads `-0` and integers past 64 bits as
/// floats, which would name them `DECIMAL_VALUE`, so they are written as `0`.
/// It refuses a decimal past f64's range outright, so that is written as
/// `0.0`. Nothing else reads these numbers. The body comes
/// back as it is when there is nothing to rewrite.
#[cfg(any(feature = "http-server", feature = "wasm-sqlite", test))]
pub(crate) fn misread_numbers_as_written(body: &str) -> std::borrow::Cow<'_, str> {
    let bytes = body.as_bytes();
    let mut rewrites: Vec<(usize, usize, &str)> = Vec::new();
    // The key whose value comes next, once its colon has been read.
    let mut key: Option<&str> = None;
    // For each open array or object, whether its members are an attribute
    // value's list or map members.
    let mut containers: Vec<bool> = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'[' => {
                containers.push(key.is_some_and(|key| LIST_MEMBERS.contains(&key)));
                key = None;
            }
            b'{' => {
                containers.push(key == Some("M"));
                key = None;
            }
            b']' | b'}' => {
                containers.pop();
                key = None;
            }
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
                let token = &body[start..i];
                let in_value = key.is_some_and(|key| VALUE_MEMBERS.contains(&key))
                    || containers.last() == Some(&true);
                let in_boolean = key.is_some_and(|key| BOOLEAN_MEMBERS.contains(&key));
                if (in_value || in_boolean) && whole_number_read_as_float(token) {
                    rewrites.push((start, i, "0"));
                } else if (in_value || in_boolean) && decimal_past_f64(token) {
                    rewrites.push((start, i, "0.0"));
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
    for (start, end, stand_in) in rewrites {
        rewritten.push_str(&body[copied..start]);
        rewritten.push_str(stand_in);
        copied = end;
    }
    rewritten.push_str(&body[copied..]);
    std::borrow::Cow::Owned(rewritten)
}

/// Whether a number token is a well-formed JSON number written with a point
/// or an exponent that serde_json refuses to read as out of range. That is
/// asked of serde_json itself, whose reading near f64's limit differs from a
/// correctly rounded parse. Anything that isn't a well-formed number is left
/// for the parser to refuse.
#[cfg(any(feature = "http-server", feature = "wasm-sqlite", test))]
fn decimal_past_f64(token: &str) -> bool {
    json_number(token)
        && token.contains(['.', 'e', 'E'])
        && serde_json::from_str::<f64>(token).is_err()
}

/// Whether a token follows JSON's number grammar: an optional `-`, an integer
/// part with no leading zero, then optionally a point with digits and an
/// exponent with digits.
#[cfg(any(feature = "http-server", feature = "wasm-sqlite", test))]
pub(crate) fn json_number(token: &str) -> bool {
    fn digits(text: &str) -> (&str, &str) {
        let end = text.bytes().take_while(u8::is_ascii_digit).count();
        text.split_at(end)
    }
    let (int, rest) = digits(token.strip_prefix('-').unwrap_or(token));
    if int.is_empty() || (int.len() > 1 && int.starts_with('0')) {
        return false;
    }
    let rest = match rest.strip_prefix('.') {
        Some(fraction) => {
            let (fraction, rest) = digits(fraction);
            if fraction.is_empty() {
                return false;
            }
            rest
        }
        None => rest,
    };
    let rest = match rest.strip_prefix(['e', 'E']) {
        Some(exponent) => {
            let (exponent, rest) = digits(exponent.strip_prefix(['+', '-']).unwrap_or(exponent));
            if exponent.is_empty() {
                return false;
            }
            rest
        }
        None => rest,
    };
    rest.is_empty()
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
        .or_else(|| clean.strip_prefix(VALIDATION_AS_READ_MARKER))
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

    // "invalid length N, expected struct X ...": a structure given as an
    // array, which DynamoDB refuses with no message (captured eu-west-2 and
    // us-east-1, 11 and 12 November 2026). The type checks run before the
    // decoder (`serialization_checks`) are what refuse an array at a
    // structure; one of the right length would be read by position here, so
    // they must. This only catches an array of the wrong length at a
    // structure those checks have no position for.
    if msg.contains("expected struct") && msg.starts_with("invalid length ") {
        return String::new();
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

    // An array where a scalar or a map belongs: DynamoDB refuses it with no
    // message (captured eu-west-2 and us-east-1, 11 and 12 November 2026).
    // An array at a structure is refused by the type checks run before the
    // decoder.
    if is_sequence {
        return String::new();
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
        if is_map {
            return "Start of structure or map found where not expected".to_string();
        }
    }

    if target_is_struct || target_is_map {
        if is_map && target_is_struct {
            return "Start of structure or map found where not expected".to_string();
        }
        if !is_map {
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

#[cfg(feature = "http-server")]
pub(crate) fn serialize<T: serde::Serialize>(val: &T) -> crate::Result<String> {
    serde_json::to_string(val).map_err(|e| crate::DynoxideError::InternalServerError(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn out_of_range_numbers_are_zeroed_outside_strings_only() {
        // Escaped quotes, number-like text and multi-byte characters inside a
        // string are copied whole; the numbers outside become `0.0`.
        let body = r#"{"a":"x\"1e400","b":-1e400,"c":"é1e400ü","d":[1e400,2,1.5e3],"e":"\\"}"#;
        assert_eq!(
            numbers_as_written(body),
            r#"{"a":"x\"1e400","b":0.0,"c":"é1e400ü","d":[0.0,2,1.5e3],"e":"\\"}"#
        );
        let value = body_value(body).unwrap();
        assert_eq!(value["a"], "x\"1e400");
        assert_eq!(value["c"], "é1e400ü");
        assert!(value["b"].is_f64() && !value["b"].is_i64());
        assert_eq!(value["e"], "\\");
    }

    #[test]
    fn a_body_without_out_of_range_numbers_reads_as_written() {
        let body = r#"{"Limit":10,"x":"1e400"}"#;
        assert_eq!(
            body_value(body).unwrap(),
            serde_json::from_str::<serde_json::Value>(body).unwrap()
        );
        assert!(body_value("{").is_err());
    }

    #[test]
    fn a_long_whole_number_stays_whole() {
        let digits = format!("1{}", "0".repeat(400));
        let body = format!(r#"{{"a":{digits},"b":-{digits},"c":{digits}.0}}"#);
        assert_eq!(numbers_as_written(&body), r#"{"a":0,"b":0,"c":0.0}"#);
    }

    #[test]
    fn whole_numbers_read_as_floats_stay_whole() {
        // `-0` and whole numbers past 64 bits are floats to serde_json; the
        // rest are left as written.
        let body = r#"{"a":-0,"b":100000000000000000000,"c":-100000000000000000000,"d":1.5,"e":-0.0,"f":18446744073709551615}"#;
        assert_eq!(
            numbers_as_written(body),
            r#"{"a":0,"b":0,"c":0,"d":1.5,"e":-0.0,"f":18446744073709551615}"#
        );
        let value = body_value(body).unwrap();
        assert!(value["b"].is_u64() && value["d"].is_f64());
    }

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
            misread_numbers_as_written(
                r#"{"v": {"BOOL": -0}, "w": {"NULL" : 18446744073709551616}}"#
            ),
            r#"{"v": {"BOOL": 0}, "w": {"NULL" : 0}}"#
        );
        assert_eq!(
            misread_numbers_as_written(r#"{"ConsistentRead":-9223372036854775809}"#),
            r#"{"ConsistentRead":0}"#
        );
        // Integers serde reads as integers, decimals, other members and
        // strings that merely mention a boolean member are left alone.
        for body in [
            r#"{"v": {"BOOL": 9223372036854775807}}"#,
            r#"{"v": {"BOOL": 18446744073709551615}}"#,
            r#"{"v": {"BOOL": -0.0}, "w": {"NULL": 1e30}}"#,
            r#"{"v": {"BOOL": 1.5e300}}"#,
            r#"{"v": {"S": 1.7976931348623157e308}}"#,
            r#"{"Limit": -0, "v": {"N": "18446744073709551616"}}"#,
            r#"{"s": "\"BOOL\": -0", "t": ["BOOL", -0]}"#,
            r#"{"v": {"BOOL": 01}}"#,
        ] {
            assert!(
                matches!(
                    misread_numbers_as_written(body),
                    std::borrow::Cow::Borrowed(_)
                ),
                "{body}"
            );
        }
    }

    #[test]
    fn attribute_value_numbers_are_rewritten_as_written() {
        // Whole numbers serde reads as floats become `0`, and decimals past
        // f64's range `0.0`, in an attribute value's members, its list members
        // and its map values.
        assert_eq!(
            misread_numbers_as_written(
                r#"{"a":{"S":-0},"b":{"N":1e400},"c":{"SS":[1, 99999999999999999999, 1.5E999]},"d":{"L":[-0,{"S":-1e400}]},"e":{"M":{"x":-0,"y":{"S":1e400}}}}"#
            ),
            r#"{"a":{"S":0},"b":{"N":0.0},"c":{"SS":[1, 0, 0.0]},"d":{"L":[0,{"S":0.0}]},"e":{"M":{"x":0,"y":{"S":0.0}}}}"#
        );
        // A decimal serde_json refuses although it rounds to a finite double.
        assert_eq!(
            misread_numbers_as_written(r#"{"v":{"S":1.7976931348623158e308}}"#),
            r#"{"v":{"S":0.0}}"#
        );
        // An attribute named for a type, deep nesting, a string holding
        // brackets and a decimal past f64's range for a boolean.
        assert_eq!(
            misread_numbers_as_written(
                r#"{"Item":{"M":{"M":{"a":{"L":[{"M":{"b":{"NS":[-0]}}}]}}},"s":{"S":"]}["},"t":{"BOOL":1e400},"ConsistentRead":-1E999}"#
            ),
            r#"{"Item":{"M":{"M":{"a":{"L":[{"M":{"b":{"NS":[0]}}}]}}},"s":{"S":"]}["},"t":{"BOOL":0.0},"ConsistentRead":0.0}"#
        );
        // Numbers in an ordinary member, in an array or object that isn't an
        // attribute value's, or inside a string are left alone, as are
        // decimals serde reads.
        for body in [
            r#"{"Limit": 1e400, "Segment": -0}"#,
            r#"{"t": ["SS", -0], "u": {"x": 1e400}}"#,
            r#"{"v": {"S": "1e400"}, "w": {"SS": ["-0"]}}"#,
            r#"{"v": {"S": 1e300}, "w": {"L": [1.5, 7]}}"#,
            r#"{"M": [1e400], "L": {"x": 1e400}}"#,
            // Numbers in a string member's array or object, a key that only
            // looks like a type once unescaped, and malformed numbers.
            r#"{"v": {"S": [1e400, -0]}, "w": {"N": {"x": -0}}}"#,
            r#"{"v": {"S\"": -0}, "w": {"\u0053": -0}}"#,
            r#"{"v": {"S": 01.5e400}, "w": {"N": 1.e400}, "x": {"BOOL": 1e}}"#,
        ] {
            assert!(
                matches!(
                    misread_numbers_as_written(body),
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

    /// An array the decoder meets where a structure, a scalar or a map
    /// belongs is a `SerializationException` with no message.
    #[test]
    fn an_array_the_decoder_meets_is_refused_with_no_message() {
        #[derive(Debug, serde::Deserialize)]
        #[allow(dead_code)]
        struct Pair {
            a: Option<String>,
            b: Option<std::collections::HashMap<String, String>>,
        }
        for body in [r#"["x"]"#, r#"[]"#, r#"{"a":["x"]}"#, r#"{"b":["x"]}"#] {
            match deserialize::<Pair>(body) {
                Err(crate::DynoxideError::SerializationException(message)) => {
                    assert_eq!(message, "", "{body}")
                }
                other => panic!("{body}: {other:?}"),
            }
        }
    }
}
