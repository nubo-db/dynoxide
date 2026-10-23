use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use serde::de;
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt;

/// DynamoDB AttributeValue - the core type system.
///
/// Each variant corresponds to a DynamoDB type descriptor:
/// S (String), N (Number as string), B (Binary), BOOL, NULL,
/// SS (String Set), NS (Number Set), BS (Binary Set),
/// L (List), M (Map).
#[derive(Clone, PartialEq)]
pub enum AttributeValue {
    /// String type
    S(String),
    /// Number type - stored as string per DynamoDB convention
    N(String),
    /// Binary type - raw bytes, serialized as base64
    B(Vec<u8>),
    /// Boolean type
    BOOL(bool),
    /// Null type
    NULL(bool),
    /// String Set
    SS(Vec<String>),
    /// Number Set - each number stored as string
    NS(Vec<String>),
    /// Binary Set - each element is raw bytes
    BS(Vec<Vec<u8>>),
    /// List - ordered collection of AttributeValues
    L(Vec<AttributeValue>),
    /// Map - key-value pairs
    M(HashMap<String, AttributeValue>),
}

/// The derived form, except that a map's entries are written in key order.
/// A map is a `HashMap`, so its own order is drawn afresh for every map, and
/// two equal maps decoded by two requests would otherwise print differently.
/// A PartiQL NextToken is bound to its statement's printed form, so it relies
/// on equal values printing alike.
impl fmt::Debug for AttributeValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AttributeValue::S(v) => f.debug_tuple("S").field(v).finish(),
            AttributeValue::N(v) => f.debug_tuple("N").field(v).finish(),
            AttributeValue::B(v) => f.debug_tuple("B").field(v).finish(),
            AttributeValue::BOOL(v) => f.debug_tuple("BOOL").field(v).finish(),
            AttributeValue::NULL(v) => f.debug_tuple("NULL").field(v).finish(),
            AttributeValue::SS(v) => f.debug_tuple("SS").field(v).finish(),
            AttributeValue::NS(v) => f.debug_tuple("NS").field(v).finish(),
            AttributeValue::BS(v) => f.debug_tuple("BS").field(v).finish(),
            AttributeValue::L(v) => f.debug_tuple("L").field(v).finish(),
            AttributeValue::M(map) => {
                struct Sorted<'a>(&'a HashMap<String, AttributeValue>);
                impl fmt::Debug for Sorted<'_> {
                    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                        let mut entries: Vec<_> = self.0.iter().collect();
                        entries.sort_unstable_by(|a, b| a.0.cmp(b.0));
                        f.debug_map().entries(entries).finish()
                    }
                }
                f.debug_tuple("M").field(&Sorted(map)).finish()
            }
        }
    }
}

impl AttributeValue {
    /// Calculate the size of this attribute value in bytes,
    /// following DynamoDB's item size calculation rules.
    ///
    /// This does NOT include the attribute name - the caller
    /// is responsible for adding the name's UTF-8 byte length.
    pub fn size(&self) -> usize {
        match self {
            AttributeValue::S(s) => s.len(),
            AttributeValue::N(n) => number_size(n),
            AttributeValue::B(b) => b.len(),
            AttributeValue::BOOL(_) => 1,
            AttributeValue::NULL(_) => 1,
            AttributeValue::SS(ss) => ss.iter().map(|s| s.len()).sum(),
            AttributeValue::NS(ns) => ns.iter().map(|n| number_size(n)).sum(),
            AttributeValue::BS(bs) => bs.iter().map(|b| b.len()).sum(),
            AttributeValue::L(items) => {
                // List overhead: 3 bytes + 1 byte per element + sum of element sizes
                3 + items.len() + items.iter().map(|v| v.size()).sum::<usize>()
            }
            AttributeValue::M(map) => {
                // Map overhead: 3 bytes + sum of (key_len + 1 + value_size) per entry
                3 + map
                    .iter()
                    .map(|(k, v)| k.len() + 1 + v.size())
                    .sum::<usize>()
            }
        }
    }

    /// Returns the DynamoDB type descriptor string for this value.
    pub fn type_name(&self) -> &'static str {
        match self {
            AttributeValue::S(_) => "S",
            AttributeValue::N(_) => "N",
            AttributeValue::B(_) => "B",
            AttributeValue::BOOL(_) => "BOOL",
            AttributeValue::NULL(_) => "NULL",
            AttributeValue::SS(_) => "SS",
            AttributeValue::NS(_) => "NS",
            AttributeValue::BS(_) => "BS",
            AttributeValue::L(_) => "L",
            AttributeValue::M(_) => "M",
        }
    }

    /// Returns true if this is a scalar type (S, N, B, BOOL, NULL).
    pub fn is_scalar(&self) -> bool {
        matches!(
            self,
            AttributeValue::S(_)
                | AttributeValue::N(_)
                | AttributeValue::B(_)
                | AttributeValue::BOOL(_)
                | AttributeValue::NULL(_)
        )
    }

    /// Returns true if this is a set type (SS, NS, BS).
    pub fn is_set(&self) -> bool {
        matches!(
            self,
            AttributeValue::SS(_) | AttributeValue::NS(_) | AttributeValue::BS(_)
        )
    }

    /// Serialize this value to a deterministic TEXT representation
    /// for use as a SQLite primary key column (pk or sk).
    ///
    /// - S: stored as-is (UTF-8 text sorts correctly)
    /// - N: normalized to a comparable string encoding
    /// - B: hex-encoded (preserves byte ordering)
    pub fn to_key_string(&self) -> Option<String> {
        match self {
            AttributeValue::S(s) => Some(format!("S:{s}")),
            AttributeValue::N(n) => Some(format!("N:{}", normalize_number_for_sort(n))),
            AttributeValue::B(b) => Some(format!("B:{}", hex_encode(b))),
            _ => None, // Only S, N, B can be key types
        }
    }
}

impl fmt::Display for AttributeValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AttributeValue::S(s) => write!(f, "\"{s}\""),
            AttributeValue::N(n) => write!(f, "{n}"),
            AttributeValue::B(b) => write!(f, "<binary {} bytes>", b.len()),
            AttributeValue::BOOL(b) => write!(f, "{b}"),
            AttributeValue::NULL(_) => write!(f, "null"),
            AttributeValue::SS(ss) => write!(f, "{ss:?}"),
            AttributeValue::NS(ns) => write!(f, "{ns:?}"),
            AttributeValue::BS(bs) => write!(f, "<binary set {} items>", bs.len()),
            AttributeValue::L(items) => write!(f, "<list {} items>", items.len()),
            AttributeValue::M(map) => write!(f, "<map {} keys>", map.len()),
        }
    }
}

// ---------------------------------------------------------------------------
// Custom serde: DynamoDB JSON format {"S": "hello"}, {"N": "42"}, etc.
// ---------------------------------------------------------------------------

impl Serialize for AttributeValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(1))?;
        match self {
            AttributeValue::S(s) => map.serialize_entry("S", s)?,
            AttributeValue::N(n) => map.serialize_entry("N", n)?,
            AttributeValue::B(b) => {
                map.serialize_entry("B", &BASE64.encode(b))?;
            }
            AttributeValue::BOOL(b) => map.serialize_entry("BOOL", b)?,
            AttributeValue::NULL(n) => map.serialize_entry("NULL", n)?,
            // DynamoDB returns set members sorted: strings and binaries by their
            // bytes, numbers by value. Sorting here as well as on write keeps a
            // row stored before sets were sorted in the same order on the way out.
            AttributeValue::SS(ss) => {
                map.serialize_entry("SS", &crate::set_order::sorted_strings(ss))?
            }
            AttributeValue::NS(ns) => {
                map.serialize_entry("NS", &crate::set_order::sorted_numbers(ns))?
            }
            AttributeValue::BS(bs) => {
                let encoded: Vec<String> = crate::set_order::sorted_binaries(bs)
                    .into_iter()
                    .map(|b| BASE64.encode(b))
                    .collect();
                map.serialize_entry("BS", &encoded)?;
            }
            AttributeValue::L(items) => map.serialize_entry("L", items)?,
            AttributeValue::M(m) => map.serialize_entry("M", m)?,
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for AttributeValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Deserialize as raw JSON Value first so we can inspect all keys
        let raw = serde_json::Value::deserialize(deserializer)?;

        let obj = raw
            .as_object()
            .ok_or_else(|| de::Error::custom("empty AttributeValue object"))?;

        if obj.is_empty() {
            return Err(de::Error::custom("empty AttributeValue object"));
        }

        // Collect known type keys. A member set to JSON null counts as absent:
        // `{"S": null}` is an empty value and `{"S": "a", "N": null}` a string
        // (captured eu-west-2 and us-east-1, 2026-10-10).
        let present: Vec<&str> = obj
            .iter()
            .filter(|(k, v)| TYPE_MEMBERS.contains(&k.as_str()) && !v.is_null())
            .map(|(k, _)| k.as_str())
            .collect();

        if present.is_empty() {
            return Err(de::Error::custom(
                "Supplied AttributeValue is empty, must contain exactly one of the supported datatypes",
            ));
        }

        // BOOL and NULL members are read first, as booleans: a malformed one
        // fails the request ahead of anything else about the value, a second
        // member included (captured eu-west-2 and us-east-1, 2026-10-10).
        let boolean = match obj.get("BOOL").filter(|v| !v.is_null()) {
            Some(v) => Some(read_lenient_bool(v).map_err(de::Error::custom)?),
            None => None,
        };
        let null = match obj.get("NULL").filter(|v| !v.is_null()) {
            Some(v) => Some(read_lenient_bool(v).map_err(de::Error::custom)?),
            None => None,
        };

        // A value with more than one member is read by DynamoDB's older
        // validation layer, and so is every other value in its request (see
        // `reads_on_older_layer`): its refusals are bare. An empty set comes
        // first, then a number it can't read, then a NULL that reads as false,
        // and only then the second member itself (captured eu-west-2 and
        // us-east-1, 2026-10-10).
        let layer = if present.len() > 1 {
            crate::validation::ValueLayer::Older
        } else {
            current_value_layer()
        };
        if present.len() > 1
            && let Some(refusal) = present
                .iter()
                .find_map(|&type_key| empty_set_refusal(type_key, &obj[type_key]))
        {
            return Err(de::Error::custom(format!(
                "{}{refusal}",
                crate::serde_errors::VALIDATION_MARKER
            )));
        }

        // Validate numbers in ALL type keys before checking for multi-type.
        // DynamoDB validates number format before rejecting multi-type.
        for &type_key in &present {
            match type_key {
                "N" => {
                    if let Some(n) = obj.get("N").and_then(|v| v.as_str()) {
                        validate_number_in_deser(n, layer).map_err(de::Error::custom)?;
                    }
                }
                "NS" => {
                    if let Some(arr) = obj.get("NS").and_then(|v| v.as_array()) {
                        for item in arr {
                            if let Some(n) = item.as_str() {
                                validate_number_in_deser(n, layer).map_err(de::Error::custom)?;
                            }
                        }
                    }
                }
                _ => {}
            }
        }

        // A NULL must read as true. JSON false on its own is refused by the
        // request's validation, in the envelope PutItem, UpdateItem and
        // DeleteItem give it, so it carries the request-validation marker. A
        // string that reads as false, or a false NULL beside another member or
        // in a request read on the older layer, is refused as the value is
        // parsed: bare, and ahead of the check for more than one member
        // (captured eu-west-2 and us-east-1, 2026-10-10).
        if let Some(LenientBool {
            value: false,
            from_string,
        }) = null
        {
            let marker = if from_string || layer == crate::validation::ValueLayer::Older {
                crate::serde_errors::VALIDATION_MARKER
            } else {
                crate::serde_errors::REQUEST_VALIDATION_MARKER
            };
            return Err(de::Error::custom(format!(
                "{marker}{}",
                crate::serde_errors::NULL_NOT_TRUE
            )));
        }

        // Check for multiple type keys
        if present.len() > 1 {
            return Err(de::Error::custom(
                "VALIDATION:Supplied AttributeValue has more than one datatypes set, \
                 must contain exactly one of the supported datatypes",
            ));
        }

        let type_key = present[0];
        let val = &obj[type_key];

        match type_key {
            "S" => {
                let s = val
                    .as_str()
                    .ok_or_else(|| de::Error::custom("expected string for S"))?;
                Ok(AttributeValue::S(s.to_string()))
            }
            "N" => {
                let n = val
                    .as_str()
                    .ok_or_else(|| de::Error::custom("expected string for N"))?;
                Ok(AttributeValue::N(n.to_string()))
            }
            "B" => {
                let encoded = val
                    .as_str()
                    .ok_or_else(|| de::Error::custom("expected string for B"))?;
                let bytes = BASE64
                    .decode(encoded)
                    .map_err(|e| de::Error::custom(format!("invalid base64: {e}")))?;
                Ok(AttributeValue::B(bytes))
            }
            // Both were read above; a NULL that reached here reads as true.
            "BOOL" => Ok(AttributeValue::BOOL(boolean.is_some_and(|b| b.value))),
            "NULL" => Ok(AttributeValue::NULL(true)),
            "SS" => {
                let arr = val
                    .as_array()
                    .ok_or_else(|| de::Error::custom("expected array for SS"))?;
                let ss: Result<Vec<String>, _> = arr
                    .iter()
                    .map(|v| {
                        v.as_str()
                            .map(|s| s.to_string())
                            .ok_or_else(|| de::Error::custom("expected string in SS"))
                    })
                    .collect();
                Ok(AttributeValue::SS(ss?))
            }
            "NS" => {
                let arr = val
                    .as_array()
                    .ok_or_else(|| de::Error::custom("expected array for NS"))?;
                let ns: Result<Vec<String>, _> = arr
                    .iter()
                    .map(|v| {
                        v.as_str()
                            .map(|s| s.to_string())
                            .ok_or_else(|| de::Error::custom("expected string in NS"))
                    })
                    .collect();
                Ok(AttributeValue::NS(ns?))
            }
            "BS" => {
                let arr = val
                    .as_array()
                    .ok_or_else(|| de::Error::custom("expected array for BS"))?;
                let mut decoded = Vec::with_capacity(arr.len());
                for item in arr {
                    let encoded = item
                        .as_str()
                        .ok_or_else(|| de::Error::custom("expected string in BS"))?;
                    decoded.push(
                        BASE64
                            .decode(encoded)
                            .map_err(|e| de::Error::custom(format!("invalid base64: {e}")))?,
                    );
                }
                Ok(AttributeValue::BS(decoded))
            }
            "L" => {
                let arr = val
                    .as_array()
                    .ok_or_else(|| de::Error::custom("expected array for L"))?;
                let list: Result<Vec<AttributeValue>, _> = arr
                    .iter()
                    .map(|v| serde_json::from_value(v.clone()).map_err(de::Error::custom))
                    .collect();
                Ok(AttributeValue::L(list?))
            }
            "M" => {
                let map_val = val
                    .as_object()
                    .ok_or_else(|| de::Error::custom("expected object for M"))?;
                let mut result = std::collections::HashMap::new();
                for (k, v) in map_val {
                    let av: AttributeValue =
                        serde_json::from_value(v.clone()).map_err(de::Error::custom)?;
                    result.insert(k.clone(), av);
                }
                Ok(AttributeValue::M(result))
            }
            _ => unreachable!(),
        }
    }
}

/// A BOOL or NULL member's value read as a boolean, and whether it was
/// written as a string.
#[derive(Clone, Copy)]
struct LenientBool {
    value: bool,
    from_string: bool,
}

/// Read a BOOL or NULL member's value the way DynamoDB does: JSON `true` and
/// `false`, or one of the strings it takes for them in any case (`true`, `t`,
/// `yes`, `y` and `1`; `false`, `f`, `no`, `n` and `0`). Anything else is a
/// malformed request, in DynamoDB's words for what was found: another string,
/// a number, a list (no message at all) or a map (captured eu-west-2 and
/// us-east-1, 2026-10-10). The error carries the serialisation marker.
fn read_lenient_bool(value: &serde_json::Value) -> Result<LenientBool, String> {
    let refused = |message: &str| format!("{}{message}", crate::serde_errors::SERIALIZATION_MARKER);
    match value {
        serde_json::Value::Bool(value) => Ok(LenientBool {
            value: *value,
            from_string: false,
        }),
        serde_json::Value::String(text) => match lenient_bool_text(text) {
            Some(value) => Ok(LenientBool {
                value,
                from_string: true,
            }),
            None => Err(refused("Unexpected token received from parser")),
        },
        // A number is named by how it was written: a whole number of any size
        // is NUMBER_VALUE and one with a point or an exponent DECIMAL_VALUE
        // (captured eu-west-2 and us-east-1, 2026-10-10). serde_json reads
        // `-0` and integers past 64 bits as floats, so the wire seams rewrite
        // those first (`serde_errors::boolean_numbers_as_written`) and a float
        // here was written as one.
        serde_json::Value::Number(n) if n.is_f64() => {
            Err(refused("DECIMAL_VALUE cannot be converted to Boolean"))
        }
        serde_json::Value::Number(_) => Err(refused("NUMBER_VALUE cannot be converted to Boolean")),
        serde_json::Value::Array(_) => Err(refused("")),
        serde_json::Value::Object(_) => Err(refused(
            "Start of structure or map found where not expected",
        )),
        // A null member counts as absent and is never read.
        serde_json::Value::Null => Err(refused("Unexpected token received from parser")),
    }
}

/// Deserialise a request's own boolean member (`ConsistentRead`,
/// `ScanIndexForward`, a legacy condition's `Exists`) the way DynamoDB reads
/// it: by the rule a `BOOL` member follows, so the strings that read as a
/// boolean there read as one here too, and JSON `null` leaves the member
/// unset (captured eu-west-2 and us-east-1, 2026-10-10). A list is refused as
/// the member's own type, as the request's other members are.
pub(crate) fn deserialize_lenient_flag<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<bool>, D::Error> {
    match serde_json::Value::deserialize(deserializer)? {
        serde_json::Value::Null => Ok(None),
        serde_json::Value::Array(_) => Err(de::Error::custom(format!(
            "{}Unrecognized collection type class java.lang.Boolean",
            crate::serde_errors::SERIALIZATION_MARKER
        ))),
        value => read_lenient_bool(&value)
            .map(|flag| Some(flag.value))
            .map_err(de::Error::custom),
    }
}

/// The malformed-request error, marked for a `SerializationException`, for the
/// first `BOOL` or `NULL` member, or `Exists`, in a legacy condition map
/// (`QueryFilter`, `ScanFilter`, `KeyConditions`, `Expected`) that doesn't read
/// as a boolean; `None` when every one reads. DynamoDB fails the whole request
/// as it is read (captured eu-west-2 and us-east-1, 2026-10-10), and the map's
/// values are read later than the request's own members, so the request
/// decoders look here first.
pub(crate) fn malformed_condition_boolean(
    conditions: Option<&serde_json::Value>,
) -> Option<String> {
    let malformed = |message: String| {
        message
            .starts_with(crate::serde_errors::SERIALIZATION_MARKER)
            .then_some(message)
    };
    for condition in conditions?.as_object()?.values() {
        let Some(condition) = condition.as_object() else {
            continue;
        };
        if let Some(exists) = condition.get("Exists")
            && let Err(e) = deserialize_lenient_flag(exists.clone())
            && let Some(message) = malformed(e.to_string())
        {
            return Some(message);
        }
        let values = condition.get("Value").into_iter().chain(
            condition
                .get("AttributeValueList")
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten(),
        );
        for value in values {
            if let Err(e) = serde_json::from_value::<AttributeValue>(value.clone())
                && let Some(message) = malformed(e.to_string())
            {
                return Some(message);
            }
        }
    }
    None
}

/// Whether a request's own boolean member written as a string reads as a
/// boolean, by the rule [`deserialize_lenient_flag`] follows.
#[cfg(any(feature = "http-server", feature = "wasm-sqlite", test))]
pub(crate) fn reads_as_flag(text: &str) -> bool {
    lenient_bool_text(text).is_some()
}

/// The boolean a string reads as by DynamoDB's rule, in any case: `true`, `t`,
/// `yes`, `y` and `1`, or `false`, `f`, `no`, `n` and `0`. Nothing is trimmed.
fn lenient_bool_text(text: &str) -> Option<bool> {
    match text.to_ascii_lowercase().as_str() {
        "true" | "t" | "yes" | "y" | "1" => Some(true),
        "false" | "f" | "no" | "n" | "0" => Some(false),
        _ => None,
    }
}

/// Validate a number string during AttributeValue deserialization.
///
/// Returns DynamoDB-matching error messages for invalid numbers.
/// Error messages are returned WITHOUT the VALIDATION: prefix since they
/// bypass the normal validation flow; the server routes them based on
/// message content (see `crate::serde_errors::deserialize`).
fn validate_number_in_deser(n: &str, layer: crate::validation::ValueLayer) -> Result<(), String> {
    // The layer the value is read under decides the rule: the current
    // layer's numbers are refused inside the envelope on PutItem, UpdateItem
    // and DeleteItem, so they carry the request-validation marker, and the
    // older layer's stay bare.
    let marker = match layer {
        crate::validation::ValueLayer::Current => crate::serde_errors::REQUEST_VALIDATION_MARKER,
        crate::validation::ValueLayer::Older => crate::serde_errors::VALIDATION_MARKER,
    };
    match layer.check_number(n) {
        Ok(()) => Ok(()),
        Err(crate::errors::DynoxideError::ValidationException(m)) => Err(format!("{marker}{m}")),
        Err(e) => Err(format!("{marker}{e}")),
    }
}

/// Validate a number by the current validation layer's rule: the stored-number
/// rule, with an empty string refused as `... numeric value: `, colon and
/// space (captured eu-west-2, us-east-1, eu-west-1 and us-west-2, 2026-10-09).
pub(crate) fn validate_current_number(
    num_str: &str,
) -> std::result::Result<(), crate::errors::DynoxideError> {
    if num_str.is_empty() {
        return Err(crate::errors::DynoxideError::ValidationException(
            "The parameter cannot be converted to a numeric value: ".to_string(),
        ));
    }
    validate_dynamo_number(num_str)
}

// ---------------------------------------------------------------------------
// Number sort key normalization
// ---------------------------------------------------------------------------

/// Normalize a DynamoDB number string into a comparable string that sorts
/// correctly in SQLite TEXT collation.
///
/// Encoding scheme:
/// - Positive numbers: "1" + zero-padded exponent (4 digits, offset by 5000) + normalized mantissa
/// - Zero: "1" + "5000" + "0" (padded)
/// - Negative numbers: "0" + complement of (exponent + mantissa) so they sort before positives
///
/// DynamoDB numbers: up to 38 digits of precision, range ~-1E+126 to ~+1E+126.
pub fn normalize_number_for_sort(num_str: &str) -> String {
    let trimmed = num_str.trim();

    if trimmed.is_empty() || trimmed == "0" || trimmed == "-0" || trimmed == "0.0" {
        return zero_encoding();
    }

    let negative = trimmed.starts_with('-');
    let abs_str = if negative { &trimmed[1..] } else { trimmed };

    // Parse into mantissa digits and exponent
    let (mantissa_digits, exponent) = parse_number_parts(abs_str);

    if mantissa_digits.is_empty() || mantissa_digits.iter().all(|&d| d == 0) {
        return zero_encoding();
    }

    if negative {
        encode_negative(&mantissa_digits, exponent)
    } else {
        encode_positive(&mantissa_digits, exponent)
    }
}

/// DynamoDB's refusal of `type_key`'s member when it is an empty set.
pub(crate) fn empty_set_refusal(
    type_key: &str,
    member: &serde_json::Value,
) -> Option<&'static str> {
    if !member.as_array().is_some_and(Vec::is_empty) {
        return None;
    }
    match type_key {
        "SS" => Some("One or more parameter values were invalid: An string set  may not be empty"),
        "NS" => Some("One or more parameter values were invalid: An number set  may not be empty"),
        "BS" => Some("One or more parameter values were invalid: Binary sets should not be empty"),
        _ => None,
    }
}

/// The type members an attribute value can carry.
const TYPE_MEMBERS: [&str; 10] = ["S", "N", "B", "BOOL", "NULL", "SS", "NS", "BS", "L", "M"];

/// Whether a request is read by DynamoDB's older validation layer whatever
/// operation it is: it holds a `BOOL` or `NULL` member, or a legacy
/// condition's `Exists`, written as a string, or an attribute value with more
/// than one member. Every value refusal in such a request takes the older
/// layer's form: bare on PutItem, UpdateItem and DeleteItem, where the current
/// layer gives the envelope, and naming its key in UpdateItem's
/// `ExpressionAttributeValues` (captured eu-west-2 and us-east-1, 2026-10-10).
///
/// A map whose members are all maps is an item or an attribute map, not a
/// value, so an attribute named `S` beside one named `N` doesn't count.
pub(crate) fn reads_on_older_layer(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Object(map) => {
            let string_flag = ["BOOL", "NULL", "Exists"]
                .iter()
                .any(|member| map.get(*member).is_some_and(serde_json::Value::is_string));
            let members: Vec<&serde_json::Value> = TYPE_MEMBERS
                .iter()
                .filter_map(|member| map.get(*member))
                .filter(|member| !member.is_null())
                .collect();
            string_flag
                || (members.len() > 1 && members.iter().any(|member| !member.is_object()))
                || map.values().any(reads_on_older_layer)
        }
        serde_json::Value::Array(items) => items.iter().any(reads_on_older_layer),
        _ => false,
    }
}

/// The validation layer values are being read under on this thread: the one
/// a wire seam set for the request in hand, or the current layer outside one.
pub(crate) fn current_value_layer() -> crate::validation::ValueLayer {
    VALUE_LAYER.with(std::cell::Cell::get)
}

thread_local! {
    /// The validation layer the values being decoded are read under. Set
    /// around a request's deserialisation, which runs synchronously; outside
    /// one it is the current layer.
    static VALUE_LAYER: std::cell::Cell<crate::validation::ValueLayer> =
        const { std::cell::Cell::new(crate::validation::ValueLayer::Current) };
}

/// Run `read` with values decoded under `layer`, restoring the layer in force
/// before, however `read` ends. `read` must not await: the layer belongs to
/// the thread.
pub(crate) fn with_value_layer<T>(
    layer: crate::validation::ValueLayer,
    read: impl FnOnce() -> T,
) -> T {
    struct Restore(crate::validation::ValueLayer);
    impl Drop for Restore {
        fn drop(&mut self) {
            VALUE_LAYER.with(|cell| cell.set(self.0));
        }
    }
    let _restore = Restore(VALUE_LAYER.with(|cell| cell.replace(layer)));
    read()
}

/// Run `future` with values read under `layer`, set around each of its polls
/// and restored after, so the layer holds across the future's awaits without
/// reaching anything polled in between. The wasm engine's single thread runs
/// other requests between those polls.
#[cfg(any(feature = "wasm-sqlite", test))]
pub(crate) async fn with_value_layer_async<F: std::future::Future>(
    layer: crate::validation::ValueLayer,
    future: F,
) -> F::Output {
    struct Layered<F> {
        layer: crate::validation::ValueLayer,
        future: std::pin::Pin<Box<F>>,
    }
    impl<F: std::future::Future> std::future::Future for Layered<F> {
        type Output = F::Output;
        fn poll(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<F::Output> {
            let layer = self.layer;
            with_value_layer(layer, || self.future.as_mut().poll(cx))
        }
    }
    Layered {
        layer,
        future: Box::pin(future),
    }
    .await
}

/// Run `read` with numbers read as PartiQL parameters are: the older layer's
/// rule, which [`validate_parameter_number`] holds.
pub(crate) fn reading_parameters<T>(read: impl FnOnce() -> T) -> T {
    with_value_layer(crate::validation::ValueLayer::Older, read)
}

/// Deserialise an operation's `ExpressionAttributeValues` on the older
/// validation layer, which refuses a number it can't store inside the
/// `ExpressionAttributeValues contains invalid value: ... for key :k` wrapper
/// (TransactWriteItems in three of four regions, Scan in seven of ten;
/// 2026-10-09). Every other value error passes through as it is.
pub(crate) fn deserialize_older_expression_values<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<HashMap<String, AttributeValue>>, D::Error> {
    let Some(raw) =
        Option::<serde_json::Map<String, serde_json::Value>>::deserialize(deserializer)?
    else {
        return Ok(None);
    };
    let mut values = HashMap::with_capacity(raw.len());
    for (key, value) in raw {
        let read = with_value_layer(crate::validation::ValueLayer::Older, || {
            serde_json::from_value::<AttributeValue>(value)
        });
        match read {
            Ok(av) => {
                values.insert(key, av);
            }
            Err(e) => {
                use crate::serde_errors::{ValueRefusal, named_value_refusal, value_refusal};
                let msg = e.to_string();
                return Err(de::Error::custom(match value_refusal(&msg) {
                    ValueRefusal::Number(inner) => format!(
                        "{}{}",
                        crate::serde_errors::VALIDATION_MARKER,
                        named_value_refusal(inner, &key)
                    ),
                    _ => msg,
                }));
            }
        }
    }
    Ok(Some(values))
}

/// Deserialise UpdateItem's `ExpressionAttributeValues`, naming the key of a
/// NULL refused as it was parsed (a string that reads as false, or false
/// beside another member) the way DynamoDB does: `ExpressionAttributeValues
/// contains invalid value: ... for key :v` (captured eu-west-2 and us-east-1,
/// 2026-10-10). Every other value error passes through as it is.
pub(crate) fn deserialize_named_null_refusals<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<HashMap<String, AttributeValue>>, D::Error> {
    deserialize_named_refusals(deserializer, false, true)
}

/// Deserialise Query's `ExpressionAttributeValues`, naming the key of an empty
/// value as well as of a NULL refused as it was parsed (captured eu-west-2 and
/// us-east-1, 2026-10-10: `{":v": {"S": null}}` and `{":v": {"NULL": "no"}}`).
pub(crate) fn deserialize_named_empty_and_null_refusals<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<HashMap<String, AttributeValue>>, D::Error> {
    deserialize_named_refusals(deserializer, true, false)
}

/// Read `ExpressionAttributeValues`, naming the key of a NULL refused as it was
/// parsed, of an empty value when `name_empty`, and, when `name_older` and the
/// request is read on the older layer, of every other value refusal.
fn deserialize_named_refusals<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
    name_empty: bool,
    name_older: bool,
) -> std::result::Result<Option<HashMap<String, AttributeValue>>, D::Error> {
    let Some(raw) =
        Option::<serde_json::Map<String, serde_json::Value>>::deserialize(deserializer)?
    else {
        return Ok(None);
    };
    let mut values = HashMap::with_capacity(raw.len());
    for (key, value) in raw {
        match serde_json::from_value::<AttributeValue>(value) {
            Ok(av) => {
                values.insert(key, av);
            }
            Err(e) => {
                use crate::serde_errors::{
                    EMPTY_VALUE, MORE_THAN_ONE_TYPE, VALIDATION_MARKER, ValueRefusal,
                    named_value_refusal, value_refusal,
                };
                let msg = e.to_string();
                // UpdateItem, read on the older layer, names the key of every
                // value refusal: a number, a value with more than one member
                // (which puts the request on that layer), an empty set beside
                // another member (captured eu-west-2 and us-east-1,
                // 2026-10-10).
                let older =
                    name_older && current_value_layer() == crate::validation::ValueLayer::Older;
                return Err(de::Error::custom(match value_refusal(&msg) {
                    ValueRefusal::ParsedNull(inner) => {
                        format!("{VALIDATION_MARKER}{}", named_value_refusal(inner, &key))
                    }
                    ValueRefusal::Empty if name_empty => {
                        format!(
                            "{VALIDATION_MARKER}{}",
                            named_value_refusal(EMPTY_VALUE, &key)
                        )
                    }
                    ValueRefusal::MoreThanOneType if name_older => format!(
                        "{VALIDATION_MARKER}{}",
                        named_value_refusal(MORE_THAN_ONE_TYPE, &key)
                    ),
                    ValueRefusal::EmptySet(inner) if name_older => {
                        format!("{VALIDATION_MARKER}{}", named_value_refusal(inner, &key))
                    }
                    ValueRefusal::Number(inner) if older => {
                        format!("{VALIDATION_MARKER}{}", named_value_refusal(inner, &key))
                    }
                    _ => msg,
                }));
            }
        }
    }
    Ok(Some(values))
}

/// Deserialise a statement's `Parameters`, reading their numbers as PartiQL
/// parameters.
pub(crate) fn deserialize_parameters<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<Vec<AttributeValue>>, D::Error> {
    reading_parameters(|| Option::<Vec<AttributeValue>>::deserialize(deserializer))
}

/// Validate a PartiQL parameter's number. DynamoDB reads it as Java's
/// `BigDecimal(String)` does, so an exponent outside an `int`, or a scale (the
/// digits after the point less the exponent) outside one, is a value it cannot
/// convert. Within those, a precision less scale at an `int`'s limit is an
/// underflow, as for a PartiQL literal, and otherwise the stored-number rule
/// applies: `1e2147483647` is an underflow here and an overflow in an item
/// (captured eu-west-2 and us-east-1, 2026-10-09).
pub(crate) fn validate_parameter_number(
    num_str: &str,
) -> std::result::Result<(), crate::errors::DynoxideError> {
    if num_str.is_empty() || !is_well_formed_dynamo_number(num_str) {
        return validate_dynamo_number(num_str);
    }
    let unconvertible = || {
        crate::errors::DynoxideError::ValidationException(format!(
            "The parameter cannot be converted to a numeric value: {num_str}"
        ))
    };
    let unsigned = num_str.trim_start_matches(['+', '-']);
    let (coefficient, exponent) = match unsigned.find(['e', 'E']) {
        Some(at) => (
            &unsigned[..at],
            unsigned[at + 1..]
                .parse::<i32>()
                .map_err(|_| unconvertible())?,
        ),
        None => (unsigned, 0),
    };
    let fraction_digits = coefficient
        .split_once('.')
        .map_or(0, |(_, fraction)| fraction.len());
    let scale = i64::try_from(fraction_digits).unwrap_or(i64::MAX) - i64::from(exponent);
    if i32::try_from(scale).is_err() {
        return Err(unconvertible());
    }
    let significant = coefficient
        .chars()
        .filter(char::is_ascii_digit)
        .skip_while(|&d| d == '0')
        .count()
        .max(1);
    if i64::try_from(significant).unwrap_or(i64::MAX) - scale >= i64::from(i32::MAX) {
        return Err(crate::errors::DynoxideError::ValidationException(
            "Number underflow. Attempting to store a number with magnitude smaller than \
             supported range"
                .to_string(),
        ));
    }
    validate_dynamo_number(num_str)
}

/// Validate a DynamoDB number string against DynamoDB's constraints:
/// - Up to 38 significant digits
/// - Magnitude at most 9.9999999999999999999999999999999999999E+125
/// - Positive values must be at least 1E-130
/// - Negative values must be at most -1E-130
/// - Zero is held to the same range, as if its last written digit were a 1
pub fn validate_dynamo_number(
    num_str: &str,
) -> std::result::Result<(), crate::errors::DynoxideError> {
    if num_str.is_empty() {
        return Err(crate::errors::DynoxideError::ValidationException(
            "The parameter cannot be converted to a numeric value".to_string(),
        ));
    }

    // DynamoDB accepts a specific numeric grammar and rejects everything else,
    // including any surrounding or internal whitespace. Verified against real
    // DynamoDB (see the unit tests below and the dynamodb-conformance suite):
    //   sign?  coefficient  exponent?
    //   coefficient = at least one digit, at most one '.' (e.g. 5, 5., .5, +1.5)
    //   exponent    = ('e'|'E') sign? at least one digit (e.g. e2, E+3, e-130)
    // Accepts: +5, -7, +.5, 5., 1e+2, 1.5E+3, 1E-130, 00042
    // Rejects: +e2, 1+2, 1.2.3, ++5, 1e, ., NaN, "1_000", " 5"
    if !is_well_formed_dynamo_number(num_str) {
        return Err(crate::errors::DynoxideError::ValidationException(format!(
            "The parameter cannot be converted to a numeric value: {num_str}"
        )));
    }

    // DynamoDB reads the exponent as a 64-bit integer, and one that does not
    // fit is a value it cannot convert rather than an overflow. Inside that,
    // an exponent of any size is range-checked below (captured eu-west-2,
    // 2026-10-08).
    if num_str
        .find(['e', 'E'])
        .is_some_and(|pos| num_str[pos + 1..].parse::<i64>().is_err())
    {
        return Err(crate::errors::DynoxideError::ValidationException(format!(
            "The parameter cannot be converted to a numeric value: {num_str}"
        )));
    }

    // parse_number_parts ignores the sign characters, so the magnitude checks
    // below hold for both signs.
    let (mantissa_digits, exponent) = parse_number_parts(num_str);

    let exponent = if mantissa_digits.is_empty() || mantissa_digits.iter().all(|&d| d == 0) {
        // Zero has no magnitude of its own, but DynamoDB still range-checks
        // it, as if its last written digit were a 1: `0e126` overflows like
        // `1e126`, and `0.0e-130` underflows like `0.1e-130` (captured
        // eu-west-2, 2026-10-08).
        let (coeff, written_exponent) = split_exponent(num_str);
        let frac_digits = coeff.find('.').map_or(0, |dot| coeff.len() - dot - 1);
        written_exponent
            .saturating_sub(frac_digits as i64)
            .saturating_add(1)
    } else {
        // Check significant digits (mantissa_digits has leading/trailing zeros already stripped)
        if mantissa_digits.len() > 38 {
            return Err(crate::errors::DynoxideError::ValidationException(
                "Attempting to store more than 38 significant digits in a Number".to_string(),
            ));
        }
        i64::from(exponent)
    };

    // Check magnitude: exponent represents the power such that value = 0.mantissa * 10^exponent
    // Max magnitude: 9.999...E+125 means exponent = 126 (since 0.999... * 10^126 = 9.99...E+125)
    if exponent > 126 {
        return Err(crate::errors::DynoxideError::ValidationException(
            "Number overflow. Attempting to store a number with magnitude larger than supported range"
                .to_string(),
        ));
    }

    // Check underflow
    // Min positive: 1E-130 means exponent = -129 (since 0.1 * 10^-129 = 1E-130)
    // But with more digits, exponent can be lower, e.g. 1.0E-130 has (mantissa=[1], exponent=-129)
    // Actually, the smallest representable is 1E-130. In our representation, 1E-130 = 0.1 * 10^-129
    // So exponent = -129 with mantissa [1].
    // For 1E-131 = 0.1 * 10^-130, exponent = -130 - that's too small.
    if exponent < -129 {
        return Err(crate::errors::DynoxideError::ValidationException(
            "Number underflow. Attempting to store a number with magnitude smaller than supported range"
                .to_string(),
        ));
    }

    Ok(())
}

/// Returns true when `s` matches DynamoDB's numeric grammar exactly:
/// `sign? coefficient exponent?` where the coefficient carries at least one
/// digit and at most one `.`, and the exponent (if present) carries at least
/// one digit. No whitespace or stray characters are tolerated. This mirrors
/// what real DynamoDB accepts (verified against AWS).
fn is_well_formed_dynamo_number(s: &str) -> bool {
    let bytes = s.as_bytes();
    let n = bytes.len();
    let mut i = 0;

    // Optional leading sign.
    if i < n && (bytes[i] == b'+' || bytes[i] == b'-') {
        i += 1;
    }

    // Coefficient: digits with at most one decimal point, at least one digit.
    let mut coeff_digits = 0usize;
    let mut dots = 0usize;
    while i < n && (bytes[i].is_ascii_digit() || bytes[i] == b'.') {
        if bytes[i] == b'.' {
            dots += 1;
            if dots > 1 {
                return false;
            }
        } else {
            coeff_digits += 1;
        }
        i += 1;
    }
    if coeff_digits == 0 {
        return false;
    }

    // Optional exponent: 'e'/'E', optional sign, at least one digit.
    if i < n && (bytes[i] == b'e' || bytes[i] == b'E') {
        i += 1;
        if i < n && (bytes[i] == b'+' || bytes[i] == b'-') {
            i += 1;
        }
        let mut exp_digits = 0usize;
        while i < n && bytes[i].is_ascii_digit() {
            exp_digits += 1;
            i += 1;
        }
        if exp_digits == 0 {
            return false;
        }
    }

    // Anything left over (stray chars, trailing whitespace) is invalid.
    i == n
}

/// Byte cost of a number, as DynamoDB accounts for it in an item's size.
///
/// One byte, plus `ceil(integer significant digits / 2)` and
/// `ceil(fraction significant digits / 2)`, over the value with leading and
/// trailing zeros trimmed. Measured byte-exact against eu-west-2 by bisecting
/// the 400KB item-size gate.
///
/// Counting significant digits rather than characters is what makes the answer
/// survive storage: DynamoDB expands scientific notation when it stores a
/// number, so a measure taken over the digits of the string would give one
/// answer for the request and another for the stored row. Every write path
/// checks the limit somewhere either side of that expansion, and they can only
/// agree if the measure does not move.
pub fn number_size(num_str: &str) -> usize {
    let trimmed = num_str.trim();
    let negative = trimmed.starts_with('-');
    let abs = trimmed.strip_prefix(['-', '+']).unwrap_or(trimmed);
    let (mantissa, exponent) = parse_number_parts(abs);

    // `exponent` is how many significant digits fall before the decimal point:
    // negative for a pure fraction, past the end of the mantissa for a value
    // whose trailing zeros were trimmed. Either way the split is a clamp.
    let int_digits = exponent.clamp(0, mantissa.len() as i32) as usize;
    let frac_digits = mantissa.len() - int_digits;

    // A negative number costs a byte more than the same magnitude positive:
    // `-42` measures 3 where `42` measures 2. Zero is never negative, whatever
    // the literal said, so `-0` costs the same as `0`.
    let sign = usize::from(negative && !mantissa.is_empty());

    int_digits.div_ceil(2) + frac_digits.div_ceil(2) + 1 + sign
}

/// Normalize a DynamoDB number string to its canonical form.
///
/// DynamoDB normalises numbers when storing them:
/// - Leading zeros are stripped (`0042` → `42`)
/// - Trailing zeros after decimal are stripped (`1.200` → `1.2`)
/// - Scientific notation is expanded to full decimal form
/// - Zero is represented as `0`
pub fn normalize_dynamo_number(num_str: &str) -> String {
    let trimmed = num_str.trim();
    if trimmed.is_empty() {
        return "0".to_string();
    }
    // The expansion below walks the exponent one character at a time, so an
    // exponent DynamoDB would never accept turns a short string into a
    // multi-gigabyte one. `1e-2147483647` is nine characters in and about two
    // gigabytes out. Outside the range DynamoDB itself accepts there is no
    // canonical form worth producing, so hand the input back untouched.
    if validate_dynamo_number(trimmed).is_err() {
        return trimmed.to_string();
    }

    let negative = trimmed.starts_with('-');
    let abs_str = if negative {
        &trimmed[1..]
    } else {
        trimmed.trim_start_matches('+')
    };

    let (mantissa_digits, exponent) = parse_number_parts(abs_str);

    // Zero
    if mantissa_digits.is_empty() {
        return "0".to_string();
    }

    // Reconstruct: mantissa_digits represent the significant digits,
    // exponent is the power of 10 such that value = 0.mantissa * 10^exponent
    // e.g., 12345 → mantissa=[1,2,3,4,5], exponent=5 → 12345
    // e.g., 0.00123 → mantissa=[1,2,3], exponent=-2 → 0.00123
    let num_digits = mantissa_digits.len() as i32;
    let int_digits = exponent; // number of digits before the decimal point

    let mut result = String::new();
    if negative {
        result.push('-');
    }

    if int_digits <= 0 {
        // Pure fraction: 0.000...digits
        result.push_str("0.");
        for _ in 0..(-int_digits) {
            result.push('0');
        }
        for &d in &mantissa_digits {
            result.push((b'0' + d) as char);
        }
    } else if int_digits >= num_digits {
        // Pure integer: digits followed by trailing zeros
        for &d in &mantissa_digits {
            result.push((b'0' + d) as char);
        }
        for _ in 0..(int_digits - num_digits) {
            result.push('0');
        }
    } else {
        // Mixed: some digits before decimal, some after
        let int_part = int_digits as usize;
        for &d in &mantissa_digits[..int_part] {
            result.push((b'0' + d) as char);
        }
        result.push('.');
        for &d in &mantissa_digits[int_part..] {
            result.push((b'0' + d) as char);
        }
    }

    result
}

fn zero_encoding() -> String {
    // Zero sorts between negative (prefix "0") and positive (prefix "2")
    format!("1{}{}", "0".repeat(4), "0".repeat(40))
}

/// The exponent as four digits, offset by 5000.
///
/// DynamoDB's range only needs -129 to 126, but a library caller can reach the
/// encoder with a number it never validated. One outside -5000 to 4999 sorts at
/// the matching extreme rather than overflowing into an unrelated key.
fn encoded_exponent(exponent: i32) -> u16 {
    (exponent.clamp(-5000, 4999) + 5000) as u16
}

fn encode_positive(mantissa: &[u8], exponent: i32) -> String {
    let exp_encoded = encoded_exponent(exponent);
    let mantissa_str = mantissa_to_string(mantissa, 40);
    format!("2{exp_encoded:04}{mantissa_str}")
}

fn encode_negative(mantissa: &[u8], exponent: i32) -> String {
    // For negatives, we complement everything so larger absolute values sort first (smaller)
    let exp_encoded = 9999 - encoded_exponent(exponent);
    let mantissa_str = complement_mantissa(mantissa, 40);
    format!("0{exp_encoded:04}{mantissa_str}")
}

/// Parse a non-negative number string into (mantissa digits, exponent).
/// Mantissa is normalized: first digit is non-zero, exponent is the power of 10
/// such that the number = 0.mantissa * 10^exponent.
///
/// An exponent outside the i32 range saturates at its bound, so a value far
/// outside DynamoDB's range still lands on the right side of it.
pub(crate) fn parse_number_parts(s: &str) -> (Vec<u8>, i32) {
    // Handle scientific notation
    let (coeff, exp_part) = split_exponent(s);

    // Split coefficient into integer and fraction parts
    let (int_part, frac_part) = if let Some(dot) = coeff.find('.') {
        (&coeff[..dot], &coeff[dot + 1..])
    } else {
        (coeff, "")
    };

    // Collect all digits
    let mut digits: Vec<u8> = Vec::new();
    for ch in int_part.chars().chain(frac_part.chars()) {
        if ch.is_ascii_digit() {
            digits.push(ch as u8 - b'0');
        }
    }

    if digits.is_empty() {
        return (vec![], 0);
    }

    // The integer part length gives us the base exponent
    let int_len = int_part.chars().filter(|c| c.is_ascii_digit()).count() as i64;

    // Find first non-zero digit
    let leading_zeros = digits.iter().take_while(|&&d| d == 0).count();
    digits.drain(..leading_zeros);

    // Trim trailing zeros
    while digits.last() == Some(&0) {
        digits.pop();
    }

    if digits.is_empty() {
        return (vec![], 0);
    }

    // exponent = int_len - leading_zeros + exp_part
    // But we need to account for whether leading zeros were in int or frac part
    let exponent = (int_len - leading_zeros as i64).saturating_add(exp_part);
    let exponent = exponent.clamp(i32::MIN.into(), i32::MAX.into()) as i32;

    (digits, exponent)
}

/// Split a number at its `e` or `E` into the coefficient and the exponent
/// written after it, which is 0 when there is none.
///
/// An exponent too large for an i64 saturates at the bound on its side rather
/// than reading as 0, which used to turn `1e99999999999` into 1. Anything
/// else that fails to parse also reads as 0, for callers that have not been
/// through `validate_dynamo_number`.
fn split_exponent(s: &str) -> (&str, i64) {
    let Some(pos) = s.find(['e', 'E']) else {
        return (s, 0);
    };
    let exponent = match s[pos + 1..].parse::<i64>() {
        Ok(exponent) => exponent,
        Err(e) => match e.kind() {
            std::num::IntErrorKind::PosOverflow => i64::MAX,
            std::num::IntErrorKind::NegOverflow => i64::MIN,
            _ => 0,
        },
    };
    (&s[..pos], exponent)
}

fn mantissa_to_string(digits: &[u8], width: usize) -> String {
    let mut s = String::with_capacity(width);
    for &d in digits.iter().take(width) {
        s.push((b'0' + d) as char);
    }
    while s.len() < width {
        s.push('0');
    }
    s
}

fn complement_mantissa(digits: &[u8], width: usize) -> String {
    let mut s = String::with_capacity(width);
    for i in 0..width {
        let d = if i < digits.len() { digits[i] } else { 0 };
        s.push((b'0' + (9 - d)) as char);
    }
    s
}

/// The key attribute a stored key string (see [`AttributeValue::to_key_string`])
/// came from, or `None` for a string that is not one. A number comes back in a
/// form that has the same value, which is all its hash depends on; the
/// sortable encoding keeps 40 digits, more than DynamoDB's 38.
pub(crate) fn key_value_from_key_string(key: &str) -> Option<AttributeValue> {
    let (kind, body) = key.split_once(':')?;
    match kind {
        "S" => Some(AttributeValue::S(body.to_string())),
        "B" => hex_decode(body).map(AttributeValue::B),
        "N" => number_from_sortable(body).map(AttributeValue::N),
        _ => None,
    }
}

/// Undo [`normalize_number_for_sort`]: a sign digit, a four-digit exponent
/// offset by 5000 and a 40-digit mantissa, both complemented for a negative.
fn number_from_sortable(body: &str) -> Option<String> {
    if body.len() != 45 || !body.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let (sign, rest) = body.split_at(1);
    let (exponent, mantissa) = rest.split_at(4);
    let exponent: i32 = exponent.parse().ok()?;
    let (negative, exponent, mantissa): (bool, i32, String) = match sign {
        "1" => return Some("0".to_string()),
        "2" => (false, exponent - 5000, mantissa.to_string()),
        "0" => (
            true,
            (9999 - exponent) - 5000,
            mantissa
                .bytes()
                .map(|b| (b'0' + (9 - (b - b'0'))) as char)
                .collect(),
        ),
        _ => return None,
    };
    let digits = mantissa.trim_end_matches('0');
    if digits.is_empty() {
        return Some("0".to_string());
    }
    Some(format!(
        "{}0.{digits}E{exponent}",
        if negative { "-" } else { "" }
    ))
}

fn hex_decode(hex: &str) -> Option<Vec<u8>> {
    if !hex.len().is_multiple_of(2) {
        return None;
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(hex.get(i..i + 2)?, 16).ok())
        .collect()
}

/// Hex-encode bytes (lowercase) for binary key storage.
fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

// ---------------------------------------------------------------------------
// Item helpers
// ---------------------------------------------------------------------------

/// A DynamoDB item: a map of attribute names to values.
pub type Item = HashMap<String, AttributeValue>;

/// SSE specification for server-side encryption settings.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SseSpecification {
    #[serde(rename = "Enabled", default)]
    pub enabled: Option<bool>,
    #[serde(rename = "SSEType", default)]
    pub sse_type: Option<String>,
    #[serde(rename = "KMSMasterKeyId", default)]
    pub kms_master_key_id: Option<String>,
}

/// DynamoDB Tag (key-value pair attached to a resource).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Tag {
    #[serde(rename = "Key")]
    pub key: String,
    #[serde(rename = "Value")]
    pub value: String,
}

/// Calculate the total size of a DynamoDB item in bytes.
pub fn item_size(item: &Item) -> usize {
    item.iter()
        .map(|(name, value)| name.len() + value.size())
        .sum()
}

/// The bytes DynamoDB bills a vector at: its f32 width, 4 bytes per
/// dimension, whatever numbers it holds. Shared by the base item's billed size
/// and the vector index entry.
pub(crate) fn vector_billed_bytes(dimensions: u32) -> usize {
    4 * dimensions as usize
}

/// The size DynamoDB bills a write or a table read of `item` at, and the
/// figure TableSizeBytes sums, on a table whose vector indexes are
/// `vector_indexes`.
///
/// An attribute a vector index reads costs its name plus
/// [`vector_billed_bytes`], once however many indexes read it; everything else
/// is sized as [`item_size`] sizes it. The 400 KB limit still uses
/// [`item_size`]: DynamoDB sizes the vector by its numbers there. Captured
/// eu-west-2 and us-east-1, 2026-10-09, at 1, 3, 6, 8, 256 and 1,024
/// dimensions, with two indexes on different attributes charged their own
/// dimensions each. Reads were sized by the numbers then too, and moved to this
/// figure by 2026-10-22 (captured again 2026-10-23).
///
/// A value that is not a valid vector for its index is sized as usual. A live
/// write refuses one, so only an item written before the index existed can
/// hold one, and the index skips it too.
pub(crate) fn billed_item_size(item: &Item, vector_indexes: &[VectorIndex]) -> usize {
    if vector_indexes.is_empty() {
        return item_size(item);
    }
    item.iter()
        .map(|(name, value)| {
            let billed = vector_indexes
                .iter()
                .filter(|vix| vix.vector_attribute.attribute_name == *name)
                .find(|vix| vector_f32_values(value, vix.dimensions).is_some())
                .map(|vix| vector_billed_bytes(vix.dimensions));
            name.len() + billed.unwrap_or_else(|| value.size())
        })
        .sum()
}

/// Maximum item size in bytes (400 KB).
pub const MAX_ITEM_SIZE: usize = 400 * 1024;

/// ItemCollectionMetrics returned when `ReturnItemCollectionMetrics: SIZE` is set
/// and the table has local secondary indexes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ItemCollectionMetrics {
    #[serde(rename = "ItemCollectionKey")]
    pub item_collection_key: HashMap<String, AttributeValue>,
    #[serde(rename = "SizeEstimateRangeGB")]
    pub size_estimate_range_gb: Vec<f64>,
}

/// ConsumedCapacity returned when `ReturnConsumedCapacity` is set.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ConsumedCapacity {
    #[serde(rename = "TableName")]
    pub table_name: String,
    #[serde(rename = "CapacityUnits")]
    pub capacity_units: f64,
    #[serde(rename = "ReadCapacityUnits", skip_serializing_if = "Option::is_none")]
    pub read_capacity_units: Option<f64>,
    #[serde(rename = "WriteCapacityUnits", skip_serializing_if = "Option::is_none")]
    pub write_capacity_units: Option<f64>,
    #[serde(rename = "Table", skip_serializing_if = "Option::is_none")]
    pub table: Option<CapacityDetail>,
    #[serde(
        rename = "GlobalSecondaryIndexes",
        skip_serializing_if = "Option::is_none"
    )]
    pub global_secondary_indexes: Option<HashMap<String, CapacityDetail>>,
    #[serde(
        rename = "LocalSecondaryIndexes",
        skip_serializing_if = "Option::is_none"
    )]
    pub local_secondary_indexes: Option<HashMap<String, CapacityDetail>>,
    /// Per-vector-index replication cost. Reported under `INDEXES` alone, and
    /// never folded into `capacity_units`: vector replication is billed in
    /// bytes on its own axis rather than in capacity units.
    #[serde(rename = "VectorIndexes", skip_serializing_if = "Option::is_none")]
    pub vector_indexes: Option<HashMap<String, VectorCapacityDetail>>,
}

/// Per-resource capacity detail.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CapacityDetail {
    #[serde(rename = "CapacityUnits")]
    pub capacity_units: f64,
    #[serde(rename = "ReadCapacityUnits", skip_serializing_if = "Option::is_none")]
    pub read_capacity_units: Option<f64>,
    #[serde(rename = "WriteCapacityUnits", skip_serializing_if = "Option::is_none")]
    pub write_capacity_units: Option<f64>,
}

/// One vector index's replication cost for a write.
///
/// Vector indexes are billed in bytes rather than capacity units, so this
/// carries no `CapacityUnits` and the figure never reaches the response's
/// total. An index a write leaves alone is absent from the map rather than
/// present and zeroed (captured 2026-08-11, eu-west-2).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VectorCapacityDetail {
    #[serde(rename = "VectorWriteRequestBytes")]
    pub vector_write_request_bytes: f64,
}

/// The `ConsumedCapacity` a `SearchVectors` response carries.
///
/// The shape has no `CapacityUnits` and no `TableName`, reads identically under
/// `TOTAL` and `INDEXES`, and is absent under `NONE` (captured 2026-08-11,
/// eu-west-2). Despite the field name, real DynamoDB bills the figure on the
/// data the search read rather than on the request, and does not reproduce it
/// between identical calls; Dynoxide reports a deterministic stand-in instead.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VectorSearchCapacity {
    #[serde(rename = "VectorSearchRequestBytes")]
    pub vector_search_request_bytes: f64,
}

/// The billable minimum for a vector request, in bytes.
///
/// DynamoDB documents a 1KB minimum on vector billing and the capture observed
/// 1024.0 for a three-dimensional fixture, which is this floor rather than a
/// constant.
///
/// The two axes above the floor are not on the same footing. The write figure
/// is captured byte-exact (eu-west-2, 2026-08-18): five fixtures from 3 to 512
/// dimensions fit `4 * dimensions + vector attribute name + item size of the
/// rest of the projected entry` with no residual, and the tests fail on a
/// one-byte error. The search figure has no oracle at all, because real
/// DynamoDB does not reproduce its own, so what Dynoxide reports there is a
/// deterministic stand-in rather than a match.
pub const MIN_VECTOR_REQUEST_BYTES: f64 = 1024.0;

/// The billable byte figure for a vector request measuring `bytes`.
pub fn vector_request_bytes(bytes: usize) -> f64 {
    (bytes as f64).max(MIN_VECTOR_REQUEST_BYTES)
}

/// The transactional capacity multiplier. `TransactWriteItems` and
/// `TransactGetItems` cost twice the equivalent single-item operation, so each
/// item's rounded-up units are doubled (the rounding happens per item, before
/// the multiplier, to match AWS at the KB/4KB boundary).
pub const TRANSACTIONAL_CAPACITY_FACTOR: f64 = 2.0;

/// Calculate write capacity units (1 WCU = 1KB, rounded up).
pub fn write_capacity_units(item_size_bytes: usize) -> f64 {
    ((item_size_bytes as f64) / 1024.0).ceil().max(1.0)
}

/// Write capacity units the base table consumes for a single write.
///
/// DynamoDB sizes a write on the larger of the item's before and after images,
/// so shrinking an item still costs what the old one did. `old_size` is `None`
/// when nothing was there beforehand, and `new_size` is `None` on a delete,
/// where only the old image exists.
pub fn table_write_capacity_units(old_size: Option<usize>, new_size: Option<usize>) -> f64 {
    let old = old_size.map(write_capacity_units);
    let new = new_size.map(write_capacity_units);
    match (old, new) {
        (Some(old), Some(new)) => old.max(new),
        (Some(units), None) | (None, Some(units)) => units,
        // No image either side is not a write DynamoDB would charge for, but a
        // write is never free, so fall back to the one-unit minimum.
        (None, None) => write_capacity_units(0),
    }
}

/// Read capacity units one write's images cost when charged as a read.
///
/// A same-token transactional replay is billed as a read against the image the
/// original write was sized on, which is the larger of the before and after
/// images, but rounded at 4KB rather than 1KB. Captured in eu-west-2: a replayed
/// put that shrank a 9KB item to nothing reports 6, and so does the replay of
/// the write that grew it, so neither side alone explains the figure.
pub fn table_read_capacity_units(old_size: Option<usize>, new_size: Option<usize>) -> f64 {
    let larger = old_size.unwrap_or(0).max(new_size.unwrap_or(0));
    read_capacity_units(larger)
}

/// Calculate read capacity units assuming strongly consistent reads
/// (1 RCU per 4KB, rounded up). Used when ConsistentRead is true or
/// when the read type is not specified.
pub fn read_capacity_units(item_size_bytes: usize) -> f64 {
    ((item_size_bytes as f64) / 4096.0).ceil().max(1.0)
}

/// Calculate read capacity units accounting for consistency mode.
///
/// Strongly consistent: 1 RCU per 4KB, rounded up.
/// Eventually consistent: 0.5 RCU per 4KB (half the strongly consistent rate).
pub fn read_capacity_units_with_consistency(item_size_bytes: usize, consistent: bool) -> f64 {
    let strongly = read_capacity_units(item_size_bytes);
    if consistent { strongly } else { strongly / 2.0 }
}

/// Whether a `ReturnConsumedCapacity` value asks for a figure at all.
///
/// Every `consumed_capacity*` builder below throws its inputs away unless the
/// mode is `TOTAL` or `INDEXES`, so work done only to feed them is wasted in
/// every other mode, and `NONE` is the default. Callers that can skip that work
/// ask here rather than each spelling the comparison out, so the set of modes
/// that mean "yes" is written down once.
pub fn capacity_wanted(mode: Option<&str>) -> bool {
    matches!(mode, Some("TOTAL") | Some("INDEXES"))
}

/// Whether a `ReturnConsumedCapacity` mode carries the vector write map.
///
/// Narrower than [`capacity_wanted`], because the per-index vector map reaches
/// the wire under `INDEXES` alone (captured 2026-08-11, eu-west-2); `TOTAL`
/// asks for capacity and still carries no vector fields. The sizing costs a
/// derivation of the old image per index, so the fan-out asks here and skips
/// the work rather than doing it and having the response builder drop it. Both
/// ends read the same predicate so they cannot disagree about which modes mean
/// yes.
pub fn vector_capacity_wanted(mode: Option<&str>) -> bool {
    matches!(mode, Some("INDEXES"))
}

/// Build a `ConsumedCapacity` for a simple table operation.
pub fn consumed_capacity(
    table_name: &str,
    capacity_units: f64,
    mode: &Option<String>,
) -> Option<ConsumedCapacity> {
    let mode = mode.as_deref().unwrap_or("NONE");
    match mode {
        "TOTAL" => Some(ConsumedCapacity {
            table_name: table_name.to_string(),
            capacity_units,
            table: None,
            global_secondary_indexes: None,
            local_secondary_indexes: None,
            ..Default::default()
        }),
        "INDEXES" => Some(ConsumedCapacity {
            table_name: table_name.to_string(),
            capacity_units,
            table: Some(CapacityDetail {
                capacity_units,
                ..Default::default()
            }),
            global_secondary_indexes: None,
            local_secondary_indexes: None,
            ..Default::default()
        }),
        _ => None,
    }
}

/// Build a `ConsumedCapacity` with per-GSI breakdown for INDEXES mode.
pub fn consumed_capacity_with_indexes(
    table_name: &str,
    table_units: f64,
    gsi_units: &HashMap<String, f64>,
    mode: &Option<String>,
) -> Option<ConsumedCapacity> {
    consumed_capacity_with_secondary_indexes(
        table_name,
        table_units,
        gsi_units,
        &HashMap::new(),
        mode,
    )
}

/// Build a `ConsumedCapacity` with per-GSI and per-LSI breakdown for INDEXES mode.
pub fn consumed_capacity_with_secondary_indexes(
    table_name: &str,
    table_units: f64,
    gsi_units: &HashMap<String, f64>,
    lsi_units: &HashMap<String, f64>,
    mode: &Option<String>,
) -> Option<ConsumedCapacity> {
    secondary_index_capacity(
        table_name,
        table_units,
        gsi_units,
        lsi_units,
        mode,
        CapacityAxis::None,
    )
}

/// Whether a response mirrors its units into a named axis alongside
/// `CapacityUnits`.
///
/// Single-item and PartiQL responses report `CapacityUnits` alone at every
/// level. Transactional responses mirror the units into `WriteCapacityUnits`
/// at every level, including each index arm. Both shapes are captured against
/// real DynamoDB.
#[derive(Clone, Copy, PartialEq)]
enum CapacityAxis {
    None,
    Write,
}

impl CapacityAxis {
    /// The `WriteCapacityUnits` value for a detail carrying `units`.
    fn write_units(self, units: f64) -> Option<f64> {
        match self {
            Self::None => None,
            Self::Write => Some(units),
        }
    }
}

/// Shared body for the per-index capacity builders. `table_units` carries the
/// transactional factor already, when one applies; the index maps never do.
fn secondary_index_capacity(
    table_name: &str,
    table_units: f64,
    gsi_units: &HashMap<String, f64>,
    lsi_units: &HashMap<String, f64>,
    mode: &Option<String>,
    axis: CapacityAxis,
) -> Option<ConsumedCapacity> {
    let total = table_units + gsi_units.values().sum::<f64>() + lsi_units.values().sum::<f64>();

    match mode.as_deref().unwrap_or("NONE") {
        "INDEXES" => Some(ConsumedCapacity {
            table_name: table_name.to_string(),
            capacity_units: total,
            write_capacity_units: axis.write_units(total),
            table: Some(CapacityDetail {
                capacity_units: table_units,
                write_capacity_units: axis.write_units(table_units),
                ..Default::default()
            }),
            global_secondary_indexes: capacity_detail_map(gsi_units, axis),
            local_secondary_indexes: capacity_detail_map(lsi_units, axis),
            ..Default::default()
        }),
        "TOTAL" => Some(ConsumedCapacity {
            table_name: table_name.to_string(),
            capacity_units: total,
            write_capacity_units: axis.write_units(total),
            ..Default::default()
        }),
        _ => None,
    }
}

/// Build a `ConsumedCapacity` carrying the classic index arms and the vector
/// arm together.
///
/// The single mode argument is the point: the classic builder and the vector
/// attach each need one, and passing them separately leaves a caller free to
/// build under one mode and attach under another, which would put a
/// `VectorIndexes` map on a `TOTAL`-shaped response.
pub fn consumed_capacity_with_vector_indexes(
    table_name: &str,
    table_units: f64,
    gsi_units: &HashMap<String, f64>,
    lsi_units: &HashMap<String, f64>,
    vector_bytes: &HashMap<String, f64>,
    mode: &Option<String>,
) -> Option<ConsumedCapacity> {
    attach_vector_index_capacity(
        consumed_capacity_with_secondary_indexes(
            table_name,
            table_units,
            gsi_units,
            lsi_units,
            mode,
        ),
        vector_bytes,
        mode,
    )
}

/// Attach per-vector-index write bytes to a capacity report.
///
/// Vector replication sits on its own axis, so this never touches
/// `capacity_units`: the bytes are reported beside the classic arms, not
/// summed into them. The map appears under `INDEXES` alone, matching the
/// capture, and an empty `vector_bytes` leaves the response untouched so a
/// write that changed no vector index reports no map at all rather than an
/// empty one.
pub(crate) fn attach_vector_index_capacity(
    capacity: Option<ConsumedCapacity>,
    vector_bytes: &HashMap<String, f64>,
    mode: &Option<String>,
) -> Option<ConsumedCapacity> {
    let mut capacity = capacity?;
    if vector_bytes.is_empty() || !vector_capacity_wanted(mode.as_deref()) {
        return Some(capacity);
    }
    capacity.vector_indexes = Some(
        vector_bytes
            .iter()
            .map(|(name, bytes)| {
                (
                    name.clone(),
                    VectorCapacityDetail {
                        vector_write_request_bytes: *bytes,
                    },
                )
            })
            .collect(),
    );
    Some(capacity)
}

/// Build a `ConsumedCapacity` for one table in a transactional read
/// (`TransactGetItems`). `units` is the table total and already includes the
/// transactional 2x factor. Under `INDEXES` the Table detail reports
/// `ReadCapacityUnits` alongside `CapacityUnits`, matching AWS.
pub fn transactional_read_capacity(
    table_name: &str,
    units: f64,
    mode: &Option<String>,
) -> Option<ConsumedCapacity> {
    match mode.as_deref().unwrap_or("NONE") {
        "TOTAL" => Some(ConsumedCapacity {
            table_name: table_name.to_string(),
            capacity_units: units,
            read_capacity_units: Some(units),
            ..Default::default()
        }),
        "INDEXES" => Some(ConsumedCapacity {
            table_name: table_name.to_string(),
            capacity_units: units,
            read_capacity_units: Some(units),
            table: Some(CapacityDetail {
                capacity_units: units,
                read_capacity_units: Some(units),
                ..Default::default()
            }),
            ..Default::default()
        }),
        _ => None,
    }
}

/// Build the per-table `ConsumedCapacity` vec for a transactional op from the
/// per-table units, using `builder` (`transactional_write_capacity` for a
/// first-call write, `transactional_read_capacity` for a read set or a
/// same-token replay). Returns `None` unless `ReturnConsumedCapacity` is
/// `TOTAL` or `INDEXES`, so the mode guard lives in one place. Shared by
/// `TransactWriteItems` and `ExecuteTransaction`.
pub fn build_transactional_capacity(
    table_units: &HashMap<String, f64>,
    mode: &Option<String>,
    builder: fn(&str, f64, &Option<String>) -> Option<ConsumedCapacity>,
) -> Option<Vec<ConsumedCapacity>> {
    if !capacity_wanted(mode.as_deref()) {
        return None;
    }

    // Sorted by table name, matching the write path. Iterating the map handed
    // back a different order per process, so a two-table read set or replay
    // could report its tables either way round while the write half of the same
    // response family was stable.
    let mut tables: Vec<&String> = table_units.keys().collect();
    tables.sort();

    Some(
        tables
            .into_iter()
            .filter_map(|table| builder(table, *table_units.get(table)?, mode))
            .collect(),
    )
}

/// Build a `ConsumedCapacity` for one table in a transactional write, with the
/// per-index breakdown.
///
/// `table_units` already carries the transactional 2x factor; the index maps do
/// not. DynamoDB applies that factor to the base table arm alone, so an index
/// arm inside a transaction costs what the same write costs outside one.
///
/// `CapacityUnits` and `WriteCapacityUnits` both report the table arm plus the
/// index arms. The `Table` detail reports the table arm on its own, and every
/// index arm carries the write axis too, which is where this shape differs from
/// the single-item one.
pub fn transactional_write_capacity_with_indexes(
    table_name: &str,
    table_units: f64,
    gsi_units: &HashMap<String, f64>,
    lsi_units: &HashMap<String, f64>,
    mode: &Option<String>,
) -> Option<ConsumedCapacity> {
    secondary_index_capacity(
        table_name,
        table_units,
        gsi_units,
        lsi_units,
        mode,
        CapacityAxis::Write,
    )
}

/// Turn per-index units into the response's detail map, or `None` when nothing
/// was charged so the arm is absent rather than present and empty.
fn capacity_detail_map(
    units: &HashMap<String, f64>,
    axis: CapacityAxis,
) -> Option<HashMap<String, CapacityDetail>> {
    if units.is_empty() {
        return None;
    }
    Some(
        units
            .iter()
            .map(|(name, &u)| {
                (
                    name.clone(),
                    CapacityDetail {
                        capacity_units: u,
                        write_capacity_units: axis.write_units(u),
                        ..Default::default()
                    },
                )
            })
            .collect(),
    )
}

/// Build a `ConsumedCapacity` for one table in a transactional write
/// (`TransactWriteItems`). `units` is the table total and already includes the
/// transactional 2x factor. Under `INDEXES` the Table detail reports
/// `WriteCapacityUnits` alongside `CapacityUnits`, matching AWS.
pub fn transactional_write_capacity(
    table_name: &str,
    units: f64,
    mode: &Option<String>,
) -> Option<ConsumedCapacity> {
    match mode.as_deref().unwrap_or("NONE") {
        "TOTAL" => Some(ConsumedCapacity {
            table_name: table_name.to_string(),
            capacity_units: units,
            write_capacity_units: Some(units),
            ..Default::default()
        }),
        "INDEXES" => Some(ConsumedCapacity {
            table_name: table_name.to_string(),
            capacity_units: units,
            write_capacity_units: Some(units),
            table: Some(CapacityDetail {
                capacity_units: units,
                write_capacity_units: Some(units),
                ..Default::default()
            }),
            ..Default::default()
        }),
        _ => None,
    }
}

/// Key schema element - defines a key attribute.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct KeySchemaElement {
    #[serde(rename = "AttributeName", alias = "attribute_name")]
    pub attribute_name: String,
    #[serde(rename = "KeyType", alias = "key_type")]
    pub key_type: KeyType,
}

/// Key type: HASH (partition key) or RANGE (sort key).
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub enum KeyType {
    #[default]
    HASH,
    RANGE,
}

/// Attribute definition - declares an attribute's type.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct AttributeDefinition {
    #[serde(rename = "AttributeName", alias = "attribute_name")]
    pub attribute_name: String,
    #[serde(rename = "AttributeType", alias = "attribute_type")]
    pub attribute_type: ScalarAttributeType,
}

/// Scalar attribute types that can be used as keys.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub enum ScalarAttributeType {
    #[default]
    S,
    N,
    B,
}

/// GSI projection type.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct Projection {
    #[serde(
        rename = "ProjectionType",
        alias = "projection_type",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub projection_type: Option<ProjectionType>,
    #[serde(
        rename = "NonKeyAttributes",
        alias = "non_key_attributes",
        skip_serializing_if = "Option::is_none"
    )]
    pub non_key_attributes: Option<Vec<String>>,
}

/// Projection type enum.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[allow(non_camel_case_types)]
pub enum ProjectionType {
    #[default]
    ALL,
    KEYS_ONLY,
    INCLUDE,
}

/// Global Secondary Index definition.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct GlobalSecondaryIndex {
    #[serde(rename = "IndexName", alias = "index_name")]
    pub index_name: String,
    #[serde(rename = "KeySchema", alias = "key_schema")]
    pub key_schema: Vec<KeySchemaElement>,
    #[serde(rename = "Projection", alias = "projection")]
    pub projection: Projection,
    #[serde(
        rename = "ProvisionedThroughput",
        alias = "provisioned_throughput",
        skip_serializing_if = "Option::is_none"
    )]
    pub provisioned_throughput: Option<ProvisionedThroughput>,
}

/// Local Secondary Index definition.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct LocalSecondaryIndex {
    #[serde(rename = "IndexName", alias = "index_name")]
    pub index_name: String,
    #[serde(rename = "KeySchema", alias = "key_schema")]
    pub key_schema: Vec<KeySchemaElement>,
    #[serde(rename = "Projection", alias = "projection")]
    pub projection: Projection,
}

/// The vector attribute a vector index is built over. A structure with a
/// single `AttributeName` member on the wire, not a bare string.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct VectorAttributeDefinition {
    #[serde(rename = "AttributeName", alias = "attribute_name")]
    pub attribute_name: String,
}

/// One element of a vector index's search schema: a `HASH` partition
/// attribute or an `INLINE_FILTER` attribute.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct SearchSchemaElement {
    #[serde(rename = "AttributeName", alias = "attribute_name")]
    pub attribute_name: String,
    #[serde(
        rename = "SearchSchemaElementType",
        alias = "search_schema_element_type"
    )]
    pub search_schema_element_type: String,
}

/// Vector index definition as supplied on CreateTable's `VectorIndexes`.
///
/// Serialised verbatim into the `_tables.vector_index_definitions` column,
/// following the `gsi_definitions` convention.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct VectorIndex {
    #[serde(rename = "IndexName", alias = "index_name")]
    pub index_name: String,
    #[serde(rename = "VectorAttribute", alias = "vector_attribute")]
    pub vector_attribute: VectorAttributeDefinition,
    #[serde(
        rename = "SearchSchema",
        alias = "search_schema",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub search_schema: Option<Vec<SearchSchemaElement>>,
    #[serde(rename = "Projection", alias = "projection")]
    pub projection: Projection,
    #[serde(rename = "Dimensions", alias = "dimensions")]
    pub dimensions: u32,
    #[serde(rename = "DistanceFunction", alias = "distance_function")]
    pub distance_function: String,
}

/// Why an attribute value is not a valid vector for an index with a given
/// dimension count. Carries the element-level detail the write-path
/// validation errors interpolate; the checks run in the order the variants
/// are listed, so a value can only report one failure.
#[derive(Debug, Clone, PartialEq)]
pub enum VectorValueError {
    /// The value is not a List at all.
    NotAList,
    /// The list's element count differs from the index's dimensions.
    WrongDimensions {
        /// The number of elements actually present.
        actual: usize,
    },
    /// An element is not a Number.
    ElementNotANumber {
        /// Zero-based element position.
        position: usize,
        /// The element's DynamoDB type descriptor.
        actual: &'static str,
    },
    /// An element does not fit the finite f32 range (out-of-range values
    /// overflow to infinity on conversion; real DynamoDB rejects them at
    /// write time, captured eu-west-2 and us-east-1, 2026-08-12).
    ElementOutOfRange {
        /// Zero-based element position.
        position: usize,
        /// The raw number string as written.
        value: String,
    },
}

/// The f32 values of a vector attribute value, or the element-level reason it
/// is not a valid vector for an index with `dimensions` elements.
///
/// Vector indexes hold f32 copies while the base table keeps full precision
/// (captured from real DynamoDB, eu-west-2, 2026-08-11), so conversion
/// happens here, where the index copy is derived. The live write path turns
/// each error variant into its captured `IndexName:`-suffixed message; the
/// backfill path discards the detail via [`vector_f32_values`] and
/// sparse-skips, so the two paths agree on validity by construction.
pub fn check_vector_f32_values(
    value: &AttributeValue,
    dimensions: u32,
) -> std::result::Result<Vec<f32>, VectorValueError> {
    let AttributeValue::L(elems) = value else {
        return Err(VectorValueError::NotAList);
    };
    if elems.len() != dimensions as usize {
        return Err(VectorValueError::WrongDimensions {
            actual: elems.len(),
        });
    }
    let mut out = Vec::with_capacity(elems.len());
    for (position, elem) in elems.iter().enumerate() {
        let AttributeValue::N(n) = elem else {
            return Err(VectorValueError::ElementNotANumber {
                position,
                actual: elem.type_name(),
            });
        };
        // A well-formed DynamoDB number always parses (overflow yields
        // infinity, not an error); anything non-finite is out of f32 range.
        match n.parse::<f32>() {
            Ok(v) if v.is_finite() => out.push(v),
            _ => {
                return Err(VectorValueError::ElementOutOfRange {
                    position,
                    value: n.clone(),
                });
            }
        }
    }
    Ok(out)
}

/// The f32 values of a vector attribute value, or `None` when the value is
/// not a valid vector for an index with `dimensions` elements. The validity
/// rule is exactly [`check_vector_f32_values`]'s, with the element-level
/// detail discarded; the backfill path uses this form to sparse-skip.
pub fn vector_f32_values(value: &AttributeValue, dimensions: u32) -> Option<Vec<f32>> {
    check_vector_f32_values(value, dimensions).ok()
}

/// Serialise an f32 for a vector index's number copy: shortest-decimal via
/// serde_json's f32 formatter, which always keeps a fractional part ("1"
/// stores as "1.0", matching how the index copy reads back from real
/// DynamoDB; captured eu-west-2, 2026-08-11).
pub fn f32_number_string(v: f32) -> String {
    serde_json::to_string(&v).unwrap_or_else(|_| "0.0".to_string())
}

/// Provisioned throughput settings (stored but not enforced).
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct ProvisionedThroughput {
    #[serde(rename = "ReadCapacityUnits", alias = "read_capacity_units", default)]
    pub read_capacity_units: Option<i64>,
    #[serde(rename = "WriteCapacityUnits", alias = "write_capacity_units", default)]
    pub write_capacity_units: Option<i64>,
}

/// On-demand (PAY_PER_REQUEST) throughput ceilings (stored but not enforced).
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct OnDemandThroughput {
    #[serde(
        rename = "MaxReadRequestUnits",
        alias = "max_read_request_units",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub max_read_request_units: Option<i64>,
    #[serde(
        rename = "MaxWriteRequestUnits",
        alias = "max_write_request_units",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub max_write_request_units: Option<i64>,
}

// ---------------------------------------------------------------------------
// Type conversion: From<T> / TryFrom<T> for AttributeValue
// ---------------------------------------------------------------------------

/// Error returned when converting between `AttributeValue` and Rust types.
#[derive(Debug, Clone, PartialEq)]
pub struct ConversionError {
    /// The expected DynamoDB or Rust type.
    pub expected: &'static str,
    /// The actual DynamoDB type encountered.
    pub actual: &'static str,
}

impl fmt::Display for ConversionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "expected {}, got {}", self.expected, self.actual)
    }
}

impl std::error::Error for ConversionError {}

// --- From<T> for AttributeValue: infallible conversions ---

impl From<String> for AttributeValue {
    fn from(value: String) -> Self {
        AttributeValue::S(value)
    }
}

impl From<&str> for AttributeValue {
    fn from(value: &str) -> Self {
        AttributeValue::S(value.to_string())
    }
}

impl From<bool> for AttributeValue {
    fn from(value: bool) -> Self {
        AttributeValue::BOOL(value)
    }
}

impl From<Vec<u8>> for AttributeValue {
    fn from(value: Vec<u8>) -> Self {
        AttributeValue::B(value)
    }
}

impl From<&[u8]> for AttributeValue {
    fn from(value: &[u8]) -> Self {
        AttributeValue::B(value.to_vec())
    }
}

// Integer types - all finite, all fit in DynamoDB's number range.
macro_rules! impl_from_integer {
    ($($t:ty),+) => {
        $(
            impl From<$t> for AttributeValue {
                fn from(value: $t) -> Self {
                    AttributeValue::N(value.to_string())
                }
            }
        )+
    };
}

impl_from_integer!(i8, i16, i32, i64, i128, u8, u16, u32, u64, u128);

// Container types
impl From<HashMap<String, AttributeValue>> for AttributeValue {
    fn from(value: HashMap<String, AttributeValue>) -> Self {
        AttributeValue::M(value)
    }
}

impl From<Vec<AttributeValue>> for AttributeValue {
    fn from(value: Vec<AttributeValue>) -> Self {
        AttributeValue::L(value)
    }
}

impl From<HashSet<String>> for AttributeValue {
    fn from(value: HashSet<String>) -> Self {
        AttributeValue::SS(value.into_iter().collect())
    }
}

impl From<BTreeSet<String>> for AttributeValue {
    fn from(value: BTreeSet<String>) -> Self {
        AttributeValue::SS(value.into_iter().collect())
    }
}

// --- TryFrom<T> for AttributeValue: fallible conversions (floats) ---

impl TryFrom<f64> for AttributeValue {
    type Error = ConversionError;

    fn try_from(value: f64) -> std::result::Result<Self, Self::Error> {
        if value.is_finite() {
            Ok(AttributeValue::N(value.to_string()))
        } else {
            Err(ConversionError {
                expected: "finite f64",
                actual: "NaN or Infinity",
            })
        }
    }
}

impl TryFrom<f32> for AttributeValue {
    type Error = ConversionError;

    fn try_from(value: f32) -> std::result::Result<Self, Self::Error> {
        if value.is_finite() {
            Ok(AttributeValue::N(value.to_string()))
        } else {
            Err(ConversionError {
                expected: "finite f32",
                actual: "NaN or Infinity",
            })
        }
    }
}

// --- TryFrom<AttributeValue> for T: extract Rust types from AV ---

impl TryFrom<AttributeValue> for String {
    type Error = ConversionError;

    fn try_from(value: AttributeValue) -> std::result::Result<Self, ConversionError> {
        match value {
            AttributeValue::S(s) => Ok(s),
            other => Err(ConversionError {
                expected: "S",
                actual: other.type_name(),
            }),
        }
    }
}

impl TryFrom<AttributeValue> for bool {
    type Error = ConversionError;

    fn try_from(value: AttributeValue) -> std::result::Result<Self, ConversionError> {
        match value {
            AttributeValue::BOOL(b) => Ok(b),
            other => Err(ConversionError {
                expected: "BOOL",
                actual: other.type_name(),
            }),
        }
    }
}

impl TryFrom<AttributeValue> for Vec<u8> {
    type Error = ConversionError;

    fn try_from(value: AttributeValue) -> std::result::Result<Self, ConversionError> {
        match value {
            AttributeValue::B(b) => Ok(b),
            other => Err(ConversionError {
                expected: "B",
                actual: other.type_name(),
            }),
        }
    }
}

macro_rules! impl_try_from_av_integer {
    ($($t:ty),+) => {
        $(
            impl TryFrom<AttributeValue> for $t {
                type Error = ConversionError;

                fn try_from(value: AttributeValue) -> std::result::Result<Self, ConversionError> {
                    match value {
                        AttributeValue::N(n) => n.parse::<$t>().map_err(|_| ConversionError {
                            expected: stringify!($t),
                            actual: "N (parse failed)",
                        }),
                        other => Err(ConversionError {
                            expected: "N",
                            actual: other.type_name(),
                        }),
                    }
                }
            }
        )+
    };
}

impl_try_from_av_integer!(i8, i16, i32, i64, i128, u8, u16, u32, u64, u128);

impl TryFrom<AttributeValue> for f64 {
    type Error = ConversionError;

    fn try_from(value: AttributeValue) -> std::result::Result<Self, ConversionError> {
        match value {
            AttributeValue::N(n) => n.parse::<f64>().map_err(|_| ConversionError {
                expected: "f64",
                actual: "N (parse failed)",
            }),
            other => Err(ConversionError {
                expected: "N",
                actual: other.type_name(),
            }),
        }
    }
}

impl TryFrom<AttributeValue> for f32 {
    type Error = ConversionError;

    fn try_from(value: AttributeValue) -> std::result::Result<Self, ConversionError> {
        match value {
            AttributeValue::N(n) => n.parse::<f32>().map_err(|_| ConversionError {
                expected: "f32",
                actual: "N (parse failed)",
            }),
            other => Err(ConversionError {
                expected: "N",
                actual: other.type_name(),
            }),
        }
    }
}

impl TryFrom<AttributeValue> for HashMap<String, AttributeValue> {
    type Error = ConversionError;

    fn try_from(value: AttributeValue) -> std::result::Result<Self, ConversionError> {
        match value {
            AttributeValue::M(m) => Ok(m),
            other => Err(ConversionError {
                expected: "M",
                actual: other.type_name(),
            }),
        }
    }
}

impl TryFrom<AttributeValue> for Vec<AttributeValue> {
    type Error = ConversionError;

    fn try_from(value: AttributeValue) -> std::result::Result<Self, ConversionError> {
        match value {
            AttributeValue::L(l) => Ok(l),
            other => Err(ConversionError {
                expected: "L",
                actual: other.type_name(),
            }),
        }
    }
}

impl TryFrom<AttributeValue> for Vec<String> {
    type Error = ConversionError;

    fn try_from(value: AttributeValue) -> std::result::Result<Self, ConversionError> {
        match value {
            AttributeValue::SS(ss) => Ok(ss),
            AttributeValue::L(l) => {
                // Lenient: extract S values from a list
                l.into_iter()
                    .map(|av| match av {
                        AttributeValue::S(s) => Ok(s),
                        other => Err(ConversionError {
                            expected: "S (within L)",
                            actual: other.type_name(),
                        }),
                    })
                    .collect()
            }
            other => Err(ConversionError {
                expected: "SS or L",
                actual: other.type_name(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A future that reports the layer in force at each of its polls, and
    /// waits once between them.
    struct SeesLayer {
        seen: Vec<crate::validation::ValueLayer>,
        waited: bool,
    }

    impl std::future::Future for SeesLayer {
        type Output = Vec<crate::validation::ValueLayer>;
        fn poll(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Self::Output> {
            self.seen.push(current_value_layer());
            if self.waited {
                std::task::Poll::Ready(std::mem::take(&mut self.seen))
            } else {
                self.waited = true;
                cx.waker().wake_by_ref();
                std::task::Poll::Pending
            }
        }
    }

    fn sees_layer() -> SeesLayer {
        SeesLayer {
            seen: Vec::new(),
            waited: false,
        }
    }

    /// The layer holds across a future's awaits without reaching anything
    /// polled in between: on the wasm engine another request runs between two
    /// polls of this one, on the same thread.
    #[test]
    fn the_value_layer_follows_a_future_across_its_polls_and_no_further() {
        use crate::validation::ValueLayer::{Current, Older};
        use std::future::Future;
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        let mut older = Box::pin(with_value_layer_async(Older, sees_layer()));
        let mut current = Box::pin(with_value_layer_async(Current, sees_layer()));

        assert_eq!(current_value_layer(), Current);
        assert!(older.as_mut().poll(&mut cx).is_pending());
        // Out of its poll the layer is back to what the thread had.
        assert_eq!(current_value_layer(), Current);
        // A neighbour polled between the two reads its own layer, not the
        // one left by the poll before it.
        assert!(current.as_mut().poll(&mut cx).is_pending());
        assert_eq!(current_value_layer(), Current);
        assert_eq!(
            with_value_layer(Older, || {
                let mut neighbour = Box::pin(sees_layer());
                let _ = neighbour.as_mut().poll(&mut cx);
                current_value_layer()
            }),
            Older
        );
        match older.as_mut().poll(&mut cx) {
            std::task::Poll::Ready(seen) => assert_eq!(seen, vec![Older, Older]),
            std::task::Poll::Pending => panic!("the future was ready on its second poll"),
        }
        match current.as_mut().poll(&mut cx) {
            std::task::Poll::Ready(seen) => assert_eq!(seen, vec![Current, Current]),
            std::task::Poll::Pending => panic!("the future was ready on its second poll"),
        }
        assert_eq!(current_value_layer(), Current);
    }

    #[test]
    fn test_serialize_string() {
        let val = AttributeValue::S("hello".to_string());
        let json = serde_json::to_string(&val).unwrap();
        assert_eq!(json, r#"{"S":"hello"}"#);
    }

    #[test]
    fn test_serialize_number() {
        let val = AttributeValue::N("42".to_string());
        let json = serde_json::to_string(&val).unwrap();
        assert_eq!(json, r#"{"N":"42"}"#);
    }

    #[test]
    fn test_serialize_binary() {
        let val = AttributeValue::B(vec![1, 2, 3]);
        let json = serde_json::to_string(&val).unwrap();
        assert_eq!(json, r#"{"B":"AQID"}"#);
    }

    #[test]
    fn test_serialize_bool() {
        let val = AttributeValue::BOOL(true);
        let json = serde_json::to_string(&val).unwrap();
        assert_eq!(json, r#"{"BOOL":true}"#);
    }

    #[test]
    fn test_serialize_null() {
        let val = AttributeValue::NULL(true);
        let json = serde_json::to_string(&val).unwrap();
        assert_eq!(json, r#"{"NULL":true}"#);
    }

    #[test]
    fn test_deserialize_null_true() {
        let val: AttributeValue = serde_json::from_str(r#"{"NULL":true}"#).unwrap();
        assert_eq!(val, AttributeValue::NULL(true));
    }

    #[test]
    fn test_deserialize_null_false_rejected() {
        // AWS requires the NULL member to be exactly `true`; {"NULL": false} is
        // rejected with a ValidationException, same as a non-boolean value.
        let err = serde_json::from_str::<AttributeValue>(r#"{"NULL":false}"#).unwrap_err();
        assert!(
            err.to_string().contains("must have the value of true"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_deserialize_null_non_boolean_rejected() {
        // A non-boolean NULL (e.g. {"NULL": "no"}) is a type error, not a value.
        let err = serde_json::from_str::<AttributeValue>(r#"{"NULL":"no"}"#).unwrap_err();
        assert!(
            err.to_string().contains("must have the value of true"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_serialize_string_set() {
        let val = AttributeValue::SS(vec!["a".to_string(), "b".to_string()]);
        let json = serde_json::to_string(&val).unwrap();
        assert_eq!(json, r#"{"SS":["a","b"]}"#);
    }

    #[test]
    fn test_serialize_list() {
        let val = AttributeValue::L(vec![
            AttributeValue::S("hello".to_string()),
            AttributeValue::N("42".to_string()),
        ]);
        let json = serde_json::to_string(&val).unwrap();
        assert_eq!(json, r#"{"L":[{"S":"hello"},{"N":"42"}]}"#);
    }

    #[test]
    fn test_serialize_map() {
        let mut m = HashMap::new();
        m.insert("key".to_string(), AttributeValue::S("value".to_string()));
        let val = AttributeValue::M(m);
        let json = serde_json::to_string(&val).unwrap();
        assert_eq!(json, r#"{"M":{"key":{"S":"value"}}}"#);
    }

    #[test]
    fn test_round_trip_all_types() {
        let values = vec![
            AttributeValue::S("hello".to_string()),
            AttributeValue::N("42.5".to_string()),
            AttributeValue::B(vec![0, 255, 128]),
            AttributeValue::BOOL(false),
            AttributeValue::NULL(true),
            AttributeValue::SS(vec!["x".to_string(), "y".to_string()]),
            AttributeValue::NS(vec!["1".to_string(), "2.5".to_string()]),
            AttributeValue::BS(vec![vec![1], vec![2, 3]]),
            AttributeValue::L(vec![
                AttributeValue::S("nested".to_string()),
                AttributeValue::N("99".to_string()),
            ]),
        ];

        for val in values {
            let json = serde_json::to_string(&val).unwrap();
            let deserialized: AttributeValue = serde_json::from_str(&json).unwrap();
            assert_eq!(val, deserialized, "Round-trip failed for {json}");
        }
    }

    #[test]
    fn test_size_string() {
        let val = AttributeValue::S("hello".to_string());
        assert_eq!(val.size(), 5);
    }

    #[test]
    fn test_size_number() {
        // "42" has 2 significant digits → ceil(2/2) + 1 = 2
        let val = AttributeValue::N("42".to_string());
        assert_eq!(val.size(), 2);
    }

    #[test]
    fn test_size_number_matches_dynamodb() {
        // Byte-exact ground truth, measured against eu-west-2 by bisecting the
        // 400KB item-size gate (which resolves to the byte, unlike the 1KB
        // granularity of consumed capacity). A number costs
        // ceil(integer significant digits / 2) + ceil(fraction significant
        // digits / 2) + 1, over the value with leading and trailing zeros
        // trimmed. The count is a property of the significant digits, so it does
        // not change when scientific notation is expanded on storage.
        let cases: &[(&str, usize)] = &[
            // digit counts either side of the decimal point
            ("1", 2),
            ("12", 2),
            ("123", 3),
            ("1234", 3),
            ("12345", 4),
            ("123456", 4),
            ("1234567", 5),
            ("12345678", 5),
            ("123456789", 6),
            ("1234567891", 6),
            ("12345678912", 7),
            ("123456789123", 7),
            ("1234567890123456789", 11),
            ("12345678901234567890123456789012345678", 20),
            // zeros the wire carries but DynamoDB trims
            ("0042", 2),
            ("100", 2),
            ("1010", 3),
            ("0.0000001", 2),
            ("0", 1),
            // scientific notation, which expands on storage without growing
            ("1E125", 2),
            ("1E-100", 2),
            // significant digits straddling the decimal point are counted in two
            // halves, so these cost a byte more than the digit total alone implies
            ("1.5", 3),
            ("1.2", 3),
            ("1.200", 3),
            ("1.234", 4),
            ("3.14159", 5),
            ("100.5", 4),
            ("0.15", 2),
            ("15", 2),
            // a sign costs a byte
            ("-42", 3),
            ("-12345", 5),
            ("-1E125", 3),
            ("-0.15", 3),
            ("-0", 1),
        ];
        for (literal, expected) in cases {
            assert_eq!(
                AttributeValue::N((*literal).to_string()).size(),
                *expected,
                "size of N {literal:?}"
            );
        }
    }

    #[test]
    fn test_size_number_survives_normalisation() {
        // The reason the write paths could disagree: sizing has to be invariant
        // under the normalisation that happens on storage, or the same item
        // measures differently either side of it.
        for literal in [
            "1E125",
            "1E-100",
            "0042",
            "1.200",
            "0.0000001",
            "100",
            "3.14159",
            "42",
        ] {
            let raw = AttributeValue::N(literal.to_string()).size();
            let normalised = AttributeValue::N(normalize_dynamo_number(literal)).size();
            assert_eq!(
                raw, normalised,
                "N {literal:?} changed size when normalised"
            );
        }
    }

    #[test]
    fn test_size_bool() {
        assert_eq!(AttributeValue::BOOL(true).size(), 1);
    }

    #[test]
    fn test_size_null() {
        assert_eq!(AttributeValue::NULL(true).size(), 1);
    }

    #[test]
    fn test_key_string_s() {
        let val = AttributeValue::S("hello".to_string());
        assert_eq!(val.to_key_string(), Some("S:hello".to_string()));
    }

    #[test]
    fn test_key_string_n() {
        let val = AttributeValue::N("42".to_string());
        let key = val.to_key_string().unwrap();
        assert!(key.starts_with("N:"));
    }

    #[test]
    fn test_key_string_b() {
        let val = AttributeValue::B(vec![0xff, 0x00, 0xab]);
        assert_eq!(val.to_key_string(), Some("B:ff00ab".to_string()));
    }

    #[test]
    fn test_key_string_non_key_type_returns_none() {
        assert_eq!(AttributeValue::BOOL(true).to_key_string(), None);
        assert_eq!(AttributeValue::L(vec![]).to_key_string(), None);
    }

    // Number sort key ordering tests
    #[test]
    fn test_number_sort_ordering() {
        let numbers = vec![
            "-1000", "-100", "-10", "-1", "-0.5", "-0.001", "0", "0.001", "0.5", "1", "10", "100",
            "1000",
        ];
        let encoded: Vec<String> = numbers
            .iter()
            .map(|n| normalize_number_for_sort(n))
            .collect();

        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] < encoded[i + 1],
                "Sort order broken: {} ({}) should be < {} ({})",
                numbers[i],
                encoded[i],
                numbers[i + 1],
                encoded[i + 1]
            );
        }
    }

    #[test]
    fn test_number_sort_zero_variants() {
        let z1 = normalize_number_for_sort("0");
        let z2 = normalize_number_for_sort("-0");
        let z3 = normalize_number_for_sort("0.0");
        assert_eq!(z1, z2);
        assert_eq!(z2, z3);
    }

    #[test]
    fn test_number_sort_decimals() {
        let a = normalize_number_for_sort("1.5");
        let b = normalize_number_for_sort("2.5");
        assert!(a < b);

        let c = normalize_number_for_sort("0.001");
        let d = normalize_number_for_sort("0.01");
        assert!(c < d);
    }

    #[test]
    fn test_number_sort_scientific() {
        let a = normalize_number_for_sort("1e10");
        let b = normalize_number_for_sort("1e11");
        assert!(a < b);

        let c = normalize_number_for_sort("-1e11");
        let d = normalize_number_for_sort("-1e10");
        assert!(c < d);
    }

    // A library caller can reach the key encoder with a number that never
    // went through validation. One far outside DynamoDB's range sorts at the
    // matching extreme rather than overflowing the encoded exponent.
    #[test]
    fn test_number_sort_out_of_range_sorts_at_the_extremes() {
        let ordered = [
            "-1e99999999999",
            "-9.9999999999999999999999999999999999999E125",
            "-1E-130",
            "-1e-99999999999",
            "0",
            "1e-99999999999",
            "1E-130",
            "9.9999999999999999999999999999999999999E125",
            "1e99999999999",
        ];
        for pair in ordered.windows(2) {
            let (a, b) = (pair[0], pair[1]);
            assert!(
                normalize_number_for_sort(a) < normalize_number_for_sort(b),
                "{a} should sort before {b}"
            );
        }
    }

    // Only an exponent too large for an i64 saturates. Anything else that
    // fails to parse keeps reading as 0 for callers that never validated.
    #[test]
    fn test_split_exponent_reads_an_unparseable_exponent_as_zero() {
        assert_eq!(split_exponent("1e"), ("1", 0));
        assert_eq!(split_exponent("1e+"), ("1", 0));
        assert_eq!(split_exponent("1e99999999999999999999"), ("1", i64::MAX));
        assert_eq!(split_exponent("1e-99999999999999999999"), ("1", i64::MIN));
    }

    // Number validation/normalisation is pinned to real DynamoDB behaviour
    // (captured against AWS for issue #109). validate_dynamo_number accepts
    // exactly the grammar DynamoDB accepts, including a leading '+'; the bare
    // and '+'-prefixed malformed forms are both rejected, matching AWS.
    #[test]
    fn test_validate_number_accepts_dynamodb_grammar() {
        for input in [
            "+5", "+1.5", "+0", "-0", "+0.0", "+1e2", "1e+2", "1.5E+3", "-7", "+.5", ".5", "5.",
            "00042", "1.23E10", "+1e-2", "1E-130", "+1E-130",
        ] {
            assert!(
                validate_dynamo_number(input).is_ok(),
                "expected {input} to validate, got {:?}",
                validate_dynamo_number(input)
            );
        }
    }

    #[test]
    fn test_validate_number_rejects_malformed() {
        // Every one of these is rejected by real DynamoDB. Note that whitespace
        // (leading, trailing, or internal) is rejected, not trimmed.
        for input in [
            "+e2", "e2", "+1+2", "1+2", "+1.2.3", "1.2.3", "++5", "+-5", "-+5", "+", "-", "1e",
            "1e+", ".", "1.2e3.4", "0x5", "NaN", "Infinity", "1_000", " 5", "5 ", "1 5", "",
        ] {
            assert!(
                matches!(
                    validate_dynamo_number(input),
                    Err(crate::errors::DynoxideError::ValidationException(_))
                ),
                "expected {input:?} to be rejected with ValidationException, got {:?}",
                validate_dynamo_number(input)
            );
        }
    }

    fn validation_message(input: &str) -> Option<String> {
        match validate_dynamo_number(input) {
            Ok(()) => None,
            Err(crate::errors::DynoxideError::ValidationException(m)) => Some(m),
            Err(e) => panic!("expected a ValidationException for {input:?}, got {e:?}"),
        }
    }

    const OVERFLOW: &str =
        "Number overflow. Attempting to store a number with magnitude larger than supported range";
    const UNDERFLOW: &str = "Number underflow. Attempting to store a number with magnitude smaller than supported range";

    // An exponent too large for an i32 used to be read as 0, so 1e99999999999
    // passed the range checks and was stored as 1. Every expectation here was
    // captured against eu-west-2, 2026-10-08.
    #[test]
    fn test_validate_number_range_checks_exponents_beyond_i32() {
        for (input, expected) in [
            ("1e99999999999", OVERFLOW),
            ("-1e99999999999", OVERFLOW),
            ("1E+99999999999", OVERFLOW),
            ("1e-99999999999", UNDERFLOW),
            ("-1e-99999999999", UNDERFLOW),
            ("1e2147483648", OVERFLOW),
            ("1e-2147483649", UNDERFLOW),
            // Fits in an i32, but the digits before the point push it past.
            ("10e2147483647", OVERFLOW),
            ("0.01e-2147483648", UNDERFLOW),
            ("1e9223372036854775807", OVERFLOW),
            ("1e-9223372036854775808", UNDERFLOW),
            ("10e9223372036854775807", OVERFLOW),
            ("0.01e-9223372036854775808", UNDERFLOW),
        ] {
            assert_eq!(
                validation_message(input).as_deref(),
                Some(expected),
                "{input}"
            );
        }
    }

    // DynamoDB reads the exponent as a 64-bit integer. One that does not fit
    // is a value it cannot convert, not an overflow (captured eu-west-2,
    // 2026-10-08).
    #[test]
    fn test_validate_number_rejects_exponent_beyond_i64_as_unconvertible() {
        for input in [
            "1e9223372036854775808",
            "1e+9223372036854775808",
            "1e-9223372036854775809",
            "0e9223372036854775808",
        ] {
            assert_eq!(
                validation_message(input),
                Some(format!(
                    "The parameter cannot be converted to a numeric value: {input}"
                )),
                "{input}"
            );
        }
    }

    // A zero is range-checked as if its last written digit were a 1, so
    // `0e126` overflows like `1e126` and `0.0e-130` underflows like
    // `0.1e-130`. Captured eu-west-2, 2026-10-08.
    #[test]
    fn test_validate_number_range_checks_zero_by_its_last_written_digit() {
        let zeros_130 = format!("0.{}", "0".repeat(130));
        let zeros_131 = format!("0.{}", "0".repeat(131));
        for (input, expected) in [
            ("0e125", None),
            ("0e126", Some(OVERFLOW)),
            ("0.0e126", None),
            ("0.0e127", Some(OVERFLOW)),
            ("0.00e127", None),
            ("00e126", Some(OVERFLOW)),
            ("00.00e126", None),
            ("-0e127", Some(OVERFLOW)),
            ("0e99999999999", Some(OVERFLOW)),
            ("0e-130", None),
            ("0e-131", Some(UNDERFLOW)),
            ("0.0e-129", None),
            ("0.0e-130", Some(UNDERFLOW)),
            ("0e-99999999999", Some(UNDERFLOW)),
            (zeros_130.as_str(), None),
            (zeros_131.as_str(), Some(UNDERFLOW)),
        ] {
            assert_eq!(validation_message(input).as_deref(), expected, "{input}");
        }
    }

    #[test]
    fn test_normalize_number_matches_dynamodb() {
        for (input, stored) in [
            ("+5", "5"),
            ("+1.5", "1.5"),
            ("+0", "0"),
            ("-0", "0"),
            ("+0.0", "0"),
            ("+1e2", "100"),
            ("1e+2", "100"),
            ("1.5E+3", "1500"),
            ("-7", "-7"),
            ("+.5", "0.5"),
            (".5", "0.5"),
            ("5.", "5"),
            ("00042", "42"),
            ("1.23E10", "12300000000"),
            ("+1e-2", "0.01"),
        ] {
            assert_eq!(
                normalize_dynamo_number(input),
                stored,
                "{input} should normalise to {stored}"
            );
        }
    }

    #[test]
    fn test_type_name() {
        assert_eq!(AttributeValue::S("".to_string()).type_name(), "S");
        assert_eq!(AttributeValue::N("0".to_string()).type_name(), "N");
        assert_eq!(AttributeValue::B(vec![]).type_name(), "B");
        assert_eq!(AttributeValue::BOOL(true).type_name(), "BOOL");
        assert_eq!(AttributeValue::NULL(true).type_name(), "NULL");
        assert_eq!(AttributeValue::SS(vec![]).type_name(), "SS");
        assert_eq!(AttributeValue::NS(vec![]).type_name(), "NS");
        assert_eq!(AttributeValue::BS(vec![]).type_name(), "BS");
        assert_eq!(AttributeValue::L(vec![]).type_name(), "L");
        assert_eq!(AttributeValue::M(HashMap::new()).type_name(), "M");
    }
}
