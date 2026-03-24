use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use serde::de;
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt;

/// DynamoDB AttributeValue — the core type system.
///
/// Each variant corresponds to a DynamoDB type descriptor:
/// S (String), N (Number as string), B (Binary), BOOL, NULL,
/// SS (String Set), NS (Number Set), BS (Binary Set),
/// L (List), M (Map).
#[derive(Debug, Clone, PartialEq)]
pub enum AttributeValue {
    /// String type
    S(String),
    /// Number type — stored as string per DynamoDB convention
    N(String),
    /// Binary type — raw bytes, serialized as base64
    B(Vec<u8>),
    /// Boolean type
    BOOL(bool),
    /// Null type
    NULL(bool),
    /// String Set
    SS(Vec<String>),
    /// Number Set — each number stored as string
    NS(Vec<String>),
    /// Binary Set — each element is raw bytes
    BS(Vec<Vec<u8>>),
    /// List — ordered collection of AttributeValues
    L(Vec<AttributeValue>),
    /// Map — key-value pairs
    M(HashMap<String, AttributeValue>),
}

impl AttributeValue {
    /// Calculate the size of this attribute value in bytes,
    /// following DynamoDB's item size calculation rules.
    ///
    /// This does NOT include the attribute name — the caller
    /// is responsible for adding the name's UTF-8 byte length.
    pub fn size(&self) -> usize {
        match self {
            AttributeValue::S(s) => s.len(),
            AttributeValue::N(n) => {
                // DynamoDB: (number of significant digits / 2) + 1, minimum 1
                let significant = n.chars().filter(|c| c.is_ascii_digit()).count();
                let significant = significant.max(1);
                (significant / 2) + 1
            }
            AttributeValue::B(b) => b.len(),
            AttributeValue::BOOL(_) => 1,
            AttributeValue::NULL(_) => 1,
            AttributeValue::SS(ss) => ss.iter().map(|s| s.len()).sum(),
            AttributeValue::NS(ns) => ns
                .iter()
                .map(|n| {
                    let significant = n.chars().filter(|c| c.is_ascii_digit()).count().max(1);
                    (significant / 2) + 1
                })
                .sum(),
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
            AttributeValue::SS(ss) => map.serialize_entry("SS", ss)?,
            AttributeValue::NS(ns) => map.serialize_entry("NS", ns)?,
            AttributeValue::BS(bs) => {
                let encoded: Vec<String> = bs.iter().map(|b| BASE64.encode(b)).collect();
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

        // Collect known type keys
        let known_types = ["S", "N", "B", "BOOL", "NULL", "SS", "NS", "BS", "L", "M"];
        let present: Vec<&str> = obj
            .keys()
            .filter(|k| known_types.contains(&k.as_str()))
            .map(|k| k.as_str())
            .collect();

        if present.is_empty() {
            return Err(de::Error::custom(
                "Supplied AttributeValue is empty, must contain exactly one of the supported datatypes",
            ));
        }

        // Validate numbers in ALL type keys before checking for multi-type.
        // DynamoDB validates number format before rejecting multi-type.
        for &type_key in &present {
            match type_key {
                "N" => {
                    if let Some(n) = obj.get("N").and_then(|v| v.as_str()) {
                        validate_number_in_deser(n).map_err(de::Error::custom)?;
                    }
                }
                "NS" => {
                    if let Some(arr) = obj.get("NS").and_then(|v| v.as_array()) {
                        for item in arr {
                            if let Some(n) = item.as_str() {
                                validate_number_in_deser(n).map_err(de::Error::custom)?;
                            }
                        }
                    }
                }
                _ => {}
            }
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
            "BOOL" => {
                let b = val
                    .as_bool()
                    .ok_or_else(|| de::Error::custom("expected boolean for BOOL"))?;
                Ok(AttributeValue::BOOL(b))
            }
            "NULL" => {
                // DynamoDB treats non-boolean NULL values (e.g. {"NULL": "no"}) as
                // NULL(false) and rejects them during validation, not serialisation.
                let n = val.as_bool().unwrap_or(false);
                Ok(AttributeValue::NULL(n))
            }
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

/// Validate a number string during AttributeValue deserialization.
///
/// Returns DynamoDB-matching error messages for invalid numbers.
/// Error messages are returned WITHOUT the VALIDATION: prefix since they
/// bypass the normal validation flow — the server routes them based on
/// message content (see `server::deserialize`).
fn validate_number_in_deser(n: &str) -> Result<(), String> {
    if n.is_empty() {
        return Err("VALIDATION:The parameter cannot be converted to a numeric value".to_string());
    }
    // Check if it's a valid number
    let trimmed = n.trim();
    let is_valid = trimmed.parse::<f64>().is_ok()
        || trimmed
            .to_lowercase()
            .contains('e')
            .then(|| trimmed.parse::<f64>().ok())
            .is_some();
    if !is_valid {
        return Err(format!(
            "VALIDATION:The parameter cannot be converted to a numeric value: {n}"
        ));
    }
    // Use the full validate_dynamo_number for precision/range checks
    if let Err(e) = validate_dynamo_number(n) {
        let msg = match e {
            crate::errors::DynoxideError::ValidationException(m) => format!("VALIDATION:{m}"),
            _ => format!("VALIDATION:{}", e),
        };
        return Err(msg);
    }
    Ok(())
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

/// Validate a DynamoDB number string against DynamoDB's constraints:
/// - Up to 38 significant digits
/// - Magnitude at most 9.9999999999999999999999999999999999999E+125
/// - Positive values must be at least 1E-130
/// - Negative values must be at most -1E-130
pub fn validate_dynamo_number(
    num_str: &str,
) -> std::result::Result<(), crate::errors::DynoxideError> {
    let trimmed = num_str.trim();

    if trimmed.is_empty() {
        return Err(crate::errors::DynoxideError::ValidationException(
            "The parameter cannot be converted to a numeric value".to_string(),
        ));
    }

    let negative = trimmed.starts_with('-');
    let abs_str = if negative { &trimmed[1..] } else { trimmed };

    // Validate that the string is a well-formed number: must contain at least one digit,
    // and only valid number characters (digits, '.', 'e'/'E', '+', '-' in exponent).
    // Rejects "NaN", "Infinity", "abc", etc.
    if abs_str.is_empty() || !abs_str.chars().any(|c| c.is_ascii_digit()) {
        return Err(crate::errors::DynoxideError::ValidationException(format!(
            "The parameter cannot be converted to a numeric value: {}",
            trimmed
        )));
    }
    let valid = abs_str.chars().enumerate().all(|(i, c)| {
        c.is_ascii_digit() || c == '.' || c == 'e' || c == 'E' || ((c == '+' || c == '-') && i > 0) // sign only after 'e'/'E'
    });
    if !valid {
        return Err(crate::errors::DynoxideError::ValidationException(format!(
            "The parameter cannot be converted to a numeric value: {}",
            trimmed
        )));
    }

    let (mantissa_digits, exponent) = parse_number_parts(abs_str);

    // Zero is always valid
    if mantissa_digits.is_empty() || mantissa_digits.iter().all(|&d| d == 0) {
        return Ok(());
    }

    // Check significant digits (mantissa_digits has leading/trailing zeros already stripped)
    if mantissa_digits.len() > 38 {
        return Err(crate::errors::DynoxideError::ValidationException(
            "Attempting to store more than 38 significant digits in a Number".to_string(),
        ));
    }

    // Check magnitude: exponent represents the power such that value = 0.mantissa * 10^exponent
    // Max magnitude: 9.999...E+125 means exponent = 126 (since 0.999... * 10^126 = 9.99...E+125)
    if exponent > 126 {
        return Err(crate::errors::DynoxideError::ValidationException(
            "Number overflow. Attempting to store a number with magnitude larger than supported range"
                .to_string(),
        ));
    }

    // Check underflow for non-zero values
    // Min positive: 1E-130 means exponent = -129 (since 0.1 * 10^-129 = 1E-130)
    // But with more digits, exponent can be lower, e.g. 1.0E-130 has (mantissa=[1], exponent=-129)
    // Actually, the smallest representable is 1E-130. In our representation, 1E-130 = 0.1 * 10^-129
    // So exponent = -129 with mantissa [1].
    // For 1E-131 = 0.1 * 10^-130, exponent = -130 — that's too small.
    if exponent < -129 {
        return Err(crate::errors::DynoxideError::ValidationException(
            "Number underflow. Attempting to store a number with magnitude smaller than supported range"
                .to_string(),
        ));
    }

    Ok(())
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

fn encode_positive(mantissa: &[u8], exponent: i32) -> String {
    let exp_encoded = (exponent + 5000) as u16;
    let mantissa_str = mantissa_to_string(mantissa, 40);
    format!("2{exp_encoded:04}{mantissa_str}")
}

fn encode_negative(mantissa: &[u8], exponent: i32) -> String {
    // For negatives, we complement everything so larger absolute values sort first (smaller)
    let exp_encoded = 9999 - (exponent + 5000) as u16;
    let mantissa_str = complement_mantissa(mantissa, 40);
    format!("0{exp_encoded:04}{mantissa_str}")
}

/// Parse a non-negative number string into (mantissa digits, exponent).
/// Mantissa is normalized: first digit is non-zero, exponent is the power of 10
/// such that the number = 0.mantissa * 10^exponent.
pub(crate) fn parse_number_parts(s: &str) -> (Vec<u8>, i32) {
    // Handle scientific notation
    let (coeff, exp_part) = if let Some(pos) = s.to_ascii_lowercase().find('e') {
        let coeff = &s[..pos];
        let exp: i32 = s[pos + 1..].parse().unwrap_or(0);
        (coeff, exp)
    } else {
        (s, 0)
    };

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
    let int_len = int_part.chars().filter(|c| c.is_ascii_digit()).count() as i32;

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
    let exponent = int_len - leading_zeros as i32 + exp_part;

    (digits, exponent)
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
}

/// Per-resource capacity detail.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CapacityDetail {
    #[serde(rename = "CapacityUnits")]
    pub capacity_units: f64,
}

/// Calculate write capacity units (1 WCU = 1KB, rounded up).
pub fn write_capacity_units(item_size_bytes: usize) -> f64 {
    ((item_size_bytes as f64) / 1024.0).ceil().max(1.0)
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
        }),
        "INDEXES" => Some(ConsumedCapacity {
            table_name: table_name.to_string(),
            capacity_units,
            table: Some(CapacityDetail { capacity_units }),
            global_secondary_indexes: None,
            local_secondary_indexes: None,
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
    let units_to_map = |units: &HashMap<String, f64>| -> Option<HashMap<String, CapacityDetail>> {
        if units.is_empty() {
            None
        } else {
            Some(
                units
                    .iter()
                    .map(|(name, &u)| (name.clone(), CapacityDetail { capacity_units: u }))
                    .collect(),
            )
        }
    };

    match mode.as_deref().unwrap_or("NONE") {
        "INDEXES" => {
            let gsi_total: f64 = gsi_units.values().sum();
            let lsi_total: f64 = lsi_units.values().sum();
            Some(ConsumedCapacity {
                table_name: table_name.to_string(),
                capacity_units: table_units + gsi_total + lsi_total,
                table: Some(CapacityDetail {
                    capacity_units: table_units,
                }),
                global_secondary_indexes: units_to_map(gsi_units),
                local_secondary_indexes: units_to_map(lsi_units),
            })
        }
        "TOTAL" => {
            let gsi_total: f64 = gsi_units.values().sum();
            let lsi_total: f64 = lsi_units.values().sum();
            Some(ConsumedCapacity {
                table_name: table_name.to_string(),
                capacity_units: table_units + gsi_total + lsi_total,
                table: None,
                global_secondary_indexes: None,
                local_secondary_indexes: None,
            })
        }
        _ => None,
    }
}

/// Key schema element — defines a key attribute.
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

/// Attribute definition — declares an attribute's type.
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

/// Provisioned throughput settings (stored but not enforced).
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct ProvisionedThroughput {
    #[serde(rename = "ReadCapacityUnits", default)]
    pub read_capacity_units: Option<i64>,
    #[serde(rename = "WriteCapacityUnits", default)]
    pub write_capacity_units: Option<i64>,
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

// Integer types — all finite, all fit in DynamoDB's number range.
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
        // "42" has 2 significant digits → (2/2) + 1 = 2
        let val = AttributeValue::N("42".to_string());
        assert_eq!(val.size(), 2);
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
