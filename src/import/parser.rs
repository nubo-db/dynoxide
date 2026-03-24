//! DynamoDB Export JSON Lines parser.
//!
//! Parses gzipped `.json.gz` files from DynamoDB Export format.
//! Each line is a JSON object with an `Item` field containing a DynamoDB-typed item.

use crate::types::{AttributeValue, Item};
use flate2::read::GzDecoder;
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read};
use std::path::Path;

/// Maximum total decompressed bytes before aborting (50 GB).
/// Legitimate DynamoDB exports can be large, but this prevents decompression bombs.
const MAX_DECOMPRESSED_BYTES: u64 = 50 * 1024 * 1024 * 1024;

/// Maximum length of a single line in bytes (4 MB).
/// DynamoDB items are at most 400 KB, so a 4 MB line cap is generous
/// even accounting for JSON overhead and base64-encoded binary attributes.
const MAX_LINE_LENGTH: usize = 4 * 1024 * 1024;

/// BufReader capacity (256 KB). The default 8 KB is too small for gzip
/// decompression — larger buffers amortize decoder overhead significantly.
const BUF_READER_CAPACITY: usize = 256 * 1024;

/// Statistics from streaming parse (no items held in memory).
pub struct StreamStats {
    /// Number of lines that failed to parse.
    pub skipped: usize,
    /// Warning messages for skipped lines.
    pub warnings: Vec<String>,
}

/// Parse a DynamoDB Export file in streaming fashion, calling `handler` for
/// each successfully parsed item. Items are never collected into memory.
pub fn parse_export_file_streaming<F>(path: &Path, mut handler: F) -> Result<StreamStats, String>
where
    F: FnMut(Item),
{
    let file =
        std::fs::File::open(path).map_err(|e| format!("Failed to open {}: {e}", path.display()))?;

    let is_gzipped = path.extension().is_some_and(|ext| ext == "gz");

    let reader: Box<dyn Read> = if is_gzipped {
        // Cap total decompressed size to prevent gzip bombs
        Box::new(GzDecoder::new(file).take(MAX_DECOMPRESSED_BYTES))
    } else {
        Box::new(file)
    };

    let buf_reader = BufReader::with_capacity(BUF_READER_CAPACITY, reader);
    let mut skipped = 0;
    let mut warnings = Vec::new();
    let mut line_buf = String::with_capacity(4096);
    let mut line_num = 0usize;

    // Use read_line with a reusable buffer instead of lines() to avoid
    // per-line allocation and to enforce a per-line length cap.
    let mut reader = buf_reader;
    loop {
        line_buf.clear();
        let bytes_read = reader.read_line(&mut line_buf).map_err(|e| {
            format!(
                "{}:{}: failed to read line: {e}",
                path.display(),
                line_num + 1
            )
        })?;
        if bytes_read == 0 {
            break; // EOF
        }
        line_num += 1;

        if line_buf.len() > MAX_LINE_LENGTH {
            skipped += 1;
            warnings.push(format!(
                "{}:{}: line exceeds maximum length of {} bytes ({} bytes)",
                path.display(),
                line_num,
                MAX_LINE_LENGTH,
                line_buf.len()
            ));
            continue;
        }

        let trimmed = line_buf.trim();
        if trimmed.is_empty() {
            continue;
        }

        match parse_export_line(trimmed) {
            Ok(item) => handler(item),
            Err(e) => {
                skipped += 1;
                warnings.push(format!("{}:{}: {e}", path.display(), line_num));
            }
        }
    }

    Ok(StreamStats { skipped, warnings })
}

/// Parse a single JSON line from DynamoDB Export format.
///
/// Expected format: `{"Item": {...DynamoDB typed attributes...}}`
fn parse_export_line(line: &str) -> Result<Item, String> {
    let value: serde_json::Value =
        serde_json::from_str(line).map_err(|e| format!("invalid JSON: {e}"))?;

    let obj = value.as_object().ok_or("expected JSON object")?;

    let item_value = obj
        .get("Item")
        .ok_or("missing 'Item' field in export line")?;

    let item_obj = item_value
        .as_object()
        .ok_or("'Item' field is not an object")?;

    parse_dynamodb_item(item_obj)
}

/// Maximum nesting depth for DynamoDB items (matches DynamoDB's own limit).
const MAX_NESTING_DEPTH: usize = 32;

/// Parse a DynamoDB-typed JSON object into an Item (HashMap<String, AttributeValue>).
fn parse_dynamodb_item(obj: &serde_json::Map<String, serde_json::Value>) -> Result<Item, String> {
    parse_dynamodb_item_with_depth(obj, 0)
}

fn parse_dynamodb_item_with_depth(
    obj: &serde_json::Map<String, serde_json::Value>,
    depth: usize,
) -> Result<Item, String> {
    if depth > MAX_NESTING_DEPTH {
        return Err(format!(
            "nesting depth exceeds maximum of {MAX_NESTING_DEPTH} levels"
        ));
    }
    let mut item = HashMap::new();
    for (key, value) in obj {
        let attr = parse_attribute_value_with_depth(value, depth)
            .map_err(|e| format!("attribute '{key}': {e}"))?;
        item.insert(key.clone(), attr);
    }
    Ok(item)
}

/// Parse a DynamoDB-typed JSON value into an AttributeValue.
///
/// DynamoDB format examples:
/// - `{"S": "hello"}` → String
/// - `{"N": "42"}` → Number (stored as string)
/// - `{"BOOL": true}` → Boolean
/// - `{"NULL": true}` → Null
/// - `{"B": "base64data"}` → Binary
/// - `{"L": [...]}` → List
/// - `{"M": {...}}` → Map
/// - `{"SS": ["a", "b"]}` → String Set
/// - `{"NS": ["1", "2"]}` → Number Set
/// - `{"BS": ["base64a", "base64b"]}` → Binary Set
fn parse_attribute_value_with_depth(
    value: &serde_json::Value,
    depth: usize,
) -> Result<AttributeValue, String> {
    let obj = value
        .as_object()
        .ok_or("expected DynamoDB-typed object (e.g., {\"S\": \"value\"})")?;

    if obj.len() != 1 {
        return Err(format!(
            "expected exactly one type descriptor, got {}",
            obj.len()
        ));
    }

    let (type_key, inner) = obj.iter().next().unwrap();

    match type_key.as_str() {
        "S" => {
            let s = inner.as_str().ok_or("S value must be a string")?;
            Ok(AttributeValue::S(s.to_string()))
        }
        "N" => {
            let n = inner.as_str().ok_or("N value must be a string")?;
            Ok(AttributeValue::N(n.to_string()))
        }
        "B" => {
            let b = inner.as_str().ok_or("B value must be a base64 string")?;
            let bytes = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, b)
                .map_err(|e| format!("invalid base64 in B value: {e}"))?;
            Ok(AttributeValue::B(bytes))
        }
        "BOOL" => {
            let b = inner.as_bool().ok_or("BOOL value must be a boolean")?;
            Ok(AttributeValue::BOOL(b))
        }
        "NULL" => Ok(AttributeValue::NULL(true)),
        "L" => {
            let arr = inner.as_array().ok_or("L value must be an array")?;
            let list: Result<Vec<_>, _> = arr
                .iter()
                .map(|v| parse_attribute_value_with_depth(v, depth + 1))
                .collect();
            Ok(AttributeValue::L(list?))
        }
        "M" => {
            let map_obj = inner.as_object().ok_or("M value must be an object")?;
            let item = parse_dynamodb_item_with_depth(map_obj, depth + 1)?;
            Ok(AttributeValue::M(item))
        }
        "SS" => {
            let arr = inner.as_array().ok_or("SS value must be an array")?;
            let set: Result<Vec<_>, _> = arr
                .iter()
                .map(|v| {
                    v.as_str()
                        .map(String::from)
                        .ok_or("SS elements must be strings")
                })
                .collect();
            Ok(AttributeValue::SS(set?))
        }
        "NS" => {
            let arr = inner.as_array().ok_or("NS value must be an array")?;
            let set: Result<Vec<_>, _> = arr
                .iter()
                .map(|v| {
                    v.as_str()
                        .map(String::from)
                        .ok_or("NS elements must be strings")
                })
                .collect();
            Ok(AttributeValue::NS(set?))
        }
        "BS" => {
            let arr = inner.as_array().ok_or("BS value must be an array")?;
            let set: Result<Vec<_>, _> = arr
                .iter()
                .map(|v| {
                    let s = v.as_str().ok_or("BS elements must be base64 strings")?;
                    base64::Engine::decode(&base64::engine::general_purpose::STANDARD, s)
                        .map_err(|e| format!("invalid base64 in BS value: {e}"))
                })
                .collect();
            Ok(AttributeValue::BS(set?))
        }
        other => Err(format!("unknown type descriptor: '{other}'")),
    }
}

/// Discover export files in a DynamoDB Export directory structure.
///
/// Supports two layouts:
/// 1. DynamoDB Export structure: `<dir>/<TableName>/data/*.json.gz`
/// 2. Flat directory: `<dir>/*.json.gz` or `<dir>/*.json`
///
/// Returns: `Vec<(table_name, Vec<file_paths>)>`
pub fn discover_export_files(
    source_dir: &Path,
    table_filter: Option<&[String]>,
) -> Result<Vec<(String, Vec<std::path::PathBuf>)>, String> {
    if !source_dir.is_dir() {
        return Err(format!("{} is not a directory", source_dir.display()));
    }

    let mut tables = Vec::new();

    // Check for DynamoDB Export structure: subdirectories with data/ folders
    let mut has_table_dirs = false;
    let entries = std::fs::read_dir(source_dir)
        .map_err(|e| format!("Failed to read {}: {e}", source_dir.display()))?;

    for entry in entries {
        let entry = entry.map_err(|e| format!("Failed to read entry: {e}"))?;
        let path = entry.path();

        if path.is_dir() {
            let data_dir = path.join("data");
            if data_dir.is_dir() {
                has_table_dirs = true;
                let table_name = path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .ok_or_else(|| format!("Invalid directory name: {}", path.display()))?
                    .to_string();

                // Apply table filter
                if let Some(filter) = table_filter {
                    if !filter.iter().any(|f| f == &table_name) {
                        continue;
                    }
                }

                let files = collect_data_files(&data_dir)?;
                if !files.is_empty() {
                    tables.push((table_name, files));
                }
            }
        }
    }

    // If no table subdirectories found, try flat directory
    if !has_table_dirs {
        let files = collect_data_files(source_dir)?;
        if !files.is_empty() {
            // For flat directories, we need a table name from context.
            // Use the directory name as the table name.
            let table_name = source_dir
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("default")
                .to_string();
            tables.push((table_name, files));
        }
    }

    // Sort for deterministic ordering
    tables.sort_by(|a, b| a.0.cmp(&b.0));

    Ok(tables)
}

/// Collect `.json.gz` and `.json` files from a directory.
fn collect_data_files(dir: &Path) -> Result<Vec<std::path::PathBuf>, String> {
    let mut files = Vec::new();

    let entries =
        std::fs::read_dir(dir).map_err(|e| format!("Failed to read {}: {e}", dir.display()))?;

    for entry in entries {
        let entry = entry.map_err(|e| format!("Failed to read entry: {e}"))?;
        let path = entry.path();

        if path.is_file() {
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if name.ends_with(".json.gz") || name.ends_with(".json") {
                files.push(path);
            }
        }
    }

    files.sort();
    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_export_line() {
        let line =
            r#"{"Item": {"pk": {"S": "USER#123"}, "name": {"S": "Alice"}, "age": {"N": "30"}}}"#;
        let item = parse_export_line(line).unwrap();
        assert_eq!(
            item.get("pk").unwrap(),
            &AttributeValue::S("USER#123".to_string())
        );
        assert_eq!(
            item.get("name").unwrap(),
            &AttributeValue::S("Alice".to_string())
        );
        assert_eq!(
            item.get("age").unwrap(),
            &AttributeValue::N("30".to_string())
        );
    }

    #[test]
    fn test_parse_export_line_with_nested() {
        let line = r#"{"Item": {"pk": {"S": "key"}, "data": {"M": {"city": {"S": "NYC"}}}}}"#;
        let item = parse_export_line(line).unwrap();
        if let AttributeValue::M(map) = item.get("data").unwrap() {
            assert_eq!(
                map.get("city").unwrap(),
                &AttributeValue::S("NYC".to_string())
            );
        } else {
            panic!("expected M type");
        }
    }

    #[test]
    fn test_parse_export_line_with_list() {
        let line = r#"{"Item": {"pk": {"S": "key"}, "tags": {"L": [{"S": "a"}, {"S": "b"}]}}}"#;
        let item = parse_export_line(line).unwrap();
        if let AttributeValue::L(list) = item.get("tags").unwrap() {
            assert_eq!(list.len(), 2);
        } else {
            panic!("expected L type");
        }
    }

    #[test]
    fn test_parse_export_line_with_sets() {
        let line =
            r#"{"Item": {"pk": {"S": "key"}, "ss": {"SS": ["a", "b"]}, "ns": {"NS": ["1", "2"]}}}"#;
        let item = parse_export_line(line).unwrap();
        if let AttributeValue::SS(set) = item.get("ss").unwrap() {
            assert_eq!(set.len(), 2);
        } else {
            panic!("expected SS type");
        }
    }

    #[test]
    fn test_parse_export_line_with_bool_null() {
        let line = r#"{"Item": {"pk": {"S": "key"}, "active": {"BOOL": true}, "deleted": {"NULL": true}}}"#;
        let item = parse_export_line(line).unwrap();
        assert_eq!(item.get("active").unwrap(), &AttributeValue::BOOL(true));
        assert_eq!(item.get("deleted").unwrap(), &AttributeValue::NULL(true));
    }

    #[test]
    fn test_parse_invalid_json() {
        assert!(parse_export_line("not json").is_err());
    }

    #[test]
    fn test_parse_missing_item_field() {
        assert!(parse_export_line(r#"{"Data": {}}"#).is_err());
    }
}
