//! Schema sourcing from `--schema <file>`.
//!
//! Parses a JSON file containing an array of DescribeTable responses
//! (raw DynamoDB JSON format) and converts them into CreateTableRequests.

use crate::actions::create_table::CreateTableRequest;
use std::path::Path;

/// A parsed table schema ready for table creation.
#[derive(Debug)]
pub struct TableSchema {
    /// The table name.
    pub table_name: String,
    /// The CreateTableRequest to create this table.
    pub create_request: CreateTableRequest,
}

/// Load table schemas from a JSON file.
///
/// The file should contain either:
/// - An array of DescribeTable responses: `[{"Table": {...}}, ...]`
/// - A single DescribeTable response: `{"Table": {...}}`
///
/// This is the output format of `aws dynamodb describe-table`.
/// Load table schemas from a JSON file.
///
/// Returns both the parsed schemas and the raw JSON value (for re-serialization
/// into CreateTableRequests without re-reading the file).
pub fn load_schemas(path: &Path) -> Result<(Vec<TableSchema>, serde_json::Value), String> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| format!("Failed to read schema file {}: {e}", path.display()))?;

    let value: serde_json::Value =
        serde_json::from_str(&content).map_err(|e| format!("Failed to parse schema JSON: {e}"))?;

    let schemas = match &value {
        serde_json::Value::Array(arr) => {
            let mut schemas = Vec::with_capacity(arr.len());
            for (i, item) in arr.iter().enumerate() {
                schemas.push(
                    parse_describe_table_response(item)
                        .map_err(|e| format!("Schema {}: {e}", i + 1))?,
                );
            }
            schemas
        }
        serde_json::Value::Object(_) => vec![parse_describe_table_response(&value)?],
        _ => return Err("Schema file must contain a JSON object or array".to_string()),
    };

    Ok((schemas, value))
}

/// Parse a single DescribeTable response into a TableSchema.
///
/// Goes through the same translation and deserialisation `run_into` uses to
/// create the table, rather than a parse of its own. The two used to be
/// separate, and this one had never learned about LocalSecondaryIndexes, so
/// the table was created with its LSIs while everything downstream that read
/// this schema believed there were none: the warning about LSI sort keys
/// keeping their values could never fire. One parse, one truth.
fn parse_describe_table_response(value: &serde_json::Value) -> Result<TableSchema, String> {
    // DescribeTable response has a "Table" wrapper
    let table = value.get("Table").unwrap_or(value);

    let table_name = table
        .get("TableName")
        .and_then(|v| v.as_str())
        .ok_or("missing TableName")?
        .to_string();

    let mut table = table.clone();
    super::unwrap_describe_table_shapes(&mut table);
    let create_request: CreateTableRequest =
        serde_json::from_value(table).map_err(|e| format!("table '{table_name}': {e}"))?;

    Ok(TableSchema {
        table_name,
        create_request,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_describe_table_json() -> serde_json::Value {
        serde_json::json!({
            "Table": {
                "TableName": "Users",
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "sk", "KeyType": "RANGE"}
                ],
                "AttributeDefinitions": [
                    {"AttributeName": "pk", "AttributeType": "S"},
                    {"AttributeName": "sk", "AttributeType": "S"},
                    {"AttributeName": "gsi1pk", "AttributeType": "S"}
                ],
                "GlobalSecondaryIndexes": [{
                    "IndexName": "gsi1",
                    "KeySchema": [
                        {"AttributeName": "gsi1pk", "KeyType": "HASH"}
                    ],
                    "Projection": {
                        "ProjectionType": "ALL"
                    }
                }]
            }
        })
    }

    #[test]
    fn test_parse_describe_table_response() {
        let schema = parse_describe_table_response(&sample_describe_table_json()).unwrap();
        assert_eq!(schema.table_name, "Users");
        assert_eq!(schema.create_request.key_schema.len(), 2);
        assert_eq!(schema.create_request.attribute_definitions.len(), 3);
        assert!(schema.create_request.global_secondary_indexes.is_some());
        assert_eq!(
            schema
                .create_request
                .global_secondary_indexes
                .as_ref()
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn test_parse_without_table_wrapper() {
        let json = serde_json::json!({
            "TableName": "Simple",
            "KeySchema": [
                {"AttributeName": "id", "KeyType": "HASH"}
            ],
            "AttributeDefinitions": [
                {"AttributeName": "id", "AttributeType": "S"}
            ]
        });
        let schema = parse_describe_table_response(&json).unwrap();
        assert_eq!(schema.table_name, "Simple");
    }

    #[test]
    fn a_local_secondary_index_survives_the_parse() {
        // The hand-written parse this replaced dropped LSIs on the floor, so
        // the deriver never saw them and the warning about their sort keys
        // keeping real values could not fire.
        let json = serde_json::json!({"Table": {
            "TableName": "Orders",
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"}
            ],
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
                {"AttributeName": "lsisk", "AttributeType": "S"}
            ],
            "LocalSecondaryIndexes": [{
                "IndexName": "LSI1",
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "lsisk", "KeyType": "RANGE"}
                ],
                "Projection": {"ProjectionType": "ALL"}
            }]
        }});
        let schema = parse_describe_table_response(&json).unwrap();
        let lsis = schema
            .create_request
            .local_secondary_indexes
            .as_deref()
            .unwrap_or(&[]);
        assert_eq!(
            lsis.len(),
            1,
            "the LSI has to reach the schema the deriver reads"
        );
        assert_eq!(lsis[0].index_name, "LSI1");
    }

    #[test]
    fn test_parse_missing_table_name() {
        let json = serde_json::json!({
            "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}]
        });
        assert!(parse_describe_table_response(&json).is_err());
    }
}
