//! Schema sourcing from `--schema <file>`.
//!
//! Parses a JSON file containing an array of DescribeTable responses
//! (raw DynamoDB JSON format) and converts them into CreateTableRequests.

use crate::actions::create_table::CreateTableRequest;
use crate::types::{
    AttributeDefinition, GlobalSecondaryIndex, KeySchemaElement, KeyType, Projection,
    ProjectionType, ScalarAttributeType,
};
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
fn parse_describe_table_response(value: &serde_json::Value) -> Result<TableSchema, String> {
    // DescribeTable response has a "Table" wrapper
    let table = value.get("Table").unwrap_or(value);

    let table_name = table
        .get("TableName")
        .and_then(|v| v.as_str())
        .ok_or("missing TableName")?
        .to_string();

    // Parse KeySchema
    let key_schema = table
        .get("KeySchema")
        .and_then(|v| v.as_array())
        .ok_or("missing KeySchema")?;

    let key_schema_parsed: Vec<KeySchemaElement> = key_schema
        .iter()
        .map(parse_key_schema_element)
        .collect::<Result<_, _>>()?;

    // Parse AttributeDefinitions
    let attr_defs = table
        .get("AttributeDefinitions")
        .and_then(|v| v.as_array())
        .ok_or("missing AttributeDefinitions")?;

    let attr_defs_parsed: Vec<AttributeDefinition> = attr_defs
        .iter()
        .map(parse_attribute_definition)
        .collect::<Result<_, _>>()?;

    // Parse GSIs (optional)
    let gsis = table
        .get("GlobalSecondaryIndexes")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().map(parse_gsi).collect::<Result<Vec<_>, _>>())
        .transpose()?;

    // Parse StreamSpecification (optional)
    let stream_spec = table
        .get("StreamSpecification")
        .map(parse_stream_specification)
        .transpose()?;

    let create_request = CreateTableRequest {
        table_name: table_name.clone(),
        key_schema: key_schema_parsed,
        attribute_definitions: attr_defs_parsed,
        global_secondary_indexes: gsis,
        billing_mode: None,
        provisioned_throughput: None,
        stream_specification: stream_spec,
        ..Default::default()
    };

    Ok(TableSchema {
        table_name,
        create_request,
    })
}

fn parse_key_schema_element(ks: &serde_json::Value) -> Result<KeySchemaElement, String> {
    let name = ks
        .get("AttributeName")
        .and_then(|v| v.as_str())
        .ok_or("KeySchema element missing AttributeName")?;
    let key_type = ks
        .get("KeyType")
        .and_then(|v| v.as_str())
        .ok_or("KeySchema element missing KeyType")?;

    Ok(KeySchemaElement {
        attribute_name: name.to_string(),
        key_type: match key_type {
            "HASH" => KeyType::HASH,
            "RANGE" => KeyType::RANGE,
            other => return Err(format!("unknown KeyType: '{other}'")),
        },
    })
}

fn parse_attribute_definition(ad: &serde_json::Value) -> Result<AttributeDefinition, String> {
    let name = ad
        .get("AttributeName")
        .and_then(|v| v.as_str())
        .ok_or("AttributeDefinition missing AttributeName")?;
    let attr_type = ad
        .get("AttributeType")
        .and_then(|v| v.as_str())
        .ok_or("AttributeDefinition missing AttributeType")?;

    Ok(AttributeDefinition {
        attribute_name: name.to_string(),
        attribute_type: match attr_type {
            "S" => ScalarAttributeType::S,
            "N" => ScalarAttributeType::N,
            "B" => ScalarAttributeType::B,
            other => return Err(format!("unknown AttributeType: '{other}'")),
        },
    })
}

fn parse_gsi(gsi: &serde_json::Value) -> Result<GlobalSecondaryIndex, String> {
    let index_name = gsi
        .get("IndexName")
        .and_then(|v| v.as_str())
        .ok_or("GSI missing IndexName")?
        .to_string();

    let key_schema = gsi
        .get("KeySchema")
        .and_then(|v| v.as_array())
        .ok_or("GSI missing KeySchema")?
        .iter()
        .map(parse_key_schema_element)
        .collect::<Result<Vec<_>, _>>()?;

    let projection = gsi
        .get("Projection")
        .map(parse_projection)
        .transpose()?
        .unwrap_or_default();

    Ok(GlobalSecondaryIndex {
        index_name,
        key_schema,
        projection,
        provisioned_throughput: None,
    })
}

fn parse_projection(proj: &serde_json::Value) -> Result<Projection, String> {
    let projection_type = proj
        .get("ProjectionType")
        .and_then(|v| v.as_str())
        .map(|pt| match pt {
            "ALL" => Ok(ProjectionType::ALL),
            "KEYS_ONLY" => Ok(ProjectionType::KEYS_ONLY),
            "INCLUDE" => Ok(ProjectionType::INCLUDE),
            other => Err(format!("unknown ProjectionType: '{other}'")),
        })
        .transpose()?;

    let non_key_attributes = proj
        .get("NonKeyAttributes")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        });

    Ok(Projection {
        projection_type,
        non_key_attributes,
    })
}

fn parse_stream_specification(
    spec: &serde_json::Value,
) -> Result<crate::actions::create_table::StreamSpecification, String> {
    let enabled = spec
        .get("StreamEnabled")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let view_type = spec
        .get("StreamViewType")
        .and_then(|v| v.as_str())
        .map(String::from);

    Ok(crate::actions::create_table::StreamSpecification {
        stream_enabled: enabled,
        stream_view_type: view_type,
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
    fn test_parse_missing_table_name() {
        let json = serde_json::json!({
            "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}]
        });
        assert!(parse_describe_table_response(&json).is_err());
    }
}
