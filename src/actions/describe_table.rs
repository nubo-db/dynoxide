use crate::actions::{TableDescription, build_table_description};
use crate::errors::{DynoxideError, Result};
use crate::storage::Storage;
use serde::{Deserialize, Serialize};

/// Internal deserialization struct for detecting missing TableName.
#[derive(Debug, Default, Deserialize)]
struct DescribeTableRequestRaw {
    #[serde(rename = "TableName", default)]
    table_name: Option<String>,
}

#[derive(Debug, Default)]
pub struct DescribeTableRequest {
    pub table_name: String,
}

impl<'de> serde::Deserialize<'de> for DescribeTableRequest {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let raw = DescribeTableRequestRaw::deserialize(deserializer)?;

        if raw.table_name.is_none() {
            return Err(serde::de::Error::custom(
                "VALIDATION:The parameter 'TableName' is required but was not present in the request",
            ));
        }
        let table_name = raw.table_name.unwrap();

        // Length check (before pattern, matching DynamoDB ordering)
        if table_name.len() < 3 || table_name.len() > 255 {
            return Err(serde::de::Error::custom(
                "VALIDATION:TableName must be at least 3 characters long and at most 255 characters long",
            ));
        }

        // Pattern check
        if !table_name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
        {
            return Err(serde::de::Error::custom(format!(
                "VALIDATION:1 validation error detected: \
                 Value '{}' at 'tableName' failed to satisfy constraint: \
                 Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+",
                table_name
            )));
        }

        Ok(DescribeTableRequest { table_name })
    }
}

#[derive(Debug, Default, Serialize)]
pub struct DescribeTableResponse {
    #[serde(rename = "Table")]
    pub table: TableDescription,
}

pub fn execute(storage: &Storage, request: DescribeTableRequest) -> Result<DescribeTableResponse> {
    // Validate table name format before checking existence (DynamoDB validates input first)
    crate::validation::validate_table_name(&request.table_name)?;

    let meta = storage
        .get_table_metadata(&request.table_name)?
        .ok_or_else(|| {
            DynoxideError::ResourceNotFoundException(format!(
                "Requested resource not found: Table: {} not found",
                request.table_name
            ))
        })?;

    // Get actual item count and size
    let item_count = storage.count_items(&request.table_name).ok();
    let table_size_bytes = item_count.map(|_| 0i64); // Approximate; real size tracking is deferred

    let desc = build_table_description(&meta, item_count, table_size_bytes);

    Ok(DescribeTableResponse { table: desc })
}
