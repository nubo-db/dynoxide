use crate::actions::{TableDescription, build_table_description};
use crate::errors::{DynoxideError, Result};
use crate::storage::Storage;
use crate::types::{GlobalSecondaryIndex, LocalSecondaryIndex};
use serde::{Deserialize, Serialize};

/// Internal deserialization struct for detecting missing TableName.
#[derive(Debug, Default, Deserialize)]
struct DeleteTableRequestRaw {
    #[serde(rename = "TableName", default)]
    table_name: Option<String>,
}

#[derive(Debug, Default)]
pub struct DeleteTableRequest {
    pub table_name: String,
}

impl<'de> serde::Deserialize<'de> for DeleteTableRequest {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let raw = DeleteTableRequestRaw::deserialize(deserializer)?;

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

        // Pattern check (only reached if length is valid)
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

        Ok(DeleteTableRequest { table_name })
    }
}

#[derive(Debug, Default, Serialize)]
pub struct DeleteTableResponse {
    #[serde(rename = "TableDescription")]
    pub table_description: TableDescription,
}

pub fn execute(storage: &Storage, request: DeleteTableRequest) -> Result<DeleteTableResponse> {
    // Validate table name format before checking existence (DynamoDB validates input first)
    crate::validation::validate_table_name(&request.table_name)?;

    // Get metadata before deletion (for the response)
    let meta = storage
        .get_table_metadata(&request.table_name)?
        .ok_or_else(|| {
            DynoxideError::ResourceNotFoundException(format!(
                "Requested resource not found: Table: {} not found",
                request.table_name
            ))
        })?;

    // Check deletion protection
    if meta.deletion_protection_enabled {
        return Err(DynoxideError::ValidationException(format!(
            "Resource {} can't be deleted because deletion protection is enabled",
            crate::streams::table_arn(&request.table_name)
        )));
    }

    // Drop GSI tables first
    if let Some(ref gsi_json) = meta.gsi_definitions {
        if let Ok(gsis) = serde_json::from_str::<Vec<GlobalSecondaryIndex>>(gsi_json) {
            for gsi in &gsis {
                storage.drop_gsi_table(&request.table_name, &gsi.index_name)?;
            }
        }
    }

    // Drop LSI tables
    if let Some(ref lsi_json) = meta.lsi_definitions {
        if let Ok(lsis) = serde_json::from_str::<Vec<LocalSecondaryIndex>>(lsi_json) {
            for lsi in &lsis {
                storage.drop_lsi_table(&request.table_name, &lsi.index_name)?;
            }
        }
    }

    // Drop data table
    storage.drop_data_table(&request.table_name)?;

    // Delete metadata
    storage.delete_table_metadata(&request.table_name)?;

    // Build response with DELETING status
    let mut desc = build_table_description(&meta, Some(0), Some(0));
    desc.table_status = "DELETING".to_string();

    Ok(DeleteTableResponse {
        table_description: desc,
    })
}
