use crate::actions::vector_lifecycle::VectorIndexLifecycle;
use crate::actions::{TableDescription, build_table_description};
use crate::errors::{DynoxideError, Result};
use crate::storage_backend::StorageBackend;
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

pub async fn execute<S: StorageBackend>(
    storage: &S,
    request: DeleteTableRequest,
    lifecycle: &VectorIndexLifecycle,
) -> Result<DeleteTableResponse> {
    // Validate table name format before checking existence (DynamoDB validates input first)
    crate::validation::validate_table_name(&request.table_name)?;

    // Get metadata before deletion (for the response)
    let meta = storage
        .get_table_metadata(&request.table_name)
        .await?
        .ok_or_else(|| {
            DynoxideError::ResourceNotFoundException(format!(
                "Requested resource not found: Table: {} not found",
                request.table_name
            ))
        })?;

    // Check deletion protection
    if meta.deletion_protection_enabled {
        return Err(DynoxideError::ValidationException(
            "Resource cannot be deleted as it is currently protected against deletion. \
             Disable deletion protection first."
                .to_string(),
        ));
    }

    // A table cannot be dropped underneath an index that is still being
    // created, and it refuses for as long as the index is creating rather than
    // for a fixed number of calls: the documented readiness pattern polls
    // DescribeTable an unbounded number of times. Sits after the deletion
    // protection check so a protected table still answers its own message
    // first. Only indexes the table still defines are considered, so a stale
    // entry for one that has gone cannot refuse a delete.
    //
    // Definitions that will not parse read as no indexes, the way every other
    // reader of this column treats them: build_table_description reports none,
    // and the drop loop below has always skipped them. A table whose metadata
    // has become unreadable should stay deletable rather than be pinned by a
    // guard nothing can now satisfy.
    let vector_indexes: Vec<crate::types::VectorIndex> = meta
        .vector_index_definitions
        .as_ref()
        .and_then(|json| serde_json::from_str(json).ok())
        .unwrap_or_default();
    let vector_phases = lifecycle.phases_of(
        storage,
        &request.table_name,
        vector_indexes.iter().map(|v| v.index_name.as_str()),
    );
    if vector_phases.any_creating() {
        return Err(DynoxideError::ResourceInUseException(
            "Cannot delete table while indexes are being created, updated, or deleted.".to_string(),
        ));
    }

    // Forget the entries here rather than after the drops below: the guard has
    // just proved every one of them has settled, so none can affect an answer,
    // and any of the drops can fail and return early. Left until the end, a
    // half-finished delete would leak an entry per attempt.
    lifecycle.forget_table(&request.table_name);

    // Drop GSI tables first
    if let Some(ref gsi_json) = meta.gsi_definitions {
        if let Ok(gsis) = serde_json::from_str::<Vec<GlobalSecondaryIndex>>(gsi_json) {
            for gsi in &gsis {
                storage
                    .drop_gsi_table(&request.table_name, &gsi.index_name)
                    .await?;
            }
        }
    }

    // Drop LSI tables
    if let Some(ref lsi_json) = meta.lsi_definitions {
        if let Ok(lsis) = serde_json::from_str::<Vec<LocalSecondaryIndex>>(lsi_json) {
            for lsi in &lsis {
                storage
                    .drop_lsi_table(&request.table_name, &lsi.index_name)
                    .await?;
            }
        }
    }

    // Drop vector index shadow tables, from the definitions the guard above
    // already parsed.
    for vix in &vector_indexes {
        storage
            .drop_vector_table(&request.table_name, &vix.index_name)
            .await?;
    }

    // Drop data table
    storage.drop_data_table(&request.table_name).await?;

    // Delete metadata
    storage.delete_table_metadata(&request.table_name).await?;

    // Build response with DELETING status. The guard above ruled out a creating
    // index, so every phase in the set reports ACTIVE.
    let mut desc = build_table_description(&meta, Some(0), Some(0), &vector_phases);
    desc.table_status = "DELETING".to_string();

    Ok(DeleteTableResponse {
        table_description: desc,
    })
}
