use crate::errors::Result;
use crate::storage_backend::StorageBackend;
use crate::validation;
use serde::{Deserialize, Serialize};

/// Internal raw deserialization struct.
#[derive(Debug, Default, Deserialize)]
struct ListTablesRequestRaw {
    #[serde(rename = "ExclusiveStartTableName", default)]
    exclusive_start_table_name: Option<String>,
    /// Read as Query's `Limit` is: a fraction truncates, a number past the
    /// range saturates and a malformed token is read by DynamoDB's integer
    /// rule (captured eu-west-2, 9 and 10 November 2026).
    #[serde(
        rename = "Limit",
        default,
        deserialize_with = "super::helpers::deserialize_int32"
    )]
    limit: Option<super::helpers::Int32>,
}

#[derive(Debug, Default)]
pub struct ListTablesRequest {
    pub exclusive_start_table_name: Option<String>,
    pub limit: Option<i64>,
}

impl<'de> serde::Deserialize<'de> for ListTablesRequest {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let raw = ListTablesRequestRaw::deserialize(deserializer)?;
        let limit = raw.limit.map(|limit| limit.value);

        let mut errors = Vec::new();

        // Validate ExclusiveStartTableName if present: the length in
        // characters, then the pattern (captured eu-west-2 and us-east-1,
        // 12 November 2026).
        if let Some(ref name) = raw.exclusive_start_table_name {
            errors.extend(crate::validation::name_errors_length_first(
                name,
                "exclusiveStartTableName",
                crate::validation::length_in_characters(name),
            ));
        }

        // Validate Limit
        if let Some(limit) = limit {
            if limit < 1 {
                errors.push(format!(
                    "Value '{}' at 'limit' failed to satisfy constraint: \
                     Member must have value greater than or equal to 1",
                    limit
                ));
            }
            if limit > 100 {
                errors.push(format!(
                    "Value '{}' at 'limit' failed to satisfy constraint: \
                     Member must have value less than or equal to 100",
                    limit
                ));
            }
        }

        if let Some(msg) = validation::format_validation_errors(&errors) {
            return Err(serde::de::Error::custom(format!("VALIDATION:{}", msg)));
        }

        Ok(ListTablesRequest {
            exclusive_start_table_name: raw.exclusive_start_table_name,
            limit,
        })
    }
}

#[derive(Debug, Default, Serialize)]
pub struct ListTablesResponse {
    #[serde(rename = "TableNames")]
    pub table_names: Vec<String>,
    #[serde(
        rename = "LastEvaluatedTableName",
        skip_serializing_if = "Option::is_none"
    )]
    pub last_evaluated_table_name: Option<String>,
}

pub async fn execute<S: StorageBackend>(
    storage: &S,
    request: ListTablesRequest,
) -> Result<ListTablesResponse> {
    let limit = request.limit.unwrap_or(100).clamp(1, 100) as usize;

    let all_tables = storage.list_table_names().await?;

    // Filter by ExclusiveStartTableName
    let filtered: Vec<String> = if let Some(ref start) = request.exclusive_start_table_name {
        all_tables
            .into_iter()
            .filter(|name| name.as_str() > start.as_str())
            .collect()
    } else {
        all_tables
    };

    // Apply limit
    let has_more = filtered.len() > limit;
    let table_names: Vec<String> = filtered.into_iter().take(limit).collect();

    let last_evaluated_table_name = if has_more {
        table_names.last().cloned()
    } else {
        None
    };

    Ok(ListTablesResponse {
        table_names,
        last_evaluated_table_name,
    })
}
