use crate::errors::Result;
use crate::storage::Storage;
use crate::validation;
use serde::{Deserialize, Serialize};

/// Internal raw deserialization struct.
#[derive(Debug, Default, Deserialize)]
struct ListTablesRequestRaw {
    #[serde(rename = "ExclusiveStartTableName", default)]
    exclusive_start_table_name: Option<String>,
    #[serde(rename = "Limit", default)]
    limit: Option<i64>,
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

        let mut errors = Vec::new();

        // Validate ExclusiveStartTableName if present
        if let Some(ref name) = raw.exclusive_start_table_name {
            if name.is_empty()
                || !name
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
            {
                errors.push(format!(
                    "Value '{}' at 'exclusiveStartTableName' failed to satisfy constraint: \
                     Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+",
                    name
                ));
            }
            if name.len() < 3 {
                errors.push(format!(
                    "Value '{}' at 'exclusiveStartTableName' failed to satisfy constraint: \
                     Member must have length greater than or equal to 3",
                    name
                ));
            }
            if name.len() > 255 {
                errors.push(format!(
                    "Value '{}' at 'exclusiveStartTableName' failed to satisfy constraint: \
                     Member must have length less than or equal to 255",
                    name
                ));
            }
        }

        // Validate Limit
        if let Some(limit) = raw.limit {
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
            limit: raw.limit,
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

pub fn execute(storage: &Storage, request: ListTablesRequest) -> Result<ListTablesResponse> {
    let limit = request.limit.unwrap_or(100).clamp(1, 100) as usize;

    let all_tables = storage.list_table_names()?;

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
