use crate::errors::{DynoxideError, Result};
use crate::storage_backend::StorageBackend;
use serde::{Deserialize, Serialize};

/// Internal raw deserialization struct.
#[derive(Debug, Default, Deserialize)]
struct DescribeTimeToLiveRequestRaw {
    #[serde(rename = "TableName", default)]
    table_name: Option<String>,
}

#[derive(Debug, Default)]
pub struct DescribeTimeToLiveRequest {
    pub table_name: String,
}

impl<'de> serde::Deserialize<'de> for DescribeTimeToLiveRequest {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let raw = DescribeTimeToLiveRequestRaw::deserialize(deserializer)?;

        let Some(table_name) = raw.table_name else {
            return Err(serde::de::Error::custom(
                "VALIDATION:The parameter 'TableName' is required but was not present in the request",
            ));
        };

        // The length in UTF-16 units, then the pattern, both listed in the
        // envelope; an empty name gets a bare sentence.
        if let Some(msg) = crate::validation::control_table_name_rejection(&table_name) {
            return Err(serde::de::Error::custom(format!("VALIDATION:{msg}")));
        }

        Ok(DescribeTimeToLiveRequest { table_name })
    }
}

#[derive(Debug, Default, Serialize)]
pub struct DescribeTimeToLiveResponse {
    #[serde(rename = "TimeToLiveDescription")]
    pub time_to_live_description: TimeToLiveDescription,
}

#[derive(Debug, Default, Serialize)]
pub struct TimeToLiveDescription {
    #[serde(rename = "TimeToLiveStatus")]
    pub time_to_live_status: String,
    #[serde(rename = "AttributeName", skip_serializing_if = "Option::is_none")]
    pub attribute_name: Option<String>,
}

pub async fn execute<S: StorageBackend>(
    storage: &S,
    request: DescribeTimeToLiveRequest,
) -> Result<DescribeTimeToLiveResponse> {
    // The checks the body was read with, for a request built in code.
    crate::validation::validate_control_table_name(&request.table_name)?;

    let meta = storage
        .get_table_metadata(&request.table_name)
        .await?
        .ok_or_else(|| {
            DynoxideError::ResourceNotFoundException(format!(
                "Requested resource not found: Table: {} not found",
                request.table_name
            ))
        })?;

    let (status, attribute_name) = if meta.ttl_enabled {
        ("ENABLED".to_string(), meta.ttl_attribute)
    } else {
        ("DISABLED".to_string(), None)
    };

    Ok(DescribeTimeToLiveResponse {
        time_to_live_description: TimeToLiveDescription {
            time_to_live_status: status,
            attribute_name,
        },
    })
}
