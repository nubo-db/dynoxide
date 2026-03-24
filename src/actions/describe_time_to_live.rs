use crate::errors::{DynoxideError, Result};
use crate::storage::Storage;
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

        if raw.table_name.is_none() || raw.table_name.as_deref() == Some("") {
            let msg = if raw.table_name.is_none() {
                "The parameter 'TableName' is required but was not present in the request"
            } else {
                "TableName must be at least 3 characters long and at most 255 characters long"
            };
            return Err(serde::de::Error::custom(format!("VALIDATION:{}", msg)));
        }

        let table_name = raw.table_name.unwrap_or_default();

        if table_name.len() < 3 || table_name.len() > 255 {
            return Err(serde::de::Error::custom(
                "VALIDATION:TableName must be at least 3 characters long and at most 255 characters long",
            ));
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

pub fn execute(
    storage: &Storage,
    request: DescribeTimeToLiveRequest,
) -> Result<DescribeTimeToLiveResponse> {
    // Table name format validation already done in Deserialize impl;
    // only pattern validation remains
    crate::validation::validate_table_name(&request.table_name)?;

    let meta = storage
        .get_table_metadata(&request.table_name)?
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
