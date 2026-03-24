use crate::errors::{DynoxideError, Result};
use crate::storage::Storage;
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Deserialize)]
pub struct UpdateTimeToLiveRequest {
    #[serde(rename = "TableName")]
    pub table_name: String,
    #[serde(rename = "TimeToLiveSpecification")]
    pub time_to_live_specification: TimeToLiveSpecification,
}

#[derive(Debug, Default, Deserialize, Serialize, Clone)]
pub struct TimeToLiveSpecification {
    #[serde(rename = "AttributeName")]
    pub attribute_name: String,
    #[serde(rename = "Enabled")]
    pub enabled: bool,
}

#[derive(Debug, Default, Serialize)]
pub struct UpdateTimeToLiveResponse {
    #[serde(rename = "TimeToLiveSpecification")]
    pub time_to_live_specification: TimeToLiveSpecification,
}

pub fn execute(
    storage: &Storage,
    request: UpdateTimeToLiveRequest,
) -> Result<UpdateTimeToLiveResponse> {
    // Validate table name format before checking existence (DynamoDB validates input first)
    crate::validation::validate_table_name(&request.table_name)?;

    // Validate attribute name is not empty
    if request.time_to_live_specification.attribute_name.is_empty() {
        return Err(DynoxideError::ValidationException(
            "1 validation error detected: Value '' at 'timeToLiveSpecification.attributeName' failed to satisfy constraint: Member must have length greater than or equal to 1".to_string(),
        ));
    }

    // Verify table exists
    let meta = storage
        .get_table_metadata(&request.table_name)?
        .ok_or_else(|| {
            DynoxideError::ResourceNotFoundException(format!(
                "Requested resource not found: Table: {} not found",
                request.table_name
            ))
        })?;

    // Validate: cannot enable TTL if it's already enabled with a different attribute
    if request.time_to_live_specification.enabled && meta.ttl_enabled {
        if let Some(ref existing_attr) = meta.ttl_attribute {
            if existing_attr != &request.time_to_live_specification.attribute_name {
                return Err(DynoxideError::ValidationException(
                    "TimeToLive is already enabled with a different attribute name".to_string(),
                ));
            }
        }
    }

    let attr_name = if request.time_to_live_specification.enabled {
        Some(request.time_to_live_specification.attribute_name.as_str())
    } else {
        None
    };

    storage.update_ttl_config(
        &request.table_name,
        attr_name,
        request.time_to_live_specification.enabled,
    )?;

    Ok(UpdateTimeToLiveResponse {
        time_to_live_specification: request.time_to_live_specification,
    })
}
