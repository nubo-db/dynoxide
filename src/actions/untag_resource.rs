use crate::actions::helpers;
use crate::errors::{DynoxideError, Result};
use crate::storage::Storage;
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Deserialize)]
pub struct UntagResourceRequest {
    #[serde(rename = "ResourceArn", default)]
    pub resource_arn: Option<String>,
    #[serde(rename = "TagKeys", default)]
    pub tag_keys: Vec<String>,
}

#[derive(Debug, Default, Serialize)]
pub struct UntagResourceResponse {}

pub fn execute(storage: &Storage, request: UntagResourceRequest) -> Result<UntagResourceResponse> {
    let arn = request.resource_arn.as_deref().unwrap_or("");
    if arn.is_empty() {
        return Err(DynoxideError::ValidationException(
            "Invalid TableArn".to_string(),
        ));
    }
    let table_name = helpers::parse_table_name_from_arn(arn)?;

    // Validate tag keys before checking table existence (DynamoDB validates input first)
    if request.tag_keys.is_empty() {
        return Err(DynoxideError::ValidationException(
            "Atleast one Tag Key needs to be provided as Input.".to_string(),
        ));
    }

    // Verify table exists
    if !storage.table_exists(table_name)? {
        return Err(DynoxideError::ResourceNotFoundException(
            "Requested resource not found".to_string(),
        ));
    }

    storage.remove_tags(table_name, &request.tag_keys)?;

    Ok(UntagResourceResponse {})
}
