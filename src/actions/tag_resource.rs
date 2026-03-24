use crate::actions::helpers;
use crate::errors::{DynoxideError, Result};
use crate::storage::Storage;
use crate::types::Tag;
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Deserialize)]
pub struct TagResourceRequest {
    #[serde(rename = "ResourceArn", default)]
    pub resource_arn: Option<String>,
    #[serde(rename = "Tags", default)]
    pub tags: Vec<Tag>,
}

#[derive(Debug, Default, Serialize)]
pub struct TagResourceResponse {}

pub fn execute(storage: &Storage, request: TagResourceRequest) -> Result<TagResourceResponse> {
    let arn = request.resource_arn.as_deref().unwrap_or("");
    if arn.is_empty() {
        return Err(DynoxideError::ValidationException(
            "Invalid TableArn".to_string(),
        ));
    }
    let table_name = helpers::parse_table_name_from_arn(arn)?;

    // Validate tags before checking table existence (DynamoDB validates input first)
    if request.tags.is_empty() {
        return Err(DynoxideError::ValidationException(
            "Atleast one Tag needs to be provided as Input.".to_string(),
        ));
    }
    for tag in &request.tags {
        validate_tag(tag)?;
    }

    // Verify table exists
    if !storage.table_exists(table_name)? {
        return Err(DynoxideError::ResourceNotFoundException(format!(
            "Requested resource not found: ResourcArn: {arn} not found"
        )));
    }

    storage.set_tags(table_name, &request.tags)?;

    Ok(TagResourceResponse {})
}

fn validate_tag(tag: &Tag) -> Result<()> {
    if tag.key.is_empty() || tag.key.len() > 128 {
        return Err(DynoxideError::ValidationException(
            "One or more parameter values were invalid: \
             Tag key must be between 1 and 128 characters"
                .to_string(),
        ));
    }
    if tag.value.len() > 256 {
        return Err(DynoxideError::ValidationException(
            "One or more parameter values were invalid: \
             Tag value must be between 0 and 256 characters"
                .to_string(),
        ));
    }
    Ok(())
}
