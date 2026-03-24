use crate::actions::helpers;
use crate::errors::{DynoxideError, Result};
use crate::storage::Storage;
use crate::types::Tag;
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Deserialize)]
pub struct ListTagsOfResourceRequest {
    #[serde(rename = "ResourceArn", default)]
    pub resource_arn: Option<String>,
}

#[derive(Debug, Default, Serialize)]
pub struct ListTagsOfResourceResponse {
    #[serde(rename = "Tags")]
    pub tags: Vec<Tag>,
}

pub fn execute(
    storage: &Storage,
    request: ListTagsOfResourceRequest,
) -> Result<ListTagsOfResourceResponse> {
    let arn = request.resource_arn.as_deref().unwrap_or("");
    if arn.is_empty() {
        return Err(DynoxideError::ValidationException(
            "Invalid TableArn".to_string(),
        ));
    }
    let table_name = helpers::parse_table_name_from_arn(arn)?;

    // Verify table exists — real DynamoDB returns AccessDeniedException for non-existent ARNs
    if !storage.table_exists(table_name)? {
        return Err(DynoxideError::AccessDeniedException(format!(
            "User: arn:aws:iam::000000000000:root is not authorized to perform: dynamodb:ListTagsOfResource on resource: {arn}"
        )));
    }

    let tags = storage.get_tags(table_name)?;

    Ok(ListTagsOfResourceResponse { tags })
}
