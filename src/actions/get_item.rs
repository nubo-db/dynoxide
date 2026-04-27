use crate::actions::helpers;
use crate::errors::{DynoxideError, Result};
use crate::storage::Storage;
use crate::types::{self, AttributeValue, Item};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Internal deserialization struct for detecting missing fields.
#[derive(Debug, Default, Deserialize)]
struct GetItemRequestRaw {
    #[serde(rename = "TableName", default)]
    table_name: Option<String>,
    #[serde(rename = "Key", default)]
    key: Option<HashMap<String, AttributeValue>>,
    #[serde(rename = "ConsistentRead", default)]
    consistent_read: Option<bool>,
    #[serde(rename = "ProjectionExpression", default)]
    projection_expression: Option<String>,
    #[serde(rename = "ExpressionAttributeNames", default)]
    expression_attribute_names: Option<HashMap<String, String>>,
    #[serde(rename = "ReturnConsumedCapacity", default)]
    return_consumed_capacity: Option<String>,
    #[serde(rename = "AttributesToGet", default)]
    attributes_to_get: Option<Vec<String>>,
}

#[derive(Debug, Default)]
pub struct GetItemRequest {
    pub table_name: String,
    pub key: HashMap<String, AttributeValue>,
    pub consistent_read: Option<bool>,
    pub projection_expression: Option<String>,
    pub expression_attribute_names: Option<HashMap<String, String>>,
    pub return_consumed_capacity: Option<String>,
    pub attributes_to_get: Option<Vec<String>>,
}

impl<'de> serde::Deserialize<'de> for GetItemRequest {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let raw = GetItemRequestRaw::deserialize(deserializer)?;
        use crate::validation::{
            format_validation_errors, table_name_constraint_errors, TableNameContext,
        };

        let mut errors = Vec::new();

        // Key constraint (checked before tableName in DynamoDB's ordering)
        if raw.key.is_none() {
            errors.push(
                "Value null at 'key' failed to satisfy constraint: \
                 Member must not be null"
                    .to_string(),
            );
        }

        // Table name constraints
        errors.extend(table_name_constraint_errors(
            raw.table_name.as_deref(),
            TableNameContext::ReadWrite,
        ));
        let table_name = raw.table_name.unwrap_or_default();

        // ReturnConsumedCapacity enum
        if let Some(ref rcc) = raw.return_consumed_capacity {
            if !["INDEXES", "TOTAL", "NONE"].contains(&rcc.as_str()) {
                errors.push(format!(
                    "Value '{}' at 'returnConsumedCapacity' failed to satisfy constraint: \
                     Member must satisfy enum value set: [INDEXES, TOTAL, NONE]",
                    rcc
                ));
            }
        }

        // AttributesToGet length check
        if let Some(ref atg) = raw.attributes_to_get {
            if atg.is_empty() {
                errors.push(
                    "Value '[]' at 'attributesToGet' failed to satisfy constraint: \
                     Member must have length greater than or equal to 1"
                        .to_string(),
                );
            }
        }

        if let Some(msg) = format_validation_errors(&errors) {
            return Err(serde::de::Error::custom(format!("VALIDATION:{}", msg)));
        }

        Ok(GetItemRequest {
            table_name,
            key: raw.key.unwrap_or_default(),
            consistent_read: raw.consistent_read,
            projection_expression: raw.projection_expression,
            expression_attribute_names: raw.expression_attribute_names,
            return_consumed_capacity: raw.return_consumed_capacity,
            attributes_to_get: raw.attributes_to_get,
        })
    }
}

#[derive(Debug, Default, Serialize)]
pub struct GetItemResponse {
    #[serde(rename = "Item", skip_serializing_if = "Option::is_none")]
    pub item: Option<HashMap<String, AttributeValue>>,
    #[serde(rename = "ConsumedCapacity", skip_serializing_if = "Option::is_none")]
    pub consumed_capacity: Option<types::ConsumedCapacity>,
}

pub fn execute(storage: &Storage, request: GetItemRequest) -> Result<GetItemResponse> {
    // Validate table name format before checking existence (DynamoDB validates input first)
    crate::validation::validate_table_name(&request.table_name)?;

    // Validate expression/non-expression parameter conflicts
    {
        let mut non_expr = Vec::new();
        let mut expr_params = Vec::new();
        if request.attributes_to_get.is_some() {
            non_expr.push("AttributesToGet");
        }
        if request.projection_expression.is_some() {
            expr_params.push("ProjectionExpression");
        }
        let ctx = helpers::ExpressionParamContext {
            non_expression_params: non_expr,
            expression_params: expr_params,
            all_expression_param_names: vec![],
            expression_attribute_names: &request.expression_attribute_names,
            expression_attribute_values: &None,
            expression_attribute_values_raw: &None,
        };
        helpers::validate_expression_params(&ctx)?;
    }

    // Validate key attribute values (unsupported datatypes, invalid numbers)
    crate::validation::validate_key_attribute_values(&request.key)?;

    // Validate duplicate AttributesToGet
    if let Some(ref attrs) = request.attributes_to_get {
        helpers::validate_attributes_to_get_no_duplicates(attrs)?;
    }

    // Validate empty ProjectionExpression
    if let Some(ref pe) = request.projection_expression {
        if pe.is_empty() {
            return Err(DynoxideError::ValidationException(
                "Invalid ProjectionExpression: The expression can not be empty;".to_string(),
            ));
        }
    }

    let meta = helpers::require_table_for_item_op(storage, &request.table_name)?;
    let key_schema = helpers::parse_key_schema(&meta)?;

    // Validate key has exactly the right attributes
    helpers::validate_key_only(&request.key, &key_schema)?;

    // Extract key values
    // TODO: validation must precede this call -- if reaching this line, caller has already validated keys.
    let (pk, sk) = helpers::extract_key_strings(&request.key, &key_schema)?;

    // Fetch item
    let item_json = storage.get_item(&request.table_name, &pk, &sk)?;
    let item: Option<HashMap<String, AttributeValue>> =
        item_json.and_then(|json| serde_json::from_str::<Item>(&json).ok());

    // Check for unused expression attribute names/values
    let tracker = crate::expressions::TrackedExpressionAttributes::new(
        &request.expression_attribute_names,
        &None, // GetItem has no ExpressionAttributeValues
    );

    // Apply ProjectionExpression or legacy AttributesToGet
    let legacy_projection = if request.projection_expression.is_none() {
        request
            .attributes_to_get
            .as_ref()
            .map(|attrs| helpers::attributes_to_get_to_projection(attrs))
    } else {
        None
    };

    let item = if let Some(proj_expr) = &request.projection_expression {
        let parsed = crate::expressions::projection::parse(proj_expr)
            .map_err(DynoxideError::ValidationException)?;
        tracker.track_projection_expr(&parsed);

        if let Some(full_item) = item {
            // GetItem does NOT automatically include key attributes in projection.
            // Only Query/Scan do that.
            let key_attrs = Vec::new();
            let projected =
                crate::expressions::projection::apply(&full_item, &parsed, &tracker, &key_attrs)
                    .map_err(DynoxideError::ValidationException)?;
            Some(projected)
        } else {
            None
        }
    } else if let Some(ref proj) = legacy_projection {
        if let Some(full_item) = item {
            let key_attrs = Vec::new(); // AttributesToGet does not include keys automatically
            let projected =
                crate::expressions::projection::apply(&full_item, proj, &tracker, &key_attrs)
                    .map_err(DynoxideError::ValidationException)?;
            Some(projected)
        } else {
            None
        }
    } else {
        item
    };

    // Check for unused expression attribute names/values
    tracker.check_unused()?;

    let consumed_capacity = {
        let size = item.as_ref().map(types::item_size).unwrap_or(0);
        let consistent = request.consistent_read.unwrap_or(false);
        let rcu = types::read_capacity_units_with_consistency(size, consistent);
        types::consumed_capacity(&request.table_name, rcu, &request.return_consumed_capacity)
    };

    Ok(GetItemResponse {
        item,
        consumed_capacity,
    })
}
