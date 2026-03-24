use crate::actions::{TableDescription, build_table_description};
use crate::errors::{DynoxideError, Result};
use crate::storage::Storage;
use crate::streams;
use crate::types::{
    AttributeDefinition, GlobalSecondaryIndex, KeySchemaElement, KeyType, LocalSecondaryIndex,
    Projection, ProjectionType, ProvisionedThroughput,
};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

/// Internal raw deserialization struct — uses serde_json::Value for fields
/// that participate in DynamoDB's multi-field constraint validation.
#[derive(Debug, Default, Deserialize)]
struct RawRequest {
    #[serde(rename = "TableName", default)]
    table_name: Option<String>,
    #[serde(rename = "KeySchema", default)]
    key_schema: Option<serde_json::Value>,
    #[serde(rename = "AttributeDefinitions", default)]
    attribute_definitions: Option<serde_json::Value>,
    #[serde(rename = "GlobalSecondaryIndexes", default)]
    global_secondary_indexes: Option<serde_json::Value>,
    #[serde(rename = "LocalSecondaryIndexes", default)]
    local_secondary_indexes: Option<serde_json::Value>,
    #[serde(rename = "BillingMode", default)]
    billing_mode: Option<String>,
    #[serde(rename = "ProvisionedThroughput", default)]
    provisioned_throughput: Option<serde_json::Value>,
    #[serde(rename = "StreamSpecification", default)]
    stream_specification: Option<StreamSpecification>,
    #[serde(rename = "SSESpecification", default)]
    sse_specification: Option<crate::types::SseSpecification>,
    #[serde(rename = "TableClass", default)]
    table_class: Option<String>,
    #[serde(rename = "Tags", default)]
    tags: Option<Vec<crate::types::Tag>>,
    #[serde(rename = "DeletionProtectionEnabled", default)]
    deletion_protection_enabled: Option<bool>,
}

/// Public request type — fully validated, typed fields.
/// Can be constructed directly (programmatic use) or deserialized from JSON.
#[derive(Debug, Default)]
pub struct CreateTableRequest {
    pub table_name: String,
    pub key_schema: Vec<KeySchemaElement>,
    pub attribute_definitions: Vec<AttributeDefinition>,
    pub global_secondary_indexes: Option<Vec<GlobalSecondaryIndex>>,
    pub local_secondary_indexes: Option<Vec<LocalSecondaryIndex>>,
    pub billing_mode: Option<String>,
    pub provisioned_throughput: Option<ProvisionedThroughput>,
    pub stream_specification: Option<StreamSpecification>,
    pub sse_specification: Option<crate::types::SseSpecification>,
    pub table_class: Option<String>,
    pub tags: Option<Vec<crate::types::Tag>>,
    pub deletion_protection_enabled: Option<bool>,
}

/// Custom Deserialize that does loose JSON parsing first, validates, then builds typed fields.
/// Validation errors use "VALIDATION:" prefix so server.rs converts them to ValidationException.
impl<'de> serde::Deserialize<'de> for CreateTableRequest {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let raw = RawRequest::deserialize(deserializer)?;
        match validate_raw_and_build(raw) {
            Ok(req) => Ok(req),
            Err(msg) => Err(serde::de::Error::custom(format!("VALIDATION:{}", msg))),
        }
    }
}

#[derive(Debug, Default, Deserialize)]
pub struct StreamSpecification {
    #[serde(rename = "StreamEnabled", alias = "stream_enabled")]
    pub stream_enabled: bool,
    #[serde(rename = "StreamViewType", alias = "stream_view_type", default)]
    pub stream_view_type: Option<String>,
}

#[derive(Debug, Default, Serialize)]
pub struct CreateTableResponse {
    #[serde(rename = "TableDescription")]
    pub table_description: TableDescription,
}

pub fn execute(storage: &Storage, request: CreateTableRequest) -> Result<CreateTableResponse> {
    // Structural validation (runs for both programmatic and JSON paths)
    validate_typed_request(&request)?;

    if let Some(ref tc) = request.table_class {
        if tc != "STANDARD" && tc != "STANDARD_INFREQUENT_ACCESS" {
            return Err(DynoxideError::ValidationException(format!(
                "1 validation error detected: Value '{tc}' at 'tableClass' failed to satisfy \
                 constraint: Member must satisfy enum value set: \
                 [STANDARD, STANDARD_INFREQUENT_ACCESS]"
            )));
        }
    }

    if storage.table_exists(&request.table_name)? {
        return Err(DynoxideError::ResourceInUseException(format!(
            "Table already exists: {}",
            request.table_name
        )));
    }

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    let key_schema_json = serde_json::to_string(&request.key_schema)
        .map_err(|e| DynoxideError::InternalServerError(e.to_string()))?;
    let attr_defs_json = serde_json::to_string(&request.attribute_definitions)
        .map_err(|e| DynoxideError::InternalServerError(e.to_string()))?;
    let gsi_json = request
        .global_secondary_indexes
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(|e| DynoxideError::InternalServerError(e.to_string()))?;
    let lsi_json = request
        .local_secondary_indexes
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(|e| DynoxideError::InternalServerError(e.to_string()))?;
    let pt_json = request
        .provisioned_throughput
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(|e| DynoxideError::InternalServerError(e.to_string()))?;
    let sse_json = request
        .sse_specification
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(|e| DynoxideError::InternalServerError(e.to_string()))?;
    let deletion_protection = request.deletion_protection_enabled.unwrap_or(false);

    let billing_mode_str = request.billing_mode.as_deref().unwrap_or("PROVISIONED");
    storage.insert_table_metadata(&crate::storage::CreateTableMetadata {
        table_name: &request.table_name,
        key_schema: &key_schema_json,
        attribute_definitions: &attr_defs_json,
        gsi_definitions: gsi_json.as_deref(),
        lsi_definitions: lsi_json.as_deref(),
        provisioned_throughput: pt_json.as_deref(),
        created_at: now,
        sse_specification: sse_json.as_deref(),
        table_class: request.table_class.as_deref(),
        deletion_protection_enabled: deletion_protection,
        billing_mode: Some(billing_mode_str),
    })?;

    storage.create_data_table(&request.table_name)?;

    if let Some(ref gsis) = request.global_secondary_indexes {
        for gsi in gsis {
            storage.create_gsi_table(&request.table_name, &gsi.index_name)?;
        }
    }

    if let Some(ref lsis) = request.local_secondary_indexes {
        for lsi in lsis {
            storage.create_lsi_table(&request.table_name, &lsi.index_name)?;
        }
    }

    if let Some(ref spec) = request.stream_specification {
        if spec.stream_enabled {
            let view_type = spec
                .stream_view_type
                .as_deref()
                .unwrap_or("NEW_AND_OLD_IMAGES");
            let label = streams::generate_stream_label();
            storage.enable_stream(&request.table_name, view_type, &label)?;
        }
    }

    if let Some(ref tags) = request.tags {
        if !tags.is_empty() {
            storage.set_tags(&request.table_name, tags)?;
        }
    }

    let meta = storage
        .get_table_metadata(&request.table_name)?
        .ok_or_else(|| {
            DynoxideError::InternalServerError("Table metadata not found after creation".into())
        })?;

    let mut desc = build_table_description(&meta, Some(0), Some(0));
    // CreateTable response shows CREATING status (table is usable immediately
    // but DynamoDB API contract says newly-created tables start as CREATING)
    desc.table_status = "CREATING".to_string();

    // Override billing mode fields based on the actual request
    let billing_mode_str = request.billing_mode.as_deref().unwrap_or("PROVISIONED");
    if billing_mode_str == "PROVISIONED" {
        desc.billing_mode_summary = None;
        desc.table_throughput_mode_summary = None;
    } else if billing_mode_str == "PAY_PER_REQUEST" {
        desc.billing_mode_summary = Some(crate::actions::BillingModeSummary {
            billing_mode: "PAY_PER_REQUEST".to_string(),
            last_update_to_pay_per_request_date_time: None,
        });
        desc.table_throughput_mode_summary = Some(crate::actions::TableThroughputModeSummary {
            table_throughput_mode: "PAY_PER_REQUEST".to_string(),
            last_update_to_pay_per_request_date_time: None,
        });
        // Ensure provisioned throughput shows zeros for PAY_PER_REQUEST
        desc.provisioned_throughput = Some(crate::actions::TableProvisionedThroughputDescription {
            read_capacity_units: 0,
            write_capacity_units: 0,
            number_of_decreases_today: 0,
            last_increase_date_time: None,
            last_decrease_date_time: None,
        });
    }

    // Set all GSI statuses to CREATING for newly created tables
    if let Some(ref mut gsis) = desc.global_secondary_indexes {
        for gsi in gsis {
            gsi.index_status = "CREATING".to_string();
        }
    }

    // Remove DeletionProtectionEnabled from response if not explicitly set
    // (DynamoDB doesn't include it in basic CreateTable response)
    if request.deletion_protection_enabled.is_none() {
        desc.deletion_protection_enabled = None;
    }

    Ok(CreateTableResponse {
        table_description: desc,
    })
}

/// Convert a String error to DynoxideError::ValidationException.
fn ve(msg: String) -> DynoxideError {
    DynoxideError::ValidationException(msg)
}

/// Validate a programmatically-constructed request (used when not deserialised from JSON).
///
/// The validation order matches DynamoDB's actual behaviour (as verified by the Dynalite
/// conformance suite):
///
/// 1. Table name (missing, length, pattern)
/// 2. BillingMode + ProvisionedThroughput consistency
/// 3. ProvisionedThroughput out-of-bounds
/// 4. Missing ProvisionedThroughput (default PROVISIONED billing)
/// 5. Key attribute definition checks ("Invalid KeySchema" / detailed missing-attr message)
/// 6. Key schema structure (duplicate names, wrong types)
/// 7. Empty LSI/GSI lists
/// 8. LSI/GSI structural validation (key schema, projections, duplicates, limits)
/// 9. Cross-index duplicate names
/// 10. Attribute definition count mismatch
fn validate_typed_request(request: &CreateTableRequest) -> Result<()> {
    if request.table_name.is_empty() {
        return Err(DynoxideError::ValidationException(
            "The parameter 'TableName' is required but was not present in the request".to_string(),
        ));
    }
    if request.table_name.len() < 3 || request.table_name.len() > 255 {
        return Err(DynoxideError::ValidationException(
            "TableName must be at least 3 characters long and at most 255 characters long"
                .to_string(),
        ));
    }

    // Table name pattern
    if !request
        .table_name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
    {
        return Err(DynoxideError::ValidationException(format!(
            "1 validation error detected: Value '{}' at 'tableName' failed to satisfy constraint: \
             Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+",
            request.table_name
        )));
    }

    // BillingMode + ProvisionedThroughput consistency
    let billing_mode_str = request.billing_mode.as_deref().unwrap_or("PROVISIONED");
    if billing_mode_str == "PAY_PER_REQUEST" && request.provisioned_throughput.is_some() {
        return Err(DynoxideError::ValidationException(
            "One or more parameter values were invalid: Neither ReadCapacityUnits nor \
             WriteCapacityUnits can be specified when BillingMode is PAY_PER_REQUEST"
                .to_string(),
        ));
    }

    // ProvisionedThroughput out-of-bounds
    if let Some(ref pt) = request.provisioned_throughput {
        const MAX_THROUGHPUT: i64 = 1_000_000_000_000;
        let rcu = pt.read_capacity_units.unwrap_or(0);
        let wcu = pt.write_capacity_units.unwrap_or(0);
        if rcu > MAX_THROUGHPUT {
            return Err(DynoxideError::ValidationException(format!(
                "Given value {} for ReadCapacityUnits is out of bounds",
                rcu
            )));
        }
        if wcu > MAX_THROUGHPUT {
            return Err(DynoxideError::ValidationException(format!(
                "Given value {} for WriteCapacityUnits is out of bounds",
                wcu
            )));
        }
    }

    // Missing ProvisionedThroughput when billing mode is explicitly PROVISIONED.
    // For the programmatic API, when BillingMode is not specified we default to
    // PAY_PER_REQUEST for convenience. The HTTP/JSON path (validate_raw_and_build)
    // applies the stricter DynamoDB default of PROVISIONED.
    if request.billing_mode.is_some()
        && billing_mode_str == "PROVISIONED"
        && request.provisioned_throughput.is_none()
    {
        return Err(DynoxideError::ValidationException(
            "One or more parameter values were invalid: ReadCapacityUnits and \
             WriteCapacityUnits must both be specified when BillingMode is PROVISIONED"
                .to_string(),
        ));
    }

    // Key attribute definition checks (before key schema structure)
    validate_key_attrs_in_defs(&request.key_schema, &request.attribute_definitions).map_err(ve)?;

    // Key schema structure
    validate_key_schema_structure(&request.key_schema).map_err(ve)?;

    // Empty LSI/GSI lists (before structural validation and attr count)
    if let Some(ref lsis) = request.local_secondary_indexes {
        if lsis.is_empty() {
            return Err(ve(
                "One or more parameter values were invalid: List of LocalSecondaryIndexes is empty"
                    .to_string(),
            ));
        }
    }
    if let Some(ref gsis) = request.global_secondary_indexes {
        if gsis.is_empty() {
            return Err(ve(
                "One or more parameter values were invalid: List of GlobalSecondaryIndexes is empty"
                    .to_string(),
            ));
        }
    }

    // LSI structural validation
    if let Some(ref lsis) = request.local_secondary_indexes {
        validate_lsi_list(lsis, &request.key_schema, &request.attribute_definitions).map_err(ve)?;
    }

    // GSI structural validation
    if let Some(ref gsis) = request.global_secondary_indexes {
        let bm = request.billing_mode.as_deref().unwrap_or("PROVISIONED");
        validate_gsi_list(gsis, &request.attribute_definitions, bm).map_err(ve)?;
    }

    // Cross-index duplicate names (checked before attr def count)
    check_cross_index_duplicates(
        &request.local_secondary_indexes,
        &request.global_secondary_indexes,
    )
    .map_err(ve)?;

    // Attribute definition count (last)
    validate_attr_def_count(
        &request.key_schema,
        &request.attribute_definitions,
        &request.local_secondary_indexes,
        &request.global_secondary_indexes,
    )
    .map_err(ve)?;

    Ok(())
}

fn check_cross_index_duplicates(
    lsis: &Option<Vec<LocalSecondaryIndex>>,
    gsis: &Option<Vec<GlobalSecondaryIndex>>,
) -> std::result::Result<(), String> {
    if let (Some(lsis), Some(gsis)) = (lsis, gsis) {
        let mut all_names = std::collections::HashSet::new();
        for lsi in lsis {
            all_names.insert(&lsi.index_name);
        }
        for gsi in gsis {
            if !all_names.insert(&gsi.index_name) {
                return Err(format!(
                    "One or more parameter values were invalid: Duplicate index name: {}",
                    gsi.index_name
                ));
            }
        }
    }
    Ok(())
}

// ---- Raw JSON validation (for deserialization path) ----

fn validate_raw_and_build(raw: RawRequest) -> std::result::Result<CreateTableRequest, String> {
    // Missing TableName is a different error format from invalid TableName
    if raw.table_name.is_none() {
        return Err(
            "The parameter 'TableName' is required but was not present in the request".to_string(),
        );
    }

    // Use the shared constraint error collector for table name validation.
    // This produces the correct multi-field constraint format for empty,
    // too-short, too-long, or invalid-pattern table names.
    let name_errors = crate::validation::table_name_constraint_errors(raw.table_name.as_deref());
    if !name_errors.is_empty() {
        let msg = format!(
            "{} validation error{} detected: {}",
            name_errors.len(),
            if name_errors.len() > 1 { "s" } else { "" },
            name_errors.join("; ")
        );
        return Err(msg);
    }
    let table_name = raw.table_name.unwrap();

    let mut errors = Vec::new();

    if let Some(ref bm) = raw.billing_mode {
        if bm != "PROVISIONED" && bm != "PAY_PER_REQUEST" {
            errors.push(format!(
                "Value '{}' at 'billingMode' failed to satisfy constraint: \
                 Member must satisfy enum value set: [PROVISIONED, PAY_PER_REQUEST]",
                bm
            ));
        }
    }

    collect_pt_errors(&raw.provisioned_throughput, &mut errors);
    collect_ks_errors(&raw.key_schema, &mut errors);
    collect_ad_errors(&raw.attribute_definitions, &mut errors);
    collect_lsi_errors(&raw.local_secondary_indexes, &mut errors);
    collect_gsi_errors(&raw.global_secondary_indexes, &mut errors);

    // DynamoDB caps multi-field constraint errors at 10
    errors.truncate(10);

    if !errors.is_empty() {
        let prefix = format!(
            "{} validation error{} detected: ",
            errors.len(),
            if errors.len() == 1 { "" } else { "s" }
        );
        return Err(format!("{}{}", prefix, errors.join("; ")));
    }

    // BillingMode + ProvisionedThroughput consistency (HTTP path only)
    let billing_mode_str = raw.billing_mode.as_deref().unwrap_or("PROVISIONED");
    if billing_mode_str == "PAY_PER_REQUEST" && raw.provisioned_throughput.is_some() {
        return Err(
            "One or more parameter values were invalid: Neither ReadCapacityUnits nor \
             WriteCapacityUnits can be specified when BillingMode is PAY_PER_REQUEST"
                .to_string(),
        );
    }

    // ProvisionedThroughput out-of-bounds (after multi-field but before struct checks)
    if let Some(ref pt) = raw.provisioned_throughput {
        if let Some(obj) = pt.as_object() {
            let rcu = obj
                .get("ReadCapacityUnits")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            let wcu = obj
                .get("WriteCapacityUnits")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            const MAX_THROUGHPUT: i64 = 1_000_000_000_000;
            if rcu > MAX_THROUGHPUT {
                return Err(format!(
                    "Given value {} for ReadCapacityUnits is out of bounds",
                    rcu
                ));
            }
            if wcu > MAX_THROUGHPUT {
                return Err(format!(
                    "Given value {} for WriteCapacityUnits is out of bounds",
                    wcu
                ));
            }
        }
    }

    // Missing ProvisionedThroughput when BillingMode is explicitly PROVISIONED.
    if raw.billing_mode.as_deref() == Some("PROVISIONED") && raw.provisioned_throughput.is_none() {
        return Err(
            "One or more parameter values were invalid: ReadCapacityUnits and \
             WriteCapacityUnits must both be specified when BillingMode is PROVISIONED"
                .to_string(),
        );
    }

    // Parse typed structures
    let key_schema: Vec<KeySchemaElement> = raw
        .key_schema
        .as_ref()
        .map(|v| serde_json::from_value(v.clone()))
        .transpose()
        .map_err(|e| e.to_string())?
        .unwrap_or_default();
    let attribute_definitions: Vec<AttributeDefinition> = raw
        .attribute_definitions
        .as_ref()
        .map(|v| serde_json::from_value(v.clone()))
        .transpose()
        .map_err(|e| e.to_string())?
        .unwrap_or_default();
    let provisioned_throughput: Option<ProvisionedThroughput> = raw
        .provisioned_throughput
        .as_ref()
        .map(|v| serde_json::from_value(v.clone()))
        .transpose()
        .map_err(|e| e.to_string())?;
    let global_secondary_indexes: Option<Vec<GlobalSecondaryIndex>> = raw
        .global_secondary_indexes
        .as_ref()
        .map(|v| serde_json::from_value(v.clone()))
        .transpose()
        .map_err(|e| e.to_string())?;
    let local_secondary_indexes: Option<Vec<LocalSecondaryIndex>> = raw
        .local_secondary_indexes
        .as_ref()
        .map(|v| serde_json::from_value(v.clone()))
        .transpose()
        .map_err(|e| e.to_string())?;

    Ok(CreateTableRequest {
        table_name,
        key_schema,
        attribute_definitions,
        global_secondary_indexes,
        local_secondary_indexes,
        billing_mode: raw.billing_mode,
        provisioned_throughput,
        stream_specification: raw.stream_specification,
        sse_specification: raw.sse_specification,
        table_class: raw.table_class,
        tags: raw.tags,
        deletion_protection_enabled: raw.deletion_protection_enabled,
    })
}

// ---- Multi-field constraint error collectors ----

fn collect_pt_errors(pt_val: &Option<serde_json::Value>, errors: &mut Vec<String>) {
    if let Some(v) = pt_val {
        if let Some(obj) = v.as_object() {
            let wcu = obj.get("WriteCapacityUnits");
            let rcu = obj.get("ReadCapacityUnits");
            if wcu.is_none() || wcu == Some(&serde_json::Value::Null) {
                errors.push("Value null at 'provisionedThroughput.writeCapacityUnits' failed to satisfy constraint: Member must not be null".to_string());
            } else if let Some(w) = wcu.and_then(|v| v.as_i64()) {
                if w < 1 {
                    errors.push(format!("Value '{}' at 'provisionedThroughput.writeCapacityUnits' failed to satisfy constraint: Member must have value greater than or equal to 1", w));
                }
            }
            if rcu.is_none() || rcu == Some(&serde_json::Value::Null) {
                errors.push("Value null at 'provisionedThroughput.readCapacityUnits' failed to satisfy constraint: Member must not be null".to_string());
            } else if let Some(r) = rcu.and_then(|v| v.as_i64()) {
                if r < 1 {
                    errors.push(format!("Value '{}' at 'provisionedThroughput.readCapacityUnits' failed to satisfy constraint: Member must have value greater than or equal to 1", r));
                }
            }
        }
    }
}

fn collect_ks_errors(ks_val: &Option<serde_json::Value>, errors: &mut Vec<String>) {
    match ks_val {
        None => {
            errors.push(
                "Value null at 'keySchema' failed to satisfy constraint: Member must not be null"
                    .to_string(),
            );
        }
        Some(v) => {
            if let Some(arr) = v.as_array() {
                if arr.is_empty() {
                    errors.push("Value '[]' at 'keySchema' failed to satisfy constraint: Member must have length greater than or equal to 1".to_string());
                } else if arr.len() > 2 {
                    errors.push(format!("Value '{}' at 'keySchema' failed to satisfy constraint: Member must have length less than or equal to 2", v));
                }
                for (i, elem) in arr.iter().enumerate().take(10) {
                    collect_ks_elem_errors(elem, i + 1, errors);
                }
            }
        }
    }
}

fn collect_ks_elem_errors(elem: &serde_json::Value, idx: usize, errors: &mut Vec<String>) {
    if let Some(obj) = elem.as_object() {
        if !obj.contains_key("AttributeName")
            || obj.get("AttributeName") == Some(&serde_json::Value::Null)
        {
            errors.push(format!("Value null at 'keySchema.{}.member.attributeName' failed to satisfy constraint: Member must not be null", idx));
        }
        let kt = obj.get("KeyType");
        if kt.is_none() || kt == Some(&serde_json::Value::Null) {
            errors.push(format!("Value null at 'keySchema.{}.member.keyType' failed to satisfy constraint: Member must not be null", idx));
        } else if let Some(s) = kt.and_then(|v| v.as_str()) {
            if s != "HASH" && s != "RANGE" {
                errors.push(format!("Value '{}' at 'keySchema.{}.member.keyType' failed to satisfy constraint: Member must satisfy enum value set: [HASH, RANGE]", s, idx));
            }
        }
    }
}

fn collect_ad_errors(ad_val: &Option<serde_json::Value>, errors: &mut Vec<String>) {
    match ad_val {
        None => {
            errors.push("Value null at 'attributeDefinitions' failed to satisfy constraint: Member must not be null".to_string());
        }
        Some(v) => {
            if let Some(arr) = v.as_array() {
                for (i, elem) in arr.iter().enumerate() {
                    if let Some(obj) = elem.as_object() {
                        if !obj.contains_key("AttributeName")
                            || obj.get("AttributeName") == Some(&serde_json::Value::Null)
                        {
                            errors.push(format!("Value null at 'attributeDefinitions.{}.member.attributeName' failed to satisfy constraint: Member must not be null", i + 1));
                        }
                        let at = obj.get("AttributeType");
                        if at.is_none() || at == Some(&serde_json::Value::Null) {
                            errors.push(format!("Value null at 'attributeDefinitions.{}.member.attributeType' failed to satisfy constraint: Member must not be null", i + 1));
                        } else if let Some(s) = at.and_then(|v| v.as_str()) {
                            if s != "S" && s != "N" && s != "B" {
                                errors.push(format!("Value '{}' at 'attributeDefinitions.{}.member.attributeType' failed to satisfy constraint: Member must satisfy enum value set: [B, N, S]", s, i + 1));
                            }
                        }
                    }
                }
            }
        }
    }
}

fn collect_lsi_errors(lsi_val: &Option<serde_json::Value>, errors: &mut Vec<String>) {
    if let Some(v) = lsi_val {
        if let Some(arr) = v.as_array() {
            for (i, elem) in arr.iter().enumerate().take(10) {
                if let Some(obj) = elem.as_object() {
                    // Order: indexName, keySchema, projection
                    if !obj.contains_key("IndexName")
                        || obj.get("IndexName") == Some(&serde_json::Value::Null)
                    {
                        errors.push(format!("Value null at 'localSecondaryIndexes.{}.member.indexName' failed to satisfy constraint: Member must not be null", i + 1));
                    } else if let Some(name) = obj.get("IndexName").and_then(|v| v.as_str()) {
                        collect_idx_name_errors(name, "localSecondaryIndexes", i + 1, errors);
                    }
                    if !obj.contains_key("KeySchema")
                        || obj.get("KeySchema") == Some(&serde_json::Value::Null)
                    {
                        errors.push(format!("Value null at 'localSecondaryIndexes.{}.member.keySchema' failed to satisfy constraint: Member must not be null", i + 1));
                    } else if let Some(ks) = obj.get("KeySchema").and_then(|v| v.as_array()) {
                        if ks.is_empty() {
                            errors.push(format!("Value '[]' at 'localSecondaryIndexes.{}.member.keySchema' failed to satisfy constraint: Member must have length greater than or equal to 1", i + 1));
                        }
                    }
                    if !obj.contains_key("Projection")
                        || obj.get("Projection") == Some(&serde_json::Value::Null)
                    {
                        errors.push(format!("Value null at 'localSecondaryIndexes.{}.member.projection' failed to satisfy constraint: Member must not be null", i + 1));
                    } else if let Some(p) = obj.get("Projection").and_then(|v| v.as_object()) {
                        collect_proj_errors(p, &format!("localSecondaryIndexes.{}", i + 1), errors);
                    }
                }
            }
        }
    }
}

fn collect_gsi_errors(gsi_val: &Option<serde_json::Value>, errors: &mut Vec<String>) {
    if let Some(v) = gsi_val {
        if let Some(arr) = v.as_array() {
            for (i, elem) in arr.iter().enumerate().take(10) {
                if let Some(obj) = elem.as_object() {
                    // Order for GSI: keySchema, projection, indexName
                    if !obj.contains_key("KeySchema")
                        || obj.get("KeySchema") == Some(&serde_json::Value::Null)
                    {
                        errors.push(format!("Value null at 'globalSecondaryIndexes.{}.member.keySchema' failed to satisfy constraint: Member must not be null", i + 1));
                    } else if let Some(ks) = obj.get("KeySchema").and_then(|v| v.as_array()) {
                        if ks.is_empty() {
                            errors.push(format!("Value '[]' at 'globalSecondaryIndexes.{}.member.keySchema' failed to satisfy constraint: Member must have length greater than or equal to 1", i + 1));
                        }
                    }
                    if !obj.contains_key("Projection")
                        || obj.get("Projection") == Some(&serde_json::Value::Null)
                    {
                        errors.push(format!("Value null at 'globalSecondaryIndexes.{}.member.projection' failed to satisfy constraint: Member must not be null", i + 1));
                    } else if let Some(p) = obj.get("Projection").and_then(|v| v.as_object()) {
                        collect_proj_errors(
                            p,
                            &format!("globalSecondaryIndexes.{}", i + 1),
                            errors,
                        );
                    }
                    if !obj.contains_key("IndexName")
                        || obj.get("IndexName") == Some(&serde_json::Value::Null)
                    {
                        errors.push(format!("Value null at 'globalSecondaryIndexes.{}.member.indexName' failed to satisfy constraint: Member must not be null", i + 1));
                    } else if let Some(name) = obj.get("IndexName").and_then(|v| v.as_str()) {
                        collect_idx_name_errors(name, "globalSecondaryIndexes", i + 1, errors);
                    }
                    // GSI ProvisionedThroughput
                    if let Some(pt) = obj.get("ProvisionedThroughput").and_then(|v| v.as_object()) {
                        let wcu = pt.get("WriteCapacityUnits");
                        let rcu = pt.get("ReadCapacityUnits");
                        if let Some(w) = wcu.and_then(|v| v.as_i64()) {
                            if w < 1 {
                                errors.push(format!("Value '{}' at 'globalSecondaryIndexes.{}.member.provisionedThroughput.writeCapacityUnits' failed to satisfy constraint: Member must have value greater than or equal to 1", w, i + 1));
                            }
                        } else if wcu.is_none() || wcu == Some(&serde_json::Value::Null) {
                            errors.push(format!("Value null at 'globalSecondaryIndexes.{}.member.provisionedThroughput.writeCapacityUnits' failed to satisfy constraint: Member must not be null", i + 1));
                        }
                        if let Some(r) = rcu.and_then(|v| v.as_i64()) {
                            if r < 1 {
                                errors.push(format!("Value '{}' at 'globalSecondaryIndexes.{}.member.provisionedThroughput.readCapacityUnits' failed to satisfy constraint: Member must have value greater than or equal to 1", r, i + 1));
                            }
                        } else if rcu.is_none() || rcu == Some(&serde_json::Value::Null) {
                            errors.push(format!("Value null at 'globalSecondaryIndexes.{}.member.provisionedThroughput.readCapacityUnits' failed to satisfy constraint: Member must not be null", i + 1));
                        }
                    }
                }
            }
        }
    }
}

fn collect_idx_name_errors(name: &str, prefix: &str, idx: usize, errors: &mut Vec<String>) {
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
    {
        errors.push(format!("Value '{}' at '{}.{}.member.indexName' failed to satisfy constraint: Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+", name, prefix, idx));
    }
    if name.len() < 3 {
        errors.push(format!("Value '{}' at '{}.{}.member.indexName' failed to satisfy constraint: Member must have length greater than or equal to 3", name, prefix, idx));
    }
    if name.len() > 255 {
        errors.push(format!("Value '{}' at '{}.{}.member.indexName' failed to satisfy constraint: Member must have length less than or equal to 255", name, prefix, idx));
    }
}

fn collect_proj_errors(
    proj: &serde_json::Map<String, serde_json::Value>,
    prefix: &str,
    errors: &mut Vec<String>,
) {
    if let Some(pt) = proj.get("ProjectionType") {
        if let Some(s) = pt.as_str() {
            if s != "ALL" && s != "KEYS_ONLY" && s != "INCLUDE" {
                errors.push(format!("Value '{}' at '{}.member.projection.projectionType' failed to satisfy constraint: Member must satisfy enum value set: [ALL, INCLUDE, KEYS_ONLY]", s, prefix));
            }
        }
    }
    if let Some(nka) = proj.get("NonKeyAttributes") {
        if let Some(arr) = nka.as_array() {
            if arr.is_empty() {
                errors.push(format!("Value '[]' at '{}.member.projection.nonKeyAttributes' failed to satisfy constraint: Member must have length greater than or equal to 1", prefix));
            }
        }
    }
}

// ---- Structural validation helpers ----

fn validate_key_schema_structure(ks: &[KeySchemaElement]) -> std::result::Result<(), String> {
    if ks.is_empty() {
        return Err("1 validation error detected: Value null at 'keySchema' failed to satisfy constraint: Member must have length less than or equal to 2".to_string());
    }
    if ks[0].key_type != KeyType::HASH {
        return Err(
            "Invalid KeySchema: The first KeySchemaElement is not a HASH key type".to_string(),
        );
    }
    if ks.len() == 2 && ks[0].attribute_name == ks[1].attribute_name {
        return Err(
            "Both the Hash Key and the Range Key element in the KeySchema have the same name"
                .to_string(),
        );
    }
    if ks.len() == 2 && ks[1].key_type != KeyType::RANGE {
        return Err(
            "Invalid KeySchema: The second KeySchemaElement is not a RANGE key type".to_string(),
        );
    }
    Ok(())
}

fn validate_key_attrs_in_defs(
    ks: &[KeySchemaElement],
    defs: &[AttributeDefinition],
) -> std::result::Result<(), String> {
    // Collect missing key attribute names
    let missing: Vec<&str> = ks
        .iter()
        .filter(|k| !defs.iter().any(|d| d.attribute_name == k.attribute_name))
        .map(|k| k.attribute_name.as_str())
        .collect();

    if missing.is_empty() {
        // Even if no keys are missing, check for structural issues (dup names/types)
        // which DynamoDB reports as generic "no definition" when defs exist
        let has_dup_names = ks.len() == 2 && ks[0].attribute_name == ks[1].attribute_name;
        if has_dup_names {
            return Err(
                "Invalid KeySchema: Some index key attribute have no definition".to_string(),
            );
        }
        return Ok(());
    }

    // Use generic message when:
    // - defs is empty (fewer defs than unique key attrs)
    // - key schema has 2 elements (regardless of structural validity)
    // - key schema has structural issues (dup names, dup types)
    // Use detailed message only when defs is non-empty AND key schema has 1 element
    let has_dup_names = ks.len() == 2 && ks[0].attribute_name == ks[1].attribute_name;
    let has_dup_types = ks.len() == 2 && ks[0].key_type == ks[1].key_type;
    let use_generic = defs.is_empty() || ks.len() >= 2 || has_dup_names || has_dup_types;

    if use_generic {
        return Err("Invalid KeySchema: Some index key attribute have no definition".to_string());
    }

    // Detailed message for single-key schema with non-empty defs
    let key_names: Vec<&str> = missing.to_vec();
    let def_names: Vec<&str> = defs.iter().map(|d| d.attribute_name.as_str()).collect();
    Err(format!(
        "One or more parameter values were invalid: Some index key attributes are not defined in \
         AttributeDefinitions. Keys: [{}], AttributeDefinitions: [{}]",
        key_names.join(", "),
        def_names.join(", ")
    ))
}

fn validate_attr_def_count(
    ks: &[KeySchemaElement],
    defs: &[AttributeDefinition],
    lsis: &Option<Vec<LocalSecondaryIndex>>,
    gsis: &Option<Vec<GlobalSecondaryIndex>>,
) -> std::result::Result<(), String> {
    let mut all_key_attrs = std::collections::HashSet::new();
    for k in ks {
        all_key_attrs.insert(k.attribute_name.as_str());
    }
    if let Some(lsis) = lsis {
        for lsi in lsis {
            for k in &lsi.key_schema {
                all_key_attrs.insert(k.attribute_name.as_str());
            }
        }
    }
    if let Some(gsis) = gsis {
        for gsi in gsis {
            for k in &gsi.key_schema {
                all_key_attrs.insert(k.attribute_name.as_str());
            }
        }
    }
    if defs.len() != all_key_attrs.len() {
        return Err("One or more parameter values were invalid: Number of attributes in KeySchema does not exactly match number of attributes defined in AttributeDefinitions".to_string());
    }
    Ok(())
}

fn validate_lsi_list(
    lsis: &[LocalSecondaryIndex],
    ks: &[KeySchemaElement],
    defs: &[AttributeDefinition],
) -> std::result::Result<(), String> {
    // Empty check is done earlier in validate_typed_request

    if !ks.iter().any(|k| k.key_type == KeyType::RANGE) {
        return Err("One or more parameter values were invalid: Table KeySchema does not have a range key, which is required when specifying a LocalSecondaryIndex".to_string());
    }

    // Check missing attribute definitions across all LSI keys
    let def_names: Vec<&str> = defs.iter().map(|d| d.attribute_name.as_str()).collect();
    let mut missing_keys = Vec::new();
    for lsi in lsis {
        for k in &lsi.key_schema {
            if !def_names.contains(&k.attribute_name.as_str())
                && !missing_keys.contains(&k.attribute_name.as_str())
            {
                missing_keys.push(k.attribute_name.as_str());
            }
        }
    }
    if !missing_keys.is_empty() {
        let mut all_keys = Vec::new();
        for lsi in lsis {
            for k in &lsi.key_schema {
                if !all_keys.contains(&k.attribute_name.as_str()) {
                    all_keys.push(k.attribute_name.as_str());
                }
            }
        }
        return Err(format!(
            "One or more parameter values were invalid: Some index key attributes are not defined in AttributeDefinitions. Keys: [{}], AttributeDefinitions: [{}]",
            all_keys.join(", "),
            def_names.join(", ")
        ));
    }

    // Structural validation for each LSI
    for lsi in lsis {
        validate_lsi_structure(lsi, ks)?;
    }

    // Duplicate index names
    let mut seen = std::collections::HashSet::new();
    for lsi in lsis {
        if !seen.insert(&lsi.index_name) {
            return Err(format!(
                "One or more parameter values were invalid: Duplicate index name: {}",
                lsi.index_name
            ));
        }
    }

    // Count limit
    if lsis.len() > 5 {
        return Err("One or more parameter values were invalid: Number of LocalSecondaryIndexes exceeds per-table limit of 5".to_string());
    }

    Ok(())
}

fn validate_gsi_list(
    gsis: &[GlobalSecondaryIndex],
    defs: &[AttributeDefinition],
    bm: &str,
) -> std::result::Result<(), String> {
    // Empty check is done earlier in validate_typed_request

    // Check missing attribute definitions across all GSI keys
    let def_names: Vec<&str> = defs.iter().map(|d| d.attribute_name.as_str()).collect();
    let mut missing_keys = Vec::new();
    for gsi in gsis {
        for k in &gsi.key_schema {
            if !def_names.contains(&k.attribute_name.as_str())
                && !missing_keys.contains(&k.attribute_name.as_str())
            {
                missing_keys.push(k.attribute_name.as_str());
            }
        }
    }
    if !missing_keys.is_empty() {
        let mut all_keys = Vec::new();
        for gsi in gsis {
            for k in &gsi.key_schema {
                if !all_keys.contains(&k.attribute_name.as_str()) {
                    all_keys.push(k.attribute_name.as_str());
                }
            }
        }
        return Err(format!(
            "One or more parameter values were invalid: Some index key attributes are not defined in AttributeDefinitions. Keys: [{}], AttributeDefinitions: [{}]",
            all_keys.join(", "),
            def_names.join(", ")
        ));
    }

    // Structural validation for each GSI
    for gsi in gsis {
        validate_gsi_structure(gsi)?;
    }

    // Duplicate index names
    let mut seen = std::collections::HashSet::new();
    for gsi in gsis {
        if !seen.insert(&gsi.index_name) {
            return Err(format!(
                "One or more parameter values were invalid: Duplicate index name: {}",
                gsi.index_name
            ));
        }
    }

    // Count limit
    if gsis.len() > 20 {
        return Err("One or more parameter values were invalid: GlobalSecondaryIndex count exceeds the per-table limit of 20".to_string());
    }

    // PAY_PER_REQUEST billing mode check
    if bm == "PAY_PER_REQUEST" {
        for gsi in gsis {
            if gsi.provisioned_throughput.is_some() {
                return Err(format!(
                    "One or more parameter values were invalid: ProvisionedThroughput should not be specified for index: {} when BillingMode is PAY_PER_REQUEST",
                    gsi.index_name
                ));
            }
        }
    }

    Ok(())
}

fn validate_lsi_structure(
    lsi: &LocalSecondaryIndex,
    table_ks: &[KeySchemaElement],
) -> std::result::Result<(), String> {
    // Key schema structure first
    validate_key_schema_structure(&lsi.key_schema)?;

    // Range key presence (before projection, per DynamoDB ordering)
    let lsi_sk = lsi.key_schema.iter().find(|k| k.key_type == KeyType::RANGE);
    if lsi_sk.is_none() {
        return Err(format!(
            "One or more parameter values were invalid: Index KeySchema does not have a range key for index: {}",
            lsi.index_name
        ));
    }

    // Hash key must match table hash key (before projection)
    let table_pk = table_ks
        .iter()
        .find(|k| k.key_type == KeyType::HASH)
        .map(|k| k.attribute_name.as_str());
    let lsi_pk = lsi
        .key_schema
        .iter()
        .find(|k| k.key_type == KeyType::HASH)
        .map(|k| k.attribute_name.as_str());
    if lsi_pk != table_pk {
        return Err(format!(
            "One or more parameter values were invalid: \
             Index KeySchema does not have the same leading hash key as table KeySchema \
             for index: {}. index hash key: {}, table hash key: {}",
            lsi.index_name,
            lsi_pk.unwrap_or("null"),
            table_pk.unwrap_or("null")
        ));
    }

    // Projection (after range key and hash key checks)
    validate_proj_structure(&lsi.projection)?;

    Ok(())
}

fn validate_gsi_structure(gsi: &GlobalSecondaryIndex) -> std::result::Result<(), String> {
    validate_key_schema_structure(&gsi.key_schema)?;
    validate_proj_structure(&gsi.projection)?;
    Ok(())
}

fn validate_proj_structure(p: &Projection) -> std::result::Result<(), String> {
    match &p.projection_type {
        None => Err(
            "One or more parameter values were invalid: Unknown ProjectionType: null".to_string(),
        ),
        Some(pt) => {
            if let Some(ref nka) = p.non_key_attributes {
                match pt {
                    ProjectionType::ALL => return Err("One or more parameter values were invalid: ProjectionType is ALL, but NonKeyAttributes is specified".to_string()),
                    ProjectionType::KEYS_ONLY => return Err("One or more parameter values were invalid: ProjectionType is KEYS_ONLY, but NonKeyAttributes is specified".to_string()),
                    ProjectionType::INCLUDE => { if nka.is_empty() { return Err("One or more parameter values were invalid: NonKeyAttributes must not be empty".to_string()); } }
                }
            }
            Ok(())
        }
    }
}
