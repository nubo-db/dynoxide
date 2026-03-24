use crate::actions::create_table::StreamSpecification;
use crate::actions::{TableDescription, build_table_description};
use crate::actions::{gsi, helpers};
use crate::errors::{DynoxideError, Result};
use crate::storage::Storage;
use crate::streams;
use crate::types::{AttributeDefinition, GlobalSecondaryIndex, KeySchemaElement, Projection};
use crate::validation;
use rusqlite;
use serde::{Deserialize, Serialize};

/// Internal raw deserialization struct.
#[derive(Debug, Default, Deserialize)]
struct UpdateTableRequestRaw {
    #[serde(rename = "TableName", default)]
    table_name: Option<String>,

    #[serde(rename = "AttributeDefinitions", default)]
    attribute_definitions: Option<Vec<AttributeDefinition>>,

    #[serde(rename = "GlobalSecondaryIndexUpdates", default)]
    global_secondary_index_updates: Option<Vec<GlobalSecondaryIndexUpdate>>,

    #[serde(rename = "StreamSpecification", default)]
    stream_specification: Option<StreamSpecification>,

    #[serde(rename = "DeletionProtectionEnabled", default)]
    deletion_protection_enabled: Option<bool>,

    #[serde(rename = "ProvisionedThroughput", default)]
    provisioned_throughput: Option<serde_json::Value>,

    #[serde(rename = "BillingMode", default)]
    billing_mode: Option<String>,
}

#[derive(Debug, Default)]
pub struct UpdateTableRequest {
    pub table_name: String,
    pub attribute_definitions: Option<Vec<AttributeDefinition>>,
    pub global_secondary_index_updates: Option<Vec<GlobalSecondaryIndexUpdate>>,
    pub stream_specification: Option<StreamSpecification>,
    pub deletion_protection_enabled: Option<bool>,
    pub provisioned_throughput: Option<serde_json::Value>,
    pub billing_mode: Option<String>,
}

impl<'de> serde::Deserialize<'de> for UpdateTableRequest {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let raw = UpdateTableRequestRaw::deserialize(deserializer)?;

        // Phase 1: Check TableName missing
        if raw.table_name.is_none() || raw.table_name.as_deref() == Some("") {
            let msg = if raw.table_name.is_none() {
                "The parameter 'TableName' is required but was not present in the request"
            } else {
                "TableName must be at least 3 characters long and at most 255 characters long"
            };
            return Err(serde::de::Error::custom(format!("VALIDATION:{}", msg)));
        }

        let table_name = raw.table_name.unwrap_or_default();

        // Phase 2: Check TableName length
        if table_name.len() < 3 || table_name.len() > 255 {
            return Err(serde::de::Error::custom(
                "VALIDATION:TableName must be at least 3 characters long and at most 255 characters long",
            ));
        }

        // Phase 3: Multi-field constraint validation
        let mut errors = Vec::new();

        if !table_name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
        {
            errors.push(format!(
                "Value '{}' at 'tableName' failed to satisfy constraint: \
                 Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+",
                table_name
            ));
        }

        if let Some(msg) = crate::validation::format_validation_errors(&errors) {
            return Err(serde::de::Error::custom(format!("VALIDATION:{}", msg)));
        }

        Ok(UpdateTableRequest {
            table_name,
            attribute_definitions: raw.attribute_definitions,
            global_secondary_index_updates: raw.global_secondary_index_updates,
            stream_specification: raw.stream_specification,
            deletion_protection_enabled: raw.deletion_protection_enabled,
            provisioned_throughput: raw.provisioned_throughput,
            billing_mode: raw.billing_mode,
        })
    }
}

#[derive(Debug, Default, Deserialize)]
pub struct GlobalSecondaryIndexUpdate {
    #[serde(rename = "Update", default)]
    pub update: Option<UpdateGsiAction>,

    #[serde(rename = "Create", default)]
    pub create: Option<CreateGsiAction>,

    #[serde(rename = "Delete", default)]
    pub delete: Option<DeleteGsiAction>,
}

#[derive(Debug, Default, Deserialize)]
pub struct UpdateGsiAction {
    #[serde(rename = "IndexName")]
    pub index_name: String,

    #[serde(rename = "ProvisionedThroughput", default)]
    pub provisioned_throughput: Option<crate::types::ProvisionedThroughput>,
}

#[derive(Debug, Default, Deserialize)]
pub struct CreateGsiAction {
    #[serde(rename = "IndexName")]
    pub index_name: String,

    #[serde(rename = "KeySchema")]
    pub key_schema: Vec<KeySchemaElement>,

    #[serde(rename = "Projection")]
    pub projection: Projection,
}

#[derive(Debug, Default, Deserialize)]
pub struct DeleteGsiAction {
    #[serde(rename = "IndexName")]
    pub index_name: String,
}

#[derive(Debug, Default, Serialize)]
pub struct UpdateTableResponse {
    #[serde(rename = "TableDescription")]
    pub table_description: TableDescription,
}

pub fn execute(storage: &Storage, request: UpdateTableRequest) -> Result<UpdateTableResponse> {
    // Table name validation is handled in the Deserialize impl

    // Phase 1: Validate request parameters BEFORE table existence check
    // (DynamoDB validates these first and returns ValidationException,
    // not ResourceNotFoundException)
    validate_update_request(&request)?;

    // Phase 2: Table existence check
    let meta = helpers::require_table(storage, &request.table_name)?;

    let current_billing_mode = meta.billing_mode.as_deref().unwrap_or("PROVISIONED");

    // Phase 3: Post-table-existence validations

    // PAY_PER_REQUEST table + ProvisionedThroughput update is not allowed
    if current_billing_mode == "PAY_PER_REQUEST"
        && request.billing_mode.is_none()
        && request.provisioned_throughput.is_some()
    {
        return Err(DynoxideError::ValidationException(
            "One or more parameter values were invalid: \
             Neither ReadCapacityUnits nor WriteCapacityUnits can be \
             specified when BillingMode is PAY_PER_REQUEST"
                .to_string(),
        ));
    }

    // BillingMode PROVISIONED without ProvisionedThroughput
    if request.billing_mode.as_deref() == Some("PROVISIONED")
        && request.provisioned_throughput.is_none()
    {
        return Err(DynoxideError::ValidationException(
            "One or more parameter values were invalid: \
             ProvisionedThroughput must be specified when BillingMode is PROVISIONED"
                .to_string(),
        ));
    }

    // Same read/write values check
    if let Some(ref pt) = request.provisioned_throughput {
        if let Some(obj) = pt.as_object() {
            let new_rcu = obj
                .get("ReadCapacityUnits")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            let new_wcu = obj
                .get("WriteCapacityUnits")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);

            // Parse current provisioned throughput from metadata
            let (cur_rcu, cur_wcu) = parse_current_throughput(&meta);

            let billing_mode_unchanged = request.billing_mode.is_none()
                || (request.billing_mode.as_deref() == Some("PROVISIONED")
                    && current_billing_mode == "PROVISIONED");

            if new_rcu == cur_rcu && new_wcu == cur_wcu && billing_mode_unchanged {
                return Err(DynoxideError::ValidationException(format!(
                    "The provisioned throughput for the table will not change. \
                     The requested value equals the current value. \
                     Current ReadCapacityUnits provisioned for the table: {}. \
                     Requested ReadCapacityUnits: {}. \
                     Current WriteCapacityUnits provisioned for the table: {}. \
                     Requested WriteCapacityUnits: {}. \
                     Refer to the Amazon DynamoDB Developer Guide for current limits \
                     and how to request higher limits.",
                    cur_rcu, new_rcu, cur_wcu, new_wcu
                )));
            }
        }
    }

    // Parse existing GSI definitions
    let mut current_gsis: Vec<GlobalSecondaryIndex> = meta
        .gsi_definitions
        .as_ref()
        .map(|json| serde_json::from_str(json))
        .transpose()
        .map_err(|e| DynoxideError::InternalServerError(format!("Bad GSI JSON: {e}")))?
        .unwrap_or_default();

    // GSI Update with high capacity on non-existent index
    if let Some(ref updates) = request.global_secondary_index_updates {
        for update in updates {
            if let Some(ref upd) = update.update {
                if !current_gsis.iter().any(|g| g.index_name == upd.index_name) {
                    // DynamoDB returns this specific message for GSI updates on
                    // non-existent indexes (even with out-of-bounds capacity)
                    return Err(DynoxideError::ValidationException(
                        "This operation cannot be performed with given input values. \
                         Please contact DynamoDB service team for more info: \
                         Action Blocked: IndexUpdate"
                            .to_string(),
                    ));
                }
            }
        }
    }

    // Check GSI update count limit (DynamoDB allows at most 5 per request)
    if let Some(ref updates) = request.global_secondary_index_updates {
        if updates.len() > 5 {
            return Err(DynoxideError::LimitExceededException(
                "Subscriber limit exceeded: Only 1 online index can be created or \
                 deleted simultaneously per table"
                    .to_string(),
            ));
        }
    }

    // Use provided attribute definitions or fall back to existing
    let existing_attr_defs: Vec<AttributeDefinition> =
        serde_json::from_str(&meta.attribute_definitions)
            .map_err(|e| DynoxideError::InternalServerError(format!("Bad attr defs JSON: {e}")))?;

    let attr_defs = request
        .attribute_definitions
        .as_ref()
        .unwrap_or(&existing_attr_defs);

    // Parse table key schema for backfill
    let key_schema = helpers::parse_key_schema(&meta)?;

    // Validate all GSI updates before making any changes
    if let Some(ref updates) = request.global_secondary_index_updates {
        for update in updates {
            if let Some(ref create) = update.create {
                if current_gsis
                    .iter()
                    .any(|g| g.index_name == create.index_name)
                {
                    return Err(DynoxideError::ValidationException(format!(
                        "One or more parameter values were invalid: \
                         Index already exists: {}",
                        create.index_name
                    )));
                }
                let gsi_def = GlobalSecondaryIndex {
                    index_name: create.index_name.clone(),
                    key_schema: create.key_schema.clone(),
                    projection: create.projection.clone(),
                    provisioned_throughput: None,
                };
                validation::validate_gsi(&gsi_def, attr_defs)?;
            }
            if let Some(ref delete) = update.delete {
                if !current_gsis
                    .iter()
                    .any(|g| g.index_name == delete.index_name)
                {
                    return Err(DynoxideError::ResourceNotFoundException(format!(
                        "Requested resource not found: Table: {} not found",
                        delete.index_name
                    )));
                }
            }
        }
    }

    // Determine if this is a throughput increase or decrease.
    // Ensure timestamps strictly increase across successive updates
    // (the dynalite test expects LastDecreaseDateTime > LastIncreaseDateTime).
    let now = {
        use std::sync::atomic::{AtomicU64, Ordering};
        static LAST_TS: AtomicU64 = AtomicU64::new(0);
        let wall = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();
        loop {
            let prev_bits = LAST_TS.load(Ordering::SeqCst);
            let prev_f = f64::from_bits(prev_bits);
            let candidate = if wall > prev_f { wall } else { prev_f + 0.001 };
            let candidate_bits = candidate.to_bits();
            if LAST_TS
                .compare_exchange(
                    prev_bits,
                    candidate_bits,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_ok()
            {
                break candidate;
            }
        }
    };

    let (cur_rcu, cur_wcu) = parse_current_throughput(&meta);
    let is_pt_update = request.provisioned_throughput.is_some();
    let (new_rcu, new_wcu) = if let Some(ref pt) = request.provisioned_throughput {
        let obj = pt.as_object();
        (
            obj.and_then(|o| o.get("ReadCapacityUnits"))
                .and_then(|v| v.as_i64())
                .unwrap_or(0),
            obj.and_then(|o| o.get("WriteCapacityUnits"))
                .and_then(|v| v.as_i64())
                .unwrap_or(0),
        )
    } else {
        (cur_rcu, cur_wcu)
    };

    let is_increase = new_rcu > cur_rcu || new_wcu > cur_wcu;
    let is_decrease = new_rcu < cur_rcu || new_wcu < cur_wcu;

    // All validation passed — perform mutations inside a transaction
    storage.begin_transaction()?;

    let result = (|| -> Result<()> {
        if let Some(ref updates) = request.global_secondary_index_updates {
            for update in updates {
                if let Some(ref create) = update.create {
                    let gsi_def = GlobalSecondaryIndex {
                        index_name: create.index_name.clone(),
                        key_schema: create.key_schema.clone(),
                        projection: create.projection.clone(),
                        provisioned_throughput: None,
                    };

                    storage.create_gsi_table(&request.table_name, &create.index_name)?;

                    let gsi_p = gsi::gsi_to_def(&gsi_def)?;
                    backfill_gsi(storage, &request.table_name, &key_schema, &gsi_p)?;

                    current_gsis.push(gsi_def);
                }

                if let Some(ref delete) = update.delete {
                    storage.drop_gsi_table(&request.table_name, &delete.index_name)?;
                    current_gsis.retain(|g| g.index_name != delete.index_name);
                }
            }
        }

        // Update metadata
        let attr_defs_json = serde_json::to_string(attr_defs)
            .map_err(|e| DynoxideError::InternalServerError(e.to_string()))?;
        let gsi_json = if current_gsis.is_empty() {
            None
        } else {
            Some(
                serde_json::to_string(&current_gsis)
                    .map_err(|e| DynoxideError::InternalServerError(e.to_string()))?,
            )
        };

        storage.update_table_metadata(&request.table_name, &attr_defs_json, gsi_json.as_deref())?;

        // Update provisioned throughput if requested
        if is_pt_update {
            let prev = parse_stored_throughput(&meta);
            let mut stored = StoredProvisionedThroughput {
                read_capacity_units: new_rcu,
                write_capacity_units: new_wcu,
                last_increase_date_time: prev.as_ref().and_then(|p| p.last_increase_date_time),
                last_decrease_date_time: prev.as_ref().and_then(|p| p.last_decrease_date_time),
                number_of_decreases_today: prev
                    .as_ref()
                    .and_then(|p| p.number_of_decreases_today)
                    .or(Some(0)),
            };
            if is_increase {
                stored.last_increase_date_time = Some(now);
            }
            if is_decrease {
                stored.last_decrease_date_time = Some(now);
                stored.number_of_decreases_today =
                    Some(stored.number_of_decreases_today.unwrap_or(0) + 1);
            }
            let pt_json = serde_json::to_string(&stored)
                .map_err(|e| DynoxideError::InternalServerError(e.to_string()))?;
            storage.update_provisioned_throughput(&request.table_name, &pt_json)?;
        }

        // Handle deletion protection changes
        if let Some(enabled) = request.deletion_protection_enabled {
            storage.update_deletion_protection(&request.table_name, enabled)?;
        }

        // Handle billing mode changes
        if let Some(ref billing_mode) = request.billing_mode {
            storage.update_billing_mode(&request.table_name, billing_mode)?;
            if billing_mode == "PAY_PER_REQUEST" {
                // Clear provisioned throughput to avoid stale data
                storage.clear_provisioned_throughput(&request.table_name)?;
            }
        }

        // Handle stream specification changes
        if let Some(ref spec) = request.stream_specification {
            if spec.stream_enabled {
                let view_type = spec
                    .stream_view_type
                    .as_deref()
                    .unwrap_or("NEW_AND_OLD_IMAGES");
                let label = streams::generate_stream_label();
                storage.enable_stream(&request.table_name, view_type, &label)?;
            } else {
                storage.disable_stream(&request.table_name)?;
            }
        }

        Ok(())
    })();

    match result {
        Ok(()) => storage.commit()?,
        Err(e) => {
            let _ = storage.rollback();
            return Err(e);
        }
    }

    // Build response from updated metadata
    let updated_meta = helpers::require_table(storage, &request.table_name)?;
    let mut desc = build_table_description(&updated_meta, Some(0), Some(0));

    // DynamoDB returns UPDATING status during throughput changes
    if is_pt_update {
        desc.table_status = "UPDATING".to_string();

        // The immediate response shows the OLD throughput values while the
        // table is in UPDATING status, but with updated timestamps.
        let stored = parse_stored_throughput(&updated_meta);
        if let Some(ref mut pt) = desc.provisioned_throughput {
            pt.read_capacity_units = cur_rcu as u64;
            pt.write_capacity_units = cur_wcu as u64;
            if let Some(ref s) = stored {
                pt.last_increase_date_time = s.last_increase_date_time;
                pt.last_decrease_date_time = s.last_decrease_date_time;
                pt.number_of_decreases_today = s.number_of_decreases_today.unwrap_or(0);
            }
        }
    }

    Ok(UpdateTableResponse {
        table_description: desc,
    })
}

/// Validate UpdateTable request parameters before checking table existence.
///
/// DynamoDB validates these parameters first and returns ValidationException
/// rather than ResourceNotFoundException when both are invalid.
fn validate_update_request(request: &UpdateTableRequest) -> Result<()> {
    // Multi-field constraint errors
    let mut errors = Vec::new();

    // Validate ProvisionedThroughput fields
    if let Some(ref pt) = request.provisioned_throughput {
        if let Some(obj) = pt.as_object() {
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

    // Validate GlobalSecondaryIndexUpdates fields
    if let Some(ref updates) = request.global_secondary_index_updates {
        for (i, update) in updates.iter().enumerate() {
            if let Some(ref upd) = update.update {
                // Validate Update.IndexName
                if upd.index_name.len() < 3 {
                    errors.push(format!("Value '{}' at 'globalSecondaryIndexUpdates.{}.member.update.indexName' failed to satisfy constraint: Member must have length greater than or equal to 3", upd.index_name, i + 1));
                }
                if !upd.index_name.is_empty()
                    && !upd
                        .index_name
                        .chars()
                        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
                {
                    errors.push(format!("Value '{}' at 'globalSecondaryIndexUpdates.{}.member.update.indexName' failed to satisfy constraint: Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+", upd.index_name, i + 1));
                }
                // Validate Update.ProvisionedThroughput
                if let Some(ref pt) = upd.provisioned_throughput {
                    let wcu = pt.write_capacity_units;
                    let rcu = pt.read_capacity_units;
                    if wcu.is_none() {
                        errors.push(format!("Value null at 'globalSecondaryIndexUpdates.{}.member.update.provisionedThroughput.writeCapacityUnits' failed to satisfy constraint: Member must not be null", i + 1));
                    } else if let Some(w) = wcu {
                        if w < 1 {
                            errors.push(format!("Value '{}' at 'globalSecondaryIndexUpdates.{}.member.update.provisionedThroughput.writeCapacityUnits' failed to satisfy constraint: Member must have value greater than or equal to 1", w, i + 1));
                        }
                    }
                    if rcu.is_none() {
                        errors.push(format!("Value null at 'globalSecondaryIndexUpdates.{}.member.update.provisionedThroughput.readCapacityUnits' failed to satisfy constraint: Member must not be null", i + 1));
                    } else if let Some(r) = rcu {
                        if r < 1 {
                            errors.push(format!("Value '{}' at 'globalSecondaryIndexUpdates.{}.member.update.provisionedThroughput.readCapacityUnits' failed to satisfy constraint: Member must have value greater than or equal to 1", r, i + 1));
                        }
                    }
                } else {
                    errors.push(format!("Value null at 'globalSecondaryIndexUpdates.{}.member.update.provisionedThroughput' failed to satisfy constraint: Member must not be null", i + 1));
                }
            }
        }
    }

    // Cap at 10 errors
    errors.truncate(10);

    if !errors.is_empty() {
        let prefix = format!(
            "{} validation error{} detected: ",
            errors.len(),
            if errors.len() == 1 { "" } else { "s" }
        );
        return Err(DynoxideError::ValidationException(format!(
            "{}{}",
            prefix,
            errors.join("; ")
        )));
    }

    // Single-error validations (after multi-field)

    // BillingMode enum validation
    if let Some(ref bm) = request.billing_mode {
        if bm != "PROVISIONED" && bm != "PAY_PER_REQUEST" {
            return Err(DynoxideError::ValidationException(format!(
                "1 validation error detected: Value '{}' at 'billingMode' \
                 failed to satisfy constraint: Member must satisfy enum value set: \
                 [PROVISIONED, PAY_PER_REQUEST]",
                bm
            )));
        }
    }

    // BillingMode PAY_PER_REQUEST with ProvisionedThroughput is not allowed
    if request.billing_mode.as_deref() == Some("PAY_PER_REQUEST")
        && request.provisioned_throughput.is_some()
    {
        return Err(DynoxideError::ValidationException(
            "One or more parameter values were invalid: \
             Neither ReadCapacityUnits nor WriteCapacityUnits can be \
             specified when BillingMode is PAY_PER_REQUEST"
                .to_string(),
        ));
    }

    // ProvisionedThroughput out-of-bounds
    if let Some(ref pt) = request.provisioned_throughput {
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
    }

    // Empty GlobalSecondaryIndexUpdates
    if let Some(ref updates) = request.global_secondary_index_updates {
        if updates.is_empty() {
            // "At least one of ..." - but only if nothing else is specified
            if request.provisioned_throughput.is_none()
                && request.billing_mode.is_none()
                && request.stream_specification.is_none()
            {
                return Err(DynoxideError::ValidationException(
                    "At least one of ProvisionedThroughput, BillingMode, UpdateStreamEnabled, GlobalSecondaryIndexUpdates or SSESpecification or ReplicaUpdates is required".to_string(),
                ));
            }
        }
    } else if request.provisioned_throughput.is_none()
        && request.billing_mode.is_none()
        && request.stream_specification.is_none()
        && request.deletion_protection_enabled.is_none()
    {
        return Err(DynoxideError::ValidationException(
            "At least one of ProvisionedThroughput, BillingMode, UpdateStreamEnabled, GlobalSecondaryIndexUpdates or SSESpecification or ReplicaUpdates is required".to_string(),
        ));
    }

    // Validate GSI update structural constraints
    if let Some(ref updates) = request.global_secondary_index_updates {
        // Check empty index struct (no Update, Create, or Delete)
        for update in updates {
            if update.update.is_none() && update.create.is_none() && update.delete.is_none() {
                return Err(DynoxideError::ValidationException(
                    "One or more parameter values were invalid: One of GlobalSecondaryIndexUpdate.Update, GlobalSecondaryIndexUpdate.Create, GlobalSecondaryIndexUpdate.Delete must not be null".to_string(),
                ));
            }
        }

        // Check repeated index names
        let mut seen_names = std::collections::HashSet::new();
        for update in updates {
            let name = if let Some(ref u) = update.update {
                Some(u.index_name.as_str())
            } else if let Some(ref c) = update.create {
                Some(c.index_name.as_str())
            } else {
                update.delete.as_ref().map(|d| d.index_name.as_str())
            };
            if let Some(name) = name {
                if !seen_names.insert(name.to_string()) {
                    return Err(DynoxideError::ValidationException(format!(
                        "One or more parameter values were invalid: Only one global secondary index update per index is allowed simultaneously. Index: {}",
                        name
                    )));
                }
            }
        }
    }

    Ok(())
}

/// Extended provisioned throughput stored in metadata, including timestamps.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct StoredProvisionedThroughput {
    #[serde(rename = "ReadCapacityUnits")]
    read_capacity_units: i64,
    #[serde(rename = "WriteCapacityUnits")]
    write_capacity_units: i64,
    #[serde(
        rename = "LastIncreaseDateTime",
        skip_serializing_if = "Option::is_none"
    )]
    last_increase_date_time: Option<f64>,
    #[serde(
        rename = "LastDecreaseDateTime",
        skip_serializing_if = "Option::is_none"
    )]
    last_decrease_date_time: Option<f64>,
    #[serde(
        rename = "NumberOfDecreasesToday",
        skip_serializing_if = "Option::is_none"
    )]
    number_of_decreases_today: Option<u64>,
}

/// Parse current provisioned throughput from table metadata.
fn parse_current_throughput(meta: &crate::storage::TableMetadata) -> (i64, i64) {
    parse_stored_throughput(meta)
        .map(|pt| (pt.read_capacity_units, pt.write_capacity_units))
        .unwrap_or((0, 0))
}

/// Parse the full stored provisioned throughput including timestamps.
fn parse_stored_throughput(
    meta: &crate::storage::TableMetadata,
) -> Option<StoredProvisionedThroughput> {
    meta.provisioned_throughput
        .as_ref()
        .and_then(|pt_json| serde_json::from_str(pt_json).ok())
}

/// Backfill existing items into a newly created GSI, processing in batches.
fn backfill_gsi(
    storage: &Storage,
    table_name: &str,
    key_schema: &helpers::KeySchema,
    gsi_def: &gsi::GsiDef,
) -> Result<()> {
    const BATCH_SIZE: usize = 1000;
    let mut last_pk: Option<String> = None;
    let mut last_sk: Option<String> = None;

    let gsi_table_name = format!("{}::gsi::{}", table_name, gsi_def.index_name);
    let insert_sql = format!(
        "INSERT OR REPLACE INTO \"{}\" (gsi_pk, gsi_sk, table_pk, table_sk, item_json) \
         VALUES (?1, ?2, ?3, ?4, ?5)",
        crate::storage::escape_table_name(&gsi_table_name)
    );
    let mut stmt = storage
        .conn()
        .prepare_cached(&insert_sql)
        .map_err(|e| DynoxideError::InternalServerError(e.to_string()))?;

    loop {
        let items = storage.scan_items(
            table_name,
            &crate::storage::ScanParams {
                limit: Some(BATCH_SIZE),
                exclusive_start_pk: last_pk.as_deref(),
                exclusive_start_sk: last_sk.as_deref(),
                ..Default::default()
            },
        )?;

        if items.is_empty() {
            break;
        }

        for (pk, sk, item_json) in &items {
            let item: crate::types::Item = serde_json::from_str(item_json)
                .map_err(|e| DynoxideError::InternalServerError(format!("Bad item JSON: {e}")))?;

            if let Some(gsi_pk_val) = item.get(&gsi_def.pk_attr) {
                let gsi_pk = gsi_pk_val.to_key_string().unwrap_or_default();
                let gsi_sk = gsi_def
                    .sk_attr
                    .as_ref()
                    .and_then(|sk_attr| item.get(sk_attr))
                    .and_then(|v| v.to_key_string())
                    .unwrap_or_default();

                let projected = gsi::build_index_item(
                    &item,
                    gsi_def,
                    &key_schema.partition_key,
                    key_schema.sort_key.as_deref(),
                );
                let projected_json = serde_json::to_string(&projected)
                    .map_err(|e| DynoxideError::InternalServerError(e.to_string()))?;

                stmt.execute(rusqlite::params![gsi_pk, gsi_sk, pk, sk, projected_json])
                    .map_err(DynoxideError::from)?;
            }
        }

        let last = &items[items.len() - 1];
        last_pk = Some(last.0.clone());
        last_sk = Some(last.1.clone());

        if items.len() < BATCH_SIZE {
            break;
        }
    }

    Ok(())
}
