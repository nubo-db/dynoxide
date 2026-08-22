use crate::actions::create_table::StreamSpecification;
use crate::actions::vector_lifecycle::{VectorIndexLifecycle, phases_armed_on};
use crate::actions::{TableDescription, build_table_description};
use crate::actions::{gsi, helpers};
use crate::errors::{DynoxideError, Result};
use crate::storage_backend::StorageBackend;
use crate::streams;
use crate::types::{
    AttributeDefinition, GlobalSecondaryIndex, Item, KeySchemaElement, Projection, VectorIndex,
};
use crate::validation;
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

    // Raw JSON so the create action's Dimensions can be normalised (fractional
    // values truncate, over-range values clamp) before the typed parse, the
    // same as the CreateTable path.
    #[serde(rename = "VectorIndexUpdates", default)]
    vector_index_updates: Option<serde_json::Value>,

    #[serde(rename = "StreamSpecification", default)]
    stream_specification: Option<StreamSpecification>,

    #[serde(rename = "DeletionProtectionEnabled", default)]
    deletion_protection_enabled: Option<bool>,

    #[serde(rename = "ProvisionedThroughput", default)]
    provisioned_throughput: Option<serde_json::Value>,

    #[serde(rename = "BillingMode", default)]
    billing_mode: Option<String>,

    #[serde(rename = "TableClass", default)]
    table_class: Option<String>,

    #[serde(rename = "OnDemandThroughput", default)]
    on_demand_throughput: Option<crate::types::OnDemandThroughput>,
}

#[derive(Debug, Default)]
pub struct UpdateTableRequest {
    pub table_name: String,
    pub attribute_definitions: Option<Vec<AttributeDefinition>>,
    pub global_secondary_index_updates: Option<Vec<GlobalSecondaryIndexUpdate>>,
    pub vector_index_updates: Option<Vec<VectorIndexUpdate>>,
    pub stream_specification: Option<StreamSpecification>,
    pub deletion_protection_enabled: Option<bool>,
    pub provisioned_throughput: Option<serde_json::Value>,
    pub billing_mode: Option<String>,
    pub table_class: Option<String>,
    pub on_demand_throughput: Option<crate::types::OnDemandThroughput>,
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

        // Request-model constraint errors for the raw vector create actions,
        // enveloped like CreateTable's collectors rather than surfacing raw
        // serde errors. Real DynamoDB reports one envelope across index
        // families, GSI entries before vector entries (captured eu-west-2 and
        // us-east-1, 2026-08-12), so the GSI Update-action errors join in
        // front here. The top-level ProvisionedThroughput family stays in the
        // operation-layer envelope in validate_update_request, so a request
        // invalid there and here gets this envelope alone; whether real
        // DynamoDB folds that family in too is uncaptured.
        if let Some(ref vix_val) = raw.vector_index_updates {
            let mut vix_errors = Vec::new();
            collect_vix_update_errors(vix_val, &mut vix_errors);
            if !vix_errors.is_empty() {
                let mut errors = Vec::new();
                if let Some(ref gsi_updates) = raw.global_secondary_index_updates {
                    collect_gsi_update_errors(gsi_updates, &mut errors);
                }
                errors.append(&mut vix_errors);
                errors.truncate(10);
                let msg = format!(
                    "{} validation error{} detected: {}",
                    errors.len(),
                    if errors.len() == 1 { "" } else { "s" },
                    errors.join("; ")
                );
                return Err(serde::de::Error::custom(format!("VALIDATION:{}", msg)));
            }
        }

        let vector_index_updates = raw
            .vector_index_updates
            .as_ref()
            .map(parse_vector_index_updates)
            .transpose()
            .map_err(serde::de::Error::custom)?;

        Ok(UpdateTableRequest {
            table_name,
            attribute_definitions: raw.attribute_definitions,
            global_secondary_index_updates: raw.global_secondary_index_updates,
            vector_index_updates,
            stream_specification: raw.stream_specification,
            deletion_protection_enabled: raw.deletion_protection_enabled,
            provisioned_throughput: raw.provisioned_throughput,
            billing_mode: raw.billing_mode,
            table_class: raw.table_class,
            on_demand_throughput: raw.on_demand_throughput,
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

/// One `VectorIndexUpdates` entry, modelled on [`GlobalSecondaryIndexUpdate`].
/// Only Create and Delete actions exist: a vector index's configuration is
/// immutable, so there is no Update action (captured from real DynamoDB,
/// eu-west-2, 2026-08-11).
#[derive(Debug, Default)]
pub struct VectorIndexUpdate {
    /// The create action carries a full vector index definition, the same
    /// shape as a CreateTable `VectorIndexes` entry.
    pub create: Option<VectorIndex>,
    pub delete: Option<DeleteVectorIndexAction>,
}

#[derive(Debug, Default, Deserialize)]
pub struct DeleteVectorIndexAction {
    #[serde(rename = "IndexName")]
    pub index_name: String,
}

/// Request-model constraint errors for `VectorIndexUpdates` create actions,
/// mirroring CreateTable's `collect_vix_errors` with this operation's field
/// paths: `vectorIndexUpdates.N.member.create.<field>` (captured from real
/// DynamoDB, eu-west-2 and us-east-1, 2026-08-12).
fn collect_vix_update_errors(val: &serde_json::Value, errors: &mut Vec<String>) {
    let Some(arr) = val.as_array() else {
        return;
    };
    for (i, elem) in arr.iter().enumerate().take(10) {
        let Some(obj) = elem.as_object() else {
            continue;
        };
        if let Some(create) = obj.get("Create").and_then(|v| v.as_object()) {
            crate::actions::create_table::collect_vix_obj_errors(
                create,
                &format!("vectorIndexUpdates.{}.member.create", i + 1),
                errors,
            );
        }
    }
}

/// Run the `VectorIndexUpdates` request-model constraints and format the
/// envelope, for a caller that does not arrive through the raw request's
/// `Deserialize`. Feed it the canonical wire spelling; see the CreateTable
/// sibling for why.
#[cfg(feature = "mcp-server")]
pub(crate) fn vector_index_updates_request_model_error(val: &serde_json::Value) -> Option<String> {
    let mut errors = Vec::new();
    collect_vix_update_errors(val, &mut errors);
    errors.truncate(10);
    if errors.is_empty() {
        return None;
    }
    Some(format!(
        "{} validation error{} detected: {}",
        errors.len(),
        if errors.len() == 1 { "" } else { "s" },
        errors.join("; ")
    ))
}

/// Parse the raw `VectorIndexUpdates` JSON into typed updates, normalising
/// each create action's `Dimensions` the way the CreateTable path does so a
/// fractional or over-range value cannot fail the typed `u32` parse.
///
/// Shared with the MCP surface, so an agent's update is parsed by the same code
/// as a wire client's and the two cannot disagree about what a create action
/// accepts.
pub(crate) fn parse_vector_index_updates(
    val: &serde_json::Value,
) -> std::result::Result<Vec<VectorIndexUpdate>, String> {
    let arr = val
        .as_array()
        .ok_or_else(|| "Unexpected field type".to_string())?;
    let mut out = Vec::with_capacity(arr.len());
    for elem in arr {
        let obj = elem
            .as_object()
            .ok_or_else(|| "Unexpected value type in payload".to_string())?;
        let mut update = VectorIndexUpdate::default();
        // Wire member names are case sensitive, and every guard around this
        // parser keys on the wire spelling: the request-model collector and the
        // pre-deserialisation type checks both read `Create` and `Delete`. A
        // second spelling accepted here would reach neither, so a lowercase
        // action would apply with nothing validating it. A caller that speaks
        // another casing canonicalises before calling in.
        if let Some(create) = obj.get("Create").filter(|v| !v.is_null()) {
            let mut create = create.clone();
            if let Some(create_obj) = create.as_object_mut() {
                crate::actions::create_table::normalise_vix_dimensions_obj(create_obj);
            }
            update.create = Some(serde_json::from_value(create).map_err(|e| e.to_string())?);
        }
        if let Some(delete) = obj.get("Delete").filter(|v| !v.is_null()) {
            update.delete =
                Some(serde_json::from_value(delete.clone()).map_err(|e| e.to_string())?);
        }
        out.push(update);
    }
    Ok(out)
}

#[derive(Debug, Default, Serialize)]
pub struct UpdateTableResponse {
    #[serde(rename = "TableDescription")]
    pub table_description: TableDescription,
}

pub async fn execute<S: StorageBackend>(
    storage: &S,
    mut request: UpdateTableRequest,
    lifecycle: &VectorIndexLifecycle,
) -> Result<UpdateTableResponse> {
    // Table name validation is handled in the Deserialize impl

    // An OnDemandThroughput object with no members carries no change, so
    // treat it as absent: it neither satisfies the at-least-one-change
    // guard nor produces an echo. Real DynamoDB returns InternalFailure for
    // this input (captured eu-west-2, 2026-07-24); a deterministic
    // validation error is a deliberate divergence from emulating a 500.
    if request.on_demand_throughput.as_ref().is_some_and(|odt| {
        odt.max_read_request_units.is_none() && odt.max_write_request_units.is_none()
    }) {
        request.on_demand_throughput = None;
    }

    // Phase 1: Validate request parameters BEFORE table existence check
    // (DynamoDB validates these first and returns ValidationException,
    // not ResourceNotFoundException)
    validate_update_request(&request)?;

    // Phase 2: Table existence check
    let meta = helpers::require_table(storage, &request.table_name).await?;

    let current_billing_mode = meta.billing_mode.as_deref().unwrap_or("PROVISIONED");

    // Parse existing vector index definitions early: the billing-mode gate
    // below and the update validation both need them.
    let mut current_vixs: Vec<VectorIndex> = meta
        .vector_index_definitions
        .as_ref()
        .map(|json| serde_json::from_str(json))
        .transpose()
        .map_err(|e| DynoxideError::InternalServerError(format!("Bad vector index JSON: {e}")))?
        .unwrap_or_default();

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

    // A vector-indexed table cannot leave PAY_PER_REQUEST. The switch has
    // its own string, distinct from the create-time gate's, and reads the
    // stored definitions, so deleting the last index and flipping in the
    // same call is still rejected. Captured from real DynamoDB (eu-west-2
    // and us-east-1, 2026-08-12).
    if request.billing_mode.as_deref() == Some("PROVISIONED") && !current_vixs.is_empty() {
        return Err(DynoxideError::ValidationException(
            "One or more parameter values were invalid: Tables with vector indexes must \
             be in PAY_PER_REQUEST mode"
                .to_string(),
        ));
    }

    // OnDemandThroughput is only valid when the table ends up PAY_PER_REQUEST:
    // either the request switches to it, or the table already is and the
    // request does not switch away. The gate reads the committed billing mode
    // and names the first present member, read checked first; the update
    // wording carries "the" and no full stop, unlike CreateTable's. The gate
    // fires before the bounds check when both are violated. All captured from
    // real DynamoDB (eu-west-2, 2026-07-24).
    if let Some(ref odt) = request.on_demand_throughput {
        let target_is_provisioned = match request.billing_mode.as_deref() {
            Some("PAY_PER_REQUEST") => false,
            Some(_) => true,
            None => current_billing_mode == "PROVISIONED",
        };
        let members = [
            ("MaxReadRequestUnits", odt.max_read_request_units),
            ("MaxWriteRequestUnits", odt.max_write_request_units),
        ];
        if target_is_provisioned {
            if let Some((member, _)) = members.iter().find(|(_, v)| v.is_some()) {
                return Err(DynoxideError::ValidationException(format!(
                    "One or more parameter values were invalid: {member} for \
                     OnDemandThroughput cannot be specified when the table BillingMode \
                     is PROVISIONED"
                )));
            }
        }
        // Bounds: members must be at least 1, or exactly -1, which removes
        // the ceiling. Message identical to CreateTable's.
        for (member, value) in members {
            if value.is_some_and(|v| v == 0 || v < -1) {
                return Err(DynoxideError::ValidationException(format!(
                    "One or more parameter values were invalid: Requested {member} for \
                     OnDemandThroughput for table is outside of valid range"
                )));
            }
        }
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

    // Refuse a stream-specification change on a backend without streams before
    // any mutation below (GSI creates, metadata writes) can land. Ordered after
    // the AWS-pinned validations above so requests real DynamoDB would reject
    // keep their pinned errors; only a request AWS would accept reaches this.
    if request.stream_specification.is_some() && !storage.supports_streams() {
        return Err(crate::storage_backend::BackendError::Unsupported {
            capability: "streams",
        }
        .into());
    }

    // Parse existing GSI definitions
    let mut current_gsis: Vec<GlobalSecondaryIndex> = meta
        .gsi_definitions
        .as_ref()
        .map(|json| serde_json::from_str(json))
        .transpose()
        .map_err(|e| DynoxideError::InternalServerError(format!("Bad GSI JSON: {e}")))?
        .unwrap_or_default();

    // Parse existing LSI definitions: the cross-family duplicate checks below
    // and the AttributeDefinitions reconciliation both need them.
    let lsi_defs = crate::actions::lsi::parse_lsi_defs(&meta)?;

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

    // Merge provided attribute definitions into the existing set. DynamoDB
    // treats UpdateTable's AttributeDefinitions as a delta: adding a GSI only
    // requires the new index's key attributes, and the existing definitions
    // (table keys, prior GSI keys) are preserved. Replacing them outright would
    // drop attributes still referenced by the key schema and other indexes.
    let mut attr_defs: Vec<AttributeDefinition> = serde_json::from_str(&meta.attribute_definitions)
        .map_err(|e| DynoxideError::InternalServerError(format!("Bad attr defs JSON: {e}")))?;

    if let Some(ref provided) = request.attribute_definitions {
        for def in provided {
            // An already-declared attribute keeps its existing type. Real
            // DynamoDB ignores a redeclaration (even one carrying a different
            // type) rather than overwriting or rejecting it, so only genuinely
            // new attributes are appended. Verified against AWS in eu-west-2.
            if !attr_defs
                .iter()
                .any(|d| d.attribute_name == def.attribute_name)
            {
                attr_defs.push(def.clone());
            }
        }
    }

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
                // A GSI create colliding with a vector index name uses the
                // vector create path's wording, not the GSI same-family
                // string above. Captured from real DynamoDB (eu-west-2 and
                // us-east-1, 2026-08-12).
                if current_vixs
                    .iter()
                    .any(|v| v.index_name == create.index_name)
                {
                    return Err(DynoxideError::ValidationException(
                        "Attempting to create an index which already exists".to_string(),
                    ));
                }
                // A GSI cannot key on a live vector attribute: the vector
                // index already defines it as a list of the index's
                // dimensions, so the proposed scalar key definition is a
                // redefinition. The message interpolates the attribute name,
                // the live index's dimensions, the request-declared scalar
                // type, and the proposed key type. Captured from real
                // DynamoDB (eu-west-2 and us-east-1, 2026-08-12). An element
                // whose attribute the request does not declare falls through
                // to validate_gsi's missing-declaration error below.
                for elem in &create.key_schema {
                    let Some(vix) = current_vixs
                        .iter()
                        .find(|v| v.vector_attribute.attribute_name == elem.attribute_name)
                    else {
                        continue;
                    };
                    let Some(def) = request
                        .attribute_definitions
                        .as_deref()
                        .unwrap_or(&[])
                        .iter()
                        .find(|d| d.attribute_name == elem.attribute_name)
                    else {
                        continue;
                    };
                    let type_letter = match def.attribute_type {
                        crate::types::ScalarAttributeType::S => "S",
                        crate::types::ScalarAttributeType::N => "N",
                        crate::types::ScalarAttributeType::B => "B",
                    };
                    let key_type = match elem.key_type {
                        crate::types::KeyType::HASH => "HASH",
                        crate::types::KeyType::RANGE => "RANGE",
                    };
                    return Err(DynoxideError::ValidationException(format!(
                        "One or more parameter values were invalid: Attributes cannot be \
                         redefined. Please check that your attribute has the same type as \
                         previously defined. Existing schema: \
                         VectorIndexSchema:[VectorAttribute: key{{{attr}:L:{dims}}}] \
                         New schema: Schema:[SchemaElement: key{{{attr}:{type_letter}:{key_type}}}]",
                        attr = elem.attribute_name,
                        dims = vix.dimensions,
                    )));
                }
                let gsi_def = GlobalSecondaryIndex {
                    index_name: create.index_name.clone(),
                    key_schema: create.key_schema.clone(),
                    projection: create.projection.clone(),
                    provisioned_throughput: None,
                };
                // A new index's key attributes must all appear in the request's
                // own AttributeDefinitions, not the merged stored set: DynamoDB
                // requires the request to (re)declare them even when the table
                // already defines the attribute.
                validation::validate_gsi(
                    &gsi_def,
                    request.attribute_definitions.as_deref().unwrap_or(&[]),
                )?;
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

    // Validate all vector index updates before making any changes.
    if let Some(ref updates) = request.vector_index_updates {
        // Only one vector index action per call; the rejection is the GSI
        // online-index machinery's own error. Captured from real DynamoDB
        // (eu-west-2, 2026-08-11). Whether a call mixing GSI and vector
        // actions trips a combined limit is uncaptured; each family keeps
        // its own count here.
        if updates.len() > 1 {
            return Err(DynoxideError::LimitExceededException(
                "Subscriber limit exceeded: Only 1 online index can be created or \
                 deleted simultaneously per table"
                    .to_string(),
            ));
        }
        for update in updates {
            if let Some(ref create) = update.create {
                // Vector indexes only exist on PAY_PER_REQUEST tables; the
                // UpdateTable path carries the same string as CreateTable's
                // gate. Captured from real DynamoDB (eu-west-2 and us-east-1,
                // 2026-08-12).
                let target_billing_mode = request
                    .billing_mode
                    .as_deref()
                    .unwrap_or(current_billing_mode);
                if target_billing_mode != "PAY_PER_REQUEST" {
                    return Err(DynoxideError::ValidationException(
                        "One or more parameter values were invalid: Vector indexes are \
                         only supported for PAY_PER_REQUEST tables"
                            .to_string(),
                    ));
                }

                // The vector create path has its own duplicate wording, with
                // no index name in it: not the GSI duplicate string and not
                // CreateTable's classic cross-index one. The check spans all
                // index families: a name held by a live GSI or LSI collides
                // too, with the same wording. Captured from real DynamoDB
                // (eu-west-2 and us-east-1, 2026-08-12).
                if current_vixs
                    .iter()
                    .any(|v| v.index_name == create.index_name)
                    || current_gsis
                        .iter()
                        .any(|g| g.index_name == create.index_name)
                    || lsi_defs.iter().any(|l| l.index_name == create.index_name)
                {
                    return Err(DynoxideError::ValidationException(
                        "Attempting to create an index which already exists".to_string(),
                    ));
                }

                // A new index's SearchSchema attributes must all appear in the
                // request's own AttributeDefinitions, mirroring the GSI rule.
                // Always entry 1 in the rendered path: a call carries at most
                // one create action.
                validation::validate_vector_index(
                    create,
                    request.attribute_definitions.as_deref().unwrap_or(&[]),
                    "vectorIndexUpdates.1.member.create",
                )?;

                // Count limit (five per table), same string as CreateTable's.
                // Captured from real DynamoDB (eu-west-2, 2026-08-11). A
                // delete cannot share the call to free a slot: one entry per
                // call, one action per entry, so the stored count is the
                // effective one.
                if current_vixs.len() >= 5 {
                    return Err(DynoxideError::ValidationException(
                        "One or more parameter values were invalid: VectorIndex count \
                         exceeds the per-table limit of 5"
                            .to_string(),
                    ));
                }

                // Indexes on one vector attribute must agree on dimensions.
                // The string is captured on the CreateTable path (eu-west-2,
                // 2026-08-11); the invariant is structural, so the same check
                // applies here.
                if current_vixs.iter().any(|v| {
                    v.vector_attribute.attribute_name == create.vector_attribute.attribute_name
                        && v.dimensions != create.dimensions
                }) {
                    return Err(DynoxideError::ValidationException(format!(
                        "One or more parameter values were invalid: Conflicting attribute \
                         definition for '{}'. All VectorIndexes on the same vector attribute \
                         must use the same dimensions.",
                        create.vector_attribute.attribute_name
                    )));
                }

                // The vector attribute must not be declared in
                // AttributeDefinitions, checked against the merged set since
                // the stored definitions count on UpdateTable. The string is
                // captured on the CreateTable path only (eu-west-2 and
                // us-east-1, 2026-08-12); the same rule is applied here.
                let attr = create.vector_attribute.attribute_name.as_str();
                if attr_defs.iter().any(|d| d.attribute_name == attr) {
                    return Err(DynoxideError::ValidationException(format!(
                        "One or more parameter values were invalid: Conflicting attribute \
                         definition for '{attr}'. An attribute cannot be defined in \
                         AttributeDefinitions when used as a VectorAttribute."
                    )));
                }
            }
            if let Some(ref delete) = update.delete {
                if !current_vixs
                    .iter()
                    .any(|v| v.index_name == delete.index_name)
                {
                    // Bare index name, no quoting. Captured from real DynamoDB
                    // (eu-west-2 and us-east-1, 2026-08-12).
                    return Err(DynoxideError::ResourceNotFoundException(format!(
                        "Requested resource not found: Index {} for table {}",
                        delete.index_name, request.table_name
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
        let wall = web_time::SystemTime::now()
            .duration_since(web_time::UNIX_EPOCH)
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

    // OnDemandThroughput merges member-wise over the stored ceilings, with -1
    // removing a member (captured from real DynamoDB, eu-west-2 2026-07-24).
    // The response echoes the merge with any -1 kept verbatim; the stored
    // state has it stripped, and an empty result clears the column.
    let odt_change = request.on_demand_throughput.as_ref().map(|req_odt| {
        let stored: crate::types::OnDemandThroughput = meta
            .on_demand_throughput
            .as_deref()
            .and_then(|json| serde_json::from_str(json).ok())
            .unwrap_or_default();
        let echo = crate::types::OnDemandThroughput {
            max_read_request_units: req_odt
                .max_read_request_units
                .or(stored.max_read_request_units),
            max_write_request_units: req_odt
                .max_write_request_units
                .or(stored.max_write_request_units),
        };
        let strip = |v: Option<i64>| v.filter(|&v| v != -1);
        let effective = crate::types::OnDemandThroughput {
            max_read_request_units: strip(echo.max_read_request_units),
            max_write_request_units: strip(echo.max_write_request_units),
        };
        (echo, effective)
    });

    // All validation passed; perform mutations inside a single transaction.
    helpers::with_write_transaction(storage, async {
        if let Some(ref updates) = request.global_secondary_index_updates {
            for update in updates {
                if let Some(ref create) = update.create {
                    let gsi_def = GlobalSecondaryIndex {
                        index_name: create.index_name.clone(),
                        key_schema: create.key_schema.clone(),
                        projection: create.projection.clone(),
                        provisioned_throughput: None,
                    };

                    storage
                        .create_gsi_table(&request.table_name, &create.index_name)
                        .await?;

                    let gsi_p = gsi::gsi_to_def(&gsi_def)?;
                    backfill_gsi(storage, &request.table_name, &key_schema, &gsi_p).await?;

                    current_gsis.push(gsi_def);
                }

                if let Some(ref delete) = update.delete {
                    storage
                        .drop_gsi_table(&request.table_name, &delete.index_name)
                        .await?;
                    current_gsis.retain(|g| g.index_name != delete.index_name);
                }
            }
        }

        if let Some(ref updates) = request.vector_index_updates {
            for update in updates {
                if let Some(ref create) = update.create {
                    storage
                        .create_vector_table(&request.table_name, &create.index_name)
                        .await?;

                    // Backfill runs synchronously inside this transaction and
                    // covers the items that exist as of this call; index
                    // maintenance on later writes arrives separately. The
                    // response still reports the index CREATING per the
                    // captured lifecycle walk.
                    backfill_vector_index(
                        storage,
                        &request.table_name,
                        &key_schema,
                        create,
                        &attr_defs,
                    )
                    .await?;

                    current_vixs.push(create.clone());
                }

                if let Some(ref delete) = update.delete {
                    storage
                        .drop_vector_table(&request.table_name, &delete.index_name)
                        .await?;
                    current_vixs.retain(|v| v.index_name != delete.index_name);
                }
            }
        }

        // Reconcile AttributeDefinitions to exactly the attributes still
        // referenced by the table key schema and surviving index key schemas.
        // Surviving vector indexes' SearchSchema attributes count as used, so
        // a GSI delete cannot prune an attribute a live vector schema needs.
        // See reconcile_attribute_definitions for the AWS-verified rules.
        reconcile_attribute_definitions(
            &mut attr_defs,
            &key_schema,
            &current_gsis,
            &lsi_defs,
            &current_vixs,
        );

        // Update metadata
        let attr_defs_json = serde_json::to_string(&attr_defs)
            .map_err(|e| DynoxideError::InternalServerError(e.to_string()))?;
        let gsi_json = if current_gsis.is_empty() {
            None
        } else {
            Some(
                serde_json::to_string(&current_gsis)
                    .map_err(|e| DynoxideError::InternalServerError(e.to_string()))?,
            )
        };

        storage
            .update_table_metadata(&request.table_name, &attr_defs_json, gsi_json.as_deref())
            .await?;

        // Persist vector index definitions only when this request changed
        // them, following the gsi_definitions convention (NULL when none
        // remain).
        if request
            .vector_index_updates
            .as_ref()
            .is_some_and(|u| !u.is_empty())
        {
            let vix_json = if current_vixs.is_empty() {
                None
            } else {
                Some(
                    serde_json::to_string(&current_vixs)
                        .map_err(|e| DynoxideError::InternalServerError(e.to_string()))?,
                )
            };
            storage
                .update_vector_index_definitions(&request.table_name, vix_json.as_deref())
                .await?;
        }

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
            storage
                .update_provisioned_throughput(&request.table_name, &pt_json)
                .await?;
        }

        // Handle deletion protection changes
        if let Some(enabled) = request.deletion_protection_enabled {
            storage
                .update_deletion_protection(&request.table_name, enabled)
                .await?;
        }

        // Handle table class changes
        if let Some(ref table_class) = request.table_class {
            storage
                .update_table_class(&request.table_name, table_class)
                .await?;
        }

        // Handle on-demand throughput changes
        if let Some((_, ref effective)) = odt_change {
            if effective.max_read_request_units.is_none()
                && effective.max_write_request_units.is_none()
            {
                storage
                    .clear_on_demand_throughput(&request.table_name)
                    .await?;
            } else {
                let json = serde_json::to_string(effective)
                    .map_err(|e| DynoxideError::InternalServerError(e.to_string()))?;
                storage
                    .update_on_demand_throughput(&request.table_name, &json)
                    .await?;
            }
        }

        // Handle billing mode changes
        if let Some(ref billing_mode) = request.billing_mode {
            storage
                .update_billing_mode(&request.table_name, billing_mode)
                .await?;
            if billing_mode == "PAY_PER_REQUEST" {
                // Clear provisioned throughput to avoid stale data
                storage
                    .clear_provisioned_throughput(&request.table_name)
                    .await?;
            } else if billing_mode == "PROVISIONED" {
                // Switching away from on-demand clears the stored ceilings,
                // matching real DynamoDB (eu-west-2 capture, 2026-07-24).
                storage
                    .clear_on_demand_throughput(&request.table_name)
                    .await?;
            }
        }

        // Handle stream specification changes
        if let Some(ref spec) = request.stream_specification {
            if spec.stream_enabled {
                let view_type = spec
                    .stream_view_type
                    .as_deref()
                    .unwrap_or("NEW_AND_OLD_IMAGES");
                let label = streams::generate_stream_label(storage.clock());
                storage
                    .enable_stream(&request.table_name, view_type, &label)
                    .await?;
            } else {
                storage.disable_stream(&request.table_name).await?;
            }
        }

        Ok(())
    })
    .await?;

    // Arm and disarm only once the transaction has committed, so a failed
    // update leaves no index claiming to be creating. A delete disarms whether
    // or not the index finished creating: cancelling a creating index has to
    // clear its entry, or DeleteTable's guard keeps refusing on an index that
    // no longer exists.
    if let Some(ref updates) = request.vector_index_updates {
        let now = storage.clock().now_unix_secs_f64();
        for update in updates {
            if let Some(ref create) = update.create {
                lifecycle.arm(&request.table_name, &create.index_name, now);
            }
            if let Some(ref delete) = update.delete {
                lifecycle.disarm(&request.table_name, &delete.index_name);
            }
        }
    }

    // Build response from updated metadata
    let updated_meta = helpers::require_table(storage, &request.table_name).await?;
    let vector_phases = phases_armed_on(storage, lifecycle, &request.table_name);
    let mut desc = build_table_description(&updated_meta, Some(0), Some(0), &vector_phases);

    // The UpdateTable response echoes the merged ceilings with any -1 kept
    // verbatim; only DescribeTable afterwards shows the post-removal state.
    if let Some((echo, _)) = odt_change {
        desc.on_demand_throughput = Some(echo);
    }

    // A vector index create reports the table UPDATING (captured eu-west-2,
    // 2026-08-11). The index's own CREATING comes from the phases above, the
    // same derivation every later DescribeTable reads, so this response and the
    // next description cannot disagree.
    if let Some(ref updates) = request.vector_index_updates {
        if updates.iter().any(|u| u.create.is_some()) {
            desc.table_status = "UPDATING".to_string();
        }
    }

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

/// Reconcile `attr_defs` to exactly the attributes referenced by the table key
/// schema plus every surviving index key schema. Real DynamoDB keeps the two in
/// lockstep: an attribute orphaned by a GSI delete is pruned, and an entry used
/// by no key schema is dropped rather than stored (neither is an error).
/// Verified against AWS in eu-west-2.
///
/// Surviving vector indexes' SearchSchema attributes join the used set: they
/// are declared in AttributeDefinitions like key attributes, so a GSI delete
/// must not prune an attribute a live vector schema still references. The
/// vector attribute itself never appears in AttributeDefinitions and needs no
/// entry here.
fn reconcile_attribute_definitions(
    attr_defs: &mut Vec<AttributeDefinition>,
    key_schema: &helpers::KeySchema,
    gsis: &[GlobalSecondaryIndex],
    lsi_defs: &[crate::actions::lsi::LsiDef],
    vixs: &[VectorIndex],
) {
    let mut used: std::collections::HashSet<&str> = std::collections::HashSet::new();
    used.insert(key_schema.partition_key.as_str());
    if let Some(ref sk) = key_schema.sort_key {
        used.insert(sk.as_str());
    }
    for g in gsis {
        for k in &g.key_schema {
            used.insert(k.attribute_name.as_str());
        }
    }
    for lsi in lsi_defs {
        used.insert(lsi.pk_attr.as_str());
        if let Some(ref sk) = lsi.sk_attr {
            used.insert(sk.as_str());
        }
    }
    for vix in vixs {
        if let Some(ref schema) = vix.search_schema {
            for elem in schema {
                used.insert(elem.attribute_name.as_str());
            }
        }
    }
    attr_defs.retain(|d| used.contains(d.attribute_name.as_str()));
}

/// Request-model constraint errors for `GlobalSecondaryIndexUpdates` Update
/// actions, shared between the operation-layer validation and the raw parse
/// path's cross-family envelope.
fn collect_gsi_update_errors(updates: &[GlobalSecondaryIndexUpdate], errors: &mut Vec<String>) {
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
        collect_gsi_update_errors(updates, &mut errors);
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

    // TableClass enum validation (mirrors CreateTable)
    if let Some(ref tc) = request.table_class {
        if tc != "STANDARD" && tc != "STANDARD_INFREQUENT_ACCESS" {
            return Err(DynoxideError::ValidationException(format!(
                "1 validation error detected: Value '{tc}' at 'tableClass' failed to satisfy \
                 constraint: Member must satisfy enum value set: \
                 [STANDARD, STANDARD_INFREQUENT_ACCESS]"
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

    // "At least one of ...": a request must change something. A lone
    // TableClass, OnDemandThroughput, or DeletionProtectionEnabled counts, the
    // same as a throughput/billing/stream change. An empty
    // GlobalSecondaryIndexUpdates array is treated as "no GSI change" rather
    // than satisfying the requirement on its own.
    let no_config_change = request.provisioned_throughput.is_none()
        && request.billing_mode.is_none()
        && request.stream_specification.is_none()
        && request.deletion_protection_enabled.is_none()
        && request.table_class.is_none()
        && request.on_demand_throughput.is_none();
    let no_gsi_change = request
        .global_secondary_index_updates
        .as_ref()
        .is_none_or(|updates| updates.is_empty());
    let no_vector_change = request
        .vector_index_updates
        .as_ref()
        .is_none_or(|updates| updates.is_empty());
    if no_gsi_change && no_config_change && no_vector_change {
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

    // Validate vector index update structural constraints. An entry carrying
    // neither action mirrors the GSI structural message (with the two actions
    // the vector family has); this shape is not captured.
    if let Some(ref updates) = request.vector_index_updates {
        for update in updates {
            if update.create.is_none() && update.delete.is_none() {
                return Err(DynoxideError::ValidationException(
                    "One or more parameter values were invalid: One of \
                     VectorIndexUpdate.Create, VectorIndexUpdate.Delete must not be null"
                        .to_string(),
                ));
            }
            // A single entry cannot carry both actions: a per-object
            // structural rule with its own string, distinct from the
            // one-action-per-call LimitExceededException. Captured from real
            // DynamoDB (eu-west-2 and us-east-1, 2026-08-12).
            if update.create.is_some() && update.delete.is_some() {
                return Err(DynoxideError::ValidationException(
                    "One or more parameter values were invalid: Only one vector index \
                     action is allowed per VectorIndexUpdate object"
                        .to_string(),
                ));
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
async fn backfill_gsi<S: StorageBackend>(
    storage: &S,
    table_name: &str,
    key_schema: &helpers::KeySchema,
    gsi_def: &gsi::GsiDef,
) -> Result<()> {
    const BATCH_SIZE: usize = 1000;
    let mut last_pk: Option<String> = None;
    let mut last_sk: Option<String> = None;

    loop {
        let items = storage
            .scan_items(
                table_name,
                &crate::storage::ScanParams {
                    limit: Some(BATCH_SIZE),
                    exclusive_start_pk: last_pk.as_deref(),
                    exclusive_start_sk: last_sk.as_deref(),
                    ..Default::default()
                },
            )
            .await?;

        if items.is_empty() {
            break;
        }

        let mut rows: Vec<crate::storage_backend::GsiItemRow> = Vec::new();
        for (pk, sk, item_json) in &items {
            let item: crate::types::Item = serde_json::from_str(item_json)
                .map_err(|e| DynoxideError::InternalServerError(format!("Bad item JSON: {e}")))?;

            // Backfill only the items that belong in this index (sparse).
            if let Some((gsi_pk, gsi_sk)) = gsi_def.index_key_strings(&item) {
                let projected = gsi::build_index_item(
                    &item,
                    gsi_def,
                    &key_schema.partition_key,
                    key_schema.sort_key.as_deref(),
                );
                let projected_json = serde_json::to_string(&projected)
                    .map_err(|e| DynoxideError::InternalServerError(e.to_string()))?;

                rows.push(crate::storage_backend::GsiItemRow {
                    gsi_pk,
                    gsi_sk,
                    table_pk: pk.clone(),
                    table_sk: sk.clone(),
                    item_json: projected_json,
                });
            }
        }

        storage
            .insert_gsi_items(table_name, &gsi_def.index_name, &rows)
            .await?;

        let last = &items[items.len() - 1];
        last_pk = Some(last.0.clone());
        last_sk = Some(last.1.clone());

        if items.len() < BATCH_SIZE {
            break;
        }
    }

    Ok(())
}

/// Backfill existing items into a newly created vector index, processing in
/// batches like [`backfill_gsi`].
///
/// Items holding values a live write would reject are skipped silently: the
/// index goes active without them, while re-putting the same item once the
/// index exists is rejected. That asymmetry is real DynamoDB's behaviour,
/// captured in eu-west-2 and us-east-1 on 2026-08-12.
async fn backfill_vector_index<S: StorageBackend>(
    storage: &S,
    table_name: &str,
    key_schema: &helpers::KeySchema,
    vix: &VectorIndex,
    attr_defs: &[AttributeDefinition],
) -> Result<()> {
    const BATCH_SIZE: usize = 1000;
    let mut last_pk: Option<String> = None;
    let mut last_sk: Option<String> = None;

    loop {
        let items = storage
            .scan_items(
                table_name,
                &crate::storage::ScanParams {
                    limit: Some(BATCH_SIZE),
                    exclusive_start_pk: last_pk.as_deref(),
                    exclusive_start_sk: last_sk.as_deref(),
                    ..Default::default()
                },
            )
            .await?;

        if items.is_empty() {
            break;
        }

        let mut rows: Vec<crate::storage_backend::VectorItemRow> = Vec::new();
        for (pk, sk, item_json) in &items {
            let item: Item = serde_json::from_str(item_json)
                .map_err(|e| DynoxideError::InternalServerError(format!("Bad item JSON: {e}")))?;

            if let Some(row) = super::vector_index::vector_index_row(
                &item,
                vix,
                &key_schema.partition_key,
                key_schema.sort_key.as_deref(),
                attr_defs,
                pk,
                sk,
            )? {
                rows.push(row);
            }
        }

        storage
            .insert_vector_items(table_name, &vix.index_name, &rows)
            .await?;

        let last = &items[items.len() - 1];
        last_pk = Some(last.0.clone());
        last_sk = Some(last.1.clone());

        if items.len() < BATCH_SIZE {
            break;
        }
    }

    Ok(())
}
