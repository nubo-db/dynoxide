//! OneTable v1.1.0 schema parser.
//!
//! Parses a OneTable schema JSON file into a [`DataModel`].

use std::collections::HashMap;
use std::path::Path;

use super::{DataModel, EntityDefinition, GsiMapping};

/// Metadata for a GSI index parsed from OneTable's `indexes` section.
struct IndexDef {
    /// The DynamoDB-facing name (from `name` field, or the OneTable key as fallback).
    dynamo_name: String,
    /// The attribute name used as the hash key (e.g. "gs1pk").
    hash_attr: String,
    /// The attribute name used as the sort key (e.g. "gs1sk"), if any.
    sort_attr: Option<String>,
}

/// Parse a OneTable v1.1.0 schema file into a [`DataModel`].
///
/// The parser:
/// 1. Validates the `format` field is `"onetable:1.1.0"`
/// 2. Reads `params.typeField` (defaults to `"_type"`)
/// 3. Reads `indexes` to build index definitions with DynamoDB name resolution
/// 4. For each model in `models`, extracts pk/sk templates and resolves GSI
///    participation by matching attribute names against index key names
pub fn parse_onetable_file(path: &Path) -> Result<DataModel, String> {
    let contents = std::fs::read_to_string(path)
        .map_err(|e| format!("data model file not found: {}: {e}", path.display()))?;

    parse_onetable(&contents)
}

/// Parse a OneTable v1.1.0 schema from a JSON string.
pub fn parse_onetable(json: &str) -> Result<DataModel, String> {
    let doc: serde_json::Value = serde_json::from_str(json)
        .map_err(|e| format!("data model file is not valid JSON: {e}"))?;

    let format = doc
        .get("format")
        .and_then(|v| v.as_str())
        .ok_or("data model file missing \"format\" field — is this a OneTable schema?")?;

    if format != "onetable:1.1.0" {
        return Err(format!(
            "data model file is not a OneTable schema (expected format \"onetable:1.1.0\", got \"{format}\")"
        ));
    }

    // Extract type attribute (default: "_type")
    let type_attribute = doc
        .pointer("/params/typeField")
        .and_then(|v| v.as_str())
        .unwrap_or("_type")
        .to_string();

    // Parse index definitions (skip "primary")
    let indexes = parse_indexes(&doc);

    // Parse models into entity definitions
    let entities = parse_models(&doc, &type_attribute, &indexes);

    Ok(DataModel {
        schema_format: format.to_string(),
        type_attribute,
        entities,
    })
}

/// Parse the `indexes` section into a map of OneTable key -> IndexDef.
/// Skips the "primary" index.
fn parse_indexes(doc: &serde_json::Value) -> HashMap<String, IndexDef> {
    let mut indexes = HashMap::new();

    let Some(idx_obj) = doc.get("indexes").and_then(|v| v.as_object()) else {
        return indexes;
    };

    for (key, def) in idx_obj {
        if key == "primary" {
            continue;
        }

        let Some(hash_attr) = def.get("hash").and_then(|v| v.as_str()) else {
            continue;
        };

        let sort_attr = def.get("sort").and_then(|v| v.as_str()).map(String::from);

        // Resolve DynamoDB name: explicit `name` field, or fall back to the OneTable key
        let dynamo_name = def
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or(key)
            .to_string();

        indexes.insert(
            key.clone(),
            IndexDef {
                dynamo_name,
                hash_attr: hash_attr.to_string(),
                sort_attr,
            },
        );
    }

    indexes
}

/// Parse the `models` section into entity definitions.
fn parse_models(
    doc: &serde_json::Value,
    type_attribute: &str,
    indexes: &HashMap<String, IndexDef>,
) -> Vec<EntityDefinition> {
    let Some(models_obj) = doc.get("models").and_then(|v| v.as_object()) else {
        return Vec::new();
    };

    let mut entities: Vec<EntityDefinition> = models_obj
        .iter()
        .map(|(name, model)| parse_single_model(name, model, type_attribute, indexes))
        .collect();

    // Sort alphabetically for deterministic output
    entities.sort_by(|a, b| a.name.cmp(&b.name));
    entities
}

/// Parse a single model definition into an EntityDefinition.
fn parse_single_model(
    name: &str,
    model: &serde_json::Value,
    type_attribute: &str,
    indexes: &HashMap<String, IndexDef>,
) -> EntityDefinition {
    let attrs = model.as_object();

    // Extract primary key templates from "pk" and "sk" attributes
    let pk_template = attrs
        .and_then(|m| m.get("pk"))
        .and_then(|v| v.get("value"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let sk_template = attrs
        .and_then(|m| m.get("sk"))
        .and_then(|v| v.get("value"))
        .and_then(|v| v.as_str())
        .map(String::from);

    // Resolve GSI participation by checking if this model defines attributes
    // matching any index's hash key attribute name
    let gsi_mappings = resolve_gsi_mappings(attrs, indexes);

    // Check for entity-level type attribute override
    let entity_type_attr = attrs
        .and_then(|m| m.get("typeField"))
        .and_then(|v| v.as_str())
        .map(String::from);

    // Use entity-level override, or inherit global (stored for agent clarity)
    let effective_type_attr = entity_type_attr.unwrap_or_else(|| type_attribute.to_string());

    EntityDefinition {
        name: name.to_string(),
        pk_template,
        sk_template,
        type_attribute: Some(effective_type_attr),
        gsi_mappings,
        description: None,
    }
}

/// Resolve which GSIs a model participates in by matching its attributes
/// against index hash/sort key attribute names.
fn resolve_gsi_mappings(
    attrs: Option<&serde_json::Map<String, serde_json::Value>>,
    indexes: &HashMap<String, IndexDef>,
) -> Vec<GsiMapping> {
    let Some(attrs) = attrs else {
        return Vec::new();
    };

    let mut mappings: Vec<GsiMapping> = indexes
        .iter()
        .filter_map(|(_, idx_def)| {
            // Check if this model has the hash key attribute with a value template
            let pk_template = attrs
                .get(&idx_def.hash_attr)
                .and_then(|v| v.get("value"))
                .and_then(|v| v.as_str())?;

            // Check for sort key template (optional)
            let sk_template = idx_def.sort_attr.as_ref().and_then(|sort_attr| {
                attrs
                    .get(sort_attr)
                    .and_then(|v| v.get("value"))
                    .and_then(|v| v.as_str())
                    .map(String::from)
            });

            Some(GsiMapping {
                index_name: idx_def.dynamo_name.clone(),
                pk_template: pk_template.to_string(),
                sk_template,
            })
        })
        .collect();

    // Sort by index name for deterministic output
    mappings.sort_by(|a, b| a.index_name.cmp(&b.index_name));
    mappings
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID_SCHEMA: &str = r#"{
        "version": "0.1.0",
        "format": "onetable:1.1.0",
        "indexes": {
            "primary": { "hash": "pk", "sort": "sk" },
            "gs1": { "hash": "gs1pk", "sort": "gs1sk", "project": "all", "name": "GSI1" },
            "gs2": { "hash": "gs2pk", "sort": "gs2sk", "project": "keys", "name": "GSI2" }
        },
        "params": {
            "typeField": "_type",
            "isoDates": true
        },
        "models": {
            "Account": {
                "pk":    { "type": "string", "value": "account#${id}" },
                "sk":    { "type": "string", "value": "account#" },
                "id":    { "type": "string", "uuid": "ulid" },
                "name":  { "type": "string", "required": true }
            },
            "User": {
                "pk":    { "type": "string", "value": "account#${accountId}" },
                "sk":    { "type": "string", "value": "user#${email}" },
                "gs1pk": { "type": "string", "value": "user#${email}" },
                "gs1sk": { "type": "string", "value": "user#" },
                "id":    { "type": "string", "uuid": "ulid" },
                "email": { "type": "string", "required": true }
            },
            "Project": {
                "pk":    { "type": "string", "value": "account#${accountId}" },
                "sk":    { "type": "string", "value": "project#${id}" },
                "gs1pk": { "type": "string", "value": "project#${id}" },
                "gs1sk": { "type": "string", "value": "project#" },
                "gs2pk": { "type": "string", "value": "account#${accountId}" },
                "gs2sk": { "type": "string", "value": "project#${status}#${name}" },
                "id":    { "type": "string", "uuid": "ulid" }
            },
            "Task": {
                "pk":    { "type": "string", "value": "project#${projectId}" },
                "sk":    { "type": "string", "value": "task#${id}" },
                "gs1pk": { "type": "string", "value": "user#${assigneeEmail}" },
                "gs1sk": { "type": "string", "value": "task#${dueDate}" },
                "id":    { "type": "string", "uuid": "ulid" }
            }
        }
    }"#;

    #[test]
    fn parse_valid_schema() {
        let model = parse_onetable(VALID_SCHEMA).unwrap();

        assert_eq!(model.schema_format, "onetable:1.1.0");
        assert_eq!(model.type_attribute, "_type");
        assert_eq!(model.entities.len(), 4);

        // Entities should be sorted alphabetically
        assert_eq!(model.entities[0].name, "Account");
        assert_eq!(model.entities[1].name, "Project");
        assert_eq!(model.entities[2].name, "Task");
        assert_eq!(model.entities[3].name, "User");
    }

    #[test]
    fn account_has_no_gsis() {
        let model = parse_onetable(VALID_SCHEMA).unwrap();
        let account = &model.entities[0];

        assert_eq!(account.name, "Account");
        assert_eq!(account.pk_template, "account#${id}");
        assert_eq!(account.sk_template.as_deref(), Some("account#"));
        assert!(account.gsi_mappings.is_empty());
    }

    #[test]
    fn user_has_one_gsi() {
        let model = parse_onetable(VALID_SCHEMA).unwrap();
        let user = &model.entities[3];

        assert_eq!(user.name, "User");
        assert_eq!(user.gsi_mappings.len(), 1);
        assert_eq!(user.gsi_mappings[0].index_name, "GSI1");
        assert_eq!(user.gsi_mappings[0].pk_template, "user#${email}");
        assert_eq!(user.gsi_mappings[0].sk_template.as_deref(), Some("user#"));
    }

    #[test]
    fn project_has_two_gsis() {
        let model = parse_onetable(VALID_SCHEMA).unwrap();
        let project = &model.entities[1];

        assert_eq!(project.name, "Project");
        assert_eq!(project.gsi_mappings.len(), 2);
        // Sorted by index_name
        assert_eq!(project.gsi_mappings[0].index_name, "GSI1");
        assert_eq!(project.gsi_mappings[1].index_name, "GSI2");
        assert_eq!(
            project.gsi_mappings[1].sk_template.as_deref(),
            Some("project#${status}#${name}")
        );
    }

    #[test]
    fn index_name_resolution_with_name_field() {
        // gs1 has name: "GSI1", gs2 has name: "GSI2"
        let model = parse_onetable(VALID_SCHEMA).unwrap();
        let user = &model.entities[3];
        assert_eq!(user.gsi_mappings[0].index_name, "GSI1");
    }

    #[test]
    fn index_name_resolution_fallback_to_key() {
        let schema = r#"{
            "format": "onetable:1.1.0",
            "indexes": {
                "primary": { "hash": "pk", "sort": "sk" },
                "gs1": { "hash": "gs1pk", "sort": "gs1sk" }
            },
            "models": {
                "Foo": {
                    "pk":    { "type": "string", "value": "foo#${id}" },
                    "sk":    { "type": "string", "value": "foo#" },
                    "gs1pk": { "type": "string", "value": "foo#${bar}" },
                    "gs1sk": { "type": "string", "value": "foo#" }
                }
            }
        }"#;

        let model = parse_onetable(schema).unwrap();
        // No "name" field on gs1, so should fall back to "gs1"
        assert_eq!(model.entities[0].gsi_mappings[0].index_name, "gs1");
    }

    #[test]
    fn type_attribute_defaults_to_underscore_type() {
        let schema = r#"{
            "format": "onetable:1.1.0",
            "indexes": { "primary": { "hash": "pk", "sort": "sk" } },
            "models": {}
        }"#;

        let model = parse_onetable(schema).unwrap();
        assert_eq!(model.type_attribute, "_type");
    }

    #[test]
    fn custom_type_attribute() {
        let schema = r#"{
            "format": "onetable:1.1.0",
            "indexes": { "primary": { "hash": "pk", "sort": "sk" } },
            "params": { "typeField": "type" },
            "models": {}
        }"#;

        let model = parse_onetable(schema).unwrap();
        assert_eq!(model.type_attribute, "type");
    }

    #[test]
    fn entity_inherits_global_type_attribute() {
        let model = parse_onetable(VALID_SCHEMA).unwrap();
        assert_eq!(model.entities[0].type_attribute.as_deref(), Some("_type"));
    }

    #[test]
    fn zero_models_accepted() {
        let schema = r#"{
            "format": "onetable:1.1.0",
            "indexes": { "primary": { "hash": "pk", "sort": "sk" } },
            "models": {}
        }"#;

        let model = parse_onetable(schema).unwrap();
        assert!(model.entities.is_empty());
    }

    #[test]
    fn missing_format_field() {
        let schema = r#"{ "models": {} }"#;
        let err = parse_onetable(schema).unwrap_err();
        assert!(err.contains("missing \"format\" field"));
    }

    #[test]
    fn wrong_format() {
        let schema = r#"{ "format": "onetable:2.0.0", "models": {} }"#;
        let err = parse_onetable(schema).unwrap_err();
        assert!(err.contains("expected format \"onetable:1.1.0\""));
        assert!(err.contains("got \"onetable:2.0.0\""));
    }

    #[test]
    fn invalid_json() {
        let err = parse_onetable("not json").unwrap_err();
        assert!(err.contains("not valid JSON"));
    }

    #[test]
    fn orphaned_gsi_attribute_skipped_gracefully() {
        // Model has gs3pk/gs3sk attributes but no gs3 index is defined
        let schema = r#"{
            "format": "onetable:1.1.0",
            "indexes": {
                "primary": { "hash": "pk", "sort": "sk" },
                "gs1": { "hash": "gs1pk", "sort": "gs1sk", "name": "GSI1" }
            },
            "models": {
                "Widget": {
                    "pk":    { "type": "string", "value": "widget#${id}" },
                    "sk":    { "type": "string", "value": "widget#" },
                    "gs1pk": { "type": "string", "value": "widget#${id}" },
                    "gs1sk": { "type": "string", "value": "widget#" },
                    "gs3pk": { "type": "string", "value": "orphaned#${id}" },
                    "gs3sk": { "type": "string", "value": "orphaned#" }
                }
            }
        }"#;

        let model = parse_onetable(schema).unwrap();
        let widget = &model.entities[0];
        // Only gs1/GSI1 should be resolved; gs3pk/gs3sk are orphaned and ignored
        assert_eq!(widget.gsi_mappings.len(), 1);
        assert_eq!(widget.gsi_mappings[0].index_name, "GSI1");
    }

    #[test]
    fn file_not_found() {
        let err = parse_onetable_file(Path::new("/nonexistent/schema.json")).unwrap_err();
        assert!(err.contains("data model file not found"));
    }

    #[test]
    fn instructions_summary_basic() {
        let model = parse_onetable(VALID_SCHEMA).unwrap();
        let summary = model.instructions_summary(20).unwrap();

        assert!(summary.contains("onetable:1.1.0"));
        assert!(summary.contains("4 entities"));
        assert!(summary.contains("_type"));
        assert!(summary.contains("Account"));
        assert!(summary.contains("GSI1"));
        assert!(summary.contains("get_database_info"));
    }

    #[test]
    fn instructions_summary_truncated() {
        let model = parse_onetable(VALID_SCHEMA).unwrap();
        let summary = model.instructions_summary(2).unwrap();

        // Should show first 2 entities and "...and 2 more"
        assert!(summary.contains("...and 2 more"));
    }

    #[test]
    fn instructions_summary_suppressed_at_zero() {
        let model = parse_onetable(VALID_SCHEMA).unwrap();
        assert!(model.instructions_summary(0).is_none());
    }
}
