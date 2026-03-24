//! Application-level data model definitions for MCP agent context.
//!
//! Parses schema files (e.g., OneTable) into a [`DataModel`] that can be
//! exposed to MCP-connected agents via instructions and `get_database_info`.

pub mod onetable;

use serde::Serialize;

/// Application-level data model parsed from a schema file (e.g., OneTable).
/// Designed for agent consumption — serializes to JSON for MCP responses.
#[derive(Debug, Clone, Serialize)]
pub struct DataModel {
    /// Schema format identifier, e.g. "onetable:1.1.0"
    pub schema_format: String,
    /// The attribute name used to discriminate entity types (e.g. "_type")
    pub type_attribute: String,
    /// Entity definitions with key templates and GSI mappings
    pub entities: Vec<EntityDefinition>,
}

/// A single entity type within the data model.
#[derive(Debug, Clone, Serialize)]
pub struct EntityDefinition {
    /// Entity name, e.g. "Account"
    pub name: String,
    /// Primary key partition template, e.g. "account#${id}"
    pub pk_template: String,
    /// Primary key sort template, e.g. "account#"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sk_template: Option<String>,
    /// Entity-level type attribute override (usually same as DataModel.type_attribute)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub type_attribute: Option<String>,
    /// Which GSIs this entity participates in, with key templates
    pub gsi_mappings: Vec<GsiMapping>,
    /// Human-readable description from schema (if present)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// A GSI mapping for an entity, describing the key templates used.
#[derive(Debug, Clone, Serialize)]
pub struct GsiMapping {
    /// DynamoDB index name resolved from the schema's index definitions.
    ///
    /// For OneTable schemas, this is resolved from the `name` field in the
    /// index definition if present, otherwise falls back to the OneTable key
    /// (e.g. "gs1"). Must match the name from CreateTable / describe_table
    /// so agents can pass it directly to query's `index_name` parameter.
    pub index_name: String,
    /// GSI partition key template
    pub pk_template: String,
    /// GSI sort key template (if the index has a sort key)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sk_template: Option<String>,
}

impl DataModel {
    /// Generate a compact summary for MCP instructions.
    ///
    /// Returns entity names with their GSI participation, truncated to `limit`.
    /// If `limit` is 0, returns `None` (summary suppressed).
    pub fn instructions_summary(&self, limit: usize) -> Option<String> {
        if limit == 0 {
            return None;
        }

        let entity_count = self.entities.len();
        let shown = self.entities.iter().take(limit);

        let entity_parts: Vec<String> = shown
            .map(|e| {
                if e.gsi_mappings.is_empty() {
                    e.name.clone()
                } else {
                    let gsis: Vec<&str> = e
                        .gsi_mappings
                        .iter()
                        .map(|g| g.index_name.as_str())
                        .collect();
                    format!("{} ({})", e.name, gsis.join(", "))
                }
            })
            .collect();

        let mut summary = format!(
            "## Data model\n\n\
             Schema: {} ({} entities, type attribute: \"{}\")\n\
             Entities: {}",
            self.schema_format,
            entity_count,
            self.type_attribute,
            entity_parts.join(", "),
        );

        if entity_count > limit {
            summary.push_str(&format!("...and {} more", entity_count - limit));
        }

        summary.push_str(
            "\n\nCall get_database_info for full entity definitions with key templates.\n\
             Note: Data model definitions describe the intended schema but are not enforced. \
             Actual database contents may differ.",
        );

        Some(summary)
    }
}
