//! TOML configuration parsing and upfront validation for import anonymisation rules.

use serde::Deserialize;
use std::collections::HashMap;
use std::path::Path;

use crate::expressions::condition::{self, ConditionExpr};
use crate::types::AttributeValue;

/// Top-level import configuration parsed from TOML.
#[derive(Debug, Deserialize)]
pub struct ImportConfig {
    /// Anonymisation rules applied to each item.
    #[serde(default)]
    pub rules: Vec<RuleConfig>,

    /// Consistency configuration for cross-table referential integrity.
    #[serde(default)]
    pub consistency: Option<ConsistencyConfig>,
}

/// A single anonymisation rule from TOML.
#[derive(Debug, Deserialize)]
pub struct RuleConfig {
    /// DynamoDB ConditionExpression syntax to match items.
    /// e.g. `attribute_exists(email)` or `begins_with(pk, 'USER#')`
    #[serde(rename = "match")]
    pub match_expr: String,

    /// Attribute path to transform (supports dot notation: `address.city`).
    pub path: String,

    /// The anonymisation action to apply.
    pub action: ActionConfig,
}

/// Anonymisation action types.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ActionConfig {
    /// Replace with fake data from a generator.
    Fake {
        /// Generator name: `safe_email`, `name`, `phone_number`, `address`,
        /// `company_name`, `sentence`, `word`, `first_name`, `last_name`.
        generator: String,
    },
    /// Mask characters, keeping the last N.
    Mask {
        #[serde(default = "default_keep_last")]
        keep_last: usize,
        #[serde(default = "default_mask_char")]
        mask_char: String,
    },
    /// One-way SHA-256 hash with salt from environment variable.
    Hash {
        /// Environment variable name containing the salt.
        salt_env: Option<String>,
    },
    /// Replace with a fixed redacted string.
    Redact,
    /// Replace with NULL.
    Null,
}

fn default_keep_last() -> usize {
    4
}
fn default_mask_char() -> String {
    "*".to_string()
}

/// Consistency configuration.
#[derive(Debug, Deserialize)]
pub struct ConsistencyConfig {
    /// Field names that should produce consistent anonymised values across tables.
    pub fields: Vec<String>,
}

/// A validated, ready-to-execute rule.
#[derive(Debug)]
pub struct ValidatedRule {
    /// Parsed condition expression.
    pub condition: ConditionExpr,
    /// Parsed path elements for navigating into items.
    pub path: Vec<crate::expressions::PathElement>,
    /// The action to apply.
    pub action: ValidatedAction,
}

/// A secret salt value with redacted Debug output.
///
/// Wraps the raw salt bytes to prevent accidental leakage through
/// `Debug` formatting (logs, panics, `dbg!()` calls). The salt exists
/// specifically to prevent rainbow table attacks — `#[derive(Debug)]`
/// on the raw bytes would undo that protection.
#[derive(Clone)]
pub struct Salt(Vec<u8>);

impl Salt {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl std::fmt::Debug for Salt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Salt([REDACTED])")
    }
}

/// A validated action with resolved values (e.g., salt from env).
#[derive(Debug, Clone)]
pub enum ValidatedAction {
    Fake { generator: String },
    Mask { keep_last: usize, mask_char: char },
    Hash { salt: Salt },
    Redact,
    Null,
}

/// Parse and validate the TOML config file.
///
/// All rules are validated upfront before any processing begins:
/// - Match expressions are parsed
/// - Generator names are checked
/// - Environment variables are resolved
/// - Paths are parsed
pub fn load_and_validate(
    path: &Path,
) -> Result<(Vec<ValidatedRule>, Option<ConsistencyConfig>), String> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| format!("Failed to read config file {}: {e}", path.display()))?;

    let config: ImportConfig =
        toml::from_str(&content).map_err(|e| format!("Failed to parse TOML config: {e}"))?;

    let mut validated = Vec::with_capacity(config.rules.len());

    for (i, rule) in config.rules.iter().enumerate() {
        let condition = condition::parse(&rule.match_expr).map_err(|e| {
            format!(
                "Rule {}: invalid match expression '{}': {e}",
                i + 1,
                rule.match_expr
            )
        })?;

        let path = parse_path(&rule.path)
            .map_err(|e| format!("Rule {}: invalid path '{}': {e}", i + 1, rule.path))?;

        let action = validate_action(&rule.action, i + 1)?;

        validated.push(ValidatedRule {
            condition,
            path,
            action,
        });
    }

    Ok((validated, config.consistency))
}

/// Parse a dot-notation path into PathElements.
/// Supports: `email`, `address.city`, `items[0].name`
fn parse_path(path: &str) -> Result<Vec<crate::expressions::PathElement>, String> {
    use crate::expressions::PathElement;

    if path.is_empty() {
        return Err("empty path".to_string());
    }

    let mut elements = Vec::new();
    for part in path.split('.') {
        if part.is_empty() {
            return Err("empty path segment".to_string());
        }

        // Handle array indexing: `items[0]`
        if let Some(bracket_pos) = part.find('[') {
            let name = &part[..bracket_pos];
            if !name.is_empty() {
                elements.push(PathElement::Attribute(name.to_string()));
            }

            let rest = &part[bracket_pos..];
            let mut remaining = rest;
            while remaining.starts_with('[') {
                let end = remaining
                    .find(']')
                    .ok_or_else(|| format!("unclosed bracket in path: {path}"))?;
                let idx: usize = remaining[1..end]
                    .parse()
                    .map_err(|_| format!("invalid array index in path: {path}"))?;
                elements.push(PathElement::Index(idx));
                remaining = &remaining[end + 1..];
            }
        } else {
            elements.push(PathElement::Attribute(part.to_string()));
        }
    }

    Ok(elements)
}

const VALID_GENERATORS: &[&str] = &[
    "safe_email",
    "name",
    "phone_number",
    "address",
    "company_name",
    "sentence",
    "word",
    "first_name",
    "last_name",
];

fn validate_action(action: &ActionConfig, rule_num: usize) -> Result<ValidatedAction, String> {
    match action {
        ActionConfig::Fake { generator } => {
            if !VALID_GENERATORS.contains(&generator.as_str()) {
                return Err(format!(
                    "Rule {rule_num}: unknown generator '{}'. Valid generators: {}",
                    generator,
                    VALID_GENERATORS.join(", ")
                ));
            }
            Ok(ValidatedAction::Fake {
                generator: generator.clone(),
            })
        }
        ActionConfig::Mask {
            keep_last,
            mask_char,
        } => {
            let ch = mask_char
                .chars()
                .next()
                .ok_or_else(|| format!("Rule {rule_num}: mask_char must not be empty"))?;
            if mask_char.chars().count() > 1 {
                return Err(format!(
                    "Rule {rule_num}: mask_char must be a single character"
                ));
            }
            Ok(ValidatedAction::Mask {
                keep_last: *keep_last,
                mask_char: ch,
            })
        }
        ActionConfig::Hash { salt_env } => {
            let salt = match salt_env {
                Some(env_var) => {
                    std::env::var(env_var).map_err(|_| {
                        format!(
                            "Rule {rule_num}: environment variable '{env_var}' not set (required for hash salt)"
                        )
                    })?.into_bytes()
                }
                None => {
                    return Err(format!(
                        "Rule {rule_num}: salt_env is required for hash actions. \
                         SHA-256 without a salt is trivially reversible via rainbow tables. \
                         Set salt_env to an environment variable containing a secret salt value."
                    ));
                }
            };
            Ok(ValidatedAction::Hash {
                salt: Salt::new(salt),
            })
        }
        ActionConfig::Redact => Ok(ValidatedAction::Redact),
        ActionConfig::Null => Ok(ValidatedAction::Null),
    }
}

/// Evaluate a match expression against an item.
///
/// ## Supported expressions
///
/// Match expressions use DynamoDB ConditionExpression syntax. The following
/// functions work without expression attribute values:
///
/// - `attribute_exists(path)` — matches if the attribute is present
/// - `attribute_not_exists(path)` — matches if the attribute is absent
/// - `attribute_type(path, type)` — matches if the attribute is the given type
/// - Boolean operators: `AND`, `OR`, `NOT`
///
/// ## Known limitation
///
/// Functions that require string literal arguments (e.g., `begins_with(pk, 'USER#')`)
/// are **not supported** because the condition parser expects `:val` expression
/// attribute value references, not inline string literals. String literal support
/// is tracked as a follow-up task.
pub fn matches_item(rule: &ValidatedRule, item: &HashMap<String, AttributeValue>) -> bool {
    crate::expressions::evaluate_without_tracking(&rule.condition, item, &None, &None)
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_path() {
        let path = parse_path("email").unwrap();
        assert_eq!(path.len(), 1);
        assert_eq!(
            path[0],
            crate::expressions::PathElement::Attribute("email".to_string())
        );
    }

    #[test]
    fn test_parse_nested_path() {
        let path = parse_path("address.city").unwrap();
        assert_eq!(path.len(), 2);
        assert_eq!(
            path[0],
            crate::expressions::PathElement::Attribute("address".to_string())
        );
        assert_eq!(
            path[1],
            crate::expressions::PathElement::Attribute("city".to_string())
        );
    }

    #[test]
    fn test_parse_indexed_path() {
        let path = parse_path("items[0].name").unwrap();
        assert_eq!(path.len(), 3);
        assert_eq!(
            path[0],
            crate::expressions::PathElement::Attribute("items".to_string())
        );
        assert_eq!(path[1], crate::expressions::PathElement::Index(0));
        assert_eq!(
            path[2],
            crate::expressions::PathElement::Attribute("name".to_string())
        );
    }

    #[test]
    fn test_empty_path_error() {
        assert!(parse_path("").is_err());
    }

    #[test]
    fn test_validate_fake_action() {
        let action = ActionConfig::Fake {
            generator: "safe_email".to_string(),
        };
        assert!(validate_action(&action, 1).is_ok());
    }

    #[test]
    fn test_validate_fake_unknown_generator() {
        let action = ActionConfig::Fake {
            generator: "unknown".to_string(),
        };
        assert!(validate_action(&action, 1).is_err());
    }

    #[test]
    fn test_validate_mask_action() {
        let action = ActionConfig::Mask {
            keep_last: 4,
            mask_char: "*".to_string(),
        };
        let result = validate_action(&action, 1).unwrap();
        match result {
            ValidatedAction::Mask {
                keep_last,
                mask_char,
            } => {
                assert_eq!(keep_last, 4);
                assert_eq!(mask_char, '*');
            }
            _ => panic!("expected Mask"),
        }
    }

    #[test]
    fn test_validate_hash_no_salt_rejected() {
        let action = ActionConfig::Hash { salt_env: None };
        let err = validate_action(&action, 1).unwrap_err();
        assert!(err.contains("salt_env is required"));
    }

    #[test]
    fn test_salt_redacted_in_debug_output() {
        let salt = Salt::new(b"super-secret-value".to_vec());
        let debug_str = format!("{:?}", salt);
        assert_eq!(debug_str, "Salt([REDACTED])");
        assert!(!debug_str.contains("super"));
        assert!(!debug_str.contains("secret"));

        // Verify redaction is transitive through ValidatedAction and ValidatedRule
        let action = ValidatedAction::Hash { salt };
        let action_debug = format!("{:?}", action);
        assert!(action_debug.contains("[REDACTED]"));
        assert!(!action_debug.contains("super"));

        let rule = ValidatedRule {
            condition: crate::expressions::condition::parse("attribute_exists(email)").unwrap(),
            path: vec![crate::expressions::PathElement::Attribute(
                "email".to_string(),
            )],
            action,
        };
        let rule_debug = format!("{:?}", rule);
        assert!(rule_debug.contains("[REDACTED]"));
        assert!(!rule_debug.contains("super"));
    }

    #[test]
    fn test_toml_parsing() {
        let toml_str = r#"
[[rules]]
match = "attribute_exists(email)"
path = "email"
action = { type = "fake", generator = "safe_email" }

[[rules]]
match = "attribute_exists(phone)"
path = "phone"
action = { type = "mask", keep_last = 4, mask_char = "*" }

[[rules]]
match = "attribute_exists(ssn)"
path = "ssn"
action = { type = "hash", salt_env = "IMPORT_SALT" }

[[rules]]
match = "attribute_exists(notes)"
path = "notes"
action = { type = "redact" }

[consistency]
fields = ["userId", "email"]
"#;
        let config: ImportConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.rules.len(), 4);
        assert!(config.consistency.is_some());
        assert_eq!(
            config.consistency.as_ref().unwrap().fields,
            vec!["userId", "email"]
        );
    }
}
