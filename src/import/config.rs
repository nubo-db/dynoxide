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
    /// e.g. `attribute_exists(email)` or `begins_with(pk, :prefix)`, with
    /// `:prefix` supplied through `values`.
    #[serde(rename = "match")]
    pub match_expr: String,

    /// Names for the `#alias` references in `match`, in the shape of
    /// ExpressionAttributeNames: `names = { "#n" = "name" }`. Needed for
    /// attributes whose names are reserved words.
    #[serde(default)]
    pub names: HashMap<String, String>,

    /// Values for the `:name` references in `match`, in the shape of
    /// ExpressionAttributeValues: `values = { ":prefix" = "USER#" }`.
    /// Strings become `S`, integers and floats `N`, booleans `BOOL`.
    #[serde(default)]
    pub values: HashMap<String, toml::Value>,

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
    /// Names behind the `#alias` references in `condition`, if it has any.
    pub names: Option<HashMap<String, String>>,
    /// Values behind the `:name` references in `condition`, if it has any.
    pub values: Option<HashMap<String, AttributeValue>>,
    /// Parsed path elements for navigating into items.
    pub path: Vec<crate::expressions::PathElement>,
    /// The action to apply.
    pub action: ValidatedAction,
}

/// A secret salt value with redacted Debug output.
///
/// Wraps the raw salt bytes to prevent accidental leakage through
/// `Debug` formatting (logs, panics, `dbg!()` calls). The salt exists
/// specifically to prevent rainbow table attacks - `#[derive(Debug)]`
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

        let names = convert_names(&rule.names, i + 1)?;
        let values = convert_values(&rule.values, i + 1)?;
        validate_name_refs(&condition, &names, i + 1)?;
        validate_value_refs(&condition, &values, i + 1)?;
        condition::validate_static(&condition, &values)
            .and_then(|()| condition::validate_operand_semantics(&condition, &names, &values))
            .map_err(|e| {
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
            names,
            values,
            path,
            action,
        });
    }

    Ok((validated, config.consistency))
}

/// Check a rule's `names` table: every alias starts with `#`.
fn convert_names(
    names: &HashMap<String, String>,
    rule_num: usize,
) -> Result<Option<HashMap<String, String>>, String> {
    if names.is_empty() {
        return Ok(None);
    }
    for alias in names.keys() {
        if !alias.starts_with('#') {
            return Err(format!(
                "Rule {rule_num}: name alias '{alias}' must start with '#' (for example \"#n\")"
            ));
        }
    }
    Ok(Some(names.clone()))
}

/// Check that `names` and the `#alias` references in the match expression
/// line up: every reference is defined, and every name is used.
fn validate_name_refs(
    condition: &ConditionExpr,
    names: &Option<HashMap<String, String>>,
    rule_num: usize,
) -> Result<(), String> {
    condition::validate_name_refs(condition, names)
        .map_err(|e| format!("Rule {rule_num}: {e}. Add it to the rule's names table"))?;

    if let Some(names) = names {
        let used = crate::expressions::condition::extract_name_refs(condition);
        let mut unused: Vec<&String> = names.keys().filter(|k| !used.contains(k)).collect();
        unused.sort();
        if let Some(alias) = unused.first() {
            return Err(format!(
                "Rule {rule_num}: name {alias} is not referenced by the match expression"
            ));
        }
    }
    Ok(())
}

/// Convert a rule's `values` table into attribute values.
///
/// Mirrors ExpressionAttributeValues: every name starts with `:`, and only
/// scalars are accepted, since that is all a match expression can compare.
fn convert_values(
    values: &HashMap<String, toml::Value>,
    rule_num: usize,
) -> Result<Option<HashMap<String, AttributeValue>>, String> {
    if values.is_empty() {
        return Ok(None);
    }

    let mut converted = HashMap::with_capacity(values.len());
    for (name, value) in values {
        if !name.starts_with(':') {
            return Err(format!(
                "Rule {rule_num}: value name '{name}' must start with ':' (for example \":prefix\")"
            ));
        }
        let attr = match value {
            toml::Value::String(s) => AttributeValue::S(s.clone()),
            toml::Value::Integer(i) => AttributeValue::N(i.to_string()),
            toml::Value::Float(f) if f.is_finite() => AttributeValue::N(f.to_string()),
            toml::Value::Float(_) => {
                return Err(format!(
                    "Rule {rule_num}: value '{name}' must be a finite number"
                ));
            }
            toml::Value::Boolean(b) => AttributeValue::BOOL(*b),
            _ => {
                return Err(format!(
                    "Rule {rule_num}: value '{name}' must be a string, number or boolean"
                ));
            }
        };
        converted.insert(name.clone(), attr);
    }
    Ok(Some(converted))
}

/// Check that `values` and the `:name` references in the match expression
/// line up: every reference is defined, and every value is used. The same
/// two checks DynamoDB applies to ExpressionAttributeValues.
fn validate_value_refs(
    condition: &ConditionExpr,
    values: &Option<HashMap<String, AttributeValue>>,
    rule_num: usize,
) -> Result<(), String> {
    let refs = condition::extract_value_refs(condition);

    for name in &refs {
        let defined = values.as_ref().is_some_and(|v| v.contains_key(name));
        if !defined {
            return Err(format!(
                "Rule {rule_num}: match expression references {name} but values does not define it. \
                 Add values = {{ \"{name}\" = \"...\" }} to the rule"
            ));
        }
    }

    if let Some(values) = values {
        let mut unused: Vec<&String> = values.keys().filter(|k| !refs.contains(k)).collect();
        unused.sort();
        if let Some(name) = unused.first() {
            return Err(format!(
                "Rule {rule_num}: value {name} is not referenced by the match expression"
            ));
        }
    }

    Ok(())
}

/// Parse a dot-notation path into PathElements.
/// Supports: `email`, `address.city`, `items[0].name`
pub(super) fn parse_path(path: &str) -> Result<Vec<crate::expressions::PathElement>, String> {
    use crate::expressions::PathElement;

    if path.is_empty() {
        return Err("empty path".to_string());
    }

    let mut elements = Vec::new();
    for part in path.split('.') {
        if part.is_empty() {
            return Err("empty path segment".to_string());
        }
        // A rule path names a real attribute. `#alias` is a match-expression
        // idea, and the names table does not reach here, so accepting one
        // would leave a rule that quietly matches nothing.
        if part.starts_with('#') {
            return Err(format!(
                "'{part}' is an expression attribute name; a path names the attribute itself, \
                 so write the attribute's own name here"
            ));
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
                    let value = std::env::var(env_var).map_err(|_| {
                        format!(
                            "Rule {rule_num}: environment variable '{env_var}' not set (required for hash salt)"
                        )
                    })?;
                    // An empty variable is the shape a missing CI secret takes,
                    // and it would hash exactly as no salt at all.
                    if value.is_empty() {
                        return Err(format!(
                            "Rule {rule_num}: environment variable '{env_var}' is empty. \
                             SHA-256 without a salt is trivially reversible via rainbow tables, \
                             so set it to a secret value"
                        ));
                    }
                    value.into_bytes()
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
/// Match expressions use DynamoDB ConditionExpression syntax, so anything a
/// ConditionExpression can say works here: `attribute_exists`,
/// `attribute_not_exists`, `attribute_type`, `begins_with`, `contains`,
/// `size`, comparisons, `BETWEEN`, `IN`, and `AND` / `OR` / `NOT`.
///
/// As on DynamoDB, an operand that is not a path is a `:name` reference,
/// never an inline literal: `begins_with(pk, :prefix)` with the prefix in the
/// rule's `values` table. A reserved word or an awkward attribute name goes
/// through the `names` table as `#alias`, as ExpressionAttributeNames would.
pub fn matches_item(rule: &ValidatedRule, item: &HashMap<String, AttributeValue>) -> bool {
    crate::expressions::evaluate_without_tracking(&rule.condition, item, &rule.names, &rule.values)
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
            names: None,
            values: None,
            path: vec![crate::expressions::PathElement::Attribute(
                "email".to_string(),
            )],
            action,
        };
        let rule_debug = format!("{:?}", rule);
        assert!(rule_debug.contains("[REDACTED]"));
        assert!(!rule_debug.contains("super"));
    }

    fn parsed_rule(toml_str: &str) -> Result<ValidatedRule, String> {
        let config: ImportConfig = toml::from_str(toml_str).map_err(|e| e.to_string())?;
        let rule = &config.rules[0];
        let condition = condition::parse(&rule.match_expr).map_err(|e| e.to_string())?;
        let names = convert_names(&rule.names, 1)?;
        let values = convert_values(&rule.values, 1)?;
        validate_name_refs(&condition, &names, 1)?;
        validate_value_refs(&condition, &values, 1)?;
        condition::validate_static(&condition, &values)?;
        condition::validate_operand_semantics(&condition, &names, &values)?;
        Ok(ValidatedRule {
            condition,
            names,
            values,
            path: parse_path(&rule.path)?,
            action: validate_action(&rule.action, 1)?,
        })
    }

    #[test]
    fn test_values_table_feeds_begins_with() {
        let rule = parsed_rule(
            r#"
[[rules]]
match = "begins_with(pk, :prefix)"
values = { ":prefix" = "USER#" }
path = "email"
action = { type = "redact" }
"#,
        )
        .unwrap();

        let mut item = HashMap::new();
        item.insert("pk".to_string(), AttributeValue::S("USER#1".to_string()));
        assert!(matches_item(&rule, &item));

        item.insert("pk".to_string(), AttributeValue::S("ORDER#1".to_string()));
        assert!(!matches_item(&rule, &item));
    }

    #[test]
    fn test_values_convert_by_toml_type() {
        let rule = parsed_rule(
            r#"
[[rules]]
match = "age > :min AND active = :yes"
values = { ":min" = 18, ":yes" = true }
path = "name"
action = { type = "redact" }
"#,
        )
        .unwrap();
        let values = rule.values.unwrap();
        assert_eq!(values[":min"], AttributeValue::N("18".to_string()));
        assert_eq!(values[":yes"], AttributeValue::BOOL(true));
    }

    #[test]
    fn test_values_missing_reference_rejected() {
        let err = parsed_rule(
            r#"
[[rules]]
match = "begins_with(pk, :prefix)"
path = "email"
action = { type = "redact" }
"#,
        )
        .unwrap_err();
        assert!(err.contains(":prefix"), "{err}");
        assert!(err.contains("does not define"), "{err}");
    }

    #[test]
    fn test_values_unused_rejected() {
        let err = parsed_rule(
            r#"
[[rules]]
match = "attribute_exists(pk)"
values = { ":prefix" = "USER#" }
path = "email"
action = { type = "redact" }
"#,
        )
        .unwrap_err();
        assert!(err.contains(":prefix"), "{err}");
        assert!(err.contains("not referenced"), "{err}");
    }

    #[test]
    fn test_values_name_must_start_with_colon() {
        let err = parsed_rule(
            r#"
[[rules]]
match = "begins_with(pk, :prefix)"
values = { "prefix" = "USER#" }
path = "email"
action = { type = "redact" }
"#,
        )
        .unwrap_err();
        assert!(err.contains("must start with ':'"), "{err}");
    }

    #[test]
    fn test_path_rejects_an_expression_attribute_name() {
        let err = parse_path("#n").unwrap_err();
        assert!(err.contains("names the attribute itself"), "{err}");
        assert!(parse_path("name").is_ok());
    }

    #[test]
    fn test_empty_salt_is_rejected() {
        // SAFETY: single-threaded test, no concurrent env reads
        unsafe { std::env::set_var("DYNOXIDE_TEST_EMPTY_SALT", "") };
        let action = ActionConfig::Hash {
            salt_env: Some("DYNOXIDE_TEST_EMPTY_SALT".to_string()),
        };
        let err = validate_action(&action, 1).unwrap_err();
        assert!(err.contains("is empty"), "{err}");

        unsafe { std::env::set_var("DYNOXIDE_TEST_EMPTY_SALT", "s3cret") };
        assert!(validate_action(&action, 1).is_ok());
    }

    #[test]
    fn test_values_reject_non_finite_floats() {
        let err = parsed_rule(
            r#"
[[rules]]
match = "amount < :max"
values = { ":max" = inf }
path = "email"
action = { type = "redact" }
"#,
        )
        .unwrap_err();
        assert!(err.contains("finite"), "{err}");
    }

    #[test]
    fn test_values_of_the_wrong_type_are_rejected_up_front() {
        let err = parsed_rule(
            r#"
[[rules]]
match = "begins_with(pk, :prefix)"
values = { ":prefix" = 123 }
path = "email"
action = { type = "redact" }
"#,
        )
        .unwrap_err();
        assert!(err.contains("begins_with"), "{err}");
    }

    #[test]
    fn test_names_table_reaches_a_reserved_word_attribute() {
        let rule = parsed_rule(
            r##"
[[rules]]
match = "attribute_exists(#n)"
names = { "#n" = "name" }
path = "email"
action = { type = "redact" }
"##,
        )
        .unwrap();
        let mut item = HashMap::new();
        item.insert("name".to_string(), AttributeValue::S("Ada".to_string()));
        assert!(matches_item(&rule, &item));
        assert!(!matches_item(&rule, &HashMap::new()));
    }

    #[test]
    fn test_names_undefined_or_unused_rejected() {
        let err = parsed_rule(
            r#"
[[rules]]
match = "attribute_exists(#n)"
path = "email"
action = { type = "redact" }
"#,
        )
        .unwrap_err();
        assert!(err.contains("#n"), "{err}");

        let err = parsed_rule(
            r##"
[[rules]]
match = "attribute_exists(pk)"
names = { "#n" = "name" }
path = "email"
action = { type = "redact" }
"##,
        )
        .unwrap_err();
        assert!(err.contains("not referenced"), "{err}");

        let err = parsed_rule(
            r#"
[[rules]]
match = "attribute_exists(pk)"
names = { "n" = "name" }
path = "email"
action = { type = "redact" }
"#,
        )
        .unwrap_err();
        assert!(err.contains("must start with '#'"), "{err}");
    }

    #[test]
    fn test_values_reject_non_scalars() {
        let err = parsed_rule(
            r#"
[[rules]]
match = "begins_with(pk, :prefix)"
values = { ":prefix" = ["USER#"] }
path = "email"
action = { type = "redact" }
"#,
        )
        .unwrap_err();
        assert!(err.contains("string, number or boolean"), "{err}");
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
