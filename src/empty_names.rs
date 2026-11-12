//! Empty attribute names.
//!
//! DynamoDB refuses an attribute name that is the empty string wherever a
//! request carries one: an item's top-level attribute, a map key at any depth
//! inside a value, a key, an ExclusiveStartKey, an ExpressionAttributeNames
//! value, AttributesToGet, the legacy Expected, AttributeUpdates, QueryFilter
//! and ScanFilter values, and PartiQL parameters. It answers before it looks the
//! table up, before it reads or parses an expression, and before it checks a key
//! against the schema. Only a ReturnValues outside its enum is reported first.
//!
//! The message is "Empty attribute name" everywhere; what changes by operation
//! is the wrapping. PutItem, UpdateItem and DeleteItem put it inside the
//! validation envelope. An ExpressionAttributeNames entry is reported as
//! "ExpressionAttributeNames contains invalid value: Empty attribute name for key
//! #n". The condition values of a TransactWriteItems action name the
//! ExpressionAttributeValues key.
//!
//! Captured in eu-west-2 and us-east-1 on 9 October 2026, and in ten regions for
//! the shapes the two disagreed on. Where they split, Dynoxide follows the
//! majority: for a transaction's condition values (8 of 10), where eu-west-2 is
//! in the minority and answers the bare message. Scan's ExpressionAttributeValues
//! and ExclusiveStartKey went the other way: 7 of 10 named them then, and 20 of
//! 33 regions answered bare on 20 October 2026, so Scan answers bare.
//!
//! An empty name in a legacy QueryFilter or ScanFilter is accepted, as DynamoDB
//! accepts it; only the values inside those filters are checked.

use crate::errors::{DynoxideError, Result};
use crate::types::AttributeValue;
use std::collections::HashMap;

/// DynamoDB's message for an empty attribute name.
pub(crate) const MESSAGE: &str = "Empty attribute name";

/// Whether a value holds a map with an empty key, at any depth.
pub(crate) fn value_has_empty_name(value: &AttributeValue) -> bool {
    match value {
        AttributeValue::M(map) => map_has_empty_name(map),
        AttributeValue::L(list) => list.iter().any(value_has_empty_name),
        _ => false,
    }
}

/// Whether a map of attributes (an item, a key, a document) names an empty
/// attribute, at the top level or inside any value.
pub(crate) fn map_has_empty_name(map: &HashMap<String, AttributeValue>) -> bool {
    map.iter()
        .any(|(name, value)| name.is_empty() || value_has_empty_name(value))
}

/// The same check over attribute values that have not been parsed yet: an
/// `{"M": {...}}` with an empty key at any depth.
pub(crate) fn json_value_has_empty_name(value: &serde_json::Value) -> bool {
    let Some(object) = value.as_object() else {
        return false;
    };
    if let Some(map) = object.get("M").and_then(|m| m.as_object()) {
        return map
            .iter()
            .any(|(name, inner)| name.is_empty() || json_value_has_empty_name(inner));
    }
    if let Some(list) = object.get("L").and_then(|l| l.as_array()) {
        return list.iter().any(json_value_has_empty_name);
    }
    false
}

/// The same check over an unparsed map of attributes.
pub(crate) fn json_map_has_empty_name(value: &serde_json::Value) -> bool {
    value.as_object().is_some_and(|map| {
        map.iter()
            .any(|(name, inner)| name.is_empty() || json_value_has_empty_name(inner))
    })
}

/// The first ExpressionAttributeNames placeholder, in placeholder order, whose
/// name is empty.
pub(crate) fn empty_placeholder(names: Option<&HashMap<String, String>>) -> Option<&str> {
    let names = names?;
    let mut empty: Vec<&str> = names
        .iter()
        .filter(|(_, name)| name.is_empty())
        .map(|(placeholder, _)| placeholder.as_str())
        .collect();
    empty.sort_unstable();
    empty.first().copied()
}

/// Read a request's `ExpressionAttributeNames`, taking a name given as JSON
/// `null` for an empty one, as DynamoDB reads it: `ExpressionAttributeNames
/// contains invalid value: Empty attribute name for key #n` beside an
/// expression, and `ExpressionAttributeNames can only be specified when using
/// expressions` without one (captured eu-west-2 and us-east-1, 2026-11-12). A
/// null in place of the whole map is still no map.
pub(crate) fn deserialize_names<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<HashMap<String, String>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let names: Option<HashMap<String, Option<String>>> =
        serde::Deserialize::deserialize(deserializer)?;
    Ok(names.map(|names| {
        names
            .into_iter()
            .map(|(placeholder, name)| (placeholder, name.unwrap_or_default()))
            .collect()
    }))
}

/// The message for an empty ExpressionAttributeNames value.
pub(crate) fn placeholder_message(placeholder: &str) -> String {
    format!("ExpressionAttributeNames contains invalid value: {MESSAGE} for key {placeholder}")
}

/// Whether the expression names are checked for an empty name ahead of
/// everything else, as the current layer checks them. The older layer, which a
/// null name puts the request on, checks them with the expressions instead,
/// after `ExpressionAttributeNames can only be specified when using
/// expressions` (captured eu-west-2 and us-east-1, 2026-11-12; see
/// `helpers::validate_expression_params`).
pub(crate) fn names_checked_early() -> bool {
    crate::types::current_value_layer() != crate::validation::ValueLayer::Older
}

/// [`check_names`] for an operation that checks its expression names again
/// with its expressions: only where [`names_checked_early`].
pub(crate) fn check_names_early(names: Option<&HashMap<String, String>>) -> Result<()> {
    if names_checked_early() {
        check_names(names)
    } else {
        Ok(())
    }
}

/// Refuse an ExpressionAttributeNames entry whose name is empty, bare, with
/// DynamoDB's message naming the first such placeholder.
pub(crate) fn check_names(names: Option<&HashMap<String, String>>) -> Result<()> {
    match empty_placeholder(names) {
        Some(placeholder) => Err(DynoxideError::ValidationException(placeholder_message(
            placeholder,
        ))),
        None => Ok(()),
    }
}

/// The first ExpressionAttributeValues key, in key order, whose value holds an
/// empty name. Reads the raw JSON when the request carries it, as the
/// validator does, otherwise the parsed values.
pub(crate) fn value_placeholder<'a>(
    values: Option<&'a HashMap<String, AttributeValue>>,
    raw: Option<&'a serde_json::Value>,
) -> Option<&'a str> {
    let mut found: Vec<&str> = match (raw, values) {
        (Some(raw), _) => raw
            .as_object()
            .map(|map| {
                map.iter()
                    .filter(|(_, value)| json_value_has_empty_name(value))
                    .map(|(key, _)| key.as_str())
                    .collect()
            })
            .unwrap_or_default(),
        (None, Some(values)) => values
            .iter()
            .filter(|(_, value)| value_has_empty_name(value))
            .map(|(key, _)| key.as_str())
            .collect(),
        (None, None) => Vec::new(),
    };
    found.sort_unstable();
    found.first().copied()
}

/// The message a transaction's condition values use for an
/// ExpressionAttributeValues entry holding an empty name.
pub(crate) fn value_placeholder_message(key: &str) -> String {
    format!("ExpressionAttributeValues contains invalid value: {MESSAGE} for key {key}")
}

/// Whether AttributesToGet names the empty attribute.
pub(crate) fn names_include_empty(names: Option<&Vec<String>>) -> bool {
    names.is_some_and(|names| names.iter().any(String::is_empty))
}

/// Whether the legacy Expected parameter names an empty attribute or compares
/// against a value holding one.
pub(crate) fn expected_has_empty_name(expected: Option<&serde_json::Value>) -> bool {
    let Some(map) = expected.and_then(|e| e.as_object()) else {
        return false;
    };
    map.iter()
        .any(|(name, condition)| name.is_empty() || condition_values_have_empty_name(condition))
}

/// Whether a legacy QueryFilter or ScanFilter compares against a value holding
/// an empty name. An empty attribute name in the filter itself is accepted.
pub(crate) fn filter_values_have_empty_name(filter: Option<&serde_json::Value>) -> bool {
    filter
        .and_then(|f| f.as_object())
        .is_some_and(|map| map.values().any(condition_values_have_empty_name))
}

/// The `Value` and `AttributeValueList` of one legacy condition.
fn condition_values_have_empty_name(condition: &serde_json::Value) -> bool {
    let Some(condition) = condition.as_object() else {
        return false;
    };
    condition
        .get("Value")
        .is_some_and(json_value_has_empty_name)
        || condition
            .get("AttributeValueList")
            .and_then(|l| l.as_array())
            .is_some_and(|list| list.iter().any(json_value_has_empty_name))
}

/// Whether any PartiQL parameter holds an empty name.
pub(crate) fn parameters_have_empty_name(parameters: Option<&[AttributeValue]>) -> bool {
    parameters.is_some_and(|params| params.iter().any(value_has_empty_name))
}

/// The bare rejection.
pub(crate) fn bare() -> DynoxideError {
    DynoxideError::ValidationException(MESSAGE.to_string())
}

/// The rejection inside the validation envelope, for PutItem, UpdateItem and
/// DeleteItem. Built enveloped rather than tagged, because DeleteItem wraps it
/// too and the tag only resolves to the envelope for PutItem and UpdateItem.
pub(crate) fn enveloped() -> DynoxideError {
    DynoxideError::ValidationException(crate::validation::envelope_message(MESSAGE))
}

/// Check the parts a single-item write shares, in the order its request lists
/// them: the item or key, the legacy Expected, then the expression names and
/// values. Every rejection goes inside the validation envelope.
///
/// `updates_empty` is whether UpdateItem's AttributeUpdates names an empty
/// attribute or carries a value holding one; it sits between the key and
/// Expected in UpdateItem's request.
pub(crate) fn check_single_item_write(
    attributes: &HashMap<String, AttributeValue>,
    updates_empty: bool,
    expected: Option<&serde_json::Value>,
    names: Option<&HashMap<String, String>>,
    values: Option<&HashMap<String, AttributeValue>>,
    raw_values: Option<&serde_json::Value>,
) -> Result<()> {
    if map_has_empty_name(attributes) || updates_empty || expected_has_empty_name(expected) {
        return Err(enveloped());
    }
    if let Some(placeholder) = empty_placeholder(names).filter(|_| names_checked_early()) {
        return Err(DynoxideError::ValidationException(
            crate::validation::envelope_message(&placeholder_message(placeholder)),
        ));
    }
    if value_placeholder(values, raw_values).is_some() {
        return Err(enveloped());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn map(entries: &[(&str, AttributeValue)]) -> HashMap<String, AttributeValue> {
        entries
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn finds_an_empty_name_at_any_depth() {
        let leaf = AttributeValue::S("x".into());
        assert!(map_has_empty_name(&map(&[("", leaf.clone())])));
        let nested =
            AttributeValue::M(map(&[("a", AttributeValue::M(map(&[("", leaf.clone())])))]));
        assert!(map_has_empty_name(&map(&[("m", nested)])));
        let in_list = AttributeValue::L(vec![AttributeValue::M(map(&[("", leaf.clone())]))]);
        assert!(map_has_empty_name(&map(&[("l", in_list)])));
        // A space is a name.
        assert!(!map_has_empty_name(&map(&[(" ", leaf.clone())])));
        // An empty string value is not an empty name.
        assert!(!map_has_empty_name(&map(&[(
            "s",
            AttributeValue::S(String::new())
        )])));
    }

    #[test]
    fn reads_unparsed_values_the_same_way() {
        assert!(json_value_has_empty_name(
            &json!({"M": {"a": {"L": [{"M": {"": {"N": "1"}}}]}}})
        ));
        assert!(!json_value_has_empty_name(&json!({"M": {" ": {"S": ""}}})));
        assert!(json_map_has_empty_name(
            &json!({"pk": {"S": "a"}, "": {"S": "x"}})
        ));
    }

    #[test]
    fn legacy_filters_check_values_but_accept_an_empty_name() {
        let named = json!({"": {"ComparisonOperator": "NOT_NULL"}});
        assert!(!filter_values_have_empty_name(Some(&named)));
        let valued = json!({"m": {"ComparisonOperator": "EQ", "AttributeValueList": [{"M": {"": {"S": "x"}}}]}});
        assert!(filter_values_have_empty_name(Some(&valued)));
        let expected_name = json!({"": {"Exists": false}});
        assert!(expected_has_empty_name(Some(&expected_name)));
        let expected_value = json!({"x": {"Value": {"M": {"": {"S": "x"}}}}});
        assert!(expected_has_empty_name(Some(&expected_value)));
    }

    #[test]
    fn the_raw_values_are_read_when_a_request_carries_both() {
        // An in-process request can carry parsed values and the raw JSON the
        // validator reads; the check must look where the validator looks.
        let parsed = map(&[(":v", AttributeValue::S("clean".into()))]);
        let raw = json!({":v": {"M": {"": {"S": "x"}}}});
        assert_eq!(value_placeholder(Some(&parsed), Some(&raw)), Some(":v"));
        assert_eq!(value_placeholder(Some(&parsed), None), None);
    }
}
