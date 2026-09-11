//! Cross-table referential consistency tracking.
//!
//! Ensures that the same original value is always anonymised to the same
//! output value across all tables in a single import run.
//!
//! Example: if `userId = "USER#123"` is anonymised to `"USER#abc"` in the
//! Users table, it should also become `"USER#abc"` in the Orders table.
//!
//! Hash actions are excluded from the map because HMAC-SHA256 over a canonical
//! encoding is deterministic - the same input always produces the same output,
//! so consistency is inherent. A seeded fake is excluded on the same terms, but
//! only for a scalar: it draws from entropy for a map, list or set, so those
//! still come through here.

use crate::types::AttributeValue;
use std::collections::HashMap;

/// Default maximum entries per field before the map stops accepting new values.
/// At ~100 bytes per entry (key + value + HashMap overhead), 1M entries ≈ 100 MB.
const DEFAULT_MAX_ENTRIES_PER_FIELD: usize = 1_000_000;

/// Maps `(field_name, original_value)` → `anonymised_value`.
///
/// Maintained across all tables in a single import run.
/// Only used for non-deterministic actions (fake, mask). Hash actions are
/// inherently consistent and bypass this map entirely.
#[derive(Debug, Default)]
pub struct ConsistencyMap {
    map: HashMap<String, HashMap<Vec<u8>, AttributeValue>>,
    max_entries_per_field: usize,
    /// Fields that have hit their cap (warned once per field).
    capped_fields: std::collections::HashSet<String>,
}

impl ConsistencyMap {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
            max_entries_per_field: DEFAULT_MAX_ENTRIES_PER_FIELD,
            capped_fields: std::collections::HashSet::new(),
        }
    }

    /// Look up a previously anonymised value.
    pub fn get(&self, field_name: &str, original: &AttributeValue) -> Option<AttributeValue> {
        let field_map = self.map.get(field_name)?;
        let key = super::anonymise::canonical_bytes(original);
        // Vec<u8> looks up from a &[u8] through Borrow.
        field_map.get(key.as_slice()).cloned()
    }

    /// Record an anonymisation mapping. Returns a warning if the field has
    /// hit its capacity cap (returned once per field, not per insert).
    pub fn insert(
        &mut self,
        field_name: String,
        original: AttributeValue,
        anonymised: AttributeValue,
    ) -> Option<String> {
        let field_map = self.map.entry(field_name.clone()).or_default();

        // Check capacity cap
        if field_map.len() >= self.max_entries_per_field {
            if self.capped_fields.insert(field_name.clone()) {
                return Some(format!(
                    "consistency map for field '{}' reached {} entries - \
                     consistency is no longer guaranteed for new unseen values of this field",
                    field_name, self.max_entries_per_field
                ));
            }
            return None;
        }

        field_map.insert(super::anonymise::canonical_bytes(&original), anonymised);
        None
    }

    /// Number of tracked fields.
    pub fn field_count(&self) -> usize {
        self.map.len()
    }

    /// Total number of tracked mappings across all fields.
    pub fn total_mappings(&self) -> usize {
        self.map.values().map(|m| m.len()).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_consistency_map_basic() {
        let mut map = ConsistencyMap::new();

        let original = AttributeValue::S("user@example.com".to_string());
        let anonymised = AttributeValue::S("fake@example.com".to_string());

        let warning = map.insert("email".to_string(), original.clone(), anonymised.clone());
        assert!(warning.is_none());

        assert_eq!(map.get("email", &original), Some(anonymised));
        assert_eq!(
            map.get("email", &AttributeValue::S("other@example.com".to_string())),
            None
        );
        assert_eq!(map.get("phone", &original), None);
    }

    #[test]
    fn test_consistency_map_multiple_fields() {
        let mut map = ConsistencyMap::new();

        map.insert(
            "email".to_string(),
            AttributeValue::S("a@b.com".to_string()),
            AttributeValue::S("fake@b.com".to_string()),
        );
        map.insert(
            "userId".to_string(),
            AttributeValue::S("USER#1".to_string()),
            AttributeValue::S("USER#xxx".to_string()),
        );

        assert_eq!(map.field_count(), 2);
        assert_eq!(map.total_mappings(), 2);
    }

    #[test]
    fn test_consistency_map_cap_warns_once() {
        let mut map = ConsistencyMap::new();
        map.max_entries_per_field = 3;

        // Fill to cap
        for i in 0..3 {
            let w = map.insert(
                "email".to_string(),
                AttributeValue::S(format!("user{i}@example.com")),
                AttributeValue::S(format!("fake{i}@example.com")),
            );
            assert!(w.is_none());
        }

        // First insert over cap warns
        let w = map.insert(
            "email".to_string(),
            AttributeValue::S("user3@example.com".to_string()),
            AttributeValue::S("fake3@example.com".to_string()),
        );
        assert!(w.is_some());
        let msg = w.unwrap();
        assert!(msg.contains("reached 3 entries"));
        assert!(msg.contains("consistency is no longer guaranteed"));

        // Subsequent inserts over cap: no duplicate warning
        let w = map.insert(
            "email".to_string(),
            AttributeValue::S("user4@example.com".to_string()),
            AttributeValue::S("fake4@example.com".to_string()),
        );
        assert!(w.is_none());

        // Existing lookups still work
        assert_eq!(
            map.get("email", &AttributeValue::S("user0@example.com".to_string())),
            Some(AttributeValue::S("fake0@example.com".to_string()))
        );
        // Over-cap value was not stored
        assert_eq!(
            map.get("email", &AttributeValue::S("user3@example.com".to_string())),
            None
        );
    }

    #[test]
    fn test_a_string_and_a_number_are_separate_entries() {
        // The map used to key on the bare text, so S("42") and N("42") shared
        // one entry and an anonymised number came back as the string that
        // happened to be seen first. Keying on the canonical encoding carries
        // the type, the way `hash` does.
        let mut map = ConsistencyMap::new();
        map.insert(
            "amount".to_string(),
            AttributeValue::S("42".to_string()),
            AttributeValue::S("as-a-string".to_string()),
        );

        assert_eq!(
            map.get("amount", &AttributeValue::S("42".to_string())),
            Some(AttributeValue::S("as-a-string".to_string()))
        );
        assert_eq!(
            map.get("amount", &AttributeValue::N("42".to_string())),
            None,
            "a number must not read the entry a string wrote"
        );
    }

    #[test]
    fn test_a_map_value_finds_its_own_entry_again() {
        // AttributeValue::M holds a HashMap, so two instances of one value
        // iterate in different orders. Keying on a serialisation of that order
        // meant a lookup never matched what insert wrote, and every item drew
        // a fresh value.
        let entries = || {
            let mut m = std::collections::HashMap::new();
            for k in ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot"] {
                m.insert(k.to_string(), AttributeValue::S(format!("{k}-value")));
            }
            AttributeValue::M(m)
        };

        let mut map = ConsistencyMap::new();
        map.insert(
            "profile".to_string(),
            entries(),
            AttributeValue::S("anonymised".to_string()),
        );

        assert_eq!(
            map.get("profile", &entries()),
            Some(AttributeValue::S("anonymised".to_string())),
            "an equal map must find the entry it wrote"
        );
    }
}
