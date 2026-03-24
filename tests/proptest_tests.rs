use dynoxide::types::{AttributeValue, normalize_number_for_sort};
use proptest::prelude::*;
use std::collections::HashMap;

// =============================================================================
// Number sort key normalization
// =============================================================================

/// Generate a valid DynamoDB number string (integer or decimal, positive or negative).
fn arb_dynamo_number() -> impl Strategy<Value = String> {
    prop_oneof![
        // Small integers
        (-99999i64..=99999i64).prop_map(|n| n.to_string()),
        // Large integers
        any::<i64>().prop_map(|n| n.to_string()),
        // Decimals
        (-99999i64..=99999i64)
            .prop_flat_map(|int_part| (Just(int_part), 1u32..999999u32))
            .prop_map(|(int_part, frac)| format!("{int_part}.{frac}")),
        // Scientific notation
        (1i64..999i64)
            .prop_flat_map(|coeff| (Just(coeff), -38i32..38i32))
            .prop_map(|(coeff, exp)| format!("{coeff}e{exp}")),
    ]
}

/// For any two distinct DynamoDB numbers, their normalized sort keys must
/// preserve numeric ordering under string comparison.
#[test]
fn prop_normalize_preserves_ordering() {
    proptest!(|(a in arb_dynamo_number(), b in arb_dynamo_number())| {
        let fa: f64 = a.parse().unwrap();
        let fb: f64 = b.parse().unwrap();

        // Skip NaN/Inf — DynamoDB numbers are always finite
        if !fa.is_finite() || !fb.is_finite() {
            return Ok(());
        }

        let na = normalize_number_for_sort(&a);
        let nb = normalize_number_for_sort(&b);

        match fa.partial_cmp(&fb) {
            Some(std::cmp::Ordering::Less) => {
                prop_assert!(na < nb, "Expected {a} ({na}) < {b} ({nb})");
            }
            Some(std::cmp::Ordering::Greater) => {
                prop_assert!(na > nb, "Expected {a} ({na}) > {b} ({nb})");
            }
            Some(std::cmp::Ordering::Equal) => {
                prop_assert!(na == nb, "Expected {a} ({na}) == {b} ({nb})");
            }
            None => {} // NaN — skip
        }
    });
}

/// Normalization of zero variants should all produce the same encoding.
#[test]
fn prop_zero_variants_equal() {
    let zeros = ["0", "-0", "0.0", "0.00", "00", "0e0", "0e5"];
    let normalized: Vec<String> = zeros.iter().map(|z| normalize_number_for_sort(z)).collect();
    for (i, n) in normalized.iter().enumerate().skip(1) {
        assert_eq!(
            &normalized[0], n,
            "Zero variant '{}' produced different encoding than '0'",
            zeros[i]
        );
    }
}

/// Positive numbers should always sort after zero.
#[test]
fn prop_positive_after_zero() {
    let zero = normalize_number_for_sort("0");
    proptest!(|(n in 1i64..=i64::MAX)| {
        let s = n.to_string();
        let norm = normalize_number_for_sort(&s);
        prop_assert!(norm > zero, "Positive {s} ({norm}) should sort after zero ({zero})");
    });
}

/// Negative numbers should always sort before zero.
#[test]
fn prop_negative_before_zero() {
    let zero = normalize_number_for_sort("0");
    proptest!(|(n in i64::MIN..=-1i64)| {
        let s = n.to_string();
        let norm = normalize_number_for_sort(&s);
        prop_assert!(norm < zero, "Negative {s} ({norm}) should sort before zero ({zero})");
    });
}

// =============================================================================
// AttributeValue serde round-trip
// =============================================================================

/// Generate an arbitrary AttributeValue tree (bounded depth to avoid stack overflow).
fn arb_attribute_value(depth: u32) -> impl Strategy<Value = AttributeValue> {
    let leaf = prop_oneof![
        any::<String>().prop_map(AttributeValue::S),
        (-99999i64..99999i64).prop_map(|n| AttributeValue::N(n.to_string())),
        proptest::collection::vec(any::<u8>(), 0..32).prop_map(AttributeValue::B),
        any::<bool>().prop_map(AttributeValue::BOOL),
        Just(AttributeValue::NULL(true)),
        proptest::collection::vec("[a-z]{1,8}", 1..5).prop_map(AttributeValue::SS),
        proptest::collection::vec((-9999i64..9999i64).prop_map(|n| n.to_string()), 1..5)
            .prop_map(AttributeValue::NS),
        proptest::collection::vec(proptest::collection::vec(any::<u8>(), 0..16), 1..5)
            .prop_map(AttributeValue::BS),
    ];

    if depth == 0 {
        leaf.boxed()
    } else {
        prop_oneof![
            4 => leaf,
            1 => proptest::collection::vec(arb_attribute_value(depth - 1), 0..4)
                .prop_map(AttributeValue::L),
            1 => proptest::collection::hash_map("[a-z]{1,8}", arb_attribute_value(depth - 1), 0..4)
                .prop_map(AttributeValue::M),
        ]
        .boxed()
    }
}

/// Any AttributeValue should survive a serialize → deserialize round-trip.
#[test]
fn prop_attribute_value_serde_roundtrip() {
    proptest!(|(val in arb_attribute_value(2))| {
        let json = serde_json::to_value(&val).unwrap();
        let deserialized: AttributeValue = serde_json::from_value(json.clone()).unwrap();
        prop_assert_eq!(
            &val, &deserialized,
            "Round-trip failed for {:?}", json
        );
    });
}

/// Items (HashMap<String, AttributeValue>) should survive a round-trip.
#[test]
fn prop_item_serde_roundtrip() {
    let arb_item =
        proptest::collection::hash_map("[a-zA-Z_][a-zA-Z0-9_]{0,16}", arb_attribute_value(1), 0..8);

    proptest!(|(item in arb_item)| {
        let json = serde_json::to_value(&item).unwrap();
        let deserialized: HashMap<String, AttributeValue> =
            serde_json::from_value(json.clone()).unwrap();
        prop_assert_eq!(
            &item, &deserialized,
            "Item round-trip failed"
        );
    });
}
