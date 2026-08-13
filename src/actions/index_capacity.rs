//! Write capacity consumed by a secondary index.
//!
//! DynamoDB charges index replication against the change to what the index
//! stores, not against the item the write leaves behind. A write that leaves an
//! index's stored view untouched costs nothing on that index; moving an index key
//! costs two writes, because it is a delete from the old position and an insert
//! into the new one.
//!
//! The rules below are captured against real DynamoDB in eu-west-2. `old` and
//! `new` are the projected index entries either side of the write, and either can
//! be absent because the item did not exist or was not a member of the index.
//!
//! | Before  | After                          | Charge                        |
//! |---------|--------------------------------|-------------------------------|
//! | absent  | absent                         | nothing                       |
//! | absent  | present                        | `ceil(new)`                   |
//! | present | absent                         | `ceil(old)`                   |
//! | present | present, same key, identical   | nothing                       |
//! | present | present, same key, differing   | `max(ceil(old), ceil(new))`   |
//! | present | present, differing key         | `ceil(old) + ceil(new)`       |
//!
//! Sizing is on the projected index entry throughout, never on the base item, so
//! a 3KB attribute the index does not project costs it nothing. LSIs follow the
//! same rules as GSIs.

use super::gsi::{IndexDef, build_index_item};
use crate::types::{Item, write_capacity_units};

/// One index's stored view of an item.
struct IndexEntry {
    /// The `(pk, sk)` position of the entry, which decides whether a write moves
    /// it or overwrites it in place.
    key: (String, String),
    projected: Item,
    size: usize,
}

/// Build the index's stored view of `item`, or `None` when the item is not a
/// member of this index. Membership is the existing sparse-index rule.
fn entry_for(
    item: &Item,
    index: &IndexDef,
    table_pk: &str,
    table_sk: Option<&str>,
) -> Option<IndexEntry> {
    let key = index.index_key_strings(item)?;
    let projected = build_index_item(item, index, table_pk, table_sk);
    let size = crate::types::item_size(&projected);
    Some(IndexEntry {
        key,
        projected,
        size,
    })
}

/// Write capacity units one index consumes for a single write, or `None` when
/// the index's stored view does not change.
///
/// `None` is distinct from `Some(0.0)`: the caller records nothing for that
/// index, so the arm is absent from the response rather than present and zeroed.
/// Pass `None` for `old_item` when no item existed beforehand, and `None` for
/// `new_item` on a delete.
pub fn index_write_units(
    old_item: Option<&Item>,
    new_item: Option<&Item>,
    index: &IndexDef,
    table_pk: &str,
    table_sk: Option<&str>,
) -> Option<f64> {
    let old = old_item.and_then(|item| entry_for(item, index, table_pk, table_sk));
    let new = new_item.and_then(|item| entry_for(item, index, table_pk, table_sk));

    match (old, new) {
        (None, None) => None,
        (None, Some(new)) => Some(write_capacity_units(new.size)),
        (Some(old), None) => Some(write_capacity_units(old.size)),
        (Some(old), Some(new)) if old.key != new.key => {
            // A move is a delete and an insert, each rounded on its own image
            // before they are added. Rounding the summed bytes once would
            // under-charge either side of a KB boundary.
            Some(write_capacity_units(old.size) + write_capacity_units(new.size))
        }
        (Some(old), Some(new)) if old.projected == new.projected => None,
        (Some(old), Some(new)) => {
            // Overwritten in place, and charged on the larger of the two images
            // rather than on the one left behind.
            Some(write_capacity_units(old.size).max(write_capacity_units(new.size)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{AttributeValue, ProjectionType};
    use std::collections::HashMap;

    /// The capture's GSI: HASH `gsiPk`, no sort key, projecting `INCLUDE [proj]`.
    fn gsi_include() -> IndexDef {
        IndexDef {
            index_name: "gsi-inc".to_string(),
            pk_attr: "gsiPk".to_string(),
            sk_attr: None,
            projection_type: ProjectionType::INCLUDE,
            non_key_attributes: Some(vec!["proj".to_string()]),
        }
    }

    fn gsi_all() -> IndexDef {
        IndexDef {
            index_name: "gsi-all".to_string(),
            pk_attr: "gsiPk".to_string(),
            sk_attr: None,
            projection_type: ProjectionType::ALL,
            non_key_attributes: None,
        }
    }

    fn gsi_keys_only() -> IndexDef {
        IndexDef {
            index_name: "gsi-keys".to_string(),
            pk_attr: "gsiPk".to_string(),
            sk_attr: None,
            projection_type: ProjectionType::KEYS_ONLY,
            non_key_attributes: None,
        }
    }

    /// The capture's LSI: the table partition key plus `lsiSk`.
    fn lsi_include() -> IndexDef {
        IndexDef {
            index_name: "lsi-inc".to_string(),
            pk_attr: "pk".to_string(),
            sk_attr: Some("lsiSk".to_string()),
            projection_type: ProjectionType::INCLUDE,
            non_key_attributes: Some(vec!["proj".to_string()]),
        }
    }

    fn s(v: &str) -> AttributeValue {
        AttributeValue::S(v.to_string())
    }

    /// An item shaped like the capture's: table keys, an optional GSI key, an
    /// optional LSI sort key, and `proj` as the size lever.
    fn item(pairs: &[(&str, &str)]) -> Item {
        let mut item: HashMap<String, AttributeValue> = HashMap::new();
        for (k, v) in pairs {
            item.insert((*k).to_string(), s(v));
        }
        item
    }

    fn pad(n: usize) -> String {
        "x".repeat(n)
    }

    fn units(old: Option<&Item>, new: Option<&Item>, index: &IndexDef) -> Option<f64> {
        index_write_units(old, new, index, "pk", Some("sk"))
    }

    #[test]
    fn absent_on_both_sides_charges_nothing() {
        // An item in neither the old nor the new index view. Capture: DeleteItem
        // of an item in no index reports total 1, table 1, and no index arm.
        let without = item(&[("pk", "a"), ("sk", "1"), ("other", "o")]);
        assert_eq!(units(Some(&without), Some(&without), &gsi_include()), None);
        assert_eq!(units(None, None, &gsi_include()), None);
    }

    #[test]
    fn joining_the_index_charges_the_new_entry() {
        // Capture V1: PutItem of a sub-1KB GSI member reports gsi 1.
        let new = item(&[("pk", "v1"), ("sk", "1"), ("gsiPk", "g"), ("proj", "p")]);
        assert_eq!(units(None, Some(&new), &gsi_include()), Some(1.0));
    }

    #[test]
    fn leaving_the_index_charges_the_old_entry() {
        // Capture S7: REMOVE of the GSI key against a 3017B entry reports gsi 3.
        let old = item(&[
            ("pk", "s7"),
            ("sk", "1"),
            ("gsiPk", "g"),
            ("proj", &pad(3000)),
        ]);
        let new = item(&[("pk", "s7"), ("sk", "1"), ("proj", &pad(3000))]);
        assert_eq!(units(Some(&old), Some(&new), &gsi_include()), Some(3.0));
    }

    #[test]
    fn delete_charges_the_old_entry() {
        // Capture S6: DeleteItem against a 3017B entry reports gsi 3, not a flat 1.
        let old = item(&[
            ("pk", "s6"),
            ("sk", "1"),
            ("gsiPk", "g"),
            ("proj", &pad(3000)),
        ]);
        assert_eq!(units(Some(&old), None, &gsi_include()), Some(3.0));
    }

    #[test]
    fn delete_of_a_non_member_charges_nothing() {
        // #176 fault 3: DeleteItem charged the GSI whether or not the item was in it.
        let old = item(&[("pk", "c"), ("sk", "1"), ("other", "o")]);
        assert_eq!(units(Some(&old), None, &gsi_include()), None);
    }

    #[test]
    fn identical_overwrite_charges_nothing() {
        // Capture V2: an identical overwrite reports total 1, table 1, no arms.
        let same = item(&[("pk", "v1"), ("sk", "1"), ("gsiPk", "g"), ("proj", "p")]);
        assert_eq!(units(Some(&same), Some(&same), &gsi_include()), None);
    }

    #[test]
    fn change_outside_the_projection_charges_nothing() {
        // #176: UpdateItem SET on a non-projected attribute reports no GSI arm
        // under INCLUDE, and none under KEYS_ONLY either.
        let old = item(&[("pk", "b"), ("sk", "1"), ("gsiPk", "g"), ("other", "o")]);
        let new = item(&[("pk", "b"), ("sk", "1"), ("gsiPk", "g"), ("other", "o2")]);
        assert_eq!(units(Some(&old), Some(&new), &gsi_include()), None);
        assert_eq!(units(Some(&old), Some(&new), &gsi_keys_only()), None);
    }

    #[test]
    fn change_outside_the_projection_still_charges_an_all_projection() {
        // The same write against a projection that stores everything does change
        // the index's view, so it is charged. This is why #176's LSI (projecting
        // ALL) is charged where the GSI (projecting INCLUDE) is not.
        let old = item(&[("pk", "b"), ("sk", "1"), ("gsiPk", "g"), ("other", "o")]);
        let new = item(&[("pk", "b"), ("sk", "1"), ("gsiPk", "g"), ("other", "o2")]);
        assert_eq!(units(Some(&old), Some(&new), &gsi_all()), Some(1.0));
    }

    #[test]
    fn in_place_change_is_charged_on_the_larger_image() {
        // Capture S2 and S3: 3017B to 18B and 18B to 3020B both report gsi 3.
        // Sizing on the new image alone would give 1 for the shrink.
        let big = item(&[
            ("pk", "s2"),
            ("sk", "1"),
            ("gsiPk", "g"),
            ("proj", &pad(3000)),
        ]);
        let small = item(&[("pk", "s2"), ("sk", "1"), ("gsiPk", "g"), ("proj", "p")]);

        assert_eq!(units(Some(&big), Some(&small), &gsi_include()), Some(3.0));
        assert_eq!(units(Some(&small), Some(&big), &gsi_include()), Some(3.0));
    }

    #[test]
    fn in_place_change_rounds_once_not_on_the_summed_bytes() {
        // Capture R1 and R2: equal-sized in-place changes at 617B and 1517B
        // report 1 and 2. Summing the two images and rounding once would give
        // 2 and 3, so this is the case that separates the two readings.
        let a = item(&[
            ("pk", "r1"),
            ("sk", "1"),
            ("gsiPk", "g"),
            ("proj", &pad(600)),
        ]);
        let b = item(&[
            ("pk", "r1"),
            ("sk", "1"),
            ("gsiPk", "g"),
            ("proj", &"y".repeat(600)),
        ]);
        assert_eq!(units(Some(&a), Some(&b), &gsi_include()), Some(1.0));

        let c = item(&[
            ("pk", "r2"),
            ("sk", "1"),
            ("gsiPk", "g"),
            ("proj", &pad(1500)),
        ]);
        let d = item(&[
            ("pk", "r2"),
            ("sk", "1"),
            ("gsiPk", "g"),
            ("proj", &"y".repeat(1500)),
        ]);
        assert_eq!(units(Some(&c), Some(&d), &gsi_include()), Some(2.0));
    }

    #[test]
    fn in_place_shrink_across_a_boundary_holds_the_larger_image() {
        // Capture R3: 2017B down to 117B reports 2, not 1.
        let old = item(&[
            ("pk", "r3"),
            ("sk", "1"),
            ("gsiPk", "g"),
            ("proj", &pad(2000)),
        ]);
        let new = item(&[
            ("pk", "r3"),
            ("sk", "1"),
            ("gsiPk", "g"),
            ("proj", &pad(100)),
        ]);
        assert_eq!(units(Some(&old), Some(&new), &gsi_include()), Some(2.0));
    }

    #[test]
    fn key_move_charges_both_halves_separately() {
        // Capture S4: a move with 1517B on both sides reports 4. Rounding the
        // summed bytes once would give 3, and the larger image alone would give 2.
        let old = item(&[
            ("pk", "s4"),
            ("sk", "1"),
            ("gsiPk", "A"),
            ("proj", &pad(1500)),
        ]);
        let new = item(&[
            ("pk", "s4"),
            ("sk", "1"),
            ("gsiPk", "B"),
            ("proj", &pad(1500)),
        ]);
        assert_eq!(units(Some(&old), Some(&new), &gsi_include()), Some(4.0));
    }

    #[test]
    fn key_move_sizes_each_half_on_its_own_image() {
        // Capture S5: 3017B moving to an 18B entry reports 4, which is 3 + 1.
        // Twice the larger half would give 6.
        let old = item(&[
            ("pk", "s5"),
            ("sk", "1"),
            ("gsiPk", "A"),
            ("proj", &pad(3000)),
        ]);
        let new = item(&[("pk", "s5"), ("sk", "1"), ("gsiPk", "B"), ("proj", "p")]);
        assert_eq!(units(Some(&old), Some(&new), &gsi_include()), Some(4.0));
    }

    #[test]
    fn sub_kilobyte_key_move_costs_two() {
        // #176: UpdateItem SET of a new gsiPk value reports gsi 2.
        let old = item(&[("pk", "b"), ("sk", "1"), ("gsiPk", "g")]);
        let new = item(&[("pk", "b"), ("sk", "1"), ("gsiPk", "g2")]);
        assert_eq!(units(Some(&old), Some(&new), &gsi_include()), Some(2.0));
    }

    #[test]
    fn sizing_ignores_attributes_the_index_does_not_project() {
        // Capture S1: a 3023B base item with an 18B projected entry reports
        // table 3 and gsi 1.
        let new = item(&[
            ("pk", "s1"),
            ("sk", "1"),
            ("gsiPk", "g"),
            ("proj", "p"),
            ("other", &pad(3000)),
        ]);
        assert_eq!(units(None, Some(&new), &gsi_include()), Some(1.0));
        // The same item under an ALL projection stores the lot, so it costs 3.
        assert_eq!(units(None, Some(&new), &gsi_all()), Some(3.0));
    }

    #[test]
    fn lsi_follows_the_same_rules() {
        // Capture L1 and L2: an LSI in-place grow reports 3, and an LSI key move
        // from a 3017B entry to a tiny one reports 4.
        let small = item(&[("pk", "l1"), ("sk", "1"), ("lsiSk", "L"), ("proj", "p")]);
        let big = item(&[
            ("pk", "l1"),
            ("sk", "1"),
            ("lsiSk", "L"),
            ("proj", &pad(3000)),
        ]);
        assert_eq!(units(Some(&small), Some(&big), &lsi_include()), Some(3.0));

        let moved_from = item(&[
            ("pk", "l2"),
            ("sk", "1"),
            ("lsiSk", "A"),
            ("proj", &pad(3000)),
        ]);
        let moved_to = item(&[("pk", "l2"), ("sk", "1"), ("lsiSk", "B"), ("proj", "p")]);
        assert_eq!(
            units(Some(&moved_from), Some(&moved_to), &lsi_include()),
            Some(4.0)
        );
    }

    #[test]
    fn sparse_membership_decides_presence_on_each_side() {
        let member = item(&[("pk", "sp"), ("sk", "1"), ("lsiSk", "L")]);
        // Missing the sort key where one is defined.
        let no_sort_key = item(&[("pk", "sp"), ("sk", "1")]);
        assert_eq!(
            units(Some(&no_sort_key), Some(&member), &lsi_include()),
            Some(1.0)
        );
        assert_eq!(
            units(Some(&member), Some(&no_sort_key), &lsi_include()),
            Some(1.0)
        );

        // A non-scalar in a key position is not indexable either.
        let mut non_scalar = member.clone();
        non_scalar.insert(
            "lsiSk".to_string(),
            AttributeValue::L(vec![AttributeValue::S("nope".to_string())]),
        );
        assert_eq!(units(Some(&non_scalar), None, &lsi_include()), None);
    }

    #[test]
    fn numeric_rendering_change_is_a_real_change() {
        // N holds its digits as written, so 1 and 1.0 are different stored bytes
        // and the index view genuinely changes. Pinned so a future normalisation
        // of numbers on ingest surfaces here rather than as a capacity drift.
        let mut old = item(&[("pk", "n"), ("sk", "1"), ("gsiPk", "g")]);
        old.insert("proj".to_string(), AttributeValue::N("1".to_string()));
        let mut new = old.clone();
        new.insert("proj".to_string(), AttributeValue::N("1.0".to_string()));
        assert_eq!(units(Some(&old), Some(&new), &gsi_include()), Some(1.0));
    }

    #[test]
    fn round_tripped_item_compares_equal() {
        // The old image is parsed back out of stored JSON while the new image is
        // the caller's in-memory item, so the two must compare equal for an
        // identical overwrite to stay free.
        let original = item(&[("pk", "rt"), ("sk", "1"), ("gsiPk", "g"), ("proj", "p")]);
        let json = serde_json::to_string(&original).unwrap();
        let round_tripped: Item = serde_json::from_str(&json).unwrap();
        assert_eq!(
            units(Some(&round_tripped), Some(&original), &gsi_include()),
            None
        );
    }
}
