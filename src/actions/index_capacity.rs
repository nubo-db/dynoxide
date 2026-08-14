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
use crate::types::{
    AttributeValue, Item, item_size, table_write_capacity_units, write_capacity_units,
};
use std::collections::HashMap;

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
        (Some(old), Some(new)) if unchanged(&old.projected, &new.projected) => None,
        (Some(old), Some(new)) => {
            // Overwritten in place, and charged on the larger of the two images
            // rather than on the one left behind.
            Some(write_capacity_units(old.size).max(write_capacity_units(new.size)))
        }
    }
}

/// Whether two projected entries hold the same thing, in DynamoDB's terms.
///
/// Sets are unordered, so a re-put listing the same members in a different order
/// leaves the index's stored view untouched and costs nothing. `SS`, `NS` and
/// `BS` are backed by `Vec`, so a derived comparison would call that a change
/// and charge for it. Lists keep their order, because DynamoDB lists are
/// ordered.
fn unchanged(old: &Item, new: &Item) -> bool {
    old.len() == new.len()
        && old
            .iter()
            .all(|(name, a)| new.get(name).is_some_and(|b| value_unchanged(a, b)))
}

fn value_unchanged(a: &AttributeValue, b: &AttributeValue) -> bool {
    match (a, b) {
        (AttributeValue::SS(x), AttributeValue::SS(y)) => set_unchanged(x, y),
        (AttributeValue::NS(x), AttributeValue::NS(y)) => set_unchanged(x, y),
        (AttributeValue::BS(x), AttributeValue::BS(y)) => set_unchanged(x, y),
        (AttributeValue::L(x), AttributeValue::L(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(p, q)| value_unchanged(p, q))
        }
        (AttributeValue::M(x), AttributeValue::M(y)) => unchanged(x, y),
        _ => a == b,
    }
}

fn set_unchanged<T: Ord>(x: &[T], y: &[T]) -> bool {
    if x.len() != y.len() {
        return false;
    }
    let mut x: Vec<&T> = x.iter().collect();
    let mut y: Vec<&T> = y.iter().collect();
    x.sort_unstable();
    y.sort_unstable();
    x == y
}

/// What one write contributes to `ConsumedCapacity`.
///
/// The base table arm is sized on the images either side of the write and the
/// index arms come from the maintenance helpers, which have already applied the
/// delta rules above. Holding the three apart until aggregation is what keeps
/// the transactional factor off the index arms.
#[derive(Debug, Clone, Default)]
pub struct WriteCapacity {
    pub table_name: String,
    /// The item before the write, absent when nothing was there.
    pub old_size: Option<usize>,
    /// The item after the write, absent on a delete.
    pub new_size: Option<usize>,
    pub gsi_units: HashMap<String, f64>,
    pub lsi_units: HashMap<String, f64>,
}

impl WriteCapacity {
    /// Build from the image sizes either side of a write and the per-index
    /// units the maintenance helpers returned.
    ///
    /// Callers that already hold a size should pass it: the write paths compute
    /// the new image's size to enforce the item-size limit, and walking the item
    /// again here would be the same work twice.
    pub fn new(
        table_name: &str,
        old_size: Option<usize>,
        new_size: Option<usize>,
        gsi_units: HashMap<String, f64>,
        lsi_units: HashMap<String, f64>,
    ) -> Self {
        Self {
            table_name: table_name.to_string(),
            old_size,
            new_size,
            gsi_units,
            lsi_units,
        }
    }

    /// Build from the images themselves, for callers that do not already hold
    /// their sizes.
    pub fn from_items(
        table_name: &str,
        old_item: Option<&Item>,
        new_item: Option<&Item>,
        gsi_units: HashMap<String, f64>,
        lsi_units: HashMap<String, f64>,
    ) -> Self {
        Self::new(
            table_name,
            old_item.map(item_size),
            new_item.map(item_size),
            gsi_units,
            lsi_units,
        )
    }

    /// A `ConditionCheck` writes nothing and touches no index, and DynamoDB
    /// still charges it against the image it reads. `item` is `None` when the
    /// target does not exist, which falls back to the one-unit minimum.
    ///
    /// The record carries no after-image, because the check leaves none behind.
    /// `table_write_capacity_units` charges a present-then-absent pair on the
    /// old image alone, which is the captured figure.
    pub fn condition_check(table_name: &str, item: Option<&Item>) -> Self {
        Self::from_items(table_name, item, None, HashMap::new(), HashMap::new())
    }
}

/// One table's share of a multi-action operation.
#[derive(Default)]
pub struct TableCapacity {
    pub table_units: f64,
    pub gsi_units: HashMap<String, f64>,
    pub lsi_units: HashMap<String, f64>,
}

/// Shapes one table's entry. The transactional surfaces mirror their units into
/// a write axis; the single-statement and batch ones report `CapacityUnits`
/// alone, so the two builders are not interchangeable.
type CapacityBuilder = fn(
    &str,
    f64,
    &HashMap<String, f64>,
    &HashMap<String, f64>,
    &Option<String>,
) -> Option<crate::types::ConsumedCapacity>;

/// Per-table units for a transactional read: 2 RCU per entry, rounded at 4KB
/// read granularity before the factor, summed by table.
///
/// Serves both an all-read transaction and a same-token replay, which are the
/// same arithmetic over the same `(table, image size)` pairs. The replay's
/// sizes come from the first call, because the request cannot supply them for
/// an action that carries only a key.
pub fn transactional_read_units(sizes: &[(String, usize)]) -> HashMap<String, f64> {
    let mut table_units: HashMap<String, f64> = HashMap::new();
    for (table, size) in sizes {
        *table_units.entry(table.clone()).or_default() +=
            crate::types::TRANSACTIONAL_CAPACITY_FACTOR * crate::types::read_capacity_units(*size);
    }
    table_units
}

/// Turn per-table totals into one `ConsumedCapacity` per table.
///
/// Sorted by table name. Aggregation is a `HashMap`, which would otherwise hand
/// back a different order on every call and leave a caller indexing into a
/// shuffled array.
///
/// Returns `None` when no capacity was asked for, which is distinct from an
/// empty vec: the response omits the field entirely rather than carrying an
/// empty list.
pub fn per_table_capacity(
    by_table: &HashMap<String, TableCapacity>,
    mode: &Option<String>,
    builder: CapacityBuilder,
) -> Option<Vec<crate::types::ConsumedCapacity>> {
    if !matches!(mode.as_deref(), Some("TOTAL") | Some("INDEXES")) {
        return None;
    }

    let mut tables: Vec<&String> = by_table.keys().collect();
    tables.sort();

    Some(
        tables
            .into_iter()
            .filter_map(|table| {
                let units = by_table.get(table)?;
                builder(
                    table,
                    units.table_units,
                    &units.gsi_units,
                    &units.lsi_units,
                    mode,
                )
            })
            .collect(),
    )
}

/// Fold per-action records into per-table totals.
///
/// `factor` is the transactional multiplier: 2 for `TransactWriteItems` and
/// `ExecuteTransaction`, 1 for the single-statement and batch surfaces. It
/// reaches the base table arm only. A capture against real DynamoDB pins index
/// arms at their single-write cost inside a transaction, so a GSI key move at
/// 1517B per side costs 4 whether or not it happens transactionally, while the
/// table arm on that same write doubles.
///
/// Each action's table units are rounded and multiplied before they are summed,
/// so an item straddling a KB boundary is not undercharged by aggregating bytes
/// first.
pub fn aggregate_by_table(
    records: &[WriteCapacity],
    factor: f64,
) -> HashMap<String, TableCapacity> {
    let mut by_table: HashMap<String, TableCapacity> = HashMap::new();

    for record in records {
        let entry = by_table.entry(record.table_name.clone()).or_default();
        entry.table_units += factor * table_write_capacity_units(record.old_size, record.new_size);
        for (name, units) in &record.gsi_units {
            *entry.gsi_units.entry(name.clone()).or_default() += units;
        }
        for (name, units) in &record.lsi_units {
            *entry.lsi_units.entry(name.clone()).or_default() += units;
        }
    }

    by_table
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ProjectionType;

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
    fn a_reordered_set_is_not_a_change() {
        // DynamoDB sets are unordered, so re-putting the same members in another
        // order leaves the index's stored view alone and costs nothing. The
        // members are backed by a Vec, so comparing them directly would charge.
        let mut old = item(&[("pk", "v1"), ("sk", "1"), ("gsiPk", "g")]);
        let mut new = old.clone();
        old.insert(
            "proj".to_string(),
            AttributeValue::SS(vec!["a".to_string(), "b".to_string(), "c".to_string()]),
        );
        new.insert(
            "proj".to_string(),
            AttributeValue::SS(vec!["c".to_string(), "a".to_string(), "b".to_string()]),
        );
        assert_eq!(units(Some(&old), Some(&new), &gsi_include()), None);
    }

    #[test]
    fn a_reordered_number_set_is_not_a_change() {
        let mut old = item(&[("pk", "v1"), ("sk", "1"), ("gsiPk", "g")]);
        let mut new = old.clone();
        old.insert(
            "proj".to_string(),
            AttributeValue::NS(vec!["1".to_string(), "2".to_string(), "3".to_string()]),
        );
        new.insert(
            "proj".to_string(),
            AttributeValue::NS(vec!["3".to_string(), "1".to_string(), "2".to_string()]),
        );
        assert_eq!(units(Some(&old), Some(&new), &gsi_include()), None);
    }

    #[test]
    fn a_reordered_binary_set_is_not_a_change() {
        let mut old = item(&[("pk", "v1"), ("sk", "1"), ("gsiPk", "g")]);
        let mut new = old.clone();
        old.insert(
            "proj".to_string(),
            AttributeValue::BS(vec![vec![1, 2], vec![3], vec![4, 5, 6]]),
        );
        new.insert(
            "proj".to_string(),
            AttributeValue::BS(vec![vec![4, 5, 6], vec![1, 2], vec![3]]),
        );
        assert_eq!(units(Some(&old), Some(&new), &gsi_include()), None);
    }

    #[test]
    fn renaming_an_attribute_is_a_change() {
        // Both sides hold the same number of attributes and the same values, so
        // a comparison that only counted entries would call this unchanged.
        let old = item(&[("pk", "v1"), ("sk", "1"), ("gsiPk", "g"), ("before", "v")]);
        let new = item(&[("pk", "v1"), ("sk", "1"), ("gsiPk", "g"), ("after", "v")]);
        assert_eq!(units(Some(&old), Some(&new), &gsi_all()), Some(1.0));
    }

    #[test]
    fn a_changed_set_member_is_still_a_change() {
        // The order-insensitive comparison must not swallow a real edit.
        let mut old = item(&[("pk", "v1"), ("sk", "1"), ("gsiPk", "g")]);
        let mut new = old.clone();
        old.insert(
            "proj".to_string(),
            AttributeValue::SS(vec!["a".to_string(), "b".to_string()]),
        );
        new.insert(
            "proj".to_string(),
            AttributeValue::SS(vec!["b".to_string(), "z".to_string()]),
        );
        assert_eq!(units(Some(&old), Some(&new), &gsi_include()), Some(1.0));
    }

    #[test]
    fn a_reordered_list_is_a_change() {
        // Lists are ordered, unlike sets, so reordering one is a real edit.
        let mut old = item(&[("pk", "v1"), ("sk", "1"), ("gsiPk", "g")]);
        let mut new = old.clone();
        old.insert("proj".to_string(), AttributeValue::L(vec![s("a"), s("b")]));
        new.insert("proj".to_string(), AttributeValue::L(vec![s("b"), s("a")]));
        assert_eq!(units(Some(&old), Some(&new), &gsi_include()), Some(1.0));
    }

    #[test]
    fn a_reordered_set_nested_in_a_map_is_not_a_change() {
        // The comparison has to reach sets inside M and L, not just top level.
        let mut old = item(&[("pk", "v1"), ("sk", "1"), ("gsiPk", "g")]);
        let mut new = old.clone();
        let nest = |members: Vec<&str>| {
            let mut m: HashMap<String, AttributeValue> = HashMap::new();
            m.insert(
                "tags".to_string(),
                AttributeValue::SS(members.into_iter().map(str::to_string).collect()),
            );
            AttributeValue::M(m)
        };
        old.insert("proj".to_string(), nest(vec!["a", "b"]));
        new.insert("proj".to_string(), nest(vec!["b", "a"]));
        assert_eq!(units(Some(&old), Some(&new), &gsi_include()), None);
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

    fn units_map(pairs: &[(&str, f64)]) -> HashMap<String, f64> {
        pairs
            .iter()
            .map(|(name, u)| ((*name).to_string(), *u))
            .collect()
    }

    /// One table's aggregate, looked up by name.
    fn table_of<'a>(agg: &'a HashMap<String, TableCapacity>, name: &str) -> &'a TableCapacity {
        agg.get(name).expect("table missing from aggregate")
    }

    #[test]
    fn record_takes_its_sizes_from_the_images() {
        let new = item(&[("pk", "a"), ("sk", "1"), ("gsiPk", "g")]);
        let insert =
            WriteCapacity::from_items("t", None, Some(&new), HashMap::new(), HashMap::new());
        assert_eq!(insert.old_size, None);
        assert_eq!(insert.new_size, Some(crate::types::item_size(&new)));

        let delete =
            WriteCapacity::from_items("t", Some(&new), None, HashMap::new(), HashMap::new());
        assert_eq!(delete.old_size, Some(crate::types::item_size(&new)));
        assert_eq!(delete.new_size, None);
    }

    #[test]
    fn record_carries_one_arm_per_index_kind() {
        // Capture A3: a transactional put of an item in both indexes reports
        // gsi 1 and lsi 1.
        let new = item(&[("pk", "a3"), ("sk", "1"), ("gsiPk", "g"), ("lsiSk", "L")]);
        let record = WriteCapacity::from_items(
            "t",
            None,
            Some(&new),
            units_map(&[("gsi-inc", 1.0)]),
            units_map(&[("lsi-all", 1.0)]),
        );
        assert_eq!(record.gsi_units.len(), 1);
        assert_eq!(record.lsi_units.len(), 1);
    }

    #[test]
    fn identical_overwrite_keeps_its_sizes_and_drops_its_arms() {
        // Capture K3: an identical overwrite of a 3037B item is charged table 6
        // and no index arm. The table arm and the index arms are independent
        // readings of the same write.
        let same = item(&[
            ("pk", "k3"),
            ("sk", "1"),
            ("gsiPk", "g"),
            ("proj", &pad(3000)),
        ]);
        let record = WriteCapacity::from_items(
            "t",
            Some(&same),
            Some(&same),
            HashMap::new(),
            HashMap::new(),
        );
        assert!(record.gsi_units.is_empty());
        assert!(record.lsi_units.is_empty());

        let agg = aggregate_by_table(&[record], crate::types::TRANSACTIONAL_CAPACITY_FACTOR);
        assert_eq!(table_of(&agg, "t").table_units, 6.0);
    }

    #[test]
    fn transactional_factor_reaches_the_table_arm_and_not_the_index_arms() {
        // The finding this whole change rests on. Capture A1: a transactional put
        // of a sub-1KB GSI member reports table 2 and gsi 1. The single-item
        // equivalent reports table 1 and gsi 1, so the table arm doubles and the
        // index arm does not move.
        let new = item(&[("pk", "a1"), ("sk", "1"), ("gsiPk", "g")]);
        let record = WriteCapacity::from_items(
            "t",
            None,
            Some(&new),
            units_map(&[("gsi-inc", 1.0)]),
            units_map(&[("lsi-all", 1.0)]),
        );

        let agg = aggregate_by_table(&[record], crate::types::TRANSACTIONAL_CAPACITY_FACTOR);
        let t = table_of(&agg, "t");
        assert_eq!(t.table_units, 2.0);
        assert_eq!(t.gsi_units.get("gsi-inc"), Some(&1.0));
        assert_eq!(t.lsi_units.get("lsi-all"), Some(&1.0));
    }

    #[test]
    fn factor_of_one_leaves_the_table_arm_undoubled() {
        // Capture C1: the same write through ExecuteStatement carries no
        // transactional factor and reports table 1.
        let new = item(&[("pk", "c1"), ("sk", "1"), ("gsiPk", "g")]);
        let record = WriteCapacity::from_items(
            "t",
            None,
            Some(&new),
            units_map(&[("gsi-inc", 1.0)]),
            HashMap::new(),
        );

        let agg = aggregate_by_table(&[record], 1.0);
        assert_eq!(table_of(&agg, "t").table_units, 1.0);
        assert_eq!(table_of(&agg, "t").gsi_units.get("gsi-inc"), Some(&1.0));
    }

    #[test]
    fn two_actions_against_one_table_sum_every_arm() {
        // Capture A13: two transactional puts, both indexed, report total 8 with
        // table 4, gsi 2 and lsi 2.
        let a = item(&[("pk", "a13"), ("sk", "1"), ("gsiPk", "g"), ("lsiSk", "L")]);
        let b = item(&[("pk", "a14"), ("sk", "1"), ("gsiPk", "g"), ("lsiSk", "L")]);
        let records = vec![
            WriteCapacity::from_items(
                "t",
                None,
                Some(&a),
                units_map(&[("gsi-inc", 1.0)]),
                units_map(&[("lsi-all", 1.0)]),
            ),
            WriteCapacity::from_items(
                "t",
                None,
                Some(&b),
                units_map(&[("gsi-inc", 1.0)]),
                units_map(&[("lsi-all", 1.0)]),
            ),
        ];

        let agg = aggregate_by_table(&records, crate::types::TRANSACTIONAL_CAPACITY_FACTOR);
        let t = table_of(&agg, "t");
        assert_eq!(t.table_units, 4.0);
        assert_eq!(t.gsi_units.get("gsi-inc"), Some(&2.0));
        assert_eq!(t.lsi_units.get("lsi-all"), Some(&2.0));
    }

    #[test]
    fn actions_against_different_tables_aggregate_separately() {
        // Capture G1: one put per table reports one entry each, carrying only
        // that table's arms.
        let a = item(&[("pk", "g1"), ("sk", "1"), ("gsiPk", "g"), ("lsiSk", "L")]);
        let b = item(&[("pk", "g1"), ("sk", "1"), ("gsiPk", "g")]);
        let records = vec![
            WriteCapacity::from_items(
                "first",
                None,
                Some(&a),
                units_map(&[("gsi-inc", 1.0)]),
                units_map(&[("lsi-all", 1.0)]),
            ),
            WriteCapacity::from_items(
                "second",
                None,
                Some(&b),
                units_map(&[("gsi-inc", 1.0)]),
                HashMap::new(),
            ),
        ];

        let agg = aggregate_by_table(&records, crate::types::TRANSACTIONAL_CAPACITY_FACTOR);
        assert_eq!(agg.len(), 2);
        assert_eq!(table_of(&agg, "first").table_units, 2.0);
        assert!(table_of(&agg, "first").lsi_units.contains_key("lsi-all"));
        assert_eq!(table_of(&agg, "second").table_units, 2.0);
        assert!(table_of(&agg, "second").lsi_units.is_empty());
    }

    #[test]
    fn an_action_with_no_image_either_side_still_costs_the_minimum() {
        // Capture K1 and K2: a transactional delete or condition check against a
        // target that does not exist reports table 2, which is the one-unit
        // minimum doubled.
        let record = WriteCapacity::from_items("t", None, None, HashMap::new(), HashMap::new());
        let agg = aggregate_by_table(&[record], crate::types::TRANSACTIONAL_CAPACITY_FACTOR);
        assert_eq!(table_of(&agg, "t").table_units, 2.0);
    }

    #[test]
    fn condition_check_against_a_missing_target_costs_the_minimum() {
        // Capture K2: a condition check whose target does not exist reports
        // table 2. Built through the constructor the action actually calls,
        // rather than through an equivalent-looking `new`.
        let record = WriteCapacity::condition_check("t", None);
        assert_eq!(record.old_size, None);
        assert_eq!(record.new_size, None);

        let agg = aggregate_by_table(&[record], crate::types::TRANSACTIONAL_CAPACITY_FACTOR);
        assert_eq!(table_of(&agg, "t").table_units, 2.0);
    }

    #[test]
    fn condition_check_is_sized_on_the_image_it_reads() {
        // Capture F1: a condition check against a 3023B item reports table 6,
        // despite writing nothing. Sizing it on the key would report 2.
        let existing = item(&[("pk", "f1"), ("sk", "1"), ("other", &pad(3000))]);
        let record = WriteCapacity::condition_check("t", Some(&existing));
        assert!(record.gsi_units.is_empty());
        assert!(record.lsi_units.is_empty());

        let agg = aggregate_by_table(&[record], crate::types::TRANSACTIONAL_CAPACITY_FACTOR);
        assert_eq!(table_of(&agg, "t").table_units, 6.0);
    }

    #[test]
    fn a_key_move_doubles_its_table_arm_and_leaves_its_index_arm_alone() {
        // Capture B7: a GSI key move with 1517B on both sides reports table 4 and
        // gsi 4. The two fours have different arithmetic behind them: the table is
        // 2 x ceil(1.5KB base item) and the GSI is ceil(1517) + ceil(1517),
        // undoubled. Doubling the index arm would report 8.
        let old = item(&[
            ("pk", "b7"),
            ("sk", "1"),
            ("gsiPk", "A"),
            ("proj", &pad(1500)),
        ]);
        let new = item(&[
            ("pk", "b7"),
            ("sk", "1"),
            ("gsiPk", "B"),
            ("proj", &pad(1500)),
        ]);
        let arm = units(Some(&old), Some(&new), &gsi_include()).unwrap();
        assert_eq!(arm, 4.0);

        let record = WriteCapacity::from_items(
            "t",
            Some(&old),
            Some(&new),
            units_map(&[("gsi-inc", arm)]),
            HashMap::new(),
        );
        let agg = aggregate_by_table(&[record], crate::types::TRANSACTIONAL_CAPACITY_FACTOR);
        let t = table_of(&agg, "t");
        assert_eq!(t.table_units, 4.0);
        assert_eq!(t.gsi_units.get("gsi-inc"), Some(&4.0));
    }

    #[test]
    fn a_shrinking_write_is_sized_on_the_image_it_replaced() {
        // Capture B3: a transactional put shrinking a 3023B item to 18B reports
        // table 6. Sizing on the request payload reports 2.
        let old = item(&[("pk", "b3"), ("sk", "1"), ("other", &pad(3000))]);
        let new = item(&[("pk", "b3"), ("sk", "1")]);
        let record =
            WriteCapacity::from_items("t", Some(&old), Some(&new), HashMap::new(), HashMap::new());
        let agg = aggregate_by_table(&[record], crate::types::TRANSACTIONAL_CAPACITY_FACTOR);
        assert_eq!(table_of(&agg, "t").table_units, 6.0);
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
