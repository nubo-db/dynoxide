//! Creation lifecycle for a vector index added to a live table.
//!
//! Real DynamoDB puts an index added through `UpdateTable` through a visible
//! `CREATING` phase before reporting it `ACTIVE`, refuses a search for the
//! whole of it, refuses to drop the table underneath it, and then keeps
//! refusing searches for minutes after it reports `ACTIVE` (characterised in
//! eu-west-2 on 2026-08-11 and 2026-08-21). Dynoxide's backfill is
//! synchronous, so without this the index is searchable in the same breath as
//! the call that added it, and a readiness check that polls for `ACTIVE` and
//! then searches passes here while failing against AWS.
//!
//! Status is derived from when the index was armed rather than stored as a
//! state that observation advances. That is what makes the `DeleteTable`
//! refusal a property rather than a budget: AWS refuses for the whole
//! backfill however many times you ask, and the documented readiness pattern
//! polls `DescribeTable` an unbounded number of times. Nothing here mutates on
//! read, so two surfaces sharing one engine, or two clients polling at once,
//! get the same answer in any order at any concurrency.
//!
//! Time comes from the [`Clock`](crate::storage_backend::Clock) the backend
//! already carries, so this repo's own tests drive it with a `ManualClock` and
//! never wait. An out-of-process caller sees the real window below.
//!
//! The `CreateTable` path is deliberately not armed: measured in eu-west-2 on
//! 2026-08-21, an index created with its table reaches `ACTIVE` in the same
//! `DescribeTable` poll as the table itself, so a window there would be a new
//! divergence rather than a fix.

use std::collections::HashMap;
use std::sync::Mutex;

/// How long an index added through `UpdateTable` spends allocating resources
/// before it starts backfilling.
///
/// `CREATING` is two phases on AWS, not one. For the first of them the table
/// reports `UPDATING`, `Backfilling` reports `false`, and a `Delete` of the
/// index is refused outright; DynamoDB says so in as many words, telling you to
/// retry during backfilling. Measured at roughly thirty seconds in eu-west-2 on
/// 2026-08-23; compressed here for the same reason the other windows are.
pub const BACKFILLING_AFTER_SECS: f64 = 5.0;

/// How long an index added through `UpdateTable` reports `CREATING` before it
/// reports `ACTIVE`.
///
/// AWS took about seventeen minutes for a 25-item table. The shape and the
/// ordering are what conformance asserts, not the magnitude, so this is chosen
/// to be comfortably observable by an out-of-process poller and no longer than
/// it needs to be. The conformance suite polls this walk at 1s and 5s against
/// 300s and 2400s ceilings, so the margin is two orders of magnitude either
/// side of any poll shape it might adopt.
pub const ACTIVE_AFTER_SECS: f64 = 15.0;

/// How long an index added through `UpdateTable` refuses a search for.
///
/// Longer than [`ACTIVE_AFTER_SECS`] on purpose: an index reports `ACTIVE`
/// before it will answer, which is the trap a poll-until-ACTIVE readiness
/// check falls into on AWS. Reproducing only the first threshold would leave
/// that check passing here and failing there.
pub const SEARCHABLE_AFTER_SECS: f64 = 30.0;

/// Where an index sits in the creation lifecycle at one instant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VectorIndexPhase {
    /// Allocating resources. The table reports `UPDATING`, the index
    /// `CREATING` with `Backfilling` false, searches refuse, and the index
    /// cannot be dropped yet.
    Allocating,
    /// Backfilling. The table is back to `ACTIVE`, the index still reports
    /// `CREATING` with `Backfilling` true, searches refuse, and the index can
    /// now be dropped, which is how a create is cancelled.
    Backfilling,
    /// Reports `ACTIVE` with `Backfilling` absent; searches still refuse.
    ActiveNotSearchable,
    /// Reports `ACTIVE` with `Backfilling` absent; searches are served.
    Searchable,
}

impl VectorIndexPhase {
    /// The `IndexStatus` string this phase reports.
    pub(crate) fn index_status(self) -> &'static str {
        match self {
            Self::Allocating | Self::Backfilling => "CREATING",
            Self::ActiveNotSearchable | Self::Searchable => "ACTIVE",
        }
    }

    /// The `Backfilling` field this phase reports, `None` meaning the field is
    /// absent rather than `false`.
    pub(crate) fn backfilling(self) -> Option<bool> {
        match self {
            Self::Allocating => Some(false),
            Self::Backfilling => Some(true),
            Self::ActiveNotSearchable | Self::Searchable => None,
        }
    }

    /// Whether a search is served.
    pub(crate) fn is_searchable(self) -> bool {
        matches!(self, Self::Searchable)
    }

    /// Whether the index still counts as being created, which is what holds a
    /// `DeleteTable` off. True for both halves of `CREATING`.
    pub(crate) fn is_creating(self) -> bool {
        matches!(self, Self::Allocating | Self::Backfilling)
    }

    /// Whether this phase holds the base table at `UPDATING`. Only the
    /// allocating half does; the table is back to `ACTIVE` for the backfill,
    /// which is the state a readiness check should be written against.
    pub(crate) fn holds_table_updating(self) -> bool {
        matches!(self, Self::Allocating)
    }

    /// Whether an `UpdateTable` may drop this index. Refused while it is
    /// allocating, which is what makes cancelling a create a two-step wait
    /// rather than something you can do the instant the create returns.
    pub(crate) fn accepts_delete(self) -> bool {
        !matches!(self, Self::Allocating)
    }
}

/// Derive the phase from when the index was armed and what the clock now says.
///
/// An index with no arming instant is `Searchable`: that is the `CreateTable`
/// path, and it is also every index after a restart, since the map is session
/// scoped. Both are harmless only because the backfill is synchronous, so the
/// data behind the index is already complete when arming happens and the one
/// thing lost is the wait. It would stop being harmless if backfill ever became
/// incremental.
///
/// A clock that has gone backwards since arming yields a negative elapsed and
/// so reports `Allocating`, which is the same answer as an index armed this
/// instant.
pub(crate) fn phase_of(armed_at: Option<f64>, now: f64) -> VectorIndexPhase {
    let Some(armed_at) = armed_at else {
        return VectorIndexPhase::Searchable;
    };
    let elapsed = now - armed_at;
    if elapsed < BACKFILLING_AFTER_SECS {
        VectorIndexPhase::Allocating
    } else if elapsed < ACTIVE_AFTER_SECS {
        VectorIndexPhase::Backfilling
    } else if elapsed < SEARCHABLE_AFTER_SECS {
        VectorIndexPhase::ActiveNotSearchable
    } else {
        VectorIndexPhase::Searchable
    }
}

/// The phase every vector index of one table is in, resolved at one instant.
///
/// Taken as an input by [`build_table_description`](super::build_table_description)
/// so two responses describing the same table cannot disagree about an index,
/// and so a caller with no lifecycle to consult (the `CreateTable` path) says
/// so by passing an empty set rather than by taking a different code path.
#[derive(Debug, Clone, Default)]
pub(crate) struct VectorIndexPhases(HashMap<String, VectorIndexPhase>);

impl VectorIndexPhases {
    /// The phase for one index. An index this set says nothing about is
    /// `Searchable`, which is the `CreateTable` path.
    pub(crate) fn get(&self, index_name: &str) -> VectorIndexPhase {
        self.0
            .get(index_name)
            .copied()
            .unwrap_or(VectorIndexPhase::Searchable)
    }

    /// Whether any index in the set is still allocating, which is what holds
    /// the base table at `UPDATING`.
    pub(crate) fn any_holding_table_updating(&self) -> bool {
        self.0.values().any(|p| p.holds_table_updating())
    }

    /// Whether any index in the set is still creating, which is what a
    /// `DeleteTable` refuses on.
    ///
    /// Only meaningful on a set built from the table's own definitions, as
    /// [`VectorIndexLifecycle::phases_of`] builds one. Asked of an unfiltered
    /// set it would let a stale entry, for an index the table has since
    /// dropped, refuse the delete forever.
    pub(crate) fn any_creating(&self) -> bool {
        self.0.values().any(|p| p.is_creating())
    }
}

/// When each vector index added through `UpdateTable` was armed, for one
/// engine instance.
///
/// Held on [`Database`](crate::Database) beside the idempotency caches and
/// shared by every clone of it, so the wire, MCP, and wasm surfaces all read
/// one answer. Session scoped: it does not survive a restart, and an index
/// armed before one reports `ACTIVE` afterwards.
#[derive(Default)]
pub struct VectorIndexLifecycle {
    /// Table name to index name to the epoch second the index was armed.
    armed: Mutex<HashMap<String, HashMap<String, f64>>>,
}

impl VectorIndexLifecycle {
    /// An engine instance with nothing creating.
    pub fn new() -> Self {
        Self::default()
    }

    /// The armed map, recovering rather than propagating a poisoned lock.
    ///
    /// The map holds only `String` and `f64`, so nothing under this lock can
    /// panic and leave it inconsistent. Treating a poisoning as fatal-but-silent
    /// would be worse than recovering: every read would fall back to "nothing
    /// armed", which is exactly the pre-lifecycle behaviour, so an engine would
    /// quietly stop reporting the window rather than say anything had gone
    /// wrong.
    fn armed(&self) -> std::sync::MutexGuard<'_, HashMap<String, HashMap<String, f64>>> {
        self.armed.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Record that `index_name` on `table_name` started creating now.
    ///
    /// Re-arming an index that already has an entry restarts its window, which
    /// is what dropping and re-adding an index under the same name should do.
    pub(crate) fn arm<S: crate::storage_backend::StorageBackend>(
        &self,
        storage: &S,
        table_name: &str,
        index_name: &str,
    ) {
        self.arm_at(table_name, index_name, now(storage));
    }

    /// Forget one index, whether or not it finished creating. Cancelling a
    /// still-creating index has to clear its entry or the table it sits on
    /// stays undeletable.
    pub(crate) fn disarm(&self, table_name: &str, index_name: &str) {
        let mut armed = self.armed();
        if let Some(table) = armed.get_mut(table_name) {
            table.remove(index_name);
            if table.is_empty() {
                armed.remove(table_name);
            }
        }
    }

    /// Forget every index on a table, for the path that drops it.
    pub(crate) fn forget_table(&self, table_name: &str) {
        self.armed().remove(table_name);
    }

    /// The phase of every index this engine has armed on `table_name`, read at
    /// the backend's own clock.
    ///
    /// Not filtered against the table's definitions. The caller that reports
    /// these walks the definitions itself and asks by name, so an index the
    /// table does not define is never looked up, and filtering would buy
    /// nothing while costing a parse of the definition JSON on every describe.
    /// For the same reason [`VectorIndexPhases::any_creating`] is not meaningful
    /// on what this returns.
    pub(crate) fn phases_armed_on<S: crate::storage_backend::StorageBackend>(
        &self,
        storage: &S,
        table_name: &str,
    ) -> VectorIndexPhases {
        self.phases_armed_on_at(table_name, now(storage))
    }

    /// The phase of each index in `index_names`, for the caller that asks
    /// whether *any* index is creating rather than about one it can name.
    ///
    /// Restricting it to the indexes the table still defines is what stops a
    /// stale entry, for an index that has since gone, from refusing a
    /// `DeleteTable`.
    pub(crate) fn phases_of<'a, S: crate::storage_backend::StorageBackend>(
        &self,
        storage: &S,
        table_name: &str,
        index_names: impl Iterator<Item = &'a str>,
    ) -> VectorIndexPhases {
        self.phases_of_at(table_name, index_names, now(storage))
    }

    /// The phase of one index the caller has already resolved against the
    /// table's definitions.
    ///
    /// For a path that holds the definition and would otherwise parse the
    /// table's vector index JSON a second time to ask one question. Resolving
    /// the name is all [`Self::phases_of`] wants the definitions for, so a
    /// caller that has already resolved it loses nothing by skipping them.
    pub(crate) fn phase_of_index<S: crate::storage_backend::StorageBackend>(
        &self,
        storage: &S,
        table_name: &str,
        index_name: &str,
    ) -> VectorIndexPhase {
        phase_of(self.armed_at(table_name, index_name), now(storage))
    }

    // The clock-free forms below carry the logic, so the unit tests can pin
    // every edge of it without a backend.

    fn arm_at(&self, table_name: &str, index_name: &str, now: f64) {
        self.armed()
            .entry(table_name.to_string())
            .or_default()
            .insert(index_name.to_string(), now);
    }

    /// When one index was armed, or `None` if it was not.
    fn armed_at(&self, table_name: &str, index_name: &str) -> Option<f64> {
        self.armed().get(table_name)?.get(index_name).copied()
    }

    fn phases_armed_on_at(&self, table_name: &str, now: f64) -> VectorIndexPhases {
        VectorIndexPhases(
            self.armed()
                .get(table_name)
                .into_iter()
                .flatten()
                .map(|(name, armed_at)| (name.clone(), phase_of(Some(*armed_at), now)))
                .collect(),
        )
    }

    fn phases_of_at<'a>(
        &self,
        table_name: &str,
        index_names: impl Iterator<Item = &'a str>,
        now: f64,
    ) -> VectorIndexPhases {
        let armed = self.armed();
        let table = armed.get(table_name);
        VectorIndexPhases(
            index_names
                .map(|name| {
                    let armed_at = table.and_then(|t| t.get(name)).copied();
                    (name.to_string(), phase_of(armed_at, now))
                })
                .collect(),
        )
    }
}

/// The backend's own clock, as fractional epoch seconds.
fn now<S: crate::storage_backend::StorageBackend>(storage: &S) -> f64 {
    storage.clock().now_unix_secs_f64()
}

#[cfg(test)]
mod tests {
    use super::*;

    const ARMED: f64 = 1_700_000_000.0;

    #[test]
    fn while_allocating_the_table_updates_and_the_index_refuses_everything() {
        let phase = phase_of(Some(ARMED), ARMED + BACKFILLING_AFTER_SECS - 0.001);
        assert_eq!(phase, VectorIndexPhase::Allocating);
        assert_eq!(phase.index_status(), "CREATING");
        assert_eq!(phase.backfilling(), Some(false));
        assert!(!phase.is_searchable());
        assert!(phase.is_creating());
        assert!(phase.holds_table_updating());
        assert!(!phase.accepts_delete());
    }

    #[test]
    fn while_backfilling_the_table_is_active_and_the_index_takes_a_delete() {
        let phase = phase_of(Some(ARMED), ARMED + ACTIVE_AFTER_SECS - 0.001);
        assert_eq!(phase, VectorIndexPhase::Backfilling);
        assert_eq!(phase.index_status(), "CREATING");
        assert_eq!(phase.backfilling(), Some(true));
        assert!(!phase.is_searchable());
        assert!(phase.is_creating());
        assert!(!phase.holds_table_updating());
        assert!(phase.accepts_delete());
    }

    #[test]
    fn between_the_thresholds_reports_active_and_still_refuses_a_search() {
        let phase = phase_of(Some(ARMED), ARMED + ACTIVE_AFTER_SECS + 0.001);
        assert_eq!(phase, VectorIndexPhase::ActiveNotSearchable);
        assert_eq!(phase.index_status(), "ACTIVE");
        assert_eq!(phase.backfilling(), None);
        assert!(!phase.is_searchable());
        assert!(!phase.is_creating());
        assert!(!phase.holds_table_updating());
        assert!(phase.accepts_delete());
    }

    #[test]
    fn beyond_the_searchable_threshold_serves_the_search() {
        let phase = phase_of(Some(ARMED), ARMED + SEARCHABLE_AFTER_SECS + 0.001);
        assert_eq!(phase, VectorIndexPhase::Searchable);
        assert_eq!(phase.index_status(), "ACTIVE");
        assert_eq!(phase.backfilling(), None);
        assert!(phase.is_searchable());
    }

    /// Both thresholds are exclusive lower bounds on the phase they end, so
    /// exactly on one resolves to the later phase.
    #[test]
    fn exactly_on_a_threshold_resolves_to_the_later_phase() {
        assert_eq!(
            phase_of(Some(ARMED), ARMED + BACKFILLING_AFTER_SECS),
            VectorIndexPhase::Backfilling
        );
        assert_eq!(
            phase_of(Some(ARMED), ARMED + ACTIVE_AFTER_SECS),
            VectorIndexPhase::ActiveNotSearchable
        );
        assert_eq!(
            phase_of(Some(ARMED), ARMED + SEARCHABLE_AFTER_SECS),
            VectorIndexPhase::Searchable
        );
    }

    #[test]
    fn an_index_with_no_entry_is_active_and_searchable() {
        let phase = phase_of(None, ARMED);
        assert_eq!(phase, VectorIndexPhase::Searchable);
        assert_eq!(phase.index_status(), "ACTIVE");
        assert_eq!(phase.backfilling(), None);
        assert!(phase.is_searchable());
    }

    #[test]
    fn a_clock_that_went_backwards_reads_as_just_armed() {
        assert_eq!(
            phase_of(Some(ARMED), ARMED - 60.0),
            VectorIndexPhase::Allocating
        );
    }

    #[test]
    fn reading_does_not_change_what_a_later_read_returns() {
        let lifecycle = VectorIndexLifecycle::new();
        lifecycle.arm_at("t", "vix", ARMED);

        for _ in 0..50 {
            let phases = lifecycle.phases_of_at("t", ["vix"].into_iter(), ARMED + 1.0);
            assert_eq!(phases.get("vix"), VectorIndexPhase::Allocating);
            assert!(phases.any_creating());
        }

        let later = lifecycle.phases_of_at("t", ["vix"].into_iter(), ARMED + SEARCHABLE_AFTER_SECS);
        assert_eq!(later.get("vix"), VectorIndexPhase::Searchable);
        assert!(!later.any_creating());
    }

    /// The describe path asks by name from the table's own definitions, so the
    /// unfiltered set may name an index the table has since dropped. It must
    /// still answer correctly for the ones it does define, and must not report
    /// another table's.
    #[test]
    fn the_unfiltered_set_carries_every_armed_index_on_that_table_alone() {
        let lifecycle = VectorIndexLifecycle::new();
        lifecycle.arm_at("t", "a", ARMED);
        lifecycle.arm_at("t", "b", ARMED - SEARCHABLE_AFTER_SECS);
        lifecycle.arm_at("other", "c", ARMED);

        let phases = lifecycle.phases_armed_on_at("t", ARMED);
        assert_eq!(phases.get("a"), VectorIndexPhase::Allocating);
        assert_eq!(phases.get("b"), VectorIndexPhase::Searchable);
        assert_eq!(phases.get("c"), VectorIndexPhase::Searchable);
        assert!(phases.any_creating());

        let none = lifecycle.phases_armed_on_at("nothing-armed-here", ARMED);
        assert_eq!(none.get("a"), VectorIndexPhase::Searchable);
        assert!(!none.any_creating());
    }

    #[test]
    fn an_index_the_table_no_longer_defines_is_not_reported() {
        let lifecycle = VectorIndexLifecycle::new();
        lifecycle.arm_at("t", "gone", ARMED);

        let phases = lifecycle.phases_of_at("t", ["still-here"].into_iter(), ARMED + 1.0);
        assert_eq!(phases.get("still-here"), VectorIndexPhase::Searchable);
        assert!(!phases.any_creating());
    }

    #[test]
    fn disarming_forgets_one_index_and_leaves_its_neighbour() {
        let lifecycle = VectorIndexLifecycle::new();
        lifecycle.arm_at("t", "a", ARMED);
        lifecycle.arm_at("t", "b", ARMED);
        lifecycle.disarm("t", "a");

        let phases = lifecycle.phases_of_at("t", ["a", "b"].into_iter(), ARMED + 1.0);
        assert_eq!(phases.get("a"), VectorIndexPhase::Searchable);
        assert_eq!(phases.get("b"), VectorIndexPhase::Allocating);
    }

    #[test]
    fn forgetting_a_table_clears_every_index_on_it() {
        let lifecycle = VectorIndexLifecycle::new();
        lifecycle.arm_at("t", "a", ARMED);
        lifecycle.arm_at("other", "a", ARMED);
        lifecycle.forget_table("t");

        let gone = lifecycle.phases_of_at("t", ["a"].into_iter(), ARMED + 1.0);
        assert_eq!(gone.get("a"), VectorIndexPhase::Searchable);
        let kept = lifecycle.phases_of_at("other", ["a"].into_iter(), ARMED + 1.0);
        assert_eq!(kept.get("a"), VectorIndexPhase::Allocating);
    }

    #[test]
    fn re_arming_restarts_the_window() {
        let lifecycle = VectorIndexLifecycle::new();
        lifecycle.arm_at("t", "vix", ARMED);
        lifecycle.arm_at("t", "vix", ARMED + SEARCHABLE_AFTER_SECS);

        let phases = lifecycle.phases_of_at(
            "t",
            ["vix"].into_iter(),
            ARMED + SEARCHABLE_AFTER_SECS + 1.0,
        );
        assert_eq!(phases.get("vix"), VectorIndexPhase::Allocating);
    }
}
