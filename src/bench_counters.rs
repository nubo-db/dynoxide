//! Counters for work the engine repeats, compiled out unless asked for.
//!
//! Two of the open performance questions are about how many times something
//! happens rather than how long it takes: whether an index entry is rebuilt when
//! one was already to hand, and whether a batch resolves the same table's
//! metadata once or twice per statement. A count answers those exactly and the
//! same on every machine, where a timing does not.
//!
//! That matters more than it sounds. The wall-clock suite runs Criterion, which
//! `AGENTS.md` calls advisory because a shared runner moves it by up to roughly
//! 3x, and the deterministic instrument, iai-callgrind, needs Valgrind and so
//! runs on Linux only. A count needs neither.
//!
//! It also answers a question that cannot be measured where it happens. The
//! wasm backend crosses a bridge to a JS worker for every metadata read and
//! caches nothing, so the cost there is the number of calls; and the number of
//! calls is decided by the action code, not by the backend under it. Counting
//! natively gives the wasm figure without a browser.
//!
//! Enabled by the `bench-counters` feature. Without it every counter is a unit
//! struct and every `record` an empty inline function, so nothing reaches a
//! release build.

#[cfg(feature = "bench-counters")]
mod enabled {
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Projected index entries built, by `actions::gsi::build_index_item`.
    pub static INDEX_ENTRIES_BUILT: AtomicUsize = AtomicUsize::new(0);
    /// Table metadata reads reaching the storage backend.
    pub static METADATA_READS: AtomicUsize = AtomicUsize::new(0);
    /// Key schemas parsed out of a table's metadata JSON.
    pub static KEY_SCHEMA_PARSES: AtomicUsize = AtomicUsize::new(0);

    pub fn record(counter: &AtomicUsize) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Zero every counter. Call before the operation under measurement.
    pub fn reset() {
        for counter in [&INDEX_ENTRIES_BUILT, &METADATA_READS, &KEY_SCHEMA_PARSES] {
            counter.store(0, Ordering::Relaxed);
        }
    }

    /// What the counters read now.
    pub fn snapshot() -> Counts {
        Counts {
            index_entries_built: INDEX_ENTRIES_BUILT.load(Ordering::Relaxed),
            metadata_reads: METADATA_READS.load(Ordering::Relaxed),
            key_schema_parses: KEY_SCHEMA_PARSES.load(Ordering::Relaxed),
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct Counts {
        pub index_entries_built: usize,
        pub metadata_reads: usize,
        pub key_schema_parses: usize,
    }
}

#[cfg(not(feature = "bench-counters"))]
mod enabled {
    /// Stands in for a counter when the feature is off, so call sites read the
    /// same either way and cost nothing.
    pub struct Counter;
    pub static INDEX_ENTRIES_BUILT: Counter = Counter;
    pub static METADATA_READS: Counter = Counter;
    pub static KEY_SCHEMA_PARSES: Counter = Counter;

    #[inline(always)]
    pub fn record(_counter: &Counter) {}
}

pub use enabled::*;
