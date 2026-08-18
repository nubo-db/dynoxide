//! How many times the engine repeats work it has already done.
//!
//! Two open performance questions are about counts rather than durations:
//! whether an index entry is rebuilt when one was already to hand, and whether a
//! batch resolves the same table's metadata once or twice per statement. Both
//! are answered exactly here and identically on every machine.
//!
//! Run with `cargo test --features bench-counters --test repeated_work_counts`.
//! Without the feature the counters compile to nothing and every test skips, so
//! the default `cargo test` is unaffected.
//!
//! The counters are process-wide, and the test harness runs tests in parallel by
//! default, so a measurement can pick up another test's work. That is not a
//! hypothetical: read under parallel threads, an insert into a two-index table
//! reported 8 entries built where the true figure is 2, and an insert costing
//! more than an overwrite is the opposite of the answer. Every test therefore
//! takes `METER` for its whole body, so the counters mean what they say however
//! the suite is run.
//!
//! The metadata figure is the one worth reading carefully. It is counted at the
//! `StorageBackend` boundary, so it is what a *caller* asks for, not what a
//! backend does about it. The native backend answers most of them from a
//! `RefCell` cache and the wasm backend crosses a bridge to a JS worker for
//! every one of them, caching nothing. The count is the same either way, which
//! is what makes the wasm cost measurable without a browser.

#![cfg(feature = "bench-counters")]

use dynoxide::Database;
use dynoxide::bench_counters::{Counts, reset, snapshot};
use std::sync::{Mutex, MutexGuard};

const TABLE: &str = "counts_tbl";

/// A table with two indexes, which is the shape that shows repeated index work.
fn two_index_table() -> Database {
    let db = Database::memory().unwrap();
    db.create_table(
        serde_json::from_value(serde_json::json!({
            "TableName": TABLE,
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"}
            ],
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
                {"AttributeName": "gsiPk", "AttributeType": "S"},
                {"AttributeName": "lsiSk", "AttributeType": "S"}
            ],
            "BillingMode": "PAY_PER_REQUEST",
            "GlobalSecondaryIndexes": [{
                "IndexName": "gsi-inc",
                "KeySchema": [{"AttributeName": "gsiPk", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "INCLUDE", "NonKeyAttributes": ["proj"]}
            }],
            "LocalSecondaryIndexes": [{
                "IndexName": "lsi-all",
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "lsiSk", "KeyType": "RANGE"}
                ],
                "Projection": {"ProjectionType": "ALL"}
            }]
        }))
        .unwrap(),
    )
    .unwrap();
    db
}

fn put(db: &Database, sk: &str, mode: Option<&str>) {
    let mut req = serde_json::json!({
        "TableName": TABLE,
        "Item": {
            "pk": {"S": "p"}, "sk": {"S": sk},
            "gsiPk": {"S": "g"}, "lsiSk": {"S": "l"},
            "proj": {"S": "v"}, "other": {"S": "w"}
        }
    });
    if let Some(mode) = mode {
        req["ReturnConsumedCapacity"] = serde_json::json!(mode);
    }
    db.put_item(serde_json::from_value(req).unwrap()).unwrap();
}

static METER: Mutex<()> = Mutex::new(());

/// Claim the counters for one test, for its whole body.
///
/// It has to cover the setup too, not just the measurements. Building a fixture
/// table reads metadata, so a test still in `two_index_table` lands its reads in
/// whatever another test is measuring at that moment. Guarding only `measure`
/// leaves that hole open, and it is the hole that made an insert read as 8
/// entries built rather than 2.
fn meter() -> MutexGuard<'static, ()> {
    // A panicking test would otherwise poison the lock and turn every later test
    // into a second, unrelated failure that buries the real one.
    METER.lock().unwrap_or_else(|e| e.into_inner())
}

/// Count what one operation costs, with the setup already done. Call `meter()`
/// at the top of the test first.
fn measure(f: impl FnOnce()) -> Counts {
    reset();
    f();
    snapshot()
}

// --- index entries rebuilt on a write -------------------------------------

#[test]
fn report_index_entry_builds_on_an_overwrite() {
    let _meter = meter();
    let db = two_index_table();
    put(&db, "s1", None); // seed, so the write measured below has an old image

    let without = measure(|| put(&db, "s1", None));
    let with_indexes = measure(|| put(&db, "s1", Some("INDEXES")));

    println!(
        "overwrite, two indexes, capacity not asked for : {} index entries built",
        without.index_entries_built
    );
    println!(
        "overwrite, two indexes, INDEXES requested      : {} index entries built",
        with_indexes.index_entries_built
    );

    // The floor is one projected entry per index: the fan-out has to build the
    // row it stores. An overwrite with no capacity asked for now sits on that
    // floor, so it costs an insert's projections rather than an insert's plus a
    // pass over the old image that only sizing wanted.
    assert_eq!(
        without.index_entries_built, 2,
        "an overwrite reporting no capacity should build one entry per index"
    );

    // Sizing an index needs the old image projected as well, and that is the
    // whole of the difference. Asking for capacity is what pays for it.
    assert_eq!(
        with_indexes.index_entries_built, 4,
        "sizing two indexes needs the old image projected for each"
    );
}

#[test]
fn report_index_entry_builds_on_an_insert() {
    let _meter = meter();
    let db = two_index_table();
    let counts = measure(|| put(&db, "fresh", None));
    println!(
        "insert, two indexes, capacity not asked for    : {} index entries built",
        counts.index_entries_built
    );

    // One entry per index and nothing more. An insert has no old image, so
    // there is nothing for sizing to project even when capacity is asked for,
    // which is what makes this the floor the overwrite test measures against.
    assert_eq!(
        counts.index_entries_built, 2,
        "an insert should build one entry per index"
    );
}

// --- metadata resolved per statement in a batch ---------------------------

fn batch(db: &Database, count: usize) {
    let statements: Vec<serde_json::Value> = (0..count)
        .map(|i| {
            serde_json::json!({
                "Statement": format!(
                    "INSERT INTO \"{TABLE}\" VALUE {{'pk':'p','sk':'b{i}','gsiPk':'g','lsiSk':'l'}}"
                )
            })
        })
        .collect();
    db.batch_execute_statement(
        serde_json::from_value(serde_json::json!({"Statements": statements})).unwrap(),
    )
    .unwrap();
}

#[test]
fn report_metadata_reads_across_a_batch() {
    let _meter = meter();
    let db = two_index_table();

    let one = measure(|| batch(&db, 1));
    let twenty_five = measure(|| batch(&db, 25));

    println!(
        "batch of 1  : {} metadata reads, {} key schema parses",
        one.metadata_reads, one.key_schema_parses
    );
    println!(
        "batch of 25 : {} metadata reads, {} key schema parses",
        twenty_five.metadata_reads, twenty_five.key_schema_parses
    );
    println!(
        "per statement: {:.1} metadata reads, {:.1} key schema parses",
        twenty_five.metadata_reads as f64 / 25.0,
        twenty_five.key_schema_parses as f64 / 25.0
    );

    // One table, so one resolution does for the whole batch. The figure not
    // moving with the statement count is the whole point: metadata and key
    // schema are per-table facts, and only the item key is per statement.
    assert_eq!(
        one.metadata_reads, 1,
        "a single-statement batch resolves its table once"
    );
    assert_eq!(
        twenty_five.metadata_reads, 1,
        "25 statements against one table still resolve it once"
    );
    assert_eq!(
        twenty_five.key_schema_parses, 1,
        "the parsed key schema is kept alongside the metadata"
    );
}

#[test]
fn report_metadata_reads_for_a_single_statement() {
    let _meter = meter();
    let db = two_index_table();
    let counts = measure(|| {
        db.execute_statement(
            serde_json::from_value(serde_json::json!({
                "Statement": format!(
                    "INSERT INTO \"{TABLE}\" VALUE {{'pk':'p','sk':'one','gsiPk':'g','lsiSk':'l'}}"
                )
            }))
            .unwrap(),
        )
        .unwrap();
    });
    println!(
        "single ExecuteStatement insert: {} metadata reads, {} key schema parses",
        counts.metadata_reads, counts.key_schema_parses
    );

    // A single statement has no preparation pass to share a resolution with, so
    // the executor resolves the table itself. Once is the floor, and pinning it
    // here is what would catch a caller that starts resolving ahead of it again.
    assert_eq!(
        counts.metadata_reads, 1,
        "one statement should read its table's metadata once"
    );
    assert_eq!(counts.key_schema_parses, 1, "and parse its key schema once");
}
