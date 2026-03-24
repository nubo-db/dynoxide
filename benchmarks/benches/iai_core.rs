// Iai-Callgrind instruction-count benchmarks for deterministic regression detection.
//
// These measure CPU instruction counts (not wall-clock time), giving <1% variance
// regardless of CI runner load. They require Valgrind (Linux only).
//
// Run with: cargo bench --bench iai_core --features iai-callgrind
// Requires: cargo install iai-callgrind-runner --version 0.14.2
//           apt-get install valgrind (Linux only)

#[cfg(feature = "iai-callgrind")]
use dynoxide::Database;
#[cfg(feature = "iai-callgrind")]
use dynoxide::actions::delete_item::DeleteItemRequest;
#[cfg(feature = "iai-callgrind")]
use dynoxide::actions::get_item::GetItemRequest;
#[cfg(feature = "iai-callgrind")]
use dynoxide::actions::put_item::PutItemRequest;
#[cfg(feature = "iai-callgrind")]
use dynoxide::actions::query::QueryRequest;
#[cfg(feature = "iai-callgrind")]
use dynoxide::actions::scan::ScanRequest;
#[cfg(feature = "iai-callgrind")]
use dynoxide::actions::update_item::UpdateItemRequest;
#[cfg(feature = "iai-callgrind")]
use dynoxide::types::*;
#[cfg(feature = "iai-callgrind")]
use dynoxide_benchmarks::{
    BENCH_TABLE, ItemSize, create_table_request, generate_item, make_key, setup_database,
};
#[cfg(feature = "iai-callgrind")]
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
#[cfg(feature = "iai-callgrind")]
use std::collections::HashMap;
#[cfg(feature = "iai-callgrind")]
use std::hint::black_box;

// ---- Setup functions (excluded from measurement) ----

#[cfg(feature = "iai-callgrind")]
fn setup_put_item() -> (Database, PutItemRequest) {
    let db = Database::memory().unwrap();
    db.create_table(create_table_request(BENCH_TABLE)).unwrap();
    let item = generate_item(0, ItemSize::Medium);
    let request = PutItemRequest {
        table_name: BENCH_TABLE.to_string(),
        item,
        ..Default::default()
    };
    (db, request)
}

#[cfg(feature = "iai-callgrind")]
fn setup_get_item() -> (Database, GetItemRequest) {
    let db = setup_database(1000, ItemSize::Medium);
    let item = generate_item(500, ItemSize::Medium);
    let key = make_key(&item);
    let request = GetItemRequest {
        table_name: BENCH_TABLE.to_string(),
        key,
        ..Default::default()
    };
    (db, request)
}

#[cfg(feature = "iai-callgrind")]
fn setup_query() -> (Database, QueryRequest) {
    let db = setup_database(1000, ItemSize::Medium);
    let mut eav = HashMap::new();
    eav.insert(
        ":pk".to_string(),
        AttributeValue::S("user#000005".to_string()),
    );
    eav.insert(":lo".to_string(), AttributeValue::N("500".to_string()));
    eav.insert(":hi".to_string(), AttributeValue::N("600".to_string()));
    eav.insert(":age".to_string(), AttributeValue::N("25".to_string()));
    let request = QueryRequest {
        table_name: BENCH_TABLE.to_string(),
        key_condition_expression: Some("pk = :pk AND sk BETWEEN :lo AND :hi".to_string()),
        filter_expression: Some("age > :age".to_string()),
        expression_attribute_values: Some(eav),
        scan_index_forward: true,
        ..Default::default()
    };
    (db, request)
}

#[cfg(feature = "iai-callgrind")]
fn setup_scan() -> (Database, ScanRequest) {
    let db = setup_database(1000, ItemSize::Medium);
    let mut eav = HashMap::new();
    eav.insert(":age".to_string(), AttributeValue::N("50".to_string()));
    let request = ScanRequest {
        table_name: BENCH_TABLE.to_string(),
        filter_expression: Some("age > :age".to_string()),
        expression_attribute_values: Some(eav),
        ..Default::default()
    };
    (db, request)
}

#[cfg(feature = "iai-callgrind")]
fn setup_update_item() -> (Database, UpdateItemRequest) {
    let db = setup_database(1000, ItemSize::Medium);
    let item = generate_item(500, ItemSize::Medium);
    let key = make_key(&item);
    let mut eav = HashMap::new();
    eav.insert(
        ":val".to_string(),
        AttributeValue::L(vec![AttributeValue::N("99".to_string())]),
    );
    let request = UpdateItemRequest {
        table_name: BENCH_TABLE.to_string(),
        key,
        update_expression: Some("SET scores = list_append(scores, :val)".to_string()),
        condition_expression: Some("attribute_exists(pk)".to_string()),
        expression_attribute_values: Some(eav),
        ..Default::default()
    };
    (db, request)
}

#[cfg(feature = "iai-callgrind")]
fn setup_delete_item() -> (Database, DeleteItemRequest) {
    let db = setup_database(1000, ItemSize::Medium);
    let item = generate_item(500, ItemSize::Medium);
    let key = make_key(&item);
    let request = DeleteItemRequest {
        table_name: BENCH_TABLE.to_string(),
        key,
        ..Default::default()
    };
    (db, request)
}

// ---- Benchmarks (only the function body is measured) ----

#[cfg(feature = "iai-callgrind")]
#[library_benchmark]
#[bench::medium(setup_put_item())]
fn bench_put_item((db, request): (Database, PutItemRequest)) {
    black_box(db.put_item(request).unwrap());
}

#[cfg(feature = "iai-callgrind")]
#[library_benchmark]
#[bench::by_key(setup_get_item())]
fn bench_get_item((db, request): (Database, GetItemRequest)) {
    black_box(db.get_item(request).unwrap());
}

#[cfg(feature = "iai-callgrind")]
#[library_benchmark]
#[bench::with_filter(setup_query())]
fn bench_query((db, request): (Database, QueryRequest)) {
    black_box(db.query(request).unwrap());
}

#[cfg(feature = "iai-callgrind")]
#[library_benchmark]
#[bench::with_filter(setup_scan())]
fn bench_scan((db, request): (Database, ScanRequest)) {
    black_box(db.scan(request).unwrap());
}

#[cfg(feature = "iai-callgrind")]
#[library_benchmark]
#[bench::list_append(setup_update_item())]
fn bench_update_item((db, request): (Database, UpdateItemRequest)) {
    black_box(db.update_item(request).unwrap());
}

#[cfg(feature = "iai-callgrind")]
#[library_benchmark]
#[bench::single(setup_delete_item())]
fn bench_delete_item((db, request): (Database, DeleteItemRequest)) {
    black_box(db.delete_item(request).unwrap());
}

// ---- Group and harness registration ----

#[cfg(feature = "iai-callgrind")]
library_benchmark_group!(
    name = core_operations;
    benchmarks =
        bench_put_item,
        bench_get_item,
        bench_query,
        bench_scan,
        bench_update_item,
        bench_delete_item
);

#[cfg(feature = "iai-callgrind")]
main!(library_benchmark_groups = core_operations);

#[cfg(not(feature = "iai-callgrind"))]
fn main() {
    eprintln!(
        "iai-callgrind benchmarks require the iai-callgrind feature and Valgrind (Linux only)."
    );
    eprintln!("Run with: cargo bench --bench iai_core --features iai-callgrind");
    eprintln!("Install: cargo install iai-callgrind-runner --version 0.14.2");
    eprintln!("         apt-get install valgrind");
}
