//! End-to-end coverage that exercises the [`StorageBackend`] trait through
//! the native [`Storage`] impl.
//!
//! Most of dynoxide's tests reach `Storage` via the sync `Database` wrapper.
//! These tests bypass that wrapper to call trait methods directly with `await`,
//! confirming the native impl's futures are actually drivable and that the
//! trait surface produces the same observable state as the sync surface.
//!
//! Tests use `#[tokio::test(flavor = "current_thread")]` because `Storage`
//! holds a `RefCell` and is `!Sync`; a multi-thread runtime would refuse the
//! futures unless `Storage` were Send and Sync.
//!
//! Inherent methods on `Storage` shadow the trait methods of the same name, so
//! these tests use fully-qualified `<Storage as StorageBackend>::method(...)`
//! syntax to call through the trait surface explicitly.
//!
//! [`StorageBackend`]: dynoxide::storage_backend::StorageBackend
//! [`Storage`]: dynoxide::storage::Storage

use dynoxide::storage::{CreateTableMetadata, QueryParams, Storage};
use dynoxide::storage_backend::{BaseItemRow, GsiItemRow, StorageBackend};

fn make_metadata<'a>(table_name: &'a str, key_schema: &'a str) -> CreateTableMetadata<'a> {
    CreateTableMetadata {
        table_name,
        key_schema,
        attribute_definitions: r#"[{"AttributeName":"pk","AttributeType":"S"}]"#,
        gsi_definitions: None,
        lsi_definitions: None,
        provisioned_throughput: None,
        created_at: 0,
        sse_specification: None,
        table_class: None,
        deletion_protection_enabled: false,
        billing_mode: None,
        on_demand_throughput: None,
    }
}

async fn seed_table(storage: &Storage, name: &str) {
    let key_schema = r#"[{"AttributeName":"pk","KeyType":"HASH"}]"#;
    <Storage as StorageBackend>::insert_table_metadata(storage, &make_metadata(name, key_schema))
        .await
        .expect("insert_table_metadata via trait");
    <Storage as StorageBackend>::create_data_table(storage, name)
        .await
        .expect("create_data_table via trait");
}

#[tokio::test(flavor = "current_thread")]
async fn put_and_get_round_trip_via_trait() {
    let storage = Storage::memory().expect("Storage::memory");
    seed_table(&storage, "users").await;

    let item = r#"{"pk":{"S":"alice"},"name":{"S":"Alice"}}"#;
    let prior =
        <Storage as StorageBackend>::put_item(&storage, "users", "alice", "", item, item.len())
            .await
            .expect("put_item via trait");
    assert!(prior.is_none(), "first put should not return a prior item");

    let fetched = <Storage as StorageBackend>::get_item(&storage, "users", "alice", "")
        .await
        .expect("get_item via trait");
    assert_eq!(fetched.as_deref(), Some(item));
}

#[tokio::test(flavor = "current_thread")]
async fn delete_via_trait_returns_old_value() {
    let storage = Storage::memory().expect("Storage::memory");
    seed_table(&storage, "users").await;

    let item = r#"{"pk":{"S":"bob"}}"#;
    <Storage as StorageBackend>::put_item(&storage, "users", "bob", "", item, item.len())
        .await
        .unwrap();

    let removed = <Storage as StorageBackend>::delete_item(&storage, "users", "bob", "")
        .await
        .expect("delete_item via trait");
    assert_eq!(removed.as_deref(), Some(item));

    let after = <Storage as StorageBackend>::get_item(&storage, "users", "bob", "")
        .await
        .unwrap();
    assert!(after.is_none(), "item should be gone after delete");
}

#[tokio::test(flavor = "current_thread")]
async fn transaction_roundtrip_via_trait_commits() {
    let storage = Storage::memory().expect("Storage::memory");
    seed_table(&storage, "users").await;

    <Storage as StorageBackend>::begin_transaction(&storage)
        .await
        .expect("begin_transaction");
    let item = r#"{"pk":{"S":"carol"}}"#;
    <Storage as StorageBackend>::put_item(&storage, "users", "carol", "", item, item.len())
        .await
        .unwrap();
    <Storage as StorageBackend>::commit(&storage)
        .await
        .expect("commit");

    let got = <Storage as StorageBackend>::get_item(&storage, "users", "carol", "")
        .await
        .unwrap();
    assert_eq!(got.as_deref(), Some(item));
}

#[tokio::test(flavor = "current_thread")]
async fn transaction_rollback_via_trait_discards_writes() {
    let storage = Storage::memory().expect("Storage::memory");
    seed_table(&storage, "users").await;

    <Storage as StorageBackend>::begin_transaction(&storage)
        .await
        .expect("begin_transaction");
    let item = r#"{"pk":{"S":"dave"}}"#;
    <Storage as StorageBackend>::put_item(&storage, "users", "dave", "", item, item.len())
        .await
        .unwrap();
    <Storage as StorageBackend>::rollback(&storage)
        .await
        .expect("rollback");

    let got = <Storage as StorageBackend>::get_item(&storage, "users", "dave", "")
        .await
        .unwrap();
    assert!(
        got.is_none(),
        "rolled-back put should leave no item visible"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn metadata_roundtrip_via_trait() {
    let storage = Storage::memory().expect("Storage::memory");
    let key_schema = r#"[{"AttributeName":"pk","KeyType":"HASH"}]"#;
    <Storage as StorageBackend>::insert_table_metadata(
        &storage,
        &make_metadata("orders", key_schema),
    )
    .await
    .unwrap();

    assert!(
        <Storage as StorageBackend>::table_exists(&storage, "orders")
            .await
            .unwrap(),
        "table_exists via trait should reflect metadata insert"
    );

    let names = <Storage as StorageBackend>::list_table_names(&storage)
        .await
        .unwrap();
    assert!(names.iter().any(|n| n == "orders"));

    let meta = <Storage as StorageBackend>::get_table_metadata(&storage, "orders")
        .await
        .unwrap()
        .expect("metadata should exist after insert");
    assert_eq!(meta.table_name, "orders");
}

#[tokio::test(flavor = "current_thread")]
async fn count_items_matches_inserted_count_via_trait() {
    let storage = Storage::memory().expect("Storage::memory");
    seed_table(&storage, "events").await;

    for i in 0..5 {
        let pk = format!("evt-{i}");
        let item = format!(r#"{{"pk":{{"S":"{pk}"}}}}"#);
        <Storage as StorageBackend>::put_item(&storage, "events", &pk, "", &item, item.len())
            .await
            .unwrap();
    }

    let count = <Storage as StorageBackend>::count_items(&storage, "events")
        .await
        .unwrap();
    assert_eq!(count, 5);
}

#[tokio::test(flavor = "current_thread")]
async fn put_base_items_bulk_insert_via_trait() {
    let storage = Storage::memory().expect("Storage::memory");
    seed_table(&storage, "bulk").await;

    let rows: Vec<BaseItemRow> = (0..3)
        .map(|i| {
            let pk = format!("k{i}");
            let item_json = format!(r#"{{"pk":{{"S":"{pk}"}}}}"#);
            BaseItemRow {
                pk,
                sk: String::new(),
                item_size: item_json.len(),
                item_json,
                // First row carries a cache timestamp; the rest leave it NULL.
                cached_at: if i == 0 { Some(123.0) } else { None },
                hash_prefix: String::new(),
            }
        })
        .collect();

    <Storage as StorageBackend>::put_base_items(&storage, "bulk", &rows)
        .await
        .expect("put_base_items via trait");

    // Every row is retrievable.
    for i in 0..3 {
        let got = <Storage as StorageBackend>::get_item(&storage, "bulk", &format!("k{i}"), "")
            .await
            .unwrap();
        assert_eq!(
            got.as_deref(),
            Some(format!(r#"{{"pk":{{"S":"k{i}"}}}}"#).as_str())
        );
    }

    // cached_at is written verbatim: only the row that set it shows up in the
    // LRU view (which excludes NULL cached_at).
    let lru = <Storage as StorageBackend>::get_lru_items(&storage, "bulk", 10)
        .await
        .unwrap();
    assert_eq!(lru.len(), 1, "only the row with a cached_at should appear");
    assert_eq!(lru[0].0, "k0");
}

#[tokio::test(flavor = "current_thread")]
async fn insert_gsi_items_bulk_insert_via_trait() {
    let storage = Storage::memory().expect("Storage::memory");
    seed_table(&storage, "orders").await;
    <Storage as StorageBackend>::create_gsi_table(&storage, "orders", "by-status")
        .await
        .expect("create_gsi_table via trait");

    let rows = vec![
        GsiItemRow {
            gsi_pk: "shipped".to_string(),
            gsi_sk: "o1".to_string(),
            table_pk: "o1".to_string(),
            table_sk: String::new(),
            item_json: r#"{"pk":{"S":"o1"},"status":{"S":"shipped"}}"#.to_string(),
        },
        GsiItemRow {
            gsi_pk: "shipped".to_string(),
            gsi_sk: "o2".to_string(),
            table_pk: "o2".to_string(),
            table_sk: String::new(),
            item_json: r#"{"pk":{"S":"o2"},"status":{"S":"shipped"}}"#.to_string(),
        },
    ];

    <Storage as StorageBackend>::insert_gsi_items(&storage, "orders", "by-status", &rows)
        .await
        .expect("insert_gsi_items via trait");

    let found = <Storage as StorageBackend>::query_gsi_items(
        &storage,
        "orders",
        "by-status",
        "shipped",
        &QueryParams::default(),
    )
    .await
    .expect("query_gsi_items via trait");
    assert_eq!(
        found.len(),
        2,
        "both bulk-inserted GSI rows should be queryable"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn put_base_items_to_missing_table_is_backend_error() {
    let storage = Storage::memory().expect("Storage::memory");
    let rows = vec![BaseItemRow {
        pk: "x".to_string(),
        sk: String::new(),
        item_json: r#"{"pk":{"S":"x"}}"#.to_string(),
        item_size: 1,
        cached_at: None,
        hash_prefix: String::new(),
    }];

    let err = <Storage as StorageBackend>::put_base_items(&storage, "does-not-exist", &rows)
        .await
        .expect_err("bulk insert into a missing table must fail");

    // From<BackendError> for DynoxideError keeps storage faults as 500s.
    let dyno: dynoxide::DynoxideError = err.into();
    assert_eq!(dyno.status_code(), 500);
}

#[tokio::test(flavor = "current_thread")]
async fn clock_via_trait_returns_wall_clock() {
    let storage = Storage::memory().expect("Storage::memory");
    let now = <Storage as StorageBackend>::clock(&storage).now_unix_secs();
    assert!(now > 0, "system clock should report a positive epoch");
}
