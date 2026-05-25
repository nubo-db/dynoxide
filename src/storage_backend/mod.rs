//! Storage backend abstraction.
//!
//! Defines the [`StorageBackend`] trait that decouples Dynoxide's data layer
//! from a specific SQLite binding. The native [`rusqlite`]-backed
//! [`Storage`](crate::storage::Storage) implements the trait. A compile-only
//! wa-sqlite stub gated behind the `wasm-stub` feature also implements it,
//! so trait-shape drift surfaces at type-check time rather than once a real
//! wa-sqlite backend is wired up. Building dynoxide for a non-native target
//! (e.g., `wasm32-unknown-unknown` itself) is not yet supported; the stub
//! validates the trait surface, not the rest of the codebase.
//!
//! Today the trait is consumed monomorphically. Nothing constructs
//! `dyn StorageBackend`, nothing awaits it at runtime in production code.
//! `Database`, action handlers, and `DynoxideError` continue to operate
//! against the native `Storage` type directly. The escape hatches
//! `Storage::conn()` and `Storage::conn_mut()` are not exposed by the trait;
//! folding them in (or migrating their callers off them) is a follow-up to
//! the wa-sqlite work.
//!
//! # No `Send + Sync` super-trait
//!
//! [`Storage`](crate::storage::Storage) carries a `RefCell<HashMap<...>>` for
//! its metadata cache, so `Storage: !Sync`. A `Send + Sync` super-trait would
//! refuse the impl on `Storage`. With no dynamic dispatch site in scope,
//! auto-trait propagation across `.await` is decided per-callsite anyway, so
//! adding `Send` to the super-trait would not earn any compile-time
//! guarantee on the futures returned by trait methods.

pub mod clock;
pub mod error;
#[cfg(feature = "native-sqlite")]
pub mod rusqlite_impl;
#[cfg(feature = "wasm-stub")]
pub mod wasm_stub;

use crate::storage::{
    CreateTableMetadata, DatabaseInfo, QueryParams, ScanParams, StreamRecord, TableMetadata,
    TableStats,
};
use crate::types::Tag;

pub use clock::{Clock, ManualClock, SystemClock};
pub use error::{BackendError, from_rusqlite};

/// One base-table row for a bulk insert via [`StorageBackend::put_base_items`].
///
/// Unlike [`StorageBackend::put_item_with_hash`], which preserves any existing
/// `cached_at` value, the bulk path writes `cached_at` verbatim: this mirrors
/// the import flow, which sets the timestamp explicitly (or clears it) for
/// every row it loads.
#[derive(Debug, Clone)]
pub struct BaseItemRow {
    /// Partition key string.
    pub pk: String,
    /// Sort key string (empty for tables without a sort key).
    pub sk: String,
    /// Serialised item JSON.
    pub item_json: String,
    /// Item size in bytes.
    pub item_size: usize,
    /// Cache timestamp written verbatim; `None` clears the column.
    pub cached_at: Option<f64>,
    /// Hash prefix used for parallel-scan ordering.
    pub hash_prefix: String,
}

/// One GSI-table row for a bulk insert via [`StorageBackend::insert_gsi_items`].
///
/// The fields mirror the argument order of the single-row
/// [`StorageBackend::insert_gsi_item`].
#[derive(Debug, Clone)]
pub struct GsiItemRow {
    /// GSI partition key string.
    pub gsi_pk: String,
    /// GSI sort key string (empty when the index has no sort key).
    pub gsi_sk: String,
    /// Base-table partition key string.
    pub table_pk: String,
    /// Base-table sort key string.
    pub table_sk: String,
    /// Projected item JSON.
    pub item_json: String,
}

/// Backend-neutral storage interface.
///
/// Method signatures mirror [`Storage`](crate::storage::Storage)'s public
/// surface 1:1, with three mechanical transformations:
///
/// 1. `Result<T, DynoxideError>` becomes `Result<T, BackendError>`.
/// 2. `fn` becomes `async fn`.
/// 3. Filesystem-typed and rusqlite-typed methods are excluded; they remain
///    on the native [`Storage`](crate::storage::Storage) only.
///
/// The trait is not consumed dynamically today. Its job is to lock the shape
/// a future wa-sqlite backend will satisfy; type-level fit is validated by
/// the compile-only stub in
/// [`wasm_stub`](crate::storage_backend::wasm_stub).
///
/// The `#[allow(async_fn_in_trait)]` reflects the monomorphic-only consumption
/// model. The lint can be revisited if and when `dyn StorageBackend` becomes
/// a real callsite.
#[allow(async_fn_in_trait)]
pub trait StorageBackend {
    // -----------------------------------------------------------------------
    // Capabilities
    // -----------------------------------------------------------------------

    /// Wall-clock access for the stream and TTL paths.
    ///
    /// Sync because reading the clock is not I/O. The native backend returns
    /// its injected [`Clock`]; a real wa-sqlite backend supplies its own.
    fn clock(&self) -> &dyn Clock;

    // -----------------------------------------------------------------------
    // Table metadata
    // -----------------------------------------------------------------------

    async fn insert_table_metadata(&self, m: &CreateTableMetadata<'_>) -> Result<(), BackendError>;

    async fn get_table_metadata(
        &self,
        table_name: &str,
    ) -> Result<Option<TableMetadata>, BackendError>;

    async fn delete_table_metadata(&self, table_name: &str) -> Result<bool, BackendError>;

    async fn update_table_metadata(
        &self,
        table_name: &str,
        attribute_definitions: &str,
        gsi_definitions: Option<&str>,
    ) -> Result<(), BackendError>;

    async fn update_provisioned_throughput(
        &self,
        table_name: &str,
        provisioned_throughput: &str,
    ) -> Result<(), BackendError>;

    async fn clear_provisioned_throughput(&self, table_name: &str) -> Result<(), BackendError>;

    async fn update_billing_mode(
        &self,
        table_name: &str,
        billing_mode: &str,
    ) -> Result<(), BackendError>;

    async fn get_tags(&self, table_name: &str) -> Result<Vec<Tag>, BackendError>;

    async fn set_tags(&self, table_name: &str, new_tags: &[Tag]) -> Result<(), BackendError>;

    async fn update_deletion_protection(
        &self,
        table_name: &str,
        enabled: bool,
    ) -> Result<(), BackendError>;

    async fn remove_tags(&self, table_name: &str, keys: &[String]) -> Result<(), BackendError>;

    async fn list_table_names(&self) -> Result<Vec<String>, BackendError>;

    async fn table_exists(&self, table_name: &str) -> Result<bool, BackendError>;

    // -----------------------------------------------------------------------
    // Dynamic data tables (DDL)
    // -----------------------------------------------------------------------

    async fn create_data_table(&self, table_name: &str) -> Result<(), BackendError>;

    async fn drop_data_table(&self, table_name: &str) -> Result<(), BackendError>;

    async fn create_gsi_table(
        &self,
        table_name: &str,
        index_name: &str,
    ) -> Result<(), BackendError>;

    async fn drop_gsi_table(&self, table_name: &str, index_name: &str) -> Result<(), BackendError>;

    async fn create_lsi_table(
        &self,
        table_name: &str,
        index_name: &str,
    ) -> Result<(), BackendError>;

    async fn drop_lsi_table(&self, table_name: &str, index_name: &str) -> Result<(), BackendError>;

    // -----------------------------------------------------------------------
    // GSI item operations
    // -----------------------------------------------------------------------

    #[allow(clippy::too_many_arguments)]
    async fn insert_gsi_item(
        &self,
        table_name: &str,
        index_name: &str,
        gsi_pk: &str,
        gsi_sk: &str,
        table_pk: &str,
        table_sk: &str,
        item_json: &str,
    ) -> Result<(), BackendError>;

    /// Bulk-insert many rows into one GSI table.
    ///
    /// Batch-shaped so a backend can amortise per-row round-trips (the native
    /// backend reuses a single cached prepared statement). Used by the GSI
    /// backfill path; the per-row [`insert_gsi_item`](Self::insert_gsi_item)
    /// covers single writes during normal fan-out.
    async fn insert_gsi_items(
        &self,
        table_name: &str,
        index_name: &str,
        rows: &[GsiItemRow],
    ) -> Result<(), BackendError>;

    async fn delete_gsi_item(
        &self,
        table_name: &str,
        index_name: &str,
        table_pk: &str,
        table_sk: &str,
    ) -> Result<(), BackendError>;

    async fn query_gsi_items(
        &self,
        table_name: &str,
        index_name: &str,
        gsi_pk: &str,
        params: &QueryParams<'_>,
    ) -> Result<Vec<(String, String, String)>, BackendError>;

    async fn scan_gsi_items(
        &self,
        table_name: &str,
        index_name: &str,
        params: &ScanParams<'_>,
    ) -> Result<Vec<(String, String, String)>, BackendError>;

    // -----------------------------------------------------------------------
    // LSI item operations
    // -----------------------------------------------------------------------

    #[allow(clippy::too_many_arguments)]
    async fn insert_lsi_item(
        &self,
        table_name: &str,
        index_name: &str,
        pk: &str,
        sk: &str,
        base_pk: &str,
        base_sk: &str,
        item_json: &str,
    ) -> Result<(), BackendError>;

    async fn delete_lsi_item(
        &self,
        table_name: &str,
        index_name: &str,
        base_pk: &str,
        base_sk: &str,
    ) -> Result<(), BackendError>;

    async fn query_lsi_items(
        &self,
        table_name: &str,
        index_name: &str,
        pk: &str,
        params: &QueryParams<'_>,
    ) -> Result<Vec<(String, String, String)>, BackendError>;

    async fn scan_lsi_items(
        &self,
        table_name: &str,
        index_name: &str,
        params: &ScanParams<'_>,
    ) -> Result<Vec<(String, String, String)>, BackendError>;

    // -----------------------------------------------------------------------
    // Transactions
    // -----------------------------------------------------------------------

    async fn begin_transaction(&self) -> Result<(), BackendError>;
    async fn commit(&self) -> Result<(), BackendError>;
    async fn rollback(&self) -> Result<(), BackendError>;

    // -----------------------------------------------------------------------
    // Bulk-loading PRAGMAs
    // -----------------------------------------------------------------------

    async fn enable_bulk_loading(&self) -> Result<(), BackendError>;
    async fn disable_bulk_loading(&self) -> Result<(), BackendError>;

    // -----------------------------------------------------------------------
    // Item CRUD
    // -----------------------------------------------------------------------

    async fn put_item(
        &self,
        table_name: &str,
        pk: &str,
        sk: &str,
        item_json: &str,
        item_size: usize,
    ) -> Result<Option<String>, BackendError>;

    #[allow(clippy::too_many_arguments)]
    async fn put_item_with_hash(
        &self,
        table_name: &str,
        pk: &str,
        sk: &str,
        item_json: &str,
        item_size: usize,
        hash_prefix: &str,
    ) -> Result<Option<String>, BackendError>;

    /// Bulk-insert many base-table rows in one call (`INSERT OR REPLACE`).
    ///
    /// Batch-shaped so a backend can amortise per-row round-trips (the native
    /// backend reuses a single cached prepared statement). Used by the import
    /// path. Writes `cached_at` verbatim from each [`BaseItemRow`]; see the
    /// note there for how this differs from
    /// [`put_item_with_hash`](Self::put_item_with_hash).
    async fn put_base_items(
        &self,
        table_name: &str,
        rows: &[BaseItemRow],
    ) -> Result<(), BackendError>;

    async fn get_item(
        &self,
        table_name: &str,
        pk: &str,
        sk: &str,
    ) -> Result<Option<String>, BackendError>;

    async fn get_partition_size(&self, table_name: &str, pk: &str) -> Result<i64, BackendError>;

    async fn get_lsi_partition_size(
        &self,
        table_name: &str,
        index_name: &str,
        pk: &str,
    ) -> Result<i64, BackendError>;

    async fn delete_item(
        &self,
        table_name: &str,
        pk: &str,
        sk: &str,
    ) -> Result<Option<String>, BackendError>;

    async fn query_items(
        &self,
        table_name: &str,
        pk: &str,
        params: &QueryParams<'_>,
    ) -> Result<Vec<(String, String, String)>, BackendError>;

    async fn scan_items(
        &self,
        table_name: &str,
        params: &ScanParams<'_>,
    ) -> Result<Vec<(String, String, String)>, BackendError>;

    async fn count_items(&self, table_name: &str) -> Result<i64, BackendError>;

    // -----------------------------------------------------------------------
    // Introspection
    // -----------------------------------------------------------------------

    async fn db_size_bytes(&self) -> Result<u64, BackendError>;
    async fn table_count(&self) -> Result<usize, BackendError>;
    async fn table_stats(&self) -> Result<Vec<TableStats>, BackendError>;
    async fn database_info(&self) -> Result<DatabaseInfo, BackendError>;
    async fn vacuum(&self) -> Result<(), BackendError>;

    // -----------------------------------------------------------------------
    // Streams
    // -----------------------------------------------------------------------

    async fn enable_stream(
        &self,
        table_name: &str,
        view_type: &str,
        label: &str,
    ) -> Result<(), BackendError>;

    async fn disable_stream(&self, table_name: &str) -> Result<(), BackendError>;

    #[allow(clippy::too_many_arguments)]
    async fn insert_stream_record(
        &self,
        table_name: &str,
        event_name: &str,
        keys_json: &str,
        new_image: Option<&str>,
        old_image: Option<&str>,
        sequence_number: &str,
        shard_id: &str,
        created_at: i64,
    ) -> Result<(), BackendError>;

    #[allow(clippy::too_many_arguments)]
    async fn insert_stream_record_with_identity(
        &self,
        table_name: &str,
        event_name: &str,
        keys_json: &str,
        new_image: Option<&str>,
        old_image: Option<&str>,
        sequence_number: &str,
        shard_id: &str,
        created_at: i64,
        user_identity: Option<&str>,
    ) -> Result<(), BackendError>;

    async fn next_stream_sequence_number(&self, table_name: &str) -> Result<i64, BackendError>;

    async fn get_stream_records(
        &self,
        table_name: &str,
        shard_id: &str,
        after_sequence: i64,
        limit: usize,
    ) -> Result<Vec<StreamRecord>, BackendError>;

    async fn list_stream_enabled_tables(&self) -> Result<Vec<TableMetadata>, BackendError>;

    // -----------------------------------------------------------------------
    // TTL operations
    // -----------------------------------------------------------------------

    async fn update_ttl_config(
        &self,
        table_name: &str,
        attribute_name: Option<&str>,
        enabled: bool,
    ) -> Result<(), BackendError>;

    async fn list_ttl_enabled_tables(&self) -> Result<Vec<TableMetadata>, BackendError>;

    async fn get_shard_sequence_range(
        &self,
        table_name: &str,
        shard_id: &str,
    ) -> Result<(Option<String>, Option<String>), BackendError>;

    // -----------------------------------------------------------------------
    // Cache tracking
    // -----------------------------------------------------------------------

    async fn touch_cached_at(
        &self,
        table_name: &str,
        pk: &str,
        sk: &str,
        timestamp: f64,
    ) -> Result<(), BackendError>;

    async fn get_lru_items(
        &self,
        table_name: &str,
        limit: usize,
    ) -> Result<Vec<(String, String, i64)>, BackendError>;
}
