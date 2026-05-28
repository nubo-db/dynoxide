//! Working wa-sqlite [`StorageBackend`] over a wasm-bindgen bridge.
//!
//! `WasmBridgeBackend` runs the same SQL the native backend issues - both
//! consume the shared builders in [`sql_builders`] - but executes it against a
//! JS wa-sqlite database through the bridge in `js/wa-sqlite-bridge.js`. The
//! bridge runs inside a Web Worker and persists to OPFS via wa-sqlite's
//! synchronous access-handle VFS, which browsers expose only in a Worker. The
//! page drives the engine over a message RPC; no cross-origin isolation
//! (COOP/COEP) is required.
//!
//! # Async, never blocking
//!
//! Every method awaits a real JS promise (the bridge call), so unlike the
//! native backend these futures genuinely suspend. The wasm `Database` facade
//! therefore exposes `async fn` and never calls `block_on` - the wasm main
//! thread must not block.
//!
//! # Preview status
//!
//! This backend is not verified by the conformance suite. It covers the
//! CRUD/query/scan/GSI/LSI surface. TTL and the cross-item `TransactWriteItems`
//! action return [`BackendError::Unsupported`]. Streams are planned - their
//! real-time delivery mechanism is a separate design - and currently return a
//! preview "not yet implemented" error rather than a hard refusal. See the
//! WASM note in the README.
//!
//! # Coverage in this commit
//!
//! The base-table CRUD spine (table metadata, data tables, put/get/delete,
//! transactions) runs against the bridge here. The query/scan and GSI/LSI
//! builders land in the following commit; until then those methods return a
//! preview "not yet implemented" error. Streams are pending (delivery
//! mechanism still to be designed) and return the same preview error; TTL is
//! [`BackendError::Unsupported`].

use std::sync::Arc;

use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

use crate::storage::{
    CreateTableMetadata, DatabaseInfo, QueryParams, ScanParams, StreamRecord, TableMetadata,
    TableStats,
};
use crate::storage_backend::sql_builders::{self, SqlParam};
use crate::storage_backend::{BackendError, BaseItemRow, Clock, GsiItemRow, StorageBackend, SystemClock};
use crate::types::Tag;

#[wasm_bindgen(module = "/js/wa-sqlite-bridge.js")]
extern "C" {
    #[wasm_bindgen(catch, js_name = "open")]
    async fn wa_open(name: &str) -> Result<JsValue, JsValue>;

    #[wasm_bindgen(catch, js_name = "exec")]
    async fn wa_exec(handle: &JsValue, sql: &str, params: js_sys::Array) -> Result<JsValue, JsValue>;

    #[wasm_bindgen(catch, js_name = "query")]
    async fn wa_query(
        handle: &JsValue,
        sql: &str,
        params: js_sys::Array,
    ) -> Result<JsValue, JsValue>;
}

/// wa-sqlite-backed storage backend driven through the JS bridge.
pub struct WasmBridgeBackend {
    /// Opaque JS handle returned by the bridge `open`.
    handle: JsValue,
    /// Wall clock for the trait's stream/TTL paths; `web-time`-backed on wasm.
    clock: Arc<dyn Clock>,
}

impl WasmBridgeBackend {
    /// Open (or create) a wa-sqlite database persisted under `name` (OPFS),
    /// bootstrapping the shared metadata schema on first use.
    pub async fn open(name: &str) -> Result<Self, BackendError> {
        let handle = wa_open(name).await.map_err(js_err)?;
        // Bootstrap the same metadata/config/stream schema the native backend
        // creates in `initialize`. `INIT_SCHEMA` is a multi-statement batch;
        // the bridge runs each statement in turn.
        wa_exec(&handle, sql_builders::INIT_SCHEMA, js_sys::Array::new())
            .await
            .map_err(js_err)?;
        Ok(Self {
            handle,
            clock: SystemClock::arc(),
        })
    }

    /// Run a statement that returns no rows.
    async fn exec(&self, sql: &str, params: Vec<SqlParam<'_>>) -> Result<(), BackendError> {
        wa_exec(&self.handle, sql, params_to_js(&params))
            .await
            .map_err(js_err)?;
        Ok(())
    }

    /// Run a query, returning rows as a JS array of column arrays.
    async fn query(&self, sql: &str, params: Vec<SqlParam<'_>>) -> Result<js_sys::Array, BackendError> {
        let rows = wa_query(&self.handle, sql, params_to_js(&params))
            .await
            .map_err(js_err)?;
        Ok(rows.unchecked_into())
    }

    /// Shared put path: capture the prior value, then insert-or-replace.
    async fn put(
        &self,
        table_name: &str,
        pk: &str,
        sk: &str,
        item_json: &str,
        item_size: usize,
        hash_prefix: &str,
    ) -> Result<Option<String>, BackendError> {
        let old_item = self.get_item(table_name, pk, sk).await?;
        let (sql, params) =
            sql_builders::put_item_with_hash(table_name, pk, sk, item_json, item_size, hash_prefix);
        self.exec(&sql, params).await?;
        Ok(old_item)
    }
}

// --- JS <-> SqlParam conversion and row readers -------------------------

/// Convert bound parameters to a positional JS array (`?1` -> index 0).
fn params_to_js(params: &[SqlParam<'_>]) -> js_sys::Array {
    let arr = js_sys::Array::new();
    for p in params {
        arr.push(&sqlparam_to_js(p));
    }
    arr
}

fn sqlparam_to_js(p: &SqlParam<'_>) -> JsValue {
    match p {
        SqlParam::Text(s) => JsValue::from_str(s),
        // JS numbers are f64. The integer parameters here (sizes, counts, epoch
        // seconds) stay well within 2^53, so this is lossless in practice.
        SqlParam::Integer(i) => JsValue::from_f64(*i as f64),
        SqlParam::Real(f) => JsValue::from_f64(*f),
        SqlParam::Blob(b) => js_sys::Uint8Array::from(&**b).into(),
        SqlParam::Null => JsValue::NULL,
    }
}

/// Read column `i` as text, treating SQL NULL/undefined as `None`.
fn col_text(row: &js_sys::Array, i: u32) -> Option<String> {
    let v = row.get(i);
    if v.is_null() || v.is_undefined() {
        None
    } else {
        v.as_string()
    }
}

/// Read column `i` as an integer (0 when absent or non-numeric).
fn col_i64(row: &js_sys::Array, i: u32) -> i64 {
    row.get(i).as_f64().map(|f| f as i64).unwrap_or(0)
}

/// Map query/scan result rows (each `[c0, c1, c2]`) to `(String, String, String)`.
fn rows_to_triples(rows: &js_sys::Array) -> Vec<(String, String, String)> {
    let mut out = Vec::with_capacity(rows.length() as usize);
    for i in 0..rows.length() {
        let row: js_sys::Array = rows.get(i).unchecked_into();
        out.push((
            col_text(&row, 0).unwrap_or_default(),
            col_text(&row, 1).unwrap_or_default(),
            col_text(&row, 2).unwrap_or_default(),
        ));
    }
    out
}

/// Map a `_tables` row (column order per [`sql_builders`]) to [`TableMetadata`].
fn row_to_metadata(row: &js_sys::Array) -> TableMetadata {
    TableMetadata {
        table_name: col_text(row, 0).unwrap_or_default(),
        key_schema: col_text(row, 1).unwrap_or_default(),
        attribute_definitions: col_text(row, 2).unwrap_or_default(),
        gsi_definitions: col_text(row, 3),
        lsi_definitions: col_text(row, 4),
        stream_enabled: col_i64(row, 5) != 0,
        stream_view_type: col_text(row, 6),
        stream_label: col_text(row, 7),
        ttl_attribute: col_text(row, 8),
        ttl_enabled: col_i64(row, 9) != 0,
        created_at: col_i64(row, 10),
        table_status: col_text(row, 11).unwrap_or_default(),
        billing_mode: col_text(row, 12),
        provisioned_throughput: col_text(row, 13),
        sse_specification: col_text(row, 14),
        table_class: col_text(row, 15),
        deletion_protection_enabled: col_i64(row, 16) != 0,
    }
}

/// Wrap a JS error from the bridge as a backend error.
fn js_err(e: JsValue) -> BackendError {
    let msg = e.as_string().unwrap_or_else(|| format!("{e:?}"));
    BackendError::Other(format!("wa-sqlite: {msg}"))
}

/// A capability the wasm backend does not provide (TTL needs a background
/// expiry sweep that the browser runtime does not drive).
fn unsupported(capability: &'static str) -> BackendError {
    BackendError::Unsupported { capability }
}

/// A method that is planned but not yet implemented in this preview - the
/// query/scan and GSI/LSI builders (next commit) and streams (delivery
/// mechanism still to be designed).
fn not_yet(what: &str) -> BackendError {
    BackendError::Other(format!("wasm backend (preview): {what} not yet implemented"))
}

impl StorageBackend for WasmBridgeBackend {
    fn clock(&self) -> &dyn Clock {
        self.clock.as_ref()
    }

    // --- Table metadata --------------------------------------------------

    async fn insert_table_metadata(&self, m: &CreateTableMetadata<'_>) -> Result<(), BackendError> {
        let (sql, params) = sql_builders::insert_table_metadata(m);
        self.exec(&sql, params).await
    }

    async fn get_table_metadata(
        &self,
        table_name: &str,
    ) -> Result<Option<TableMetadata>, BackendError> {
        let (sql, params) = sql_builders::get_table_metadata(table_name);
        let rows = self.query(&sql, params).await?;
        if rows.length() == 0 {
            return Ok(None);
        }
        let row: js_sys::Array = rows.get(0).unchecked_into();
        Ok(Some(row_to_metadata(&row)))
    }

    async fn delete_table_metadata(&self, table_name: &str) -> Result<bool, BackendError> {
        let existed = self.table_exists(table_name).await?;
        let (sql, params) = sql_builders::delete_table_metadata(table_name);
        self.exec(&sql, params).await?;
        Ok(existed)
    }

    async fn update_table_metadata(
        &self,
        _table_name: &str,
        _attribute_definitions: &str,
        _gsi_definitions: Option<&str>,
    ) -> Result<(), BackendError> {
        Err(not_yet("update_table_metadata"))
    }

    async fn update_provisioned_throughput(
        &self,
        _table_name: &str,
        _provisioned_throughput: &str,
    ) -> Result<(), BackendError> {
        Err(not_yet("update_provisioned_throughput"))
    }

    async fn clear_provisioned_throughput(&self, _table_name: &str) -> Result<(), BackendError> {
        Err(not_yet("clear_provisioned_throughput"))
    }

    async fn update_billing_mode(
        &self,
        _table_name: &str,
        _billing_mode: &str,
    ) -> Result<(), BackendError> {
        Err(not_yet("update_billing_mode"))
    }

    async fn get_tags(&self, _table_name: &str) -> Result<Vec<Tag>, BackendError> {
        Err(not_yet("get_tags"))
    }

    async fn set_tags(&self, _table_name: &str, _new_tags: &[Tag]) -> Result<(), BackendError> {
        Err(not_yet("set_tags"))
    }

    async fn update_deletion_protection(
        &self,
        _table_name: &str,
        _enabled: bool,
    ) -> Result<(), BackendError> {
        Err(not_yet("update_deletion_protection"))
    }

    async fn remove_tags(&self, _table_name: &str, _keys: &[String]) -> Result<(), BackendError> {
        Err(not_yet("remove_tags"))
    }

    async fn list_table_names(&self) -> Result<Vec<String>, BackendError> {
        let (sql, params) = sql_builders::list_table_names();
        let rows = self.query(&sql, params).await?;
        let mut names = Vec::with_capacity(rows.length() as usize);
        for i in 0..rows.length() {
            let row: js_sys::Array = rows.get(i).unchecked_into();
            if let Some(name) = col_text(&row, 0) {
                names.push(name);
            }
        }
        Ok(names)
    }

    async fn table_exists(&self, table_name: &str) -> Result<bool, BackendError> {
        let (sql, params) = sql_builders::table_exists(table_name);
        let rows = self.query(&sql, params).await?;
        if rows.length() == 0 {
            return Ok(false);
        }
        let row: js_sys::Array = rows.get(0).unchecked_into();
        Ok(col_i64(&row, 0) > 0)
    }

    // --- Data tables -----------------------------------------------------

    async fn create_data_table(&self, table_name: &str) -> Result<(), BackendError> {
        let (sql, params) = sql_builders::create_data_table(table_name);
        self.exec(&sql, params).await
    }

    async fn drop_data_table(&self, table_name: &str) -> Result<(), BackendError> {
        let (sql, params) = sql_builders::drop_data_table(table_name);
        self.exec(&sql, params).await
    }

    async fn create_gsi_table(
        &self,
        table_name: &str,
        index_name: &str,
    ) -> Result<(), BackendError> {
        let (sql, params) = sql_builders::create_gsi_table(table_name, index_name);
        self.exec(&sql, params).await
    }

    async fn drop_gsi_table(
        &self,
        table_name: &str,
        index_name: &str,
    ) -> Result<(), BackendError> {
        let (sql, params) = sql_builders::drop_gsi_table(table_name, index_name);
        self.exec(&sql, params).await
    }

    async fn create_lsi_table(
        &self,
        table_name: &str,
        index_name: &str,
    ) -> Result<(), BackendError> {
        let (sql, params) = sql_builders::create_lsi_table(table_name, index_name);
        self.exec(&sql, params).await
    }

    async fn drop_lsi_table(
        &self,
        table_name: &str,
        index_name: &str,
    ) -> Result<(), BackendError> {
        let (sql, params) = sql_builders::drop_lsi_table(table_name, index_name);
        self.exec(&sql, params).await
    }

    // --- GSI items -------------------------------------------------------

    async fn insert_gsi_item(
        &self,
        table_name: &str,
        index_name: &str,
        gsi_pk: &str,
        gsi_sk: &str,
        table_pk: &str,
        table_sk: &str,
        item_json: &str,
    ) -> Result<(), BackendError> {
        let sql = sql_builders::gsi_insert_sql(table_name, index_name);
        let params = sql_builders::gsi_insert_params(gsi_pk, gsi_sk, table_pk, table_sk, item_json);
        self.exec(&sql, params).await
    }

    async fn insert_gsi_items(
        &self,
        table_name: &str,
        index_name: &str,
        rows: &[GsiItemRow],
    ) -> Result<(), BackendError> {
        let sql = sql_builders::gsi_insert_sql(table_name, index_name);
        for row in rows {
            let params = sql_builders::gsi_insert_params(
                &row.gsi_pk,
                &row.gsi_sk,
                &row.table_pk,
                &row.table_sk,
                &row.item_json,
            );
            self.exec(&sql, params).await?;
        }
        Ok(())
    }

    async fn delete_gsi_item(
        &self,
        table_name: &str,
        index_name: &str,
        table_pk: &str,
        table_sk: &str,
    ) -> Result<(), BackendError> {
        let (sql, params) =
            sql_builders::delete_gsi_item(table_name, index_name, table_pk, table_sk);
        self.exec(&sql, params).await
    }

    async fn query_gsi_items(
        &self,
        table_name: &str,
        index_name: &str,
        gsi_pk: &str,
        params: &QueryParams<'_>,
    ) -> Result<Vec<(String, String, String)>, BackendError> {
        let (sql, p) = sql_builders::query_gsi_items(table_name, index_name, gsi_pk, params);
        let rows = self.query(&sql, p).await?;
        Ok(rows_to_triples(&rows))
    }

    async fn scan_gsi_items(
        &self,
        table_name: &str,
        index_name: &str,
        params: &ScanParams<'_>,
    ) -> Result<Vec<(String, String, String)>, BackendError> {
        let (sql, p) = sql_builders::scan_gsi_items(table_name, index_name, params);
        let rows = self.query(&sql, p).await?;
        Ok(rows_to_triples(&rows))
    }

    // --- LSI items -------------------------------------------------------

    async fn insert_lsi_item(
        &self,
        table_name: &str,
        index_name: &str,
        pk: &str,
        sk: &str,
        base_pk: &str,
        base_sk: &str,
        item_json: &str,
    ) -> Result<(), BackendError> {
        let sql = sql_builders::lsi_insert_sql(table_name, index_name);
        let params = sql_builders::lsi_insert_params(pk, sk, base_pk, base_sk, item_json);
        self.exec(&sql, params).await
    }

    async fn delete_lsi_item(
        &self,
        table_name: &str,
        index_name: &str,
        base_pk: &str,
        base_sk: &str,
    ) -> Result<(), BackendError> {
        let (sql, params) =
            sql_builders::delete_lsi_item(table_name, index_name, base_pk, base_sk);
        self.exec(&sql, params).await
    }

    async fn query_lsi_items(
        &self,
        table_name: &str,
        index_name: &str,
        pk: &str,
        params: &QueryParams<'_>,
    ) -> Result<Vec<(String, String, String)>, BackendError> {
        let (sql, p) = sql_builders::query_lsi_items(table_name, index_name, pk, params);
        let rows = self.query(&sql, p).await?;
        Ok(rows_to_triples(&rows))
    }

    async fn scan_lsi_items(
        &self,
        table_name: &str,
        index_name: &str,
        params: &ScanParams<'_>,
    ) -> Result<Vec<(String, String, String)>, BackendError> {
        let (sql, p) = sql_builders::scan_lsi_items(table_name, index_name, params);
        let rows = self.query(&sql, p).await?;
        Ok(rows_to_triples(&rows))
    }

    // --- Transactions ----------------------------------------------------

    async fn begin_transaction(&self) -> Result<(), BackendError> {
        self.exec(sql_builders::BEGIN, Vec::new()).await
    }

    async fn commit(&self) -> Result<(), BackendError> {
        self.exec(sql_builders::COMMIT, Vec::new()).await
    }

    async fn rollback(&self) -> Result<(), BackendError> {
        self.exec(sql_builders::ROLLBACK, Vec::new()).await
    }

    // Bulk-loading PRAGMAs do not apply to the wa-sqlite OPFS VFS; treat the
    // toggles as no-ops so callers that bracket writes still work.
    async fn enable_bulk_loading(&self) -> Result<(), BackendError> {
        Ok(())
    }

    async fn disable_bulk_loading(&self) -> Result<(), BackendError> {
        Ok(())
    }

    // --- Item CRUD -------------------------------------------------------

    async fn put_item(
        &self,
        table_name: &str,
        pk: &str,
        sk: &str,
        item_json: &str,
        item_size: usize,
    ) -> Result<Option<String>, BackendError> {
        self.put(table_name, pk, sk, item_json, item_size, "").await
    }

    async fn put_item_with_hash(
        &self,
        table_name: &str,
        pk: &str,
        sk: &str,
        item_json: &str,
        item_size: usize,
        hash_prefix: &str,
    ) -> Result<Option<String>, BackendError> {
        self.put(table_name, pk, sk, item_json, item_size, hash_prefix)
            .await
    }

    async fn put_base_items(
        &self,
        _table_name: &str,
        _rows: &[BaseItemRow],
    ) -> Result<(), BackendError> {
        Err(not_yet("put_base_items"))
    }

    async fn get_item(
        &self,
        table_name: &str,
        pk: &str,
        sk: &str,
    ) -> Result<Option<String>, BackendError> {
        let (sql, params) = sql_builders::get_item(table_name, pk, sk);
        let rows = self.query(&sql, params).await?;
        if rows.length() == 0 {
            return Ok(None);
        }
        let row: js_sys::Array = rows.get(0).unchecked_into();
        Ok(col_text(&row, 0))
    }

    async fn get_partition_size(&self, table_name: &str, pk: &str) -> Result<i64, BackendError> {
        let (sql, params) = sql_builders::get_partition_size(table_name, pk);
        let rows = self.query(&sql, params).await?;
        if rows.length() == 0 {
            return Ok(0);
        }
        let row: js_sys::Array = rows.get(0).unchecked_into();
        Ok(col_i64(&row, 0))
    }

    async fn get_lsi_partition_size(
        &self,
        table_name: &str,
        index_name: &str,
        pk: &str,
    ) -> Result<i64, BackendError> {
        let (sql, params) = sql_builders::get_lsi_partition_size(table_name, index_name, pk);
        let rows = self.query(&sql, params).await?;
        if rows.length() == 0 {
            return Ok(0);
        }
        let row: js_sys::Array = rows.get(0).unchecked_into();
        Ok(col_i64(&row, 0))
    }

    async fn delete_item(
        &self,
        table_name: &str,
        pk: &str,
        sk: &str,
    ) -> Result<Option<String>, BackendError> {
        let old_item = self.get_item(table_name, pk, sk).await?;
        let (sql, params) = sql_builders::delete_item(table_name, pk, sk);
        self.exec(&sql, params).await?;
        Ok(old_item)
    }

    async fn query_items(
        &self,
        table_name: &str,
        pk: &str,
        params: &QueryParams<'_>,
    ) -> Result<Vec<(String, String, String)>, BackendError> {
        let (sql, p) = sql_builders::query_items(table_name, pk, params);
        let rows = self.query(&sql, p).await?;
        Ok(rows_to_triples(&rows))
    }

    async fn scan_items(
        &self,
        table_name: &str,
        params: &ScanParams<'_>,
    ) -> Result<Vec<(String, String, String)>, BackendError> {
        let (sql, p) = sql_builders::scan_items(table_name, params);
        let rows = self.query(&sql, p).await?;
        Ok(rows_to_triples(&rows))
    }

    async fn count_items(&self, table_name: &str) -> Result<i64, BackendError> {
        let (sql, params) = sql_builders::count_items(table_name);
        let rows = self.query(&sql, params).await?;
        if rows.length() == 0 {
            return Ok(0);
        }
        let row: js_sys::Array = rows.get(0).unchecked_into();
        Ok(col_i64(&row, 0))
    }

    // --- Introspection ---------------------------------------------------

    async fn db_size_bytes(&self) -> Result<u64, BackendError> {
        Err(not_yet("db_size_bytes"))
    }

    async fn table_count(&self) -> Result<usize, BackendError> {
        Err(not_yet("table_count"))
    }

    async fn table_stats(&self) -> Result<Vec<TableStats>, BackendError> {
        Err(not_yet("table_stats"))
    }

    async fn database_info(&self) -> Result<DatabaseInfo, BackendError> {
        Err(not_yet("database_info"))
    }

    async fn vacuum(&self) -> Result<(), BackendError> {
        Err(not_yet("vacuum"))
    }

    // --- Streams (planned; delivery mechanism to be designed) ------------

    async fn enable_stream(
        &self,
        _table_name: &str,
        _view_type: &str,
        _label: &str,
    ) -> Result<(), BackendError> {
        Err(not_yet("streams"))
    }

    async fn disable_stream(&self, _table_name: &str) -> Result<(), BackendError> {
        Err(not_yet("streams"))
    }

    async fn insert_stream_record(
        &self,
        _table_name: &str,
        _event_name: &str,
        _keys_json: &str,
        _new_image: Option<&str>,
        _old_image: Option<&str>,
        _sequence_number: &str,
        _shard_id: &str,
        _created_at: i64,
    ) -> Result<(), BackendError> {
        Err(not_yet("streams"))
    }

    async fn insert_stream_record_with_identity(
        &self,
        _table_name: &str,
        _event_name: &str,
        _keys_json: &str,
        _new_image: Option<&str>,
        _old_image: Option<&str>,
        _sequence_number: &str,
        _shard_id: &str,
        _created_at: i64,
        _user_identity: Option<&str>,
    ) -> Result<(), BackendError> {
        Err(not_yet("streams"))
    }

    async fn next_stream_sequence_number(&self, _table_name: &str) -> Result<i64, BackendError> {
        Err(not_yet("streams"))
    }

    async fn get_stream_records(
        &self,
        _table_name: &str,
        _shard_id: &str,
        _after_sequence: i64,
        _limit: usize,
    ) -> Result<Vec<StreamRecord>, BackendError> {
        Err(not_yet("streams"))
    }

    async fn list_stream_enabled_tables(&self) -> Result<Vec<TableMetadata>, BackendError> {
        Err(not_yet("streams"))
    }

    async fn get_shard_sequence_range(
        &self,
        _table_name: &str,
        _shard_id: &str,
    ) -> Result<(Option<String>, Option<String>), BackendError> {
        Err(not_yet("streams"))
    }

    // --- TTL (unsupported on wasm) ---------------------------------------

    async fn update_ttl_config(
        &self,
        _table_name: &str,
        _attribute_name: Option<&str>,
        _enabled: bool,
    ) -> Result<(), BackendError> {
        Err(unsupported("ttl"))
    }

    async fn list_ttl_enabled_tables(&self) -> Result<Vec<TableMetadata>, BackendError> {
        Err(unsupported("ttl"))
    }

    // --- Cache (deferred to the follow-up commit) ------------------------

    async fn touch_cached_at(
        &self,
        _table_name: &str,
        _pk: &str,
        _sk: &str,
        _timestamp: f64,
    ) -> Result<(), BackendError> {
        Err(not_yet("touch_cached_at"))
    }

    async fn get_lru_items(
        &self,
        _table_name: &str,
        _limit: usize,
    ) -> Result<Vec<(String, String, i64)>, BackendError> {
        Err(not_yet("get_lru_items"))
    }
}
