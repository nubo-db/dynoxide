//! Native [`StorageBackend`] implementation backed by the rusqlite-typed
//! [`Storage`].
//!
//! Each impl method is a thin async wrapper over the existing sync method on
//! `Storage`: the body invokes the sync method synchronously and returns a
//! ready future. No `spawn_blocking` is used — today's behaviour of running
//! rusqlite calls on the active executor thread is preserved exactly.

use crate::errors::DynoxideError;
use crate::storage::{
    CreateTableMetadata, DatabaseInfo, QueryParams, ScanParams, Storage, StreamRecord,
    TableMetadata, TableStats,
};
use crate::storage_backend::{BackendError, StorageBackend, error};
use crate::types::Tag;

/// Convert a [`DynoxideError`] into a [`BackendError`].
///
/// Storage's sync surface returns `Result<T, DynoxideError>`; the trait surface
/// returns `Result<T, BackendError>`. The conversion preserves rusqlite error
/// codes via [`error::from_rusqlite`] and falls through any other variant
/// (`InternalServerError`, `ValidationException`, etc.) into
/// [`BackendError::Other`] carrying the original `Display` output.
fn dyno_to_backend(err: DynoxideError) -> BackendError {
    match err {
        DynoxideError::SqliteError(e) => error::from_rusqlite(e),
        other => BackendError::Other(other.to_string()),
    }
}

impl StorageBackend for Storage {
    async fn insert_table_metadata(&self, m: &CreateTableMetadata<'_>) -> Result<(), BackendError> {
        Storage::insert_table_metadata(self, m).map_err(dyno_to_backend)
    }

    async fn get_table_metadata(
        &self,
        table_name: &str,
    ) -> Result<Option<TableMetadata>, BackendError> {
        Storage::get_table_metadata(self, table_name).map_err(dyno_to_backend)
    }

    async fn delete_table_metadata(&self, table_name: &str) -> Result<bool, BackendError> {
        Storage::delete_table_metadata(self, table_name).map_err(dyno_to_backend)
    }

    async fn update_table_metadata(
        &self,
        table_name: &str,
        attribute_definitions: &str,
        gsi_definitions: Option<&str>,
    ) -> Result<(), BackendError> {
        Storage::update_table_metadata(self, table_name, attribute_definitions, gsi_definitions)
            .map_err(dyno_to_backend)
    }

    async fn update_provisioned_throughput(
        &self,
        table_name: &str,
        provisioned_throughput: &str,
    ) -> Result<(), BackendError> {
        Storage::update_provisioned_throughput(self, table_name, provisioned_throughput)
            .map_err(dyno_to_backend)
    }

    async fn clear_provisioned_throughput(&self, table_name: &str) -> Result<(), BackendError> {
        Storage::clear_provisioned_throughput(self, table_name).map_err(dyno_to_backend)
    }

    async fn update_billing_mode(
        &self,
        table_name: &str,
        billing_mode: &str,
    ) -> Result<(), BackendError> {
        Storage::update_billing_mode(self, table_name, billing_mode).map_err(dyno_to_backend)
    }

    async fn get_tags(&self, table_name: &str) -> Result<Vec<Tag>, BackendError> {
        Storage::get_tags(self, table_name).map_err(dyno_to_backend)
    }

    async fn set_tags(&self, table_name: &str, new_tags: &[Tag]) -> Result<(), BackendError> {
        Storage::set_tags(self, table_name, new_tags).map_err(dyno_to_backend)
    }

    async fn update_deletion_protection(
        &self,
        table_name: &str,
        enabled: bool,
    ) -> Result<(), BackendError> {
        Storage::update_deletion_protection(self, table_name, enabled).map_err(dyno_to_backend)
    }

    async fn remove_tags(&self, table_name: &str, keys: &[String]) -> Result<(), BackendError> {
        Storage::remove_tags(self, table_name, keys).map_err(dyno_to_backend)
    }

    async fn list_table_names(&self) -> Result<Vec<String>, BackendError> {
        Storage::list_table_names(self).map_err(dyno_to_backend)
    }

    async fn table_exists(&self, table_name: &str) -> Result<bool, BackendError> {
        Storage::table_exists(self, table_name).map_err(dyno_to_backend)
    }

    async fn create_data_table(&self, table_name: &str) -> Result<(), BackendError> {
        Storage::create_data_table(self, table_name).map_err(dyno_to_backend)
    }

    async fn drop_data_table(&self, table_name: &str) -> Result<(), BackendError> {
        Storage::drop_data_table(self, table_name).map_err(dyno_to_backend)
    }

    async fn create_gsi_table(
        &self,
        table_name: &str,
        index_name: &str,
    ) -> Result<(), BackendError> {
        Storage::create_gsi_table(self, table_name, index_name).map_err(dyno_to_backend)
    }

    async fn drop_gsi_table(&self, table_name: &str, index_name: &str) -> Result<(), BackendError> {
        Storage::drop_gsi_table(self, table_name, index_name).map_err(dyno_to_backend)
    }

    async fn create_lsi_table(
        &self,
        table_name: &str,
        index_name: &str,
    ) -> Result<(), BackendError> {
        Storage::create_lsi_table(self, table_name, index_name).map_err(dyno_to_backend)
    }

    async fn drop_lsi_table(&self, table_name: &str, index_name: &str) -> Result<(), BackendError> {
        Storage::drop_lsi_table(self, table_name, index_name).map_err(dyno_to_backend)
    }

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
        Storage::insert_gsi_item(
            self, table_name, index_name, gsi_pk, gsi_sk, table_pk, table_sk, item_json,
        )
        .map_err(dyno_to_backend)
    }

    async fn delete_gsi_item(
        &self,
        table_name: &str,
        index_name: &str,
        table_pk: &str,
        table_sk: &str,
    ) -> Result<(), BackendError> {
        Storage::delete_gsi_item(self, table_name, index_name, table_pk, table_sk)
            .map_err(dyno_to_backend)
    }

    async fn query_gsi_items(
        &self,
        table_name: &str,
        index_name: &str,
        gsi_pk: &str,
        params: &QueryParams<'_>,
    ) -> Result<Vec<(String, String, String)>, BackendError> {
        Storage::query_gsi_items(self, table_name, index_name, gsi_pk, params)
            .map_err(dyno_to_backend)
    }

    async fn scan_gsi_items(
        &self,
        table_name: &str,
        index_name: &str,
        params: &ScanParams<'_>,
    ) -> Result<Vec<(String, String, String)>, BackendError> {
        Storage::scan_gsi_items(self, table_name, index_name, params).map_err(dyno_to_backend)
    }

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
        Storage::insert_lsi_item(
            self, table_name, index_name, pk, sk, base_pk, base_sk, item_json,
        )
        .map_err(dyno_to_backend)
    }

    async fn delete_lsi_item(
        &self,
        table_name: &str,
        index_name: &str,
        base_pk: &str,
        base_sk: &str,
    ) -> Result<(), BackendError> {
        Storage::delete_lsi_item(self, table_name, index_name, base_pk, base_sk)
            .map_err(dyno_to_backend)
    }

    async fn query_lsi_items(
        &self,
        table_name: &str,
        index_name: &str,
        pk: &str,
        params: &QueryParams<'_>,
    ) -> Result<Vec<(String, String, String)>, BackendError> {
        Storage::query_lsi_items(self, table_name, index_name, pk, params).map_err(dyno_to_backend)
    }

    async fn scan_lsi_items(
        &self,
        table_name: &str,
        index_name: &str,
        params: &ScanParams<'_>,
    ) -> Result<Vec<(String, String, String)>, BackendError> {
        Storage::scan_lsi_items(self, table_name, index_name, params).map_err(dyno_to_backend)
    }

    async fn begin_transaction(&self) -> Result<(), BackendError> {
        Storage::begin_transaction(self).map_err(dyno_to_backend)
    }

    async fn commit(&self) -> Result<(), BackendError> {
        Storage::commit(self).map_err(dyno_to_backend)
    }

    async fn rollback(&self) -> Result<(), BackendError> {
        Storage::rollback(self).map_err(dyno_to_backend)
    }

    async fn enable_bulk_loading(&self) -> Result<(), BackendError> {
        Storage::enable_bulk_loading(self).map_err(dyno_to_backend)
    }

    async fn disable_bulk_loading(&self) -> Result<(), BackendError> {
        Storage::disable_bulk_loading(self).map_err(dyno_to_backend)
    }

    async fn put_item(
        &self,
        table_name: &str,
        pk: &str,
        sk: &str,
        item_json: &str,
        item_size: usize,
    ) -> Result<Option<String>, BackendError> {
        Storage::put_item(self, table_name, pk, sk, item_json, item_size).map_err(dyno_to_backend)
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
        Storage::put_item_with_hash(self, table_name, pk, sk, item_json, item_size, hash_prefix)
            .map_err(dyno_to_backend)
    }

    async fn get_item(
        &self,
        table_name: &str,
        pk: &str,
        sk: &str,
    ) -> Result<Option<String>, BackendError> {
        Storage::get_item(self, table_name, pk, sk).map_err(dyno_to_backend)
    }

    async fn get_partition_size(&self, table_name: &str, pk: &str) -> Result<i64, BackendError> {
        Storage::get_partition_size(self, table_name, pk).map_err(dyno_to_backend)
    }

    async fn get_lsi_partition_size(
        &self,
        table_name: &str,
        index_name: &str,
        pk: &str,
    ) -> Result<i64, BackendError> {
        Storage::get_lsi_partition_size(self, table_name, index_name, pk).map_err(dyno_to_backend)
    }

    async fn delete_item(
        &self,
        table_name: &str,
        pk: &str,
        sk: &str,
    ) -> Result<Option<String>, BackendError> {
        Storage::delete_item(self, table_name, pk, sk).map_err(dyno_to_backend)
    }

    async fn query_items(
        &self,
        table_name: &str,
        pk: &str,
        params: &QueryParams<'_>,
    ) -> Result<Vec<(String, String, String)>, BackendError> {
        Storage::query_items(self, table_name, pk, params).map_err(dyno_to_backend)
    }

    async fn scan_items(
        &self,
        table_name: &str,
        params: &ScanParams<'_>,
    ) -> Result<Vec<(String, String, String)>, BackendError> {
        Storage::scan_items(self, table_name, params).map_err(dyno_to_backend)
    }

    async fn count_items(&self, table_name: &str) -> Result<i64, BackendError> {
        Storage::count_items(self, table_name).map_err(dyno_to_backend)
    }

    async fn db_size_bytes(&self) -> Result<u64, BackendError> {
        Storage::db_size_bytes(self).map_err(dyno_to_backend)
    }

    async fn table_count(&self) -> Result<usize, BackendError> {
        Storage::table_count(self).map_err(dyno_to_backend)
    }

    async fn table_stats(&self) -> Result<Vec<TableStats>, BackendError> {
        Storage::table_stats(self).map_err(dyno_to_backend)
    }

    async fn database_info(&self) -> Result<DatabaseInfo, BackendError> {
        Storage::database_info(self).map_err(dyno_to_backend)
    }

    async fn vacuum(&self) -> Result<(), BackendError> {
        Storage::vacuum(self).map_err(dyno_to_backend)
    }

    async fn enable_stream(
        &self,
        table_name: &str,
        view_type: &str,
        label: &str,
    ) -> Result<(), BackendError> {
        Storage::enable_stream(self, table_name, view_type, label).map_err(dyno_to_backend)
    }

    async fn disable_stream(&self, table_name: &str) -> Result<(), BackendError> {
        Storage::disable_stream(self, table_name).map_err(dyno_to_backend)
    }

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
    ) -> Result<(), BackendError> {
        Storage::insert_stream_record(
            self,
            table_name,
            event_name,
            keys_json,
            new_image,
            old_image,
            sequence_number,
            shard_id,
            created_at,
        )
        .map_err(dyno_to_backend)
    }

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
    ) -> Result<(), BackendError> {
        Storage::insert_stream_record_with_identity(
            self,
            table_name,
            event_name,
            keys_json,
            new_image,
            old_image,
            sequence_number,
            shard_id,
            created_at,
            user_identity,
        )
        .map_err(dyno_to_backend)
    }

    async fn next_stream_sequence_number(&self, table_name: &str) -> Result<i64, BackendError> {
        Storage::next_stream_sequence_number(self, table_name).map_err(dyno_to_backend)
    }

    async fn get_stream_records(
        &self,
        table_name: &str,
        shard_id: &str,
        after_sequence: i64,
        limit: usize,
    ) -> Result<Vec<StreamRecord>, BackendError> {
        Storage::get_stream_records(self, table_name, shard_id, after_sequence, limit)
            .map_err(dyno_to_backend)
    }

    async fn list_stream_enabled_tables(&self) -> Result<Vec<TableMetadata>, BackendError> {
        Storage::list_stream_enabled_tables(self).map_err(dyno_to_backend)
    }

    async fn update_ttl_config(
        &self,
        table_name: &str,
        attribute_name: Option<&str>,
        enabled: bool,
    ) -> Result<(), BackendError> {
        Storage::update_ttl_config(self, table_name, attribute_name, enabled)
            .map_err(dyno_to_backend)
    }

    async fn list_ttl_enabled_tables(&self) -> Result<Vec<TableMetadata>, BackendError> {
        Storage::list_ttl_enabled_tables(self).map_err(dyno_to_backend)
    }

    async fn get_shard_sequence_range(
        &self,
        table_name: &str,
        shard_id: &str,
    ) -> Result<(Option<String>, Option<String>), BackendError> {
        Storage::get_shard_sequence_range(self, table_name, shard_id).map_err(dyno_to_backend)
    }

    async fn touch_cached_at(
        &self,
        table_name: &str,
        pk: &str,
        sk: &str,
        timestamp: f64,
    ) -> Result<(), BackendError> {
        Storage::touch_cached_at(self, table_name, pk, sk, timestamp).map_err(dyno_to_backend)
    }

    async fn get_lru_items(
        &self,
        table_name: &str,
        limit: usize,
    ) -> Result<Vec<(String, String, i64)>, BackendError> {
        Storage::get_lru_items(self, table_name, limit).map_err(dyno_to_backend)
    }
}
