//! Compile-only sanity check for the [`StorageBackend`] trait.
//!
//! This test does not exercise behaviour; it confirms the trait can be
//! satisfied natively by a hand-rolled stub, catching signature drift between
//! the trait and any future impls. A companion `wasm32-unknown-unknown`
//! `cargo check` job is the cross-platform sibling.
//!
//! [`StorageBackend`]: dynoxide::storage_backend::StorageBackend

use dynoxide::storage::{
    CreateTableMetadata, DatabaseInfo, QueryParams, ScanParams, StreamRecord, TableMetadata,
    TableStats,
};
use dynoxide::storage_backend::{BackendError, BaseItemRow, Clock, GsiItemRow, StorageBackend};
use dynoxide::types::Tag;

/// A type that satisfies [`StorageBackend`] with `unimplemented!()` bodies.
///
/// Catches missing methods, signature drift, and basic type-fit issues.
pub struct TestBackend;

impl StorageBackend for TestBackend {
    fn clock(&self) -> &dyn Clock {
        unimplemented!()
    }

    async fn insert_gsi_items(
        &self,
        _table_name: &str,
        _index_name: &str,
        _rows: &[GsiItemRow],
    ) -> Result<(), BackendError> {
        unimplemented!()
    }

    async fn put_base_items(
        &self,
        _table_name: &str,
        _rows: &[BaseItemRow],
    ) -> Result<(), BackendError> {
        unimplemented!()
    }

    async fn insert_table_metadata(
        &self,
        _m: &CreateTableMetadata<'_>,
    ) -> Result<(), BackendError> {
        unimplemented!()
    }

    async fn get_table_metadata(
        &self,
        _table_name: &str,
    ) -> Result<Option<TableMetadata>, BackendError> {
        unimplemented!()
    }

    async fn delete_table_metadata(&self, _table_name: &str) -> Result<bool, BackendError> {
        unimplemented!()
    }

    async fn update_table_metadata(
        &self,
        _table_name: &str,
        _attribute_definitions: &str,
        _gsi_definitions: Option<&str>,
    ) -> Result<(), BackendError> {
        unimplemented!()
    }

    async fn update_provisioned_throughput(
        &self,
        _table_name: &str,
        _provisioned_throughput: &str,
    ) -> Result<(), BackendError> {
        unimplemented!()
    }

    async fn clear_provisioned_throughput(&self, _table_name: &str) -> Result<(), BackendError> {
        unimplemented!()
    }

    async fn update_billing_mode(
        &self,
        _table_name: &str,
        _billing_mode: &str,
    ) -> Result<(), BackendError> {
        unimplemented!()
    }

    async fn update_table_class(
        &self,
        _table_name: &str,
        _table_class: &str,
    ) -> Result<(), BackendError> {
        unimplemented!()
    }

    async fn update_on_demand_throughput(
        &self,
        _table_name: &str,
        _on_demand_throughput: &str,
    ) -> Result<(), BackendError> {
        unimplemented!()
    }

    async fn get_tags(&self, _table_name: &str) -> Result<Vec<Tag>, BackendError> {
        unimplemented!()
    }

    async fn set_tags(&self, _table_name: &str, _new_tags: &[Tag]) -> Result<(), BackendError> {
        unimplemented!()
    }

    async fn update_deletion_protection(
        &self,
        _table_name: &str,
        _enabled: bool,
    ) -> Result<(), BackendError> {
        unimplemented!()
    }

    async fn remove_tags(&self, _table_name: &str, _keys: &[String]) -> Result<(), BackendError> {
        unimplemented!()
    }

    async fn list_table_names(&self) -> Result<Vec<String>, BackendError> {
        unimplemented!()
    }

    async fn table_exists(&self, _table_name: &str) -> Result<bool, BackendError> {
        unimplemented!()
    }

    async fn create_data_table(&self, _table_name: &str) -> Result<(), BackendError> {
        unimplemented!()
    }

    async fn drop_data_table(&self, _table_name: &str) -> Result<(), BackendError> {
        unimplemented!()
    }

    async fn create_gsi_table(
        &self,
        _table_name: &str,
        _index_name: &str,
    ) -> Result<(), BackendError> {
        unimplemented!()
    }

    async fn drop_gsi_table(
        &self,
        _table_name: &str,
        _index_name: &str,
    ) -> Result<(), BackendError> {
        unimplemented!()
    }

    async fn create_lsi_table(
        &self,
        _table_name: &str,
        _index_name: &str,
    ) -> Result<(), BackendError> {
        unimplemented!()
    }

    async fn drop_lsi_table(
        &self,
        _table_name: &str,
        _index_name: &str,
    ) -> Result<(), BackendError> {
        unimplemented!()
    }

    async fn insert_gsi_item(
        &self,
        _table_name: &str,
        _index_name: &str,
        _gsi_pk: &str,
        _gsi_sk: &str,
        _table_pk: &str,
        _table_sk: &str,
        _item_json: &str,
    ) -> Result<(), BackendError> {
        unimplemented!()
    }

    async fn delete_gsi_item(
        &self,
        _table_name: &str,
        _index_name: &str,
        _table_pk: &str,
        _table_sk: &str,
    ) -> Result<(), BackendError> {
        unimplemented!()
    }

    async fn query_gsi_items(
        &self,
        _table_name: &str,
        _index_name: &str,
        _gsi_pk: &str,
        _params: &QueryParams<'_>,
    ) -> Result<Vec<(String, String, String)>, BackendError> {
        unimplemented!()
    }

    async fn scan_gsi_items(
        &self,
        _table_name: &str,
        _index_name: &str,
        _params: &ScanParams<'_>,
    ) -> Result<Vec<(String, String, String)>, BackendError> {
        unimplemented!()
    }

    async fn insert_lsi_item(
        &self,
        _table_name: &str,
        _index_name: &str,
        _pk: &str,
        _sk: &str,
        _base_pk: &str,
        _base_sk: &str,
        _item_json: &str,
    ) -> Result<(), BackendError> {
        unimplemented!()
    }

    async fn delete_lsi_item(
        &self,
        _table_name: &str,
        _index_name: &str,
        _base_pk: &str,
        _base_sk: &str,
    ) -> Result<(), BackendError> {
        unimplemented!()
    }

    async fn query_lsi_items(
        &self,
        _table_name: &str,
        _index_name: &str,
        _pk: &str,
        _params: &QueryParams<'_>,
    ) -> Result<Vec<(String, String, String)>, BackendError> {
        unimplemented!()
    }

    async fn scan_lsi_items(
        &self,
        _table_name: &str,
        _index_name: &str,
        _params: &ScanParams<'_>,
    ) -> Result<Vec<(String, String, String)>, BackendError> {
        unimplemented!()
    }

    async fn begin_transaction(&self) -> Result<(), BackendError> {
        unimplemented!()
    }
    async fn commit(&self) -> Result<(), BackendError> {
        unimplemented!()
    }
    async fn rollback(&self) -> Result<(), BackendError> {
        unimplemented!()
    }

    async fn enable_bulk_loading(&self) -> Result<(), BackendError> {
        unimplemented!()
    }
    async fn disable_bulk_loading(&self) -> Result<(), BackendError> {
        unimplemented!()
    }

    async fn put_item(
        &self,
        _table_name: &str,
        _pk: &str,
        _sk: &str,
        _item_json: &str,
        _item_size: usize,
    ) -> Result<Option<String>, BackendError> {
        unimplemented!()
    }

    async fn put_item_with_hash(
        &self,
        _table_name: &str,
        _pk: &str,
        _sk: &str,
        _item_json: &str,
        _item_size: usize,
        _hash_prefix: &str,
    ) -> Result<Option<String>, BackendError> {
        unimplemented!()
    }

    async fn get_item(
        &self,
        _table_name: &str,
        _pk: &str,
        _sk: &str,
    ) -> Result<Option<String>, BackendError> {
        unimplemented!()
    }

    async fn get_partition_size(&self, _table_name: &str, _pk: &str) -> Result<i64, BackendError> {
        unimplemented!()
    }

    async fn get_lsi_partition_size(
        &self,
        _table_name: &str,
        _index_name: &str,
        _pk: &str,
    ) -> Result<i64, BackendError> {
        unimplemented!()
    }

    async fn delete_item(
        &self,
        _table_name: &str,
        _pk: &str,
        _sk: &str,
    ) -> Result<Option<String>, BackendError> {
        unimplemented!()
    }

    async fn query_items(
        &self,
        _table_name: &str,
        _pk: &str,
        _params: &QueryParams<'_>,
    ) -> Result<Vec<(String, String, String)>, BackendError> {
        unimplemented!()
    }

    async fn scan_items(
        &self,
        _table_name: &str,
        _params: &ScanParams<'_>,
    ) -> Result<Vec<(String, String, String)>, BackendError> {
        unimplemented!()
    }

    async fn count_items(&self, _table_name: &str) -> Result<i64, BackendError> {
        unimplemented!()
    }

    async fn db_size_bytes(&self) -> Result<u64, BackendError> {
        unimplemented!()
    }

    async fn table_count(&self) -> Result<usize, BackendError> {
        unimplemented!()
    }

    async fn table_stats(&self) -> Result<Vec<TableStats>, BackendError> {
        unimplemented!()
    }

    async fn database_info(&self) -> Result<DatabaseInfo, BackendError> {
        unimplemented!()
    }

    async fn vacuum(&self) -> Result<(), BackendError> {
        unimplemented!()
    }

    async fn enable_stream(
        &self,
        _table_name: &str,
        _view_type: &str,
        _label: &str,
    ) -> Result<(), BackendError> {
        unimplemented!()
    }

    async fn disable_stream(&self, _table_name: &str) -> Result<(), BackendError> {
        unimplemented!()
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
        unimplemented!()
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
        unimplemented!()
    }

    async fn next_stream_sequence_number(&self, _table_name: &str) -> Result<i64, BackendError> {
        unimplemented!()
    }

    async fn get_stream_records(
        &self,
        _table_name: &str,
        _shard_id: &str,
        _after_sequence: i64,
        _limit: usize,
    ) -> Result<Vec<StreamRecord>, BackendError> {
        unimplemented!()
    }

    async fn list_stream_enabled_tables(&self) -> Result<Vec<TableMetadata>, BackendError> {
        unimplemented!()
    }

    async fn update_ttl_config(
        &self,
        _table_name: &str,
        _attribute_name: Option<&str>,
        _enabled: bool,
    ) -> Result<(), BackendError> {
        unimplemented!()
    }

    async fn list_ttl_enabled_tables(&self) -> Result<Vec<TableMetadata>, BackendError> {
        unimplemented!()
    }

    async fn get_shard_sequence_range(
        &self,
        _table_name: &str,
        _shard_id: &str,
    ) -> Result<(Option<String>, Option<String>), BackendError> {
        unimplemented!()
    }

    async fn touch_cached_at(
        &self,
        _table_name: &str,
        _pk: &str,
        _sk: &str,
        _timestamp: f64,
    ) -> Result<(), BackendError> {
        unimplemented!()
    }

    async fn get_lru_items(
        &self,
        _table_name: &str,
        _limit: usize,
    ) -> Result<Vec<(String, String, i64)>, BackendError> {
        unimplemented!()
    }
}

#[test]
fn trait_is_satisfiable_natively() {
    // No assertion: the test passes by the file compiling. If a method's
    // signature drifts between the trait and TestBackend, this test fails to
    // compile and the regression is caught.
    let _backend = TestBackend;
}
