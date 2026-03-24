//! # Dynoxide
//!
//! A lightweight, embeddable DynamoDB emulator backed by SQLite.
//!
//! ```rust
//! use dynoxide::Database;
//!
//! let db = Database::memory().unwrap();
//! ```

#[cfg(all(feature = "native-sqlite", feature = "_has-encryption"))]
compile_error!(
    "Features `native-sqlite` and `encryption`/`encryption-cc` are mutually exclusive.\n\
     If you ran `cargo install`, use:\n  \
     cargo install dynoxide-rs --no-default-features --features encrypted-server\n\
     If using as a library dependency, set `default-features = false` \
     and enable only one backend."
);

#[cfg(all(feature = "encryption", feature = "encryption-cc"))]
compile_error!(
    "Features `encryption` and `encryption-cc` are mutually exclusive. \
     Use `encryption` for vendored OpenSSL or `encryption-cc` for Apple CommonCrypto."
);

#[cfg(all(feature = "encryption-cc", not(target_vendor = "apple")))]
compile_error!(
    "The `encryption-cc` feature is intended for Apple platforms only (CommonCrypto). \
     Use the `encryption` feature for vendored OpenSSL on non-Apple platforms."
);

#[cfg(not(any(feature = "native-sqlite", feature = "_has-encryption")))]
compile_error!(
    "Either `native-sqlite`, `encryption`, or `encryption-cc` feature must be enabled. \
     Default features include `native-sqlite`. If you used \
     `default-features = false`, add one of these features."
);

pub mod actions;
pub mod errors;
pub mod expressions;
#[cfg(feature = "import")]
pub mod import;
#[doc(hidden)]
pub mod macros;
#[cfg(feature = "mcp-server")]
pub mod mcp;
pub mod partiql;
pub mod schema;
#[cfg(feature = "http-server")]
pub mod server;
#[cfg(feature = "mcp-server")]
pub(crate) mod snapshots;
pub mod storage;
pub mod streams;
pub mod ttl;
pub mod types;
pub mod validation;

#[doc(hidden)]
pub use macros::ItemInsert;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

pub use errors::{DynoxideError, Result};
pub use storage::{DatabaseInfo, TableInfoEntry, TableMetadata, TableStats};
pub use types::{AttributeValue, ConversionError, Item};

/// Options for `Database::import_items()`.
#[derive(Debug, Clone, Default)]
pub struct ImportOptions {
    /// Whether to record stream events for imported items. Default: false.
    pub record_streams: bool,
    /// Whether to set `cached_at` to the current timestamp. Default: false.
    pub set_cached_at: bool,
}

/// Result of a bulk import operation.
#[derive(Debug, Clone)]
pub struct ImportResult {
    /// Number of items imported.
    pub items_imported: usize,
    /// Total bytes imported (sum of item_size values).
    pub bytes_imported: usize,
}

/// Cached transaction response with timestamp and request hash for idempotency.
type TokenCache = HashMap<
    String,
    (
        Instant,
        u64,
        actions::transact_write_items::TransactWriteItemsResponse,
    ),
>;

/// The main entry point for the DynamoDB emulator.
///
/// Wraps a SQLite-backed storage layer and provides DynamoDB-compatible
/// operations. Thread-safe via `Arc<Mutex<>>` — clone freely across threads.
#[derive(Clone)]
pub struct Database {
    inner: Arc<Mutex<storage::Storage>>,
    idempotency_tokens: Arc<Mutex<TokenCache>>,
}

impl Database {
    /// Open a persistent database at the given path.
    pub fn new(path: &str) -> Result<Self> {
        let storage = storage::Storage::new(path)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(storage)),
            idempotency_tokens: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Open or create an encrypted database at the given path.
    ///
    /// The key must be a 64-character hex string representing a 32-byte key.
    /// Example: `"000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"`
    ///
    /// The key is passed to SQLCipher via `PRAGMA key`. The database file is
    /// encrypted at rest using AES-256-CBC.
    ///
    /// # Security
    ///
    /// This function borrows the key as `&str` and cannot zeroize the caller's
    /// copy. The caller is responsible for zeroizing owned key material after
    /// this call returns (e.g., by using `zeroize::Zeroizing<String>`).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The key format is invalid (not 64 hex characters)
    /// - The database exists but was created without encryption
    /// - The database exists but the key is wrong
    #[cfg(feature = "_has-encryption")]
    pub fn new_encrypted(path: &str, key: &str) -> Result<Self> {
        if key.len() != 64 || !key.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(DynoxideError::ValidationException(
                "Encryption key must be a 64-character hex string (32 bytes)".to_string(),
            ));
        }

        let storage = storage::Storage::new_encrypted(path, key)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(storage)),
            idempotency_tokens: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Open an in-memory database (for tests and ephemeral use).
    pub fn memory() -> Result<Self> {
        let storage = storage::Storage::memory()?;
        Ok(Self {
            inner: Arc::new(Mutex::new(storage)),
            idempotency_tokens: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Execute a closure with exclusive access to the storage layer.
    pub(crate) fn with_storage<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&storage::Storage) -> Result<T>,
    {
        let guard = self
            .inner
            .lock()
            .map_err(|e| DynoxideError::InternalServerError(format!("Lock poisoned: {e}")))?;
        f(&guard)
    }

    /// Execute a closure with mutable exclusive access to the storage layer.
    pub(crate) fn with_storage_mut<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&mut storage::Storage) -> Result<T>,
    {
        let mut guard = self
            .inner
            .lock()
            .map_err(|e| DynoxideError::InternalServerError(format!("Lock poisoned: {e}")))?;
        f(&mut guard)
    }

    // -------------------------------------------------------------------
    // Table operations
    // -------------------------------------------------------------------

    /// Create a new DynamoDB table.
    pub fn create_table(
        &self,
        request: actions::create_table::CreateTableRequest,
    ) -> Result<actions::create_table::CreateTableResponse> {
        self.with_storage(|s| actions::create_table::execute(s, request))
    }

    /// Delete a DynamoDB table.
    pub fn delete_table(
        &self,
        request: actions::delete_table::DeleteTableRequest,
    ) -> Result<actions::delete_table::DeleteTableResponse> {
        self.with_storage(|s| actions::delete_table::execute(s, request))
    }

    /// Describe a DynamoDB table.
    pub fn describe_table(
        &self,
        request: actions::describe_table::DescribeTableRequest,
    ) -> Result<actions::describe_table::DescribeTableResponse> {
        self.with_storage(|s| actions::describe_table::execute(s, request))
    }

    /// Update a DynamoDB table (add/remove GSIs).
    pub fn update_table(
        &self,
        request: actions::update_table::UpdateTableRequest,
    ) -> Result<actions::update_table::UpdateTableResponse> {
        self.with_storage(|s| actions::update_table::execute(s, request))
    }

    /// List DynamoDB tables.
    pub fn list_tables(
        &self,
        request: actions::list_tables::ListTablesRequest,
    ) -> Result<actions::list_tables::ListTablesResponse> {
        self.with_storage(|s| actions::list_tables::execute(s, request))
    }

    // -------------------------------------------------------------------
    // Tags
    // -------------------------------------------------------------------

    /// Add tags to a DynamoDB table.
    pub fn tag_resource(
        &self,
        request: actions::tag_resource::TagResourceRequest,
    ) -> Result<actions::tag_resource::TagResourceResponse> {
        self.with_storage(|s| actions::tag_resource::execute(s, request))
    }

    /// Remove tags from a DynamoDB table.
    pub fn untag_resource(
        &self,
        request: actions::untag_resource::UntagResourceRequest,
    ) -> Result<actions::untag_resource::UntagResourceResponse> {
        self.with_storage(|s| actions::untag_resource::execute(s, request))
    }

    /// List tags for a DynamoDB table.
    pub fn list_tags_of_resource(
        &self,
        request: actions::list_tags_of_resource::ListTagsOfResourceRequest,
    ) -> Result<actions::list_tags_of_resource::ListTagsOfResourceResponse> {
        self.with_storage(|s| actions::list_tags_of_resource::execute(s, request))
    }

    // -------------------------------------------------------------------
    // Item operations
    // -------------------------------------------------------------------

    /// Put an item into a DynamoDB table.
    pub fn put_item(
        &self,
        request: actions::put_item::PutItemRequest,
    ) -> Result<actions::put_item::PutItemResponse> {
        self.with_storage(|s| actions::put_item::execute(s, request))
    }

    /// Get an item from a DynamoDB table.
    pub fn get_item(
        &self,
        request: actions::get_item::GetItemRequest,
    ) -> Result<actions::get_item::GetItemResponse> {
        self.with_storage(|s| actions::get_item::execute(s, request))
    }

    /// Delete an item from a DynamoDB table.
    pub fn delete_item(
        &self,
        request: actions::delete_item::DeleteItemRequest,
    ) -> Result<actions::delete_item::DeleteItemResponse> {
        self.with_storage(|s| actions::delete_item::execute(s, request))
    }

    /// Update an item in a DynamoDB table.
    pub fn update_item(
        &self,
        request: actions::update_item::UpdateItemRequest,
    ) -> Result<actions::update_item::UpdateItemResponse> {
        self.with_storage(|s| actions::update_item::execute(s, request))
    }

    // -------------------------------------------------------------------
    // Batch operations
    // -------------------------------------------------------------------

    /// Batch get items from one or more DynamoDB tables.
    pub fn batch_get_item(
        &self,
        request: actions::batch_get_item::BatchGetItemRequest,
    ) -> Result<actions::batch_get_item::BatchGetItemResponse> {
        self.with_storage(|s| actions::batch_get_item::execute(s, request))
    }

    /// Batch write items to one or more DynamoDB tables.
    pub fn batch_write_item(
        &self,
        request: actions::batch_write_item::BatchWriteItemRequest,
    ) -> Result<actions::batch_write_item::BatchWriteItemResponse> {
        self.with_storage(|s| actions::batch_write_item::execute(s, request))
    }

    /// Import items in bulk, bypassing per-item size validation.
    ///
    /// All items are inserted in a single transaction. If any item fails,
    /// the entire import is rolled back. Items with duplicate keys within
    /// the batch are resolved by last-write-wins (later items in the vec
    /// overwrite earlier items with the same primary key).
    ///
    /// GSI entries are maintained: items with GSI key attributes are
    /// inserted into the appropriate GSI tables. Items missing GSI key
    /// attributes are silently omitted from the GSI (sparse GSI behavior,
    /// matching DynamoDB semantics).
    ///
    /// Stream records are NOT generated by default. Use
    /// `ImportOptions { record_streams: true, .. }` if stream recording is needed.
    pub fn import_items(
        &self,
        table_name: &str,
        items: Vec<Item>,
        options: ImportOptions,
    ) -> Result<ImportResult> {
        self.with_storage(|s| actions::import_items::execute(s, table_name, items, &options))
    }

    /// Import items in bulk, skipping GSI DELETE-before-INSERT.
    ///
    /// Same as `import_items` but assumes the database is fresh (no
    /// pre-existing rows), so GSI cleanup deletes are skipped entirely.
    /// This eliminates the dominant bottleneck for large imports.
    #[cfg(feature = "import")]
    pub(crate) fn import_items_fresh(
        &self,
        table_name: &str,
        items: Vec<Item>,
        options: ImportOptions,
    ) -> Result<ImportResult> {
        self.with_storage(|s| {
            actions::import_items::execute_skip_gsi_deletes(s, table_name, items, &options)
        })
    }

    // -------------------------------------------------------------------
    // Bulk loading
    // -------------------------------------------------------------------

    /// Set aggressive SQLite PRAGMAs for bulk loading.
    ///
    /// Only safe when data loss on crash is acceptable (e.g., fresh import).
    /// Call `disable_bulk_loading()` after the import to restore normal settings.
    pub fn enable_bulk_loading(&self) -> Result<()> {
        self.with_storage(|s| s.enable_bulk_loading())
    }

    /// Restore normal SQLite PRAGMAs after bulk loading.
    pub fn disable_bulk_loading(&self) -> Result<()> {
        self.with_storage(|s| s.disable_bulk_loading())
    }

    // -------------------------------------------------------------------
    // Query & Scan
    // -------------------------------------------------------------------

    /// Query a DynamoDB table.
    pub fn query(
        &self,
        request: actions::query::QueryRequest,
    ) -> Result<actions::query::QueryResponse> {
        self.with_storage(|s| actions::query::execute(s, request))
    }

    /// Scan a DynamoDB table.
    pub fn scan(&self, request: actions::scan::ScanRequest) -> Result<actions::scan::ScanResponse> {
        self.with_storage(|s| actions::scan::execute(s, request))
    }

    // -------------------------------------------------------------------
    // Transactions
    // -------------------------------------------------------------------

    /// Execute a transactional write (up to 100 actions, all-or-nothing).
    pub fn transact_write_items(
        &self,
        request: actions::transact_write_items::TransactWriteItemsRequest,
    ) -> Result<actions::transact_write_items::TransactWriteItemsResponse> {
        const TOKEN_EXPIRY_SECS: u64 = 600; // 10 minutes
        const MAX_TOKEN_LEN: usize = 36;

        // Validate token length
        if let Some(ref token) = request.client_request_token {
            if token.len() > MAX_TOKEN_LEN {
                return Err(DynoxideError::ValidationException(format!(
                    "1 validation error detected: Value '{}' at 'clientRequestToken' failed to satisfy constraint: Member must have length less than or equal to {}",
                    token, MAX_TOKEN_LEN
                )));
            }
        }

        // Compute request hash for idempotency comparison.
        // Normalise via serde_json::Value (uses BTreeMap internally) to ensure
        // deterministic key ordering regardless of HashMap iteration order.
        let request_hash = if request.client_request_token.is_some() {
            use std::hash::{Hash, Hasher};
            let normalised = serde_json::to_value(&request.transact_items)
                .and_then(|v| serde_json::to_vec(&v))
                .unwrap_or_default();
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            normalised.hash(&mut hasher);
            hasher.finish()
        } else {
            0
        };

        // Check idempotency cache
        if let Some(ref token) = request.client_request_token {
            let mut cache = self
                .idempotency_tokens
                .lock()
                .map_err(|e| DynoxideError::InternalServerError(format!("Lock poisoned: {e}")))?;
            // Evict expired entries
            cache.retain(|_, (ts, _, _)| ts.elapsed().as_secs() < TOKEN_EXPIRY_SECS);
            if let Some((_, cached_hash, resp)) = cache.get(token) {
                if *cached_hash != request_hash {
                    return Err(DynoxideError::IdempotentParameterMismatchException(
                        "An error occurred (IdempotentParameterMismatchException)".to_string(),
                    ));
                }
                return Ok(resp.clone());
            }
        }

        let resp =
            self.with_storage(|s| actions::transact_write_items::execute(s, request.clone()))?;

        // Cache the response if token was provided
        if let Some(ref token) = request.client_request_token {
            if let Ok(mut cache) = self.idempotency_tokens.lock() {
                cache.insert(token.clone(), (Instant::now(), request_hash, resp.clone()));
            }
        }

        Ok(resp)
    }

    /// Execute a transactional read (up to 100 gets).
    pub fn transact_get_items(
        &self,
        request: actions::transact_get_items::TransactGetItemsRequest,
    ) -> Result<actions::transact_get_items::TransactGetItemsResponse> {
        self.with_storage(|s| actions::transact_get_items::execute(s, request))
    }

    // -------------------------------------------------------------------
    // Streams
    // -------------------------------------------------------------------

    /// List DynamoDB Streams.
    pub fn list_streams(
        &self,
        request: actions::list_streams::ListStreamsRequest,
    ) -> Result<actions::list_streams::ListStreamsResponse> {
        self.with_storage(|s| actions::list_streams::execute(s, request))
    }

    /// Describe a DynamoDB Stream.
    pub fn describe_stream(
        &self,
        request: actions::describe_stream::DescribeStreamRequest,
    ) -> Result<actions::describe_stream::DescribeStreamResponse> {
        self.with_storage(|s| actions::describe_stream::execute(s, request))
    }

    /// Get a shard iterator.
    pub fn get_shard_iterator(
        &self,
        request: actions::get_shard_iterator::GetShardIteratorRequest,
    ) -> Result<actions::get_shard_iterator::GetShardIteratorResponse> {
        self.with_storage(|s| actions::get_shard_iterator::execute(s, request))
    }

    /// Get stream records.
    pub fn get_records(
        &self,
        request: actions::get_records::GetRecordsRequest,
    ) -> Result<actions::get_records::GetRecordsResponse> {
        self.with_storage(|s| actions::get_records::execute(s, request))
    }

    // -------------------------------------------------------------------
    // TTL
    // -------------------------------------------------------------------

    /// Update time to live configuration.
    pub fn update_time_to_live(
        &self,
        request: actions::update_time_to_live::UpdateTimeToLiveRequest,
    ) -> Result<actions::update_time_to_live::UpdateTimeToLiveResponse> {
        self.with_storage(|s| actions::update_time_to_live::execute(s, request))
    }

    /// Describe time to live configuration.
    pub fn describe_time_to_live(
        &self,
        request: actions::describe_time_to_live::DescribeTimeToLiveRequest,
    ) -> Result<actions::describe_time_to_live::DescribeTimeToLiveResponse> {
        self.with_storage(|s| actions::describe_time_to_live::execute(s, request))
    }

    /// Run a TTL sweep, deleting expired items from all TTL-enabled tables.
    /// Returns the number of items deleted.
    pub fn sweep_ttl(&self) -> Result<usize> {
        self.with_storage(ttl::sweep_expired_items)
    }

    // -------------------------------------------------------------------
    // PartiQL
    // -------------------------------------------------------------------

    /// Execute a single PartiQL statement.
    pub fn execute_statement(
        &self,
        request: actions::execute_statement::ExecuteStatementRequest,
    ) -> Result<actions::execute_statement::ExecuteStatementResponse> {
        self.with_storage(|s| actions::execute_statement::execute(s, request))
    }

    /// Execute PartiQL statements transactionally (all-or-nothing).
    pub fn execute_transaction(
        &self,
        request: actions::execute_transaction::ExecuteTransactionRequest,
    ) -> Result<actions::execute_transaction::ExecuteTransactionResponse> {
        self.with_storage(|s| actions::execute_transaction::execute(s, request))
    }

    /// Execute a batch of PartiQL statements.
    pub fn batch_execute_statement(
        &self,
        request: actions::batch_execute_statement::BatchExecuteStatementRequest,
    ) -> Result<actions::batch_execute_statement::BatchExecuteStatementResponse> {
        self.with_storage(|s| actions::batch_execute_statement::execute(s, request))
    }

    // -------------------------------------------------------------------
    // Cache tracking
    // -------------------------------------------------------------------

    /// Update the `cached_at` timestamp for a single item.
    ///
    /// Used by cache layers to track when items were last fetched from a
    /// remote source. The timestamp is a Unix epoch in seconds (f64).
    pub fn touch_cached_at(
        &self,
        table_name: &str,
        pk: &str,
        sk: &str,
        timestamp: f64,
    ) -> Result<()> {
        self.with_storage(|s| s.touch_cached_at(table_name, pk, sk, timestamp))
    }

    /// Get items ordered by `cached_at` (oldest first) for LRU eviction.
    ///
    /// Returns `(pk, sk, item_size)` tuples. Items with NULL `cached_at`
    /// are excluded (they were never cached from a remote source).
    pub fn get_lru_items(
        &self,
        table_name: &str,
        limit: usize,
    ) -> Result<Vec<(String, String, i64)>> {
        self.with_storage(|s| s.get_lru_items(table_name, limit))
    }

    // -------------------------------------------------------------------
    // Introspection
    // -------------------------------------------------------------------

    /// Get the database file path, or `None` for in-memory databases.
    pub fn db_path(&self) -> Result<Option<String>> {
        self.with_storage(|s| Ok(s.db_path()))
    }

    /// Get the total database size in bytes.
    pub fn db_size_bytes(&self) -> Result<u64> {
        self.with_storage(|s| s.db_size_bytes())
    }

    /// Count the number of DynamoDB tables.
    pub fn table_count(&self) -> Result<usize> {
        self.with_storage(|s| s.table_count())
    }

    /// Get per-table statistics: name, item count, and approximate size in bytes.
    pub fn table_stats(&self) -> Result<Vec<TableStats>> {
        self.with_storage(|s| s.table_stats())
    }

    /// Get metadata for a specific table (key schema, GSIs, TTL config, etc.).
    pub fn get_table_metadata(&self, table_name: &str) -> Result<Option<storage::TableMetadata>> {
        self.with_storage(|s| s.get_table_metadata(table_name))
    }

    /// Get combined database info atomically in a single lock acquisition.
    ///
    /// Returns path, size, table count, and per-table stats + metadata.
    /// Avoids the consistency issues of calling individual methods separately.
    pub fn database_info(&self) -> Result<DatabaseInfo> {
        self.with_storage(|s| s.database_info())
    }

    // -------------------------------------------------------------------
    // Snapshot operations
    // -------------------------------------------------------------------

    /// Run VACUUM to compact the database file in place.
    pub fn vacuum(&self) -> Result<()> {
        self.with_storage(|s| s.vacuum())
    }

    /// Create a snapshot of the database by copying it to the given path.
    ///
    /// Uses SQLite's `VACUUM INTO` which works for both in-memory and
    /// file-backed databases. The snapshot is a standalone SQLite file.
    pub fn vacuum_into(&self, path: &str) -> Result<()> {
        self.with_storage(|s| s.vacuum_into(path))
    }

    /// Restore the database from a snapshot file.
    ///
    /// Uses SQLite's backup API to replace the current database contents
    /// with the snapshot. Works for both in-memory and file-backed databases.
    /// The backup is atomic — either all pages are copied or none are.
    pub fn restore_from(&self, path: &str) -> Result<()> {
        self.with_storage_mut(|s| s.restore_from(path))
    }

    /// Backup the current database to a new in-memory SQLite connection.
    ///
    /// Returns an owned `Connection` holding a complete copy. Used for
    /// in-memory snapshot storage — no filesystem side-effects.
    #[cfg(feature = "mcp-server")]
    pub(crate) fn backup_to_memory(&self) -> Result<rusqlite::Connection> {
        self.with_storage(|s| s.backup_to_memory())
    }

    /// Restore the database from an in-memory SQLite connection.
    ///
    /// Replaces current contents with the source connection's data.
    #[cfg(feature = "mcp-server")]
    pub(crate) fn restore_from_connection(&self, source: &rusqlite::Connection) -> Result<()> {
        self.with_storage_mut(|s| s.restore_from_connection(source))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_database_memory() {
        let db = Database::memory().unwrap();
        // Should be able to clone (Arc)
        let _db2 = db.clone();
    }

    #[test]
    fn test_database_with_storage() {
        let db = Database::memory().unwrap();
        let tables = db.with_storage(|s| s.list_table_names()).unwrap();
        assert!(tables.is_empty());
    }

    #[test]
    fn test_database_thread_safe() {
        let db = Database::memory().unwrap();
        let db2 = db.clone();

        let handle =
            std::thread::spawn(move || db2.with_storage(|s| s.list_table_names()).unwrap());

        let tables = handle.join().unwrap();
        assert!(tables.is_empty());
    }
}
