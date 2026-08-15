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

#[cfg(all(
    feature = "wasm-sqlite",
    any(
        feature = "native-sqlite",
        feature = "_has-encryption",
        feature = "cli"
    )
))]
compile_error!(
    "The `wasm-sqlite` backend is mutually exclusive with the native backends and \
     with the CLI, server and MCP features, which are native-only.\n\
     If your manifest names none of those, they came from the defaults: Cargo adds \
     the default features to whatever you list, so `features = [\"wasm-sqlite\"]` \
     enables the native backend as well. Turn them off:\n  \
     dynoxide-rs = { version = \"...\", default-features = false, features = [\"wasm-sqlite\"] }"
);

#[cfg(not(any(
    feature = "native-sqlite",
    feature = "_has-encryption",
    feature = "wasm-sqlite"
)))]
compile_error!(
    "A storage backend feature must be enabled: `native-sqlite`, `encryption`, \
     `encryption-cc`, or `wasm-sqlite`. Default features include `native-sqlite`. \
     If you used `default-features = false`, add one of these features."
);

pub mod actions;
pub mod auth_material;
pub mod errors;
pub mod expressions;
#[cfg(feature = "import")]
pub mod import;
#[doc(hidden)]
pub mod macros;
#[cfg(feature = "mcp-server")]
pub mod mcp;
#[cfg(any(feature = "http-server", feature = "mcp-server"))]
pub(crate) mod net;
pub mod partiql;
pub mod schema;
// Surface-neutral mapping of serde failures to DynamoDB errors; the HTTP
// server consumes the request wrappers and the shared message cleaning is
// used wherever raw serde messages are decoded by hand.
pub(crate) mod serde_errors;
#[cfg(feature = "http-server")]
pub mod server;
#[cfg(feature = "mcp-server")]
pub(crate) mod snapshots;
pub mod storage;
pub mod storage_backend;
pub mod streams;
pub mod ttl;
pub mod types;
pub mod validation;
// The single source of truth for DynamoDB operation names, shared by the HTTP
// server and the wasm engine API so the two lists cannot drift. Compiled only
// for the builds that consume it.
#[cfg(any(feature = "http-server", feature = "wasm-sqlite", test))]
pub(crate) mod dynamo_ops;
// Operation-level engine API for the browser playground. The generic dispatch
// is backend-agnostic and verified natively in tests, so the module compiles
// for the wasm build and under `cargo test`; a plain native build gains no
// extra public surface.
#[cfg(any(feature = "wasm-sqlite", test))]
pub mod wasm_api;
#[cfg(feature = "wasm-harness")]
pub mod wasm_harness;

#[doc(hidden)]
pub use macros::ItemInsert;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use web_time::{Duration, Instant};

pub use errors::{DynoxideError, Result};
pub use storage::{DatabaseInfo, TableInfoEntry, TableMetadata, TableStats};
pub use storage_backend::BackendError;
#[cfg(feature = "wasm-sqlite")]
pub use storage_backend::WasmBridgeBackend;
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

/// One idempotency slot: when the token was claimed, the request hash it was
/// claimed with, and the response once the call finishes.
///
/// `None` in the response position means a call has claimed the token and is
/// still running. Only the asynchronous driver writes that state; the
/// synchronous one holds the cache lock across its call instead, so a slot it
/// finds is always a finished one.
type TokenSlot<T> = (Instant, u64, Option<T>);

/// Idempotency cache keyed by `ClientRequestToken`.
type TokenCache<T> = HashMap<String, TokenSlot<T>>;

/// Cached `TransactWriteItems` responses, alongside the image sizes a replay is
/// billed against. Those sizes are cache bookkeeping, so they ride here rather
/// than on the response type a caller sees.
type TransactWriteTokenCache = TokenCache<actions::transact_write_items::CachedWrite>;

/// Cached `ExecuteTransaction` responses. Separate from
/// [`TransactWriteTokenCache`] because the response type differs and
/// `ClientRequestToken` idempotency is scoped per API operation in AWS: a token
/// reused across `TransactWriteItems` and `ExecuteTransaction` executes once in
/// each, so the two caches are independent by design.
type ExecuteTransactionTokenCache = TokenCache<actions::execute_transaction::CachedTransaction>;

/// The transactional idempotency caches one engine instance owns.
///
/// Opaque by design: the slot shape is an implementation detail. Construct one
/// with [`TokenCaches::new`] and lend it to a dispatch that needs it.
#[derive(Default)]
pub struct TokenCaches {
    // Read by the native transactional path; a backend-neutral build has no
    // such caller yet, so scope the exemption to exactly that configuration
    // rather than blanket-allowing it.
    #[cfg_attr(
        not(any(feature = "native-sqlite", feature = "_has-encryption")),
        allow(dead_code)
    )]
    transact_write: Mutex<TransactWriteTokenCache>,
    execute_transaction: Mutex<ExecuteTransactionTokenCache>,
}

impl TokenCaches {
    /// An empty set of caches.
    pub fn new() -> Self {
        Self::default()
    }

    #[cfg(any(feature = "wasm-sqlite", test))]
    pub(crate) fn execute_transaction(&self) -> &Mutex<ExecuteTransactionTokenCache> {
        &self.execute_transaction
    }
}

/// AWS caps `ClientRequestToken` at 36 characters.
const MAX_TOKEN_LEN: usize = 36;

/// AWS scopes transactional idempotency to a 10-minute window. Entries older
/// than this are evicted on the next token-bearing call.
const TOKEN_EXPIRY_SECS: u64 = 600;

/// Reject a token longer than DynamoDB accepts, with its exact message.
fn validate_token(token: Option<&str>) -> Result<()> {
    match token {
        Some(token) if token.len() > MAX_TOKEN_LEN => {
            Err(DynoxideError::ValidationException(format!(
                "1 validation error detected: Value '{token}' at 'clientRequestToken' failed to satisfy constraint: Member must have length less than or equal to {MAX_TOKEN_LEN}"
            )))
        }
        _ => Ok(()),
    }
}

/// Hash the idempotency key material.
///
/// The input is the items or statements only, never `ReturnConsumedCapacity`,
/// so a same-token call differing only in the capacity mode replays rather than
/// mismatching. Normalised through `serde_json::Value` first so the digest does
/// not depend on map iteration order.
fn request_hash<H: serde::Serialize>(input: &H) -> u64 {
    use std::hash::{Hash, Hasher};
    let normalised = serde_json::to_value(input)
        .and_then(|v| serde_json::to_vec(&v))
        .unwrap_or_default();
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    normalised.hash(&mut hasher);
    hasher.finish()
}

fn lock_cache<T>(cache: &Mutex<TokenCache<T>>) -> Result<std::sync::MutexGuard<'_, TokenCache<T>>> {
    cache
        .lock()
        .map_err(|e| DynoxideError::InternalServerError(format!("Lock poisoned: {e}")))
}

/// Drop entries older than `window`.
///
/// The window is a parameter so a test can drive expiry without back-dating an
/// `Instant`, which is not always possible: `Instant`'s origin is boot, so
/// subtracting ten minutes fails on a machine that has been up for less.
fn evict_expired<T>(cache: &mut TokenCache<T>, window: Duration) {
    cache.retain(|_, (claimed_at, _, _)| claimed_at.elapsed() < window);
}

/// The expiry window every caller outside the tests uses.
fn token_window() -> Duration {
    Duration::from_secs(TOKEN_EXPIRY_SECS)
}

/// Run a transactional operation with `ClientRequestToken` idempotency, shared
/// by [`Database::transact_write_items`] and [`Database::execute_transaction`].
///
/// The cache lock is held across the whole first call (check, execute, insert)
/// so two concurrent same-token calls cannot both execute: the second
/// serialises behind the first and replays. That hold is the exclusion, which
/// is why this driver never needs to claim a slot the way the asynchronous one
/// does. A cache hit clones the stored response, releases the lock, then
/// rebuilds the reply via `replay` (which re-derives read capacity).
///
/// Lock ordering is cache-then-storage: `execute` takes the storage lock
/// second, and nothing takes storage first and then a token cache, so there is
/// no reverse path to deadlock against. Any future code touching both locks
/// must keep this order.
///
/// A failed `execute` is propagated without caching, so a same-token retry
/// re-executes.
#[cfg(any(feature = "native-sqlite", feature = "_has-encryption"))]
fn run_idempotent<T, H, E, R>(
    cache: &Mutex<TokenCache<T>>,
    token: Option<&str>,
    hash_input: &H,
    execute: E,
    replay: R,
) -> Result<T>
where
    T: Clone,
    H: serde::Serialize,
    E: FnOnce() -> Result<T>,
    R: FnOnce(&T) -> T,
{
    validate_token(token)?;

    // No idempotency token: execute without touching the cache.
    let Some(token) = token else {
        return execute();
    };
    let hash = request_hash(hash_input);

    let mut cache = lock_cache(cache)?;
    evict_expired(&mut cache, token_window());
    let cached = match cache.get(token) {
        Some((_, cached_hash, _)) if *cached_hash != hash => {
            return Err(DynoxideError::IdempotentParameterMismatchException(
                "An error occurred (IdempotentParameterMismatchException)".to_string(),
            ));
        }
        // A claimed-but-unfinished slot is treated as absent: this driver
        // holds its lock across the call, so it never writes one and never
        // shares a cache with the driver that does.
        Some((_, _, slot)) => slot.clone(),
        None => None,
    };
    if let Some(cached) = cached {
        // Clone the cached response, release the lock, then rebuild the reply.
        drop(cache);
        return Ok(replay(&cached));
    }
    // Cache miss: execute and record the result while still holding the lock,
    // so a concurrent same-token call waits and then replays rather than
    // executing the transaction a second time.
    let resp = execute()?;
    cache.insert(
        token.to_string(),
        (Instant::now(), hash, Some(resp.clone())),
    );
    Ok(resp)
}

/// What a refused caller is told. Unreachable from any shipped surface today,
/// so it stands for "something upstream stopped serialising callers".
#[cfg(any(feature = "wasm-sqlite", test))]
const IN_FLIGHT_MESSAGE: &str = "a call under this ClientRequestToken is still in flight";

/// What a caller found when it tried to claim a token slot.
#[cfg(any(feature = "wasm-sqlite", test))]
enum TokenClaim<T> {
    /// The slot was free and now belongs to this call, which must either
    /// complete it or clear it. Carries the stamp that identifies the claim,
    /// so a settle can tell its own slot from one a later call has taken over.
    Marked(Instant),
    /// Another call holds the slot and has not finished.
    InFlight,
    /// A call under this token used different request material.
    Mismatch,
    /// A finished call under this token; replay its response.
    Hit(T),
}

/// Look up a token and, when it is free, claim it - under one lock.
///
/// The lookup and the claim cannot be separate acquisitions: two callers would
/// both see a free slot and both proceed, which is the race the claim exists to
/// prevent.
#[cfg(any(feature = "wasm-sqlite", test))]
fn lookup_or_claim<T: Clone>(
    cache: &Mutex<TokenCache<T>>,
    token: &str,
    hash: u64,
) -> Result<TokenClaim<T>> {
    lookup_or_claim_within(cache, token, hash, token_window())
}

/// [`lookup_or_claim`] with an explicit expiry window, so a test can drive
/// expiry without depending on how long the machine has been running.
#[cfg(any(feature = "wasm-sqlite", test))]
fn lookup_or_claim_within<T: Clone>(
    cache: &Mutex<TokenCache<T>>,
    token: &str,
    hash: u64,
    window: Duration,
) -> Result<TokenClaim<T>> {
    let mut cache = lock_cache(cache)?;
    evict_expired(&mut cache, window);
    Ok(match cache.get(token) {
        Some((_, cached_hash, _)) if *cached_hash != hash => TokenClaim::Mismatch,
        Some((_, _, None)) => TokenClaim::InFlight,
        Some((_, _, Some(resp))) => TokenClaim::Hit(resp.clone()),
        None => {
            let claimed_at = Instant::now();
            cache.insert(token.to_string(), (claimed_at, hash, None));
            TokenClaim::Marked(claimed_at)
        }
    })
}

/// Is this slot still the claim `claimed_at` made?
///
/// A claim can expire while its call is in flight, and a later call can then
/// take the token over. The original call must not settle or release a slot
/// that is no longer its own, or it would overwrite the newcomer's response or
/// free a claim still being worked on.
#[cfg(any(feature = "wasm-sqlite", test))]
fn still_ours<T>(cache: &TokenCache<T>, token: &str, claimed_at: Instant) -> bool {
    matches!(cache.get(token), Some((at, _, None)) if *at == claimed_at)
}

/// Settle a claimed slot with the response its call produced.
#[cfg(any(feature = "wasm-sqlite", test))]
fn record_complete<T>(
    cache: &Mutex<TokenCache<T>>,
    token: &str,
    claimed_at: Instant,
    hash: u64,
    resp: T,
) -> Result<()> {
    let mut cache = lock_cache(cache)?;
    if still_ours(&cache, token, claimed_at) {
        cache.insert(token.to_string(), (claimed_at, hash, Some(resp)));
    }
    Ok(())
}

/// Release a claimed slot whose call failed, so a retry re-executes rather than
/// replaying the failure.
#[cfg(any(feature = "wasm-sqlite", test))]
fn clear_claim<T>(cache: &Mutex<TokenCache<T>>, token: &str, claimed_at: Instant) -> Result<()> {
    let mut cache = lock_cache(cache)?;
    if still_ours(&cache, token, claimed_at) {
        cache.remove(token);
    }
    Ok(())
}

/// [`run_idempotent`] for a caller that cannot block.
///
/// Takes a future rather than a closure, because the wasm engine awaits real
/// bridge promises. That rules out holding the cache lock across the call, so
/// exclusion comes from claiming the token slot up front instead: a concurrent
/// same-token caller finds the claim and is refused rather than starting a
/// second execution.
///
/// A refused caller currently gets an internal error. No shipped surface can
/// reach it - the engine serialises callers on the backend lock before either
/// arrives here - so it stands for "something upstream stopped serialising"
/// until an operation exists that can genuinely produce it.
#[cfg(any(feature = "wasm-sqlite", test))]
async fn run_idempotent_async<T, H, F, R>(
    cache: &Mutex<TokenCache<T>>,
    token: Option<&str>,
    hash_input: &H,
    execute: F,
    replay: R,
) -> Result<T>
where
    T: Clone,
    H: serde::Serialize,
    F: std::future::Future<Output = Result<T>>,
    R: FnOnce(&T) -> T,
{
    validate_token(token)?;

    let Some(token) = token else {
        return execute.await;
    };
    let hash = request_hash(hash_input);

    let claimed_at = match lookup_or_claim(cache, token, hash)? {
        TokenClaim::Hit(cached) => return Ok(replay(&cached)),
        TokenClaim::Mismatch => {
            return Err(DynoxideError::IdempotentParameterMismatchException(
                "An error occurred (IdempotentParameterMismatchException)".to_string(),
            ));
        }
        TokenClaim::InFlight => {
            return Err(DynoxideError::InternalServerError(
                IN_FLIGHT_MESSAGE.to_string(),
            ));
        }
        TokenClaim::Marked(claimed_at) => claimed_at,
    };

    match execute.await {
        Ok(resp) => {
            // The work has committed by this point, so a bookkeeping failure
            // must not be reported as a failed call. The worst it costs is a
            // replay: the slot stays claimed until it expires.
            let _ = record_complete(cache, token, claimed_at, hash, resp.clone());
            Ok(resp)
        }
        Err(e) => {
            let _ = clear_claim(cache, token, claimed_at);
            Err(e)
        }
    }
}

/// The native storage backend: the rusqlite-backed [`storage::Storage`].
///
/// `Database`'s type parameter defaults to this, so existing native callers
/// keep writing `Database` and get the synchronous rusqlite-backed engine.
#[cfg(any(feature = "native-sqlite", feature = "_has-encryption"))]
pub type RusqliteBackend = storage::Storage;

/// The native, synchronous `Database`.
///
/// Alias for the default [`Database`] monomorphisation over
/// [`RusqliteBackend`]. It exposes the historical synchronous public API
/// unchanged: each method drives an async handler future to completion with
/// `block_on`. Because the native backend's futures never suspend, that
/// `block_on` never parks the thread.
#[cfg(any(feature = "native-sqlite", feature = "_has-encryption"))]
pub type NativeDatabase = Database<RusqliteBackend>;

/// The wasm, asynchronous `Database` over the wasm SQLite backend.
///
/// Alias for [`Database`] monomorphised over [`WasmBridgeBackend`]. Unlike
/// [`NativeDatabase`], its methods are `async fn` and never call `block_on`:
/// the wasm backend awaits real SQLite-bridge promises, and the wasm main thread
/// must not block.
#[cfg(feature = "wasm-sqlite")]
pub type WasmDatabase = Database<WasmBridgeBackend>;

/// Build-visible preview marker for the wasm-sqlite backend.
///
/// `true` when built with `--no-default-features --features wasm-sqlite`,
/// `false` otherwise. The wasm backend covers CRUD, query, scan, GSI/LSI, and
/// PartiQL, and passes the conformance cases for all of them, but it still
/// leaves several operations unimplemented. Consumers can read this constant to
/// tell whether the artifact they hold is the fully conformant native build or
/// the wasm preview.
#[cfg(feature = "wasm-sqlite")]
pub const WASM_PREVIEW: bool = true;
/// Build-visible preview marker for the wasm-sqlite backend. See the
/// `wasm-sqlite` variant for details.
#[cfg(not(feature = "wasm-sqlite"))]
pub const WASM_PREVIEW: bool = false;

/// The main entry point for the DynamoDB emulator.
///
/// Generic over the storage backend `S`, monomorphised (no `dyn`). The type
/// parameter defaults to [`RusqliteBackend`], so `Database` means the native
/// engine and the public synchronous API is preserved via [`NativeDatabase`].
///
/// Wraps a storage layer and provides DynamoDB-compatible operations.
/// Thread-safe via `Arc<Mutex<>>`, so clone freely across threads.
#[cfg(any(feature = "native-sqlite", feature = "_has-encryption"))]
pub struct Database<S = RusqliteBackend> {
    inner: Arc<Mutex<S>>,
    tokens: Arc<TokenCaches>,
}

/// Serialises backend access on the backend-neutral build. On wasm this is an
/// async mutex: the bridge calls genuinely suspend, so a std mutex held across
/// them would deadlock concurrent callers on the single-threaded runtime,
/// whereas an async mutex queues them. Off wasm (the degenerate no-backend
/// shell, which can never construct a `Database`) a std mutex stands in.
#[cfg(all(
    not(any(feature = "native-sqlite", feature = "_has-encryption")),
    feature = "wasm-sqlite"
))]
use async_lock::Mutex as BackendMutex;
#[cfg(all(
    not(any(feature = "native-sqlite", feature = "_has-encryption")),
    not(feature = "wasm-sqlite")
))]
use std::sync::Mutex as BackendMutex;

/// The main entry point for the DynamoDB emulator (backend-neutral build).
///
/// On a build with no native backend (for example the `wasm-sqlite` build)
/// there is no native default, so the backend must be named explicitly - for
/// example `Database<WasmBridgeBackend>`, aliased as `WasmDatabase`.
///
/// Wraps a storage layer and provides DynamoDB-compatible operations. Backend
/// access is serialised by [`BackendMutex`] (an async mutex on wasm); clone
/// freely, only the `Arc`s are copied.
#[cfg(not(any(feature = "native-sqlite", feature = "_has-encryption")))]
pub struct Database<S> {
    inner: Arc<BackendMutex<S>>,
    tokens: Arc<TokenCaches>,
}

// Hand-written so cloning never requires `S: Clone`; only the `Arc`s clone.
impl<S> Clone for Database<S> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            tokens: Arc::clone(&self.tokens),
        }
    }
}

#[cfg(any(feature = "native-sqlite", feature = "_has-encryption"))]
impl Database<RusqliteBackend> {
    /// Open a persistent database at the given path.
    pub fn new(path: &str) -> Result<Self> {
        let storage = storage::Storage::new(path)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(storage)),
            tokens: Arc::new(TokenCaches::new()),
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
            tokens: Arc::new(TokenCaches::new()),
        })
    }

    /// Open an in-memory database (for tests and ephemeral use).
    pub fn memory() -> Result<Self> {
        let storage = storage::Storage::memory()?;
        Ok(Self {
            inner: Arc::new(Mutex::new(storage)),
            tokens: Arc::new(TokenCaches::new()),
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
        self.with_storage(|s| pollster::block_on(actions::create_table::execute(s, request)))
    }

    /// Delete a DynamoDB table.
    pub fn delete_table(
        &self,
        request: actions::delete_table::DeleteTableRequest,
    ) -> Result<actions::delete_table::DeleteTableResponse> {
        self.with_storage(|s| pollster::block_on(actions::delete_table::execute(s, request)))
    }

    /// Describe a DynamoDB table.
    pub fn describe_table(
        &self,
        request: actions::describe_table::DescribeTableRequest,
    ) -> Result<actions::describe_table::DescribeTableResponse> {
        self.with_storage(|s| pollster::block_on(actions::describe_table::execute(s, request)))
    }

    /// Update a DynamoDB table (add/remove GSIs).
    pub fn update_table(
        &self,
        request: actions::update_table::UpdateTableRequest,
    ) -> Result<actions::update_table::UpdateTableResponse> {
        self.with_storage(|s| pollster::block_on(actions::update_table::execute(s, request)))
    }

    /// List DynamoDB tables.
    pub fn list_tables(
        &self,
        request: actions::list_tables::ListTablesRequest,
    ) -> Result<actions::list_tables::ListTablesResponse> {
        self.with_storage(|s| pollster::block_on(actions::list_tables::execute(s, request)))
    }

    // -------------------------------------------------------------------
    // Tags
    // -------------------------------------------------------------------

    /// Add tags to a DynamoDB table.
    pub fn tag_resource(
        &self,
        request: actions::tag_resource::TagResourceRequest,
    ) -> Result<actions::tag_resource::TagResourceResponse> {
        self.with_storage(|s| pollster::block_on(actions::tag_resource::execute(s, request)))
    }

    /// Remove tags from a DynamoDB table.
    pub fn untag_resource(
        &self,
        request: actions::untag_resource::UntagResourceRequest,
    ) -> Result<actions::untag_resource::UntagResourceResponse> {
        self.with_storage(|s| pollster::block_on(actions::untag_resource::execute(s, request)))
    }

    /// List tags for a DynamoDB table.
    pub fn list_tags_of_resource(
        &self,
        request: actions::list_tags_of_resource::ListTagsOfResourceRequest,
    ) -> Result<actions::list_tags_of_resource::ListTagsOfResourceResponse> {
        self.with_storage(|s| {
            pollster::block_on(actions::list_tags_of_resource::execute(s, request))
        })
    }

    // -------------------------------------------------------------------
    // Item operations
    // -------------------------------------------------------------------

    /// Put an item into a DynamoDB table.
    pub fn put_item(
        &self,
        request: actions::put_item::PutItemRequest,
    ) -> Result<actions::put_item::PutItemResponse> {
        self.with_storage(|s| pollster::block_on(actions::put_item::execute(s, request)))
    }

    /// Get an item from a DynamoDB table.
    pub fn get_item(
        &self,
        request: actions::get_item::GetItemRequest,
    ) -> Result<actions::get_item::GetItemResponse> {
        self.with_storage(|s| pollster::block_on(actions::get_item::execute(s, request)))
    }

    /// Delete an item from a DynamoDB table.
    pub fn delete_item(
        &self,
        request: actions::delete_item::DeleteItemRequest,
    ) -> Result<actions::delete_item::DeleteItemResponse> {
        self.with_storage(|s| pollster::block_on(actions::delete_item::execute(s, request)))
    }

    /// Update an item in a DynamoDB table.
    pub fn update_item(
        &self,
        request: actions::update_item::UpdateItemRequest,
    ) -> Result<actions::update_item::UpdateItemResponse> {
        self.with_storage(|s| pollster::block_on(actions::update_item::execute(s, request)))
    }

    // -------------------------------------------------------------------
    // Batch operations
    // -------------------------------------------------------------------

    /// Batch get items from one or more DynamoDB tables.
    pub fn batch_get_item(
        &self,
        request: actions::batch_get_item::BatchGetItemRequest,
    ) -> Result<actions::batch_get_item::BatchGetItemResponse> {
        self.with_storage(|s| pollster::block_on(actions::batch_get_item::execute(s, request)))
    }

    /// Batch write items to one or more DynamoDB tables.
    pub fn batch_write_item(
        &self,
        request: actions::batch_write_item::BatchWriteItemRequest,
    ) -> Result<actions::batch_write_item::BatchWriteItemResponse> {
        self.with_storage(|s| pollster::block_on(actions::batch_write_item::execute(s, request)))
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
        self.with_storage(|s| {
            pollster::block_on(actions::import_items::execute(
                s, table_name, items, &options,
            ))
        })
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
            pollster::block_on(actions::import_items::execute_skip_gsi_deletes(
                s, table_name, items, &options,
            ))
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
        self.with_storage(|s| pollster::block_on(actions::query::execute(s, request)))
    }

    /// Scan a DynamoDB table.
    pub fn scan(&self, request: actions::scan::ScanRequest) -> Result<actions::scan::ScanResponse> {
        self.with_storage(|s| pollster::block_on(actions::scan::execute(s, request)))
    }

    // -------------------------------------------------------------------
    // Transactions
    // -------------------------------------------------------------------

    /// Execute a transactional write (up to 100 actions, all-or-nothing).
    ///
    /// Honours `ClientRequestToken` idempotency via [`run_idempotent`]: a
    /// same-token, same-items call within the expiry window replays the stored
    /// result (reported as transactional read capacity) without re-applying the
    /// writes.
    pub fn transact_write_items(
        &self,
        request: actions::transact_write_items::TransactWriteItemsRequest,
    ) -> Result<actions::transact_write_items::TransactWriteItemsResponse> {
        run_idempotent(
            &self.tokens.transact_write,
            request.client_request_token.as_deref(),
            &request.transact_items,
            || {
                self.with_storage(|s| {
                    pollster::block_on(actions::transact_write_items::execute_cached(
                        s,
                        request.clone(),
                    ))
                })
            },
            |cached| {
                // The replay recomputes a transactional read cost against the
                // image sizes the first call recorded (4KB read granularity,
                // diverging from that call's 1KB-granular write above 1KB) and
                // carries over its item collection metrics.
                actions::transact_write_items::replay_response(
                    cached,
                    &request.return_consumed_capacity,
                )
            },
        )
        .map(|cached| cached.response)
    }

    /// Execute a transactional read (up to 100 gets).
    pub fn transact_get_items(
        &self,
        request: actions::transact_get_items::TransactGetItemsRequest,
    ) -> Result<actions::transact_get_items::TransactGetItemsResponse> {
        self.with_storage(|s| pollster::block_on(actions::transact_get_items::execute(s, request)))
    }

    // -------------------------------------------------------------------
    // Streams
    // -------------------------------------------------------------------

    /// List DynamoDB Streams.
    pub fn list_streams(
        &self,
        request: actions::list_streams::ListStreamsRequest,
    ) -> Result<actions::list_streams::ListStreamsResponse> {
        self.with_storage(|s| pollster::block_on(actions::list_streams::execute(s, request)))
    }

    /// Describe a DynamoDB Stream.
    pub fn describe_stream(
        &self,
        request: actions::describe_stream::DescribeStreamRequest,
    ) -> Result<actions::describe_stream::DescribeStreamResponse> {
        self.with_storage(|s| pollster::block_on(actions::describe_stream::execute(s, request)))
    }

    /// Get a shard iterator.
    pub fn get_shard_iterator(
        &self,
        request: actions::get_shard_iterator::GetShardIteratorRequest,
    ) -> Result<actions::get_shard_iterator::GetShardIteratorResponse> {
        self.with_storage(|s| pollster::block_on(actions::get_shard_iterator::execute(s, request)))
    }

    /// Get stream records.
    pub fn get_records(
        &self,
        request: actions::get_records::GetRecordsRequest,
    ) -> Result<actions::get_records::GetRecordsResponse> {
        self.with_storage(|s| pollster::block_on(actions::get_records::execute(s, request)))
    }

    // -------------------------------------------------------------------
    // TTL
    // -------------------------------------------------------------------

    /// Update time to live configuration.
    pub fn update_time_to_live(
        &self,
        request: actions::update_time_to_live::UpdateTimeToLiveRequest,
    ) -> Result<actions::update_time_to_live::UpdateTimeToLiveResponse> {
        self.with_storage(|s| pollster::block_on(actions::update_time_to_live::execute(s, request)))
    }

    /// Describe time to live configuration.
    pub fn describe_time_to_live(
        &self,
        request: actions::describe_time_to_live::DescribeTimeToLiveRequest,
    ) -> Result<actions::describe_time_to_live::DescribeTimeToLiveResponse> {
        self.with_storage(|s| {
            pollster::block_on(actions::describe_time_to_live::execute(s, request))
        })
    }

    /// Run a TTL sweep, deleting expired items from all TTL-enabled tables.
    /// Returns the number of items deleted.
    pub fn sweep_ttl(&self) -> Result<usize> {
        self.with_storage(|s| pollster::block_on(ttl::sweep_expired_items(s)))
    }

    // -------------------------------------------------------------------
    // PartiQL
    // -------------------------------------------------------------------

    /// Execute a single PartiQL statement.
    pub fn execute_statement(
        &self,
        request: actions::execute_statement::ExecuteStatementRequest,
    ) -> Result<actions::execute_statement::ExecuteStatementResponse> {
        self.with_storage(|s| pollster::block_on(actions::execute_statement::execute(s, request)))
    }

    /// Execute PartiQL statements transactionally (all-or-nothing).
    ///
    /// Honours `ClientRequestToken` idempotency via [`run_idempotent`], the same
    /// way as [`transact_write_items`](Self::transact_write_items): a same-token,
    /// same-statements call within the expiry window replays the stored result
    /// without re-applying the statements. The cache is separate from the
    /// `TransactWriteItems` one (see [`ExecuteTransactionTokenCache`]).
    pub fn execute_transaction(
        &self,
        request: actions::execute_transaction::ExecuteTransactionRequest,
    ) -> Result<actions::execute_transaction::ExecuteTransactionResponse> {
        run_idempotent(
            &self.tokens.execute_transaction,
            request.client_request_token.as_deref(),
            &request.transact_statements,
            || {
                self.with_storage(|s| {
                    pollster::block_on(actions::execute_transaction::execute_cached(
                        s,
                        request.clone(),
                    ))
                })
            },
            |cached| {
                // The replay reports transactional read capacity against the
                // sizes the first call recorded, and carries over its responses.
                actions::execute_transaction::replay_response(
                    cached,
                    &request.return_consumed_capacity,
                )
            },
        )
        .map(|cached| cached.response)
    }

    /// Execute a batch of PartiQL statements.
    pub fn batch_execute_statement(
        &self,
        request: actions::batch_execute_statement::BatchExecuteStatementRequest,
    ) -> Result<actions::batch_execute_statement::BatchExecuteStatementResponse> {
        self.with_storage(|s| {
            pollster::block_on(actions::batch_execute_statement::execute(s, request))
        })
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

/// The wasm, asynchronous facade over the wasm SQLite backend.
///
/// Mirrors the native facade method-for-method, but each call is `async` and
/// awaits the shared action handler directly - there is no `block_on`, because
/// the wasm backend's bridge calls genuinely suspend.
///
/// Calls on one instance are serialised: each holds an async mutex over the
/// single SQLite connection for the whole handler, so a transaction's
/// begin..commit cannot interleave with another call, and concurrent callers
/// (for example two awaited operations on one `WasmDatabase`) queue rather
/// than deadlock. Because the mutex is async, queuing suspends instead of
/// blocking the single-threaded runtime; because there is only ever one
/// writer at a time, `BEGIN IMMEDIATE` cannot return `SQLITE_BUSY`.
#[cfg(feature = "wasm-sqlite")]
impl Database<WasmBridgeBackend> {
    /// Open (or create) a SQLite database persisted to OPFS under `name`,
    /// degrading to an ephemeral in-memory session where OPFS is unavailable.
    pub async fn open(name: &str) -> Result<Self> {
        Self::open_with(name, false).await
    }

    /// Open as [`open`](Self::open), but force an ephemeral in-memory session
    /// when `ephemeral` is true.
    pub async fn open_with(name: &str, ephemeral: bool) -> Result<Self> {
        let backend = WasmBridgeBackend::open_with(name, ephemeral)
            .await
            .map_err(DynoxideError::from)?;
        Ok(Self {
            inner: Arc::new(BackendMutex::new(backend)),
            tokens: Arc::new(TokenCaches::new()),
        })
    }

    /// The active persistence mode: `"opfs"`, `"memory"`, or `"unknown"`.
    pub async fn persistence_mode(&self) -> String {
        self.backend().await.persistence_mode().to_string()
    }

    /// Close the underlying SQLite connection. The operation-level engine
    /// calls this before re-opening, so the previous connection is released
    /// rather than leaked when a new database replaces it.
    pub async fn close(&self) -> Result<()> {
        self.backend()
            .await
            .close()
            .await
            .map_err(DynoxideError::from)
    }

    /// Lock the single backend for the span of one handler call. The guard is
    /// held across the whole call so the operation (including any transaction)
    /// is atomic; the async mutex queues concurrent callers rather than
    /// deadlocking, and never poisons.
    ///
    /// `pub(crate)` so the operation-level [`wasm_api`](crate::wasm_api) engine
    /// can hold the lock across a whole `execute` dispatch, matching the
    /// per-handler atomicity of the wrappers below.
    pub(crate) async fn backend(&self) -> async_lock::MutexGuard<'_, WasmBridgeBackend> {
        self.inner.lock().await
    }

    /// The idempotency caches this instance owns, for a dispatch that has to
    /// honour `ClientRequestToken`. Outlives any single call, so a replay finds
    /// the earlier one's result.
    pub(crate) fn token_caches(&self) -> &TokenCaches {
        &self.tokens
    }

    /// Create a new DynamoDB table.
    pub async fn create_table(
        &self,
        request: actions::create_table::CreateTableRequest,
    ) -> Result<actions::create_table::CreateTableResponse> {
        let backend = self.backend().await;
        actions::create_table::execute(&*backend, request).await
    }

    /// Delete a DynamoDB table.
    pub async fn delete_table(
        &self,
        request: actions::delete_table::DeleteTableRequest,
    ) -> Result<actions::delete_table::DeleteTableResponse> {
        let backend = self.backend().await;
        actions::delete_table::execute(&*backend, request).await
    }

    /// Describe a DynamoDB table.
    pub async fn describe_table(
        &self,
        request: actions::describe_table::DescribeTableRequest,
    ) -> Result<actions::describe_table::DescribeTableResponse> {
        let backend = self.backend().await;
        actions::describe_table::execute(&*backend, request).await
    }

    /// List DynamoDB tables.
    pub async fn list_tables(
        &self,
        request: actions::list_tables::ListTablesRequest,
    ) -> Result<actions::list_tables::ListTablesResponse> {
        let backend = self.backend().await;
        actions::list_tables::execute(&*backend, request).await
    }

    /// Put an item into a DynamoDB table.
    pub async fn put_item(
        &self,
        request: actions::put_item::PutItemRequest,
    ) -> Result<actions::put_item::PutItemResponse> {
        let backend = self.backend().await;
        actions::put_item::execute(&*backend, request).await
    }

    /// Get an item from a DynamoDB table.
    pub async fn get_item(
        &self,
        request: actions::get_item::GetItemRequest,
    ) -> Result<actions::get_item::GetItemResponse> {
        let backend = self.backend().await;
        actions::get_item::execute(&*backend, request).await
    }

    /// Delete an item from a DynamoDB table.
    pub async fn delete_item(
        &self,
        request: actions::delete_item::DeleteItemRequest,
    ) -> Result<actions::delete_item::DeleteItemResponse> {
        let backend = self.backend().await;
        actions::delete_item::execute(&*backend, request).await
    }

    /// Query a DynamoDB table or secondary index.
    pub async fn query(
        &self,
        request: actions::query::QueryRequest,
    ) -> Result<actions::query::QueryResponse> {
        let backend = self.backend().await;
        actions::query::execute(&*backend, request).await
    }

    /// Scan a DynamoDB table or secondary index.
    pub async fn scan(
        &self,
        request: actions::scan::ScanRequest,
    ) -> Result<actions::scan::ScanResponse> {
        let backend = self.backend().await;
        actions::scan::execute(&*backend, request).await
    }
}

#[cfg(all(test, any(feature = "native-sqlite", feature = "_has-encryption")))]
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

    #[test]
    fn test_native_database_alias_round_trips() {
        // The `NativeDatabase` alias is the default `Database<RusqliteBackend>`
        // and must drive the async handlers through the synchronous facade
        // transparently: a put/get round-trip behaves exactly as before.
        let db: NativeDatabase = Database::memory().unwrap();

        db.create_table(actions::create_table::CreateTableRequest {
            table_name: "tbl".to_string(),
            key_schema: vec![types::KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: types::KeyType::HASH,
            }],
            attribute_definitions: vec![types::AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: types::ScalarAttributeType::S,
            }],
            ..Default::default()
        })
        .unwrap();

        let mut item = HashMap::new();
        item.insert("pk".to_string(), AttributeValue::S("a".to_string()));
        db.put_item(actions::put_item::PutItemRequest {
            table_name: "tbl".to_string(),
            item,
            ..Default::default()
        })
        .unwrap();

        let mut key = HashMap::new();
        key.insert("pk".to_string(), AttributeValue::S("a".to_string()));
        let got = db
            .get_item(actions::get_item::GetItemRequest {
                table_name: "tbl".to_string(),
                key,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(
            got.item.unwrap().get("pk"),
            Some(&AttributeValue::S("a".to_string()))
        );
    }
}

/// Cache primitives and the asynchronous driver.
///
/// These are private, so they are exercised here rather than from an
/// integration test. The public-facade behaviour both drivers must preserve
/// lives in `tests/execute_transaction.rs` and `tests/transactions.rs`.
#[cfg(test)]
mod idempotency_tests {
    use super::*;
    use std::cell::Cell;

    const KEY: &str = "statements";
    const TOKEN: &str = "tok";

    fn cache() -> Mutex<TokenCache<u32>> {
        Mutex::new(HashMap::new())
    }

    fn hash() -> u64 {
        request_hash(&KEY)
    }

    fn drive<F>(cache: &Mutex<TokenCache<u32>>, token: Option<&str>, execute: F) -> Result<u32>
    where
        F: std::future::Future<Output = Result<u32>>,
    {
        pollster::block_on(run_idempotent_async(cache, token, &KEY, execute, |c| *c))
    }

    #[test]
    fn a_claim_is_visible_to_the_next_caller_and_settles_into_a_hit() {
        let cache = cache();

        let TokenClaim::Marked(at) = lookup_or_claim(&cache, TOKEN, hash()).unwrap() else {
            panic!("the first caller should have claimed the token");
        };
        assert!(matches!(
            lookup_or_claim(&cache, TOKEN, hash()).unwrap(),
            TokenClaim::InFlight
        ));

        record_complete(&cache, TOKEN, at, hash(), 7).unwrap();
        assert!(matches!(
            lookup_or_claim(&cache, TOKEN, hash()).unwrap(),
            TokenClaim::Hit(7)
        ));
    }

    #[test]
    fn only_one_caller_can_claim_a_token() {
        // Split the lookup and the claim into two acquisitions and this goes
        // red: several racing callers would each find the slot free. Sequential
        // calls would not catch that, so this has to be threaded.
        use std::sync::Barrier;

        const N: usize = 16;
        let cache = cache();
        let barrier = Barrier::new(N);

        let marked = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..N)
                .map(|_| {
                    scope.spawn(|| {
                        barrier.wait();
                        matches!(
                            lookup_or_claim(&cache, TOKEN, hash()).unwrap(),
                            TokenClaim::Marked(_)
                        )
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|h| h.join().unwrap())
                .filter(|claimed| *claimed)
                .count()
        });

        assert_eq!(marked, 1);
    }

    #[test]
    fn a_call_whose_claim_expired_does_not_disturb_the_caller_that_took_over() {
        // A claim can expire while its call is still running, letting a second
        // caller take the token. The first must not then settle or release a
        // slot that is no longer its own.
        let cache = cache();
        let TokenClaim::Marked(first) = lookup_or_claim(&cache, TOKEN, hash()).unwrap() else {
            panic!("the first caller should have claimed the token");
        };
        let TokenClaim::Marked(second) =
            lookup_or_claim_within(&cache, TOKEN, hash(), Duration::ZERO).unwrap()
        else {
            panic!("the expired claim should have been re-issued");
        };
        assert_ne!(first, second);

        // The first call finishing must not overwrite the second's slot.
        record_complete(&cache, TOKEN, first, hash(), 1).unwrap();
        assert!(matches!(
            lookup_or_claim(&cache, TOKEN, hash()).unwrap(),
            TokenClaim::InFlight
        ));

        // Nor must the first call failing release it.
        clear_claim(&cache, TOKEN, first).unwrap();
        assert!(matches!(
            lookup_or_claim(&cache, TOKEN, hash()).unwrap(),
            TokenClaim::InFlight
        ));

        // The caller that owns the claim still settles it.
        record_complete(&cache, TOKEN, second, hash(), 2).unwrap();
        assert!(matches!(
            lookup_or_claim(&cache, TOKEN, hash()).unwrap(),
            TokenClaim::Hit(2)
        ));
    }

    #[test]
    fn a_different_request_under_the_same_token_mismatches_in_both_states() {
        let other = request_hash(&"different");

        let cache = cache();
        let TokenClaim::Marked(at) = lookup_or_claim(&cache, TOKEN, hash()).unwrap() else {
            panic!("expected a free slot to claim");
        };
        assert!(matches!(
            lookup_or_claim(&cache, TOKEN, other).unwrap(),
            TokenClaim::Mismatch
        ));

        record_complete(&cache, TOKEN, at, hash(), 7).unwrap();
        assert!(matches!(
            lookup_or_claim(&cache, TOKEN, other).unwrap(),
            TokenClaim::Mismatch
        ));
    }

    #[test]
    fn the_claim_lands_before_the_call_is_polled() {
        // Swapping the claim and the poll in the driver turns this red.
        let cache = cache();
        let claimed_first = Cell::new(false);

        let out = drive(&cache, Some(TOKEN), async {
            // try_lock, not lock: a driver still holding the guard across the
            // call would hang here, and a hang reads worse than an assertion.
            let slots = cache
                .try_lock()
                .expect("the cache lock must be released before the call runs");
            claimed_first.set(matches!(slots.get(TOKEN), Some((_, h, None)) if *h == hash()));
            Ok(7)
        });

        assert_eq!(out.unwrap(), 7);
        assert!(claimed_first.get());
    }

    #[test]
    fn a_caller_arriving_under_a_live_claim_does_not_execute() {
        let cache = cache();
        let second_ran = Cell::new(false);

        let out = drive(&cache, Some(TOKEN), async {
            let second = drive(&cache, Some(TOKEN), async {
                second_ran.set(true);
                Ok(0)
            });
            // Pinned exactly: InternalServerError is also what a poisoned lock
            // produces, so the variant alone would not tell them apart.
            assert_eq!(second.unwrap_err().to_string(), IN_FLIGHT_MESSAGE);
            // The refusal must leave the claim alone; releasing it here would
            // let a third caller start a second execution.
            assert!(matches!(
                lookup_or_claim(&cache, TOKEN, hash()).unwrap(),
                TokenClaim::InFlight
            ));
            Ok(1)
        });

        assert_eq!(out.unwrap(), 1);
        assert!(!second_ran.get(), "the second call must not run the work");
        // And the outer call still settles the slot it owns.
        assert!(matches!(
            lookup_or_claim(&cache, TOKEN, hash()).unwrap(),
            TokenClaim::Hit(1)
        ));
    }

    #[test]
    fn the_synchronous_driver_treats_a_live_claim_as_absent() {
        // The two drivers never share a cache today, so this cannot happen -
        // the synchronous driver holds its lock across the call and never
        // writes a claim. Pinned so that if they ever do share one, the
        // double-apply shows up here rather than in production.
        let cache = cache();
        lookup_or_claim(&cache, TOKEN, hash()).unwrap();

        let out = run_idempotent(&cache, Some(TOKEN), &KEY, || Ok(9), |c| *c).unwrap();
        assert_eq!(
            out, 9,
            "a claimed slot is not treated as a replayable result"
        );
    }

    #[test]
    fn a_settled_token_replays_without_re_executing() {
        let cache = cache();
        let runs = Cell::new(0);

        for _ in 0..2 {
            let out = drive(&cache, Some(TOKEN), async {
                runs.set(runs.get() + 1);
                Ok(7)
            });
            assert_eq!(out.unwrap(), 7);
        }
        assert_eq!(runs.get(), 1);
    }

    #[test]
    fn a_failed_call_releases_its_claim_so_a_retry_re_executes() {
        let cache = cache();
        let runs = Cell::new(0);

        let first = drive(&cache, Some(TOKEN), async {
            runs.set(runs.get() + 1);
            Err(DynoxideError::ValidationException("no".into()))
        });
        assert!(first.is_err());
        assert!(
            cache.lock().unwrap().is_empty(),
            "the claim must be released"
        );

        let second = drive(&cache, Some(TOKEN), async {
            runs.set(runs.get() + 1);
            Ok(7)
        });
        assert_eq!(second.unwrap(), 7);
        assert_eq!(runs.get(), 2);
    }

    #[test]
    fn a_claim_left_by_a_dropped_call_expires_rather_than_wedging_the_token() {
        // A dropped future leaves its claim behind. Expiry is what stops that
        // wedging the token until the process restarts.
        let cache = cache();
        lookup_or_claim(&cache, TOKEN, hash()).unwrap();

        assert!(matches!(
            lookup_or_claim_within(&cache, TOKEN, hash(), Duration::ZERO).unwrap(),
            TokenClaim::Marked(_)
        ));
    }

    #[test]
    fn a_settled_token_stops_replaying_once_it_expires() {
        let cache = cache();
        let TokenClaim::Marked(at) = lookup_or_claim(&cache, TOKEN, hash()).unwrap() else {
            panic!("expected a free slot to claim");
        };
        record_complete(&cache, TOKEN, at, hash(), 7).unwrap();

        // Inside the window it still replays.
        assert!(matches!(
            lookup_or_claim_within(&cache, TOKEN, hash(), token_window()).unwrap(),
            TokenClaim::Hit(7)
        ));
        // Past it, the token is free again.
        assert!(matches!(
            lookup_or_claim_within(&cache, TOKEN, hash(), Duration::ZERO).unwrap(),
            TokenClaim::Marked(_)
        ));
    }

    #[test]
    fn concurrent_callers_under_one_token_execute_the_work_once() {
        // The asynchronous driver exists because it cannot hold a lock across
        // its call, so this is the property that matters most for it. The
        // synchronous driver has the same test in tests/execute_transaction.rs.
        use std::sync::Barrier;
        use std::sync::atomic::{AtomicUsize, Ordering};

        const N: usize = 16;
        let cache = cache();
        let runs = AtomicUsize::new(0);
        let barrier = Barrier::new(N);

        let outcomes: Vec<Result<u32>> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..N)
                .map(|_| {
                    scope.spawn(|| {
                        barrier.wait();
                        drive(&cache, Some(TOKEN), async {
                            runs.fetch_add(1, Ordering::SeqCst);
                            Ok(7)
                        })
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });

        assert_eq!(runs.load(Ordering::SeqCst), 1, "the work must run once");
        // Every caller either gets the value or is told a call is in flight;
        // none may run the work a second time.
        for outcome in outcomes {
            match outcome {
                Ok(v) => assert_eq!(v, 7),
                Err(e) => assert_eq!(e.to_string(), IN_FLIGHT_MESSAGE),
            }
        }
    }

    #[test]
    fn an_overlong_token_is_rejected_with_dynamodbs_message() {
        let token = "x".repeat(MAX_TOKEN_LEN + 1);
        let err = drive(&cache(), Some(&token), async { Ok(7) }).unwrap_err();
        assert_eq!(
            err.to_string(),
            format!(
                "1 validation error detected: Value '{token}' at 'clientRequestToken' failed to satisfy constraint: Member must have length less than or equal to {MAX_TOKEN_LEN}"
            )
        );
    }

    #[test]
    fn a_tokenless_call_never_touches_the_cache() {
        let cache = cache();
        let runs = Cell::new(0);

        for _ in 0..2 {
            drive(&cache, None, async {
                runs.set(runs.get() + 1);
                Ok(7)
            })
            .unwrap();
        }
        assert_eq!(runs.get(), 2);
        assert!(cache.lock().unwrap().is_empty());
    }
}
